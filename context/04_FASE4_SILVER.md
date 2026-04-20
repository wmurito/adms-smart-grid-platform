# Fase 4 — Camada Silver (Transformação e Enriquecimento)

## Objetivo

Transformar os dados brutos Bronze em dados confiáveis, deduplicados e enriquecidos para análise. A Silver é a camada de **dado curado** — qualidade garantida, schema tipado, campos de negócio adicionados.

**Princípio Silver**: Bronze + deduplicação + tipagem correta + enriquecimento contextual + flags de negócio.

---

## Notebooks desta fase

| Notebook | Fonte | Destino |
|---|---|---|
| `03_silver_eventos_rede.py` | `adms.bronze.eventos_rede_raw` | `adms.silver.eventos_rede_enriquecido` |
| `04_silver_leituras_medidor.py` | `adms.bronze.leituras_medidor_raw` | `adms.silver.leituras_medidor_norm` |

**Local:** `silver/notebooks/`

---

## Notebook 03: Silver — Eventos de Rede Enriquecidos

### O que este notebook faz (em ordem)

```
1. Carregar Bronze
2. Injetar dados mestres de subestações (topologia inline)
3. Deduplicar por evento_id
4. Join com topologia (enriquecer com dados da subestação)
5. Converter timestamps para TimestampType
6. Parear INTERRUPCAO_INICIO ↔ INTERRUPCAO_FIM
7. Calcular flags regulatórias e score de criticidade
8. Salvar Silver (overwrite)
```

### Célula 2: Topologia inline (importante!)

Como o Databricks não acessa o `topologia.py` local, os dados das 20 subestações são recriados como DataFrame Spark inline:

```python
subestacoes_data = [
    ("SE01","SE Cataguases",      "Zona da Mata",    "Cataguases",      -21.389,-42.696,138,40.0,1985),
    ("SE02","SE Leopoldina",      ...),
    # ... 20 subestações
]

schema_se = StructType([
    StructField("se_id",            StringType(),  False),
    StructField("se_nome",          StringType(),  True),
    StructField("se_regional",      StringType(),  True),
    StructField("se_municipio",     StringType(),  True),
    StructField("se_latitude",      DoubleType(),  True),
    StructField("se_longitude",     DoubleType(),  True),
    StructField("se_tensao_kv",     IntegerType(), True),
    StructField("se_capacidade_mva",DoubleType(),  True),
    StructField("se_ano_instalacao",IntegerType(), True),
])

df_subestacoes = spark.createDataFrame(subestacoes_data, schema=schema_se)
# Calcular idade da subestação (ANO_ATUAL - se_ano_instalacao)
df_subestacoes = df_subestacoes.withColumn("se_idade_anos", lit(2026) - col("se_ano_instalacao"))
```

> ⚠️ **Manutenção**: Se adicionar novas subestações no `topologia.py`, lembrar de atualizar `subestacoes_data` neste notebook.

### Célula 3: Deduplicação por evento_id

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, desc

# Manter apenas o registro mais recente por evento_id
window_dedup = Window.partitionBy("evento_id").orderBy(desc("_ingest_timestamp"))

df_dedup = (
    df_bronze
    .withColumn("_rn", row_number().over(window_dedup))
    .filter(col("_rn") == 1)
    .drop("_rn")
)

# Verificar: duplicatas = total_bronze - total_dedup
```

### Célula 4: Enriquecimento com topologia

```python
df_enriquecido = (
    df_dedup
    .join(
        df_subestacoes.select(
            "se_id", "se_capacidade_mva", "se_tensao_kv",
            "se_idade_anos", "se_latitude", "se_longitude"
        ),
        df_dedup["subestacao_id"] == df_subestacoes["se_id"],
        how="left"    # left join: preservar eventos mesmo sem match
    )
    .drop("se_id")
    .withColumn("ts_evento",   to_timestamp(col("timestamp_evento")))   # String → Timestamp
    .withColumn("ts_ingestao", to_timestamp(col("timestamp_ingestao")))
)
```

### Célula 5: Pareamento INTERRUPCAO_INICIO ↔ INTERRUPCAO_FIM

**Problema**: eventos de interrupção chegam em pares. Para calcular a duração real (não estimada), precisamos associar cada INICIO com seu FIM correspondente.

**Lógica de pareamento**:
1. Separar eventos de INICIO e FIM
2. Join por `alimentador_id`
3. Filtrar: FIM deve ser após INICIO e dentro de 6 horas
4. Para cada INICIO, pegar o FIM mais próximo (row_number)

```python
# Separar eventos de início de interrupção
df_inicio = (
    df_enriquecido
    .filter(col("tipo_evento") == "INTERRUPCAO_INICIO")
    .select(
        col("evento_id").alias("id_inicio"),
        col("alimentador_id").alias("al_inicio"),
        col("ts_evento").alias("ts_inicio"),
        col("clientes_afetados").alias("clientes_inicio"),
        col("duracao_estimada_min"),
    )
)

# Separar eventos de fim
df_fim = (
    df_enriquecido
    .filter(col("tipo_evento") == "INTERRUPCAO_FIM")
    .select(
        col("evento_id").alias("id_fim"),
        col("alimentador_id").alias("al_fim"),
        col("ts_evento").alias("ts_fim"),
    )
)

# Join e filtro de janela temporal (6 horas = 21600 segundos)
df_pares = (
    df_inicio
    .join(df_fim, df_inicio["al_inicio"] == df_fim["al_fim"], how="left")
    .filter(
        col("ts_fim").isNull() |
        (
            (col("ts_fim") >= col("ts_inicio")) &
            (unix_timestamp("ts_fim") - unix_timestamp("ts_inicio") <= 6 * 3600)
        )
    )
    .withColumn(
        "duracao_real_min",
        when(
            col("ts_fim").isNotNull(),
            _round((unix_timestamp("ts_fim") - unix_timestamp("ts_inicio")) / 60, 1)
        ).otherwise(col("duracao_estimada_min"))  # fallback se não encontrar FIM
    )
    # Para cada INICIO, pegar o FIM mais próximo
    .withColumn("_rn_par", row_number().over(
        Window.partitionBy("id_inicio").orderBy("ts_fim")
    ))
    .filter(col("_rn_par") == 1)
    .select("id_inicio", "duracao_real_min")
)
```

### Célula 6: Flags regulatórias e score de criticidade

#### Flag climático
```python
is_evento_climatico = (precipitacao_mm > 5.0) | (velocidade_vento_kmh > 60.0)
```

#### Reclassificação ANEEL (classe_aneel)
```python
classe_aneel = (
    when(tipo_evento == "MANOBRA_PLANEJADA", "PLANEJADA")
    .when(tipo_evento == "MANOBRA_EMERGENCIAL", "EMERGENCIAL")
    .when(is_evento_climatico & tipo_evento.isin("FALHA_EQUIPAMENTO", "INTERRUPCAO_INICIO"),
          "CASO_FORTUITO")   # Climático → exonera parcialmente
    .when(tipo_evento.isin("FALHA_EQUIPAMENTO", "INTERRUPCAO_INICIO"),
          "NAO_PLANEJADA")
    .otherwise("INFORMATIVO")
)
```

#### Score de criticidade (0-100)
Composto por 4 dimensões com pesos diferentes:

| Dimensão | Peso | Cálculo |
|---|---|---|
| Severidade | 40 pts | CRITICO=40, ALTO=30, MEDIO=15, BAIXO=5 |
| Impacto em clientes | 30 pts | min(30, clientes_afetados/10000*30) |
| Duração estimada | 20 pts | min(20, duracao_estimada_min/240*20) |
| Idade da subestação | 10 pts | min(10, se_idade_anos/40*10) |

```python
score_criticidade = score_severidade + score_impacto + score_duracao + score_ativo

faixa_criticidade = (
    when(score_criticidade >= 70, "CRITICO")
    .when(score_criticidade >= 40, "ALTO")
    .when(score_criticidade >= 20, "MEDIO")
    .otherwise("BAIXO")
)
```

#### Enriquecimento temporal
```python
hora_evento  = hour(ts_evento)           # 0-23
dia_semana   = dayofweek(ts_evento)      # 1=Domingo, 7=Sábado
periodo_dia  = (0-5h: MADRUGADA | 6-11h: MANHA | 12-17h: TARDE | 18+: NOITE)
```

### Célula 7: Persistência Silver

```python
(
    df_silver
    .write
    .format("delta")
    .mode("overwrite")               # Overwrite (Silver é reprocessada)
    .option("overwriteSchema", "true")
    .partitionBy("_ano", "_mes", "regional")   # Por regional (analytics por região)
    .saveAsTable("adms.silver.eventos_rede_enriquecido")
)
```

### Colunas Silver — Eventos Enriquecidos

**Herdadas do Bronze** (todos os campos originais) **+**:

| Coluna nova | Tipo | Descrição |
|---|---|---|
| `ts_evento` | Timestamp | `timestamp_evento` convertido |
| `ts_ingestao` | Timestamp | `timestamp_ingestao` convertido |
| `se_capacidade_mva` | Double | Capacidade da subestação (join) |
| `se_tensao_kv` | Integer | Tensão da subestação (join) |
| `se_idade_anos` | Integer | Idade da subestação em anos |
| `se_latitude`, `se_longitude` | Double | Coordenadas da SE (join) |
| `duracao_interrupcao_min` | Double | Duração real calculada (ou estimada) |
| `is_evento_climatico` | Boolean | Clima influenciou? (chuva>5mm ou vento>60km/h) |
| `classe_aneel` | String | PLANEJADA/NAO_PLANEJADA/EMERGENCIAL/CASO_FORTUITO/INFORMATIVO |
| `score_criticidade` | Double | Score 0-100 composto |
| `faixa_criticidade` | String | CRITICO/ALTO/MEDIO/BAIXO |
| `hora_evento` | Integer | Hora do dia (0-23) |
| `dia_semana` | Integer | Dia da semana (1-7) |
| `periodo_dia` | String | MADRUGADA/MANHA/TARDE/NOITE |
| `silver_processado_em` | Timestamp | Quando Silver processou |
| `versao_silver` | String | "1.0.0" |

---

## Notebook 04: Silver — Leituras de Medidores Normalizadas

### O que este notebook faz (em ordem)

```
1. Carregar Bronze
2. Deduplicar por leitura_id
3. Calcular delta_kwh com LAG por medidor (consumo real no intervalo)
4. Classificar tensão conforme PRODIST ANEEL Módulo 8
5. Calcular score de anomalia de consumo (z-score por alimentador)
6. Calcular score de fraude (lógica multi-sinal)
7. Enriquecer temporalmente
8. Persistir Silver
```

### Célula 3: Delta kWh com LAG (consumo no intervalo)

```python
# Janela por medidor ordenada por timestamp de leitura
window_medidor = Window.partitionBy("medidor_id").orderBy("ts_leitura")

df_delta = (
    df_dedup
    .withColumn("kwh_ativo_anterior", lag("kwh_ativo", 1).over(window_medidor))
    .withColumn("ts_leitura_anterior", lag("ts_leitura", 1).over(window_medidor))
    .withColumn(
        "intervalo_real_min",
        _round((unix_timestamp("ts_leitura") - unix_timestamp("ts_leitura_anterior")) / 60, 1)
    )
    .withColumn(
        "delta_kwh",
        when(
            col("kwh_ativo_anterior").isNotNull(),
            _round(col("kwh_ativo") - col("kwh_ativo_anterior"), 4)
        ).otherwise(col("kwh_ativo"))   # Primeira leitura: usar valor atual
    )
    .withColumn("is_delta_negativo", col("delta_kwh") < 0)  # Reset ou fraude
    .withColumn(
        "delta_kwh_ajustado",
        when(col("delta_kwh") < 0, lit(0.0)).otherwise(col("delta_kwh"))  # Normalizar neg para 0
    )
)
```

**Delta negativo** = leitura atual menor que anterior → indica: reset do medidor ou manipulação para burlar faturamento.

### Célula 4: Classificação ANEEL de Tensão (PRODIST Módulo 8)

```python
def classificar_tensao_aneel(tensao_col, nominal_col):
    return (
        when(
            (tensao_col >= nominal_col * 0.95) & (tensao_col <= nominal_col * 1.05),
            "ADEQUADA"      # ±5% da nominal
        )
        .when(
            ((tensao_col >= nominal_col * 0.90) & (tensao_col < nominal_col * 0.95)) |
            ((tensao_col > nominal_col * 1.05) & (tensao_col <= nominal_col * 1.10)),
            "PRECARIA"      # ±10% (fora de ±5%)
        )
        .otherwise("CRITICA")  # Fora de ±10%
    )

# Tensão nominal varia por tipo de medidor
tensao_nominal_v = (
    when(tipo_medidor == "INDUSTRIAL", lit(380.0))
    .when(tipo_medidor == "RURAL",     lit(127.0))
    .otherwise(lit(220.0))
)
```

### Célula 5: Score de anomalia de consumo (z-score)

**Lógica**: consumo de um medidor é comparado com a média do seu alimentador (mesmo tipo de medidor). Desvios estatísticos indicam fraude ou problema técnico.

```python
# Média e desvio por alimentador + tipo de medidor
window_alim = Window.partitionBy("alimentador_id", "tipo_medidor")

df_score = (
    df_tensao
    .withColumn("media_kwh_alim",   avg("delta_kwh_ajustado").over(window_alim))
    .withColumn("stddev_kwh_alim",  stddev("delta_kwh_ajustado").over(window_alim))
    .withColumn(
        "z_score_consumo",
        when(stddev > 0, _abs(delta - media) / stddev).otherwise(lit(0.0))
    )
    .withColumn(
        "score_anomalia",    # z=3 → score=100
        _round(when(z_score >= 3, lit(100.0)).otherwise(z_score / 3 * 100), 1)
    )
    .withColumn(
        "faixa_anomalia",
        when(score >= 80, "ALTA").when(score >= 50, "MEDIA")
        .when(score >= 20, "BAIXA").otherwise("NORMAL")
    )
)
```

### Célula 6: Score de fraude (multi-sinal)

Combina 4 sinais independentes para calcular um score composto de suspeita de fraude:

```python
score_fraude = (
    # Consumo anormalmente baixo (< 30% da média do alimentador)
    when(delta_kwh_ajustado < media_kwh_alim * 0.3, lit(40))
    .otherwise(lit(0))
    +
    # Delta negativo (manipulação do medidor)
    when(is_delta_negativo, lit(30))
    .otherwise(lit(0))
    +
    # Flag original do simulador
    when(is_possivel_fraude, lit(20))
    .otherwise(lit(0))
    +
    # Anomalia de consumo alta
    when(score_anomalia >= 80, lit(10))
    .otherwise(lit(0))
)

is_suspeita_fraude = score_fraude >= 50

nivel_suspeita = (
    when(score_fraude >= 70, "ALTO")
    .when(score_fraude >= 50, "MEDIO")
    .when(score_fraude >= 20, "BAIXO")
    .otherwise("NORMAL")
)
```

### Colunas Silver — Leituras Normalizadas

**Herdadas do Bronze** **+**:

| Coluna nova | Tipo | Descrição |
|---|---|---|
| `ts_leitura` | Timestamp | `timestamp_leitura` convertido |
| `delta_kwh` | Double | Consumo no intervalo (leitura atual - anterior) |
| `delta_kwh_ajustado` | Double | Delta normalizado (negativo → 0) |
| `is_delta_negativo` | Boolean | True se delta < 0 (suspeito) |
| `intervalo_real_min` | Double | Minutos desde leitura anterior |
| `tensao_nominal_v` | Double | 220V / 380V / 127V por tipo |
| `classe_tensao_aneel` | String | ADEQUADA / PRECARIA / CRITICA |
| `desvio_tensao_pct` | Double | % de desvio da tensão nominal |
| `media_kwh_alim` | Double | Média do alimentador (removida antes de salvar) |
| `z_score_consumo` | Double | Z-score vs média do alimentador (removido) |
| `score_anomalia` | Double | 0-100 baseado no z-score |
| `faixa_anomalia` | String | ALTA / MEDIA / BAIXA / NORMAL |
| `score_fraude` | Integer | 0-100 score multi-sinal |
| `is_suspeita_fraude` | Boolean | True se score >= 50 |
| `nivel_suspeita` | String | ALTO / MEDIO / BAIXO / NORMAL |
| `hora_leitura` | Integer | Hora do dia (0-23) |
| `dia_semana` | Integer | Dia da semana |
| `mes_leitura` | Integer | Mês da leitura |
| `periodo_dia` | String | MADRUGADA / MANHA / TARDE / NOITE |
| `classe_fp` | String | EXCELENTE(≥0.95) / BOM(≥0.92) / REGULAR(≥0.85) / RUIM |
| `silver_processado_em` | Timestamp | Data de processamento Silver |
| `versao_silver` | String | "1.0.0" |

### Particionamento Silver Leituras
```python
.partitionBy("_ano", "_mes", "subestacao_id")
# Por subestação (queries operacionais por área geográfica)
```

---

## Análises Silver — Consultas SQL de referência

### Eventos enriquecidos
```sql
-- DEC/FEC preliminar por regional
SELECT regional, classe_aneel, faixa_criticidade,
       COUNT(*) AS total,
       SUM(clientes_afetados) AS clientes_afetados,
       ROUND(AVG(score_criticidade), 1) AS score_medio,
       SUM(CASE WHEN is_evento_climatico THEN 1 ELSE 0 END) AS eventos_climaticos
FROM adms.silver.eventos_rede_enriquecido
GROUP BY regional, classe_aneel, faixa_criticidade
ORDER BY clientes_afetados DESC;

-- Top alimentadores críticos
SELECT alimentador_id, regional, municipio,
       COUNT(*) as total_eventos,
       SUM(clientes_afetados) as total_clientes_afetados
FROM adms.silver.eventos_rede_enriquecido
WHERE tipo_evento IN ('FALHA_EQUIPAMENTO', 'INTERRUPCAO_INICIO')
GROUP BY alimentador_id, regional, municipio
ORDER BY total_clientes_afetados DESC
LIMIT 10;
```

### Leituras normalizadas
```sql
-- Classificação tensão por tipo de medidor
SELECT tipo_medidor, classe_tensao_aneel, COUNT(*) as total
FROM adms.silver.leituras_medidor_norm
GROUP BY tipo_medidor, classe_tensao_aneel
ORDER BY tipo_medidor, classe_tensao_aneel;

-- Top alimentadores com maior fraude
SELECT alimentador_id, regional, municipio,
       COUNT(*) as suspeitas,
       ROUND(AVG(score_fraude), 1) as score_medio
FROM adms.silver.leituras_medidor_norm
WHERE is_suspeita_fraude = true
GROUP BY alimentador_id, regional, municipio
ORDER BY suspeitas DESC
LIMIT 10;
```

---

## Pontos de atenção Silver

### Window Functions com grandes volumes
- `Window.partitionBy("medidor_id").orderBy("ts_leitura")` pode ser pesado com 10k medidores × muitas leituras
- Para produção: considerar repartição adequada antes de aplicar a window

### Colunas temporárias removidas antes de salvar
```python
df_silver.drop(
    "kwh_ativo_anterior", "ts_leitura_anterior",   # Temporárias do LAG
    "media_kwh_alim", "stddev_kwh_alim",            # Temporárias do z-score
    "z_score_consumo",
    "score_severidade", "score_impacto",             # Temporárias do score eventos
    "score_duracao", "score_ativo"
)
```
