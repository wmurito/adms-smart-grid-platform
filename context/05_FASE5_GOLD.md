# Fase 5 — Camada Gold (KPIs Regulatórios e de Negócio)

## Objetivo

Calcular os indicadores de negócio e regulatórios finais a partir dos dados Silver. A Gold é a camada de **consumo direto** — tabelas otimizadas para dashboards, relatórios e tomada de decisão.

**Princípio Gold**: Silver agregado + métricas de negócio + conformidade regulatória + pronto para BI.

---

## Notebooks desta fase

| Notebook | Fontes Silver | Destino Gold |
|---|---|---|
| `05_gold_kpi_dec_fec.py` | `eventos_rede_enriquecido` + `leituras_medidor_norm` | `adms.gold.kpi_dec_fec_mensal` |
| `06_gold_saude_ativos.py` | `eventos_rede_enriquecido` | `adms.gold.saude_ativo_alimentador` |
| `07_gold_perdas_comerciais.py` | `leituras_medidor_norm` + `eventos_rede_enriquecido` | `adms.gold.perdas_comerciais_regional` |

**Local:** `gold/notebooks/`

---

## Notebook 05: Gold — KPIs DEC/FEC (ANEEL)

### Contexto regulatório

O **DEC** e **FEC** são os dois principais indicadores de qualidade de fornecimento exigidos pela ANEEL para distribuidoras de energia:

- **DEC** (Duração Equivalente de Interrupção por Unidade Consumidora):
  ```
  DEC = Σ(Ci × ti) / Cc
  Ci = clientes afetados no evento i
  ti = duração em horas do evento i
  Cc = total de clientes do conjunto (alimentador)
  ```

- **FEC** (Frequência Equivalente de Interrupção por Unidade Consumidora):
  ```
  FEC = Σ(Ci) / Cc
  ```

**Granularidade**: alimentador × mês × ano (PRODIST Módulo 8 é mensal).

### Quais eventos entram no cálculo?

Apenas interrupções **não planejadas** e de responsabilidade da distribuidora:

```python
TIPOS_INTERRUPCAO   = ["INTERRUPCAO_INICIO", "FALHA_EQUIPAMENTO"]
CLASSES_REGULATORIAS = ["NAO_PLANEJADA", "CASO_FORTUITO", "EMERGENCIAL"]
# Excluídos: PLANEJADA (obra programada), INFORMATIVO
```

### Cálculo completo (Célula 4)

```python
# 1. Numeradores por alimentador×mês
df_numeradores = (
    df_interrupcoes
    .groupBy("alimentador_id", "subestacao_id", "regional", "municipio", "ano", "mes")
    .agg(
        _sum(col("clientes_afetados") * col("duracao_horas")).alias("soma_client_horas"),  # DEC numerador
        _sum("clientes_afetados").alias("soma_clientes"),                                    # FEC numerador
        count("*").alias("num_interrupcoes"),
        avg("duracao_horas").alias("duracao_media_horas"),
        _max("duracao_horas").alias("duracao_max_horas"),
        _sum(when(col("is_evento_climatico"), 1).otherwise(0)).alias("interrupcoes_climaticas"),
        avg("score_criticidade").alias("score_criticidade_medio"),
    )
)

# 2. Join com total de clientes (denominador)
df_clientes = (
    spark.table("adms.silver.leituras_medidor_norm")
    .groupBy("alimentador_id", ...)
    .agg(countDistinct("cliente_id").alias("total_clientes_conjunto"))
)

# 3. Calcular DEC e FEC
df_dec_fec = (
    df_numeradores
    .join(df_clientes, on="alimentador_id", how="left")
    .withColumn("dec_horas",   _round(soma_client_horas / total_clientes_conjunto, 4))
    .withColumn("fec_vezes",   _round(soma_clientes / total_clientes_conjunto, 4))
    .withColumn("dec_minutos", _round(dec_horas * 60, 2))
)
```

### Limites regulatórios ANEEL (referência MG)

```python
LIMITE_DEC_URBANO_HORAS  = 8.0    # horas/mês
LIMITE_DEC_RURAL_HORAS   = 16.0   # horas/mês
LIMITE_FEC_URBANO_VEZES  = 6.0    # interrupções/mês
LIMITE_FEC_RURAL_VEZES   = 10.0   # interrupções/mês

# Pelo ID do alimentador (AL01-AL03=urbano, AL04-AL05=suburbano, AL06=rural)
tipo_area = (
    when(alimentador_id.endswith("AL01")|..."AL03", "URBANA")
    .when(..."AL05", "SUBURBANA")
    .otherwise("RURAL")
)
```

### Semáforo regulatório

```python
semaforo_dec = (
    when(pct_limite_dec >= 100, "VERMELHO")   # Violação
    .when(pct_limite_dec >= 80,  "AMARELO")   # Alerta (>80% do limite)
    .otherwise("VERDE")
)
```

### Schema da tabela `adms.gold.kpi_dec_fec_mensal`

| Coluna | Tipo | Descrição |
|---|---|---|
| `alimentador_id` | String | Chave primária de fact |
| `subestacao_id`, `regional`, `municipio` | String | Hierarquia geográfica |
| `ano`, `mes` | Integer | Granularidade temporal |
| `tipo_area` | String | URBANA / SUBURBANA / RURAL |
| `num_interrupcoes` | Long | Quantidade de interrupções no mês |
| `duracao_media_horas` | Double | Duração média das interrupções |
| `duracao_max_horas` | Double | Pior interrupção do mês |
| `interrupcoes_climaticas` | Long | Quantas foram por clima |
| `score_criticidade_medio` | Double | Score médio Silver |
| `total_clientes_conjunto` | Long | Total de clientes do alimentador |
| **`dec_horas`** | Double | **DEC em horas (KPI regulatório)** |
| **`dec_minutos`** | Double | **DEC em minutos (operacional)** |
| **`fec_vezes`** | Double | **FEC (KPI regulatório)** |
| `limite_dec_horas` | Double | Limite ANEEL por tipo de área |
| `limite_fec_vezes` | Double | Limite ANEEL por tipo de área |
| `pct_limite_dec` | Double | Uso do limite DEC em % |
| `pct_limite_fec` | Double | Uso do limite FEC em % |
| `violacao_dec` | Boolean | True se DEC > limite |
| `violacao_fec` | Boolean | True se FEC > limite |
| `violacao_alguma` | Boolean | True se qualquer violação |
| `semaforo_dec` | String | VERMELHO / AMARELO / VERDE |
| `semaforo_fec` | String | VERMELHO / AMARELO / VERDE |

### Particionamento
```python
.partitionBy("ano", "mes", "regional")
# Queries mais comuns: por período (ano/mes) e regional
```

---

## Notebook 06: Gold — Saúde de Ativos da Rede

### O que é o índice de saúde?

Score 0-100 que representa a **confiabilidade de um alimentador** baseado em seu histórico de falhas. Alimentadores com baixo score precisam de intervenção.

```
0-30  → CRÍTICO  → Inspeção e substituição urgente (prazo: 7 dias)
31-60 → ATENÇÃO  → Manutenção preventiva (prazo: 30 dias)
61-100 → NORMAL  → Monitoramento padrão (prazo: 90 dias)
```

### Composição do Índice (5 dimensões)

| Dimensão | Peso máx | Lógica de pontuação |
|---|---|---|
| Frequência de falhas | 30 pts | 0 falhas=30pts, 50+ falhas=0pts |
| MTTR (tempo de reparo) | 25 pts | 0 min=25pts, 240+ min=0pts |
| Eventos críticos | 20 pts | 0 críticos=20pts, 20+ críticos=0pts |
| MTBF (entre falhas) | 15 pts | 0h=0pts, 24h+=15pts |
| Impacto em clientes | 10 pts | 0 afetados=10pts, 50k+ afetados=0pts |

### Cálculo MTBF (Célula 3)

```python
# MTBF = intervalo médio (horas) entre FALHA_EQUIPAMENTO consecutivas
window_falhas = Window.partitionBy("alimentador_id").orderBy("ts_evento")

df_intervalos = (
    df_falhas
    .filter(col("tipo_evento") == "FALHA_EQUIPAMENTO")
    .withColumn("ts_falha_anterior", lag("ts_evento", 1).over(window_falhas))
    .withColumn(
        "intervalo_horas",
        (unix_timestamp("ts_evento") - unix_timestamp("ts_falha_anterior")) / 3600
    )
    .filter(col("ts_falha_anterior").isNotNull())
    .groupBy("alimentador_id")
    .agg(_round(avg("intervalo_horas"), 2).alias("mtbf_horas"))
)
```

### Score por dimensão (Célula 4)

```python
# 1. Frequência (30 pts): 0 falhas=30, decai linearmente até 50 falhas
score_frequencia = _round(greatest(0.0, lit(30.0) - (total_falhas / 50.0 * 30.0)), 1)

# 2. MTTR (25 pts): 0 min=25, decai até 240 min
score_mttr = _round(greatest(0.0, lit(25.0) - (mttr_min / 240.0 * 25.0)), 1)

# 3. Eventos críticos (20 pts): 0 críticos=20, decai até 20 críticos
score_criticos = _round(greatest(0.0, lit(20.0) - (eventos_criticos / 20.0 * 20.0)), 1)

# 4. MTBF (15 pts): 0h=0, sobe até 24h
score_mtbf = _round(least(15.0, mtbf_horas / 24.0 * 15.0), 1)

# 5. Impacto (10 pts): 0 afet=10, decai até 50k afetados
score_impacto = _round(greatest(0.0, lit(10.0) - (total_clientes_afetados / 50000.0 * 10.0)), 1)

# Índice composto
indice_saude = score_frequencia + score_mttr + score_criticos + score_mtbf + score_impacto
```

### Schema da tabela `adms.gold.saude_ativo_alimentador`

| Coluna | Tipo | Descrição |
|---|---|---|
| `alimentador_id`, `subestacao_id`, `regional`, `municipio` | String | Identidade |
| `se_capacidade_mva`, `se_idade_anos` | Double/Int | Dados da subestação |
| `total_falhas` | Long | Total de eventos de falha |
| `falhas_equipamento`, `religamentos` | Long | Por tipo de evento |
| `alarmes_sobrecarga`, `alarmes_temperatura` | Long | Por tipo de alarme |
| `total_clientes_afetados` | Long | Acumulado |
| `media_clientes_por_evento` | Double | Média por evento |
| `mttr_min` | Double | Mean Time To Repair (minutos) |
| `mtbf_horas` | Double | Mean Time Between Failures (horas) |
| `duracao_max_min` | Double | Pior interrupção |
| `score_criticidade_medio` | Double | Score médio dos eventos |
| `falhas_climaticas`, `falhas_tecnicas` | Long | Origem das falhas |
| `eventos_criticos`, `eventos_altos` | Long | Por severidade |
| `score_frequencia`, `score_mttr`, ... | Double | Scores individuais |
| **`indice_saude`** | Double | **Índice 0-100** |
| **`classificacao_saude`** | String | **CRITICO / ATENCAO / NORMAL** |
| `acao_recomendada` | String | Texto de recomendação operacional |
| `prazo_intervencao_dias` | Integer | 7 / 30 / 90 dias |

---

## Notebook 07: Gold — Perdas Comerciais e Técnicas

### O que calcula?

Estima a **receita não realizada** por dois vetores:
1. **Fraude** (perdas comerciais): consumo sub-reportado por medidores suspeitos
2. **ENF** (perdas técnicas por interrupção): Energia Não Fornecida durante apagões

### Célula 2: Perdas por fraude

```python
# Para medidores suspeitos, assumir que consumo real é 50% maior que reportado
# (baseado no fator de fraude do simulador: 20-60% de desvio, média ~40%)

df_fraude_alim = (
    df_leituras
    .withColumn("tarifa_rs_kwh", tarifa_por_tipo(col("tipo_medidor")))  # R$/kWh por tipo
    .groupBy("alimentador_id", ...)
    .agg(
        count("*").alias("total_leituras"),
        count(when(col("is_suspeita_fraude"), col("medidor_id"))).alias("leituras_suspeitas"),
        _round(_sum("delta_kwh_ajustado"), 2).alias("kwh_total_reportado"),
        # Estimativa: desvio = 50% do consumo reportado nas leituras suspeitas
        _round(_sum(when(col("is_suspeita_fraude"), delta_kwh_ajustado * 1.5).otherwise(0)), 2)
         .alias("kwh_estimado_desviado"),
        # Receita perdida: desvio × tarifa
        _round(_sum(when(col("is_suspeita_fraude"), delta_kwh_ajustado * 0.5 * tarifa).otherwise(0)), 2)
         .alias("receita_perdida_fraude_rs"),
    )
    .withColumn("pct_suspeitas", _round(leituras_suspeitas / total_leituras * 100, 1))
)
```

### Tarifas por tipo de medidor (referência ANEEL 2024)

```python
TARIFA_RESIDENCIAL_RS   = 0.75   # R$/kWh
TARIFA_COMERCIAL_RS     = 0.65
TARIFA_INDUSTRIAL_RS    = 0.45   # Mais barato (demanda contratada)
TARIFA_RURAL_RS         = 0.55
TARIFA_PODER_PUBLICO_RS = 0.60
TARIFA_MEDIA_RS         = 0.65   # Fallback
```

### Célula 3: ENF — Energia Não Fornecida

```python
# ENF = demanda média × duração da interrupção × clientes afetados
# Estimativa: 0.5 kW por cliente (perfil residencial misto)
DEMANDA_MEDIA_KW_POR_CLIENTE = 0.5

df_enf = (
    df_eventos
    .filter(tipo_evento.isin("INTERRUPCAO_INICIO", "FALHA_EQUIPAMENTO") &
            classe_aneel.isin("NAO_PLANEJADA", "CASO_FORTUITO", "EMERGENCIAL"))
    .groupBy("alimentador_id", ...)
    .agg(
        count("*").alias("num_interrupcoes"),
        _sum("clientes_afetados").alias("soma_clientes_afetados"),
        avg("duracao_interrupcao_min").alias("duracao_media_min"),
        _sum(
            col("clientes_afetados") *
            (col("duracao_interrupcao_min") / 60) *
            lit(DEMANDA_MEDIA_KW_POR_CLIENTE)
        ).alias("enf_kwh"),
    )
    .withColumn("enf_receita_perdida_rs", _round(enf_kwh * TARIFA_MEDIA_RS, 2))
)
```

### Célula 4: Consolidação

```python
df_perdas = (
    df_fraude_alim
    .join(df_enf, on="alimentador_id", how="left")
    .withColumn("total_perda_estimada_rs",
                receita_perdida_fraude_rs + enf_receita_perdida_rs)
    .withColumn(
        "nivel_risco_perda",
        when(total_perda >= 50000, "CRITICO")
        .when(total_perda >= 10000, "ALTO")
        .when(total_perda >= 1000,  "MEDIO")
        .otherwise("BAIXO")
    )
)
```

### Schema da tabela `adms.gold.perdas_comerciais_regional`

| Coluna | Tipo | Descrição |
|---|---|---|
| `alimentador_id`, `subestacao_id`, `regional`, `municipio` | String | Identidade |
| `total_leituras` | Long | Volume analisado |
| `leituras_suspeitas` | Long | Com suspeita de fraude |
| `pct_suspeitas` | Double | % do total |
| `kwh_total_reportado` | Double | kWh registrados no medidor |
| `kwh_estimado_desviado` | Double | kWh estimados desviados (fraude) |
| `receita_perdida_fraude_rs` | Double | R$ perdidos por fraude |
| `score_fraude_medio` | Double | Score médio de fraude |
| `num_interrupcoes` | Long | Interrupções não planejadas |
| `soma_clientes_afetados` | Long | Acumulado de clientes×ocorrência |
| `duracao_media_min` | Double | Duração média das interrupções |
| `enf_kwh` | Double | Energia Não Fornecida (kWh) |
| `enf_receita_perdida_rs` | Double | R$ perdidos por ENF |
| **`total_perda_estimada_rs`** | Double | **Total fraude + ENF (R$)** |
| **`nivel_risco_perda`** | String | **CRITICO/ALTO/MEDIO/BAIXO** |

---

## Consultas SQL analíticas (Gold)

### DEC/FEC
```sql
-- Alimentadores em violação DEC
SELECT alimentador_id, municipio, dec_horas, limite_dec_horas, pct_limite_dec
FROM adms.gold.kpi_dec_fec_mensal
WHERE violacao_dec = true
ORDER BY pct_limite_dec DESC;

-- DEC acumulado por subestação
SELECT subestacao_id, regional, SUM(dec_horas) as dec_acumulado
FROM adms.gold.kpi_dec_fec_mensal
GROUP BY subestacao_id, regional
ORDER BY dec_acumulado DESC;

-- Semáforo por regional
SELECT regional, tipo_area, semaforo_dec,
       COUNT(*) AS alimentadores,
       ROUND(AVG(dec_horas), 3) AS dec_medio_horas,
       SUM(CASE WHEN violacao_dec THEN 1 ELSE 0 END) AS violacoes_dec
FROM adms.gold.kpi_dec_fec_mensal
GROUP BY regional, tipo_area, semaforo_dec
ORDER BY dec_medio_horas DESC;
```

### Saúde de ativos
```sql
-- Alimentadores críticos
SELECT alimentador_id, regional, municipio,
       indice_saude, total_falhas, mttr_min, mtbf_horas,
       acao_recomendada, prazo_intervencao_dias
FROM adms.gold.saude_ativo_alimentador
WHERE classificacao_saude = 'CRITICO'
ORDER BY indice_saude;

-- Correlação: idade da SE vs saúde
SELECT
    CASE WHEN se_idade_anos <= 15 THEN '0-15 anos'
         WHEN se_idade_anos <= 30 THEN '16-30 anos'
         ELSE '31+ anos' END AS faixa_idade,
    ROUND(AVG(indice_saude), 1) AS saude_media,
    COUNT(*) AS alimentadores
FROM adms.gold.saude_ativo_alimentador
GROUP BY faixa_idade
ORDER BY faixa_idade;
```

### Perdas combinadas (view cruzada)
```sql
-- Perdas × Saúde por alimentador
SELECT p.regional, p.alimentador_id, p.municipio,
       p.nivel_risco_perda,
       ROUND(p.total_perda_estimada_rs, 0) AS perda_total_rs,
       ROUND(p.receita_perdida_fraude_rs, 0) AS perda_fraude_rs,
       ROUND(p.enf_receita_perdida_rs, 0) AS perda_enf_rs,
       p.pct_suspeitas AS pct_fraude,
       s.indice_saude,
       s.classificacao_saude
FROM adms.gold.perdas_comerciais_regional p
LEFT JOIN adms.gold.saude_ativo_alimentador s
    ON p.alimentador_id = s.alimentador_id
WHERE p.nivel_risco_perda IN ('CRITICO', 'ALTO')
ORDER BY p.total_perda_estimada_rs DESC
LIMIT 15;
```

---

## Dependências entre tabelas Gold

```
Silver: eventos_rede_enriquecido
    ├──→ Gold: kpi_dec_fec_mensal
    └──→ Gold: saude_ativo_alimentador

Silver: leituras_medidor_norm
    ├──→ Gold: kpi_dec_fec_mensal (para total_clientes_conjunto)
    └──→ Gold: perdas_comerciais_regional

Gold: perdas_comerciais_regional
    └──→ JOIN com: saude_ativo_alimentador (view analítica)
```

**Ordem de execução obrigatória**: Bronze → Silver Eventos → Silver Leituras → Gold 05 → Gold 06 → Gold 07

---

## Limitações e próximos passos

1. **Tarifas hardcoded**: deveria vir de uma tabela de referência que atualiza anualmente
2. **Limites DEC/FEC genéricos**: cada distribuidora tem limites específicos definidos pela ANEEL
3. **ENF simplificado**: demanda por cliente é estimada como constante; ideal seria usar a demanda real das leituras Silver
4. **Score de saúde sem janela temporal**: considera todo o histórico, não apenas os últimos N dias
5. **Ausência de time series**: para análise de tendência, precisaria de partição por período
