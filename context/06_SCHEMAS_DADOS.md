# Schemas de Dados — Referência Completa

## Visão geral

Este arquivo documenta todos os schemas de dados do projeto ADMS Smart Grid Platform, desde o contrato Kafka até o schema físico das tabelas Delta Lake.

---

## Tópicos Kafka

| Tópico | Partições | Chave | Retenção |
|---|---|---|---|
| `adms.eventos.rede` | 6 | `subestacao_id` | 7 dias |
| `adms.leituras.medidor` | 12 | `alimentador_id` | 7 dias |
| `adms.alarmes` | 3 | `subestacao_id` | 7 dias |

---

## Schema: EventoRede (Kafka JSON)

**Tópico**: `adms.eventos.rede`  
**Arquivo de definição**: `ingestion/kafka/producers/schemas.py`

```json
{
  "evento_id": "EVT-A1B2C3D4E5F6",
  "tipo_evento": "FALHA_EQUIPAMENTO",
  "severidade": "CRITICO",
  "status": "ATIVO",
  "subestacao_id": "SE01",
  "subestacao_nome": "SE Cataguases",
  "alimentador_id": "SE01-AL01",
  "alimentador_nome": "Alimentador Cataguases 01",
  "equipamento_id": "SE01-AL01-CF001",
  "tipo_equipamento": "CHAVE_FACA",
  "regional": "Zona da Mata",
  "municipio": "Cataguases",
  "tensao_kv": 13.1,
  "corrente_a": 245.3,
  "potencia_mw": 5.2,
  "fator_potencia": 0.92,
  "clientes_afetados": 1450,
  "classe_interrupcao": "NAO_PLANEJADA",
  "duracao_estimada_min": 45,
  "temperatura_c": 28.5,
  "umidade_pct": 78.0,
  "precipitacao_mm": 12.3,
  "velocidade_vento_kmh": 35.0,
  "timestamp_evento": "2026-04-19T20:38:20+00:00",
  "timestamp_ingestao": "2026-04-19T20:38:20+00:00",
  "fonte_sistema": "ADMS-SCADA",
  "versao_schema": "1.0.0"
}
```

### Domínios dos campos

```
tipo_evento     : FALHA_EQUIPAMENTO | RELIGAMENTO_AUTOMATICO | MANOBRA_PLANEJADA |
                  MANOBRA_EMERGENCIAL | ANOMALIA_TENSAO | ANOMALIA_CORRENTE |
                  INTERRUPCAO_INICIO | INTERRUPCAO_FIM | ALARME_SOBRECARGA |
                  ALARME_TEMPERATURA

severidade      : CRITICO | ALTO | MEDIO | BAIXO

status          : ATIVO | RESOLVIDO | EM_ANALISE | IGNORADO

classe_interrupcao : NAO_PLANEJADA | PLANEJADA | EMERGENCIAL | TERCEIROS | CASO_FORTUITO

subestacao_id   : SE01..SE20
alimentador_id  : SE{nn}-AL{01..06}
equipamento_id  : SE{nn}-AL{mm}-{CF|RL|TR}{xxx}

tipo_equipamento: CHAVE_FACA | RELIGADOR | TRANSFORMADOR

regional        : "Zona da Mata" | "Campo das Vert." | "Sul de Minas"
```

---

## Schema: LeituraMedidor (Kafka JSON)

**Tópico**: `adms.leituras.medidor`

```json
{
  "leitura_id": "LTR-A1B2C3D4E5F6",
  "medidor_id": "MED-SE01-000001",
  "cliente_id": "CLI-00000001",
  "tipo_medidor": "RESIDENCIAL",
  "alimentador_id": "SE01-AL01",
  "subestacao_id": "SE01",
  "regional": "Zona da Mata",
  "municipio": "Cataguases",
  "latitude": -21.389,
  "longitude": -42.696,
  "kwh_ativo": 0.0523,
  "kwh_reativo": 0.0089,
  "kw_demanda": 2.15,
  "tensao_v": 218.5,
  "corrente_a": 9.82,
  "fator_potencia": 0.923,
  "num_interrupcoes": 0,
  "duracao_interrupcao_min": 0,
  "tensao_minima_v": 215.2,
  "tensao_maxima_v": 221.1,
  "is_leitura_estimada": false,
  "is_anomalia_consumo": false,
  "is_tensao_critica": false,
  "is_possivel_fraude": false,
  "timestamp_leitura": "2026-04-19T20:38:20+00:00",
  "timestamp_ingestao": "2026-04-19T20:38:20+00:00",
  "fonte_sistema": "AMI-MEDIDOR",
  "versao_schema": "1.0.0"
}
```

### Domínios dos campos

```
tipo_medidor : RESIDENCIAL | COMERCIAL | INDUSTRIAL | RURAL | PODER_PUBLICO

medidor_id   : MED-{SE_ID}-{000001..999999}
cliente_id   : CLI-{00000001..99999999}

kwh_ativo    : consumo no intervalo de 15 min (não acumulado total)
               Residencial: ~0.02-0.08 kWh por leitura
               Comercial:   ~0.1-0.5 kWh
               Industrial:  ~1-5 kWh

tensao_v     : medição de tensão (V)
               Normal: 220V ±5% (209V-231V)
               Industrial: 380V
               Rural: 127V
```

---

## Schema: Alarme (Kafka JSON)

**Tópico**: `adms.alarmes`

```json
{
  "alarme_id": "ALM-A1B2C3D4",
  "tipo_alarme": "ALARME_FALHA_EQUIPAMENTO",
  "severidade": "CRITICO",
  "descricao": "Alarme gerado a partir de evento: FALHA_EQUIPAMENTO",
  "subestacao_id": "SE01",
  "alimentador_id": "SE01-AL01",
  "equipamento_id": "SE01-AL01-CF001",
  "valor_medido": 245.3,
  "valor_limite": 0.0,
  "unidade": "A",
  "acao_recomendada": "Despachar equipe de manutencao",
  "requer_desligamento": true,
  "timestamp_alarme": "2026-04-19T20:38:20+00:00",
  "timestamp_ingestao": "2026-04-19T20:38:20+00:00",
  "evento_id_origem": "EVT-A1B2C3D4E5F6",
  "fonte_sistema": "ADMS-PROTECAO",
  "versao_schema": "1.0.0"
}
```

---

## Tabelas Delta Lake — Bronze

### `adms.bronze.eventos_rede_raw`
Todos os campos de `EventoRede` + metadados Kafka:
```
_kafka_key       STRING       -- subestacao_id como string
_kafka_partition LONG
_kafka_offset    LONG
_kafka_timestamp TIMESTAMP
_kafka_topic     STRING
_ingest_timestamp TIMESTAMP
_batch_id        LONG
_ano             INTEGER      -- Partição
_mes             INTEGER      -- Partição
_dia             INTEGER      -- Partição
subestacao_id    STRING       -- Partição
```

### `adms.bronze.leituras_medidor_raw`
Todos os campos de `LeituraMedidor` + mesmos metadados Kafka acima.

---

## Tabelas Delta Lake — Silver

### `adms.silver.eventos_rede_enriquecido`
Tudo do Bronze + campos calculados:
```
ts_evento              TIMESTAMP    -- timestamp_evento como Timestamp (não String)
ts_ingestao            TIMESTAMP
se_capacidade_mva      DOUBLE       -- da topologia (join SE)
se_tensao_kv           INTEGER
se_idade_anos          INTEGER
se_latitude            DOUBLE
se_longitude           DOUBLE
duracao_interrupcao_min DOUBLE      -- duração real (pareamento INICIO/FIM) ou estimada
is_evento_climatico    BOOLEAN
classe_aneel           STRING       -- PLANEJADA|NAO_PLANEJADA|EMERGENCIAL|CASO_FORTUITO|INFORMATIVO
score_criticidade      DOUBLE       -- 0-100
faixa_criticidade      STRING       -- CRITICO|ALTO|MEDIO|BAIXO
hora_evento            INTEGER
dia_semana             INTEGER
periodo_dia            STRING       -- MADRUGADA|MANHA|TARDE|NOITE
silver_processado_em   TIMESTAMP
versao_silver          STRING
```

### `adms.silver.leituras_medidor_norm`
Tudo do Bronze + campos calculados:
```
ts_leitura             TIMESTAMP
kwh_ativo_anterior     DOUBLE       -- REMOVIDO antes de salvar
ts_leitura_anterior    TIMESTAMP    -- REMOVIDO antes de salvar
intervalo_real_min     DOUBLE
delta_kwh              DOUBLE       -- consumo real no intervalo
delta_kwh_ajustado     DOUBLE       -- delta_kwh sem negativos
is_delta_negativo      BOOLEAN
tensao_nominal_v       DOUBLE       -- 220|380|127 por tipo
classe_tensao_aneel    STRING       -- ADEQUADA|PRECARIA|CRITICA
desvio_tensao_pct      DOUBLE
media_kwh_alim         DOUBLE       -- REMOVIDO antes de salvar
stddev_kwh_alim        DOUBLE       -- REMOVIDO antes de salvar
z_score_consumo        DOUBLE       -- REMOVIDO antes de salvar
score_anomalia         DOUBLE       -- 0-100
faixa_anomalia         STRING       -- ALTA|MEDIA|BAIXA|NORMAL
score_fraude           INTEGER      -- 0-100 multi-sinal
is_suspeita_fraude     BOOLEAN
nivel_suspeita         STRING       -- ALTO|MEDIO|BAIXO|NORMAL
hora_leitura           INTEGER
dia_semana             INTEGER
mes_leitura            INTEGER
periodo_dia            STRING
classe_fp              STRING       -- EXCELENTE|BOM|REGULAR|RUIM
silver_processado_em   TIMESTAMP
versao_silver          STRING
```

---

## Tabelas Delta Lake — Gold

### `adms.gold.kpi_dec_fec_mensal`
```
alimentador_id           STRING
subestacao_id            STRING
regional                 STRING
municipio                STRING
ano                      INTEGER     -- Partição
mes                      INTEGER     -- Partição
tipo_area                STRING      -- URBANA|SUBURBANA|RURAL
num_interrupcoes         LONG
duracao_media_horas      DOUBLE
duracao_max_horas        DOUBLE
interrupcoes_climaticas  LONG
score_criticidade_medio  DOUBLE
total_clientes_conjunto  LONG
dec_horas                DOUBLE      -- KPI regulatório
dec_minutos              DOUBLE
fec_vezes                DOUBLE      -- KPI regulatório
limite_dec_horas         DOUBLE
limite_fec_vezes         DOUBLE
pct_limite_dec           DOUBLE
pct_limite_fec           DOUBLE
violacao_dec             BOOLEAN
violacao_fec             BOOLEAN
violacao_alguma          BOOLEAN
semaforo_dec             STRING      -- VERMELHO|AMARELO|VERDE
semaforo_fec             STRING
versao_gold              STRING
```

### `adms.gold.saude_ativo_alimentador`
```
alimentador_id           STRING
subestacao_id            STRING
regional                 STRING      -- Partição
municipio                STRING
se_capacidade_mva        DOUBLE
se_idade_anos            INTEGER
total_falhas             LONG
falhas_equipamento       LONG
religamentos             LONG
alarmes_sobrecarga       LONG
alarmes_temperatura      LONG
total_clientes_afetados  LONG
media_clientes_por_evento DOUBLE
mttr_min                 DOUBLE
duracao_max_min          DOUBLE
mtbf_horas               DOUBLE
score_criticidade_medio  DOUBLE
falhas_climaticas        LONG
falhas_tecnicas          LONG
eventos_criticos         LONG
eventos_altos            LONG
score_frequencia         DOUBLE
score_mttr               DOUBLE
score_criticos           DOUBLE
score_mtbf               DOUBLE
score_impacto            DOUBLE
indice_saude             DOUBLE      -- 0-100
classificacao_saude      STRING      -- CRITICO|ATENCAO|NORMAL  -- Partição
acao_recomendada         STRING
prazo_intervencao_dias   INTEGER
versao_gold              STRING
```

### `adms.gold.perdas_comerciais_regional`
```
alimentador_id             STRING
subestacao_id              STRING
regional                   STRING    -- Partição
municipio                  STRING
total_leituras             LONG
leituras_suspeitas         LONG
pct_suspeitas              DOUBLE
kwh_total_reportado        DOUBLE
kwh_estimado_desviado      DOUBLE
receita_perdida_fraude_rs  DOUBLE
score_fraude_medio         DOUBLE
num_interrupcoes           LONG
soma_clientes_afetados     LONG
duracao_media_min          DOUBLE
enf_kwh                    DOUBLE
enf_receita_perdida_rs     DOUBLE
total_perda_estimada_rs    DOUBLE
nivel_risco_perda          STRING    -- CRITICO|ALTO|MEDIO|BAIXO  -- Partição
versao_gold                STRING
```

---

## Mapeamento de IDs

```
Subestação : SE01..SE20                         → 20 registros
Alimentador: SE{nn}-AL{01..06}                  → 120 registros
Equipamento: SE{nn}-AL{mm}-CF{xxx}              → Chave de faca
             SE{nn}-AL{mm}-RL{xxx}              → Religador
             SE{nn}-AL{mm}-TR{xxx}              → Transformador
Medidor    : MED-{SEID}-{000001..N}             → 10.000 registros
Cliente    : CLI-{00000000..N}                  → 1 cliente por medidor
Evento     : EVT-{12 hex uppercase}             → UUID parcial
Leitura    : LTR-{12 hex uppercase}
Alarme     : ALM-{8 hex uppercase}
```

---

## Convenções de nomenclatura

| Padrão | Onde | Exemplo |
|---|---|---|
| `_campo` | Metadados técnicos (não-negócio) | `_kafka_offset`, `_ingest_timestamp` |
| `is_campo` | Flags booleanas | `is_tensao_critica`, `is_suspeita_fraude` |
| `ts_campo` | Timestamps tipados (Silver+) | `ts_evento`, `ts_leitura` |
| `score_campo` | Scores numéricos | `score_criticidade`, `score_fraude` |
| `faixa_campo` | Categorização de scores | `faixa_criticidade`, `faixa_anomalia` |
| `nivel_campo` | Níveis categóricos | `nivel_suspeita`, `nivel_risco_perda` |
| `classe_campo` | Classificações | `classe_aneel`, `classe_tensao_aneel` |
| `indice_campo` | Índices compostos | `indice_saude` |
| `se_campo` | Dados da subestação (join) | `se_capacidade_mva`, `se_idade_anos` |
