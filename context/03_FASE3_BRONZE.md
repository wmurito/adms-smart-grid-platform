# Fase 3 — Camada Bronze (Ingestão Kafka → Delta Lake)

## Objetivo

Consumir mensagens dos tópicos Kafka (Confluent Cloud) e persistir na camada Bronze do Databricks Delta Lake **sem transformações**, mantendo os dados brutos íntegros com metadados de ingestão.

**Princípio Bronze**: dado bruto + metadados de rastreabilidade. Nunca transformar ou filtrar nesta camada.

---

## Notebooks desta fase

| Notebook | Destino | Tópico Kafka |
|---|---|---|
| `01_bronze_eventos_rede.py` | `adms.bronze.eventos_rede_raw` | `adms.eventos.rede` |
| `02_bronze_leituras_medidor.py` | `adms.bronze.leituras_medidor_raw` | `adms.leituras.medidor` |

**Local:** `bronze/notebooks/`  
**Plataforma:** Databricks (executar como notebook)

---

## Notebook 01: Bronze — Eventos de Rede

### Configuração (Célula 1)
```python
CONFLUENT_BOOTSTRAP = "pkc-XXXXX.us-east-1.aws.confluent.cloud:9092"
CONFLUENT_API_KEY   = "<API_KEY>"
CONFLUENT_SECRET    = "<API_SECRET>"

TOPICO          = "adms.eventos.rede"
CATALOG         = "adms"
SCHEMA_BRONZE   = "bronze"
TABELA          = "eventos_rede_raw"
CHECKPOINT_PATH = f"/Volumes/{CATALOG}/{SCHEMA_BRONZE}/checkpoints/eventos_rede"
```

> ⚠️ Em produção usar Databricks Secrets: `dbutils.secrets.get(scope="adms", key="confluent-api-key")`

### Unity Catalog setup (Célula 2)
```python
spark.sql("CREATE CATALOG IF NOT EXISTS adms")
spark.sql("USE CATALOG adms")
spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS gold")
spark.sql("CREATE VOLUME IF NOT EXISTS adms.bronze.checkpoints")
```

### Schema Bronze para Eventos de Rede (Célula 3)

```python
from pyspark.sql.types import *

SCHEMA_EVENTO_REDE = StructType([
    StructField("evento_id",              StringType(),  True),
    StructField("tipo_evento",            StringType(),  True),
    StructField("severidade",             StringType(),  True),
    StructField("status",                 StringType(),  True),
    StructField("subestacao_id",          StringType(),  True),
    StructField("subestacao_nome",        StringType(),  True),
    StructField("alimentador_id",         StringType(),  True),
    StructField("alimentador_nome",       StringType(),  True),
    StructField("equipamento_id",         StringType(),  True),
    StructField("tipo_equipamento",       StringType(),  True),
    StructField("regional",               StringType(),  True),
    StructField("municipio",              StringType(),  True),
    StructField("tensao_kv",              DoubleType(),  True),
    StructField("corrente_a",             DoubleType(),  True),
    StructField("potencia_mw",            DoubleType(),  True),
    StructField("fator_potencia",         DoubleType(),  True),
    StructField("clientes_afetados",      IntegerType(), True),
    StructField("classe_interrupcao",     StringType(),  True),
    StructField("duracao_estimada_min",   IntegerType(), True),
    StructField("temperatura_c",          DoubleType(),  True),
    StructField("umidade_pct",            DoubleType(),  True),
    StructField("precipitacao_mm",        DoubleType(),  True),
    StructField("velocidade_vento_kmh",   DoubleType(),  True),
    StructField("timestamp_evento",       StringType(),  True),   # ISO-8601 string
    StructField("timestamp_ingestao",     StringType(),  True),
    StructField("fonte_sistema",          StringType(),  True),   # "ADMS-SCADA"
    StructField("versao_schema",          StringType(),  True),   # "1.0.0"
])
```

> ⚠️ Timestamps ficam como `StringType()` no Bronze — conversão para `TimestampType()` acontece no Silver.

### Configuração JAAS para Confluent Cloud (Célula 4)

```python
jaas_config = (
    "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required "
    f'username="{CONFLUENT_API_KEY}" '
    f'password="{CONFLUENT_SECRET}";'
)

kafka_options = {
    "kafka.bootstrap.servers":      CONFLUENT_BOOTSTRAP,
    "subscribe":                    TOPICO,
    "startingOffsets":              "earliest",      # Sempre do início
    "failOnDataLoss":               "false",         # Não falhar se offset perdido
    "kafka.security.protocol":      "SASL_SSL",
    "kafka.sasl.mechanism":         "PLAIN",
    "kafka.sasl.jaas.config":       jaas_config,
    "kafka.request.timeout.ms":     "60000",
    "kafka.session.timeout.ms":     "30000",
    "maxOffsetsPerTrigger":         "5000",          # Limitar micro-batch
}
```

> ⚠️ **Databricks usa `kafkashaded`** (não `org.apache.kafka`) no JAAS config. Esta é uma diferença importante que causa erros de autenticação se usar o prefixo errado.

### Teste de conectividade antes do streaming (Célula 5)
```python
# Leitura estática para validar antes de iniciar o stream
df_teste = (
    spark.read
    .format("kafka")
    .options(**kafka_options)
    .option("endingOffsets", "latest")
    .load()
)
count = df_teste.count()
print(f"Mensagens disponíveis: {count:,}")
```

### Pipeline Bronze com Structured Streaming (Célula 6)

```python
def processar_batch_bronze(df_batch, batch_id):
    if df_batch.isEmpty():
        return

    df_parsed = (
        df_batch
        .select(
            col("key").cast("string").alias("_kafka_key"),
            from_json(col("value").cast("string"), SCHEMA_EVENTO_REDE).alias("data"),
            col("partition").alias("_kafka_partition"),
            col("offset").alias("_kafka_offset"),
            col("timestamp").alias("_kafka_timestamp"),      # Timestamp do Kafka (quando chegou)
            col("topic").alias("_kafka_topic"),
        )
        .select(
            "data.*",            # Todos os campos do JSON parsed
            "_kafka_key",        # Chave de particionamento (subestacao_id)
            "_kafka_partition",  # Partição Kafka
            "_kafka_offset",     # Offset para rastreabilidade
            "_kafka_timestamp",  # Timestamp do Kafka
            "_kafka_topic",      # Nome do tópico
        )
        .withColumn("_ingest_timestamp", current_timestamp())  # Quando o Databricks processou
        .withColumn("_batch_id", lit(batch_id))
        .withColumn("_ano",  year(col("_kafka_timestamp")))    # Colunas de particionamento
        .withColumn("_mes",  month(col("_kafka_timestamp")))
        .withColumn("_dia",  dayofmonth(col("_kafka_timestamp")))
    )

    # Salvar no Delta Lake Bronze (sempre append)
    (
        df_parsed
        .write
        .format("delta")
        .mode("append")
        .partitionBy("_ano", "_mes", "_dia", "subestacao_id")
        .saveAsTable("adms.bronze.eventos_rede_raw")
    )

# Iniciar streaming
query_eventos = (
    spark.readStream
    .format("kafka")
    .options(**kafka_options)
    .load()
    .writeStream
    .foreachBatch(processar_batch_bronze)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(availableNow=True)   # Processar tudo disponível e parar
    .start()
)
```

### trigger(availableNow=True) vs trigger(processingTime="30 seconds")
- `availableNow=True` → processa todo o backlog e para (ideal para Databricks Free Edition com DBUs limitados)
- `processingTime="30 seconds"` → micro-batch contínuo a cada 30s (produção)

---

## Notebook 02: Bronze — Leituras de Medidores

### Schema Bronze para Leituras AMI
```python
SCHEMA_LEITURA_MEDIDOR = StructType([
    StructField("leitura_id",              StringType(),  True),
    StructField("medidor_id",              StringType(),  True),
    StructField("cliente_id",              StringType(),  True),
    StructField("tipo_medidor",            StringType(),  True),
    StructField("alimentador_id",          StringType(),  True),
    StructField("subestacao_id",           StringType(),  True),
    StructField("regional",                StringType(),  True),
    StructField("municipio",               StringType(),  True),
    StructField("latitude",                DoubleType(),  True),
    StructField("longitude",               DoubleType(),  True),
    StructField("kwh_ativo",               DoubleType(),  True),
    StructField("kwh_reativo",             DoubleType(),  True),
    StructField("kw_demanda",              DoubleType(),  True),
    StructField("tensao_v",                DoubleType(),  True),
    StructField("corrente_a",              DoubleType(),  True),
    StructField("fator_potencia",          DoubleType(),  True),
    StructField("num_interrupcoes",        IntegerType(), True),
    StructField("duracao_interrupcao_min", IntegerType(), True),
    StructField("tensao_minima_v",         DoubleType(),  True),
    StructField("tensao_maxima_v",         DoubleType(),  True),
    StructField("is_leitura_estimada",     BooleanType(), True),
    StructField("is_anomalia_consumo",     BooleanType(), True),
    StructField("is_tensao_critica",       BooleanType(), True),
    StructField("is_possivel_fraude",      BooleanType(), True),
    StructField("timestamp_leitura",       StringType(),  True),
    StructField("timestamp_ingestao",      StringType(),  True),
    StructField("fonte_sistema",           StringType(),  True),   # "AMI-MEDIDOR"
    StructField("versao_schema",           StringType(),  True),
])
```

### Particionamento do Bronze Leituras
```python
.partitionBy("_ano", "_mes", "_dia", "subestacao_id")
# maxOffsetsPerTrigger: "10000"  (dobro do eventos, pois mais volume)
```

---

## Estrutura das tabelas Bronze

### Colunas adicionadas pelo pipeline (metadados `_`)
| Coluna | Tipo | Origem |
|---|---|---|
| `_kafka_key` | String | Chave Kafka original (ex: "SE01") |
| `_kafka_partition` | Long | Número da partição Kafka |
| `_kafka_offset` | Long | Offset da mensagem (rastreabilidade) |
| `_kafka_timestamp` | Timestamp | Quando chegou no Kafka broker |
| `_kafka_topic` | String | Nome do tópico |
| `_ingest_timestamp` | Timestamp | Quando o Databricks processou |
| `_batch_id` | Long | ID do micro-batch Spark |
| `_ano`, `_mes`, `_dia` | Integer | Derivados de `_kafka_timestamp` (particionamento) |

---

## Verificação e validação Bronze (Célula 7-8)

### Verificar tabela
```sql
SELECT * FROM adms.bronze.eventos_rede_raw LIMIT 10;
SELECT * FROM adms.bronze.leituras_medidor_raw LIMIT 10;
```

### Análise de qualidade Bronze
```python
# Distribuição por tipo de evento
df_bronze.groupBy("tipo_evento").count().orderBy("count", ascending=False).show()

# Nulls em campos críticos
campos_criticos = ["evento_id", "tipo_evento", "severidade", "subestacao_id",
                   "alimentador_id", "timestamp_evento"]
for campo in campos_criticos:
    nulls = df_bronze.filter(col(campo).isNull()).count()
    print(f"{campo}: {nulls} nulls ({nulls/total*100:.1f}%)")
```

### Consultas SQL úteis (Leituras)
```sql
-- Ver últimas leituras
SELECT medidor_id, tipo_medidor, alimentador_id, kwh_ativo, tensao_v, is_possivel_fraude
FROM adms.bronze.leituras_medidor_raw
ORDER BY _ingest_timestamp DESC
LIMIT 20;

-- Medidores com possível fraude
SELECT medidor_id, cliente_id, alimentador_id, municipio, kwh_ativo
FROM adms.bronze.leituras_medidor_raw
WHERE is_possivel_fraude = true
ORDER BY _ingest_timestamp DESC;

-- Tensão crítica por regional
SELECT regional,
       COUNT(*) as total,
       SUM(CASE WHEN is_tensao_critica THEN 1 ELSE 0 END) as criticos
FROM adms.bronze.leituras_medidor_raw
GROUP BY regional;
```

---

## Pontos de atenção

### Deduplicação
- Bronze **não deduplica** — pode ter duplicatas (reprocessamento, retry do producer)
- A deduplicação acontece no Silver usando `row_number()` over `evento_id`/`leitura_id`

### Checkpoint
- O checkpoint em `/Volumes/adms/bronze/checkpoints/` garante que cada mensagem é processada exatamente uma vez
- Se precisar reprocessar tudo: deletar o checkpoint e reexecutar com `startingOffsets: "earliest"`

### Credenciais nos notebooks
- Os notebooks contêm credenciais hardcoded por limitação do Databricks Free Edition
- Em Databricks pago: usar `dbutils.secrets.get(scope="adms", key="confluent-api-key")`

### `from_json` com schema incorreto
- Se o `SCHEMA_EVENTO_REDE` não corresponder ao JSON real → campos ficam `null`
- Verificar: `df_parsed.filter(col("evento_id").isNull()).count()` — deve ser 0
