# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze — Eventos de Rede (adms.eventos.rede)
# MAGIC
# MAGIC Consome eventos do tópico Kafka no Confluent Cloud e persiste na camada Bronze
# MAGIC do Delta Lake com schema completo e metadados de ingestão.
# MAGIC
# MAGIC **Tópico fonte:** `adms.eventos.rede`
# MAGIC **Destino:** `adms.bronze.eventos_rede_raw`
# MAGIC **Trigger:** 30 segundos (micro-batch)

# COMMAND ----------

# MAGIC %md ## Célula 1 — Configuração

# COMMAND ----------

# Credenciais do Confluent Cloud
# Em produção: usar Databricks Secrets. No Free Edition: variáveis diretas.
CONFLUENT_BOOTSTRAP = "pkc-oxqxx9.us-east-1.aws.confluent.cloud:9092"
CONFLUENT_API_KEY   = "5GDS2QXL5XJZCZCU"
CONFLUENT_SECRET    = "cfltvJELpk5YL3vNQ3Q7VECZrp/uoU3J6DH3xaYIS0+keskv24VeF5cEiFYmzylQ"

TOPICO              = "adms.eventos.rede"
CATALOG             = "adms"
SCHEMA_BRONZE       = "bronze"
TABELA              = "eventos_rede_raw"
CHECKPOINT_PATH     = f"/Volumes/{CATALOG}/{SCHEMA_BRONZE}/checkpoints/eventos_rede"

print(f"Bootstrap : {CONFLUENT_BOOTSTRAP}")
print(f"Tópico    : {TOPICO}")
print(f"Destino   : {CATALOG}.{SCHEMA_BRONZE}.{TABELA}")

# COMMAND ----------

# MAGIC %md ## Célula 2 — Criar catálogo e schema no Unity Catalog

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_BRONZE}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS silver")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS gold")

# Criar volume para checkpoints
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA_BRONZE}.checkpoints")

print("Unity Catalog configurado:")
spark.sql(f"SHOW SCHEMAS IN {CATALOG}").show()

# COMMAND ----------

# MAGIC %md ## Célula 3 — Schema do evento (espelha o schemas.py local)

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, BooleanType, LongType
)

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
    StructField("timestamp_evento",       StringType(),  True),
    StructField("timestamp_ingestao",     StringType(),  True),
    StructField("fonte_sistema",          StringType(),  True),
    StructField("versao_schema",          StringType(),  True),
])

print(f"Schema definido com {len(SCHEMA_EVENTO_REDE.fields)} campos")

# COMMAND ----------

# MAGIC %md ## Célula 4 — Configuração JAAS para Confluent Cloud

# COMMAND ----------

jaas_config = (
    "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required "
    f'username="{CONFLUENT_API_KEY}" '
    f'password="{CONFLUENT_SECRET}";'
)
kafka_options = {
    "kafka.bootstrap.servers":      CONFLUENT_BOOTSTRAP,
    "subscribe":                    TOPICO,
    "startingOffsets":              "earliest",
    "failOnDataLoss":               "false",
    "kafka.security.protocol":      "SASL_SSL",
    "kafka.sasl.mechanism":         "PLAIN",
    "kafka.sasl.jaas.config":       jaas_config,
    "kafka.request.timeout.ms":     "60000",
    "kafka.session.timeout.ms":     "30000",
    "maxOffsetsPerTrigger":         "5000",
}

print("Kafka options configuradas para Confluent Cloud SASL_SSL")

# COMMAND ----------

# MAGIC %md ## Célula 5 — Testar leitura do Kafka (batch estático)

# COMMAND ----------

# Leitura estática para validar conectividade antes do streaming
df_teste = (
    spark.read
    .format("kafka")
    .options(**kafka_options)
    .option("endingOffsets", "latest")
    .load()
)

count = df_teste.count()
print(f"Mensagens disponíveis no tópico: {count:,}")

if count > 0:
    # Mostrar raw bytes e parsear sample
    from pyspark.sql.functions import col, from_json, cast

    df_sample = (
        df_teste
        .select(
            col("key").cast("string").alias("kafka_key"),
            col("value").cast("string").alias("json_raw"),
            col("partition"),
            col("offset"),
            col("timestamp").alias("kafka_timestamp"),
        )
        .limit(3)
    )
    display(df_sample)

# COMMAND ----------

# MAGIC %md ## Célula 6 — Pipeline Bronze com Structured Streaming

# COMMAND ----------

from pyspark.sql.functions import (
    col, from_json, current_timestamp,
    cast, lit, year, month, dayofmonth
)
from datetime import datetime

def processar_batch_bronze(df_batch, batch_id):
    """
    Processa cada micro-batch do Kafka e persiste no Delta Lake Bronze.
    Bronze = dado bruto com metadados de ingestão, sem transformações.
    """
    if df_batch.isEmpty():
        return

    # Parse JSON do value
    df_parsed = (
        df_batch
        .select(
            col("key").cast("string").alias("_kafka_key"),
            from_json(col("value").cast("string"), SCHEMA_EVENTO_REDE).alias("data"),
            col("partition").alias("_kafka_partition"),
            col("offset").alias("_kafka_offset"),
            col("timestamp").alias("_kafka_timestamp"),
            col("topic").alias("_kafka_topic"),
        )
        .select(
            "data.*",
            "_kafka_key",
            "_kafka_partition",
            "_kafka_offset",
            "_kafka_timestamp",
            "_kafka_topic",
        )
        .withColumn("_ingest_timestamp", current_timestamp())
        .withColumn("_batch_id", lit(batch_id))
        .withColumn("_ano",  year(col("_kafka_timestamp")))
        .withColumn("_mes",  month(col("_kafka_timestamp")))
        .withColumn("_dia",  dayofmonth(col("_kafka_timestamp")))
    )

    count = df_parsed.count()

    # Salvar no Delta Lake Bronze (append)
    (
        df_parsed
        .write
        .format("delta")
        .mode("append")
        .partitionBy("_ano", "_mes", "_dia", "subestacao_id")
        .saveAsTable(f"{CATALOG}.{SCHEMA_BRONZE}.{TABELA}")
    )

    print(f"[Batch {batch_id}] {count} eventos salvos → {CATALOG}.{SCHEMA_BRONZE}.{TABELA}")


# Streaming query
query_eventos = (
    spark.readStream
    .format("kafka")
    .options(**kafka_options)
    .load()
    .writeStream
    .foreachBatch(processar_batch_bronze)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(availableNow=True)
    .start()
)

print(f"Streaming iniciado — ID: {query_eventos.id}")
print(f"Status: {query_eventos.status}")

# COMMAND ----------

# MAGIC %md ## Célula 7 — Monitorar e validar dados Bronze

# COMMAND ----------

import time

# Aguardar 2 batches para ter dados
print("Aguardando dados chegarem (60s)...")
time.sleep(65)

# Validar tabela Bronze
df_bronze = spark.table(f"{CATALOG}.{SCHEMA_BRONZE}.{TABELA}")

total = df_bronze.count()
print(f"\n{'='*50}")
print(f"Tabela: {CATALOG}.{SCHEMA_BRONZE}.{TABELA}")
print(f"Total de registros: {total:,}")
print(f"{'='*50}")

if total > 0:
    print("\nDistribuição por tipo de evento:")
    df_bronze.groupBy("tipo_evento").count().orderBy("count", ascending=False).show()

    print("Distribuição por severidade:")
    df_bronze.groupBy("severidade").count().orderBy("count", ascending=False).show()

    print("Distribuição por regional:")
    df_bronze.groupBy("regional").count().orderBy("count", ascending=False).show()

    print("Schema da tabela Bronze:")
    df_bronze.printSchema()

# COMMAND ----------

# MAGIC %md ## Célula 8 — Estatísticas de qualidade Bronze

# COMMAND ----------

from pyspark.sql.functions import (
    count, sum as _sum, avg, min as _min, max as _max,
    countDistinct, when, col
)

if total > 0:
    # Verificar nulls em campos críticos
    campos_criticos = [
        "evento_id", "tipo_evento", "severidade",
        "subestacao_id", "alimentador_id", "timestamp_evento"
    ]

    print("Qualidade dos dados Bronze:")
    print(f"{'Campo':<30} {'Nulls':>8} {'%':>8}")
    print("-" * 48)
    for campo in campos_criticos:
        nulls = df_bronze.filter(col(campo).isNull()).count()
        pct = nulls / total * 100 if total > 0 else 0
        status = "✓" if nulls == 0 else "⚠"
        print(f"{status} {campo:<28} {nulls:>8} {pct:>7.1f}%")

    print(f"\nTotal clientes afetados: {df_bronze.agg(_sum('clientes_afetados')).collect()[0][0]:,.0f}")
    print(f"Subestações distintas  : {df_bronze.select(countDistinct('subestacao_id')).collect()[0][0]}")
    print(f"Alimentadores distintos: {df_bronze.select(countDistinct('alimentador_id')).collect()[0][0]}")

# COMMAND ----------

# MAGIC %md ## Célula 9 — Parar o streaming (executar quando quiser encerrar)

# COMMAND ----------

# Descomentar para parar
# query_eventos.stop()
# print("Streaming encerrado")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Próximos passos
# MAGIC - Executar `02_bronze_leituras_medidor` para ingerir as leituras AMI
# MAGIC - Executar `03_silver_eventos_rede` para aplicar transformações e enriquecimento
# MAGIC - Verificar tabela com: `SELECT * FROM adms.bronze.eventos_rede_raw LIMIT 10`
