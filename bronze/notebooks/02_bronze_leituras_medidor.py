# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze — Leituras de Medidores AMI (adms.leituras.medidor)
# MAGIC
# MAGIC Consome leituras AMI do Confluent Cloud e persiste na camada Bronze.
# MAGIC
# MAGIC **Tópico fonte:** `adms.leituras.medidor`
# MAGIC **Destino:** `adms.bronze.leituras_medidor_raw`
# MAGIC **Trigger:** 30 segundos

# COMMAND ----------

# MAGIC %md ## Célula 1 — Configuração

# COMMAND ----------

CONFLUENT_BOOTSTRAP = "pkc-oxqxx9.us-east-1.aws.confluent.cloud:9092"
CONFLUENT_API_KEY   = "5GDS2QXL5XJZCZCU"
CONFLUENT_SECRET    = "cfltvJELpk5YL3vNQ3Q7VECZrp/uoU3J6DH3xaYIS0+keskv24VeF5cEiFYmzylQ"

TOPICO              = "adms.leituras.medidor"
CATALOG             = "adms"
SCHEMA_BRONZE       = "bronze"
TABELA              = "leituras_medidor_raw"
CHECKPOINT_PATH     = f"/Volumes/{CATALOG}/{SCHEMA_BRONZE}/checkpoints/leituras_medidor"

print(f"Bootstrap : {CONFLUENT_BOOTSTRAP}")
print(f"Tópico    : {TOPICO}")
print(f"Destino   : {CATALOG}.{SCHEMA_BRONZE}.{TABELA}")

# COMMAND ----------

# MAGIC %md ## Célula 2 — Schema da leitura AMI

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, BooleanType
)

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
    StructField("fonte_sistema",           StringType(),  True),
    StructField("versao_schema",           StringType(),  True),
])

print(f"Schema definido com {len(SCHEMA_LEITURA_MEDIDOR.fields)} campos")

# COMMAND ----------

# MAGIC %md ## Célula 3 — Kafka options Confluent Cloud

# COMMAND ----------

jaas_config = (
    "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required "
    f'username="{CONFLUENT_API_KEY}" '
    f'password="{CONFLUENT_SECRET}";'
)

kafka_options = {
    "kafka.bootstrap.servers":  CONFLUENT_BOOTSTRAP,
    "subscribe":                TOPICO,
    "startingOffsets":          "earliest",
    "failOnDataLoss":           "false",
    "kafka.security.protocol":  "SASL_SSL",
    "kafka.sasl.mechanism":     "PLAIN",
    "kafka.sasl.jaas.config":   jaas_config,
    "kafka.request.timeout.ms": "60000",
    "kafka.session.timeout.ms": "30000",
    "maxOffsetsPerTrigger":     "10000",
}

# Verificar mensagens disponíveis
df_teste = (
    spark.read.format("kafka")
    .options(**kafka_options)
    .option("endingOffsets", "latest")
    .load()
)
print(f"Mensagens disponíveis no tópico: {df_teste.count():,}")

# COMMAND ----------

# MAGIC %md ## Célula 4 — Pipeline Bronze Streaming

# COMMAND ----------

from pyspark.sql.functions import (
    col, from_json, current_timestamp,
    lit, year, month, dayofmonth
)

def processar_batch_medidores(df_batch, batch_id):
    if df_batch.isEmpty():
        return

    df_parsed = (
        df_batch
        .select(
            col("key").cast("string").alias("_kafka_key"),
            from_json(col("value").cast("string"), SCHEMA_LEITURA_MEDIDOR).alias("data"),
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

    (
        df_parsed
        .write
        .format("delta")
        .mode("append")
        .partitionBy("_ano", "_mes", "_dia", "subestacao_id")
        .saveAsTable(f"{CATALOG}.{SCHEMA_BRONZE}.{TABELA}")
    )

    print(f"[Batch {batch_id}] {count:,} leituras salvas → {CATALOG}.{SCHEMA_BRONZE}.{TABELA}")


query_medidores = (
    spark.readStream
    .format("kafka")
    .options(**kafka_options)
    .load()
    .writeStream
    .foreachBatch(processar_batch_medidores)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(availableNow=True)
    .start()
)

print(f"Streaming iniciado — ID: {query_medidores.id}")

# COMMAND ----------

# MAGIC %md ## Célula 5 — Validar dados Bronze

# COMMAND ----------

import time
print("Aguardando dados (60s)...")
time.sleep(65)

df_bronze = spark.table(f"{CATALOG}.{SCHEMA_BRONZE}.{TABELA}")
total = df_bronze.count()

print(f"\nTotal registros: {total:,}")

if total > 0:
    from pyspark.sql.functions import sum as _sum, avg, count, when

    print("\nDistribuição por tipo de medidor:")
    df_bronze.groupBy("tipo_medidor").count().orderBy("count", ascending=False).show()

    print("Distribuição por regional:")
    df_bronze.groupBy("regional").count().orderBy("count", ascending=False).show()

    # Métricas de qualidade e negócio
    stats = df_bronze.agg(
        count("*").alias("total"),
        _sum(when(col("is_anomalia_consumo"), 1).otherwise(0)).alias("anomalias"),
        _sum(when(col("is_tensao_critica"), 1).otherwise(0)).alias("tensao_critica"),
        _sum(when(col("is_possivel_fraude"), 1).otherwise(0)).alias("fraudes"),
        _sum(when(col("is_leitura_estimada"), 1).otherwise(0)).alias("estimadas"),
        avg("kwh_ativo").alias("kwh_medio"),
        avg("tensao_v").alias("tensao_media_v"),
    ).collect()[0]

    print(f"\nMétricas de negócio:")
    print(f"  Total leituras    : {stats['total']:,}")
    print(f"  Anomalias consumo : {stats['anomalias']:,} ({stats['anomalias']/stats['total']*100:.1f}%)")
    print(f"  Tensão crítica    : {stats['tensao_critica']:,} ({stats['tensao_critica']/stats['total']*100:.1f}%)")
    print(f"  Fraudes detectadas: {stats['fraudes']:,}")
    print(f"  Leituras estimadas: {stats['estimadas']:,}")
    print(f"  kWh médio/leitura : {stats['kwh_medio']:.4f}")
    print(f"  Tensão média      : {stats['tensao_media_v']:.1f}V")

# COMMAND ----------

# MAGIC %md ## Célula 6 — Parar streaming

# COMMAND ----------

# query_medidores.stop()
# print("Streaming encerrado")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Consultas SQL úteis após ingestão
# MAGIC
# MAGIC ```sql
# MAGIC -- Ver últimas leituras
# MAGIC SELECT medidor_id, tipo_medidor, alimentador_id, kwh_ativo, tensao_v, is_possivel_fraude
# MAGIC FROM adms.bronze.leituras_medidor_raw
# MAGIC ORDER BY _ingest_timestamp DESC
# MAGIC LIMIT 20;
# MAGIC
# MAGIC -- Medidores com possível fraude
# MAGIC SELECT medidor_id, cliente_id, alimentador_id, municipio, kwh_ativo
# MAGIC FROM adms.bronze.leituras_medidor_raw
# MAGIC WHERE is_possivel_fraude = true
# MAGIC ORDER BY _ingest_timestamp DESC;
# MAGIC
# MAGIC -- Tensão crítica por regional
# MAGIC SELECT regional, COUNT(*) as total, SUM(CASE WHEN is_tensao_critica THEN 1 ELSE 0 END) as criticos
# MAGIC FROM adms.bronze.leituras_medidor_raw
# MAGIC GROUP BY regional;
# MAGIC ```
