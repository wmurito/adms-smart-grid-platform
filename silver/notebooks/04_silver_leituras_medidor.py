# Databricks notebook source
# MAGIC %md
# MAGIC # Silver — Leituras de Medidores AMI Normalizadas
# MAGIC
# MAGIC Transforma as leituras brutas da camada Bronze aplicando:
# MAGIC - Deduplicação por leitura_id
# MAGIC - Cálculo de delta_kwh entre leituras consecutivas (LAG por medidor)
# MAGIC - Classificação ANEEL de tensão (PRODIST Módulo 8): ADEQUADA / PRECÁRIA / CRÍTICA
# MAGIC - Score de anomalia de consumo por desvio em relação à média do alimentador
# MAGIC - Flag de possível fraude com lógica aprimorada
# MAGIC - Perfil horário de consumo por tipo de medidor
# MAGIC
# MAGIC **Fonte:** `adms.bronze.leituras_medidor_raw`
# MAGIC **Destino:** `adms.silver.leituras_medidor_norm`

# COMMAND ----------

# MAGIC %md ## Célula 1 — Configuração e carga Bronze

# COMMAND ----------

CATALOG        = "adms"
SCHEMA_BRONZE  = "bronze"
SCHEMA_SILVER  = "silver"
TABELA_FONTE   = "leituras_medidor_raw"
TABELA_DESTINO = "leituras_medidor_norm"

spark.sql(f"USE CATALOG {CATALOG}")

df_bronze = spark.table(f"{CATALOG}.{SCHEMA_BRONZE}.{TABELA_FONTE}")
total_bronze = df_bronze.count()
print(f"Registros Bronze : {total_bronze:,}")
print(f"Destino Silver   : {CATALOG}.{SCHEMA_SILVER}.{TABELA_DESTINO}")

df_bronze.printSchema()

# COMMAND ----------

# MAGIC %md ## Célula 2 — Deduplicação

# COMMAND ----------

from pyspark.sql.functions import col, row_number, desc, to_timestamp
from pyspark.sql.window import Window

window_dedup = Window.partitionBy("leitura_id").orderBy(desc("_ingest_timestamp"))

df_dedup = (
    df_bronze
    .withColumn("_rn", row_number().over(window_dedup))
    .filter(col("_rn") == 1)
    .drop("_rn")
    .withColumn("ts_leitura", to_timestamp(col("timestamp_leitura")))
)

total_dedup = df_dedup.count()
print(f"Bronze total      : {total_bronze:,}")
print(f"Após deduplicação : {total_dedup:,}")
print(f"Duplicatas        : {total_bronze - total_dedup:,}")

# COMMAND ----------

# MAGIC %md ## Célula 3 — Delta kWh com LAG por medidor
# MAGIC
# MAGIC Calcula o consumo real no intervalo entre leituras consecutivas.
# MAGIC Leituras com delta negativo indicam reset do medidor ou possível fraude.

# COMMAND ----------

from pyspark.sql.functions import (
    lag, col, when, lit, round as _round,
    unix_timestamp, abs as _abs
)

# Janela por medidor ordenada por timestamp
window_medidor = Window.partitionBy("medidor_id").orderBy("ts_leitura")

df_delta = (
    df_dedup
    .withColumn(
        "kwh_ativo_anterior",
        lag("kwh_ativo", 1).over(window_medidor)
    )
    .withColumn(
        "ts_leitura_anterior",
        lag("ts_leitura", 1).over(window_medidor)
    )
    .withColumn(
        "intervalo_real_min",
        _round(
            (unix_timestamp("ts_leitura") - unix_timestamp("ts_leitura_anterior")) / 60,
            1
        )
    )
    .withColumn(
        "delta_kwh",
        when(
            col("kwh_ativo_anterior").isNotNull(),
            _round(col("kwh_ativo") - col("kwh_ativo_anterior"), 4)
        ).otherwise(col("kwh_ativo"))
    )
    # Flag: delta negativo = reset ou fraude
    .withColumn(
        "is_delta_negativo",
        col("delta_kwh") < 0
    )
    # Normalizar delta negativo para 0 (não penalizar o consumo acumulado)
    .withColumn(
        "delta_kwh_ajustado",
        when(col("delta_kwh") < 0, lit(0.0)).otherwise(col("delta_kwh"))
    )
)

print(f"Registros com delta calculado: {df_delta.count():,}")

# Verificar deltas negativos (indicadores de anomalia)
negativos = df_delta.filter(col("is_delta_negativo")).count()
print(f"Deltas negativos (anomalias): {negativos:,} ({negativos/total_dedup*100:.2f}%)")

# COMMAND ----------

# MAGIC %md ## Célula 4 — Classificação ANEEL de Tensão (PRODIST Módulo 8)
# MAGIC
# MAGIC Faixas para tensão nominal 220V:
# MAGIC - ADEQUADA  : 209V – 231V (±5%)
# MAGIC - PRECÁRIA  : 198V – 209V ou 231V – 242V (±10%)
# MAGIC - CRÍTICA   : < 198V ou > 242V

# COMMAND ----------

from pyspark.sql.functions import when, col, lit

def classificar_tensao_aneel(tensao_col, nominal_col):
    """
    Classifica tensão conforme PRODIST Módulo 8 ANEEL.
    Retorna: ADEQUADA, PRECARIA, CRITICA
    """
    return (
        when(
            (tensao_col >= nominal_col * 0.95) & (tensao_col <= nominal_col * 1.05),
            "ADEQUADA"
        )
        .when(
            ((tensao_col >= nominal_col * 0.90) & (tensao_col < nominal_col * 0.95)) |
            ((tensao_col > nominal_col * 1.05) & (tensao_col <= nominal_col * 1.10)),
            "PRECARIA"
        )
        .otherwise("CRITICA")
    )

# Aplicar classificação — tensão nominal varia por tipo de medidor
df_tensao = (
    df_delta
    .withColumn(
        "tensao_nominal_v",
        when(col("tipo_medidor") == "INDUSTRIAL", lit(380.0))
        .when(col("tipo_medidor") == "RURAL",      lit(127.0))
        .otherwise(lit(220.0))
    )
    .withColumn(
        "classe_tensao_aneel",
        classificar_tensao_aneel(col("tensao_v"), col("tensao_nominal_v"))
    )
    .withColumn(
        "desvio_tensao_pct",
        _round(
            (_abs(col("tensao_v") - col("tensao_nominal_v")) / col("tensao_nominal_v")) * 100,
            2
        )
    )
)

print("Classificação ANEEL de tensão:")
df_tensao.groupBy("classe_tensao_aneel").count().orderBy("count", ascending=False).show()

# COMMAND ----------

# MAGIC %md ## Célula 5 — Score de anomalia de consumo por alimentador
# MAGIC
# MAGIC Calcula desvio do consumo de cada medidor em relação à média do seu
# MAGIC alimentador no mesmo período. Score alto indica possível fraude ou
# MAGIC defeito no medidor.

# COMMAND ----------

from pyspark.sql.functions import avg, stddev, col, when, lit, round as _round
from pyspark.sql.window import Window

# Média e desvio padrão do delta_kwh por alimentador e tipo de medidor
window_alim = Window.partitionBy("alimentador_id", "tipo_medidor")

df_score = (
    df_tensao
    .withColumn("media_kwh_alim",  avg("delta_kwh_ajustado").over(window_alim))
    .withColumn("stddev_kwh_alim", stddev("delta_kwh_ajustado").over(window_alim))
    .withColumn(
        "z_score_consumo",
        when(
            col("stddev_kwh_alim") > 0,
            _round(
                _abs(col("delta_kwh_ajustado") - col("media_kwh_alim")) /
                col("stddev_kwh_alim"),
                2
            )
        ).otherwise(lit(0.0))
    )
    # Score de anomalia: 0-100 baseado em z-score (z=3 → score=100)
    .withColumn(
        "score_anomalia",
        _round(
            when(col("z_score_consumo") >= 3, lit(100.0))
            .otherwise(col("z_score_consumo") / 3 * 100),
            1
        )
    )
    .withColumn(
        "faixa_anomalia",
        when(col("score_anomalia") >= 80, "ALTA")
        .when(col("score_anomalia") >= 50, "MEDIA")
        .when(col("score_anomalia") >= 20, "BAIXA")
        .otherwise("NORMAL")
    )
)

print("Distribuição de anomalia de consumo:")
df_score.groupBy("faixa_anomalia").count().orderBy("count", ascending=False).show()

# COMMAND ----------

# MAGIC %md ## Célula 6 — Lógica aprimorada de detecção de fraude

# COMMAND ----------

from pyspark.sql.functions import (
    col, when, lit, count, avg,
    round as _round
)

# Fraude aprimorada: combina múltiplos sinais
df_fraude = (
    df_score
    .withColumn(
        "score_fraude",
        # Consumo anormalmente baixo (possível desvio de energia)
        when(col("delta_kwh_ajustado") < col("media_kwh_alim") * 0.3,  lit(40))
        .otherwise(lit(0))
        +
        # Delta negativo (manipulação do medidor)
        when(col("is_delta_negativo"), lit(30))
        .otherwise(lit(0))
        +
        # Flag original do simulador
        when(col("is_possivel_fraude"), lit(20))
        .otherwise(lit(0))
        +
        # Anomalia de consumo alta
        when(col("score_anomalia") >= 80, lit(10))
        .otherwise(lit(0))
    )
    .withColumn(
        "is_suspeita_fraude",
        col("score_fraude") >= 50
    )
    .withColumn(
        "nivel_suspeita",
        when(col("score_fraude") >= 70, "ALTO")
        .when(col("score_fraude") >= 50, "MEDIO")
        .when(col("score_fraude") >= 20, "BAIXO")
        .otherwise("NORMAL")
    )
)

print("Níveis de suspeita de fraude:")
df_fraude.groupBy("nivel_suspeita").count().orderBy("count", ascending=False).show()

suspeitos = df_fraude.filter(col("is_suspeita_fraude")).count()
print(f"Total suspeitas de fraude: {suspeitos:,} ({suspeitos/total_dedup*100:.2f}%)")

# COMMAND ----------

# MAGIC %md ## Célula 7 — Campos finais e persistência Silver

# COMMAND ----------

from pyspark.sql.functions import (
    col, hour, dayofweek, month, lit,
    when, round as _round
)

df_silver = (
    df_fraude

    # Enriquecimento temporal
    .withColumn("hora_leitura",  hour(col("ts_leitura")))
    .withColumn("dia_semana",    dayofweek(col("ts_leitura")))
    .withColumn("mes_leitura",   month(col("ts_leitura")))
    .withColumn(
        "periodo_dia",
        when((col("hora_leitura") >= 0)  & (col("hora_leitura") < 6),  "MADRUGADA")
        .when((col("hora_leitura") >= 6)  & (col("hora_leitura") < 12), "MANHA")
        .when((col("hora_leitura") >= 12) & (col("hora_leitura") < 18), "TARDE")
        .otherwise("NOITE")
    )

    # Fator de potência classificado
    .withColumn(
        "classe_fp",
        when(col("fator_potencia") >= 0.95, "EXCELENTE")
        .when(col("fator_potencia") >= 0.92, "BOM")
        .when(col("fator_potencia") >= 0.85, "REGULAR")
        .otherwise("RUIM")
    )

    # Controle Silver
    .withColumn("silver_processado_em", col("_ingest_timestamp"))
    .withColumn("versao_silver", lit("1.0.0"))

    # Remover colunas auxiliares de cálculo
    .drop(
        "kwh_ativo_anterior", "ts_leitura_anterior",
        "media_kwh_alim", "stddev_kwh_alim", "z_score_consumo"
    )
)

# Persistir Silver
(
    df_silver
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("_ano", "_mes", "subestacao_id")
    .saveAsTable(f"{CATALOG}.{SCHEMA_SILVER}.{TABELA_DESTINO}")
)

total_silver = spark.table(f"{CATALOG}.{SCHEMA_SILVER}.{TABELA_DESTINO}").count()
print(f"Tabela Silver salva: {CATALOG}.{SCHEMA_SILVER}.{TABELA_DESTINO}")
print(f"Total registros   : {total_silver:,}")

# COMMAND ----------

# MAGIC %md ## Célula 8 — Análise Silver

# COMMAND ----------

from pyspark.sql.functions import (
    count, sum as _sum, avg, round as _round, col
)

df_s = spark.table(f"{CATALOG}.{SCHEMA_SILVER}.{TABELA_DESTINO}")

print("=== Classificação ANEEL por tipo de medidor ===")
(
    df_s.groupBy("tipo_medidor", "classe_tensao_aneel")
    .count()
    .orderBy("tipo_medidor", "classe_tensao_aneel")
    .show()
)

print("=== Consumo médio por tipo de medidor e período ===")
(
    df_s
    .groupBy("tipo_medidor", "periodo_dia")
    .agg(
        _round(avg("delta_kwh_ajustado"), 4).alias("kwh_medio"),
        _round(avg("kw_demanda"), 3).alias("demanda_media_kw"),
        count("*").alias("leituras"),
    )
    .orderBy("tipo_medidor", "periodo_dia")
    .show()
)

print("=== Top 5 alimentadores com maior suspeita de fraude ===")
(
    df_s
    .filter(col("is_suspeita_fraude"))
    .groupBy("alimentador_id", "regional", "municipio")
    .agg(
        count("*").alias("suspeitas"),
        _round(avg("score_fraude"), 1).alias("score_medio"),
    )
    .orderBy("suspeitas", ascending=False)
    .limit(5)
    .show(truncate=False)
)

print("=== Tensão crítica por regional ===")
(
    df_s
    .groupBy("regional")
    .agg(
        count("*").alias("total_leituras"),
        _sum(when(col("classe_tensao_aneel") == "CRITICA", 1).otherwise(0))
         .alias("tensao_critica"),
        _sum(when(col("classe_tensao_aneel") == "PRECARIA", 1).otherwise(0))
         .alias("tensao_precaria"),
        _round(avg("desvio_tensao_pct"), 2).alias("desvio_medio_pct"),
    )
    .orderBy("tensao_critica", ascending=False)
    .show()
)

print("=== Fator de potência por tipo de medidor ===")
(
    df_s
    .groupBy("tipo_medidor", "classe_fp")
    .count()
    .orderBy("tipo_medidor", "classe_fp")
    .show()
)
