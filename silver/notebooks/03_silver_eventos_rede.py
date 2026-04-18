# Databricks notebook source
# MAGIC %md
# MAGIC # Silver — Eventos de Rede Enriquecidos
# MAGIC
# MAGIC Transforma os dados brutos da camada Bronze aplicando:
# MAGIC - Deduplicação por evento_id
# MAGIC - Enriquecimento com metadados da topologia de rede
# MAGIC - Pareamento INTERRUPCAO_INICIO ↔ INTERRUPCAO_FIM para calcular duração real
# MAGIC - Flags regulatórias ANEEL (classificação de interrupção)
# MAGIC - Flag de evento climático
# MAGIC - Índice de criticidade do evento
# MAGIC
# MAGIC **Fonte:** `adms.bronze.eventos_rede_raw`
# MAGIC **Destino:** `adms.silver.eventos_rede_enriquecido`

# COMMAND ----------

# MAGIC %md ## Célula 1 — Configuração

# COMMAND ----------

CATALOG        = "adms"
SCHEMA_BRONZE  = "bronze"
SCHEMA_SILVER  = "silver"
TABELA_FONTE   = "eventos_rede_raw"
TABELA_DESTINO = "eventos_rede_enriquecido"

spark.sql(f"USE CATALOG {CATALOG}")

# Verificar dados disponíveis no Bronze
df_bronze = spark.table(f"{CATALOG}.{SCHEMA_BRONZE}.{TABELA_FONTE}")
total_bronze = df_bronze.count()
print(f"Registros no Bronze: {total_bronze:,}")
print(f"Destino Silver    : {CATALOG}.{SCHEMA_SILVER}.{TABELA_DESTINO}")

# COMMAND ----------

# MAGIC %md ## Célula 2 — Topologia de rede (dado mestre)
# MAGIC
# MAGIC Como o Databricks Free Edition não acessa o arquivo topologia.py local,
# MAGIC recriamos os dados mestres de subestações e alimentadores inline.
# MAGIC Em produção, viriam de uma tabela de referência no próprio Lakehouse.

# COMMAND ----------

from pyspark.sql import Row

# Dados das 20 subestações (espelho do topologia.py)
subestacoes_data = [
    ("SE01","SE Cataguases",      "Zona da Mata",    "Cataguases",      -21.389,-42.696,138,40.0,1985),
    ("SE02","SE Leopoldina",      "Zona da Mata",    "Leopoldina",      -21.531,-42.640,138,30.0,1990),
    ("SE03","SE Muriaé",          "Zona da Mata",    "Muriaé",          -21.130,-42.367,138,50.0,1982),
    ("SE04","SE Juiz de Fora N",  "Zona da Mata",    "Juiz de Fora",    -21.754,-43.350,230,80.0,1978),
    ("SE05","SE Juiz de Fora S",  "Zona da Mata",    "Juiz de Fora",    -21.790,-43.380,230,80.0,1988),
    ("SE06","SE Ubá",             "Zona da Mata",    "Ubá",             -21.119,-42.940,138,35.0,1992),
    ("SE07","SE Viçosa",          "Zona da Mata",    "Viçosa",          -20.755,-42.881,138,25.0,1995),
    ("SE08","SE Governador Val.", "Zona da Mata",    "Governador Val.", -20.929,-42.651,138,30.0,1987),
    ("SE09","SE Manhuaçu",       "Zona da Mata",    "Manhuaçu",        -20.258,-42.029,138,28.0,1993),
    ("SE10","SE Carangola",       "Zona da Mata",    "Carangola",       -20.734,-42.027, 69,15.0,1998),
    ("SE11","SE Além Paraíba",   "Zona da Mata",    "Além Paraíba",    -21.881,-42.713,138,22.0,1996),
    ("SE12","SE Santos Dumont",   "Zona da Mata",    "Santos Dumont",   -21.462,-43.546,138,20.0,1999),
    ("SE13","SE Barbacena",       "Campo das Vert.", "Barbacena",       -21.226,-43.773,138,35.0,1983),
    ("SE14","SE São João Del Rei","Campo das Vert.", "São João DR",     -21.133,-44.261,138,30.0,1986),
    ("SE15","SE Lavras",          "Campo das Vert.", "Lavras",          -21.244,-44.999,138,28.0,1991),
    ("SE16","SE Varginha",        "Sul de Minas",    "Varginha",        -21.551,-45.430,138,45.0,1984),
    ("SE17","SE Poços de Caldas", "Sul de Minas",    "Poços de Caldas", -21.786,-46.565,138,40.0,1980),
    ("SE18","SE Itajubá",         "Sul de Minas",    "Itajubá",         -22.425,-45.451,138,32.0,1989),
    ("SE19","SE Três Corações",  "Sul de Minas",    "Três Corações",  -21.693,-45.253, 69,18.0,1997),
    ("SE20","SE Passos",          "Sul de Minas",    "Passos",          -20.718,-46.609,138,25.0,1994),
]

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

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

# Calcular idade da subestação
from pyspark.sql.functions import lit, col
ANO_ATUAL = 2026
df_subestacoes = df_subestacoes.withColumn(
    "se_idade_anos", lit(ANO_ATUAL) - col("se_ano_instalacao")
)

print(f"Subestações carregadas: {df_subestacoes.count()}")
df_subestacoes.select("se_id","se_nome","se_capacidade_mva","se_idade_anos").show(5)

# COMMAND ----------

# MAGIC %md ## Célula 3 — Deduplicação Bronze

# COMMAND ----------

from pyspark.sql.functions import (
    col, row_number, desc, to_timestamp,
    when, lit, round as _round
)
from pyspark.sql.window import Window

# Deduplicar por evento_id — manter o registro mais recente por _ingest_timestamp
window_dedup = Window.partitionBy("evento_id").orderBy(desc("_ingest_timestamp"))

df_dedup = (
    df_bronze
    .withColumn("_rn", row_number().over(window_dedup))
    .filter(col("_rn") == 1)
    .drop("_rn")
)

total_dedup = df_dedup.count()
duplicatas = total_bronze - total_dedup

print(f"Bronze total        : {total_bronze:,}")
print(f"Após deduplicação   : {total_dedup:,}")
print(f"Duplicatas removidas: {duplicatas:,} ({duplicatas/total_bronze*100:.1f}%)")

# COMMAND ----------

# MAGIC %md ## Célula 4 — Enriquecimento com topologia de subestação

# COMMAND ----------

# Join com metadados da subestação
df_enriquecido = (
    df_dedup
    .join(
        df_subestacoes.select(
            "se_id", "se_capacidade_mva", "se_tensao_kv",
            "se_idade_anos", "se_latitude", "se_longitude"
        ),
        df_dedup["subestacao_id"] == df_subestacoes["se_id"],
        how="left"
    )
    .drop("se_id")
)

# Converter timestamps para tipo correto
df_enriquecido = (
    df_enriquecido
    .withColumn("ts_evento",    to_timestamp(col("timestamp_evento")))
    .withColumn("ts_ingestao",  to_timestamp(col("timestamp_ingestao")))
)

print(f"Registros após join topologia: {df_enriquecido.count():,}")
df_enriquecido.select(
    "evento_id","subestacao_id","se_capacidade_mva","se_tensao_kv","se_idade_anos"
).show(3)

# COMMAND ----------

# MAGIC %md ## Célula 5 — Pareamento INTERRUPCAO_INICIO ↔ INTERRUPCAO_FIM
# MAGIC
# MAGIC Calcula a duração real das interrupções pareando eventos de início e fim
# MAGIC pelo alimentador_id dentro de uma janela de 6 horas.

# COMMAND ----------

from pyspark.sql.functions import (
    unix_timestamp, expr, lag, lead,
    datediff, abs as _abs
)

# Separar eventos de início e fim de interrupção
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

df_fim = (
    df_enriquecido
    .filter(col("tipo_evento") == "INTERRUPCAO_FIM")
    .select(
        col("evento_id").alias("id_fim"),
        col("alimentador_id").alias("al_fim"),
        col("ts_evento").alias("ts_fim"),
    )
)

# Join: para cada INICIO, encontrar o FIM mais próximo no mesmo alimentador
# dentro de uma janela de 6 horas (360 min)
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
        ).otherwise(col("duracao_estimada_min"))  # fallback para estimado
    )
    # Para cada início, pegar o fim mais próximo
    .withColumn(
        "_rn_par",
        row_number().over(
            Window.partitionBy("id_inicio").orderBy("ts_fim")
        )
    )
    .filter(col("_rn_par") == 1)
    .select("id_inicio", "duracao_real_min")
)

print(f"Pares INICIO-FIM encontrados: {df_pares.count():,}")

# Incorporar duração real nos eventos de início
df_enriquecido = (
    df_enriquecido
    .join(
        df_pares,
        df_enriquecido["evento_id"] == df_pares["id_inicio"],
        how="left"
    )
    .withColumn(
        "duracao_interrupcao_min",
        when(col("duracao_real_min").isNotNull(), col("duracao_real_min"))
        .otherwise(col("duracao_estimada_min"))
    )
    .drop("id_inicio", "duracao_real_min")
)

# COMMAND ----------

# MAGIC %md ## Célula 6 — Flags regulatórias e de negócio

# COMMAND ----------

from pyspark.sql.functions import (
    when, col, lit, greatest, least,
    hour, dayofweek
)

df_silver = (
    df_enriquecido

    # Flag climático: chuva > 5mm OU vento > 60 km/h
    .withColumn(
        "is_evento_climatico",
        (col("precipitacao_mm") > 5.0) | (col("velocidade_vento_kmh") > 60.0)
    )

    # Reclassificação ANEEL com base no contexto climático
    .withColumn(
        "classe_aneel",
        when(col("tipo_evento") == "MANOBRA_PLANEJADA", "PLANEJADA")
        .when(
            (col("tipo_evento") == "MANOBRA_EMERGENCIAL"), "EMERGENCIAL"
        )
        .when(
            col("is_evento_climatico") &
            (col("tipo_evento").isin("FALHA_EQUIPAMENTO", "INTERRUPCAO_INICIO")),
            "CASO_FORTUITO"
        )
        .when(
            col("tipo_evento").isin("FALHA_EQUIPAMENTO", "INTERRUPCAO_INICIO"),
            "NAO_PLANEJADA"
        )
        .otherwise("INFORMATIVO")
    )

    # Computa score de criticidade (0-100) baseado em:
    # - Severidade (40 pts)
    # - Clientes afetados (30 pts, normalizado por 10k)
    # - Duração estimada (20 pts, normalizado por 240 min)
    # - Idade da subestação (10 pts, normalizado por 40 anos)
    .withColumn(
        "score_severidade",
        when(col("severidade") == "CRITICO", 40)
        .when(col("severidade") == "ALTO",   30)
        .when(col("severidade") == "MEDIO",  15)
        .otherwise(5)
    )
    .withColumn(
        "score_impacto",
        least(lit(30), _round(col("clientes_afetados") / 10000 * 30, 1))
    )
    .withColumn(
        "score_duracao",
        least(lit(20), _round(col("duracao_estimada_min") / 240 * 20, 1))
    )
    .withColumn(
        "score_ativo",
        least(lit(10), _round(col("se_idade_anos") / 40 * 10, 1))
    )
    .withColumn(
        "score_criticidade",
        _round(
            col("score_severidade") + col("score_impacto") +
            col("score_duracao") + col("score_ativo"),
            1
        )
    )
    .withColumn(
        "faixa_criticidade",
        when(col("score_criticidade") >= 70, "CRITICO")
        .when(col("score_criticidade") >= 40, "ALTO")
        .when(col("score_criticidade") >= 20, "MEDIO")
        .otherwise("BAIXO")
    )

    # Hora do dia e dia da semana para análise de padrão
    .withColumn("hora_evento",      hour(col("ts_evento")))
    .withColumn("dia_semana",       dayofweek(col("ts_evento")))
    .withColumn(
        "periodo_dia",
        when((col("hora_evento") >= 0)  & (col("hora_evento") < 6),  "MADRUGADA")
        .when((col("hora_evento") >= 6)  & (col("hora_evento") < 12), "MANHA")
        .when((col("hora_evento") >= 12) & (col("hora_evento") < 18), "TARDE")
        .otherwise("NOITE")
    )

    # Campos de controle Silver
    .withColumn("silver_processado_em", col("_ingest_timestamp"))
    .withColumn("versao_silver", lit("1.0.0"))
)

# Remover colunas auxiliares de score
df_silver = df_silver.drop(
    "score_severidade", "score_impacto", "score_duracao", "score_ativo"
)

print(f"Registros Silver: {df_silver.count():,}")

# COMMAND ----------

# MAGIC %md ## Célula 7 — Salvar na camada Silver

# COMMAND ----------

(
    df_silver
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("_ano", "_mes", "regional")
    .saveAsTable(f"{CATALOG}.{SCHEMA_SILVER}.{TABELA_DESTINO}")
)

total_silver = spark.table(f"{CATALOG}.{SCHEMA_SILVER}.{TABELA_DESTINO}").count()
print(f"Tabela Silver salva: {CATALOG}.{SCHEMA_SILVER}.{TABELA_DESTINO}")
print(f"Total registros    : {total_silver:,}")

# COMMAND ----------

# MAGIC %md ## Célula 8 — Validação e análise Silver

# COMMAND ----------

from pyspark.sql.functions import count, sum as _sum, avg, countDistinct, round as _round

df_s = spark.table(f"{CATALOG}.{SCHEMA_SILVER}.{TABELA_DESTINO}")

print("=== Distribuição por classe ANEEL ===")
df_s.groupBy("classe_aneel").count().orderBy("count", ascending=False).show()

print("=== Distribuição por faixa de criticidade ===")
df_s.groupBy("faixa_criticidade").count().orderBy("count", ascending=False).show()

print("=== Eventos climáticos ===")
df_s.groupBy("is_evento_climatico").count().show()

print("=== Top 5 alimentadores mais críticos ===")
(
    df_s
    .filter(col("tipo_evento").isin("FALHA_EQUIPAMENTO","INTERRUPCAO_INICIO"))
    .groupBy("alimentador_id", "regional", "municipio")
    .agg(
        count("*").alias("total_eventos"),
        _sum("clientes_afetados").alias("total_clientes_afetados"),
        avg("score_criticidade").alias("score_medio"),
    )
    .orderBy("total_clientes_afetados", ascending=False)
    .limit(5)
    .show(truncate=False)
)

print("=== Distribuição por período do dia ===")
df_s.groupBy("periodo_dia").count().orderBy("count", ascending=False).show()

print("=== Score de criticidade médio por regional ===")
(
    df_s
    .groupBy("regional")
    .agg(
        avg("score_criticidade").alias("score_medio"),
        count("*").alias("total_eventos"),
        _sum("clientes_afetados").alias("clientes_afetados"),
    )
    .orderBy("score_medio", ascending=False)
    .show()
)

# COMMAND ----------

# MAGIC %md ## Célula 9 — Verificar via SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     regional,
# MAGIC     classe_aneel,
# MAGIC     faixa_criticidade,
# MAGIC     COUNT(*) AS total,
# MAGIC     SUM(clientes_afetados) AS clientes_afetados,
# MAGIC     ROUND(AVG(score_criticidade), 1) AS score_medio,
# MAGIC     SUM(CASE WHEN is_evento_climatico THEN 1 ELSE 0 END) AS eventos_climaticos
# MAGIC FROM adms.silver.eventos_rede_enriquecido
# MAGIC GROUP BY regional, classe_aneel, faixa_criticidade
# MAGIC ORDER BY clientes_afetados DESC
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %md
# MAGIC ## Próximos passos
# MAGIC - Executar `04_silver_leituras_medidor` para normalizar as leituras AMI
# MAGIC - Executar `05_gold_kpi_dec_fec` para calcular os indicadores regulatórios ANEEL
# MAGIC - Executar `06_gold_saude_ativos` para o índice de saúde da rede
