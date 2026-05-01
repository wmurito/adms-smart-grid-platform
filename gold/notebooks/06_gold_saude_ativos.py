# Databricks notebook source
# MAGIC %md
# MAGIC # Gold — Índice de Saúde de Ativos da Rede
# MAGIC
# MAGIC Calcula o índice de saúde (0-100) de cada alimentador combinando:
# MAGIC - Frequência de falhas nos últimos períodos
# MAGIC - Duração média das interrupções (MTTR)
# MAGIC - Tempo médio entre falhas (MTBF)
# MAGIC - Proporção de eventos climáticos vs técnicos
# MAGIC - Score de criticidade médio dos eventos
# MAGIC
# MAGIC Score 0-30 = CRÍTICO (substituição urgente)
# MAGIC Score 31-60 = ATENÇÃO (manutenção preventiva)
# MAGIC Score 61-100 = NORMAL (monitoramento padrão)
# MAGIC
# MAGIC **Fonte:** `adms.silver.eventos_rede_enriquecido`
# MAGIC **Destino:** `adms.gold.saude_ativo_alimentador`

# COMMAND ----------

# MAGIC %md ## Célula 1 — Configuração

# COMMAND ----------

CATALOG       = "adms"
SCHEMA_SILVER = "silver"
SCHEMA_GOLD   = "gold"

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_GOLD}")

df_silver = spark.table(f"{CATALOG}.{SCHEMA_SILVER}.eventos_rede_enriquecido")
total = df_silver.count()
print(f"Registros Silver: {total:,}")

# COMMAND ----------

# MAGIC %md ## Célula 2 — Métricas base por alimentador

# COMMAND ----------

from pyspark.sql.functions import (
    col, count, sum as _sum, avg, max as _max, min as _min,
    when, round as _round, lit, datediff,
    unix_timestamp, countDistinct
)

# Filtrar apenas eventos operacionais relevantes (excluir informativos)
TIPOS_FALHA = [
    "FALHA_EQUIPAMENTO", "INTERRUPCAO_INICIO",
    "RELIGAMENTO_AUTOMATICO", "ALARME_SOBRECARGA", "ALARME_TEMPERATURA"
]

df_falhas = df_silver.filter(col("tipo_evento").isin(TIPOS_FALHA))

# Métricas por alimentador
df_metricas = (
    df_falhas
    .groupBy(
        "alimentador_id", "subestacao_id", "regional",
        "municipio", "se_capacidade_mva", "se_idade_anos"
    )
    .agg(
        # Volume de falhas
        count("*").alias("total_falhas"),
        count(when(col("tipo_evento") == "FALHA_EQUIPAMENTO", 1))
         .alias("falhas_equipamento"),
        count(when(col("tipo_evento") == "RELIGAMENTO_AUTOMATICO", 1))
         .alias("religamentos"),
        count(when(col("tipo_evento") == "ALARME_SOBRECARGA", 1))
         .alias("alarmes_sobrecarga"),
        count(when(col("tipo_evento") == "ALARME_TEMPERATURA", 1))
         .alias("alarmes_temperatura"),

        # Impacto
        _sum("clientes_afetados").alias("total_clientes_afetados"),
        avg("clientes_afetados").alias("media_clientes_por_evento"),

        # Duração (MTTR — Mean Time To Repair)
        avg("duracao_interrupcao_min").alias("mttr_min"),
        _max("duracao_interrupcao_min").alias("duracao_max_min"),

        # Score médio de criticidade
        avg("score_criticidade").alias("score_criticidade_medio"),

        # Eventos climáticos vs técnicos
        count(when(col("is_evento_climatico"), 1)).alias("falhas_climaticas"),
        count(when(~col("is_evento_climatico"), 1)).alias("falhas_tecnicas"),

        # Severidade
        count(when(col("severidade") == "CRITICO", 1)).alias("eventos_criticos"),
        count(when(col("severidade") == "ALTO", 1)).alias("eventos_altos"),
    )
)

print(f"Alimentadores com métricas calculadas: {df_metricas.count()}")
df_metricas.select(
    "alimentador_id", "total_falhas", "falhas_equipamento",
    "mttr_min", "score_criticidade_medio"
).orderBy("total_falhas", ascending=False).show(5)

# COMMAND ----------

# MAGIC %md ## Célula 3 — Cálculo do MTBF (Mean Time Between Failures)
# MAGIC
# MAGIC MTBF = intervalo médio (em horas) entre falhas consecutivas no mesmo alimentador.
# MAGIC Quanto maior o MTBF, mais confiável o alimentador.

# COMMAND ----------

from pyspark.sql.functions import lag, unix_timestamp, col, avg, round as _round
from pyspark.sql.window import Window

# Calcular intervalo entre falhas consecutivas por alimentador
window_falhas = Window.partitionBy("alimentador_id").orderBy("ts_evento")

df_intervalos = (
    df_falhas
    .filter(col("tipo_evento") == "FALHA_EQUIPAMENTO")
    .withColumn(
        "ts_falha_anterior",
        lag("ts_evento", 1).over(window_falhas)
    )
    .withColumn(
        "intervalo_horas",
        (unix_timestamp("ts_evento") - unix_timestamp("ts_falha_anterior")) / 3600
    )
    .filter(col("ts_falha_anterior").isNotNull())
    .groupBy("alimentador_id")
    .agg(
        _round(avg("intervalo_horas"), 2).alias("mtbf_horas"),
        count("*").alias("pares_falha")
    )
)

print(f"Alimentadores com MTBF calculado: {df_intervalos.count()}")
df_intervalos.orderBy("mtbf_horas").show(5)

# COMMAND ----------

# MAGIC %md ## Célula 4 — Composição do Índice de Saúde (0-100)
# MAGIC
# MAGIC O índice é composto por 5 dimensões, cada uma contribuindo com peso:
# MAGIC
# MAGIC | Dimensão              | Peso | Lógica                                        |
# MAGIC |-----------------------|------|-----------------------------------------------|
# MAGIC | Frequência de falhas  | 30%  | Menos falhas = maior score                    |
# MAGIC | MTTR (tempo reparo)   | 25%  | Menor tempo de reparo = maior score           |
# MAGIC | Eventos críticos      | 20%  | Menos eventos críticos = maior score          |
# MAGIC | MTBF (entre falhas)   | 15%  | Maior intervalo entre falhas = maior score    |
# MAGIC | Impacto em clientes   | 10%  | Menos clientes afetados = maior score         |

# COMMAND ----------

from pyspark.sql.functions import (
    col, when, lit, round as _round,
    greatest, least, coalesce
)

# Join métricas + MTBF
df_completo = (
    df_metricas
    .join(df_intervalos, on="alimentador_id", how="left")
    .withColumn("mtbf_horas", coalesce(col("mtbf_horas"), lit(0.0)))
)

# Normalização de cada dimensão para 0-100
# Usando limites baseados no universo simulado

# 1. Score frequência (30 pontos) — menos falhas = mais pontos
# 0 falhas = 30pts, 50+ falhas = 0pts
df_score = (
    df_completo
    .withColumn(
        "score_frequencia",
        _round(
            greatest(lit(0.0),
                lit(30.0) - (col("total_falhas") / 50.0 * 30.0)
            ), 1
        )
    )

    # 2. Score MTTR (25 pontos) — menor tempo reparo = mais pontos
    # 0 min = 25pts, 240+ min = 0pts
    .withColumn(
        "score_mttr",
        _round(
            greatest(lit(0.0),
                lit(25.0) - (col("mttr_min") / 240.0 * 25.0)
            ), 1
        )
    )

    # 3. Score eventos críticos (20 pontos)
    # 0 críticos = 20pts, 20+ críticos = 0pts
    .withColumn(
        "score_criticos",
        _round(
            greatest(lit(0.0),
                lit(20.0) - (col("eventos_criticos") / 20.0 * 20.0)
            ), 1
        )
    )

    # 4. Score MTBF (15 pontos) — maior intervalo = mais pontos
    # 0h = 0pts, 24h+ = 15pts
    .withColumn(
        "score_mtbf",
        _round(
            least(lit(15.0),
                col("mtbf_horas") / 24.0 * 15.0
            ), 1
        )
    )

    # 5. Score impacto clientes (10 pontos)
    # 0 clientes afetados = 10pts, 50k+ = 0pts
    .withColumn(
        "score_impacto",
        _round(
            greatest(lit(0.0),
                lit(10.0) - (col("total_clientes_afetados") / 50000.0 * 10.0)
            ), 1
        )
    )

    # Índice composto
    .withColumn(
        "indice_saude",
        _round(
            col("score_frequencia") + col("score_mttr") +
            col("score_criticos") + col("score_mtbf") +
            col("score_impacto"),
            1
        )
    )

    # Classificação
    .withColumn(
        "classificacao_saude",
        when(col("indice_saude") <= 30, "CRITICO")
        .when(col("indice_saude") <= 60, "ATENCAO")
        .otherwise("NORMAL")
    )

    # Ação recomendada
    .withColumn(
        "acao_recomendada",
        when(col("classificacao_saude") == "CRITICO",
             "Inspeção e substituição de equipamentos urgente")
        .when(col("classificacao_saude") == "ATENCAO",
             "Agendar manutenção preventiva nos próximos 30 dias")
        .otherwise("Monitoramento padrão — sem ação imediata")
    )

    # Prazo estimado de intervenção (dias)
    .withColumn(
        "prazo_intervencao_dias",
        when(col("classificacao_saude") == "CRITICO",  lit(7))
        .when(col("classificacao_saude") == "ATENCAO", lit(30))
        .otherwise(lit(90))
    )

    # Controle Gold
    .withColumn("versao_gold", lit("1.0.0"))
)

print(f"Registros com índice calculado: {df_score.count()}")

# COMMAND ----------

# MAGIC %md ## Célula 5 — Salvar Gold

# COMMAND ----------

colunas_gold = [
    "alimentador_id", "subestacao_id", "regional", "municipio",
    "se_capacidade_mva", "se_idade_anos",
    "total_falhas", "falhas_equipamento", "religamentos",
    "alarmes_sobrecarga", "alarmes_temperatura",
    "total_clientes_afetados", "media_clientes_por_evento",
    "mttr_min", "duracao_max_min", "mtbf_horas",
    "score_criticidade_medio",
    "falhas_climaticas", "falhas_tecnicas",
    "eventos_criticos", "eventos_altos",
    "score_frequencia", "score_mttr", "score_criticos",
    "score_mtbf", "score_impacto",
    "indice_saude", "classificacao_saude",
    "acao_recomendada", "prazo_intervencao_dias",
    "versao_gold"
]

(
    df_score.select(colunas_gold)
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("regional", "classificacao_saude")
    .saveAsTable(f"{CATALOG}.{SCHEMA_GOLD}.saude_ativo_alimentador")
)

total_gold = spark.table(f"{CATALOG}.{SCHEMA_GOLD}.saude_ativo_alimentador").count()
print(f"Tabela Gold salva: {CATALOG}.{SCHEMA_GOLD}.saude_ativo_alimentador")
print(f"Total registros  : {total_gold:,}")

# COMMAND ----------

# MAGIC %md ## Célula 6 — Análise e ranking de saúde

# COMMAND ----------

from pyspark.sql.functions import count, avg, round as _round, col, sum as _sum

df_g = spark.table(f"{CATALOG}.{SCHEMA_GOLD}.saude_ativo_alimentador")

print("=== Distribuição por classificação de saúde ===")
df_g.groupBy("classificacao_saude").count().orderBy("count", ascending=False).show()

print("=== Saúde média por regional ===")
(
    df_g
    .groupBy("regional")
    .agg(
        _round(avg("indice_saude"), 1).alias("indice_medio"),
        count("*").alias("alimentadores"),
        count(when(col("classificacao_saude") == "CRITICO", 1)).alias("criticos"),
        count(when(col("classificacao_saude") == "ATENCAO", 1)).alias("atencao"),
        count(when(col("classificacao_saude") == "NORMAL", 1)).alias("normais"),
        _round(avg("mttr_min"), 1).alias("mttr_medio_min"),
        _round(avg("mtbf_horas"), 1).alias("mtbf_medio_horas"),
    )
    .orderBy("indice_medio")
    .show()
)

print("=== Top 10 alimentadores CRÍTICOS (menor índice de saúde) ===")
(
    df_g
    .filter(col("classificacao_saude") == "CRITICO")
    .select(
        "alimentador_id", "regional", "municipio",
        "indice_saude", "total_falhas", "mttr_min",
        "mtbf_horas", "eventos_criticos",
        "acao_recomendada", "prazo_intervencao_dias"
    )
    .orderBy("indice_saude")
    .limit(10)
    .show(truncate=False)
)

print("=== Correlação idade SE vs saúde ===")
(
    df_g
    .withColumn(
        "faixa_idade",
        when(col("se_idade_anos") <= 15, "0-15 anos")
        .when(col("se_idade_anos") <= 30, "16-30 anos")
        .otherwise("31+ anos")
    )
    .groupBy("faixa_idade")
    .agg(
        _round(avg("indice_saude"), 1).alias("saude_media"),
        _round(avg("total_falhas"), 1).alias("falhas_media"),
        count("*").alias("alimentadores"),
    )
    .orderBy("faixa_idade")
    .show()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     regional,
# MAGIC     classificacao_saude,
# MAGIC     COUNT(*) AS alimentadores,
# MAGIC     ROUND(AVG(indice_saude), 1) AS indice_medio,
# MAGIC     ROUND(AVG(total_falhas), 1) AS falhas_medio,
# MAGIC     ROUND(AVG(mttr_min), 0) AS mttr_medio_min,
# MAGIC     SUM(total_clientes_afetados) AS clientes_afetados_total
# MAGIC FROM adms.gold.saude_ativo_alimentador
# MAGIC GROUP BY regional, classificacao_saude
# MAGIC ORDER BY regional, indice_medio
