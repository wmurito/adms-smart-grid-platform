# Databricks notebook source
# MAGIC %md
# MAGIC # Gold — KPIs Regulatórios DEC/FEC (ANEEL)
# MAGIC
# MAGIC Calcula os indicadores de qualidade regulatórios exigidos pela ANEEL:
# MAGIC
# MAGIC - **DEC** (Duração Equivalente de Interrupção por Unidade Consumidora)
# MAGIC   `DEC = Σ(clientes_afetados × duração_horas) / total_clientes_conjunto`
# MAGIC
# MAGIC - **FEC** (Frequência Equivalente de Interrupção por Unidade Consumidora)
# MAGIC   `FEC = Σ(clientes_afetados) / total_clientes_conjunto`
# MAGIC
# MAGIC Granularidade: alimentador × mês × ano
# MAGIC Comparação com limites regulatórios ANEEL por tipo de área (urbana/rural)
# MAGIC
# MAGIC **Fonte:** `adms.silver.eventos_rede_enriquecido`
# MAGIC **Destino:** `adms.gold.kpi_dec_fec_mensal`

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
print(f"Registros Silver disponíveis: {total:,}")

# Verificar tipos de evento disponíveis
print("\nTipos de evento para cálculo DEC/FEC:")
df_silver.groupBy("tipo_evento").count().orderBy("count", ascending=False).show()

# COMMAND ----------

# MAGIC %md ## Célula 2 — Dados mestres: total de clientes por alimentador
# MAGIC
# MAGIC O denominador do DEC/FEC é o total de clientes do conjunto (alimentador).
# MAGIC Extraímos do histórico de leituras de medidores (count distinto de clientes).

# COMMAND ----------

from pyspark.sql.functions import countDistinct, col, lit

# Total de clientes únicos por alimentador (fonte: Silver leituras)
df_clientes = (
    spark.table(f"{CATALOG}.{SCHEMA_SILVER}.leituras_medidor_norm")
    .groupBy("alimentador_id", "subestacao_id", "regional", "municipio")
    .agg(
        countDistinct("cliente_id").alias("total_clientes_conjunto")
    )
)

print(f"Alimentadores com dados de clientes: {df_clientes.count()}")
df_clientes.orderBy("total_clientes_conjunto", ascending=False).show(5)

# COMMAND ----------

# MAGIC %md ## Célula 3 — Filtrar apenas interrupções para cálculo DEC/FEC
# MAGIC
# MAGIC Conforme PRODIST Módulo 8, apenas interrupções NÃO PLANEJADAS e
# MAGIC CASO FORTUITO entram no cálculo de DEC/FEC regulatório.
# MAGIC Interrupções PLANEJADAS e INFORMATIVAS são excluídas.

# COMMAND ----------

from pyspark.sql.functions import (
    col, when, lit, round as _round,
    year, month, sum as _sum, count,
    avg, max as _max, min as _min
)

# Eventos que entram no cálculo regulatório DEC/FEC
TIPOS_INTERRUPCAO = ["INTERRUPCAO_INICIO", "FALHA_EQUIPAMENTO"]
CLASSES_REGULATORIAS = ["NAO_PLANEJADA", "CASO_FORTUITO", "EMERGENCIAL"]

df_interrupcoes = (
    df_silver
    .filter(
        col("tipo_evento").isin(TIPOS_INTERRUPCAO) &
        col("classe_aneel").isin(CLASSES_REGULATORIAS)
    )
    .withColumn(
        "duracao_horas",
        _round(col("duracao_interrupcao_min") / 60, 4)
    )
    .withColumn("ano",  year(col("ts_evento")))
    .withColumn("mes",  month(col("ts_evento")))
)

total_int = df_interrupcoes.count()
print(f"Interrupções regulatórias para DEC/FEC: {total_int:,}")
print(f"\nDistribuição por classe ANEEL:")
df_interrupcoes.groupBy("classe_aneel").count().show()

# COMMAND ----------

# MAGIC %md ## Célula 4 — Cálculo DEC/FEC por alimentador × mês

# COMMAND ----------

# Σ(clientes_afetados × duração) e Σ(clientes_afetados) por alimentador e mês
df_numeradores = (
    df_interrupcoes
    .groupBy("alimentador_id", "subestacao_id", "regional", "municipio", "ano", "mes")
    .agg(
        # Numerador DEC: soma de (clientes × duração em horas)
        _sum(col("clientes_afetados") * col("duracao_horas")).alias("soma_client_horas"),
        # Numerador FEC: soma de clientes afetados
        _sum("clientes_afetados").alias("soma_clientes"),
        # Contagem de interrupções distintas
        count("*").alias("num_interrupcoes"),
        # Métricas auxiliares
        avg("duracao_horas").alias("duracao_media_horas"),
        _max("duracao_horas").alias("duracao_max_horas"),
        _sum(when(col("is_evento_climatico"), 1).otherwise(0))
         .alias("interrupcoes_climaticas"),
        avg("score_criticidade").alias("score_criticidade_medio"),
    )
)

# Join com total de clientes para calcular DEC e FEC
df_dec_fec = (
    df_numeradores
    .join(df_clientes.select("alimentador_id", "total_clientes_conjunto"),
          on="alimentador_id", how="left")
    .withColumn(
        "total_clientes_conjunto",
        # Fallback: se não tiver dados de leituras, usar 1000 como estimativa
        when(col("total_clientes_conjunto").isNull(), lit(1000))
        .otherwise(col("total_clientes_conjunto"))
    )
    # DEC = Σ(Ca × t) / Cc
    .withColumn(
        "dec_horas",
        _round(col("soma_client_horas") / col("total_clientes_conjunto"), 4)
    )
    # FEC = Σ(Ca) / Cc
    .withColumn(
        "fec_vezes",
        _round(col("soma_clientes") / col("total_clientes_conjunto"), 4)
    )
    # DEC em minutos (mais fácil de interpretar operacionalmente)
    .withColumn(
        "dec_minutos",
        _round(col("dec_horas") * 60, 2)
    )
)

print(f"Combinações alimentador×mês calculadas: {df_dec_fec.count()}")
df_dec_fec.select(
    "alimentador_id", "regional", "ano", "mes",
    "num_interrupcoes", "dec_horas", "fec_vezes",
    "total_clientes_conjunto"
).orderBy("dec_horas", ascending=False).show(10)

# COMMAND ----------

# MAGIC %md ## Célula 5 — Limites regulatórios ANEEL e flag de violação
# MAGIC
# MAGIC Limites de DEC e FEC variam por distribuidora e são definidos anualmente
# MAGIC pela ANEEL. Usamos valores de referência típicos para MG.

# COMMAND ----------

# Limites DEC/FEC ANEEL — referência para distribuidoras de MG
# Fonte: PRODIST Módulo 8 — valores típicos para conjuntos de média tensão
# Alimentadores urbanos têm limites mais rígidos que rurais
LIMITE_DEC_URBANO_HORAS  = 8.0    # horas/mês
LIMITE_DEC_RURAL_HORAS   = 16.0   # horas/mês
LIMITE_FEC_URBANO_VEZES  = 6.0    # interrupções/mês
LIMITE_FEC_RURAL_VEZES   = 10.0   # interrupções/mês

df_gold = (
    df_dec_fec

    # Tipo de área baseado no padrão de ID do alimentador
    # (AL01-AL03 = urbano, AL04-AL05 = suburbano, AL06 = rural)
    .withColumn(
        "tipo_area",
        when(col("alimentador_id").endswith("AL01") |
             col("alimentador_id").endswith("AL02") |
             col("alimentador_id").endswith("AL03"), "URBANA")
        .when(col("alimentador_id").endswith("AL04") |
              col("alimentador_id").endswith("AL05"), "SUBURBANA")
        .otherwise("RURAL")
    )

    # Limites aplicáveis por tipo de área
    .withColumn(
        "limite_dec_horas",
        when(col("tipo_area") == "URBANA",    lit(LIMITE_DEC_URBANO_HORAS))
        .when(col("tipo_area") == "SUBURBANA", lit(LIMITE_DEC_URBANO_HORAS * 1.5))
        .otherwise(lit(LIMITE_DEC_RURAL_HORAS))
    )
    .withColumn(
        "limite_fec_vezes",
        when(col("tipo_area") == "URBANA",    lit(LIMITE_FEC_URBANO_VEZES))
        .when(col("tipo_area") == "SUBURBANA", lit(LIMITE_FEC_URBANO_VEZES * 1.5))
        .otherwise(lit(LIMITE_FEC_RURAL_VEZES))
    )

    # Flags de violação regulatória
    .withColumn("violacao_dec", col("dec_horas") > col("limite_dec_horas"))
    .withColumn("violacao_fec", col("fec_vezes") > col("limite_fec_vezes"))
    .withColumn("violacao_alguma", col("violacao_dec") | col("violacao_fec"))

    # % de utilização do limite (quanto do limite foi consumido)
    .withColumn(
        "pct_limite_dec",
        _round(col("dec_horas") / col("limite_dec_horas") * 100, 1)
    )
    .withColumn(
        "pct_limite_fec",
        _round(col("fec_vezes") / col("limite_fec_vezes") * 100, 1)
    )

    # Semáforo regulatório
    .withColumn(
        "semaforo_dec",
        when(col("pct_limite_dec") >= 100, "VERMELHO")
        .when(col("pct_limite_dec") >= 80,  "AMARELO")
        .otherwise("VERDE")
    )
    .withColumn(
        "semaforo_fec",
        when(col("pct_limite_fec") >= 100, "VERMELHO")
        .when(col("pct_limite_fec") >= 80,  "AMARELO")
        .otherwise("VERDE")
    )

    # Controle Gold
    .withColumn("versao_gold", lit("1.0.0"))
    .drop("soma_client_horas", "soma_clientes")
)

# Salvar Gold
(
    df_gold
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("ano", "mes", "regional")
    .saveAsTable(f"{CATALOG}.{SCHEMA_GOLD}.kpi_dec_fec_mensal")
)

total_gold = spark.table(f"{CATALOG}.{SCHEMA_GOLD}.kpi_dec_fec_mensal").count()
print(f"Tabela Gold salva: {CATALOG}.{SCHEMA_GOLD}.kpi_dec_fec_mensal")
print(f"Total registros  : {total_gold:,}")

# COMMAND ----------

# MAGIC %md ## Célula 6 — Análise dos KPIs Gold

# COMMAND ----------

from pyspark.sql.functions import count, sum as _sum, avg, round as _round, col, when

df_g = spark.table(f"{CATALOG}.{SCHEMA_GOLD}.kpi_dec_fec_mensal")

print("=== Resumo geral DEC/FEC ===")
df_g.agg(
    avg("dec_horas").alias("dec_medio_horas"),
    avg("fec_vezes").alias("fec_medio_vezes"),
    avg("dec_minutos").alias("dec_medio_minutos"),
    _sum(when(col("violacao_dec"), 1).otherwise(0)).alias("violacoes_dec"),
    _sum(when(col("violacao_fec"), 1).otherwise(0)).alias("violacoes_fec"),
    _sum(when(col("violacao_alguma"), 1).otherwise(0)).alias("violacoes_total"),
    count("*").alias("total_registros"),
).show()

print("=== Semáforo DEC por regional ===")
df_g.groupBy("regional", "semaforo_dec").count().orderBy("regional", "semaforo_dec").show()

print("=== Top 10 alimentadores com maior DEC ===")
(
    df_g
    .orderBy("dec_horas", ascending=False)
    .select(
        "alimentador_id", "regional", "municipio", "tipo_area",
        "dec_horas", "dec_minutos", "fec_vezes",
        "limite_dec_horas", "pct_limite_dec", "semaforo_dec",
        "violacao_dec", "num_interrupcoes"
    )
    .limit(10)
    .show(truncate=False)
)

print("=== Violações por tipo de área ===")
(
    df_g
    .groupBy("tipo_area")
    .agg(
        count("*").alias("total"),
        _sum(when(col("violacao_dec"), 1).otherwise(0)).alias("violacoes_dec"),
        _sum(when(col("violacao_fec"), 1).otherwise(0)).alias("violacoes_fec"),
        _round(avg("dec_horas"), 3).alias("dec_medio"),
        _round(avg("fec_vezes"), 3).alias("fec_medio"),
    )
    .orderBy("tipo_area")
    .show()
)

print("=== DEC/FEC médio por regional ===")
(
    df_g
    .groupBy("regional")
    .agg(
        _round(avg("dec_horas"), 4).alias("dec_medio_horas"),
        _round(avg("fec_vezes"), 4).alias("fec_medio_vezes"),
        _round(avg("pct_limite_dec"), 1).alias("pct_limite_dec_medio"),
        count("*").alias("alimentadores"),
        _sum(when(col("violacao_alguma"), 1).otherwise(0)).alias("violacoes"),
    )
    .orderBy("dec_medio_horas", ascending=False)
    .show()
)

# COMMAND ----------

# MAGIC %md ## Célula 7 — SQL analítico

# COMMAND ----------

spark.sql("""
SELECT
    regional,
    tipo_area,
    semaforo_dec,
    COUNT(*)                              AS alimentadores,
    ROUND(AVG(dec_horas), 3)              AS dec_medio_horas,
    ROUND(AVG(fec_vezes), 3)              AS fec_medio_vezes,
    ROUND(AVG(pct_limite_dec), 1)         AS uso_limite_dec_pct,
    SUM(CASE WHEN violacao_dec THEN 1 ELSE 0 END) AS violacoes_dec,
    SUM(num_interrupcoes)                 AS total_interrupcoes,
    SUM(interrupcoes_climaticas)          AS interrupcoes_climaticas
FROM adms.gold.kpi_dec_fec_mensal
GROUP BY regional, tipo_area, semaforo_dec
ORDER BY dec_medio_horas DESC
""").show(30, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Consultas úteis
# MAGIC ```sql
# MAGIC -- Alimentadores em violação DEC
# MAGIC SELECT alimentador_id, municipio, dec_horas, limite_dec_horas, pct_limite_dec
# MAGIC FROM adms.gold.kpi_dec_fec_mensal
# MAGIC WHERE violacao_dec = true
# MAGIC ORDER BY pct_limite_dec DESC;
# MAGIC
# MAGIC -- DEC acumulado por subestação
# MAGIC SELECT subestacao_id, regional, SUM(dec_horas) as dec_acumulado
# MAGIC FROM adms.gold.kpi_dec_fec_mensal
# MAGIC GROUP BY subestacao_id, regional
# MAGIC ORDER BY dec_acumulado DESC;
# MAGIC ```
