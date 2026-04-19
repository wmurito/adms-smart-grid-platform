# Databricks notebook source
# MAGIC %md
# MAGIC # Gold — Perdas Comerciais e Técnicas por Regional
# MAGIC
# MAGIC Estima perdas comerciais cruzando dados de:
# MAGIC - Leituras de medidores com suspeita de fraude (Silver AMI)
# MAGIC - Eventos de interrupção não faturada (Silver Eventos)
# MAGIC - Energia não fornecida durante interrupções
# MAGIC
# MAGIC **Métricas calculadas:**
# MAGIC - Energia estimada perdida por fraude (kWh/mês por alimentador)
# MAGIC - Energia não fornecida (ENF) durante interrupções
# MAGIC - Receita estimada não realizada (R$)
# MAGIC - Ranking de alimentadores com maior risco de perda
# MAGIC
# MAGIC **Fonte:** `adms.silver.leituras_medidor_norm` + `adms.silver.eventos_rede_enriquecido`
# MAGIC **Destino:** `adms.gold.perdas_comerciais_regional`

# COMMAND ----------

# MAGIC %md ## Célula 1 — Configuração

# COMMAND ----------

CATALOG       = "adms"
SCHEMA_SILVER = "silver"
SCHEMA_GOLD   = "gold"

# Tarifa média de energia elétrica em R$/kWh (referência ANEEL 2024)
TARIFA_RESIDENCIAL_RS  = 0.75
TARIFA_COMERCIAL_RS    = 0.65
TARIFA_INDUSTRIAL_RS   = 0.45
TARIFA_RURAL_RS        = 0.55
TARIFA_PODER_PUBLICO_RS = 0.60
TARIFA_MEDIA_RS        = 0.65  # fallback

spark.sql(f"USE CATALOG {CATALOG}")

df_leituras = spark.table(f"{CATALOG}.{SCHEMA_SILVER}.leituras_medidor_norm")
df_eventos  = spark.table(f"{CATALOG}.{SCHEMA_SILVER}.eventos_rede_enriquecido")

print(f"Leituras Silver : {df_leituras.count():,}")
print(f"Eventos Silver  : {df_eventos.count():,}")

# COMMAND ----------

# MAGIC %md ## Célula 2 — Perdas por fraude (consumo sub-reportado)

# COMMAND ----------

from pyspark.sql.functions import (
    col, sum as _sum, avg, count, when,
    round as _round, lit
)

# Tarifa por tipo de medidor
def tarifa_por_tipo(tipo_col):
    return (
        when(tipo_col == "RESIDENCIAL",   lit(TARIFA_RESIDENCIAL_RS))
        .when(tipo_col == "COMERCIAL",    lit(TARIFA_COMERCIAL_RS))
        .when(tipo_col == "INDUSTRIAL",   lit(TARIFA_INDUSTRIAL_RS))
        .when(tipo_col == "RURAL",        lit(TARIFA_RURAL_RS))
        .when(tipo_col == "PODER_PUBLICO",lit(TARIFA_PODER_PUBLICO_RS))
        .otherwise(lit(TARIFA_MEDIA_RS))
    )

# Suspeitas de fraude por alimentador
df_fraude_alim = (
    df_leituras
    .withColumn("tarifa_rs_kwh", tarifa_por_tipo(col("tipo_medidor")))
    .groupBy("alimentador_id", "subestacao_id", "regional", "municipio")
    .agg(
        # Total de leituras
        count("*").alias("total_leituras"),

        # Medidores suspeitos únicos
        count(when(col("is_suspeita_fraude"), col("medidor_id")))
         .alias("leituras_suspeitas"),

        # Consumo real total (delta ajustado)
        _round(_sum("delta_kwh_ajustado"), 2).alias("kwh_total_reportado"),

        # Estimativa de consumo "desviado":
        # Para leituras suspeitas, estimamos que o consumo real
        # seria 2x o reportado (baseado no fator de fraude do simulador: 20-60%)
        _round(
            _sum(
                when(col("is_suspeita_fraude"),
                     col("delta_kwh_ajustado") * lit(1.5)  # 50% de desvio médio estimado
                ).otherwise(lit(0.0))
            ), 2
        ).alias("kwh_estimado_desviado"),

        # Receita perdida estimada
        _round(
            _sum(
                when(col("is_suspeita_fraude"),
                     col("delta_kwh_ajustado") * lit(0.5) * col("tarifa_rs_kwh")
                ).otherwise(lit(0.0))
            ), 2
        ).alias("receita_perdida_fraude_rs"),

        # Score médio de fraude
        avg("score_fraude").alias("score_fraude_medio"),
    )
    # % de leituras suspeitas
    .withColumn(
        "pct_suspeitas",
        _round(col("leituras_suspeitas") / col("total_leituras") * 100, 1)
    )
)

print(f"Alimentadores com análise de fraude: {df_fraude_alim.count()}")
df_fraude_alim.orderBy("receita_perdida_fraude_rs", ascending=False).show(5)

# COMMAND ----------

# MAGIC %md ## Célula 3 — Energia Não Fornecida (ENF) durante interrupções

# COMMAND ----------

from pyspark.sql.functions import (
    col, sum as _sum, avg, count, when,
    round as _round, lit, countDistinct
)

# ENF = demanda média do alimentador × duração da interrupção
# Estimamos demanda média como 0.5 kW por cliente (perfil residencial misto)
DEMANDA_MEDIA_KW_POR_CLIENTE = 0.5

# Interrupções com clientes afetados
df_enf = (
    df_eventos
    .filter(
        col("tipo_evento").isin(["INTERRUPCAO_INICIO", "FALHA_EQUIPAMENTO"]) &
        col("classe_aneel").isin(["NAO_PLANEJADA", "CASO_FORTUITO", "EMERGENCIAL"])
    )
    .groupBy("alimentador_id", "subestacao_id", "regional", "municipio")
    .agg(
        count("*").alias("num_interrupcoes"),
        _sum("clientes_afetados").alias("soma_clientes_afetados"),
        avg("duracao_interrupcao_min").alias("duracao_media_min"),
        _sum(
            col("clientes_afetados") *
            (col("duracao_interrupcao_min") / 60) *
            lit(DEMANDA_MEDIA_KW_POR_CLIENTE)
        ).alias("enf_kwh"),  # Energia não fornecida em kWh
    )
    .withColumn(
        "enf_receita_perdida_rs",
        _round(col("enf_kwh") * lit(TARIFA_MEDIA_RS), 2)
    )
    .withColumn(
        "enf_kwh",
        _round(col("enf_kwh"), 2)
    )
)

print(f"Alimentadores com ENF calculado: {df_enf.count()}")
df_enf.orderBy("enf_kwh", ascending=False).show(5)

# COMMAND ----------

# MAGIC %md ## Célula 4 — Consolidação e persistência Gold

# COMMAND ----------

from pyspark.sql.functions import coalesce, lit, col, round as _round

# Join fraude + ENF por alimentador
df_perdas = (
    df_fraude_alim
    .join(
        df_enf.select(
            "alimentador_id",
            "num_interrupcoes",
            "soma_clientes_afetados",
            "duracao_media_min",
            "enf_kwh",
            "enf_receita_perdida_rs"
        ),
        on="alimentador_id",
        how="left"
    )
    .withColumn("num_interrupcoes",     coalesce(col("num_interrupcoes"),     lit(0)))
    .withColumn("enf_kwh",              coalesce(col("enf_kwh"),              lit(0.0)))
    .withColumn("enf_receita_perdida_rs", coalesce(col("enf_receita_perdida_rs"), lit(0.0)))
    .withColumn("soma_clientes_afetados", coalesce(col("soma_clientes_afetados"), lit(0)))

    # Total de perdas (fraude + ENF)
    .withColumn(
        "total_perda_estimada_rs",
        _round(col("receita_perdida_fraude_rs") + col("enf_receita_perdida_rs"), 2)
    )

    # Classificação do risco de perda
    .withColumn(
        "nivel_risco_perda",
        when(col("total_perda_estimada_rs") >= 50000, "CRITICO")
        .when(col("total_perda_estimada_rs") >= 10000, "ALTO")
        .when(col("total_perda_estimada_rs") >= 1000,  "MEDIO")
        .otherwise("BAIXO")
    )

    .withColumn("versao_gold", lit("1.0.0"))
)

# Persistir
(
    df_perdas
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("regional", "nivel_risco_perda")
    .saveAsTable(f"{CATALOG}.{SCHEMA_GOLD}.perdas_comerciais_regional")
)

total = spark.table(f"{CATALOG}.{SCHEMA_GOLD}.perdas_comerciais_regional").count()
print(f"Tabela Gold salva: {CATALOG}.{SCHEMA_GOLD}.perdas_comerciais_regional")
print(f"Total registros  : {total:,}")

# COMMAND ----------

# MAGIC %md ## Célula 5 — Análise de perdas

# COMMAND ----------

from pyspark.sql.functions import sum as _sum, avg, count, round as _round, col

df_g = spark.table(f"{CATALOG}.{SCHEMA_GOLD}.perdas_comerciais_regional")

print("=== Perdas totais por regional ===")
(
    df_g
    .groupBy("regional")
    .agg(
        _round(_sum("receita_perdida_fraude_rs"), 2).alias("perda_fraude_rs"),
        _round(_sum("enf_receita_perdida_rs"), 2).alias("perda_enf_rs"),
        _round(_sum("total_perda_estimada_rs"), 2).alias("perda_total_rs"),
        _round(_sum("kwh_total_reportado"), 2).alias("kwh_total"),
        _round(_sum("enf_kwh"), 2).alias("enf_total_kwh"),
        count("*").alias("alimentadores"),
    )
    .orderBy("perda_total_rs", ascending=False)
    .show()
)

print("=== Nível de risco por regional ===")
(
    df_g
    .groupBy("regional", "nivel_risco_perda")
    .count()
    .orderBy("regional", "nivel_risco_perda")
    .show()
)

print("=== Top 10 alimentadores com maior perda estimada ===")
(
    df_g
    .select(
        "alimentador_id", "regional", "municipio",
        "leituras_suspeitas", "pct_suspeitas",
        "kwh_estimado_desviado",
        "receita_perdida_fraude_rs",
        "enf_kwh", "enf_receita_perdida_rs",
        "total_perda_estimada_rs",
        "nivel_risco_perda"
    )
    .orderBy("total_perda_estimada_rs", ascending=False)
    .limit(10)
    .show(truncate=False)
)

print("=== Resumo consolidado ===")
(
    df_g
    .agg(
        _round(_sum("total_perda_estimada_rs"), 2).alias("perda_total_rs"),
        _round(_sum("receita_perdida_fraude_rs"), 2).alias("fraude_rs"),
        _round(_sum("enf_receita_perdida_rs"), 2).alias("enf_rs"),
        _round(_sum("enf_kwh"), 2).alias("enf_total_kwh"),
        _round(_sum("kwh_estimado_desviado"), 2).alias("kwh_desviado"),
        count("*").alias("alimentadores_analisados"),
        count(when(col("nivel_risco_perda") == "CRITICO", 1)).alias("risco_critico"),
        count(when(col("nivel_risco_perda") == "ALTO", 1)).alias("risco_alto"),
    )
    .show()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     p.regional,
# MAGIC     p.alimentador_id,
# MAGIC     p.municipio,
# MAGIC     p.nivel_risco_perda,
# MAGIC     ROUND(p.total_perda_estimada_rs, 0)    AS perda_total_rs,
# MAGIC     ROUND(p.receita_perdida_fraude_rs, 0)  AS perda_fraude_rs,
# MAGIC     ROUND(p.enf_receita_perdida_rs, 0)     AS perda_enf_rs,
# MAGIC     p.pct_suspeitas                        AS pct_fraude,
# MAGIC     s.indice_saude,
# MAGIC     s.classificacao_saude
# MAGIC FROM adms.gold.perdas_comerciais_regional p
# MAGIC LEFT JOIN adms.gold.saude_ativo_alimentador s
# MAGIC     ON p.alimentador_id = s.alimentador_id
# MAGIC WHERE p.nivel_risco_perda IN ('CRITICO', 'ALTO')
# MAGIC ORDER BY p.total_perda_estimada_rs DESC
# MAGIC LIMIT 15
