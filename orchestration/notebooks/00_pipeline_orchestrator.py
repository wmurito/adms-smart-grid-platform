# Databricks notebook source
# MAGIC %md
# MAGIC # ADMS Smart Grid — Pipeline Orchestrator
# MAGIC
# MAGIC Orquestrador principal do pipeline Medallion Bronze → Silver → Gold.
# MAGIC
# MAGIC **Execução:** Manual via Databricks Workflow Job ou via `Run All`
# MAGIC
# MAGIC | Etapa | Notebook | Destino |
# MAGIC |---|---|---|
# MAGIC | 1 | `01_bronze_eventos_rede` | `adms.bronze.eventos_rede_raw` |
# MAGIC | 2 | `02_bronze_leituras_medidor` | `adms.bronze.leituras_medidor_raw` |
# MAGIC | 3 | `03_silver_eventos_rede` | `adms.silver.eventos_rede_enriquecido` |
# MAGIC | 4 | `04_silver_leituras_medidor` | `adms.silver.leituras_medidor_norm` |
# MAGIC | 5 | `05_gold_kpi_dec_fec` | `adms.gold.kpi_dec_fec_mensal` |
# MAGIC | 6 | `06_gold_saude_ativos` | `adms.gold.saude_ativo_alimentador` |
# MAGIC | 7 | `07_gold_perdas_comerciais` | `adms.gold.perdas_comerciais_regional` |

# COMMAND ----------

# MAGIC %md ## 1 — Parâmetros do Orquestrador

# COMMAND ----------

import json
from datetime import datetime

# ── Parâmetros recebidos via Databricks Workflow (widgets) ──────────────────
dbutils.widgets.text("run_bronze",   "true",  "Executar Bronze?")
dbutils.widgets.text("run_silver",   "true",  "Executar Silver?")
dbutils.widgets.text("run_gold",     "true",  "Executar Gold?")
dbutils.widgets.text("timeout_seg",  "1800",  "Timeout por notebook (seg)")

RUN_BRONZE  = dbutils.widgets.get("run_bronze").lower()  == "true"
RUN_SILVER  = dbutils.widgets.get("run_silver").lower()  == "true"
RUN_GOLD    = dbutils.widgets.get("run_gold").lower()    == "true"
TIMEOUT_SEG = int(dbutils.widgets.get("timeout_seg"))

INICIO_PIPELINE = datetime.now()
print(f"{'='*60}")
print(f"  ADMS Smart Grid — Pipeline Orchestrator")
print(f"  Início : {INICIO_PIPELINE.strftime('%Y-%m-%d %H:%M:%S')}")
print(f"  Bronze : {RUN_BRONZE}")
print(f"  Silver : {RUN_SILVER}")
print(f"  Gold   : {RUN_GOLD}")
print(f"  Timeout: {TIMEOUT_SEG}s por notebook")
print(f"{'='*60}")

# COMMAND ----------

# MAGIC %md ## 2 — Utilitários de Execução

# COMMAND ----------

import traceback

resultados = []   # log global de execuções

def executar_notebook(nome: str, path_relativo: str, argumentos: dict = {}) -> dict:
    """
    Executa um notebook filho e retorna um dict com status e métricas.

    Parâmetros
    ----------
    nome            : Nome amigável do notebook (para logs)
    path_relativo   : Caminho relativo ao diretório deste notebook
    argumentos      : Parâmetros passados pro notebook filho via widgets

    Retorna
    -------
    dict com: nome, status, duracao_seg, output, erro
    """
    inicio = datetime.now()
    print(f"\n▶  Iniciando: {nome}")
    print(f"   Path   : {path_relativo}")
    print(f"   Params : {argumentos}")

    try:
        saida = dbutils.notebook.run(
            path_relativo,
            timeout_seconds=TIMEOUT_SEG,
            arguments=argumentos
        )
        fim = datetime.now()
        duracao = (fim - inicio).total_seconds()
        resultado = {
            "nome":         nome,
            "status":       "✅ SUCESSO",
            "duracao_seg":  round(duracao, 1),
            "output":       saida,
            "erro":         None,
        }
        print(f"   ✅ Concluído em {duracao:.0f}s")

    except Exception as e:
        fim = datetime.now()
        duracao = (fim - inicio).total_seconds()
        resultado = {
            "nome":         nome,
            "status":       "❌ FALHA",
            "duracao_seg":  round(duracao, 1),
            "output":       None,
            "erro":         str(e),
        }
        print(f"   ❌ FALHOU após {duracao:.0f}s")
        print(f"   Erro: {str(e)[:300]}")

    resultados.append(resultado)
    return resultado


def checar_falha(resultado: dict, abortarPipeline: bool = True):
    """Para o pipeline se o notebook falhou e abortarPipeline=True."""
    if resultado["status"].startswith("❌") and abortarPipeline:
        raise Exception(
            f"Pipeline abortado! Notebook '{resultado['nome']}' falhou.\n"
            f"Erro: {resultado['erro']}"
        )

# COMMAND ----------

# MAGIC %md ## 3 — Camada Bronze (Ingestão Kafka → Delta)

# COMMAND ----------

if RUN_BRONZE:
    print("\n" + "="*60)
    print("  BRONZE — Ingestão Kafka → Delta Lake")
    print("="*60)

    # Bronze 1: Eventos de Rede
    r1 = executar_notebook(
        nome           = "Bronze: Eventos de Rede",
        path_relativo  = "../bronze/notebooks/01_bronze_eventos_rede",
        argumentos     = {}
    )
    checar_falha(r1)

    # Bronze 2: Leituras de Medidor
    r2 = executar_notebook(
        nome           = "Bronze: Leituras de Medidor",
        path_relativo  = "../bronze/notebooks/02_bronze_leituras_medidor",
        argumentos     = {}
    )
    checar_falha(r2)

else:
    print("\n⏭  Camada Bronze IGNORADA (run_bronze=false)")

# COMMAND ----------

# MAGIC %md ## 4 — Camada Silver (Transformação + Enriquecimento)

# COMMAND ----------

if RUN_SILVER:
    print("\n" + "="*60)
    print("  SILVER — Transformação + Enriquecimento")
    print("="*60)

    # Silver 3: Eventos de Rede enriquecidos
    r3 = executar_notebook(
        nome           = "Silver: Eventos de Rede Enriquecido",
        path_relativo  = "../silver/notebooks/03_silver_eventos_rede",
        argumentos     = {}
    )
    checar_falha(r3)

    # Silver 4: Leituras de Medidor normalizadas
    r4 = executar_notebook(
        nome           = "Silver: Leituras de Medidor Normalizadas",
        path_relativo  = "../silver/notebooks/04_silver_leituras_medidor",
        argumentos     = {}
    )
    checar_falha(r4)

else:
    print("\n⏭  Camada Silver IGNORADA (run_silver=false)")

# COMMAND ----------

# MAGIC %md ## 5 — Camada Gold (KPIs Regulatórios / Negócio)

# COMMAND ----------

if RUN_GOLD:
    print("\n" + "="*60)
    print("  GOLD — KPIs Regulatórios e de Negócio")
    print("="*60)

    # Gold 5: KPI DEC/FEC (ANEEL PRODIST)
    r5 = executar_notebook(
        nome           = "Gold: KPI DEC/FEC Mensal",
        path_relativo  = "../gold/notebooks/05_gold_kpi_dec_fec",
        argumentos     = {}
    )
    checar_falha(r5)

    # Gold 6: Saúde dos Ativos por Alimentador
    r6 = executar_notebook(
        nome           = "Gold: Saúde dos Ativos",
        path_relativo  = "../gold/notebooks/06_gold_saude_ativos",
        argumentos     = {}
    )
    checar_falha(r6)

    # Gold 7: Perdas Comerciais Regionais
    r7 = executar_notebook(
        nome           = "Gold: Perdas Comerciais Regionais",
        path_relativo  = "../gold/notebooks/07_gold_perdas_comerciais",
        argumentos     = {}
    )
    checar_falha(r7)

else:
    print("\n⏭  Camada Gold IGNORADA (run_gold=false)")

# COMMAND ----------

# MAGIC %md ## 6 — Sumário de Execução

# COMMAND ----------

fim_pipeline = datetime.now()
duracao_total = (fim_pipeline - INICIO_PIPELINE).total_seconds()

print(f"\n{'='*60}")
print(f"  SUMÁRIO DE EXECUÇÃO — ADMS Pipeline")
print(f"{'='*60}")
print(f"  Início  : {INICIO_PIPELINE.strftime('%Y-%m-%d %H:%M:%S')}")
print(f"  Fim     : {fim_pipeline.strftime('%Y-%m-%d %H:%M:%S')}")
print(f"  Duração : {duracao_total/60:.1f} minutos\n")

total_ok   = sum(1 for r in resultados if r["status"].startswith("✅"))
total_fail = sum(1 for r in resultados if r["status"].startswith("❌"))

for r in resultados:
    print(f"  {r['status']}  {r['nome']:<45}  {r['duracao_seg']:>6.0f}s")

print(f"\n  Notebooks OK  : {total_ok}")
print(f"  Notebooks FAIL: {total_fail}")
print(f"{'='*60}")

# Logar sumário como JSON (visível nos logs do Databricks Job)
sumario = {
    "inicio":         INICIO_PIPELINE.isoformat(),
    "fim":            fim_pipeline.isoformat(),
    "duracao_min":    round(duracao_total / 60, 2),
    "notebooks_ok":   total_ok,
    "notebooks_fail": total_fail,
    "detalhes":       resultados,
}
print("\nJSON_SUMARIO:", json.dumps(sumario, ensure_ascii=False, indent=2))

# Falhar o Job se algum notebook falhou
if total_fail > 0:
    raise Exception(f"Pipeline concluído com {total_fail} falha(s). Verifique os logs acima.")

dbutils.notebook.exit(json.dumps({"status": "OK", "duracao_min": round(duracao_total/60, 2)}))
