# Databricks notebook source
# MAGIC %md
# MAGIC # Simulador ADMS — Runner para Databricks Workflows
# MAGIC
# MAGIC Wrapper que executa o simulador de eventos ADMS como task de um Job.
# MAGIC Recebe parâmetros via base_parameters do Job e roda por duração configurável.
# MAGIC
# MAGIC **Parâmetros do Job:**
# MAGIC - `duration_seconds`: duração da simulação (padrão: 300)
# MAGIC - `eventos_rate`: eventos de rede por segundo (padrão: 5)
# MAGIC - `medidores_rate`: leituras de medidor por segundo (padrão: 50)

# COMMAND ----------

import subprocess
import sys
import os

# Ler parâmetros do Job (base_parameters)
duration  = int(dbutils.widgets.get("duration_seconds")) if "duration_seconds" in [w.name for w in dbutils.widgets.getAll()] else 300
ev_rate   = float(dbutils.widgets.get("eventos_rate"))   if "eventos_rate"   in [w.name for w in dbutils.widgets.getAll()] else 5.0
med_rate  = float(dbutils.widgets.get("medidores_rate")) if "medidores_rate" in [w.name for w in dbutils.widgets.getAll()] else 50.0

print(f"Iniciando simulador ADMS:")
print(f"  duration  : {duration}s")
print(f"  ev_rate   : {ev_rate} ev/s")
print(f"  med_rate  : {med_rate} leit/s")

# COMMAND ----------

# Configurações Confluent Cloud (usar Databricks Secrets em produção)
CONFLUENT_BOOTSTRAP = "pkc-oxqxx9.us-east-1.aws.confluent.cloud:9092"
CONFLUENT_KEY       = "5GDS2QXL5XJZCZCU"
CONFLUENT_SECRET    = "cfltvJELpk5YL3vNQ3Q7VECZrp/uoU3J6DH3xaYIS0+keskv24VeF5cEiFYmzylQ"

os.environ["KAFKA_BOOTSTRAP_SERVERS"] = CONFLUENT_BOOTSTRAP
os.environ["KAFKA_SASL_USERNAME"]     = CONFLUENT_KEY
os.environ["KAFKA_SASL_PASSWORD"]     = CONFLUENT_SECRET
os.environ["KAFKA_SECURITY_PROTOCOL"] = "SASL_SSL"
os.environ["KAFKA_SASL_MECHANISM"]    = "PLAIN"

# COMMAND ----------

# Instalar dependências se necessário
%pip install kafka-python loguru python-dotenv --quiet

# COMMAND ----------

# Importar e rodar simulador inline
# (o código está replicado aqui para rodar sem dependência de arquivo local)
import json, random, signal, time, uuid
from datetime import datetime, timezone
from kafka import KafkaProducer
from loguru import logger

def get_producer():
    return KafkaProducer(
        bootstrap_servers=CONFLUENT_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        security_protocol="SASL_SSL",
        sasl_mechanism="PLAIN",
        sasl_plain_username=CONFLUENT_KEY,
        sasl_plain_password=CONFLUENT_SECRET,
        acks=1,
        compression_type="gzip",
        batch_size=16384,
        linger_ms=20,
    )

SUBESTACOES = [f"SE{i:02d}" for i in range(1, 21)]
ALIMENTADORES = [f"SE{s:02d}-AL{a:02d}" for s in range(1, 21) for a in range(1, 7)]
TIPOS_EVENTO = [
    "FALHA_EQUIPAMENTO", "RELIGAMENTO_AUTOMATICO", "MANOBRA_PLANEJADA",
    "MANOBRA_EMERGENCIAL", "ANOMALIA_TENSAO", "ANOMALIA_CORRENTE",
    "INTERRUPCAO_INICIO", "INTERRUPCAO_FIM", "ALARME_SOBRECARGA", "ALARME_TEMPERATURA"
]
SEVERIDADES = ["CRITICO", "ALTO", "MEDIO", "BAIXO"]

rng = random.Random(42)

def gerar_evento():
    al = rng.choice(ALIMENTADORES)
    se = al[:4]
    tipo = rng.choices(TIPOS_EVENTO, weights=[20,15,10,8,15,10,10,7,3,2], k=1)[0]
    ts = datetime.now(timezone.utc).isoformat()
    return {
        "evento_id":            f"EVT-{uuid.uuid4().hex[:12].upper()}",
        "tipo_evento":          tipo,
        "severidade":           rng.choice(SEVERIDADES),
        "status":               "ATIVO",
        "subestacao_id":        se,
        "subestacao_nome":      f"SE {se}",
        "alimentador_id":       al,
        "alimentador_nome":     f"Alimentador {al}",
        "equipamento_id":       f"{al}-EQ001",
        "tipo_equipamento":     rng.choice(["RELIGADOR","CHAVE_FACA","TRANSFORMADOR"]),
        "regional":             "Zona da Mata",
        "municipio":            "Cataguases",
        "tensao_kv":            round(rng.uniform(11.0, 15.0), 3),
        "corrente_a":           round(rng.uniform(50, 800), 1),
        "potencia_mw":          round(rng.uniform(0.5, 10.0), 3),
        "fator_potencia":       round(rng.uniform(0.85, 0.98), 3),
        "clientes_afetados":    rng.randint(0, 8000),
        "classe_interrupcao":   rng.choice(["NAO_PLANEJADA","PLANEJADA","CASO_FORTUITO","EMERGENCIAL"]),
        "duracao_estimada_min": rng.randint(0, 180),
        "temperatura_c":        round(rng.uniform(18, 35), 1),
        "umidade_pct":          round(rng.uniform(50, 95), 1),
        "precipitacao_mm":      round(rng.uniform(0, 50), 1) if rng.random() < 0.2 else 0.0,
        "velocidade_vento_kmh": round(rng.uniform(0, 80), 1),
        "timestamp_evento":     ts,
        "timestamp_ingestao":   ts,
        "fonte_sistema":        "ADMS-RUNNER",
        "versao_schema":        "1.0.0",
    }

producer = get_producer()
inicio = time.time()
enviados = 0
erros = 0
intervalo = 1.0 / ev_rate

logger.info(f"Simulador iniciado — {ev_rate} ev/s por {duration}s")

while (time.time() - inicio) < duration:
    t0 = time.time()
    try:
        ev = gerar_evento()
        producer.send("adms.eventos.rede", key=ev["subestacao_id"], value=ev)
        enviados += 1
    except Exception as e:
        erros += 1
        logger.error(f"Erro: {e}")

    elapsed = time.time() - t0
    sleep = max(0, intervalo - elapsed)
    if sleep > 0:
        time.sleep(sleep)

producer.flush()
producer.close()

elapsed_total = round(time.time() - inicio, 1)
taxa_real = round(enviados / elapsed_total, 1)

print(f"\nSimulador encerrado:")
print(f"  Duração  : {elapsed_total}s")
print(f"  Enviados : {enviados:,} eventos")
print(f"  Taxa real: {taxa_real} ev/s")
print(f"  Erros    : {erros}")

if erros > 0:
    raise Exception(f"Simulador encerrou com {erros} erros de publicação Kafka")
