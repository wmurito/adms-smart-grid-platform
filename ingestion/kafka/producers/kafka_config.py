"""
Configuração centralizada do Kafka — ADMS Smart Grid Platform
Detecta automaticamente se está usando Confluent Cloud (SASL_SSL)
ou Kafka local (PLAINTEXT) baseado nas variáveis de ambiente.

Uso:
    from kafka_config import get_producer_config, get_consumer_config, is_confluent_cloud
"""

import os
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Detecção automática do modo
# ---------------------------------------------------------------------------

def is_confluent_cloud() -> bool:
    """Retorna True se as variáveis do Confluent Cloud estão configuradas."""
    return (
        os.getenv("KAFKA_SASL_USERNAME", "") != "" and
        os.getenv("KAFKA_SASL_PASSWORD", "") != "" and
        "confluent.cloud" in os.getenv("KAFKA_BOOTSTRAP_SERVERS", "")
    )


def get_bootstrap_servers() -> str:
    return os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


# ---------------------------------------------------------------------------
# Configurações do Producer
# ---------------------------------------------------------------------------

def get_producer_config(
    acks: str = "all",
    compression: str = "gzip",
    batch_size: int = 16384,
    linger_ms: int = 10,
) -> dict:
    """
    Retorna configuração completa para KafkaProducer.
    Funciona tanto para Confluent Cloud quanto para Kafka local.
    """
    config = {
        "bootstrap_servers": get_bootstrap_servers(),
        "acks": acks,
        "retries": 3,
        "retry_backoff_ms": 500,
        "compression_type": compression,
        "batch_size": batch_size,
        "linger_ms": linger_ms,
        "request_timeout_ms": 30000,
        "connections_max_idle_ms": 540000,
    }

    if is_confluent_cloud():
        config.update({
            "security_protocol": "SASL_SSL",
            "sasl_mechanism": "PLAIN",
            "sasl_plain_username": os.getenv("KAFKA_SASL_USERNAME"),
            "sasl_plain_password": os.getenv("KAFKA_SASL_PASSWORD"),
            "ssl_check_hostname": True,
        })

    return config


# ---------------------------------------------------------------------------
# Configurações do Consumer
# ---------------------------------------------------------------------------

def get_consumer_config(
    group_id: str,
    auto_offset_reset: str = "earliest",
    enable_auto_commit: bool = False,
) -> dict:
    """
    Retorna configuração completa para KafkaConsumer.
    """
    config = {
        "bootstrap_servers": get_bootstrap_servers(),
        "group_id": group_id,
        "auto_offset_reset": auto_offset_reset,
        "enable_auto_commit": enable_auto_commit,
        "session_timeout_ms": 30000,
        "heartbeat_interval_ms": 10000,
        "max_poll_records": 500,
        "fetch_max_wait_ms": 500,
    }

    if is_confluent_cloud():
        config.update({
            "security_protocol": "SASL_SSL",
            "sasl_mechanism": "PLAIN",
            "sasl_plain_username": os.getenv("KAFKA_SASL_USERNAME"),
            "sasl_plain_password": os.getenv("KAFKA_SASL_PASSWORD"),
            "ssl_check_hostname": True,
        })

    return config


# ---------------------------------------------------------------------------
# Configuração para Spark Structured Streaming (Databricks)
# Retorna as opções no formato que o Spark readStream espera
# ---------------------------------------------------------------------------

def get_spark_kafka_options(topic: str, group_id: str = "databricks-bronze") -> dict:
    """
    Retorna opções de conexão Kafka para uso no Spark readStream.
    Usado nos notebooks Bronze do Databricks.
    """
    bootstrap = get_bootstrap_servers()

    options = {
        "kafka.bootstrap.servers": bootstrap,
        "subscribe": topic,
        "startingOffsets": "earliest",
        "failOnDataLoss": "false",
        "kafka.request.timeout.ms": "30000",
        "kafka.session.timeout.ms": "30000",
        "maxOffsetsPerTrigger": "10000",
    }

    if is_confluent_cloud():
        jaas = (
            "org.apache.kafka.common.security.plain.PlainLoginModule required "
            f'username="{os.getenv("KAFKA_SASL_USERNAME")}" '
            f'password="{os.getenv("KAFKA_SASL_PASSWORD")}";'
        )
        options.update({
            "kafka.security.protocol": "SASL_SSL",
            "kafka.sasl.mechanism": "PLAIN",
            "kafka.sasl.jaas.config": jaas,
        })

    return options


# ---------------------------------------------------------------------------
# Informações de diagnóstico
# ---------------------------------------------------------------------------

def print_config_info():
    modo = "Confluent Cloud (SASL_SSL)" if is_confluent_cloud() else "Kafka Local (PLAINTEXT)"
    bootstrap = get_bootstrap_servers()
    username = os.getenv("KAFKA_SASL_USERNAME", "N/A")

    print(f"{'='*55}")
    print(f"  Kafka Config — ADMS Smart Grid Platform")
    print(f"{'='*55}")
    print(f"  Modo       : {modo}")
    print(f"  Bootstrap  : {bootstrap}")
    if is_confluent_cloud():
        print(f"  API Key    : {username[:8]}****")
    print(f"{'='*55}")


if __name__ == "__main__":
    print_config_info()

    # Teste de conectividade
    print("\nTestando conectividade...")
    from kafka import KafkaAdminClient
    try:
        cfg = get_producer_config()
        admin_cfg = {
            "bootstrap_servers": cfg["bootstrap_servers"],
            "request_timeout_ms": 10000,
        }
        if is_confluent_cloud():
            admin_cfg.update({
                "security_protocol": "SASL_SSL",
                "sasl_mechanism": "PLAIN",
                "sasl_plain_username": os.getenv("KAFKA_SASL_USERNAME"),
                "sasl_plain_password": os.getenv("KAFKA_SASL_PASSWORD"),
            })

        admin = KafkaAdminClient(**admin_cfg)
        topics = admin.list_topics()
        adms_topics = [t for t in topics if t.startswith("adms.")]
        print(f"Conectado! Tópicos ADMS encontrados: {adms_topics}")
        admin.close()
    except Exception as e:
        print(f"Erro de conexão: {e}")