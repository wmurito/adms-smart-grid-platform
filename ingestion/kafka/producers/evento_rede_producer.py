"""
Produtor Kafka — Eventos de Rede Elétrica
ADMS Smart Grid Platform

Simula o sistema ADMS/SCADA gerando eventos da rede e publicando
no tópico Kafka: adms.eventos.rede

Uso:
    python evento_rede_producer.py                     # produção contínua
    python evento_rede_producer.py --dry-run           # sem Kafka (só imprime)
    python evento_rede_producer.py --rate 5            # 5 eventos/segundo
    python evento_rede_producer.py --duration 60       # roda por 60 segundos
    python evento_rede_producer.py --dry-run --rate 5 --duration 10
"""

import argparse
import json
import logging
import random
import sys
import time
from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# Importações internas do projeto
# ---------------------------------------------------------------------------

# topologia.py está no mesmo diretório
from topologia import (
    ALIMENTADORES,
    ALIMENTADOR_MAP,
    SUBESTACAO_MAP,
    EQUIPAMENTOS,
    EQUIPAMENTO_MAP,
)

# schemas.py — importado do mesmo diretório (produtores/)
from schemas import (
    EventoRede,
    TipoEvento,
    SeveridadeEvento,
    StatusEvento,
    ClasseInterrupcao,
    novo_evento_id,
    timestamp_utc_agora,
    Alarme,
    novo_alarme_id,
)

# ---------------------------------------------------------------------------
# Configuração de logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("evento_rede_producer")

# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------

TOPICO = "adms.eventos.rede"

KAFKA_CONFIG_PADRAO = {
    "bootstrap.servers": "localhost:9092",
    "client.id": "adms-evento-rede-producer",
    "acks": "all",
    "retries": 5,
    "retry.backoff.ms": 300,
    "compression.type": "lz4",
    "linger.ms": 5,
    "batch.size": 32768,
}

# Distribuição de probabilidade dos tipos de evento
_TIPOS_PESOS = {
    TipoEvento.FALHA_EQUIPAMENTO:      0.15,
    TipoEvento.RELIGAMENTO_AUTOMATICO: 0.10,
    TipoEvento.MANOBRA_PLANEJADA:      0.12,
    TipoEvento.MANOBRA_EMERGENCIAL:    0.08,
    TipoEvento.ANOMALIA_TENSAO:        0.18,
    TipoEvento.ANOMALIA_CORRENTE:      0.12,
    TipoEvento.INTERRUPÇÃO_INICIO:     0.10,
    TipoEvento.INTERRUPÇÃO_FIM:        0.07,
    TipoEvento.ALARME_SOBRECARGA:      0.05,
    TipoEvento.ALARME_TEMPERATURA:     0.03,
}

_SEVERIDADE_POR_TIPO = {
    TipoEvento.FALHA_EQUIPAMENTO:      SeveridadeEvento.CRITICO,
    TipoEvento.RELIGAMENTO_AUTOMATICO: SeveridadeEvento.ALTO,
    TipoEvento.MANOBRA_PLANEJADA:      SeveridadeEvento.BAIXO,
    TipoEvento.MANOBRA_EMERGENCIAL:    SeveridadeEvento.ALTO,
    TipoEvento.ANOMALIA_TENSAO:        SeveridadeEvento.MEDIO,
    TipoEvento.ANOMALIA_CORRENTE:      SeveridadeEvento.MEDIO,
    TipoEvento.INTERRUPÇÃO_INICIO:     SeveridadeEvento.CRITICO,
    TipoEvento.INTERRUPÇÃO_FIM:        SeveridadeEvento.BAIXO,
    TipoEvento.ALARME_SOBRECARGA:      SeveridadeEvento.ALTO,
    TipoEvento.ALARME_TEMPERATURA:     SeveridadeEvento.MEDIO,
}


# ---------------------------------------------------------------------------
# Gerador de eventos sintéticos
# ---------------------------------------------------------------------------

def _simular_contexto_climatico() -> dict:
    """Gera contexto climático realista para Minas Gerais."""
    hora = datetime.now().hour
    temp_base = 28 - abs(hora - 14) * 0.5
    return {
        "temperatura_c": round(random.gauss(temp_base, 3), 1),
        "umidade_pct": round(random.uniform(45, 95), 1),
        "precipitacao_mm": round(
            random.choices([0.0, random.uniform(0.1, 50)], weights=[0.7, 0.3])[0], 1
        ),
        "velocidade_vento_kmh": round(random.uniform(0, 60), 1),
    }


def gerar_evento_rede() -> EventoRede:
    """Cria um EventoRede sintético baseado na topologia real do projeto."""
    alimentador = random.choice(ALIMENTADORES)
    subestacao = SUBESTACAO_MAP[alimentador.subestacao_id]

    equips_alim = [eq for eq in EQUIPAMENTOS if eq.alimentador_id == alimentador.id]
    equipamento = random.choice(equips_alim) if equips_alim else random.choice(EQUIPAMENTOS)

    tipos = list(_TIPOS_PESOS.keys())
    pesos = list(_TIPOS_PESOS.values())
    tipo = random.choices(tipos, weights=pesos, k=1)[0]
    severidade = _SEVERIDADE_POR_TIPO[tipo]

    tensao_nominal = subestacao.tensao_kv
    tensao_kv = round(tensao_nominal * random.uniform(0.88, 1.08), 2)
    corrente_a = round(random.uniform(50, 800), 1)
    potencia_mw = round(tensao_kv * corrente_a * 1.732 / 1000, 3)
    fator_pot = round(random.uniform(0.80, 0.99), 3)

    clientes_max = alimentador.num_clientes
    clientes_afetados = (
        random.randint(0, clientes_max)
        if severidade == SeveridadeEvento.CRITICO
        else random.randint(0, clientes_max // 4)
    )

    classe = random.choice(list(ClasseInterrupcao))
    duracao_est = random.randint(5, 240) if clientes_afetados > 0 else 0

    clima = _simular_contexto_climatico()
    agora = timestamp_utc_agora()

    return EventoRede(
        evento_id=novo_evento_id(),
        tipo_evento=tipo.value,
        severidade=severidade.value,
        status=StatusEvento.ATIVO.value,
        subestacao_id=subestacao.id,
        subestacao_nome=subestacao.nome,
        alimentador_id=alimentador.id,
        alimentador_nome=alimentador.nome,
        equipamento_id=equipamento.id,
        tipo_equipamento=equipamento.tipo,
        regional=subestacao.regional,
        municipio=subestacao.municipio,
        tensao_kv=tensao_kv,
        corrente_a=corrente_a,
        potencia_mw=potencia_mw,
        fator_potencia=fator_pot,
        clientes_afetados=clientes_afetados,
        classe_interrupcao=classe.value,
        duracao_estimada_min=duracao_est,
        temperatura_c=clima["temperatura_c"],
        umidade_pct=clima["umidade_pct"],
        precipitacao_mm=clima["precipitacao_mm"],
        velocidade_vento_kmh=clima["velocidade_vento_kmh"],
        timestamp_evento=agora,
        timestamp_ingestao=agora,
    )


# ---------------------------------------------------------------------------
# Compatibilidade OOP para o run_simuladores.py
# ---------------------------------------------------------------------------

class EventoRedeGerador:
    """Wrapper OOP sobre gerar_evento_rede() para uso pelo orquestrador."""
    def __init__(self, seed: int = 42):
        random.seed(seed)

    def gerar_evento(self) -> EventoRede:
        return gerar_evento_rede()


def evento_para_alarme(evento: EventoRede) -> "Alarme | None":
    """Converte um EventoRede de alta severidade em Alarme, ou retorna None."""
    if evento.severidade in (SeveridadeEvento.CRITICO.value, SeveridadeEvento.ALTO.value):
        return Alarme(
            alarme_id=novo_alarme_id(),
            tipo_alarme=f"ALARME_{evento.tipo_evento}",
            severidade=evento.severidade,
            descricao=f"Alarme gerado a partir de evento: {evento.tipo_evento}",
            subestacao_id=evento.subestacao_id,
            alimentador_id=evento.alimentador_id,
            equipamento_id=evento.equipamento_id,
            valor_medido=evento.corrente_a,
            valor_limite=0.0,
            unidade="A",
            acao_recomendada="Despachar equipe de manutencao",
            requer_desligamento=(evento.severidade == SeveridadeEvento.CRITICO.value),
            timestamp_alarme=evento.timestamp_evento,
            timestamp_ingestao=timestamp_utc_agora(),
            evento_id_origem=evento.evento_id,
        )
    return None


class ADMSEventoProducer:
    """Producer Kafka OOP para eventos e alarmes — usa kafka-python."""

    def __init__(self, bootstrap_servers: str, dry_run: bool = False):
        self._dry_run = dry_run
        self._producer = None
        self._bootstrap = bootstrap_servers
        if not dry_run:
            self._conectar()

    def _conectar(self, tentativas: int = 5, espera: float = 2.0):
        from kafka import KafkaProducer
        from kafka.errors import NoBrokersAvailable
        for i in range(tentativas):
            try:
                self._producer = KafkaProducer(
                    bootstrap_servers=self._bootstrap,
                    value_serializer=lambda v: v,   # payload ja e bytes
                    key_serializer=lambda k: k,     # key ja e bytes
                    acks=1,
                    compression_type="gzip",
                    linger_ms=5,
                )
                logger.info(
                    "ADMSEventoProducer conectado: %s", self._bootstrap
                )
                return
            except NoBrokersAvailable:
                if i < tentativas - 1:
                    logger.warning(
                        "Kafka indisponivel (tentativa %d/%d), aguardando %.0fs...",
                        i + 1, tentativas, espera,
                    )
                    import time
                    time.sleep(espera)
                else:
                    logger.error(
                        "Nao foi possivel conectar ao Kafka após %d tentativas.", tentativas
                    )

    def publicar_evento(self, evento: EventoRede) -> bool:
        if self._dry_run:
            return True
        if not self._producer:
            return False
        try:
            payload = json.dumps(evento.to_dict(), ensure_ascii=False).encode("utf-8")
            self._producer.send(
                TOPICO,
                key=evento.to_kafka_key().encode("utf-8"),
                value=payload,
            )
            return True
        except Exception as e:
            logger.error("Erro ao publicar evento: %s", e)
            return False

    def publicar_alarme(self, alarme: Alarme) -> bool:
        if self._dry_run:
            return True
        if not self._producer:
            return False
        try:
            payload = json.dumps(alarme.to_dict(), ensure_ascii=False).encode("utf-8")
            self._producer.send(
                "adms.alarmes",
                key=alarme.to_kafka_key().encode("utf-8"),
                value=payload,
            )
            return True
        except Exception as e:
            logger.error("Erro ao publicar alarme: %s", e)
            return False

    def fechar(self):
        if self._producer:
            self._producer.flush()
            self._producer.close()


# ---------------------------------------------------------------------------
# Callbacks de entrega Kafka
# ---------------------------------------------------------------------------

def _on_delivery(err, msg):
    if err:
        logger.error("Erro ao entregar mensagem: %s | tópico=%s", err, msg.topic())
    else:
        logger.debug(
            "Mensagem entregue | tópico=%s | partição=%d | offset=%d",
            msg.topic(), msg.partition(), msg.offset(),
        )


# ---------------------------------------------------------------------------
# Modo dry-run (sem Kafka)
# ---------------------------------------------------------------------------

def _publicar_dry_run(evento: EventoRede, contador: int):
    dados = evento.to_dict()
    print(
        f"\n{'='*62}\n"
        f"  [DRY-RUN] Evento #{contador:04d}\n"
        f"{'='*62}\n"
        f"  Tópico  : {TOPICO}\n"
        f"  Key     : {evento.to_kafka_key()}\n"
        f"  ID      : {evento.evento_id}\n"
        f"  Tipo    : {evento.tipo_evento}  ({evento.severidade})\n"
        f"  Local   : {evento.subestacao_nome} › {evento.alimentador_id}\n"
        f"  Equip.  : {evento.equipamento_id} ({evento.tipo_equipamento})\n"
        f"  Clientes afetados : {evento.clientes_afetados:,}\n"
        f"  Tensão  : {evento.tensao_kv} kV | Corrente: {evento.corrente_a} A\n"
        f"  Clima   : {evento.temperatura_c}°C | {evento.precipitacao_mm} mm chuva\n"
        f"  Payload : {len(json.dumps(dados))} bytes\n"
    )


# ---------------------------------------------------------------------------
# Loop principal do produtor
# ---------------------------------------------------------------------------

def run(rate: float, duration: float | None, dry_run: bool):
    """
    Produz eventos de rede elétrica no Kafka (ou apenas imprime em dry-run).

    Args:
        rate:     Eventos por segundo a gerar.
        duration: Segundos de execução (None = infinito).
        dry_run:  Se True, não conecta ao Kafka.
    """
    intervalo = 1.0 / rate
    producer = None

    if not dry_run:
        try:
            from confluent_kafka import Producer  # type: ignore
            producer = Producer(KAFKA_CONFIG_PADRAO)
            logger.info(
                "Conectado ao Kafka em %s",
                KAFKA_CONFIG_PADRAO["bootstrap.servers"],
            )
        except ImportError:
            logger.error(
                "confluent-kafka não instalado. "
                "Execute: pip install confluent-kafka  "
                "Ou use --dry-run para testar sem Kafka."
            )
            sys.exit(1)
        except Exception as exc:
            logger.error("Falha ao conectar ao Kafka: %s", exc)
            sys.exit(1)

    logger.info(
        "Iniciando produtor | rate=%.1f ev/s | duration=%s | dry_run=%s | tópico=%s",
        rate,
        f"{duration:.0f}s" if duration else "∞",
        dry_run,
        TOPICO,
    )

    inicio = time.monotonic()
    contador = 0
    try:
        while True:
            elapsed = time.monotonic() - inicio
            if duration and elapsed >= duration:
                break

            evento = gerar_evento_rede()
            contador += 1

            if dry_run:
                _publicar_dry_run(evento, contador)
            else:
                payload = json.dumps(evento.to_dict(), ensure_ascii=False).encode("utf-8")
                producer.produce(
                    topic=TOPICO,
                    key=evento.to_kafka_key().encode("utf-8"),
                    value=payload,
                    callback=_on_delivery,
                )
                producer.poll(0)

                if contador % 100 == 0:
                    logger.info(
                        "Produzidos %d eventos | elapsed=%.1fs", contador, elapsed
                    )
                    producer.flush()

            time.sleep(intervalo)

    except KeyboardInterrupt:
        logger.info("Interrompido pelo usuário.")
    finally:
        if producer:
            logger.info("Aguardando flush de mensagens pendentes...")
            producer.flush(timeout=10)

    elapsed_total = time.monotonic() - inicio
    taxa_real = contador / elapsed_total if elapsed_total > 0 else 0
    logger.info(
        "Produtor encerrado | total=%d eventos | %.2f ev/s | %.1fs",
        contador,
        taxa_real,
        elapsed_total,
    )


# ---------------------------------------------------------------------------
# Entrypoint CLI
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Produtor Kafka de eventos de rede elétrica — ADMS Smart Grid Platform"
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Imprime eventos na stdout sem publicar no Kafka.",
    )
    parser.add_argument(
        "--rate",
        type=float,
        default=1.0,
        metavar="EVT/S",
        help="Taxa de geração de eventos por segundo.",
    )
    parser.add_argument(
        "--duration",
        type=float,
        default=None,
        metavar="SEGUNDOS",
        help="Duração máxima em segundos (omitir = infinito).",
    )
    parser.add_argument(
        "--bootstrap-servers",
        default=KAFKA_CONFIG_PADRAO["bootstrap.servers"],
        help="Endereço do broker Kafka.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Seed para reprodutibilidade dos dados sintéticos.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()

    if args.seed is not None:
        random.seed(args.seed)
        logger.info("Random seed fixado em %d", args.seed)

    if not args.dry_run:
        KAFKA_CONFIG_PADRAO["bootstrap.servers"] = args.bootstrap_servers

    run(rate=args.rate, duration=args.duration, dry_run=args.dry_run)