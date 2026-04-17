"""
Orquestrador dos simuladores ADMS — Smart Grid Data Platform
Roda evento_rede_producer e medidor_producer em paralelo via threads
e exibe painel de monitoramento no terminal com atualização a cada 5s.

Uso:
  python run_simuladores.py                        # modo padrão
  python run_simuladores.py --dry-run              # sem Kafka
  python run_simuladores.py --duration 120         # rodar por 2 minutos
  python run_simuladores.py --eventos-rate 3 --medidores-rate 100
"""

import argparse
import os
import signal
import sys
import threading
import time
from datetime import datetime, timezone
from typing import Optional

from dotenv import load_dotenv
from loguru import logger

# Importa os módulos já criados
from topologia import SUBESTACOES, ALIMENTADORES, EQUIPAMENTOS, total_clientes
from schemas import timestamp_utc_agora
from evento_rede_producer import (
    EventoRedeGerador, ADMSEventoProducer, evento_para_alarme,
)
from medidor_producer import (
    CatalogoMedidores, GeradorLeitura, MedidorProducer,
    modo_batch, modo_stream,
)

load_dotenv()

# ---------------------------------------------------------------------------
# Estado compartilhado entre threads (thread-safe com lock)
# ---------------------------------------------------------------------------

class EstadoSimulador:
    def __init__(self):
        self._lock = threading.Lock()
        self.inicio = time.time()
        self.rodando = True

        # Contadores de eventos
        self.eventos_gerados = 0
        self.eventos_enviados = 0
        self.alarmes_enviados = 0
        self.eventos_erros = 0

        # Contadores de medidores
        self.leituras_geradas = 0
        self.leituras_enviadas = 0
        self.leituras_erros = 0

        # Métricas de negócio
        self.clientes_afetados_acum = 0
        self.anomalias_consumo = 0
        self.tensao_critica = 0
        self.fraudes_detectadas = 0

        # Últimos eventos para display
        self.ultimo_evento_tipo = "—"
        self.ultimo_evento_local = "—"
        self.ultimo_evento_severidade = "—"

    def atualizar_evento(self, evento, enviado: bool, alarme_enviado: bool):
        with self._lock:
            self.eventos_gerados += 1
            if enviado:
                self.eventos_enviados += 1
            else:
                self.eventos_erros += 1
            if alarme_enviado:
                self.alarmes_enviados += 1
            self.clientes_afetados_acum += evento.clientes_afetados
            self.ultimo_evento_tipo = evento.tipo_evento
            self.ultimo_evento_local = f"{evento.municipio} › {evento.alimentador_id}"
            self.ultimo_evento_severidade = evento.severidade

    def atualizar_leitura(self, leitura, enviada: bool):
        with self._lock:
            self.leituras_geradas += 1
            if enviada:
                self.leituras_enviadas += 1
            else:
                self.leituras_erros += 1
            if leitura.is_anomalia_consumo:
                self.anomalias_consumo += 1
            if leitura.is_tensao_critica:
                self.tensao_critica += 1
            if leitura.is_possivel_fraude:
                self.fraudes_detectadas += 1

    def snapshot(self) -> dict:
        with self._lock:
            elapsed = max(1, time.time() - self.inicio)
            return {
                "elapsed_s": int(elapsed),
                "eventos_gerados": self.eventos_gerados,
                "eventos_enviados": self.eventos_enviados,
                "alarmes_enviados": self.alarmes_enviados,
                "eventos_erros": self.eventos_erros,
                "taxa_eventos": round(self.eventos_gerados / elapsed, 1),
                "leituras_geradas": self.leituras_geradas,
                "leituras_enviadas": self.leituras_enviadas,
                "leituras_erros": self.leituras_erros,
                "taxa_leituras": round(self.leituras_geradas / elapsed, 1),
                "clientes_afetados_acum": self.clientes_afetados_acum,
                "anomalias_consumo": self.anomalias_consumo,
                "tensao_critica": self.tensao_critica,
                "fraudes_detectadas": self.fraudes_detectadas,
                "ultimo_evento_tipo": self.ultimo_evento_tipo,
                "ultimo_evento_local": self.ultimo_evento_local,
                "ultimo_evento_severidade": self.ultimo_evento_severidade,
            }


# ---------------------------------------------------------------------------
# Thread: producer de eventos de rede
# ---------------------------------------------------------------------------

def thread_eventos(
    estado: EstadoSimulador,
    rate: float,
    dry_run: bool,
    seed: int,
):
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    gerador = EventoRedeGerador(seed=seed)
    producer = ADMSEventoProducer(bootstrap_servers=bootstrap, dry_run=dry_run)
    intervalo = 1.0 / rate

    try:
        while estado.rodando:
            t0 = time.time()

            evento = gerador.gerar_evento()
            ok_evento = producer.publicar_evento(evento)

            alarme = evento_para_alarme(evento)
            ok_alarme = False
            if alarme:
                ok_alarme = producer.publicar_alarme(alarme)

            estado.atualizar_evento(evento, ok_evento, ok_alarme)

            elapsed = time.time() - t0
            sleep = max(0, intervalo - elapsed)
            if sleep > 0:
                time.sleep(sleep)
    finally:
        producer.fechar()


# ---------------------------------------------------------------------------
# Thread: producer de leituras de medidores
# ---------------------------------------------------------------------------

def thread_medidores(
    estado: EstadoSimulador,
    catalogo: CatalogoMedidores,
    rate: float,
    dry_run: bool,
    seed: int,
):
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    gerador = GeradorLeitura(seed=seed)
    producer = MedidorProducer(bootstrap_servers=bootstrap, dry_run=dry_run)
    intervalo = 1.0 / rate
    idx = 0

    try:
        while estado.rodando:
            t0 = time.time()

            med = catalogo.medidores[idx % len(catalogo.medidores)]
            leitura = gerador.gerar(med)
            ok = producer.publicar(leitura)
            estado.atualizar_leitura(leitura, ok)
            idx += 1

            elapsed = time.time() - t0
            sleep = max(0, intervalo - elapsed)
            if sleep > 0:
                time.sleep(sleep)
    finally:
        producer.fechar()


# ---------------------------------------------------------------------------
# Painel de monitoramento no terminal
# ---------------------------------------------------------------------------

SEVERIDADE_COR = {
    "CRITICO":  "\033[91m",   # vermelho
    "ALTO":     "\033[93m",   # amarelo
    "MEDIO":    "\033[94m",   # azul
    "BAIXO":    "\033[92m",   # verde
    "-":        "\033[0m",
}
RESET = "\033[0m"
BOLD  = "\033[1m"
CYAN  = "\033[96m"
GREEN = "\033[92m"
GRAY  = "\033[90m"


def formatar_tempo(segundos: int) -> str:
    h = segundos // 3600
    m = (segundos % 3600) // 60
    s = segundos % 60
    return f"{h:02d}:{m:02d}:{s:02d}"


def imprimir_painel(snap: dict, dry_run: bool, total_medidores: int):
    os.system("cls" if os.name == "nt" else "clear")

    modo = f"{BOLD}[DRY-RUN]{RESET}" if dry_run else f"{GREEN}[KAFKA ATIVO]{RESET}"
    ts = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    elapsed = formatar_tempo(snap["elapsed_s"])

    sev = snap["ultimo_evento_severidade"]
    cor_sev = SEVERIDADE_COR.get(sev, RESET)

    sep = "=" * 64
    print(f"{BOLD}{CYAN}{sep}{RESET}")
    print(f"{BOLD}{CYAN}       ADMS SMART GRID - Painel de Simulacao{RESET}")
    print(f"{BOLD}{CYAN}{sep}{RESET}")
    print(f"  {GRAY}Data/hora: {ts}  |  Uptime: {elapsed}  |  {modo}{RESET}")
    print()

    print(f"{BOLD}  Infraestrutura da rede simulada{RESET}")
    print(f"  {GRAY}{'-'*58}{RESET}")
    print(f"  Subestacoes   : {len(SUBESTACOES):>6,}")
    print(f"  Alimentadores : {len(ALIMENTADORES):>6,}")
    print(f"  Equipamentos  : {len(EQUIPAMENTOS):>6,}")
    print(f"  Medidores AMI : {total_medidores:>6,}")
    print(f"  Clientes      : {total_clientes():>6,}")
    print()

    print(f"{BOLD}  Eventos de rede  {GRAY}(topico: adms.eventos.rede){RESET}")
    print(f"  {GRAY}{'-'*58}{RESET}")
    print(f"  Gerados      : {snap['eventos_gerados']:>8,}   Taxa: {snap['taxa_eventos']} ev/s")
    print(f"  Enviados     : {snap['eventos_enviados']:>8,}")
    print(f"  Alarmes      : {snap['alarmes_enviados']:>8,}   {GRAY}(topico: adms.alarmes){RESET}")
    print(f"  Erros        : {snap['eventos_erros']:>8,}")
    print(f"  Clientes afetados acum.: {snap['clientes_afetados_acum']:>10,}")
    print()
    print(f"  Ultimo evento : {cor_sev}{snap['ultimo_evento_tipo']}{RESET}  [{cor_sev}{sev}{RESET}]")
    print(f"  Local         : {snap['ultimo_evento_local']}")
    print()

    print(f"{BOLD}  Leituras de medidores  {GRAY}(topico: adms.leituras.medidor){RESET}")
    print(f"  {GRAY}{'-'*58}{RESET}")
    print(f"  Geradas      : {snap['leituras_geradas']:>8,}   Taxa: {snap['taxa_leituras']} leit/s")
    print(f"  Enviadas     : {snap['leituras_enviadas']:>8,}")
    print(f"  Erros        : {snap['leituras_erros']:>8,}")
    print(f"  Anomalias    : {snap['anomalias_consumo']:>8,}   Tensao critica: {snap['tensao_critica']:,}")
    print(f"  Fraudes det. : {snap['fraudes_detectadas']:>8,}")
    print()

    total_msgs = snap["eventos_enviados"] + snap["leituras_enviadas"] + snap["alarmes_enviados"]
    taxa_total = round(total_msgs / max(1, snap["elapsed_s"]), 1)
    print(f"{BOLD}  Total Kafka: {total_msgs:,} mensagens  |  Taxa combinada: {taxa_total} msg/s{RESET}")
    print()
    print(f"  {GRAY}Ctrl+C para encerrar{RESET}")


# ---------------------------------------------------------------------------
# Thread: painel de monitoramento
# ---------------------------------------------------------------------------

def thread_painel(
    estado: EstadoSimulador,
    dry_run: bool,
    total_medidores: int,
    intervalo_s: float = 5.0,
):
    while estado.rodando:
        snap = estado.snapshot()
        imprimir_painel(snap, dry_run, total_medidores)
        time.sleep(intervalo_s)

    # Print final após encerramento
    snap = estado.snapshot()
    imprimir_painel(snap, dry_run, total_medidores)


# ---------------------------------------------------------------------------
# Entrypoint principal
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Orquestrador ADMS — Simuladores em paralelo")
    parser.add_argument("--eventos-rate",   type=float, default=2.0,    help="Eventos de rede por segundo")
    parser.add_argument("--medidores-rate", type=float, default=50.0,   help="Leituras de medidor por segundo")
    parser.add_argument("--medidores",      type=int,   default=10_000, help="Total de medidores simulados")
    parser.add_argument("--duration",       type=int,   default=None,   help="Duração em segundos (None = infinito)")
    parser.add_argument("--seed",           type=int,   default=42)
    parser.add_argument("--dry-run",        action="store_true",         help="Não publicar no Kafka")
    args = parser.parse_args()

    os.makedirs("logs", exist_ok=True)

    # Inicializar catálogo de medidores (compartilhado entre threads)
    catalogo = CatalogoMedidores(total_medidores=args.medidores, seed=args.seed)

    estado = EstadoSimulador()

    # Graceful shutdown
    def _sair(sig, frame):
        print("\n\nEncerrando simuladores...")
        estado.rodando = False

    signal.signal(signal.SIGINT, _sair)
    signal.signal(signal.SIGTERM, _sair)

    # Criar e iniciar threads
    threads = [
        threading.Thread(
            target=thread_eventos,
            args=(estado, args.eventos_rate, args.dry_run, args.seed),
            name="Thread-Eventos",
            daemon=True,
        ),
        threading.Thread(
            target=thread_medidores,
            args=(estado, catalogo, args.medidores_rate, args.dry_run, args.seed),
            name="Thread-Medidores",
            daemon=True,
        ),
        threading.Thread(
            target=thread_painel,
            args=(estado, args.dry_run, len(catalogo.medidores)),
            name="Thread-Painel",
            daemon=True,
        ),
    ]

    for t in threads:
        t.start()

    # Aguardar duração ou sinal de encerramento
    inicio = time.time()
    try:
        while estado.rodando:
            if args.duration and (time.time() - inicio) >= args.duration:
                estado.rodando = False
                break
            time.sleep(0.5)
    finally:
        estado.rodando = False
        time.sleep(1)   # dar tempo para threads finalizarem

    # Resumo final no log
    snap = estado.snapshot()
    logger.info("=" * 60)
    logger.info(f"Simulação encerrada | duração: {snap['elapsed_s']}s")
    logger.info(f"Eventos     : {snap['eventos_gerados']:,} gerados | {snap['eventos_enviados']:,} enviados")
    logger.info(f"Alarmes     : {snap['alarmes_enviados']:,} enviados")
    logger.info(f"Leituras    : {snap['leituras_geradas']:,} geradas | {snap['leituras_enviadas']:,} enviadas")
    logger.info(f"Total Kafka : {snap['eventos_enviados'] + snap['leituras_enviadas']:,} mensagens")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()