"""
Simulador de leituras de medidores inteligentes (AMI) — ADMS Smart Grid Platform
Publica leituras periódicas no tópico: adms.leituras.medidor

Simula 10.000 medidores distribuídos pelos 120 alimentadores com perfis
de consumo realistas por classe (residencial, comercial, industrial, rural).

Modo de operação:
  - BATCH: gera um "ciclo" completo de leituras de todos os medidores de uma vez
  - STREAM: publica continuamente simulando o ritmo real de 15 em 15 minutos

Uso:
  python medidor_producer.py --mode stream --rate 50  # 50 medidores/segundo
  python medidor_producer.py --mode batch             # um ciclo completo e sai
  python medidor_producer.py --mode stream --dry-run  # sem publicar no Kafka
"""

import argparse
import json
import math
import os
import random
import signal
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional

from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.errors import KafkaError
from loguru import logger

from kafka_config import get_producer_config
from topologia import ALIMENTADORES, SUBESTACAO_MAP, Alimentador
from schemas import (
    LeituraMedidor, TipoMedidor,
    nova_leitura_id, timestamp_utc_agora, validar_tensao_aneel,
)

load_dotenv()

logger.remove()
logger.add(
    sys.stdout,
    format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | {message}",
    level="INFO",
    colorize=True,
)
logger.add(
    "logs/medidor_producer.log",
    rotation="10 MB",
    retention="7 days",
    level="DEBUG",
)

# ---------------------------------------------------------------------------
# Perfis de consumo por tipo de medidor
# Cada perfil define: consumo_base_kwh/dia, variação_hora, fator_fp
# ---------------------------------------------------------------------------

@dataclass
class PerfilConsumo:
    tipo: str
    kwh_dia_base: float          # consumo médio diário em kWh
    kwh_dia_variacao: float      # desvio padrão do consumo diário
    tensao_nominal_v: float      # tensão nominal de fornecimento
    demanda_pico_kw: float       # demanda máxima no pico
    fator_potencia_base: float   # fator de potência típico
    prob_fraude: float           # probabilidade de ser um caso de fraude


PERFIS: Dict[str, PerfilConsumo] = {
    TipoMedidor.RESIDENCIAL.value: PerfilConsumo(
        tipo=TipoMedidor.RESIDENCIAL.value,
        kwh_dia_base=12.0,
        kwh_dia_variacao=6.0,
        tensao_nominal_v=220.0,
        demanda_pico_kw=3.5,
        fator_potencia_base=0.92,
        prob_fraude=0.015,          # 1.5% de fraude
    ),
    TipoMedidor.COMERCIAL.value: PerfilConsumo(
        tipo=TipoMedidor.COMERCIAL.value,
        kwh_dia_base=85.0,
        kwh_dia_variacao=40.0,
        tensao_nominal_v=220.0,
        demanda_pico_kw=25.0,
        fator_potencia_base=0.88,
        prob_fraude=0.008,
    ),
    TipoMedidor.INDUSTRIAL.value: PerfilConsumo(
        tipo=TipoMedidor.INDUSTRIAL.value,
        kwh_dia_base=850.0,
        kwh_dia_variacao=200.0,
        tensao_nominal_v=380.0,
        demanda_pico_kw=300.0,
        fator_potencia_base=0.85,
        prob_fraude=0.003,
    ),
    TipoMedidor.RURAL.value: PerfilConsumo(
        tipo=TipoMedidor.RURAL.value,
        kwh_dia_base=8.0,
        kwh_dia_variacao=4.0,
        tensao_nominal_v=127.0,
        demanda_pico_kw=2.0,
        fator_potencia_base=0.90,
        prob_fraude=0.025,          # mais fraude em área rural
    ),
    TipoMedidor.PODER_PUBLICO.value: PerfilConsumo(
        tipo=TipoMedidor.PODER_PUBLICO.value,
        kwh_dia_base=45.0,
        kwh_dia_variacao=15.0,
        tensao_nominal_v=220.0,
        demanda_pico_kw=15.0,
        fator_potencia_base=0.90,
        prob_fraude=0.001,
    ),
}

# Curva de carga horária — multiplicador sobre consumo base
# Índice = hora do dia (0-23)
CURVA_CARGA_RESIDENCIAL = [
    0.30, 0.25, 0.22, 0.20, 0.22, 0.28,  # 00-05h (madrugada)
    0.45, 0.70, 0.75, 0.65, 0.60, 0.65,  # 06-11h (manhã)
    0.70, 0.65, 0.60, 0.65, 0.75, 0.90,  # 12-17h (tarde)
    1.00, 0.95, 0.85, 0.75, 0.60, 0.45,  # 18-23h (noite — pico às 18h)
]

CURVA_CARGA_COMERCIAL = [
    0.10, 0.08, 0.08, 0.08, 0.10, 0.15,  # madrugada
    0.30, 0.60, 0.90, 1.00, 0.95, 0.90,  # manhã — pico 9-10h
    0.85, 0.90, 0.95, 0.90, 0.80, 0.50,  # tarde
    0.30, 0.20, 0.15, 0.12, 0.10, 0.10,  # noite
]

CURVA_CARGA_INDUSTRIAL = [
    0.70, 0.70, 0.70, 0.70, 0.72, 0.80,  # 3 turnos — mais constante
    0.90, 1.00, 1.00, 0.98, 0.95, 0.90,
    0.88, 0.90, 0.92, 0.88, 0.82, 0.75,
    0.70, 0.70, 0.70, 0.70, 0.70, 0.70,
]

CURVAS_POR_TIPO = {
    TipoMedidor.RESIDENCIAL.value:   CURVA_CARGA_RESIDENCIAL,
    TipoMedidor.COMERCIAL.value:     CURVA_CARGA_COMERCIAL,
    TipoMedidor.INDUSTRIAL.value:    CURVA_CARGA_INDUSTRIAL,
    TipoMedidor.RURAL.value:         CURVA_CARGA_RESIDENCIAL,   # similar ao residencial
    TipoMedidor.PODER_PUBLICO.value: CURVA_CARGA_COMERCIAL,
}


# ---------------------------------------------------------------------------
# Cadastro de medidores (gerado uma vez e mantido em memória)
# ---------------------------------------------------------------------------

@dataclass
class CadastroMedidor:
    medidor_id: str
    cliente_id: str
    tipo: str
    alimentador_id: str
    subestacao_id: str
    regional: str
    municipio: str
    latitude: float
    longitude: float
    perfil: PerfilConsumo
    kwh_acumulado: float          # leitura acumulada desde instalação
    is_fraude_simulada: bool      # flag interno — nunca publicado
    consumo_kwh_dia: float        # consumo diário personalizado deste medidor


class CatalogoMedidores:
    """Gera e mantém o cadastro de todos os medidores simulados."""

    # Mix de tipos por área
    _MIX_URBANO = {
        TipoMedidor.RESIDENCIAL.value:   0.65,
        TipoMedidor.COMERCIAL.value:     0.25,
        TipoMedidor.INDUSTRIAL.value:    0.05,
        TipoMedidor.PODER_PUBLICO.value: 0.05,
    }
    _MIX_SUBURBANO = {
        TipoMedidor.RESIDENCIAL.value:   0.75,
        TipoMedidor.COMERCIAL.value:     0.15,
        TipoMedidor.RURAL.value:         0.07,
        TipoMedidor.PODER_PUBLICO.value: 0.03,
    }
    _MIX_RURAL = {
        TipoMedidor.RESIDENCIAL.value:   0.50,
        TipoMedidor.RURAL.value:         0.40,
        TipoMedidor.PODER_PUBLICO.value: 0.05,
        TipoMedidor.COMERCIAL.value:     0.05,
    }

    def __init__(self, total_medidores: int = 10_000, seed: int = 42):
        self._rng = random.Random(seed)
        self.medidores: List[CadastroMedidor] = []
        self._gerar(total_medidores)
        logger.info(
            f"Catálogo: {len(self.medidores):,} medidores gerados | "
            f"fraudes simuladas: {sum(1 for m in self.medidores if m.is_fraude_simulada)}"
        )

    def _mix_para_area(self, tipo_area: str) -> dict:
        return {
            "URBANA":    self._MIX_URBANO,
            "SUBURBANA": self._MIX_SUBURBANO,
            "RURAL":     self._MIX_RURAL,
        }.get(tipo_area, self._MIX_URBANO)

    def _gerar(self, total: int):
        # Distribuir medidores entre alimentadores proporcionalmente ao num_clientes
        total_clientes = sum(al.num_clientes for al in ALIMENTADORES)
        contador = 0

        for al in ALIMENTADORES:
            se = SUBESTACAO_MAP[al.subestacao_id]
            frac = al.num_clientes / total_clientes
            n_al = max(1, round(total * frac))
            mix = self._mix_para_area(al.tipo_area)

            tipos = list(mix.keys())
            pesos = list(mix.values())

            for i in range(n_al):
                tipo = self._rng.choices(tipos, weights=pesos, k=1)[0]
                perfil = PERFIS[tipo]
                is_fraude = self._rng.random() < perfil.prob_fraude

                # Consumo diário personalizado para este medidor
                kwh_dia = max(0.5, self._rng.gauss(
                    perfil.kwh_dia_base, perfil.kwh_dia_variacao
                ))

                # Leitura acumulada simulando anos de uso (1-15 anos)
                anos_uso = self._rng.uniform(1, 15)
                kwh_acum = kwh_dia * 365 * anos_uso

                med_id = f"MED-{al.subestacao_id}-{contador:06d}"
                cli_id = f"CLI-{contador:08d}"

                lat = se.latitude + self._rng.uniform(-0.08, 0.08)
                lon = se.longitude + self._rng.uniform(-0.08, 0.08)

                self.medidores.append(CadastroMedidor(
                    medidor_id=med_id,
                    cliente_id=cli_id,
                    tipo=tipo,
                    alimentador_id=al.id,
                    subestacao_id=al.subestacao_id,
                    regional=se.regional,
                    municipio=se.municipio,
                    latitude=round(lat, 6),
                    longitude=round(lon, 6),
                    perfil=perfil,
                    kwh_acumulado=round(kwh_acum, 2),
                    is_fraude_simulada=is_fraude,
                    consumo_kwh_dia=round(kwh_dia, 3),
                ))
                contador += 1

                if len(self.medidores) >= total:
                    return


# ---------------------------------------------------------------------------
# Gerador de leitura para um medidor específico
# ---------------------------------------------------------------------------

class GeradorLeitura:
    """Gera uma leitura AMI realista para um medidor no instante atual."""

    def __init__(self, seed: int = 42):
        self._rng = random.Random(seed + 1)

    def gerar(self, med: CadastroMedidor, intervalo_min: int = 15) -> LeituraMedidor:
        agora = datetime.now(timezone.utc)
        hora = agora.hour
        perfil = med.perfil

        # Curva de carga para a hora atual
        curva = CURVAS_POR_TIPO[med.tipo]
        fator_hora = curva[hora]

        # Consumo no intervalo (kWh)
        kwh_intervalo_base = med.consumo_kwh_dia * (intervalo_min / 1440)
        kwh_intervalo = kwh_intervalo_base * fator_hora
        kwh_intervalo *= self._rng.uniform(0.85, 1.15)   # ruído realista

        # Fraude: consumo sub-reportado (desvio no medidor)
        kwh_reportado = kwh_intervalo
        if med.is_fraude_simulada:
            fator_fraude = self._rng.uniform(0.2, 0.6)   # reporta 20-60% do real
            kwh_reportado = kwh_intervalo * fator_fraude

        # Atualizar acumulado
        med.kwh_acumulado = round(med.kwh_acumulado + kwh_reportado, 2)

        # Demanda instantânea (kW)
        kw_demanda = perfil.demanda_pico_kw * fator_hora * self._rng.uniform(0.7, 1.0)

        # Tensão — com possibilidade de anomalia
        tensao_nominal = perfil.tensao_nominal_v
        prob_tensao_anomala = 0.05   # 5% das leituras com tensão fora do padrão
        if self._rng.random() < prob_tensao_anomala:
            if self._rng.random() < 0.7:
                tensao = tensao_nominal * self._rng.uniform(0.82, 0.93)  # subtensão
            else:
                tensao = tensao_nominal * self._rng.uniform(1.06, 1.15)  # sobretensão
        else:
            tensao = tensao_nominal * self._rng.uniform(0.95, 1.05)

        tensao = round(tensao, 1)
        tensao_min = round(tensao * self._rng.uniform(0.96, 1.0), 1)
        tensao_max = round(tensao * self._rng.uniform(1.0, 1.04), 1)

        # Corrente
        corrente = (kw_demanda * 1000) / (tensao * math.sqrt(3)) if tensao > 0 else 0
        corrente = round(corrente, 2)

        # Fator de potência com variação
        fp = round(
            min(1.0, perfil.fator_potencia_base + self._rng.uniform(-0.05, 0.05)),
            3,
        )

        # kWh reativo proporcional ao FP
        angulo = math.acos(fp) if abs(fp) <= 1 else 0
        kwh_reativo = round(kwh_reportado * math.tan(angulo), 4)

        # Interrupções no período (raras)
        num_int = 0
        dur_int = 0
        if self._rng.random() < 0.02:   # 2% chance de interrupção na janela
            num_int = self._rng.randint(1, 3)
            dur_int = self._rng.randint(5, intervalo_min)

        # Flags de qualidade
        is_estimada = self._rng.random() < 0.01      # 1% falha de comunicação
        is_anomalia = med.is_fraude_simulada or kwh_reportado < 0.1
        is_tensao_critica = not validar_tensao_aneel(tensao, tensao_nominal)
        is_fraude_flag = (
            med.is_fraude_simulada and self._rng.random() < 0.3  # detecta 30% das fraudes
        )

        ts = agora.isoformat()

        return LeituraMedidor(
            leitura_id=nova_leitura_id(),
            medidor_id=med.medidor_id,
            cliente_id=med.cliente_id,
            tipo_medidor=med.tipo,
            alimentador_id=med.alimentador_id,
            subestacao_id=med.subestacao_id,
            regional=med.regional,
            municipio=med.municipio,
            latitude=med.latitude,
            longitude=med.longitude,
            kwh_ativo=round(kwh_reportado, 4),
            kwh_reativo=kwh_reativo,
            kw_demanda=round(kw_demanda, 3),
            tensao_v=tensao,
            corrente_a=corrente,
            fator_potencia=fp,
            num_interrupcoes=num_int,
            duracao_interrupcao_min=dur_int,
            tensao_minima_v=tensao_min,
            tensao_maxima_v=tensao_max,
            is_leitura_estimada=is_estimada,
            is_anomalia_consumo=is_anomalia,
            is_tensao_critica=is_tensao_critica,
            is_possivel_fraude=is_fraude_flag,
            timestamp_leitura=ts,
            timestamp_ingestao=ts,
        )


# ---------------------------------------------------------------------------
# Producer Kafka (reutiliza mesmo padrão do evento_rede_producer)
# ---------------------------------------------------------------------------

class MedidorProducer:
    def __init__(self, bootstrap_servers: str, dry_run: bool = False):
        self._dry_run = dry_run
        self._producer: Optional[KafkaProducer] = None
        self._enviados = 0
        self._erros = 0

        if not dry_run:
            cfg = get_producer_config(acks=1, compression="gzip", batch_size=32768, linger_ms=50)
            self._producer = KafkaProducer(
                **cfg,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8"),
            )
        else:
            logger.warning("Modo DRY-RUN ativo")

    def _conectar(self, bootstrap_servers: str, tentativas: int = 5, espera: float = 2.0):
        from kafka.errors import NoBrokersAvailable
        for i in range(tentativas):
            try:
                self._producer = KafkaProducer(
                    bootstrap_servers=bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                    key_serializer=lambda k: k.encode("utf-8"),
                    acks=1,
                    compression_type="gzip",
                    batch_size=32768,
                    linger_ms=50,
                )
                logger.info(f"MedidorProducer conectado: {bootstrap_servers}")
                return
            except NoBrokersAvailable:
                if i < tentativas - 1:
                    logger.warning(
                        f"Kafka indisponivel (tentativa {i+1}/{tentativas}), aguardando {espera:.0f}s..."
                    )
                    time.sleep(espera)
                else:
                    logger.error(
                        f"Nao foi possivel conectar ao Kafka apos {tentativas} tentativas."
                    )

    def publicar(self, leitura: LeituraMedidor) -> bool:
        topico = "adms.leituras.medidor"
        if self._dry_run:
            self._enviados += 1
            return True
        try:
            self._producer.send(topico, key=leitura.to_kafka_key(), value=leitura.to_dict())
            self._enviados += 1
            return True
        except KafkaError as e:
            logger.error(f"Erro Kafka: {e}")
            self._erros += 1
            return False

    def flush(self):
        if self._producer:
            self._producer.flush()

    def fechar(self):
        if self._producer:
            self._producer.flush()
            self._producer.close()

    @property
    def stats(self) -> dict:
        return {"enviados": self._enviados, "erros": self._erros}


# ---------------------------------------------------------------------------
# Modos de execução
# ---------------------------------------------------------------------------

def modo_batch(
    catalogo: CatalogoMedidores,
    producer: MedidorProducer,
    gerador: GeradorLeitura,
) -> None:
    """Gera uma leitura para cada medidor e publica tudo de uma vez."""
    logger.info(f"Modo BATCH — gerando {len(catalogo.medidores):,} leituras...")
    inicio = time.time()

    anomalias = 0
    tensao_critica = 0
    fraudes_detectadas = 0

    for i, med in enumerate(catalogo.medidores):
        leitura = gerador.gerar(med)
        producer.publicar(leitura)

        if leitura.is_anomalia_consumo:
            anomalias += 1
        if leitura.is_tensao_critica:
            tensao_critica += 1
        if leitura.is_possivel_fraude:
            fraudes_detectadas += 1

        if (i + 1) % 1000 == 0:
            logger.info(f"  Progresso: {i+1:,}/{len(catalogo.medidores):,} leituras")

    producer.flush()
    elapsed = time.time() - inicio
    taxa = len(catalogo.medidores) / elapsed

    logger.info("=" * 55)
    logger.info(f"Batch concluído em {elapsed:.1f}s ({taxa:.0f} leituras/s)")
    logger.info(f"Total publicado   : {producer.stats['enviados']:,}")
    logger.info(f"Anomalias consumo : {anomalias:,}")
    logger.info(f"Tensão crítica    : {tensao_critica:,}")
    logger.info(f"Fraudes detectadas: {fraudes_detectadas:,}")
    logger.info("=" * 55)


def modo_stream(
    catalogo: CatalogoMedidores,
    producer: MedidorProducer,
    gerador: GeradorLeitura,
    rate: float = 50.0,
    duration_s: Optional[int] = None,
) -> None:
    """
    Publica leituras continuamente simulando o ritmo AMI.
    Em produção real, cada medidor envia a cada 15 min.
    No simulador, publicamos na taxa configurada e reiniciamos o ciclo.
    """
    intervalo = 1.0 / rate
    inicio = time.time()
    ultimo_log = time.time()
    idx = 0

    rodando = [True]
    def _sair(sig, frame):
        logger.info("Encerrando...")
        rodando[0] = False

    signal.signal(signal.SIGINT, _sair)
    signal.signal(signal.SIGTERM, _sair)

    logger.info(
        f"Modo STREAM — {rate} leituras/s | "
        f"{len(catalogo.medidores):,} medidores | "
        f"ciclo completo a cada {len(catalogo.medidores)/rate/60:.1f} min"
    )

    try:
        while rodando[0]:
            if duration_s and (time.time() - inicio) >= duration_s:
                break

            t0 = time.time()
            med = catalogo.medidores[idx % len(catalogo.medidores)]
            leitura = gerador.gerar(med)
            producer.publicar(leitura)
            idx += 1

            if time.time() - ultimo_log >= 10:
                elapsed = int(time.time() - inicio)
                ciclos = idx // len(catalogo.medidores)
                logger.info(
                    f"[{elapsed}s] Enviadas: {producer.stats['enviados']:,} | "
                    f"Ciclos completos: {ciclos} | "
                    f"Erros: {producer.stats['erros']}"
                )
                ultimo_log = time.time()

            elapsed_ciclo = time.time() - t0
            sleep = max(0, intervalo - elapsed_ciclo)
            if sleep > 0:
                time.sleep(sleep)

    finally:
        producer.flush()
        producer.fechar()
        elapsed = int(time.time() - inicio)
        logger.info(f"Stream encerrado após {elapsed}s | {producer.stats['enviados']:,} leituras enviadas")


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    os.makedirs("logs", exist_ok=True)

    parser = argparse.ArgumentParser(description="Simulador de medidores AMI")
    parser.add_argument("--mode",     choices=["stream", "batch"], default="stream")
    parser.add_argument("--rate",     type=float, default=50.0,  help="Leituras/segundo (stream)")
    parser.add_argument("--duration", type=int,   default=None,  help="Duração em segundos")
    parser.add_argument("--medidores",type=int,   default=10_000,help="Total de medidores")
    parser.add_argument("--seed",     type=int,   default=42)
    parser.add_argument("--dry-run",  action="store_true")
    args = parser.parse_args()

    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    catalogo = CatalogoMedidores(total_medidores=args.medidores, seed=args.seed)
    gerador  = GeradorLeitura(seed=args.seed)
    producer = MedidorProducer(bootstrap_servers=bootstrap, dry_run=args.dry_run)

    if args.mode == "batch":
        modo_batch(catalogo, producer, gerador)
    else:
        modo_stream(catalogo, producer, gerador, rate=args.rate, duration_s=args.duration)