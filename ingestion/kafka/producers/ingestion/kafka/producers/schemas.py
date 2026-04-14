"""
Schemas dos eventos Kafka — ADMS Smart Grid Platform
Define a estrutura de cada tipo de mensagem publicada nos tópicos.
Usado por producers e consumers para garantir consistência.
"""

from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from enum import Enum
from typing import Optional
import uuid


# ---------------------------------------------------------------------------
# Enums — domínio do negócio elétrico
# ---------------------------------------------------------------------------

class TipoEvento(str, Enum):
    """Tipos de evento gerados pelo sistema ADMS/SCADA."""
    FALHA_EQUIPAMENTO     = "FALHA_EQUIPAMENTO"
    RELIGAMENTO_AUTOMATICO = "RELIGAMENTO_AUTOMATICO"
    MANOBRA_PLANEJADA     = "MANOBRA_PLANEJADA"
    MANOBRA_EMERGENCIAL   = "MANOBRA_EMERGENCIAL"
    ANOMALIA_TENSAO       = "ANOMALIA_TENSAO"
    ANOMALIA_CORRENTE     = "ANOMALIA_CORRENTE"
    INTERRUPÇÃO_INICIO    = "INTERRUPCAO_INICIO"
    INTERRUPÇÃO_FIM       = "INTERRUPCAO_FIM"
    ALARME_SOBRECARGA     = "ALARME_SOBRECARGA"
    ALARME_TEMPERATURA    = "ALARME_TEMPERATURA"


class StatusEvento(str, Enum):
    ATIVO      = "ATIVO"
    RESOLVIDO  = "RESOLVIDO"
    EM_ANALISE = "EM_ANALISE"
    IGNORADO   = "IGNORADO"


class SeveridadeEvento(str, Enum):
    CRITICO  = "CRITICO"   # Interrupção de fornecimento
    ALTO     = "ALTO"      # Risco de interrupção iminente
    MEDIO    = "MEDIO"     # Degradação de qualidade
    BAIXO    = "BAIXO"     # Informativo / monitoramento


class ClasseInterrupcao(str, Enum):
    """Classificação ANEEL para cálculo de DEC/FEC."""
    NAO_PLANEJADA     = "NAO_PLANEJADA"
    PLANEJADA         = "PLANEJADA"
    EMERGENCIAL       = "EMERGENCIAL"
    TERCEIROS         = "TERCEIROS"
    CASO_FORTUITO     = "CASO_FORTUITO"


class TipoMedidor(str, Enum):
    RESIDENCIAL  = "RESIDENCIAL"
    COMERCIAL    = "COMERCIAL"
    INDUSTRIAL   = "INDUSTRIAL"
    RURAL        = "RURAL"
    PODER_PUBLICO = "PODER_PUBLICO"


# ---------------------------------------------------------------------------
# Schema: Evento de Rede (tópico adms.eventos.rede)
# ---------------------------------------------------------------------------

@dataclass
class EventoRede:
    """
    Evento gerado pelo sistema ADMS/SCADA ao detectar uma ocorrência na rede.
    Publicado no tópico: adms.eventos.rede
    Particionado por: subestacao_id (garante ordem por subestação)
    """
    # Identificação
    evento_id: str
    tipo_evento: str                    # TipoEvento
    severidade: str                     # SeveridadeEvento
    status: str                         # StatusEvento

    # Localização na rede
    subestacao_id: str
    subestacao_nome: str
    alimentador_id: str
    alimentador_nome: str
    equipamento_id: str
    tipo_equipamento: str
    regional: str
    municipio: str

    # Medições no momento do evento
    tensao_kv: float
    corrente_a: float
    potencia_mw: float
    fator_potencia: float

    # Impacto
    clientes_afetados: int
    classe_interrupcao: str             # ClasseInterrupcao
    duracao_estimada_min: int

    # Contexto climático
    temperatura_c: float
    umidade_pct: float
    precipitacao_mm: float
    velocidade_vento_kmh: float

    # Timestamps (UTC ISO-8601)
    timestamp_evento: str
    timestamp_ingestao: str

    # Metadados técnicos
    fonte_sistema: str = "ADMS-SCADA"
    versao_schema: str = "1.0.0"

    def to_dict(self) -> dict:
        return asdict(self)

    def to_kafka_key(self) -> str:
        """Chave de particionamento — garante ordem por subestação."""
        return self.subestacao_id


# ---------------------------------------------------------------------------
# Schema: Leitura de Medidor AMI (tópico adms.leituras.medidor)
# ---------------------------------------------------------------------------

@dataclass
class LeituraMedidor:
    """
    Leitura periódica de medidor inteligente (AMI) a cada 15 minutos.
    Publicado no tópico: adms.leituras.medidor
    Particionado por: alimentador_id
    """
    # Identificação
    leitura_id: str
    medidor_id: str
    cliente_id: str
    tipo_medidor: str                   # TipoMedidor

    # Localização
    alimentador_id: str
    subestacao_id: str
    regional: str
    municipio: str
    latitude: float
    longitude: float

    # Medições elétricas
    kwh_ativo: float                    # Energia ativa acumulada
    kwh_reativo: float                  # Energia reativa acumulada
    kw_demanda: float                   # Demanda instantânea
    tensao_v: float                     # Tensão de fornecimento
    corrente_a: float
    fator_potencia: float

    # Qualidade do fornecimento
    num_interrupcoes: int               # Interrupções desde última leitura
    duracao_interrupcao_min: int        # Duração total de interrupções
    tensao_minima_v: float
    tensao_maxima_v: float

    # Flags de anomalia
    is_leitura_estimada: bool = False   # True se medidor não comunicou
    is_anomalia_consumo: bool = False   # True se consumo fora do padrão
    is_tensao_critica: bool = False     # True se tensão fora dos limites ANEEL
    is_possivel_fraude: bool = False    # True se padrão suspeito

    # Timestamps
    timestamp_leitura: str = ""
    timestamp_ingestao: str = ""

    # Metadados
    fonte_sistema: str = "AMI-MEDIDOR"
    versao_schema: str = "1.0.0"

    def to_dict(self) -> dict:
        return asdict(self)

    def to_kafka_key(self) -> str:
        return self.alimentador_id


# ---------------------------------------------------------------------------
# Schema: Alarme (tópico adms.alarmes)
# ---------------------------------------------------------------------------

@dataclass
class Alarme:
    """
    Alarme crítico gerado pelo sistema de proteção da rede.
    Publicado no tópico: adms.alarmes
    Particionado por: subestacao_id
    """
    alarme_id: str
    tipo_alarme: str
    severidade: str
    descricao: str

    subestacao_id: str
    alimentador_id: str
    equipamento_id: str

    valor_medido: float
    valor_limite: float
    unidade: str

    acao_recomendada: str
    requer_desligamento: bool

    timestamp_alarme: str
    timestamp_ingestao: str

    evento_id_origem: Optional[str] = None
    fonte_sistema: str = "ADMS-PROTECAO"
    versao_schema: str = "1.0.0"

    def to_dict(self) -> dict:
        return asdict(self)

    def to_kafka_key(self) -> str:
        return self.subestacao_id


# ---------------------------------------------------------------------------
# Factory functions — constroem objetos com defaults sensatos
# ---------------------------------------------------------------------------

def novo_evento_id() -> str:
    return f"EVT-{uuid.uuid4().hex[:12].upper()}"

def nova_leitura_id() -> str:
    return f"LTR-{uuid.uuid4().hex[:12].upper()}"

def novo_alarme_id() -> str:
    return f"ALM-{uuid.uuid4().hex[:8].upper()}"

def timestamp_utc_agora() -> str:
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# Validação básica
# ---------------------------------------------------------------------------

def validar_tensao_aneel(tensao_v: float, tensao_nominal_v: float = 220.0) -> bool:
    """
    Verifica se a tensão está dentro dos limites ANEEL (PRODIST Módulo 8).
    Faixa adequada: ±5% da tensão nominal (208V a 231V para 220V).
    Faixa precária: ±10% (198V a 242V).
    """
    limite_inf_adequado = tensao_nominal_v * 0.95
    limite_sup_adequado = tensao_nominal_v * 1.05
    return limite_inf_adequado <= tensao_v <= limite_sup_adequado


if __name__ == "__main__":
    import json

    print("=== Validando schemas ===\n")

    evento = EventoRede(
        evento_id=novo_evento_id(),
        tipo_evento=TipoEvento.FALHA_EQUIPAMENTO,
        severidade=SeveridadeEvento.CRITICO,
        status=StatusEvento.ATIVO,
        subestacao_id="SE01",
        subestacao_nome="SE Cataguases",
        alimentador_id="SE01-AL01",
        alimentador_nome="Alimentador Cataguases 01",
        equipamento_id="SE01-AL01-RL001",
        tipo_equipamento="RELIGADOR",
        regional="Zona da Mata",
        municipio="Cataguases",
        tensao_kv=13.1,
        corrente_a=245.3,
        potencia_mw=5.2,
        fator_potencia=0.92,
        clientes_afetados=1450,
        classe_interrupcao=ClasseInterrupcao.NAO_PLANEJADA,
        duracao_estimada_min=45,
        temperatura_c=28.5,
        umidade_pct=78.0,
        precipitacao_mm=12.3,
        velocidade_vento_kmh=35.0,
        timestamp_evento=timestamp_utc_agora(),
        timestamp_ingestao=timestamp_utc_agora(),
    )

    print("EventoRede:")
    print(json.dumps(evento.to_dict(), indent=2, ensure_ascii=False))
    print(f"\nKafka key: {evento.to_kafka_key()}")
    print(f"\nTensão 210V adequada (ANEEL): {validar_tensao_aneel(210.0)}")
    print(f"Tensão 195V adequada (ANEEL): {validar_tensao_aneel(195.0)}")