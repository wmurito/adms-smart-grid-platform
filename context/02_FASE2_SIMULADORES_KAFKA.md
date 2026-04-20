# Fase 2 — Simuladores Kafka (Producers)

## Objetivo

Gerar dados sintéticos realistas de uma rede elétrica e publicá-los nos tópicos Kafka. Os simuladores rodam localmente (ou em qualquer máquina) e servem como fonte de dados para o pipeline.

---

## Arquivos desta fase

| Arquivo | Localização | Propósito |
|---|---|---|
| `schemas.py` | `ingestion/kafka/producers/` | Definição dos dataclasses (contratos de dados) |
| `topologia.py` | `ingestion/kafka/producers/` | Dados mestres da rede elétrica simulada |
| `kafka_config.py` | `ingestion/kafka/producers/` | Configuração centralizada do Kafka |
| `evento_rede_producer.py` | `ingestion/kafka/producers/` | Producer de eventos de rede |
| `medidor_producer.py` | `ingestion/kafka/producers/` | Producer de leituras de medidores AMI |
| `run_simuladores.py` | `ingestion/kafka/producers/` | Orquestrador: roda tudo em paralelo |

---

## Módulo: `topologia.py`

### O que faz
Define a **rede elétrica simulada** como dados mestres em memória Python. Representa uma distribuidora real de MG.

### Estruturas de dados

```python
@dataclass
class Subestacao:
    id: str              # Ex: "SE01"
    nome: str            # Ex: "SE Cataguases"
    regional: str        # "Zona da Mata" | "Campo das Vert." | "Sul de Minas"
    municipio: str
    latitude: float
    longitude: float
    tensao_kv: int       # 69 | 138 | 230 kV
    capacidade_mva: float
    ano_instalacao: int

@dataclass
class Alimentador:
    id: str              # Ex: "SE01-AL01"
    subestacao_id: str
    tipo_area: str       # "URBANA" | "SUBURBANA" | "RURAL"
    num_clientes: int    # Varia: URBANA 3k-8k, SUBURBANA 1.5k-4k, RURAL 200-1.5k
    extensao_km: float
    num_chaves: int
    num_religadores: int

@dataclass
class Equipamento:
    id: str              # Ex: "SE01-AL01-CF001"
    tipo: str            # "CHAVE_FACA" | "RELIGADOR" | "TRANSFORMADOR"
    alimentador_id: str
    subestacao_id: str
    fabricante: str
    ano_fabricacao: int
```

### Dados gerados com seed fixo (42)
- **20 subestações** com dados reais de MG (coordenadas geográficas corretas)
- **120 alimentadores** gerados proceduralmente (6 por subestação: 3 URBANA, 2 SUBURBANA, 1 RURAL)
- **~1.500 equipamentos** (chaves + religadores + transformadores por alimentador)

### Índices de acesso rápido
```python
SUBESTACAO_MAP = {se.id: se for se in SUBESTACOES}   # O(1) lookup por ID
ALIMENTADOR_MAP = {al.id: al for al in ALIMENTADORES}
EQUIPAMENTO_MAP = {eq.id: eq for eq in EQUIPAMENTOS}
```

### Helpers
```python
total_clientes()                           # Soma de clientes de todos os alimentadores
alimentadores_por_subestacao(se_id)        # List[Alimentador]
equipamentos_por_alimentador(al_id)        # List[Equipamento]
```

---

## Módulo: `schemas.py`

### O que faz
Define os **contratos de dados** (schemas) de cada mensagem Kafka como Python dataclasses. É o "espelho Python" dos schemas que serão definidos no Databricks.

### Enums do domínio elétrico

```python
class TipoEvento(str, Enum):
    FALHA_EQUIPAMENTO       # 15% dos eventos
    RELIGAMENTO_AUTOMATICO  # 10%
    MANOBRA_PLANEJADA       # 12%
    MANOBRA_EMERGENCIAL     # 8%
    ANOMALIA_TENSAO         # 18%
    ANOMALIA_CORRENTE       # 12%
    INTERRUPÇÃO_INICIO      # 10% → valor: "INTERRUPCAO_INICIO" (sem acento)
    INTERRUPÇÃO_FIM         # 7%  → valor: "INTERRUPCAO_FIM"
    ALARME_SOBRECARGA       # 5%
    ALARME_TEMPERATURA      # 3%

class SeveridadeEvento(str, Enum):
    CRITICO  # → Interrupção de fornecimento
    ALTO     # → Risco iminente
    MEDIO    # → Degradação de qualidade
    BAIXO    # → Informativo

class ClasseInterrupcao(str, Enum):     # Conforme PRODIST ANEEL
    NAO_PLANEJADA
    PLANEJADA
    EMERGENCIAL
    TERCEIROS
    CASO_FORTUITO

class TipoMedidor(str, Enum):
    RESIDENCIAL | COMERCIAL | INDUSTRIAL | RURAL | PODER_PUBLICO
```

### Schema: EventoRede (tópico `adms.eventos.rede`)
```python
@dataclass
class EventoRede:
    # Identificação
    evento_id: str           # "EVT-<12hex>" — gerado por novo_evento_id()
    tipo_evento: str
    severidade: str
    status: str              # Sempre "ATIVO" ao sair do producer

    # Localização
    subestacao_id, subestacao_nome
    alimentador_id, alimentador_nome  
    equipamento_id, tipo_equipamento
    regional, municipio

    # Medições elétricas no momento do evento
    tensao_kv: float
    corrente_a: float
    potencia_mw: float
    fator_potencia: float

    # Impacto
    clientes_afetados: int
    classe_interrupcao: str
    duracao_estimada_min: int

    # Contexto climático (Minas Gerais)
    temperatura_c, umidade_pct, precipitacao_mm, velocidade_vento_kmh

    # Timestamps ISO-8601 UTC
    timestamp_evento: str
    timestamp_ingestao: str

    # Particionamento Kafka
    def to_kafka_key(self) -> str:
        return self.subestacao_id    # Garante ordenação por subestação
```

### Schema: LeituraMedidor (tópico `adms.leituras.medidor`)
```python
@dataclass
class LeituraMedidor:
    leitura_id: str          # "LTR-<12hex>"
    medidor_id: str          # "MED-<SE_ID>-<000001>"
    cliente_id: str          # "CLI-<00000001>"
    tipo_medidor: str

    alimentador_id, subestacao_id, regional, municipio
    latitude, longitude

    # Medições elétricas (intervalo 15 min)
    kwh_ativo: float         # Energia ativa no intervalo
    kwh_reativo: float
    kw_demanda: float        # Demanda instantânea
    tensao_v: float
    corrente_a: float
    fator_potencia: float

    # Qualidade ANEEL
    num_interrupcoes: int
    duracao_interrupcao_min: int
    tensao_minima_v, tensao_maxima_v: float

    # Flags de anomalia
    is_leitura_estimada: bool   # True = medidor não comunicou (estimada)
    is_anomalia_consumo: bool
    is_tensao_critica: bool     # Fora dos ±5% ANEEL
    is_possivel_fraude: bool    # Detectado 30% das fraudes simuladas

    def to_kafka_key(self) -> str:
        return self.alimentador_id   # Ordenação por alimentador
```

### Schema: Alarme (tópico `adms.alarmes`)
```python
@dataclass
class Alarme:
    alarme_id: str           # "ALM-<8hex>"
    tipo_alarme: str         # "ALARME_<TIPO_EVENTO>"
    severidade: str
    descricao: str
    subestacao_id, alimentador_id, equipamento_id
    valor_medido, valor_limite: float
    unidade: str             # "A" (ampères)
    acao_recomendada: str
    requer_desligamento: bool
    evento_id_origem: str    # Rastreabilidade para o evento original
```

### Funções utilitárias
```python
novo_evento_id()   → "EVT-A1B2C3D4E5F6"
nova_leitura_id()  → "LTR-A1B2C3D4E5F6"
novo_alarme_id()   → "ALM-A1B2C3D4"
timestamp_utc_agora() → "2026-04-19T20:38:20+00:00"

# Validação regulatória ANEEL (PRODIST Módulo 8)
validar_tensao_aneel(tensao_v=210.0, tensao_nominal_v=220.0)
# Faixa adequada: ±5% (209V-231V para 220V)
# Retorna True se dentro da faixa
```

---

## Módulo: `kafka_config.py`

### O que faz
Configuração centralizada que **detecta automaticamente** se deve usar Confluent Cloud (SASL_SSL) ou Kafka local (PLAINTEXT) com base nas variáveis de ambiente.

### Lógica de detecção automática
```python
def is_confluent_cloud() -> bool:
    # Retorna True se KAFKA_SASL_USERNAME e KAFKA_SASL_PASSWORD estiverem preenchidos
    # E o bootstrap server contiver "confluent.cloud"
    return (
        os.getenv("KAFKA_SASL_USERNAME", "") != "" and
        os.getenv("KAFKA_SASL_PASSWORD", "") != "" and
        "confluent.cloud" in os.getenv("KAFKA_BOOTSTRAP_SERVERS", "")
    )
```

### Uso nos producers
```python
from kafka_config import get_producer_config

# Config automática (local ou cloud)
cfg = get_producer_config(acks="all", compression="gzip", batch_size=16384, linger_ms=10)
producer = KafkaProducer(**cfg, value_serializer=..., key_serializer=...)
```

### Configurações retornadas
```python
# Kafka local (PLAINTEXT)
{
    "bootstrap_servers": "localhost:9092",
    "acks": "all",
    "retries": 3,
    "retry_backoff_ms": 500,
    "compression_type": "gzip",
    "batch_size": 16384,
    "linger_ms": 10,
    "request_timeout_ms": 30000,
}

# Confluent Cloud (SASL_SSL) — adicional
{
    ...base...,
    "security_protocol": "SASL_SSL",
    "sasl_mechanism": "PLAIN",
    "sasl_plain_username": "<API_KEY>",
    "sasl_plain_password": "<API_SECRET>",
    "ssl_check_hostname": True,
}
```

### Configuração para Spark (Databricks)
```python
get_spark_kafka_options(topic="adms.eventos.rede", group_id="databricks-bronze")
# Retorna dict com "kafka." prefix que o Spark readStream espera
# Para Confluent Cloud, inclui JAAS config com PlainLoginModule
```

---

## Módulo: `evento_rede_producer.py`

### O que faz
Gera e publica eventos sintéticos de rede elétrica no tópico `adms.eventos.rede`.

### Classe principal: `ADMSEventoProducer`
```python
class ADMSEventoProducer:
    def __init__(self, bootstrap_servers: str, dry_run: bool = False):
        # dry_run=True → não conecta ao Kafka (modo teste)
        # cfg = get_producer_config() → configuração automática cloud/local
        ...

    def publicar_evento(self, evento: EventoRede) -> bool:
        # Publica no tópico adms.eventos.rede
        # key = evento.subestacao_id (particionamento)
        # value = JSON serializado
        ...

    def publicar_alarme(self, alarme: Alarme) -> bool:
        # Publica no tópico adms.alarmes
        ...

    def fechar(self):
        # flush() + close() do producer
        ...
```

### Classe geradora: `EventoRedeGerador`
```python
class EventoRedeGerador:
    def __init__(self, seed: int = 42):
        random.seed(seed)   # Reprodutibilidade

    def gerar_evento(self) -> EventoRede:
        # Seleciona alimentador aleatório
        # Seleciona tipo de evento por distribuição de probabilidade
        # Gera contexto climático realista para MG (temperatura por hora do dia)
        # Calcula impacto em clientes (CRITICO = 0 a 100%, outros = 0 a 25%)
        ...
```

### Função de conversão: `evento_para_alarme()`
```python
def evento_para_alarme(evento: EventoRede) -> "Alarme | None":
    # Gera alarme apenas para eventos CRITICO ou ALTO
    # Retorna None para MEDIO e BAIXO
    ...
```

### Distribuição de probabilidade dos eventos
```python
_TIPOS_PESOS = {
    ANOMALIA_TENSAO:          0.18,   # mais comum
    FALHA_EQUIPAMENTO:        0.15,
    MANOBRA_PLANEJADA:        0.12,
    ANOMALIA_CORRENTE:        0.12,
    RELIGAMENTO_AUTOMATICO:   0.10,
    INTERRUPÇÃO_INICIO:       0.10,
    MANOBRA_EMERGENCIAL:      0.08,
    INTERRUPÇÃO_FIM:          0.07,
    ALARME_SOBRECARGA:        0.05,
    ALARME_TEMPERATURA:       0.03,
}
```

### Modo de execução standalone
```bash
python evento_rede_producer.py                     # produção contínua (1 ev/s)
python evento_rede_producer.py --dry-run           # sem Kafka, só imprime
python evento_rede_producer.py --rate 5            # 5 eventos/segundo
python evento_rede_producer.py --duration 60       # roda por 60 segundos
```

---

## Módulo: `medidor_producer.py`

### O que faz
Simula 10.000 medidores inteligentes (AMI) com perfis de consumo realistas e publica leituras em `adms.leituras.medidor`.

### Perfis de consumo por tipo de medidor
```python
PERFIS = {
    "RESIDENCIAL": PerfilConsumo(
        kwh_dia_base=12.0,       # consumo médio diário
        kwh_dia_variacao=6.0,    # desvio padrão
        tensao_nominal_v=220.0,
        demanda_pico_kw=3.5,
        fator_potencia_base=0.92,
        prob_fraude=0.015,        # 1.5% dos residenciais fraudam
    ),
    "COMERCIAL":  kwh_dia_base=85.0,  prob_fraude=0.008
    "INDUSTRIAL": kwh_dia_base=850.0, prob_fraude=0.003
    "RURAL":      kwh_dia_base=8.0,   prob_fraude=0.025  # maior fraude rural
    "PODER_PUBLICO": kwh_dia_base=45.0, prob_fraude=0.001
}
```

### Curvas de carga horárias
Índice 0-23 representa a hora do dia; valor é multiplicador sobre consumo base:
```python
CURVA_CARGA_RESIDENCIAL = [
    0.30, 0.25, 0.22, 0.20, ...,  # madrugada (baixo)
    1.00, 0.95, 0.85, ...          # pico às 18h
]
CURVA_CARGA_COMERCIAL = [
    0.10, 0.08, ...,               # madrugada (mínimo)
    1.00, 0.95, 0.90, ...          # pico 9-10h manhã
]
CURVA_CARGA_INDUSTRIAL = [
    0.70, 0.70, ...,               # operação 3 turnos (mais constante)
]
```

### Classes de gerenciamento
```python
class CatalogoMedidores:
    # Gera e mantém todos os medidores em memória
    # Distribui medidores entre alimentadores proporcionalmente ao num_clientes
    # Mix por tipo de área (URBANA: 65% residencial, RURAL: 40% rural)
    def __init__(self, total_medidores: int = 10_000, seed: int = 42):
        ...
    # medidores: List[CadastroMedidor]

class GeradorLeitura:
    # Gera uma leitura realista para um medidor no instante atual
    # Aplica curva de carga horária
    # Simula fraude (reduz 20-60% do consumo real)
    # Simula anomalias de tensão (5% das leituras)
    # Simula falhas de comunicação (1% → is_leitura_estimada=True)
    def gerar(self, med: CadastroMedidor, intervalo_min: int = 15) -> LeituraMedidor:
        ...
```

### Mix de tipos por área (distribuição dos 10k medidores)
```
URBANA:    65% Residencial, 25% Comercial,  5% Industrial, 5% Poder Público
SUBURBANA: 75% Residencial, 15% Comercial,  7% Rural,      3% Poder Público
RURAL:     50% Residencial, 40% Rural,       5% Comercial,  5% Poder Público
```

### Modos de execução
```bash
# Modo STREAM — publicação contínua (padrão quando run como módulo)
python medidor_producer.py --mode stream --rate 50
python medidor_producer.py --mode stream --dry-run  # sem Kafka

# Modo BATCH — gera um ciclo completo (todos os medidores) e sai
python medidor_producer.py --mode batch

# Parâmetros disponíveis
--mode      stream | batch          (padrão: stream)
--rate      leituras/segundo        (padrão: 50.0)
--duration  segundos (None=infinito)
--medidores total de medidores      (padrão: 10000)
--seed      seed randômica          (padrão: 42)
--dry-run   sem publicar no Kafka
```

---

## Orquestrador: `run_simuladores.py`

### O que faz
Roda ambos os producers **em paralelo** via threads Python e exibe um painel de monitoramento no terminal.

### Arquitetura de threads
```
Thread-Eventos   → EventoRedeGerador + ADMSEventoProducer
Thread-Medidores → CatalogoMedidores + GeradorLeitura + MedidorProducer  
Thread-Painel    → Imprime estatísticas a cada 5s (cls/clear da tela)
Thread-Main      → Aguarda SIGINT/SIGTERM ou duração configurada
```

### Classe `EstadoSimulador` (estado compartilhado, thread-safe)
```python
class EstadoSimulador:
    _lock = threading.Lock()     # Protege acesso concorrente

    # Contadores de eventos
    eventos_gerados, eventos_enviados, alarmes_enviados, eventos_erros

    # Contadores de medidores
    leituras_geradas, leituras_enviadas, leituras_erros

    # Métricas de negócio acumuladas
    clientes_afetados_acum, anomalias_consumo, tensao_critica, fraudes_detectadas

    def snapshot(self) -> dict:   # Cópia thread-safe do estado atual
        ...
```

### Comandos de execução
```bash
# Modo padrão (2 ev/s + 50 leit/s + 10k medidores)
python run_simuladores.py

# Modo dry-run (sem Kafka — só conta e imprime)
python run_simuladores.py --dry-run

# Customizado
python run_simuladores.py --eventos-rate 5 --medidores-rate 200 --duration 120

# Parâmetros disponíveis
--eventos-rate    eventos por segundo    (padrão: 2.0)
--medidores-rate  leituras por segundo   (padrão: 50.0)
--medidores       total de medidores     (padrão: 10000)
--duration        segundos (None=inf)
--seed            seed randômica         (padrão: 42)
--dry-run         não publicar no Kafka
```

### Encerramento gracioso
- `Ctrl+C` → SIGINT → `estado.rodando = False` → threads finalizam + flush Kafka

---

## Fluxo de dados dos simuladores

```
topologia.py (dados mestres)
    ↓
evento_rede_producer.py
    gerar_evento_rede()
        → seleciona alimentador (ALIMENTADORES)
        → seleciona tipo por probabilidade (_TIPOS_PESOS)
        → gera contexto climático (temperatura por hora)
        → calcula clientes afetados
        → EventoRede (dataclass)
    ADMSEventoProducer.publicar_evento()
        → JSON serialize
        → kafka.send("adms.eventos.rede", key=subestacao_id, value=JSON)
    evento_para_alarme()
        → se CRITICO ou ALTO → Alarme
        → kafka.send("adms.alarmes", ...)

medidor_producer.py
    CatalogoMedidores.__init__()
        → distribui medidores por alimentador
        → define is_fraude_simulada por probabilidade do perfil
    GeradorLeitura.gerar(med)
        → aplica curva de carga horária
        → se fraude → reporta 20-60% do real
        → gera anomalias de tensão (5%)
        → LeituraMedidor (dataclass)
    MedidorProducer.publicar()
        → JSON serialize
        → kafka.send("adms.leituras.medidor", key=alimentador_id, value=JSON)
```

---

## Problemas conhecidos

### Dupla serialização (bug já corrigido)
- **Problema**: chamar `json.dumps()` no producer e ter `value_serializer=json.dumps` ao mesmo tempo → string duplamente escapada
- **Solução atual**: o `value_serializer` no `KafkaProducer` faz o JSON. O `to_dict()` do dataclass retorna dict. Não serializar antes de passar para `.send()`.

### `_on_delivery` no `evento_rede_producer.py`
- Existe código legado do `confluent_kafka` (`run()` function) que usa `producer.produce()` e callbacks `_on_delivery`
- **Esse código NÃO é usado** pelo orquestrador — o orquestrador usa a classe `ADMSEventoProducer` que usa `kafka-python`
- Pode ser removido em refatoração futura
