# Fase 1 — Infraestrutura Local

## Objetivo

Preparar o ambiente de desenvolvimento local com:
- Python virtual environment com todas as dependências
- Kafka local via Docker (para desenvolvimento offline)
- Estrutura de pastas do projeto
- Configuração de variáveis de ambiente

---

## Estrutura de Pastas do Projeto

```
adms-smart-grid-platform/
├── .env                          # Variáveis de ambiente (NÃO commitar)
├── .env.example                  # Template das variáveis
├── docker-compose.yml            # Kafka local
├── requirements.txt              # Dependências produção
├── requirements-dev.txt          # Dependências desenvolvimento
├── setup_project.py              # Script de inicialização do projeto
│
├── ingestion/
│   ├── kafka/
│   │   ├── producers/            # Simuladores Python (Fase 2)
│   │   ├── consumers/            # Consumers (placeholder)
│   │   └── schemas/              # Schemas Avro (placeholder)
│   ├── batch/
│   └── eventstream/
│
├── bronze/notebooks/             # Databricks notebooks Bronze (Fase 3)
├── silver/notebooks/             # Databricks notebooks Silver (Fase 4)
├── gold/notebooks/               # Databricks notebooks Gold (Fase 5)
│   └── semantic/                 # Semantic layer (placeholder)
│
├── ml/
│   ├── fault_prediction/         # Predição de falhas (Fase 6)
│   └── loss_detection/           # Detecção de perdas (Fase 6)
│
├── governance/unity_catalog/     # Governança (Fase 7)
├── serving/
│   ├── api/                      # API REST (Fase 8)
│   └── powerbi/                  # Power BI (Fase 8)
│
└── infrastructure/
    ├── terraform/                # IaC (placeholder)
    └── ci_cd/                    # CI/CD (placeholder)
```

---

## Docker Compose — Kafka Local

**Arquivo:** `docker-compose.yml`

Serviços definidos:
| Serviço | Porta | Imagem | Propósito |
|---|---|---|---|
| `adms-zookeeper` | 2181 | `confluentinc/cp-zookeeper:7.6.1` | Coordenação Kafka |
| `adms-kafka` | 9092 | `confluentinc/cp-kafka:7.6.1` | Broker Kafka local |
| `adms-schema-registry` | 8081 | `confluentinc/cp-schema-registry:7.6.1` | Registry de schemas |
| `adms-kafka-ui` | 8080 | `provectuslabs/kafka-ui:latest` | UI de monitoramento |

### Configurações importantes do Kafka local
```yaml
KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"    # Tópicos devem ser criados manualmente
KAFKA_LOG_RETENTION_HOURS: 168              # 7 dias de retenção de mensagens
KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1   # Fator 1 (single broker local)
```

### Listeners configurados
- `PLAINTEXT://kafka:29092` — comunicação interna entre containers
- `PLAINTEXT_HOST://localhost:9092` — acesso externo (da máquina host)

### Comandos de operação

```bash
# Subir infraestrutura
docker compose up -d

# Ver logs do Kafka
docker compose logs kafka -f

# Criar tópicos Kafka manualmente (executar APÓS subir os containers)
docker exec adms-kafka kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic adms.eventos.rede \
  --partitions 6 \
  --replication-factor 1

docker exec adms-kafka kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic adms.leituras.medidor \
  --partitions 12 \
  --replication-factor 1

docker exec adms-kafka kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic adms.alarmes \
  --partitions 3 \
  --replication-factor 1

# Listar tópicos
docker exec adms-kafka kafka-topics --list --bootstrap-server localhost:9092

# Parar infraestrutura
docker compose down
```

---

## Python Virtual Environment

### Criar e ativar (Windows PowerShell)
```powershell
# Na pasta adms-smart-grid-platform/
python -m venv adms-venv
.\adms-venv\Scripts\Activate.ps1

# Instalar dependências
pip install -r requirements.txt
pip install -r requirements-dev.txt
```

### requirements.txt
```
pyspark>=3.5.1
delta-spark>=3.2.0
kafka-python>=2.0.2          # Biblioteca usada nos producers (NÃO confluent-kafka)
confluent-kafka>=2.4.0       # Bloqueada por DLL em Windows — usar kafka-python
fastavro>=1.9.4
mlflow>=2.13.0
scikit-learn>=1.4.2
xgboost>=2.0.3
pandas>=2.2.2
pyarrow>=16.0.0
great-expectations>=0.18.19
requests>=2.32.3
python-dotenv>=1.0.1
faker>=25.0.0
loguru>=0.7.2
```

> ⚠️ **IMPORTANTE — Windows**: A `confluent-kafka` usa DLLs nativas que costumam ser bloqueadas por antivírus corporativos no Windows. Os producers usam `kafka-python` como alternativa compatível. Não trocar para `confluent-kafka` nos producers sem testar.

---

## Configuração de Variáveis de Ambiente

**Arquivo:** `.env` (raiz do projeto)

### Modo Confluent Cloud (produção/Databricks)
```env
KAFKA_BOOTSTRAP_SERVERS=pkc-XXXXX.region.aws.confluent.cloud:9092
KAFKA_SASL_USERNAME=<API_KEY_CONFLUENT>
KAFKA_SASL_PASSWORD=<API_SECRET_CONFLUENT>
KAFKA_SECURITY_PROTOCOL=SASL_SSL
KAFKA_SASL_MECHANISM=PLAIN
```

### Modo Kafka Local (desenvolvimento)
```env
# Comentar as linhas do Confluent e descomentar:
# KAFKA_BOOTSTRAP_SERVERS=localhost:9092
```

### Databricks
```env
DATABRICKS_HOST=https://dbc-XXXXX.cloud.databricks.com
DATABRICKS_TOKEN=dapi...
DATABRICKS_CATALOG=adms
DATABRICKS_SCHEMA_BRONZE=bronze
DATABRICKS_SCHEMA_SILVER=silver
DATABRICKS_SCHEMA_GOLD=gold
```

### Simulador
```env
SIMULATOR_EVENTS_PER_SECOND=10
SIMULATOR_NUM_METERS=10000
SIMULATOR_SEED=42              # Seed para reprodutibilidade dos dados gerados
```

---

## Tópicos Kafka do Projeto

| Tópico | Partições sugeridas | Chave de particionamento | Conteúdo |
|---|---|---|---|
| `adms.eventos.rede` | 6 | `subestacao_id` | Eventos de rede (falhas, manobras, alarmes) |
| `adms.leituras.medidor` | 12 | `alimentador_id` | Leituras AMI de medidores inteligentes |
| `adms.alarmes` | 3 | `subestacao_id` | Alarmes críticos |

### Lógica de particionamento
- **Por subestação**: garante que todos os eventos de uma subestação ficam na mesma partição → ordenação temporal preservada por subestação
- **Por alimentador**: leituras de medidores do mesmo alimentador ficam juntas → facilita queries por alimentador

---

## Databricks Free Edition — Setup Unity Catalog

Execute no primeiro notebook Bronze (célula 2):

```python
spark.sql("CREATE CATALOG IF NOT EXISTS adms")
spark.sql("USE CATALOG adms")
spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS gold")
spark.sql("CREATE VOLUME IF NOT EXISTS adms.bronze.checkpoints")
```

---

## Problemas conhecidos e soluções

### `confluent-kafka` bloqueada no Windows
- **Sintoma**: ImportError ou DLL load failed
- **Solução**: usar `kafka-python` (já implementado nos producers)

### Kafka local não aceita conexão
- **Sintoma**: `NoBrokersAvailable`
- **Solução**: aguardar 30-60s após `docker compose up` antes de conectar; os producers já implementam retry automático com 5 tentativas

### UnicodeEncodeError no terminal Windows
- **Sintoma**: erro ao imprimir caracteres especiais (✓, ─, etc.) no PowerShell
- **Solução**: usar somente ASCII puro no painel de monitoramento (já corrigido no `run_simuladores.py`)
