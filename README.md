# ADMS Smart Grid Data Platform

> Plataforma de dados para distribuidoras de energia elétrica — ingestão em tempo real, processamento medallion e KPIs regulatórios ANEEL automatizados.

[![Pipeline](https://img.shields.io/badge/pipeline-passing-639922?style=flat-square)]()
[![Delta Lake](https://img.shields.io/badge/Delta_Lake-3.2-003087?style=flat-square)](https://delta.io)
[![Databricks](https://img.shields.io/badge/Databricks-Free_Edition-FF3621?style=flat-square)](https://databricks.com)
[![Kafka](https://img.shields.io/badge/Confluent_Cloud-Kafka-231F20?style=flat-square)](https://confluent.io)
[![Python](https://img.shields.io/badge/Python-3.11-3776AB?style=flat-square)](https://python.org)

---

## Visão geral

O **ADMS** (*Advanced Distribution Management System*) é o sistema que distribuidoras de energia elétrica usam para monitorar e controlar a rede em tempo real — detectando falhas, coordenando religamentos automáticos e calculando os indicadores regulatórios exigidos pela ANEEL.

Este projeto constrói a infraestrutura de dados que alimenta esse "cérebro": um pipeline end-to-end que ingere eventos do ADMS/SCADA em tempo real, processa leituras de 9.994 medidores inteligentes (AMI), calcula DEC/FEC regulatório e gera índices de saúde de ativos — tudo orquestrado automaticamente via Databricks Workflows com Asset Bundles.

### Números do projeto

| Métrica | Valor |
|---|---|
| Subestações simuladas | 20 (3 regionais de MG) |
| Alimentadores | 120 |
| Equipamentos mapeados | 2.147 |
| Medidores inteligentes (AMI) | 9.994 |
| Clientes cobertos | 453.238 |
| Leituras AMI processadas | 89.086 |
| Throughput médio | 52 msg/s |
| Tempo pipeline end-to-end | ~13 minutos |

---

## Arquitetura

```
┌──────────────────────────────────────────────────────────────┐
│  Simulador Python → Confluent Cloud Kafka                    │
│  3 tópicos │ 21 partições │ SASL/SSL                         │
└───────────────────────────┬──────────────────────────────────┘
                            │ Spark Structured Streaming
┌───────────────────────────▼──────────────────────────────────┐
│  Bronze — Delta Lake                                          │
│  eventos_rede_raw │ leituras_medidor_raw                      │
│  Dado bruto + metadados Kafka + _batch_id                    │
└───────────────────────────┬──────────────────────────────────┘
                            │ dedup + enrich + classify
┌───────────────────────────▼──────────────────────────────────┐
│  Silver — Delta Lake                                          │
│  eventos_rede_enriquecido │ leituras_medidor_norm             │
│  Topologia │ ANEEL PRODIST M8 │ Score criticidade │ Fraude   │
└───────────────────────────┬──────────────────────────────────┘
                            │ aggregate + kpis
┌───────────────────────────▼──────────────────────────────────┐
│  Gold — Delta Lake                                            │
│  kpi_dec_fec_mensal │ saude_ativo_alimentador                 │
│  perdas_comerciais_regional                                   │
└───────────────────────────┬──────────────────────────────────┘
                            │ Databricks Asset Bundles
┌───────────────────────────▼──────────────────────────────────┐
│  Orquestração — Databricks Workflows                          │
│  4 Jobs │ 8 Tasks │ dependências │ deploy via CLI            │
└──────────────────────────────────────────────────────────────┘
```

### Stack tecnológica

| Camada | Tecnologia |
|---|---|
| Streaming | Apache Kafka — Confluent Cloud 7.6 |
| Processamento | Apache Spark — Databricks Serverless 15.4 LTS |
| Armazenamento | Delta Lake 3.2 |
| Catálogo | Databricks Unity Catalog |
| Orquestração | Databricks Workflows + Asset Bundles |
| Linguagem | Python 3.11 / PySpark / SQL |
| IaC | Databricks Asset Bundles (YAML) |
| Kafka local | Docker Compose (dev) |
| Versionamento | Git + GitHub |

---

## Tópicos Kafka

| Tópico | Partições | Retenção | Conteúdo |
|---|---|---|---|
| `adms.eventos.rede` | 6 | 7 dias | Falhas, manobras, alarmes, interrupções |
| `adms.leituras.medidor` | 12 | 3 dias | Leituras AMI a cada 15 min |
| `adms.alarmes` | 3 | 7 dias | Alarmes críticos de proteção |

Particionamento por `subestacao_id` garante ordenação temporal por subestação — requisito para cálculo correto de DEC/FEC.

---

## Camadas de dados

### Bronze — dado bruto fiel à fonte

Preserva o payload original sem transformações. Acrescenta metadados de ingestão para rastreabilidade e auditoria regulatória.

Campos adicionados: `_kafka_key`, `_kafka_partition`, `_kafka_offset`, `_kafka_timestamp`, `_ingest_timestamp`, `_batch_id`, `_ano`, `_mes`, `_dia`.

### Silver — dado limpo e enriquecido

Aplica a lógica de negócio do setor elétrico.

**`eventos_rede_enriquecido`:**
- Deduplicação por `evento_id` via `ROW_NUMBER() OVER (PARTITION BY evento_id)`
- Join com topologia: capacidade MVA, tensão nominal, idade da subestação
- Classificação ANEEL (PRODIST M8): `NAO_PLANEJADA`, `PLANEJADA`, `CASO_FORTUITO`, `EMERGENCIAL`
- Flag `is_evento_climatico` (precipitação > 5mm ou vento > 60 km/h)
- Score de criticidade 0-100: severidade (40%) + impacto clientes (30%) + duração (20%) + idade ativo (10%)

**`leituras_medidor_norm`:**
- Classificação de tensão ANEEL: `ADEQUADA` (±5%), `PRECARIA` (±10%), `CRITICA` (>10%)
- Score de anomalia via z-score em relação à média do alimentador
- Score de fraude multi-sinal: consumo sub-reportado + delta negativo + anomalia
- Classificação de fator de potência: `EXCELENTE`, `BOM`, `REGULAR`, `RUIM`

### Gold — KPIs regulatórios e de gestão

**`kpi_dec_fec_mensal`** — Indicadores ANEEL por alimentador × mês:

```
DEC (h) = Σ(clientes_afetados × duração_horas) / total_clientes_conjunto
FEC (n) = Σ(clientes_afetados)                / total_clientes_conjunto
```

Campos de conformidade: `limite_dec_horas`, `pct_limite_dec`, `semaforo_dec` (VERDE/AMARELO/VERMELHO), `violacao_dec`.

**`saude_ativo_alimentador`** — Índice de saúde 0-100:

| Dimensão | Peso |
|---|---|
| Frequência de falhas | 30% |
| MTTR (tempo médio de reparo) | 25% |
| Eventos críticos | 20% |
| MTBF (tempo médio entre falhas) | 15% |
| Impacto em clientes | 10% |

Saídas: `classificacao_saude` (CRITICO/ATENCAO/NORMAL), `acao_recomendada`, `prazo_intervencao_dias`.

**`perdas_comerciais_regional`** — Perdas por alimentador:
- Perda por fraude: consumo sub-reportado em medidores suspeitos
- ENF (Energia Não Fornecida): demanda média × duração das interrupções
- Receita perdida em R$ pelas tarifas ANEEL 2024 por tipo de consumidor

---

## Orquestração — Databricks Asset Bundles

O pipeline é gerenciado como código YAML versionado no Git, deployado via CLI.

### Grafo de execução validado

```
simulador_kafka  (5m 24s)
    ├── bronze_eventos  (3m 02s) ─┐ paralelo
    └── bronze_leituras (2m 54s) ─┤
                                  ├── silver_eventos  (41s) ─┐ paralelo
                                  └── silver_leituras (44s) ─┤
                                                             ├── gold_dec_fec (25s)
                                                             │       ├── gold_perdas (27s) ─┐
                                                             │       └── gold_saude  (31s) ─┘
Tempo total: ~13 minutos end-to-end │ Status: SUCESSO
```

### Comandos

```bash
databricks bundle validate
databricks bundle deploy --target dev
databricks bundle run adms_00_pipeline_completo --target dev
databricks bundle run adms_01_ingestao_bronze --target dev
```

---

## Estrutura do repositório

```
adms-smart-grid-platform/
├── databricks.yml                        # Bundle config (dev + prod)
├── resources/jobs/
│   ├── job_00_pipeline_completo.yml      # Orquestrador end-to-end (8 tasks)
│   ├── job_01_ingestao_bronze.yml
│   ├── job_02_transformacao_silver.yml
│   └── job_03_gold_kpis.yml
├── ingestion/kafka/producers/
│   ├── topologia.py                      # Dados mestres da rede (20 SEs, 120 ALs)
│   ├── schemas.py                        # Contratos EventoRede, LeituraMedidor, Alarme
│   ├── kafka_config.py                   # SASL/SSL Confluent Cloud automático
│   ├── evento_rede_producer.py           # Simulador de eventos ADMS
│   ├── medidor_producer.py               # Simulador AMI (9.994 medidores)
│   ├── run_simuladores.py                # Orquestrador com painel terminal
│   └── simulador_runner.py              # Wrapper para Databricks Workflows
├── bronze/notebooks/
│   ├── 01_bronze_eventos_rede.py
│   └── 02_bronze_leituras_medidor.py
├── silver/notebooks/
│   ├── 03_silver_eventos_rede.py
│   └── 04_silver_leituras_medidor.py
├── gold/notebooks/
│   ├── 05_gold_kpi_dec_fec.py
│   ├── 06_gold_saude_ativos.py
│   └── 07_gold_perdas_comerciais.py
├── dbt/                                  # Estrutura dbt pronta (migração futura)
│   ├── dbt_project.yml
│   ├── models/staging|silver|gold/
│   ├── seeds/subestacoes|limites_aneel|tarifas.csv
│   └── macros/classificar_tensao_aneel|score_criticidade.sql
├── docker-compose.yml                    # Kafka local dev
├── requirements.txt
└── setup_project.py
```

---

## Setup local

### Pré-requisitos

- Python 3.10+, Docker Desktop, Git
- [Databricks Free Edition](https://signup.databricks.com)
- [Confluent Cloud](https://confluent.io/confluent-cloud/tryfree) ($400 crédito gratuito)

### Passos

```bash
# Clonar e instalar
git clone https://github.com/seu-usuario/adms-smart-grid-platform.git
cd adms-smart-grid-platform
python -m venv adms-venv && adms-venv\Scripts\activate
pip install -r requirements-dev.txt

# Configurar credenciais
cp .env.example .env
# Editar .env com bootstrap Confluent Cloud, API Key/Secret e token Databricks

# Kafka local
docker compose up -d

# Criar tópicos
docker exec adms-kafka kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic adms.eventos.rede --partitions 6 --replication-factor 1

# Rodar simulador (dry-run)
cd ingestion/kafka/producers
python run_simuladores.py --dry-run --duration 30

# Deploy do pipeline
databricks bundle deploy --target dev
databricks bundle run adms_00_pipeline_completo --target dev
```

---

## Decisões de arquitetura

**`availableNow` em vez de `processingTime` no Spark Streaming**
O Databricks Free Edition usa compute serverless que não suporta streaming contínuo. O trigger `availableNow` processa todos os offsets disponíveis e encerra — compatível com serverless e adequado para o padrão de carga diária de uma distribuidora.

**Kafka com Confluent Cloud em vez de Delta Live Tables**
O objetivo era simular a arquitetura real de uma distribuidora, onde o ADMS publica eventos via mensageria. Confluent Cloud reproduz fielmente esse padrão com SASL/SSL, múltiplas partições e retenção configurável.

**Topologia de rede inline nos notebooks**
Em produção, viria do GIS via integração. Para o projeto, os dados das 20 subestações inline eliminam dependência de sistemas externos. A estrutura dbt com `seeds/subestacoes.csv` já prepara a migração para fonte versionada.

**dbt preparado mas não ativo**
A estrutura completa (models, macros, seeds, tests, schema.yml) foi desenvolvida. A decisão de manter notebooks é operacional — o Free Edition tem limitações de conexão SQL Warehouse que adicionam complexidade sem retorno imediato. Ativação prevista na próxima fase.

---

## Contexto de negócio

O projeto simula uma distribuidora de médio porte em Minas Gerais, endereçando as principais demandas de dados que um COD enfrenta diariamente:

- **DEC/FEC regulatório**: toda distribuidora reporta mensalmente à ANEEL. Ultrapassar os limites gera multas. O pipeline calcula e monitora automaticamente com semáforo VERDE/AMARELO/VERMELHO.
- **Saúde de ativos**: com subestações de 30-40 anos de idade média, priorizar manutenção preventiva baseada em dados reduz custos e interrupções não planejadas.
- **Perdas comerciais**: identificar medidores com consumo sub-reportado e calcular ENF são estratégicos para a receita e conformidade regulatória da distribuidora.

---

## Autor

**Wellington Murito**
Data Engineer — Energisa | Co-desenvolvedor Projeto VIDA (AWS + Databricks)

[LinkedIn](https://www.linkedin.com/in/wmurito/) · [GitHub](https://github.com/wmurito)
