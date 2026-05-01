# ADMS Smart Grid Data Platform — Visão Geral do Projeto

## O que é este projeto?

Plataforma de dados para **distribuidoras de energia elétrica** inspirada em sistemas ADMS (Advanced Distribution Management System) reais. Simula e processa dados de redes elétricas inteligentes usando uma stack moderna de Data Engineering com Kafka + Databricks + Delta Lake.

Este projeto serve como **portfólio de Data Engineering** e pode ser reutilizado como base para projetos similares em utilities/energia.

---

## Stack Tecnológica

| Camada | Tecnologia | Papel |
|---|---|---|
| **Streaming / Mensageria** | Apache Kafka (Confluent Cloud) | Barramento central de eventos da rede |
| **Lakehouse** | Databricks (Free Edition) | Processamento Spark + armazenamento Delta |
| **Formatos** | Delta Lake, JSON, Parquet | Persistência imutável com versionamento |
| **Governança** | Unity Catalog (Databricks) | Catálogo central: `adms.bronze/silver/gold` |
| **Dev Local** | Docker Compose + kafka-python | Kafka local para desenvolvimento offline |
| **Orquestração** | Python threading | Simulação multi-thread de produtores |
| **ML (roadmap)** | MLflow + scikit-learn + XGBoost | Predição de falhas, detecção de fraude |

---

## Arquitetura Medallion (Bronze → Silver → Gold)

```
[Simuladores Python]
     ↓ Kafka Topics
 adms.eventos.rede
 adms.leituras.medidor
 adms.alarmes
     ↓
[BRONZE — Delta Lake]          Raw + metadados Kafka
 adms.bronze.eventos_rede_raw
 adms.bronze.leituras_medidor_raw
     ↓
[SILVER — Delta Lake]          Deduplicado + enriquecido + validado
 adms.silver.eventos_rede_enriquecido
 adms.silver.leituras_medidor_norm
     ↓
[GOLD — Delta Lake]            KPIs de negócio
 adms.gold.kpi_dec_fec_mensal
 adms.gold.saude_ativo_alimentador
 adms.gold.perdas_comerciais_regional
```

---

## Domínio de Negócio Simulado

### Topologia da Rede (Minas Gerais)
- **20 subestações** em 4 regionais (Zona da Mata, Campo das Vertentes, Sul de Minas)
- **120 alimentadores** (6 por subestação): URBANA, SUBURBANA, RURAL
- **~1.500 equipamentos** (chaves de faca, religadores, transformadores)
- **~10.000 medidores AMI** (Advanced Metering Infrastructure)
- **~350.000 clientes** simulados

### Tipos de Dados Gerados
1. **Eventos de rede** (`adms.eventos.rede`): falhas de equipamento, manobras, anomalias de tensão/corrente, interrupções, alarmes.
2. **Leituras de medidores** (`adms.leituras.medidor`): consumo kWh, tensão, corrente, fator de potência, flags de fraude/anomalia.
3. **Alarmes** (`adms.alarmes`): alertas críticos derivados de eventos.

### Conformidade Regulatória ANEEL
- **DEC/FEC**: Duração/Frequência Equivalente de Interrupção (PRODIST Módulo 8)
- **Classificação de tensão**: ADEQUADA / PRECÁRIA / CRÍTICA (±5% / ±10% da nominal)
- **Classes de interrupção**: Planejada, Não Planejada, Emergencial, Caso Fortuito, Terceiros

---

## Fases de Implementação

| Fase | Descrição | Status |
|---|---|---|
| **Fase 1** | Infraestrutura local (Docker + Python env) | ✅ Completo |
| **Fase 2** | Simuladores de dados (Kafka Producers) | ✅ Completo |
| **Fase 3** | Camada Bronze (ingestão Kafka → Delta) | ✅ Completo |
| **Fase 4** | Camada Silver (transformação + enriquecimento) | ✅ Completo |
| **Fase 5** | Camada Gold (KPIs regulatórios e de negócio) | ✅ Completo |
| **Fase 6** | ML — Predição de falhas / detecção de fraude | 🔲 Scaffolded |
| **Fase 7** | Governança (Unity Catalog + data lineage) | 🔲 Scaffolded |
| **Fase 8** | Serving (API REST + Power BI) | 🔲 Scaffolded |

---

## Variáveis de Ambiente (.env)

```
# Confluent Cloud (Kafka gerenciado)
KAFKA_BOOTSTRAP_SERVERS=pkc-xxx.confluent.cloud:9092
KAFKA_SASL_USERNAME=<API_KEY>
KAFKA_SASL_PASSWORD=<API_SECRET>

# Databricks
DATABRICKS_HOST=https://dbc-xxx.cloud.databricks.com
DATABRICKS_TOKEN=dapi...
DATABRICKS_CATALOG=adms
DATABRICKS_SCHEMA_BRONZE=bronze
DATABRICKS_SCHEMA_SILVER=silver
DATABRICKS_SCHEMA_GOLD=gold

# Simulador
SIMULATOR_EVENTS_PER_SECOND=10
SIMULATOR_NUM_METERS=10000
SIMULATOR_SEED=42
```

> ⚠️ **ATENÇÃO**: O arquivo `.env` atual contém credenciais reais. Use Databricks Secrets em produção. O `.gitignore` deve incluir `.env`.

---

## Como usar este contexto com uma LLM

Ao reutilizar este projeto ou pedir ajuda a uma LLM, forneça os seguintes arquivos de contexto em ordem:

1. `00_PROJECT_OVERVIEW.md` — Este arquivo (sempre incluir)
2. `01_FASE1_INFRAESTRUTURA.md` — Se trabalhar com Docker/ambiente
3. `02_FASE2_SIMULADORES_KAFKA.md` — Se trabalhar com producers
4. `03_FASE3_BRONZE.md` — Se trabalhar com ingestão Bronze
5. `04_FASE4_SILVER.md` — Se trabalhar com transformação Silver
6. `05_FASE5_GOLD.md` — Se trabalhar com KPIs Gold
7. `06_SCHEMAS_DADOS.md` — Se mudar estrutura de dados

**Prompt sugerido para LLM:**
```
Contexto: Estou trabalhando no projeto ADMS Smart Grid Data Platform.
[Cole o conteúdo do arquivo de contexto relevante aqui]

Minha dúvida/tarefa: [sua pergunta]
```
