# Troubleshooting e Decisões de Arquitetura

## Problemas conhecidos e soluções

---

## 1. confluent-kafka bloqueada no Windows

**Sintoma**: `ImportError: DLL load failed while importing _confluent_kafka`

**Causa**: Antivírus corporativo ou políticas de segurança Windows bloqueando DLLs nativas da `confluent-kafka`.

**Solução implementada**: Migrar todos os producers para `kafka-python` (pura Python, sem DLLs).

**Impacto no código**:
- `from confluent_kafka import Producer` → **REMOVIDO** dos producers em uso
- `from kafka import KafkaProducer` → **USADO** em `medidor_producer.py` e `evento_rede_producer.py`
- O arquivo `evento_rede_producer.py` ainda tem a função `run()` com `confluent_kafka` (código legado) mas ela **não é chamada** pelo orquestrador

**Nota**: O `requirements.txt` mantém `confluent-kafka` como dependência pois pode ser necessária em Linux/Mac ou em ambientes sem restrições.

---

## 2. UnicodeEncodeError no terminal Windows

**Sintoma**: `UnicodeEncodeError: 'charmap' codec can't encode character '\u2713'`

**Causa**: O PowerShell padrão do Windows usa codificação CP1252 que não suporta caracteres Unicode como ✓, ─, ═.

**Solução implementada** (em `run_simuladores.py`):
```python
# ANTES (causava erro)
print(f"{'='*55}")
print(f"  ✓ Conectado")

# DEPOIS (ASCII puro)
print(f"{'='*55}")
print(f"  Conectado")
sep = "=" * 64  # sem caracteres unicode
```

**Alternativa** (se quiser manter unicode):
```powershell
# Configurar UTF-8 no PowerShell antes de executar
chcp 65001
python run_simuladores.py
```

---

## 3. NoBrokersAvailable ao iniciar

**Sintoma**: `kafka.errors.NoBrokersAvailable`

**Causa**: Producer tentou conectar antes do Kafka terminar de inicializar (Docker demora ~30-60s).

**Solução implementada**: Retry automático nos producers com espera exponencial:
```python
def _conectar(self, bootstrap_servers: str, tentativas: int = 5, espera: float = 2.0):
    from kafka.errors import NoBrokersAvailable
    for i in range(tentativas):
        try:
            self._producer = KafkaProducer(bootstrap_servers=bootstrap_servers, ...)
            return
        except NoBrokersAvailable:
            if i < tentativas - 1:
                time.sleep(espera)
            else:
                logger.error(f"Falha após {tentativas} tentativas")
```

---

## 4. Dupla serialização no Kafka

**Sintoma**: Mensagem chega no Kafka como string JSON dentro de outra string JSON: `"\"{'evento_id': ...}\""`

**Causa**: Serializar com `json.dumps()` e depois ter `value_serializer=lambda v: json.dumps(v).encode()`.

**Solução**: O `value_serializer` do `KafkaProducer` já faz o JSON. Passar apenas o `dict` para `.send()`:
```python
# CORRETO
producer = KafkaProducer(
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
    ...
)
producer.send(topico, key=..., value=evento.to_dict())   # to_dict() retorna DICT

# ERRADO (dupla serialização)
producer.send(topico, value=json.dumps(evento.to_dict()))  # já é bytes aqui
```

---

## 5. Credenciais Confluent Cloud expostas nos notebooks

**Situação atual**: Databricks Free Edition não suporta Secrets via `dbutils.secrets` sem cluster configurado.

**Solução temporária** (desenvolvimento):
```python
CONFLUENT_API_KEY = "5GDS2QXL5XJZCZCU"  # hardcoded
CONFLUENT_SECRET  = "..."
```

**Solução para produção** (Databricks pago com cluster):
```python
CONFLUENT_API_KEY = dbutils.secrets.get(scope="adms-confluent", key="api-key")
CONFLUENT_SECRET  = dbutils.secrets.get(scope="adms-confluent", key="api-secret")
```

**Protocolo JAAS no Databricks**:
- Use `kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule` (não `org.apache.kafka...`)
- O Databricks usa uma versão "shaded" (renomeada) do Kafka para evitar conflitos de classpath

---

## 6. topologia.py não acessível no Databricks

**Situação**: O arquivo `topologia.py` existe localmente mas o Databricks não tem acesso ao filesystem local.

**Solução implementada**: Recriar os dados das 20 subestações como DataFrame Spark inline no notebook Silver:
```python
subestacoes_data = [
    ("SE01","SE Cataguases", "Zona da Mata", ...),
    # ... 20 subestações
]
df_subestacoes = spark.createDataFrame(subestacoes_data, schema=schema_se)
```

**Alternativa para produção**: Criar tabela de referência no Unity Catalog:
```sql
CREATE TABLE adms.reference.subestacoes
USING DELTA
AS SELECT * FROM <dados_das_subestacoes>;
```

---

## 7. trigger(availableNow=True) vs streaming contínuo

**Situação**: Databricks Free Edition tem limite de DBUs por mês.

**Solução**: Usar `trigger(availableNow=True)` que processa todo o backlog disponível e para o job:
```python
query = (
    spark.readStream...
    .writeStream
    .trigger(availableNow=True)   # Processa e para
    .start()
)
query.awaitTermination()  # Aguarda conclusão
```

**Para produção real** (streaming contínuo):
```python
.trigger(processingTime="30 seconds")
```

---

## 8. Checkpoint corrompido

**Sintoma**: Streaming falha ao reiniciar com erro de checkpoint incompatível.

**Causa**: Mudança no schema da tabela ou no código do `foreachBatch` sem limpar o checkpoint.

**Solução**:
```python
# Deletar checkpoint e reprocessar tudo
dbutils.fs.rm("/Volumes/adms/bronze/checkpoints/eventos_rede", recurse=True)
# Reexecutar notebook com startingOffsets: "earliest"
```

---

## Decisões de arquitetura

### Por que kafka-python em vez de confluent-kafka?

| Critério | kafka-python | confluent-kafka |
|---|---|---|
| Windows | ✅ Funciona (pure Python) | ❌ Problemas com DLL |
| Performance | Boa para desenvolvimento | Superior para produção |
| Confluent Cloud | ✅ Suporta SASL_SSL | ✅ Suporte nativo |
| API | KafkaProducer + KafkaConsumer | Producer + Consumer |
| Manutenção | Ativo | Ativo (Confluent patrocinado) |

**Decisão**: kafka-python em desenvolvimento Windows; pode migrar para confluent-kafka em produção Linux.

### Por que modo `overwrite` no Silver e Gold?

- **Bronze**: `append` — nunca sobrescrever dados brutos, rastreabilidade total
- **Silver**: `overwrite` — reprocessamento completo garante consistência (sem estado acumulado)
- **Gold**: `overwrite` — KPIs sempre recalculados do zero para consistência

Para produção com grandes volumes, Silver/Gold deveriam usar **MERGE (upsert)** no Delta Lake:
```python
from delta.tables import DeltaTable

delta_table = DeltaTable.forName(spark, "adms.silver.eventos_rede_enriquecido")
delta_table.alias("target").merge(
    df_silver.alias("source"),
    "target.evento_id = source.evento_id"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
```

### Por que particionamento por `subestacao_id`?

- Subestações são a unidade geográfica principal de operação
- Queries mais comuns filtram por subestação ou regional
- 20 valores de subestação = cardinalidade adequada para particionamento (evita too many small files)
- Para Gold com menos dados, partição por `regional` (4 valores) é suficiente

### Por que seed 42 fixo?

- Garante **reprodutibilidade** dos dados simulados
- Com o mesmo seed, a topologia de medidores é sempre a mesma
- Facilita comparação de cenários: o mesmo medidor gera o mesmo comportamento
- Para simular diferentes distribuidoras: mudar a seed

### Por que `foreachBatch` em vez de `writeStream.format("delta")`?

`writeStream.format("delta")` direto não permite:
- Adicionar colunas calculadas (como `_batch_id`, `_ano`, `_mes`)
- Logging por batch
- Lógica condicional

`foreachBatch` dá controle total sobre como cada micro-batch é processado.

---

## Variáveis de ambiente — referência rápida

```bash
# Verificar qual modo está ativo (local ou cloud)
python kafka_config.py

# Output esperado (modo cloud):
# ====================================================
#   Kafka Config — ADMS Smart Grid Platform
# ====================================================
#   Modo       : Confluent Cloud (SASL_SSL)
#   Bootstrap  : pkc-oxqxx9.us-east-1.aws.confluent.cloud:9092
#   API Key    : 5GDS2QXL****
# ====================================================
```

---

## Comandos úteis de diagnóstico

```bash
# Ver estado do Docker
docker compose ps
docker compose logs kafka --tail 20

# Contar mensagens em um tópico (requer Kafka local)
docker exec adms-kafka kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list localhost:9092 \
  --topic adms.eventos.rede \
  --time -1

# Consumir mensagens do tópico (debug)
docker exec adms-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic adms.eventos.rede \
  --from-beginning \
  --max-messages 3

# Testar conectividade Confluent Cloud
python ingestion/kafka/producers/kafka_config.py

# Dry-run dos simuladores (sem Kafka)
cd ingestion/kafka/producers
python run_simuladores.py --dry-run --duration 30
```

```python
# No Databricks — verificar tabelas disponíveis
spark.sql("SHOW TABLES IN adms.bronze").show()
spark.sql("SHOW TABLES IN adms.silver").show()
spark.sql("SHOW TABLES IN adms.gold").show()

# Ver histórico Delta (versionamento)
spark.sql("DESCRIBE HISTORY adms.bronze.eventos_rede_raw").show()

# Contar registros
spark.table("adms.bronze.eventos_rede_raw").count()
spark.table("adms.silver.eventos_rede_enriquecido").count()
spark.table("adms.gold.kpi_dec_fec_mensal").count()
```
