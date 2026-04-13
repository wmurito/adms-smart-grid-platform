from pathlib import Path

dirs = [
    "ingestion/kafka/producers",
    "ingestion/kafka/consumers",
    "ingestion/kafka/schemas",
    "ingestion/batch/pipelines",
    "ingestion/eventstream",
    "bronze/notebooks",
    "bronze/schemas",
    "silver/notebooks",
    "silver/tests",
    "gold/notebooks",
    "gold/semantic",
    "ml/fault_prediction",
    "ml/loss_detection",
    "serving/powerbi",
    "serving/api",
    "governance/unity_catalog",
    "infrastructure/terraform",
    "infrastructure/ci_cd",
    "docs/architecture",
    "data/seed",
    "data/samples",
    "tests/unit",
    "tests/integration",
]

files = [
    "ingestion/kafka/producers/__init__.py",
    "ingestion/kafka/consumers/__init__.py",
    "ingestion/kafka/schemas/evento_rede.avsc",
    "ingestion/kafka/schemas/leitura_medidor.avsc",
    "bronze/notebooks/.gitkeep",
    "silver/notebooks/.gitkeep",
    "gold/notebooks/.gitkeep",
    "ml/fault_prediction/__init__.py",
    "ml/loss_detection/__init__.py",
    "serving/api/__init__.py",
    "docs/architecture/ADR-001-tech-stack.md",
    "docs/architecture/ADR-002-medallion.md",
    ".env.example",
    "docker-compose.yml",
    "requirements.txt",
    "requirements-dev.txt",
    "Makefile",
]

for d in dirs:
    Path(d).mkdir(parents=True, exist_ok=True)
    (Path(d) / ".gitkeep").touch()

for f in files:
    Path(f).touch()

print("Estrutura criada com sucesso!")