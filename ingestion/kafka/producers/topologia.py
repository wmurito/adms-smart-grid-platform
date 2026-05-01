"""
Topologia da rede elétrica simulada — ADMS Smart Grid Platform
Representa uma distribuidora com 20 subestações e 120 alimentadores
cobrindo regiões urbanas, suburbanas e rurais de Minas Gerais.
"""

from dataclasses import dataclass, field
from typing import List
import random

# ---------------------------------------------------------------------------
# Estruturas de dados
# ---------------------------------------------------------------------------

@dataclass
class Subestacao:
    id: str
    nome: str
    regional: str
    municipio: str
    latitude: float
    longitude: float
    tensao_kv: int
    capacidade_mva: float
    ano_instalacao: int

@dataclass
class Alimentador:
    id: str
    nome: str
    subestacao_id: str
    tipo_area: str          # URBANA | SUBURBANA | RURAL
    num_clientes: int
    extensao_km: float
    tensao_kv: int
    num_chaves: int
    num_religadores: int
    ano_instalacao: int

@dataclass
class Equipamento:
    id: str
    tipo: str               # CHAVE_FACA | RELIGADOR | DISJUNTOR | TRANSFORMADOR | CABO
    alimentador_id: str
    subestacao_id: str
    fabricante: str
    modelo: str
    ano_fabricacao: int
    latitude: float
    longitude: float

# ---------------------------------------------------------------------------
# Dados mestres — Subestações
# ---------------------------------------------------------------------------

SUBESTACOES: List[Subestacao] = [
    Subestacao("SE01", "SE Cataguases",       "Zona da Mata", "Cataguases",      -21.389, -42.696, 138, 40.0,  1985),
    Subestacao("SE02", "SE Leopoldina",       "Zona da Mata", "Leopoldina",      -21.531, -42.640, 138, 30.0,  1990),
    Subestacao("SE03", "SE Muriaé",           "Zona da Mata", "Muriaé",          -21.130, -42.367, 138, 50.0,  1982),
    Subestacao("SE04", "SE Juiz de Fora N",   "Zona da Mata", "Juiz de Fora",    -21.754, -43.350, 230, 80.0,  1978),
    Subestacao("SE05", "SE Juiz de Fora S",   "Zona da Mata", "Juiz de Fora",    -21.790, -43.380, 230, 80.0,  1988),
    Subestacao("SE06", "SE Ubá",              "Zona da Mata", "Ubá",             -21.119, -42.940, 138, 35.0,  1992),
    Subestacao("SE07", "SE Viçosa",           "Zona da Mata", "Viçosa",          -20.755, -42.881, 138, 25.0,  1995),
    Subestacao("SE08", "SE Governador Val.",  "Zona da Mata", "Governador Val.", -20.929, -42.651, 138, 30.0,  1987),
    Subestacao("SE09", "SE Manhuaçu",        "Zona da Mata", "Manhuaçu",        -20.258, -42.029, 138, 28.0,  1993),
    Subestacao("SE10", "SE Carangola",        "Zona da Mata", "Carangola",       -20.734, -42.027, 69,  15.0,  1998),
    Subestacao("SE11", "SE Além Paraíba",    "Zona da Mata", "Além Paraíba",    -21.881, -42.713, 138, 22.0,  1996),
    Subestacao("SE12", "SE Santos Dumont",    "Zona da Mata", "Santos Dumont",   -21.462, -43.546, 138, 20.0,  1999),
    Subestacao("SE13", "SE Barbacena",        "Campo das Vert.", "Barbacena",    -21.226, -43.773, 138, 35.0,  1983),
    Subestacao("SE14", "SE São João Del Rei", "Campo das Vert.", "São João DR", -21.133, -44.261, 138, 30.0,  1986),
    Subestacao("SE15", "SE Lavras",           "Campo das Vert.", "Lavras",        -21.244, -44.999, 138, 28.0,  1991),
    Subestacao("SE16", "SE Varginha",         "Sul de Minas",  "Varginha",        -21.551, -45.430, 138, 45.0,  1984),
    Subestacao("SE17", "SE Poços de Caldas",  "Sul de Minas",  "Poços de Caldas",-21.786, -46.565, 138, 40.0,  1980),
    Subestacao("SE18", "SE Itajubá",          "Sul de Minas",  "Itajubá",         -22.425, -45.451, 138, 32.0,  1989),
    Subestacao("SE19", "SE Três Corações",    "Sul de Minas",  "Três Corações",  -21.693, -45.253, 69,  18.0,  1997),
    Subestacao("SE20", "SE Passos",           "Sul de Minas",  "Passos",          -20.718, -46.609, 138, 25.0,  1994),
]

# Índice por ID para lookup rápido
SUBESTACAO_MAP = {se.id: se for se in SUBESTACOES}

# ---------------------------------------------------------------------------
# Dados mestres — Alimentadores (6 por subestação = 120 total)
# ---------------------------------------------------------------------------

_TIPOS_AREA = ["URBANA", "URBANA", "URBANA", "SUBURBANA", "SUBURBANA", "RURAL"]
_FABRICANTES_CHAVE = ["ABB", "Schneider Electric", "Siemens", "Eaton", "WEG"]
_FABRICANTES_RELIGADOR = ["Cooper", "ABB", "Schneider Electric", "Nulec"]
_FABRICANTES_TRAFO = ["ABB", "WEG", "Siemens", "Trafo", "TMEC"]

def _gerar_alimentadores() -> List[Alimentador]:
    alimentadores = []
    for se in SUBESTACOES:
        for i in range(1, 7):
            tipo = _TIPOS_AREA[i - 1]
            if tipo == "URBANA":
                clientes = random.randint(3000, 8000)
                extensao = round(random.uniform(8, 25), 1)
                chaves = random.randint(12, 20)
                religadores = random.randint(2, 4)
            elif tipo == "SUBURBANA":
                clientes = random.randint(1500, 4000)
                extensao = round(random.uniform(20, 50), 1)
                chaves = random.randint(8, 14)
                religadores = random.randint(1, 3)
            else:  # RURAL
                clientes = random.randint(200, 1500)
                extensao = round(random.uniform(40, 120), 1)
                chaves = random.randint(4, 10)
                religadores = random.randint(1, 2)

            alim_id = f"{se.id}-AL{i:02d}"
            alimentadores.append(Alimentador(
                id=alim_id,
                nome=f"Alimentador {se.nome.replace('SE ', '')} {i:02d}",
                subestacao_id=se.id,
                tipo_area=tipo,
                num_clientes=clientes,
                extensao_km=extensao,
                tensao_kv=13,
                num_chaves=chaves,
                num_religadores=religadores,
                ano_instalacao=random.randint(1985, 2020),
            ))
    return alimentadores

# Seed fixo para reprodutibilidade
random.seed(42)
ALIMENTADORES: List[Alimentador] = _gerar_alimentadores()
ALIMENTADOR_MAP = {al.id: al for al in ALIMENTADORES}

# ---------------------------------------------------------------------------
# Dados mestres — Equipamentos (aprox. 8-12 por alimentador)
# ---------------------------------------------------------------------------

def _gerar_equipamentos() -> List[Equipamento]:
    equipamentos = []
    random.seed(42)
    for al in ALIMENTADORES:
        se = SUBESTACAO_MAP[al.subestacao_id]
        lat_base = se.latitude + random.uniform(-0.1, 0.1)
        lon_base = se.longitude + random.uniform(-0.1, 0.1)

        # Chaves de faca
        for j in range(al.num_chaves):
            equipamentos.append(Equipamento(
                id=f"{al.id}-CF{j+1:03d}",
                tipo="CHAVE_FACA",
                alimentador_id=al.id,
                subestacao_id=al.subestacao_id,
                fabricante=random.choice(_FABRICANTES_CHAVE),
                modelo=random.choice(["CF-36kV", "CF-15kV", "CF-24kV"]),
                ano_fabricacao=random.randint(1990, 2022),
                latitude=lat_base + random.uniform(-0.05, 0.05),
                longitude=lon_base + random.uniform(-0.05, 0.05),
            ))

        # Religadores
        for j in range(al.num_religadores):
            equipamentos.append(Equipamento(
                id=f"{al.id}-RL{j+1:03d}",
                tipo="RELIGADOR",
                alimentador_id=al.id,
                subestacao_id=al.subestacao_id,
                fabricante=random.choice(_FABRICANTES_RELIGADOR),
                modelo=random.choice(["Nova 27", "Form 6", "Kyle 38", "VK Series"]),
                ano_fabricacao=random.randint(1995, 2023),
                latitude=lat_base + random.uniform(-0.05, 0.05),
                longitude=lon_base + random.uniform(-0.05, 0.05),
            ))

        # Transformadores (2 por alimentador)
        for j in range(2):
            equipamentos.append(Equipamento(
                id=f"{al.id}-TR{j+1:03d}",
                tipo="TRANSFORMADOR",
                alimentador_id=al.id,
                subestacao_id=al.subestacao_id,
                fabricante=random.choice(_FABRICANTES_TRAFO),
                modelo=random.choice(["TR-150kVA", "TR-300kVA", "TR-500kVA", "TR-1000kVA"]),
                ano_fabricacao=random.randint(1980, 2020),
                latitude=lat_base + random.uniform(-0.03, 0.03),
                longitude=lon_base + random.uniform(-0.03, 0.03),
            ))

    return equipamentos

EQUIPAMENTOS: List[Equipamento] = _gerar_equipamentos()
EQUIPAMENTO_MAP = {eq.id: eq for eq in EQUIPAMENTOS}

# ---------------------------------------------------------------------------
# Helpers de consulta
# ---------------------------------------------------------------------------

def alimentadores_por_subestacao(subestacao_id: str) -> List[Alimentador]:
    return [al for al in ALIMENTADORES if al.subestacao_id == subestacao_id]

def equipamentos_por_alimentador(alimentador_id: str) -> List[Equipamento]:
    return [eq for eq in EQUIPAMENTOS if eq.alimentador_id == alimentador_id]

def total_clientes() -> int:
    return sum(al.num_clientes for al in ALIMENTADORES)

def resumo() -> dict:
    return {
        "subestacoes": len(SUBESTACOES),
        "alimentadores": len(ALIMENTADORES),
        "equipamentos": len(EQUIPAMENTOS),
        "total_clientes": total_clientes(),
        "regionais": list({se.regional for se in SUBESTACOES}),
    }


if __name__ == "__main__":
    import json
    print(json.dumps(resumo(), indent=2, ensure_ascii=False))
    print(f"\nExemplo de subestação: {SUBESTACOES[0]}")
    print(f"Exemplo de alimentador: {ALIMENTADORES[0]}")
    print(f"Exemplo de equipamento: {EQUIPAMENTOS[0]}")