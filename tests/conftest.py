"""Fixtures compartilhadas para os testes do pipeline ETL."""
import importlib.util
import sys
import types
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).parent.parent


def _load_module(filename: str, name: str):
    path = ROOT / "python" / filename
    spec = importlib.util.spec_from_file_location(name, path)
    mod = types.ModuleType(name)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="session")
def clean_mod():
    return _load_module("02_clean.py", "etl_02")


@pytest.fixture(scope="session")
def validate_mod():
    return _load_module("03_validate.py", "etl_03")


@pytest.fixture
def raw_clientes() -> pd.DataFrame:
    return pd.DataFrame({
        "cliente_id": ["C001", "C002", "C003"],
        "segmento": ["Corporate", "Middle Market", "Corporate"],
        "porte": ["Grande", "Médio", "Grande"],
        "setor": ["Energia", "Varejo", "Agro"],
        "subsetor": ["Petróleo", "Supermercado", "Grãos"],
        "data_inicio_relacionamento": ["2020-01-01", "2019-06-15", "2021-03-10"],
        "regiao": ["SP", "RJ", "MT"],
        "status_cliente": ["Ativo", "Ativo", "Ativo"],
    })


@pytest.fixture
def raw_ratings() -> pd.DataFrame:
    return pd.DataFrame({
        "cliente_id": ["C001", "C002", "C003"],
        "data_referencia": ["2024-12-31", "2024-12-31", "2024-12-31"],
        "rating_interno": ["AA", "BBB", "A"],
        "rating_externo": ["AA", "BBB", "A"],
        "pd_12m": [0.001, 0.02, 0.005],
        "score_interno": [920, 750, 860],
    })


@pytest.fixture
def raw_exposicoes() -> pd.DataFrame:
    return pd.DataFrame({
        "cliente_id": ["C001", "C002", "C003"],
        "data_referencia": ["2024-12-31", "2024-12-31", "2024-12-31"],
        "exposicao_total": [10_000_000.0, 5_000_000.0, 8_000_000.0],
        "exposicao_garantida": [7_000_000.0, 2_000_000.0, 6_000_000.0],
        "exposicao_descoberta": [3_000_000.0, 3_000_000.0, 2_000_000.0],
    })


@pytest.fixture
def raw_operacoes() -> pd.DataFrame:
    return pd.DataFrame({
        "operacao_id": ["OP001", "OP002", "OP003"],
        "cliente_id": ["C001", "C002", "C003"],
        "produto": ["CCB", "CDB", "CCB"],
        "modalidade": ["Pré-fixado", "Pós-fixado", "Pré-fixado"],
        "valor_aprovado": [5_000_000.0, 3_000_000.0, 4_000_000.0],
        "valor_utilizado": [4_500_000.0, 2_800_000.0, 3_200_000.0],
        "taxa_juros": [0.12, 0.13, 0.115],
        "prazo_meses": [24, 36, 18],
        "data_aprovacao": ["2023-01-01", "2022-06-01", "2023-03-01"],
        "data_vencimento": ["2025-01-01", "2025-06-01", "2024-09-01"],
        "garantia_tipo": ["Imóvel", "Recebíveis", "Veículos"],
        "status_operacao": ["Ativa", "Ativa", "Ativa"],
    })


@pytest.fixture
def raw_limites() -> pd.DataFrame:
    return pd.DataFrame({
        "cliente_id": ["C001", "C002", "C003"],
        "tipo_limite": ["Global", "Global", "Global"],
        "valor_limite": [15_000_000.0, 8_000_000.0, 12_000_000.0],
        "valor_utilizado": [10_000_000.0, 5_000_000.0, 8_000_000.0],
        "data_aprovacao": ["2023-01-01", "2022-06-01", "2023-03-01"],
        "data_revisao": ["2024-01-01", "2023-06-01", "2024-03-01"],
        "status_limite": ["Ativo", "Ativo", "Ativo"],
    })


@pytest.fixture
def raw_all(raw_clientes, raw_ratings, raw_exposicoes, raw_operacoes, raw_limites):
    return {
        "clientes": raw_clientes,
        "ratings": raw_ratings,
        "exposicoes": raw_exposicoes,
        "operacoes": raw_operacoes,
        "limites": raw_limites,
    }
