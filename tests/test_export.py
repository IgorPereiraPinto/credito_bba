"""Testes para 04_export.py — serialização dos dados validados."""
import importlib.util
import sys
import types
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).parent.parent


def _load_export():
    path = ROOT / "python" / "04_export.py"
    spec = importlib.util.spec_from_file_location("etl_04", path)
    mod = types.ModuleType("etl_04")
    sys.modules["etl_04"] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def export_mod():
    return _load_export()


@pytest.fixture
def sample_data():
    return {
        "clientes": pd.DataFrame({
            "cliente_id": ["C001", "C002"],
            "segmento": ["CORPORATE", "MIDDLE MARKET"],
            "status_cliente": ["ATIVO", "ATIVO"],
        }),
        "ratings": pd.DataFrame({
            "cliente_id": ["C001", "C002"],
            "pd_12m": [0.001, 0.02],
            "score_interno": [920, 750],
        }),
    }


class TestExportCSV:
    def test_csv_criado(self, export_mod, sample_data, tmp_path):
        import os
        os.environ["DATA_PROCESSED_PATH"] = str(tmp_path)
        os.environ["EXPORT_FORMAT"] = "csv"
        # recarrega variáveis de ambiente no módulo
        export_mod.DATA_PROCESSED_PATH = str(tmp_path)
        export_mod.EXPORT_FORMAT = "csv"
        export_mod.export_all(sample_data)
        assert (tmp_path / "clientes.csv").exists()
        assert (tmp_path / "ratings.csv").exists()

    def test_csv_separador_ponto_virgula(self, export_mod, sample_data, tmp_path):
        export_mod.DATA_PROCESSED_PATH = str(tmp_path)
        export_mod.EXPORT_FORMAT = "csv"
        export_mod.export_all(sample_data)
        content = (tmp_path / "clientes.csv").read_text(encoding="utf-8-sig")
        assert ";" in content

    def test_csv_sem_index(self, export_mod, sample_data, tmp_path):
        export_mod.DATA_PROCESSED_PATH = str(tmp_path)
        export_mod.EXPORT_FORMAT = "csv"
        export_mod.export_all(sample_data)
        df = pd.read_csv(tmp_path / "clientes.csv", sep=";", encoding="utf-8-sig")
        assert "Unnamed: 0" not in df.columns

    def test_csv_linhas_preservadas(self, export_mod, sample_data, tmp_path):
        export_mod.DATA_PROCESSED_PATH = str(tmp_path)
        export_mod.EXPORT_FORMAT = "csv"
        export_mod.export_all(sample_data)
        df = pd.read_csv(tmp_path / "clientes.csv", sep=";", encoding="utf-8-sig")
        assert len(df) == 2


pyarrow = pytest.importorskip("pyarrow", reason="pyarrow não instalado — skip testes parquet")


class TestExportParquet:
    def test_parquet_criado(self, export_mod, sample_data, tmp_path):
        export_mod.DATA_PROCESSED_PATH = str(tmp_path)
        export_mod.EXPORT_FORMAT = "parquet"
        export_mod.export_all(sample_data)
        assert (tmp_path / "clientes.parquet").exists()

    def test_parquet_linhas_preservadas(self, export_mod, sample_data, tmp_path):
        export_mod.DATA_PROCESSED_PATH = str(tmp_path)
        export_mod.EXPORT_FORMAT = "parquet"
        export_mod.export_all(sample_data)
        df = pd.read_parquet(tmp_path / "clientes.parquet")
        assert len(df) == 2
