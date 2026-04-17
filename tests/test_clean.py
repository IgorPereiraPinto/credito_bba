"""Testes para 02_clean.py — limpeza e padronização."""
import pandas as pd
import pytest


class TestNormalizeStrings:
    def test_strings_categoria_uppercase(self, clean_mod, raw_clientes):
        cleaned = clean_mod.clean_all({"clientes": raw_clientes})
        df = cleaned["clientes"]
        assert df["segmento"].str.isupper().all()
        assert df["porte"].str.isupper().all()

    def test_cliente_id_preserva_case(self, clean_mod, raw_clientes):
        cleaned = clean_mod.clean_all({"clientes": raw_clientes})
        assert cleaned["clientes"]["cliente_id"].tolist() == ["C001", "C002", "C003"]


class TestConvertTypes:
    def test_pd_12m_float(self, clean_mod, raw_ratings):
        cleaned = clean_mod.clean_all({"ratings": raw_ratings})
        assert cleaned["ratings"]["pd_12m"].dtype == float

    def test_data_referencia_convertida(self, clean_mod, raw_ratings):
        cleaned = clean_mod.clean_all({"ratings": raw_ratings})
        # pd.to_datetime com .dt.date retorna dtype object (Python date objects)
        col = cleaned["ratings"]["data_referencia"]
        assert col.notna().all()

    def test_valor_aprovado_float(self, clean_mod, raw_operacoes):
        cleaned = clean_mod.clean_all({"operacoes": raw_operacoes})
        assert cleaned["operacoes"]["valor_aprovado"].dtype == float


class TestHandleNulls:
    def test_drop_cliente_id_nulo(self, clean_mod, raw_clientes):
        raw_clientes.loc[0, "cliente_id"] = None
        cleaned = clean_mod.clean_all({"clientes": raw_clientes.copy()})
        assert len(cleaned["clientes"]) == 2

    def test_fill_zero_valor_utilizado(self, clean_mod, raw_operacoes):
        raw_operacoes.loc[0, "valor_utilizado"] = None
        cleaned = clean_mod.clean_all({"operacoes": raw_operacoes.copy()})
        assert cleaned["operacoes"]["valor_utilizado"].iloc[0] == 0.0

    def test_fill_unknown_segmento(self, clean_mod, raw_clientes):
        raw_clientes.loc[1, "segmento"] = None
        cleaned = clean_mod.clean_all({"clientes": raw_clientes.copy()})
        assert "DESCONHECIDO" in cleaned["clientes"]["segmento"].values

    def test_dedup_remove_duplicatas(self, clean_mod, raw_clientes):
        duplicated = pd.concat([raw_clientes, raw_clientes.iloc[[0]]], ignore_index=True)
        cleaned = clean_mod.clean_all({"clientes": duplicated})
        assert len(cleaned["clientes"]) == 3
