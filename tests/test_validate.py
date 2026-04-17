"""Testes para 03_validate.py — regras de negócio de crédito."""
import pandas as pd
import pytest


class TestRegra1UtilizadoVsAprovado:
    def test_utilizado_maior_que_aprovado_gera_warning(self, validate_mod, raw_all):
        raw_all["operacoes"].loc[0, "valor_utilizado"] = 9_999_999.0
        raw_all["operacoes"].loc[0, "valor_aprovado"] = 1_000_000.0
        validated = validate_mod.validate_all(raw_all)
        # warning não remove linha
        assert len(validated["operacoes"]) == len(raw_all["operacoes"])

    def test_dados_validos_nao_afetam_operacoes(self, validate_mod, raw_all):
        original_len = len(raw_all["operacoes"])
        validated = validate_mod.validate_all(raw_all)
        assert len(validated["operacoes"]) == original_len


class TestRegra2PD12m:
    def test_pd_invalida_remove_linha(self, validate_mod, raw_all):
        raw_all["ratings"].loc[0, "pd_12m"] = 1.5  # fora de [0,1]
        validated = validate_mod.validate_all(raw_all)
        assert len(validated["ratings"]) == 2

    def test_pd_zero_valida(self, validate_mod, raw_all):
        raw_all["ratings"].loc[0, "pd_12m"] = 0.0
        validated = validate_mod.validate_all(raw_all)
        assert len(validated["ratings"]) == 3

    def test_pd_um_valida(self, validate_mod, raw_all):
        raw_all["ratings"].loc[0, "pd_12m"] = 1.0
        validated = validate_mod.validate_all(raw_all)
        assert len(validated["ratings"]) == 3


class TestRegra3ExposicaoAritmetica:
    def test_inconsistencia_aritmetica_gera_warning(self, validate_mod, raw_all):
        raw_all["exposicoes"].loc[0, "exposicao_descoberta"] = 9_999.0  # errado
        # warning não remove linhas
        validated = validate_mod.validate_all(raw_all)
        assert len(validated["exposicoes"]) == 3

    def test_aritmetica_correta_sem_alerta(self, validate_mod, raw_all):
        # exposicao_descoberta = total - garantida → já correto nas fixtures
        validated = validate_mod.validate_all(raw_all)
        assert len(validated["exposicoes"]) == 3


class TestRegra4Datas:
    def test_vencimento_antes_aprovacao_remove_linha(self, validate_mod, raw_all):
        raw_all["operacoes"].loc[0, "data_vencimento"] = "2020-01-01"
        raw_all["operacoes"].loc[0, "data_aprovacao"] = "2023-01-01"
        validated = validate_mod.validate_all(raw_all)
        assert len(validated["operacoes"]) == 2

    def test_mesma_data_valida(self, validate_mod, raw_all):
        raw_all["operacoes"].loc[0, "data_vencimento"] = "2023-01-01"
        raw_all["operacoes"].loc[0, "data_aprovacao"] = "2023-01-01"
        validated = validate_mod.validate_all(raw_all)
        assert len(validated["operacoes"]) == 3


class TestRegra5LimitePositivo:
    def test_limite_zero_remove_linha(self, validate_mod, raw_all):
        raw_all["limites"].loc[0, "valor_limite"] = 0
        validated = validate_mod.validate_all(raw_all)
        assert len(validated["limites"]) == 2

    def test_limite_negativo_remove_linha(self, validate_mod, raw_all):
        raw_all["limites"].loc[1, "valor_limite"] = -100
        validated = validate_mod.validate_all(raw_all)
        assert len(validated["limites"]) == 2


class TestRegra6PKUnicidade:
    def test_duplicata_pk_clientes_removida(self, validate_mod, raw_all):
        dup = raw_all["clientes"].iloc[[0]].copy()
        raw_all["clientes"] = pd.concat(
            [raw_all["clientes"], dup], ignore_index=True
        )
        validated = validate_mod.validate_all(raw_all)
        assert len(validated["clientes"]) == 3

    def test_pk_composta_ratings(self, validate_mod, raw_all):
        # PK de ratings = (cliente_id, data_referencia)
        dup = raw_all["ratings"].iloc[[0]].copy()
        raw_all["ratings"] = pd.concat(
            [raw_all["ratings"], dup], ignore_index=True
        )
        validated = validate_mod.validate_all(raw_all)
        assert len(validated["ratings"]) == 3
