# ================================================================
# Makefile — analise_de_credito
# Requer: GNU Make (nativo em Linux/Mac · no Windows use Git Bash ou WSL)
# Alternativa Windows: python -m etl | python -m pytest -q
# ================================================================

.DEFAULT_GOAL := help
.PHONY: help setup etl test check open clean

help:  ## Mostra este menu de ajuda
	@echo ""
	@echo "  analise_de_credito — comandos disponíveis"
	@echo "  ─────────────────────────────────────────"
	@echo "  make setup    instala dependências (requirements.txt + dev)"
	@echo "  make etl      roda o pipeline ETL completo"
	@echo "  make test     executa a suite de testes (pytest -q)"
	@echo "  make check    valida .env e arquivos antes de rodar"
	@echo "  make open     abre o dashboard no navegador padrão"
	@echo "  make clean    remove arquivos gerados em data/processed/"
	@echo ""

setup:  ## Instala dependências de produção e desenvolvimento
	pip install -r requirements.txt
	pip install -r requirements-dev.txt

etl:  ## Roda o pipeline ETL completo (extract → clean → validate → export)
	python run_etl.py

test:  ## Executa a suite de testes com saída compacta
	pytest -q

check:  ## Valida .env e existência dos arquivos necessários
	@python -c "\
from pathlib import Path; \
from dotenv import load_dotenv; \
import os; \
load_dotenv(); \
r = os.getenv('DATA_RAW_PATH', 'data/raw/dados_sinteticos_case.xlsx'); \
p = os.getenv('DATA_PROCESSED_PATH', 'data/processed/'); \
print('✓  DATA_RAW_PATH:', r, '→', '✓ existe' if Path(r).exists() else '✗ NÃO ENCONTRADO'); \
print('✓  DATA_PROCESSED_PATH:', p, '→', '✓ existe' if Path(p).exists() else '⚠ será criada'); \
import pandas, openpyxl, loguru; print('✓  dependências: OK'); \
"

open:  ## Abre o dashboard HTML no navegador padrão
	@python -c "\
import webbrowser, pathlib; \
p = pathlib.Path('dashboards/dashboard_credito_bba.html').resolve(); \
webbrowser.open(p.as_uri()); \
print('Dashboard aberto:', p); \
"

clean:  ## Remove arquivos gerados pelo ETL (mantém .gitkeep)
	@python -c "\
import glob, os; \
files = [f for f in glob.glob('data/processed/*') if '.gitkeep' not in f]; \
[os.remove(f) for f in files]; \
print(f'Removido: {len(files)} arquivo(s) de data/processed/'); \
"
