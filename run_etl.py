"""Runner do pipeline ETL completo — executa os 4 scripts em sequência.

Os scripts em python/ são carregados via importlib em vez de import direto porque
o projeto não tem __init__.py (não é um pacote instalável). Essa abordagem permite
executar `python run_etl.py` da raiz do projeto sem setup adicional, mantendo cada
script também executável de forma independente com `python python/01_extract.py`.
"""
import importlib.util
import os
import sys
import types
from pathlib import Path

from dotenv import load_dotenv
from loguru import logger

load_dotenv()

BASE = Path(__file__).parent


def _check_env() -> None:
    """Valida variáveis críticas do .env e existência dos arquivos necessários.

    Encerra com saída guiada se a configuração estiver incompleta, evitando
    erros silenciosos ou mensagens genéricas do Python nas etapas seguintes.
    """
    errors: list[str] = []

    required_vars = ["DATA_RAW_PATH", "DATA_PROCESSED_PATH"]
    for var in required_vars:
        if not os.getenv(var):
            errors.append(
                f"  ✗ {var} não definida no .env\n"
                f"    → copie .env.example para .env e preencha os valores"
            )

    raw_path = os.getenv("DATA_RAW_PATH", "data/raw/dados_sinteticos_case.xlsx")
    if not (BASE / raw_path).exists():
        errors.append(
            f"  ✗ Arquivo de entrada não encontrado: {raw_path}\n"
            f"    → coloque o Excel em data/raw/ ou ajuste DATA_RAW_PATH no .env"
        )

    processed_path = os.getenv("DATA_PROCESSED_PATH", "data/processed/")
    processed_dir = BASE / processed_path
    if not processed_dir.exists():
        logger.warning(f"Pasta de saída não encontrada — criando: {processed_path}")
        processed_dir.mkdir(parents=True, exist_ok=True)

    if errors:
        logger.error("Configuração incompleta — corrija os itens abaixo antes de continuar:\n")
        for e in errors:
            logger.error(e)
        logger.info("\nDica: copie .env.example para .env e revise os caminhos e variáveis.")
        sys.exit(1)

    logger.info(
        f"Configuração validada — entrada: {raw_path} | saída: {processed_path}"
    )


_check_env()


def _load(filename: str, name: str):
    path = BASE / "python" / filename
    spec = importlib.util.spec_from_file_location(name, path)
    mod = types.ModuleType(name)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


e01 = _load("01_extract.py",  "etl_01")
e02 = _load("02_clean.py",    "etl_02")
e03 = _load("03_validate.py", "etl_03")
e04 = _load("04_export.py",   "etl_04")

logger.info("━━━ ETAPA 1/4 — Extração ━━━")
raw = e01.main()

logger.info("━━━ ETAPA 2/4 — Limpeza ━━━")
cleaned = e02.clean_all(raw)

logger.info("━━━ ETAPA 3/4 — Validação ━━━")
validated = e03.validate_all(cleaned)

logger.info("━━━ ETAPA 4/4 — Exportação ━━━")
e04.export_all(validated)

logger.success("Pipeline ETL concluído.")
