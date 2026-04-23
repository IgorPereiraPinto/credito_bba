# Python ETL — analise_de_credito

Scripts de extração, limpeza, validação e exportação dos dados de crédito corporativo.

---

## Ordem de execução

Execute sempre nesta sequência. Cada script depende da saída do anterior.

```text
01_extract.py   →   02_clean.py   →   03_validate.py   →   04_export.py
     ↓                   ↓                   ↓                    ↓
 Lê o Excel       Limpa e padroniza    Valida regras        Exporta CSVs
 Valida abas       tipos e strings      de negócio           para carga
 Valida schema     trata nulos          gera relatório        no banco
```

---

## Scripts

### 01_extract.py — Extração

**O que faz:** lê o arquivo Excel, valida se as abas esperadas existem, valida o schema mínimo de cada aba (colunas obrigatórias) e carrega os dados em DataFrames pandas.

**Etapa no pipeline:** entrada — primeiro contato com os dados brutos.

**Parâmetros para reutilização:**
- `DATA_RAW_PATH` no `.env`: caminho para o Excel de entrada
- `EXPECTED_SHEETS`: dicionário com nome de cada aba e colunas obrigatórias
- Para um novo case: ajuste `EXPECTED_SHEETS` com as abas e colunas do novo arquivo

**Regras de negócio embutidas:** nenhuma. Esta etapa é puramente estrutural.

**Saída:** dicionário de DataFrames `{nome_aba: DataFrame}` em memória.

---

### 02_clean.py — Limpeza e Padronização

**O que faz:** recebe os DataFrames brutos e aplica limpeza e padronização:
- converte tipos (string → date, string → decimal)
- normaliza strings (strip, upper/lower conforme campo)
- trata nulos com estratégia explícita por campo (`drop`, `fill_zero`, `fill_unknown`)
- remove linhas duplicadas (`drop_duplicates`)

**Etapa no pipeline:** Silver — dado limpo, ainda sem regra de negócio.

**Parâmetros para reutilização:**
- `DTYPE_MAP`: mapeamento de tipos por tabela e campo
- `NULL_STRATEGY`: estratégia de nulo por campo (`drop`, `fill_zero`, `fill_unknown`)
- Para um novo case: ajuste os dois dicionários acima

**Regras de negócio embutidas:**
- campos de valor (`valor_aprovado`, `valor_utilizado`, `exposicao_total`) não aceitam nulo → linha descartada com log

**Saída:** dicionário de DataFrames limpos `{nome_tabela: DataFrame}`.

---

### 03_validate.py — Validação de Qualidade e Negócio

**O que faz:** aplica validações de qualidade de dados e regras de negócio, gerando um relatório consolidado de inconsistências.

**Validações aplicadas:**
1. `valor_utilizado <= valor_aprovado` (operacoes)
2. `exposicao_descoberta = exposicao_total - exposicao_garantida` (exposicoes)
3. `pd_12m` entre 0 e 1 (ratings)
4. `data_vencimento >= data_aprovacao` (operacoes)
5. `valor_limite > 0` (limites)
6. Unicidade de PK em cada tabela

**Etapa no pipeline:** ponto de controle entre Silver e Gold.

**Parâmetros para reutilização:**
- As validações estão implementadas como funções na seção `validate_all()`
- Para um novo case: adicione ou remova blocos de validação diretamente no script
- Cada regra declara tabela, condição, severidade (`error` remove a linha, `warning` registra)

**Saída:**
- DataFrames validados (linhas com erro severo removidas)
- `data/processed/validation_report.csv` com detalhamento de cada falha

---

### 04_export.py — Exportação

**O que faz:** recebe os DataFrames validados e os exporta como CSVs na pasta `data/processed/`, prontos para carga no SQL Server ou upload para S3/Athena.

**Etapa no pipeline:** saída — dado pronto para o banco.

**Parâmetros para reutilização:**
- `DATA_PROCESSED_PATH` no `.env`: pasta de destino
- `EXPORT_FORMAT`: `csv` (padrão), `parquet` (para S3/Athena)
- Para Athena: troque para `parquet` e ajuste o path para o bucket S3

**Saída:**
- `data/processed/clientes.csv`
- `data/processed/operacoes.csv`
- `data/processed/ratings.csv`
- `data/processed/limites.csv`
- `data/processed/exposicoes.csv`
- `data/processed/validation_report.csv`

---

## Como reutilizar em outro case

1. Copie a pasta `python/` inteira para o novo projeto
2. Em `01_extract.py`, ajuste `EXPECTED_SHEETS` com as abas e colunas do novo Excel
3. Em `02_clean.py`, ajuste `DTYPE_MAP` e `NULL_STRATEGY` para os campos do novo domínio
4. Em `03_validate.py`, adicione ou remova blocos de validação na função `validate_all()`
5. `04_export.py` geralmente não precisa de ajuste — funciona com qualquer conjunto de tabelas

---

## Dependências

```bash
pip install -r requirements.txt
```

Principais: `pandas`, `openpyxl`, `python-dotenv`, `loguru`
