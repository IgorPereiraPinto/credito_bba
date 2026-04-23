# Template — Como Reutilizar para um Novo Case

Guia direto para adaptar este projeto para um novo domínio de crédito ou análise de dados, com mapa explícito do que deve ser clonado sem alteração e do que deve ser reescrito.

---

## Lógica de Reutilização

Este projeto tem duas camadas bem separadas:

- **Infraestrutura técnica** — estrutura de pastas, runner, testes, Makefile, CI. Pode ser clonada sem alteração na maioria dos casos.
- **Regras de negócio e domínio** — schemas, validações, KPIs, SQL, dashboard. Deve ser reescrita para o novo case.

---

## Mapa de Arquivos: Clone vs Reescrever

### Clone sem alteracao (infraestrutura fixa)

| Arquivo / Pasta | Por que pode clonar |
| --- | --- |
| `run_etl.py` | Orquestrador genérico — funciona para qualquer pipeline de 4 etapas |
| `Makefile` | Comandos independentes de domínio |
| `requirements.txt` | Dependências de infraestrutura |
| `requirements-dev.txt` | Ferramentas de desenvolvimento |
| `.env.example` | Estrutura de variáveis (ajuste apenas os nomes do banco e paths) |
| `.gitignore` | Regras de versionamento |
| `tests/conftest.py` | Fixtures base — ajuste apenas os nomes das tabelas |
| `docs/arquitetura.md` | Padrão medallion é reutilizável — atualize apenas o diagrama |
| `roadmap/` | Sequência didática independente de domínio |

### Reescrever para o novo case (regras de negócio)

| Arquivo / Pasta | O que ajustar |
| --- | --- |
| `python/01_extract.py` | `EXPECTED_SHEETS`: abas e colunas da nova fonte |
| `python/02_clean.py` | `DTYPE_MAP`: tipos dos campos do novo domínio. `NULL_STRATEGY`: regra de nulos por campo |
| `python/03_validate.py` | `VALIDATION_RULES`: regras de qualidade e negócio específicas |
| `sql/sqlserver/00_ddl.sql` | Estrutura das tabelas — ajuste campos, tipos e chaves |
| `sql/sqlserver/01_raw_insert.sql` | Caminhos e colunas do BULK INSERT |
| `sql/sqlserver/02_stage_views.sql` | Lógica de enriquecimento específica do domínio |
| `sql/sqlserver/03_dw_views.sql` | KPIs e métricas do novo case |
| `sql/sqlserver/05_views_kpi.sql` | Camada semântica para consumo BI |
| `dashboards/` | Dashboard com dados, KPIs e narrativa do novo caso |
| `docs/regras_de_negocio.md` | Benchmarks e thresholds do novo domínio |
| `docs/dicionario_de_dados.md` | Tabelas, campos e views do novo modelo |
| `data/raw/` | Substituir pela nova fonte |
| `README.md` | Descrição, links e contexto do novo projeto |

---

## Passo a Passo para um Novo Case

### 1. Clonar a estrutura

```bash
git clone https://github.com/IgorPereiraPinto/analise_de_credito.git novo_case
cd novo_case
git remote remove origin
git remote add origin https://github.com/seu-usuario/novo_case.git
```

### 2. Substituir a fonte de dados

Coloque o novo arquivo em `data/raw/` e ajuste o `.env`:

```
DATA_RAW_PATH=data/raw/seu_arquivo.xlsx
```

### 3. Ajustar a extração

Em `python/01_extract.py`, localize e edite:

```python
EXPECTED_SHEETS = {
    "nome_aba": ["col1", "col2", "col3"],   # abas e colunas obrigatórias
}
```

### 4. Ajustar a limpeza

Em `python/02_clean.py`, localize e edite:

```python
DTYPE_MAP = {
    "campo_data": "datetime",
    "campo_valor": "float",
    "campo_id": "str",
}

NULL_STRATEGY = {
    "campo_critico": "drop",      # linha removida se nulo
    "campo_opcional": "fill_unknown",
    "campo_numerico": "fill_zero",
}
```

### 5. Ajustar as validações

Em `python/03_validate.py`, localize e edite `VALIDATION_RULES`. Cada regra tem:

- `campo`: campo a validar
- `condicao`: função que retorna True quando o dado está inválido
- `severidade`: `"error"` (remove a linha) ou `"warning"` (apenas loga)
- `mensagem`: texto explicativo

### 6. Ajustar o SQL

Execute os scripts na ordem e adapte para o novo modelo:

```text
00_ddl.sql         → ajuste nome do banco, tabelas e tipos
01_raw_insert.sql  → ajuste caminhos do BULK INSERT
02_stage_views.sql → ajuste joins e enriquecimentos
03_dw_views.sql    → ajuste KPIs e métricas de negócio
05_views_kpi.sql   → ajuste camada semântica
```

### 7. Atualizar documentação

- `docs/regras_de_negocio.md` — novos benchmarks e thresholds
- `docs/dicionario_de_dados.md` — novas tabelas, campos e views
- `README.md` — novo título, contexto e links

### 8. Validar tudo

```bash
make check    # valida .env e arquivos
make etl      # roda o pipeline
make test     # valida com testes
```

---

## Perguntas de Negócio para Começar

Antes de alterar qualquer código, responda:

1. Qual é a entidade principal da análise? (cliente, contrato, produto, operação)
2. Quais riscos o negócio quer monitorar?
3. O que caracteriza um estado saudável? O que caracteriza atenção ou criticidade?
4. Quais campos sustentam os KPIs do dashboard?
5. Qual é a granularidade de cada tabela?
6. Onde estão as chaves primárias e estrangeiras?
7. Quais campos mudam no tempo e precisam de histórico?
8. Quais são os benchmarks regulatórios ou internos para as métricas principais?

---

## O Que Nao Mudar

Se você estiver apenas trocando a fonte de dados sem mudar o domínio de crédito, estes arquivos provavelmente não precisam de alteração:

- `run_etl.py`
- `Makefile`
- `tests/test_integration.py` (estrutura geral)
- `sql/sqlserver/03_dw_views.sql` (se os KPIs de crédito forem os mesmos)

---

## Referencia

- [docs/faq_reutilizacao.md](faq_reutilizacao.md) — perguntas frequentes sobre portabilidade
- [docs/como_executar.md](como_executar.md) — guia de execução com checklists
- [roadmap/14_como_reutilizar_o_projeto.md](../roadmap/14_como_reutilizar_o_projeto.md) — contexto completo
