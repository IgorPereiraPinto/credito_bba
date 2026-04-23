# Como Executar - analise_de_credito

Guia passo a passo para rodar o projeto com contexto didatico: o que fazer, em que ordem fazer, em qual ferramenta fazer e por que essa ordem faz sentido.

---

## Visao Rapida Da Sequencia

```text
1. preparar pasta e Git
2. abrir e entender as bases
3. abrir o projeto no VS Code
4. criar ambiente virtual Python
5. instalar bibliotecas
6. configurar .env
7. rodar ETL Python
8. rodar testes
9. rodar SQL
10. abrir dashboard
11. publicar e documentar
```

---

## Pre-Requisitos

- Python 3.11+
- Git
- VS Code
- Para SQL Server: SQL Server Developer Edition + SSMS
- Para Athena: conta AWS com acesso a S3, Glue e Athena
- Arquivo `dados_sinteticos_case.xlsx`

---

## Passo 1 - Preparar a pasta e o repositorio

Se o projeto ja existe no GitHub:

```bash
git clone https://github.com/IgorPereiraPinto/analise_de_credito.git
cd analise_de_credito
```

Por que comecar com `git clone`:

- baixa a estrutura correta
- mantem o nome da pasta igual ao nome do repositorio
- traz historico e documentacao junto

Se o projeto fosse criado do zero:

```bash
mkdir analise_de_credito
cd analise_de_credito
git init
```

---

## Passo 2 - Abrir e analisar a base antes do codigo

Antes do ETL, entenda o dado.

O que verificar na base:

- quais abas ou arquivos existem
- qual e a granularidade de cada tabela
- quais campos sao chave
- quais colunas representam valor, data, status, score, rating e exposicao
- onde estao os campos que sustentam os KPIs do dashboard

Perguntas de negocio recomendadas:

1. quem e a entidade principal da analise?
2. quais riscos o negocio quer monitorar?
3. o que caracteriza uma carteira saudavel?
4. o que caracteriza uma carteira em atencao?
5. quais campos permitem medir limite, utilizacao, inadimplencia, score e probabilidade de default?

---

## Passo 3 - Abrir no VS Code

Comando:

```bash
code .
```

Se `code .` nao funcionar, abra a pasta manualmente no VS Code.

Por que isso vem cedo:

- voce ganha navegacao rapida entre docs, scripts e SQL
- terminal e codigo ficam no mesmo contexto
- fica mais facil alternar entre execucao e leitura

---

## Passo 4 - Criar o ambiente Python

Comando:

```bash
python -m venv .venv
```

Ativacao:

```bash
.venv\Scripts\activate
```

No Linux/Mac:

```bash
source .venv/bin/activate
```

Por que essa etapa existe:

- isola dependencias do projeto
- evita poluir o Python global
- melhora reprodutibilidade

---

## Passo 5 - Instalar as bibliotecas

```bash
pip install -r requirements.txt
pip install -r requirements-dev.txt
```

Por que nessa ordem:

- `requirements.txt` faz o projeto rodar
- `requirements-dev.txt` adiciona testes e apoio ao desenvolvimento

Bibliotecas principais do projeto:

- `pandas`
- `openpyxl`
- `python-dotenv`
- `loguru`
- `pytest`

---

## Passo 6 - Configurar o `.env`

Windows:

```bash
copy .env.example .env
```

Linux/Mac:

```bash
cp .env.example .env
```

Depois disso, revise:

- caminho da base bruta
- pasta de saida processada
- formato de exportacao
- conexao SQL Server, se aplicavel
- parametros AWS/Athena, se aplicavel

---

## Passo 7 - O que fazer no terminal e o que fazer no codigo

No terminal:

- ativar ambiente virtual
- instalar dependencias
- rodar ETL
- rodar testes
- executar Git

No codigo:

- ajustar regras de extracao
- ajustar limpeza e tipagem
- ajustar validacoes
- adaptar SQL
- ajustar dashboard e documentacao

Regra simples:

- terminal executa
- codigo define a logica

---

## Passo 8 - Rodar o ETL Python

Forma recomendada:

```bash
python run_etl.py
```

Por que esta e a forma recomendada:

- executa as 4 etapas em ordem
- reduz erro operacional
- representa a execucao oficial do pipeline local

Execucao detalhada para estudo ou debug:

```bash
python python/01_extract.py
python python/02_clean.py
python python/03_validate.py
python python/04_export.py
```

O que cada etapa faz:

- `01_extract.py`: le o Excel e valida estrutura minima
- `02_clean.py`: limpa, tipa e padroniza os dados
- `03_validate.py`: aplica regras de qualidade e negocio
- `04_export.py`: gera arquivos prontos para carga

Saidas esperadas em `data/processed/`:

- `clientes.csv`
- `operacoes.csv`
- `ratings.csv`
- `limites.csv`
- `exposicoes.csv`
- `validation_report.csv`

---

## Passo 9 - Rodar os testes

```bash
pytest -q
```

Por que testar antes do SQL:

- garante que a base exportada nao foi produzida com erro silencioso
- valida regras essenciais do pipeline
- melhora confianca antes da carga analitica

---

## Passo 10 - Executar o SQL Server

Ordem obrigatoria:

```text
sql/sqlserver/00_ddl.sql
sql/sqlserver/01_raw_insert.sql
sql/sqlserver/02_stage_views.sql
sql/sqlserver/03_dw_views.sql
sql/sqlserver/04_queries_analiticas.sql
sql/sqlserver/05_views_kpi.sql
```

Por que nessa ordem:

- primeiro cria banco e tabelas
- depois carrega os dados processados
- depois cria as camadas enriquecidas
- por fim disponibiliza queries e KPIs de consumo

Atencao:

- no `01_raw_insert.sql`, ajuste o caminho do `BULK INSERT` para a sua maquina

---

## Passo 11 - Executar a versao Athena

Se o alvo for AWS/Athena, ajuste o `.env` e rode o ETL com `EXPORT_FORMAT=parquet`.

Depois execute:

```text
sql/athena/00_ddl_external.sql
sql/athena/02_stage_views.sql
sql/athena/03_dw_views.sql
sql/athena/04_queries_analiticas.sql
sql/athena/05_views_kpi.sql
```

---

## Passo 12 - Abrir o dashboard

Opcao publicada:

- [Dashboard no GitHub Pages](https://igorpereirapinto.github.io/analise_de_credito/)

Opcao local:

```bash
start dashboards/dashboard_credito_bba.html
```

No Mac:

```bash
open dashboards/dashboard_credito_bba.html
```

---

## Passo 13 - Versionar no Git

Comandos basicos:

```bash
git status
git add .
git commit -m "feat: executa pipeline e atualiza documentacao"
git pull origin master
git push origin master
```

Justificativa da ordem:

1. `git status` para entender o estado atual
2. `git add` para selecionar o que vai entrar no commit
3. `git commit` para registrar uma unidade de trabalho
4. `git pull` para alinhar com o remoto antes de publicar
5. `git push` para enviar e, quando aplicavel, disparar o GitHub Pages

---

## Onde o processo termina

O processo termina quando:

- ETL roda com sucesso
- testes passam
- SQL entrega a camada analitica
- KPIs batem com a regra de negocio
- dashboard abre corretamente
- documentacao explica como repetir tudo

Se a ideia for estudar o projeto inteiro, siga o workflow da pasta [roadmap/](../roadmap/).

---

## Proximos Guias

- [roadmap/00_setup_local_e_git.md](../roadmap/00_setup_local_e_git.md)
- [roadmap/03_analise_da_base_excel.md](../roadmap/03_analise_da_base_excel.md)
- [roadmap/05_etl_extracao.md](../roadmap/05_etl_extracao.md)
- [roadmap/14_como_reutilizar_o_projeto.md](../roadmap/14_como_reutilizar_o_projeto.md)
