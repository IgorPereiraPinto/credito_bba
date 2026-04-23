# Arquitetura do Pipeline — analise_de_credito

## Visão geral

O pipeline segue a arquitetura **Medallion** (Bronze/Silver/Gold), adaptada para o contexto
deste projeto onde a fonte é um Excel e o destino é um banco relacional local (SQL Server)
ou um Data Lake na AWS (Athena + S3).

```text
[FONTE]      [PYTHON ETL — porta de qualidade]    [SQL — 3 CAMADAS]       [CONSUMO]

Excel        01_extract.py  →  validação de
dados_sint.                    schema e abas
             02_clean.py    →  tipagem, nulos,
                               normalização
             03_validate.py →  regras de negócio   RAW  ← landing SQL
                               relatório de         │    (dado validado
             04_export.py   →  CSV / Parquet   ─────┘     pelo ETL)
                                                    ↓
                                                   STAGE  →  Power BI
                                                   (Silver)   QuickSight
                                                    ↓
                                                    DW    →  Dashboard HTML
                                                   (Gold)     Apresentação
```

> **Decisão arquitetural explícita:** a camada RAW do SQL recebe o output de `data/processed/`,
> ou seja, dado já validado pelo Python ETL. O Python é a **porta de qualidade da fonte** —
> lida com o que SQL não faz bem (leitura de Excel, coerção de tipos, validação de schema).
> A partir do RAW SQL, vale a regra medallion: nenhuma transformação de negócio no RAW,
> enriquecimento no STAGE, KPIs no DW. Para um pipeline com ingestão direta (sem ETL Python),
> o RAW SQL poderia receber um dump literal da fonte — essa é a extensão natural do design.

---

## Camadas e responsabilidades

### Python ETL — porta de qualidade

- **Papel:** extração e validação antes do SQL. Não é uma camada SQL, mas um pré-requisito.
- **O que produz:** arquivos em `data/processed/` prontos para carga no banco
- **Regra:** não aplica regras de negócio analíticas — só qualidade estrutural (schema, tipos, nulos)

### RAW (Bronze / primeiro landing SQL)

- **O que contém:** dado validado pelo ETL Python, sem transformações de negócio no SQL
- **No SQL Server:** tabelas relacionais com PKs, FKs e índices
- **No Athena:** tabelas externas apontando para Parquet no S3
- **Regra:** a partir daqui nenhuma linha é removida ou modificada por SQL — apenas lida
- **Arquivos:** `sql/sqlserver/00_ddl.sql`, `sql/sqlserver/01_raw_insert.sql`

### STAGE (Silver)

- **O que contém:** dado limpo, enriquecido e consolidado
- **Transformações aplicadas:**
  - Joins base entre as 5 tabelas
  - Campo `pct_exposicao_descoberta` calculado
  - Seleção do último snapshot de rating e exposição por cliente
  - Consolidação de limites por cliente
  - Escala numérica de rating (1-17)
- **Arquivos:** `sql/sqlserver/02_stage_views.sql`

### DW / Gold

- **O que contém:** KPIs prontos para consumo, classificações de risco, rankings
- **Transformações aplicadas:**
  - Score ponderado por exposição
  - Matriz de risco combinado (deterioração + utilização + descoberta)
  - Evolução temporal de rating com LAG e RANK
  - Flags de alerta regulatório por subsetor
- **Arquivos:** `sql/sqlserver/03_dw_views.sql`, `sql/sqlserver/05_views_kpi.sql`

---

## Dualidade SQL Server / Athena

> **Nota técnica:** As queries SQL foram escritas originalmente em **SQL Server** para
> prototipagem e validação local. O script completo foi convertido para **Amazon Athena**,
> mantendo equivalência funcional entre os dois ambientes.

As duas implementações são **funcionalmente equivalentes**. A diferença é o ambiente:

| Dimensão           | SQL Server (local)             | Athena (AWS)                       |
|--------------------|--------------------------------|------------------------------------|
| Propósito          | Desenvolvimento e análise      | Produção escalável                 |
| Armazenamento      | Tabelas relacionais locais     | Parquet no S3                      |
| Integridade ref.   | FK e PK declaradas             | Garantida pelo ETL Python          |
| Escala             | Até ~100M linhas com conforto  | Petabytes — serverless             |
| Custo              | Infraestrutura local           | Pay-per-query                      |
| BI conectado       | Power BI DirectQuery           | Amazon QuickSight SPICE            |

As diferenças de **sintaxe** estão documentadas em [roadmap/10_sql_server_vs_athena.md](../roadmap/10_sql_server_vs_athena.md).

---

## Decisões de arquitetura

### Por que views e não tabelas materializadas na camada STAGE?

Views permitem que qualquer atualização nos dados RAW seja imediatamente refletida
nos KPIs sem re-execução do pipeline SQL. Para um projeto de portfólio com carga mensal,
views são suficientes e mais simples de manter.

> Para projetos de alta frequência (diário ou near-realtime), considere materializar
> as views STAGE como tabelas com carga incremental via dbt ou Glue.

### Por que Parquet no Athena?

Parquet com compressão Snappy reduz custo de scan no Athena em até 85% comparado ao CSV,
além de permitir consultas seletivas por coluna — crítico para tabelas de exposições
com muitas colunas.

### Por que separar ETL Python do SQL?

O Python lida com o que o SQL não faz bem: leitura de Excel, validação de schema,
tratamento de nulos com lógica condicional e geração de relatório de qualidade.
O SQL lida com o que o Python não faz melhor: joins, window functions e camada semântica.
A separação também facilita reutilização — os scripts Python funcionam com qualquer
Excel estruturado, independente do banco de destino.
