# 01 — Visão Geral do Projeto

## Objetivo da etapa

Entender o que este projeto faz, por que foi construído desta forma e como navegar
por ele antes de começar a execução.

---

## O que é o credito_bba

Um pipeline de dados ponta a ponta para análise de portfólio de crédito corporativo,
construído a partir de um case técnico com dados fictícios.

Ele resolve um problema concreto: transformar dados brutos de um Excel com 5 tabelas
relacionadas em KPIs acionáveis para o Comitê de Crédito — com SQL, Python e um
dashboard executivo.

---

## Por que este projeto existe

Além de responder ao case técnico, o projeto foi estruturado para ser:

- **Didático:** cada pasta e arquivo tem um papel claro no pipeline
- **Funcional:** o código roda do começo ao fim sem adaptação manual
- **Reutilizável:** os scripts Python e as views SQL foram pensados para outro case de crédito

---

## Fluxo em 4 blocos

```text
[1] FONTE          Excel com dados sintéticos de 5 tabelas
        ↓
[2] PYTHON ETL     Extração → Limpeza → Validação → Exportação
        ↓
[3] SQL — 3 CAMADAS
    RAW   → dado bruto carregado
    STAGE → dado enriquecido com joins e campos calculados
    DW    → KPIs prontos para consumo
        ↓
[4] CONSUMO        Dashboard HTML | Power BI | Amazon QuickSight
```

---

## Mapa do repositório

| Pasta         | O que tem                                     | Quando usar                          |
|---------------|-----------------------------------------------|--------------------------------------|
| `python/`     | Scripts ETL comentados                        | Antes do SQL — sempre                |
| `sql/sqlserver/` | Pipeline completo para SQL Server          | Desenvolvimento local                |
| `sql/athena/` | Pipeline completo para Athena                 | Produção AWS                         |
| `data/`       | Entrada (raw) e saída do ETL (processed)      | Não versionar dados reais            |
| `docs/`       | Dicionário, regras de negócio, arquitetura    | Referência durante a execução        |
| `dashboards/` | Dashboard HTML single-file                    | Visualização final                   |
| `roadmap/`    | Guia de execução passo a passo (você está aqui) | Estudo e primeiro projeto           |

---

## Ordem de execução recomendada

1. Leia este arquivo (`01_visao_geral.md`)
2. Leia `02_entendimento_do_case.md`
3. Configure o ambiente (`docs/como_executar.md`)
4. Execute o Python ETL (`05_etl_extracao.md` → `06_etl_padronizacao_e_validacoes.md`)
5. Execute o SQL na ordem das camadas (`07_raw` → `08_stage` → `09_dw`)
6. Explore os KPIs e o dashboard
7. Para reutilização, leia `14_como_reutilizar_o_projeto.md`

---

## Checklist de conclusão da etapa

- [ ] Entendi o objetivo do projeto
- [ ] Entendi o fluxo dos 4 blocos
- [ ] Mapeei as pastas e sei o que cada uma contém
- [ ] Conheço a ordem de execução
- [ ] Avancei para `02_entendimento_do_case.md`
