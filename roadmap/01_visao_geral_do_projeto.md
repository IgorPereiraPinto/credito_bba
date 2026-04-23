# 01 - Visao Geral do Projeto

## Objetivo da etapa

Entender o que este projeto faz, por que foi construido desta forma e como navegar por ele antes de entrar no detalhe de ETL, SQL e dashboard.

Se voce ainda nao preparou o ambiente local, volte para [00_setup_local_e_git.md](00_setup_local_e_git.md).

---

## O que e este projeto

Um pipeline de dados ponta a ponta para analise de portfolio de credito corporativo, construido a partir de um case tecnico real.

Problema central:

- transformar dados brutos de um Excel com 5 tabelas relacionadas em indicadores acionaveis para analise de risco e decisao de credito

---

## Por que o projeto foi estruturado assim

Ele foi desenhado para atender 3 objetivos ao mesmo tempo:

- **Didatico:** cada pasta e cada etapa explicam um papel especifico
- **Funcional:** o pipeline pode ser executado do inicio ao fim
- **Reutilizavel:** a estrutura pode ser adaptada para outro case de credito com ajustes localizados

---

## Fluxo macro do projeto

```text
[1] Fonte
    Excel com dados de clientes, operacoes, ratings, limites e exposicoes

[2] Python ETL
    Extracao -> Limpeza -> Validacao -> Exportacao

[3] SQL - 3 camadas
    RAW   -> landing controlado
    STAGE -> enriquecimento e consolidacao
    DW    -> KPIs e views de consumo

[4] Consumo
    Dashboard HTML -> Power BI -> QuickSight -> apresentacao executiva
```

---

## Mapa do repositorio

| Pasta | Papel no fluxo | Quando usar |
|---|---|---|
| `python/` | ETL e validacoes | primeiro bloco tecnico |
| `sql/sqlserver/` | execucao local da camada analitica | apos o ETL |
| `sql/athena/` | variante AWS/Athena | quando o alvo e cloud |
| `data/` | entrada e saida do ETL | durante extracao e exportacao |
| `docs/` | regras, dicionario, arquitetura e execucao | consulta transversal |
| `dashboards/` | entrega visual do projeto | etapa final |
| `roadmap/` | workflow didatico de ponta a ponta | estudo guiado |
| `tests/` | garantia de qualidade do ETL | antes de promover a saida |

---

## Ordem de execucao recomendada

1. preparar ambiente e Git
2. entender o problema de negocio
3. analisar a base Excel
4. desenhar o pipeline
5. executar o Python ETL
6. executar SQL `raw`, `stage` e `dw`
7. validar KPIs e queries
8. montar e revisar dashboard
9. consolidar storytelling e reutilizacao

---

## Onde esta etapa termina

Esta etapa termina quando voce:

- entende o objetivo do projeto
- conhece a estrutura das pastas
- sabe a ordem macro de execucao
- consegue apontar onde comeca e onde termina o fluxo completo

Proximo passo: [02_entendimento_do_case.md](02_entendimento_do_case.md)
