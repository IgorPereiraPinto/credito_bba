# Dicionário de Dados — analise_de_credito

## Tabela: clientes

Cadastro base dos clientes corporativos do portfólio.

| Campo                        | Tipo        | Descrição                                          | Valores esperados               |
|------------------------------|-------------|----------------------------------------------------|---------------------------------|
| `cliente_id`                 | VARCHAR(10) | Identificador único do cliente. **PK**             | CLI001 a CLI100 (sintético)     |
| `segmento`                   | VARCHAR(20) | Segmento comercial do cliente                      | Corporate, Middle Market, Varejo|
| `porte`                      | VARCHAR(10) | Porte da empresa                                   | Grande, Médio, Pequeno          |
| `setor`                      | VARCHAR(30) | Setor econômico (CNAE nível 1)                     | Indústria, Serviços, Agro...    |
| `subsetor`                   | VARCHAR(50) | Subsetor econômico (CNAE nível 2)                  | Farmacêutico, Software e TI...  |
| `data_inicio_relacionamento` | DATE        | Data de abertura do relacionamento com o banco     | —                               |
| `regiao`                     | VARCHAR(15) | Região geográfica de atuação                       | Sudeste, Sul, Nordeste...       |
| `status_cliente`             | VARCHAR(10) | Status atual do cadastro                           | Ativo, Inativo, Suspenso        |

---

## Tabela: operacoes

Operações de crédito vigentes e históricas por cliente.

| Campo             | Tipo          | Descrição                                        | Observações                        |
|-------------------|---------------|--------------------------------------------------|------------------------------------|
| `operacao_id`     | VARCHAR(20)   | Identificador único da operação. **PK**          | —                                  |
| `cliente_id`      | VARCHAR(10)   | Referência ao cliente. **FK → clientes**         | —                                  |
| `produto`         | VARCHAR(30)   | Produto de crédito                               | CCB, CDB, Fiança, Capital Giro...  |
| `modalidade`      | VARCHAR(30)   | Modalidade de contratação                        | Pré-fixado, Pós-fixado, Flutuante  |
| `valor_aprovado`  | DECIMAL(15,2) | Valor total aprovado na operação                 | Nunca nulo                         |
| `valor_utilizado` | DECIMAL(15,2) | Valor efetivamente utilizado pelo cliente        | ≤ `valor_aprovado`                 |
| `taxa_juros`      | DECIMAL(8,4)  | Taxa de juros anual (decimal)                    | 0.0850 = 8,50% a.a.               |
| `prazo_meses`     | INT           | Prazo total da operação em meses                 | —                                  |
| `data_aprovacao`  | DATE          | Data de aprovação da operação                    | ≤ `data_vencimento`                |
| `data_vencimento` | DATE          | Data de vencimento                               | ≥ `data_aprovacao`                 |
| `garantia_tipo`   | VARCHAR(30)   | Tipo de garantia oferecida                       | Aval, Hipoteca, Penhor, Sem...     |
| `status_operacao` | VARCHAR(15)   | Status atual da operação                         | Ativa, Vencida, Liquidada          |

---

## Tabela: ratings

Histórico mensal de rating de crédito por cliente.

| Campo             | Tipo          | Descrição                                      | Observações                          |
|-------------------|---------------|------------------------------------------------|--------------------------------------|
| `cliente_id`      | VARCHAR(10)   | Referência ao cliente. **FK → clientes**       | Parte da PK composta                 |
| `data_referencia` | DATE          | Mês de referência do rating. Parte da **PK**   | Geralmente primeiro dia do mês       |
| `rating_interno`  | VARCHAR(5)    | Rating atribuído internamente pelo banco       | AAA, AA+, AA, ..., B-, C            |
| `rating_externo`  | VARCHAR(5)    | Rating de agência externa (S&P, Moody's, Fitch)| Pode ser nulo                        |
| `pd_12m`          | DECIMAL(8,6)  | Probabilidade de default em 12 meses           | Entre 0 e 1 (ex.: 0.012 = 1,2%)     |
| `score_interno`   | INT           | Score interno em escala numérica               | Escala 0-1000 (benchmarks na docs)   |
| `observacao`      | VARCHAR(150)  | Observação qualitativa do analista             | Campo livre, pode ser nulo           |

---

## Tabela: limites

Limites de crédito aprovados por cliente e tipo.

| Campo             | Tipo          | Descrição                                          | Observações                      |
|-------------------|---------------|----------------------------------------------------|----------------------------------|
| `cliente_id`      | VARCHAR(10)   | Referência ao cliente. **FK → clientes**           | Parte da PK composta             |
| `tipo_limite`     | VARCHAR(20)   | Tipo de limite aprovado. Parte da **PK**           | Global, Curto Prazo, Trade...    |
| `valor_limite`    | DECIMAL(15,2) | Valor do limite aprovado                           | > 0                              |
| `valor_utilizado` | DECIMAL(15,2) | Valor utilizado contra o limite                    | ≤ `valor_limite` esperado        |
| `data_aprovacao`  | DATE          | Data de aprovação do limite                        | —                                |
| `data_revisao`    | DATE          | Data prevista para revisão do limite               | —                                |
| `aprovador`       | VARCHAR(60)   | Nome/código do aprovador do limite                 | Campo sensível — omitido no case |
| `status_limite`   | VARCHAR(10)   | Status atual do limite                             | Ativo, Vencido, Cancelado        |

---

## Tabela: exposicoes

Posição consolidada mensal de exposição por cliente.

| Campo                  | Tipo          | Descrição                                          | Observações                          |
|------------------------|---------------|----------------------------------------------------|--------------------------------------|
| `cliente_id`           | VARCHAR(10)   | Referência ao cliente. **FK → clientes**           | Parte da PK composta                 |
| `data_referencia`      | DATE          | Data de referência da posição. Parte da **PK**     | Consolidação mensal                  |
| `exposicao_total`      | DECIMAL(15,2) | Exposição total do cliente no período              | Nunca nulo                           |
| `exposicao_garantida`  | DECIMAL(15,2) | Parcela da exposição coberta por garantias         | ≤ `exposicao_total`                  |
| `exposicao_descoberta` | DECIMAL(15,2) | Parcela sem cobertura de garantia                  | = `total` - `garantida`              |
| `provisao_necessaria`  | DECIMAL(15,2) | Provisão calculada conforme classificação de risco | Baseada na resolução CMN 2.682       |
| `classificacao_risco`  | VARCHAR(2)    | Classificação de risco (resolução CMN 2.682)       | AA, A, B, C, D, E, F, G, H          |

---

## Views derivadas principais

| View                             | Camada | Descrição                                              |
|----------------------------------|--------|--------------------------------------------------------|
| `vw_stage_exposicao_recente`     | STAGE  | Último snapshot de exposição por cliente               |
| `vw_stage_rating_recente`        | STAGE  | Último rating + nota numérica por cliente              |
| `vw_stage_limite_consolidado`    | STAGE  | Limite total consolidado com % de utilização           |
| `vw_stage_cliente_enriquecido`   | STAGE  | Visão 360° unindo as 5 tabelas                         |
| `vw_kpi_por_subsetor`            | DW     | KPIs de concentração e risco por subsetor              |
| `vw_matriz_risco`                | DW     | Classificação individual de risco com flags combinados |
| `vw_evolucao_rating_segmento`    | DW     | Série histórica mensal de rating por segmento          |
| `vw_kpi_exposicao`               | DW     | KPI consolidado de exposição do portfólio              |
| `vw_kpi_ratings`                 | DW     | Score ponderado e PD médio da carteira                 |
| `vw_kpi_limites`                 | DW     | Utilização agregada de limites                         |
| `vw_kpi_risco`                   | DW     | Distribuição da carteira por classificação de risco    |
