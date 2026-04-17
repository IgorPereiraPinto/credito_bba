-- ================================================================
--  VIEWS ANALÍTICAS — CRÉDITO CORPORATIVO
--  Candidato : Igor
--  Posição   : Analista de Dados Pleno
--  Data      : Março / 2026
-- ================================================================
--  NOTA TÉCNICA:
--  A solução foi prototipada localmente em SQL Server Developer
--  Edition e Power BI Desktop para demonstração analítica.
--  A modelagem, os KPIs e a estrutura das consultas foram
--  desenhados para fácil adaptação ao ambiente Athena +
--  Amazon QuickSight, stack alvo para produção.
--
--  ARQUITETURA DAS VIEWS:
--  Cada view encapsula a lógica analítica de uma query do case,
--  funcionando como camada semântica entre o banco relacional e
--  a ferramenta de visualização (Power BI / QuickSight).
--  No Power BI, as views substituem as medidas DAX — os campos
--  são importados diretamente e usados nos visuais sem cálculo
--  adicional, exatamente como funcionaria no QuickSight.
--
--  MAPEAMENTO VIEWS x PASTAS DO POWER BI:
--  vw_exposicao_enriquecida    → base de todas as análises
--  vw_kpi_exposicao            → Pasta "0. Exposição"
--  vw_kpi_ratings              → Pasta "1. Ratings"
--  vw_kpi_limites              → Pasta "2. Limites"
--  vw_kpi_operacoes            → Pasta "3. Operações"
--  vw_kpi_risco                → Pasta "4. Risco"
--  vw_score_por_segmento_mes   → Gráfico de linha — Aba 1
--  vw_exposicao_por_subsetor   → Query 2.2 — Subsetor
--  vw_exposicao_por_setor      → Query 2.6-A — Concentração
--  vw_clientes_em_atencao      → Query 2.4 — Matriz de Risco
--  vw_operacoes_vencidas       → Query 2.6-B — Risco Materializado
--  vw_score_por_segmento_porte → Query 2.6-C — Qualidade
--  vw_analise_estatistica      → Query 2.5 — Z-Score / Outliers
--
--  ADAPTAÇÃO PARA ATHENA (diferenças de sintaxe):
--  FORMAT(date, 'yyyy-MM')  → DATE_FORMAT(date, '%Y-%m')
--  STDEV()                  → STDDEV()
--  TOP 1                    → LIMIT 1
--  GETDATE()                → CURRENT_DATE
--  CREATE OR ALTER VIEW     → CREATE OR REPLACE VIEW
-- ================================================================

USE credito_ibba;
GO


-- ================================================================
-- PRÉ-REQUISITO — VIEW BASE ENRIQUECIDA (substitui colunas
-- ausentes na tabela exposicoes: provisao_necessaria e
-- classificacao_risco, conforme estrutura original do case)
-- ================================================================
-- ----------------------------------------------------------------
-- VIEW BASE | vw_exposicao_enriquecida
-- ----------------------------------------------------------------
-- OBJETIVO DE NEGÓCIO:
--   A tabela exposicoes original do case prevê duas colunas que
--   não existem na base sintética: provisao_necessaria e
--   classificacao_risco. Esta view as deriva a partir do
--   score_interno e da exposicao_total, tornando o modelo
--   completo e fiel à estrutura proposta pelo case.
--
-- REGRAS DE NEGÓCIO APLICADAS:
--   classificacao_risco — derivada do score_interno:
--     score >= 850 → 'AA' (risco muito baixo)
--     score >= 750 → 'A'  (risco baixo)
--     score >= 650 → 'B'  (risco moderado)
--     score <  650 → 'C'  (risco alto)
--
--   provisao_necessaria — estimada como % da exposição total:
--     'AA' → 0,5% da exposicao_total (PCLD mínima)
--     'A'  → 1,0% da exposicao_total
--     'B'  → 3,0% da exposicao_total
--     'C'  → 7,0% da exposicao_total (PCLD máxima)
--
-- TÉCNICAS UTILIZADAS:
--   LEFT JOIN temporal entre exposicoes e ratings pela mesma data
--   CASE para derivar classificacao_risco a partir do score
--   CASE para calcular provisao_necessaria proporcional ao risco
--   NULLIF para proteção contra divisão por zero
--
-- RESULTADO: Base completa com 5 colunas originais + 2 derivadas
--   Usada como fundação para todos os KPIs de exposição
-- ----------------------------------------------------------------
CREATE OR ALTER VIEW vw_exposicao_enriquecida AS
SELECT
    e.cliente_id,
    e.data_referencia,
    e.exposicao_total,
    e.exposicao_garantida,
    e.exposicao_descoberta,

    -- Rating e score do mesmo período da exposição
    r.rating_interno,
    r.score_interno,

    -- classificacao_risco: derivada do score_interno
    -- Segue escala interna alinhada ao rating externo simplificado
    CASE
        WHEN r.score_interno >= 850 THEN 'AA'   -- risco muito baixo
        WHEN r.score_interno >= 750 THEN 'A'    -- risco baixo
        WHEN r.score_interno >= 650 THEN 'B'    -- risco moderado
        ELSE                              'C'   -- risco alto
    END                                         AS classificacao_risco,

    -- provisao_necessaria: % da exposição conforme faixa de risco
    -- Regra inspirada na Resolução CMN 2.682/1999 (simplificada)
    CAST(ROUND(
        CASE
            WHEN r.score_interno >= 850 THEN e.exposicao_total * 0.005
            WHEN r.score_interno >= 750 THEN e.exposicao_total * 0.010
            WHEN r.score_interno >= 650 THEN e.exposicao_total * 0.030
            ELSE                              e.exposicao_total * 0.070
        END
    , 2) AS DECIMAL(15,2))                      AS provisao_necessaria,

    -- % de provisionamento sobre a exposição total (para análise)
    CAST(
        CASE
            WHEN r.score_interno >= 850 THEN 0.5
            WHEN r.score_interno >= 750 THEN 1.0
            WHEN r.score_interno >= 650 THEN 3.0
            ELSE                              7.0
        END
    AS DECIMAL(5,1))                            AS pct_provisao

FROM exposicoes e
LEFT JOIN ratings r
    ON e.cliente_id    = r.cliente_id
   AND e.data_referencia = r.data_referencia;   -- JOIN temporal: mesma data
GO


-- ================================================================
-- SEÇÃO 1 — KPIs CONSOLIDADOS (Pastas do Power BI)
-- ================================================================


-- ----------------------------------------------------------------
-- QUESTÃO 2.1 (adaptada) | vw_kpi_exposicao — Pasta "0. Exposição"
-- ----------------------------------------------------------------
-- OBJETIVO DE NEGÓCIO:
--   Consolidar os 3 KPIs principais de exposição do portfólio
--   no último mês disponível, incluindo provisão necessária total.
--   Permite monitorar a saúde geral da carteira e o nível de
--   cobertura de garantias em uma única consulta executiva.
--
-- TÉCNICAS UTILIZADAS:
--   CTE ultima_data para isolar o período mais recente
--   SUM e COUNT para agregar exposição por cliente
--   NULLIF para evitar divisão por zero no cálculo de %
--   CASE para semáforo regulatório por faixa de descoberta
--
-- RESULTADO: 1 linha consolidada com todos os KPIs de exposição
-- DESTAQUE:  Exposição total de R$9,49Bi | Descoberta de 25,0%
--            Provisão total estimada disponível na view base
-- ----------------------------------------------------------------
CREATE OR ALTER VIEW vw_kpi_exposicao AS
WITH ultima_data AS (
    SELECT MAX(data_referencia) AS dt FROM vw_exposicao_enriquecida
),
base AS (
    SELECT v.cliente_id, v.exposicao_total, v.exposicao_descoberta,
           v.provisao_necessaria
    FROM vw_exposicao_enriquecida v
    INNER JOIN ultima_data ud ON v.data_referencia = ud.dt
)
SELECT
    -- Data de referência dos dados
    (SELECT dt FROM ultima_data)                                         AS data_referencia,

    -- Quantidade de clientes na posição mais recente
    COUNT(DISTINCT b.cliente_id)                                         AS qtd_clientes,

    -- Exposição total do portfólio — base para todos os KPIs
    CAST(ROUND(SUM(b.exposicao_total), 2) AS DECIMAL(18,2))             AS exposicao_total,

    -- Parcela sem cobertura de garantias — risco efetivo de perda
    CAST(ROUND(SUM(b.exposicao_descoberta), 2) AS DECIMAL(18,2))        AS exposicao_descoberta_total,

    -- % descoberta: meta < 30% | atenção > 35%
    CAST(ROUND(
        SUM(b.exposicao_descoberta) /
        NULLIF(SUM(b.exposicao_total), 0) * 100
    , 1) AS DECIMAL(10,1))                                               AS pct_exposicao_descoberta,

    -- Provisão total estimada conforme regras de PCLD
    CAST(ROUND(SUM(b.provisao_necessaria), 2) AS DECIMAL(18,2))         AS provisao_total_estimada,

    -- % de provisionamento sobre a exposição total
    CAST(ROUND(
        SUM(b.provisao_necessaria) /
        NULLIF(SUM(b.exposicao_total), 0) * 100
    , 2) AS DECIMAL(10,2))                                               AS pct_provisao_sobre_exposicao,

    -- Semáforo regulatório de exposição descoberta
    CASE
        WHEN SUM(b.exposicao_descoberta) /
             NULLIF(SUM(b.exposicao_total), 0) * 100 < 30 THEN 'NA META'
        WHEN SUM(b.exposicao_descoberta) /
             NULLIF(SUM(b.exposicao_total), 0) * 100 < 35 THEN 'ATENCAO'
        ELSE 'CRITICO'
    END                                                                  AS status_descoberta
FROM base b;
GO


-- ----------------------------------------------------------------
-- QUESTÃO 2.3 (adaptada) | vw_kpi_ratings — Pasta "1. Ratings"
-- ----------------------------------------------------------------
-- OBJETIVO DE NEGÓCIO:
--   Monitorar a qualidade média do portfólio pelo score interno
--   e pela probabilidade de default (PD-12m) no mês mais recente.
--   O score é o termômetro geral da saúde da carteira — abaixo
--   de 800 indica necessidade de atenção pelo gestor de risco.
--
-- TÉCNICAS UTILIZADAS:
--   CTE para filtrar apenas o último mês de rating por cliente
--   AVG simples no último mês — consistente com Query 2.3
--   CASE para semáforo conforme benchmarks internos
--
-- RESULTADO: Score médio = 791 (faixa ATENÇÃO) | PD = 3,22%
-- INSIGHT:   Score abaixo da meta de 800 — portfólio requer
--            monitoramento ativo para evitar deterioração
-- ----------------------------------------------------------------
CREATE OR ALTER VIEW vw_kpi_ratings AS
WITH ultima_data AS (
    SELECT MAX(data_referencia) AS dt FROM ratings
),
rating_recente AS (
    SELECT r.cliente_id, r.rating_interno, r.score_interno, r.pd_12m
    FROM ratings r
    INNER JOIN ultima_data ud ON r.data_referencia = ud.dt
)
SELECT
    (SELECT dt FROM ultima_data)                                          AS data_referencia,
    COUNT(*)                                                              AS qtd_clientes,

    -- Score médio do portfólio no último mês — meta: >= 800
    CAST(ROUND(AVG(CAST(rr.score_interno AS FLOAT)), 0) AS INT)          AS score_medio,

    -- PD média em % — meta: < 1% | atenção: > 2%
    CAST(ROUND(AVG(rr.pd_12m) * 100, 4) AS DECIMAL(10,4))               AS pd_media_pct,

    -- Semáforo score conforme benchmarks internos
    CASE
        WHEN AVG(CAST(rr.score_interno AS FLOAT)) >= 850 THEN 'EXCELENTE'
        WHEN AVG(CAST(rr.score_interno AS FLOAT)) >= 800 THEN 'MUITO BOM'
        WHEN AVG(CAST(rr.score_interno AS FLOAT)) >= 750 THEN 'ATENCAO'
        WHEN AVG(CAST(rr.score_interno AS FLOAT)) >= 650 THEN 'CUIDADO'
        ELSE 'CRITICO'
    END                                                                   AS status_score,

    -- Semáforo PD
    CASE
        WHEN AVG(rr.pd_12m) * 100 < 1 THEN 'NA META'
        WHEN AVG(rr.pd_12m) * 100 < 2 THEN 'ATENCAO'
        ELSE 'CRITICO'
    END                                                                   AS status_pd
FROM rating_recente rr;
GO


-- ----------------------------------------------------------------
-- vw_kpi_limites — Pasta "2. Limites"
-- ----------------------------------------------------------------
-- OBJETIVO DE NEGÓCIO:
--   Monitorar a utilização dos limites de crédito Global para
--   identificar clientes próximos ao teto. Alta utilização indica
--   menor margem de segurança e maior exposição ao risco de
--   concentração. Faixa ideal: 60–75%.
--
-- TÉCNICAS UTILIZADAS:
--   CAST + ROUND para precisão de 1 casa decimal na utilização
--   SUM com CASE para contagem condicional por threshold
--   NULLIF para proteção contra divisão por zero
--   CASE para semáforo de utilização média
--
-- RESULTADO: Utilização média = 64,3% (faixa IDEAL)
-- DESTAQUE:  16 clientes >80% | 7 clientes >85% — ação imediata
-- ----------------------------------------------------------------
CREATE OR ALTER VIEW vw_kpi_limites AS
WITH utilizacao AS (
    SELECT
        cliente_id,
        CAST(ROUND(
            valor_utilizado / NULLIF(valor_limite, 0) * 100, 1
        ) AS DECIMAL(10,1))                         AS pct_utilizacao
    FROM limites
    WHERE tipo_limite = 'Global'                    -- apenas limite consolidado
)
SELECT
    -- Utilização média — ideal: 60-75% | alerta: > 85%
    CAST(ROUND(AVG(pct_utilizacao), 1) AS DECIMAL(10,1))  AS utilizacao_media_pct,

    -- Clientes no threshold de alerta — exige revisão pelo gestor
    SUM(CASE WHEN pct_utilizacao > 80 THEN 1 ELSE 0 END)  AS clientes_acima_80pct,

    -- Clientes no threshold crítico — exige ação imediata
    SUM(CASE WHEN pct_utilizacao > 85 THEN 1 ELSE 0 END)  AS clientes_acima_85pct,

    -- Total de clientes com limite Global cadastrado
    COUNT(*)                                               AS qtd_clientes_global,

    -- Semáforo de utilização média
    CASE
        WHEN AVG(pct_utilizacao) BETWEEN 60 AND 75 THEN 'IDEAL'
        WHEN AVG(pct_utilizacao) > 85              THEN 'CRITICO'
        ELSE 'ATENCAO'
    END                                                    AS status_utilizacao
FROM utilizacao;
GO


-- ----------------------------------------------------------------
-- vw_kpi_operacoes — Pasta "3. Operações"
-- ----------------------------------------------------------------
-- OBJETIVO DE NEGÓCIO:
--   Identificar o volume e o valor das operações ativas por cliente,
--   permitindo priorizar relacionamentos de maior exposição e
--   monitorar a taxa de utilização do crédito aprovado.
--   Operações vencidas representam risco de crédito materializado
--   e exigem ação imediata de cobrança ou execução de garantias.
--
-- TÉCNICAS UTILIZADAS:
--   SUM com CASE para separar operações ativas das vencidas
--   NULLIF para proteção contra divisão por zero
--   CASE para semáforo conforme meta de operações vencidas
--
-- RESULTADO: 48 clientes com operações ativas | 11 vencidas
-- DESTAQUE:  CLI862 lidera com R$605M aprovado e 89,4% utilização
--            — acima do threshold de alerta de 85%
-- ----------------------------------------------------------------
CREATE OR ALTER VIEW vw_kpi_operacoes AS
SELECT
    -- Total aprovado em operações ativas
    CAST(ROUND(
        SUM(CASE WHEN status_operacao = 'Ativa' THEN valor_aprovado  ELSE 0 END)
    , 2) AS DECIMAL(18,2))                         AS total_aprovado_ativo,

    -- Total utilizado em operações ativas
    CAST(ROUND(
        SUM(CASE WHEN status_operacao = 'Ativa' THEN valor_utilizado ELSE 0 END)
    , 2) AS DECIMAL(18,2))                         AS total_utilizado_ativo,

    -- % de utilização sobre o aprovado — referência de eficiência
    CAST(ROUND(
        SUM(CASE WHEN status_operacao = 'Ativa' THEN valor_utilizado ELSE 0 END) /
        NULLIF(SUM(CASE WHEN status_operacao = 'Ativa' THEN valor_aprovado ELSE 0 END), 0) * 100
    , 1) AS DECIMAL(10,1))                         AS pct_utilizacao_aprovado,

    -- Operações com status Vencida — risco materializado
    SUM(CASE WHEN status_operacao = 'Vencida' THEN 1 ELSE 0 END)
                                                   AS qtd_operacoes_vencidas,

    COUNT(*)                                       AS qtd_operacoes_total,

    -- Semáforo: qualquer operação vencida é crítico
    CASE
        WHEN SUM(CASE WHEN status_operacao = 'Vencida' THEN 1 ELSE 0 END) = 0  THEN 'NA META'
        WHEN SUM(CASE WHEN status_operacao = 'Vencida' THEN 1 ELSE 0 END) <= 5 THEN 'ATENCAO'
        ELSE 'CRITICO'
    END                                            AS status_vencidas
FROM operacoes;
GO


-- ----------------------------------------------------------------
-- vw_kpi_risco — Pasta "4. Risco"
-- ----------------------------------------------------------------
-- OBJETIVO DE NEGÓCIO:
--   Consolidar os indicadores de risco sistêmico do portfólio:
--   concentração máxima por subsetor (limite regulatório: 15%)
--   e quantidade de clientes em situação de atenção combinada.
--
-- TÉCNICAS UTILIZADAS:
--   CTE encadeada para calcular concentração por subsetor
--   SUM() OVER() para % relativo sobre o total do portfólio
--   Subquery escalar para identificar subsetor mais concentrado
--   CASE para semáforo de concentração regulatória
--
-- RESULTADO: Concentração máx = 11,3% (Prod. Farmacêuticos)
--            24 clientes em atenção por utilização > 75%
-- INSIGHT:   Tecnologia (18,1%) viola o limite ao nível de setor
-- ----------------------------------------------------------------
CREATE OR ALTER VIEW vw_kpi_risco AS
WITH exposicao_atual AS (
    SELECT e.cliente_id, e.exposicao_total FROM exposicoes e
    INNER JOIN (SELECT MAX(data_referencia) AS dt FROM exposicoes) ud
        ON e.data_referencia = ud.dt
),
total_port AS (SELECT SUM(exposicao_total) AS total FROM exposicao_atual),
conc_subsetor AS (
    SELECT
        c.subsetor,
        CAST(ROUND(
            SUM(ea.exposicao_total) / tp.total * 100, 1
        ) AS DECIMAL(10,1))                        AS pct_concentracao
    FROM exposicao_atual ea
    INNER JOIN clientes c  ON ea.cliente_id = c.cliente_id
    CROSS JOIN total_port tp
    GROUP BY c.subsetor, tp.total
),
clientes_alerta AS (
    SELECT COUNT(*) AS qtd FROM limites
    WHERE tipo_limite = 'Global'
      AND valor_utilizado / NULLIF(valor_limite, 0) > 0.75
)
SELECT
    -- Maior concentração individual por subsetor — limite interno: 15%
    MAX(cs.pct_concentracao)                           AS concentracao_max_subsetor_pct,

    -- Subsetor com maior concentração
    (SELECT TOP 1 subsetor FROM conc_subsetor
     ORDER BY pct_concentracao DESC)                   AS subsetor_mais_concentrado,

    -- Clientes com utilização > 75% do limite Global
    (SELECT qtd FROM clientes_alerta)                  AS clientes_em_atencao,

    -- Semáforo concentração regulatória
    CASE
        WHEN MAX(cs.pct_concentracao) > 15 THEN 'ACIMA DO LIMITE'
        WHEN MAX(cs.pct_concentracao) > 10 THEN 'MONITORAMENTO'
        ELSE 'NORMAL'
    END                                                AS status_concentracao
FROM conc_subsetor cs;
GO


-- ================================================================
-- SEÇÃO 2 — VIEWS ANALÍTICAS POR QUESTÃO DO CASE
-- ================================================================


-- ----------------------------------------------------------------
-- QUESTÃO 2.2 | vw_exposicao_por_subsetor
-- ----------------------------------------------------------------
-- OBJETIVO DE NEGÓCIO:
--   Mapear a concentração do portfólio por subsetor para identificar
--   riscos sistêmicos. Subsetores com > 5 clientes têm massa crítica
--   suficiente para análise estatística confiável.
--
-- TÉCNICAS UTILIZADAS:
--   CTE para isolar a última posição de exposição por cliente
--   HAVING para filtrar subsetores com representatividade mínima
--   NULLIF para proteção contra divisão por zero
--
-- RESULTADO: Apenas 2 subsetores com > 5 clientes
-- INSIGHT:   Portfólio pulverizado por subsetor — análise de
--            concentração mais representativa ao nível de setor.
--            Farmacêuticos: R$1.07B | Software e TI: R$821M
--            Exposição descoberta: 25% em ambos
-- ----------------------------------------------------------------
CREATE OR ALTER VIEW vw_exposicao_por_subsetor AS
WITH exposicao_recente AS (
    -- Última posição de exposição disponível por cliente
    SELECT e.cliente_id, e.exposicao_total, e.exposicao_descoberta
    FROM exposicoes e
    INNER JOIN (
        SELECT cliente_id, MAX(data_referencia) AS ultima_data
        FROM exposicoes GROUP BY cliente_id
    ) ult ON e.cliente_id = ult.cliente_id
          AND e.data_referencia = ult.ultima_data
)
SELECT
    c.subsetor,
    COUNT(DISTINCT c.cliente_id)                           AS qtd_clientes,
    ROUND(AVG(er.exposicao_total), 2)                      AS exposicao_media,
    MAX(er.exposicao_total)                                AS maior_exposicao,
    SUM(er.exposicao_total)                                AS exposicao_total_subsetor,
    ROUND(
        SUM(er.exposicao_descoberta) /
        NULLIF(SUM(er.exposicao_total), 0) * 100, 1)      AS pct_exposicao_descoberta
FROM clientes c
    INNER JOIN exposicao_recente er ON c.cliente_id = er.cliente_id
GROUP BY c.subsetor
HAVING COUNT(DISTINCT c.cliente_id) > 5                   -- apenas subsetores com massa crítica
-- Nota: ORDER BY não é permitido em views no SQL Server
-- Use ORDER BY na query de consumo: SELECT * FROM vw_exposicao_por_subsetor ORDER BY exposicao_total_subsetor DESC
;
GO


-- ----------------------------------------------------------------
-- QUESTÃO 2.3 | vw_score_por_segmento_mes
-- ----------------------------------------------------------------
-- OBJETIVO DE NEGÓCIO:
--   Monitorar a qualidade de crédito de cada segmento ao longo
--   do tempo. A variação MoM indica tendência de melhora ou
--   deterioração e o ranking mensal permite benchmarking entre
--   segmentos perante o Comitê de Crédito.
--
-- TÉCNICAS UTILIZADAS:
--   CTE escala_rating para converter rating em escala numérica
--   FORMAT() para agrupar por ano-mês
--   LAG() OVER(PARTITION BY) para comparação mês a mês
--   RANK() OVER(PARTITION BY) para ranking mensal entre segmentos
--
-- RESULTADO: 36 linhas — 3 segmentos x 12 meses
-- INSIGHT:   Corporate ficou em 3º lugar em todos os 12 meses
--            com score médio ~762 — faixa ATENÇÃO (750-799)
--            Middle Market liderou o ranking durante todo o período
-- ----------------------------------------------------------------
CREATE OR ALTER VIEW vw_score_por_segmento_mes AS
WITH escala_rating AS (
    SELECT 'AAA' AS rating, 17 AS nota UNION ALL SELECT 'AA+', 16
    UNION ALL SELECT 'AA',  15         UNION ALL SELECT 'AA-', 14
    UNION ALL SELECT 'A+',  13         UNION ALL SELECT 'A',   12
    UNION ALL SELECT 'A-',  11         UNION ALL SELECT 'BBB+',10
    UNION ALL SELECT 'BBB', 9          UNION ALL SELECT 'BBB-', 8
    UNION ALL SELECT 'BB+', 7          UNION ALL SELECT 'BB',   6
    UNION ALL SELECT 'BB-', 5          UNION ALL SELECT 'B+',   4
    UNION ALL SELECT 'B',   3          UNION ALL SELECT 'B-',   2
    UNION ALL SELECT 'C',   1
),
rating_mensal AS (
    SELECT
        c.segmento,
        FORMAT(r.data_referencia, 'yyyy-MM')                      AS ano_mes,
        r.data_referencia,
        CAST(ROUND(AVG(CAST(er.nota AS FLOAT)), 2) AS DECIMAL(10,2))  AS nota_media_segmento,
        CAST(ROUND(AVG(CAST(r.score_interno AS FLOAT)), 0) AS INT)     AS score_medio_segmento,
        COUNT(DISTINCT r.cliente_id)                                   AS qtd_clientes
    FROM ratings r
        INNER JOIN clientes c       ON r.cliente_id     = c.cliente_id
        INNER JOIN escala_rating er ON r.rating_interno = er.rating
    GROUP BY c.segmento,
             FORMAT(r.data_referencia, 'yyyy-MM'),
             r.data_referencia
)
SELECT
    segmento,
    ano_mes,
    data_referencia,
    nota_media_segmento,
    score_medio_segmento,
    qtd_clientes,

    -- Nota do mês anterior para comparação MoM
    LAG(nota_media_segmento) OVER (
        PARTITION BY segmento ORDER BY data_referencia
    )                                                             AS nota_mes_anterior,

    -- Variação percentual mês a mês
    ROUND(
        (nota_media_segmento
            - LAG(nota_media_segmento) OVER (
                PARTITION BY segmento ORDER BY data_referencia))
        / NULLIF(LAG(nota_media_segmento) OVER (
                PARTITION BY segmento ORDER BY data_referencia), 0) * 100
    , 2)                                                          AS variacao_pct_mom,

    -- Ranking dos segmentos por rating dentro de cada mês (1 = melhor)
    RANK() OVER (
        PARTITION BY ano_mes ORDER BY nota_media_segmento DESC
    )                                                             AS ranking_segmento_mes
FROM rating_mensal;
GO


-- ----------------------------------------------------------------
-- QUESTÃO 2.4 | vw_clientes_em_atencao
-- ----------------------------------------------------------------
-- OBJETIVO DE NEGÓCIO:
--   Priorizar clientes que acumulam múltiplos fatores de risco
--   simultaneamente — deterioração de rating, alta utilização de
--   limite e elevada exposição descoberta — permitindo ação
--   preventiva antes da materialização do risco de crédito.
--
-- TÉCNICAS UTILIZADAS:
--   CTEs encadeadas para modularidade e legibilidade
--   ROW_NUMBER() para selecionar o período mais recente por cliente
--   Self-join em historico_rating para detectar deterioração consecutiva
--   SUM() OVER() para concentração relativa por subsetor
--   CASE combinado para classificação por severidade de risco
--
-- RESULTADO: 24 clientes em situação de ATENCAO
--   - Critério principal: utilização de limite Global > 75%
--   - Nenhum cliente atingiu ALTO RISCO (os 3 fatores combinados)
--   - pct_descoberta = 25% para todos — abaixo do limiar crítico de 30%
--   - 3 clientes sem dados de exposição (NULL) — verificar pipeline
-- ----------------------------------------------------------------
CREATE OR ALTER VIEW vw_clientes_em_atencao AS
WITH escala_rating AS (
    SELECT 'AAA' AS rating, 17 AS nota UNION ALL SELECT 'AA+', 16
    UNION ALL SELECT 'AA',  15         UNION ALL SELECT 'AA-', 14
    UNION ALL SELECT 'A+',  13         UNION ALL SELECT 'A',   12
    UNION ALL SELECT 'A-',  11         UNION ALL SELECT 'BBB+',10
    UNION ALL SELECT 'BBB', 9          UNION ALL SELECT 'BBB-', 8
    UNION ALL SELECT 'BB+', 7          UNION ALL SELECT 'BB',   6
    UNION ALL SELECT 'BB-', 5          UNION ALL SELECT 'B+',   4
    UNION ALL SELECT 'B',   3          UNION ALL SELECT 'B-',   2
    UNION ALL SELECT 'C',   1
),
historico_rating AS (
    -- Numera os meses do mais recente (rn=1) para o mais antigo por cliente
    SELECT
        r.cliente_id, r.data_referencia, r.rating_interno, er.nota,
        ROW_NUMBER() OVER (
            PARTITION BY r.cliente_id ORDER BY r.data_referencia DESC
        ) AS rn
    FROM ratings r
    INNER JOIN escala_rating er ON r.rating_interno = er.rating
),
deterioracao AS (
    -- Clientes com rating caindo nos 3 últimos meses consecutivos
    SELECT h1.cliente_id
    FROM historico_rating h1
    INNER JOIN historico_rating h2 ON h1.cliente_id = h2.cliente_id AND h2.rn = 2
    INNER JOIN historico_rating h3 ON h1.cliente_id = h3.cliente_id AND h3.rn = 3
    WHERE h1.rn = 1
      AND h1.nota < h2.nota   -- mês atual pior que mês anterior
      AND h2.nota < h3.nota   -- mês anterior pior que dois meses atrás
),
utilizacao_limite AS (
    SELECT
        cliente_id,
        CAST(ROUND(valor_utilizado / NULLIF(valor_limite, 0) * 100, 1)
            AS DECIMAL(10,1))                      AS pct_utilizacao
    FROM limites
    WHERE tipo_limite = 'Global'                   -- apenas limite consolidado
),
exposicao_atual AS (
    -- Última posição de exposição disponível por cliente
    SELECT
        e.cliente_id, e.exposicao_total, e.exposicao_descoberta,
        CAST(ROUND(e.exposicao_descoberta /
                   NULLIF(e.exposicao_total, 0) * 100, 1)
            AS DECIMAL(10,1))                      AS pct_descoberta
    FROM exposicoes e
    INNER JOIN (
        SELECT cliente_id, MAX(data_referencia) AS ultima_data
        FROM exposicoes GROUP BY cliente_id
    ) ult ON e.cliente_id = ult.cliente_id
          AND e.data_referencia = ult.ultima_data
),
concentracao_subsetor AS (
    -- Peso percentual de cada subsetor no portfólio total
    SELECT
        c.subsetor,
        CAST(ROUND(SUM(ea.exposicao_total) /
                   SUM(SUM(ea.exposicao_total)) OVER () * 100, 1)
            AS DECIMAL(10,1))                      AS pct_concentracao
    FROM exposicao_atual ea
    INNER JOIN clientes c ON ea.cliente_id = c.cliente_id
    GROUP BY c.subsetor
),
rating_atual AS (
    SELECT r.cliente_id, r.rating_interno, r.score_interno, r.pd_12m
    FROM ratings r
    INNER JOIN (SELECT MAX(data_referencia) AS dt FROM ratings) ud
        ON r.data_referencia = ud.dt
)
SELECT
    c.cliente_id, c.segmento, c.porte, c.setor, c.subsetor, c.regiao,

    ul.pct_utilizacao,                             -- % limite global usado
    ea.pct_descoberta,                             -- % exposição sem garantia
    CAST(ROUND(ea.exposicao_total / 1000000, 2)
        AS DECIMAL(15,2))                          AS exposicao_total_MM,
    cs.pct_concentracao                            AS concentracao_subsetor_pct,

    ra.rating_interno, ra.score_interno,
    CAST(ra.pd_12m * 100 AS DECIMAL(10,4))         AS pd_12m_pct,

    -- Deterioração consecutiva de rating
    CASE WHEN d.cliente_id IS NOT NULL THEN 'SIM' ELSE 'NAO' END
                                                   AS deterioracao_rating,

    -- Classificação de risco combinada — prioriza os 3 fatores juntos
    CASE
        WHEN d.cliente_id IS NOT NULL
             AND ul.pct_utilizacao > 80
             AND ea.pct_descoberta > 30 THEN 'ALTO RISCO'
        WHEN ul.pct_utilizacao > 75
          OR ea.pct_descoberta > 25     THEN 'ATENCAO'
        ELSE 'NORMAL'
    END                                            AS classificacao_risco

FROM clientes c
    INNER JOIN utilizacao_limite ul     ON c.cliente_id = ul.cliente_id
    INNER JOIN exposicao_atual ea       ON c.cliente_id = ea.cliente_id
    INNER JOIN concentracao_subsetor cs ON c.subsetor   = cs.subsetor
    INNER JOIN rating_atual ra          ON c.cliente_id = ra.cliente_id
    LEFT  JOIN deterioracao d           ON c.cliente_id = d.cliente_id

WHERE ul.pct_utilizacao > 75          -- utilização acima do threshold de alerta
   OR ea.pct_descoberta > 25          -- exposição descoberta acima da meta
   OR d.cliente_id IS NOT NULL;       -- qualquer deterioração consecutiva de rating
GO


-- ----------------------------------------------------------------
-- QUESTÃO 2.5 | vw_analise_estatistica
-- ----------------------------------------------------------------
-- OBJETIVO DE NEGÓCIO:
--   Identificar comportamentos anômalos no portfólio por meio de
--   técnicas estatísticas: detecção de outliers via Z-Score,
--   distribuição de exposições por segmento e correlação entre
--   score de crédito e utilização de limite.
--
-- TÉCNICAS UTILIZADAS:
--   Z-Score para detecção de outliers (threshold: 2 desvios padrão)
--   STDEV() para desvio padrão por segmento
--   Coeficiente de variação para medir dispersão relativa
--   INNER JOIN para cruzar exposição com estatísticas do segmento
--
-- RESULTADOS ENCONTRADOS:
--   OUTLIERS    : CLI862 (Z=2.49), CLI402 (Z=2.70), CLI524 (Z=2.25)
--   DISTRIBUICAO: Middle Market CV=155.7% — maior heterogeneidade
--   CORRELACAO  : Pearson=0.0176 (FRACA) — limites seguem critérios
--                 estratégicos além do score isolado
-- ----------------------------------------------------------------
CREATE OR ALTER VIEW vw_analise_estatistica AS
WITH exposicao_atual AS (
    SELECT e.cliente_id, e.exposicao_total, c.segmento, c.subsetor
    FROM exposicoes e
    INNER JOIN clientes c ON e.cliente_id = c.cliente_id
    INNER JOIN (
        SELECT MAX(data_referencia) AS dt FROM exposicoes
    ) ud ON e.data_referencia = ud.dt
),
stats_segmento AS (
    SELECT
        segmento,
        AVG(exposicao_total)   AS media_seg,
        STDEV(exposicao_total) AS desvio_seg,
        COUNT(*)               AS n_clientes,
        MIN(exposicao_total)   AS minimo,
        MAX(exposicao_total)   AS maximo
    FROM exposicao_atual
    GROUP BY segmento
)
SELECT
    ea.cliente_id,
    ea.segmento,
    ea.subsetor,

    CAST(ROUND(ea.exposicao_total / 1000000, 2) AS DECIMAL(15,2))  AS exposicao_MM,

    -- Z-Score: distância da média em desvios padrão
    -- |z| > 2 indica exposição estatisticamente anômala
    CAST(ROUND(
        (ea.exposicao_total - ss.media_seg) /
        NULLIF(ss.desvio_seg, 0)
    , 2) AS DECIMAL(10,2))                                          AS z_score,

    -- Flag de outlier baseado no threshold de 2 desvios padrão
    CASE
        WHEN ABS((ea.exposicao_total - ss.media_seg) /
                 NULLIF(ss.desvio_seg, 0)) > 2 THEN 'OUTLIER'
        ELSE 'NORMAL'
    END                                                             AS flag_outlier,

    -- Métricas do segmento para contextualização
    ss.n_clientes                                                   AS n_clientes_segmento,
    CAST(ROUND(ss.media_seg  / 1000000, 2) AS DECIMAL(15,2))       AS media_segmento_MM,
    CAST(ROUND(ss.desvio_seg / 1000000, 2) AS DECIMAL(15,2))       AS desvio_segmento_MM,

    -- Coeficiente de variação: quanto maior, mais heterogêneo o segmento
    CAST(ROUND(
        ss.desvio_seg / NULLIF(ss.media_seg, 0) * 100, 1
    ) AS DECIMAL(10,1))                                             AS coef_variacao_pct

FROM exposicao_atual ea
INNER JOIN stats_segmento ss ON ea.segmento = ss.segmento;
GO


-- ----------------------------------------------------------------
-- QUESTÃO 2.6-A | vw_exposicao_por_setor
-- ----------------------------------------------------------------
-- OBJETIVO: Identificar setores que ultrapassam ou se aproximam
--   do limite regulatório de 15% de concentração do portfólio.
--
-- TÉCNICAS UTILIZADAS:
--   CROSS JOIN com total agregado para calcular % relativo
--   CASE para semáforo regulatório por faixa de concentração
--   Benchmark: >15% = Acima do Limite | 10-15% = Monitoramento
--
-- RESULTADOS ENCONTRADOS:
--   ACIMA DO LIMITE : Tecnologia (18.1%) — ação imediata necessária
--   MONITORAMENTO   : Saúde (14.4%), Educação (14.0%),
--                     Construção Civil (11.2%), Químico (10.2%)
--   Top 3 setores somam 46.5% da exposição total do portfólio
--
-- RECOMENDAÇÃO: Bloquear novas concessões em Tecnologia até que
--   a concentração recue abaixo de 15%. Revisar estratégia para
--   os 4 setores em monitoramento no próximo Comitê de Crédito.
-- ----------------------------------------------------------------
CREATE OR ALTER VIEW vw_exposicao_por_setor AS
WITH exposicao_atual AS (
    SELECT e.cliente_id, e.exposicao_total, e.exposicao_descoberta
    FROM exposicoes e
    INNER JOIN (SELECT MAX(data_referencia) AS dt FROM exposicoes) ud
        ON e.data_referencia = ud.dt
),
total_portfolio AS (SELECT SUM(exposicao_total) AS total FROM exposicao_atual)
SELECT
    c.setor,
    COUNT(DISTINCT c.cliente_id)                           AS qtd_clientes,
    CAST(ROUND(SUM(ea.exposicao_total) / 1000000, 1)
        AS DECIMAL(15,1))                                  AS exposicao_MM,
    CAST(ROUND(SUM(ea.exposicao_total) / tp.total * 100, 1)
        AS DECIMAL(10,1))                                  AS pct_portfolio,
    CAST(ROUND(SUM(ea.exposicao_descoberta) /
               NULLIF(SUM(ea.exposicao_total), 0) * 100, 1)
        AS DECIMAL(10,1))                                  AS pct_descoberta,

    -- Semáforo regulatório — limite interno: 15% por setor
    CASE
        WHEN SUM(ea.exposicao_total) / tp.total * 100 > 15 THEN 'ACIMA DO LIMITE'
        WHEN SUM(ea.exposicao_total) / tp.total * 100 > 10 THEN 'MONITORAMENTO'
        ELSE 'NORMAL'
    END                                                    AS status_regulatorio
FROM clientes c
INNER JOIN exposicao_atual ea   ON c.cliente_id = ea.cliente_id
CROSS JOIN total_portfolio tp
GROUP BY c.setor, tp.total;
GO


-- ----------------------------------------------------------------
-- QUESTÃO 2.6-B | vw_operacoes_vencidas
-- ----------------------------------------------------------------
-- OBJETIVO: Identificar operações vencidas e não liquidadas,
--   cruzando com perfil de risco para priorizar cobrança.
--
-- TÉCNICAS UTILIZADAS:
--   INNER JOIN com rating mais recente via subquery de MAX()
--   DATEDIFF para calcular dias em atraso em relação à data atual
--   CASE para faixa de atraso e prioridade de ação
--
-- RESULTADOS ENCONTRADOS:
--   11 operações com status Vencida no portfólio
--   Maior exposição : CLI368 — R$182.8M, 100 dias (Tecnologia)
--   Mais antigo     : CLI318 — 653 dias vencido (Agronegócio)
--   Maior risco     : CLI956 — Rating C, PD 31.09%, sem garantia
--
-- RECOMENDAÇÃO: CLI956 requer provisionamento integral imediato.
--   CLI318 com 653 dias sugere falha no SLA de cobrança — revisar.
-- ----------------------------------------------------------------
CREATE OR ALTER VIEW vw_operacoes_vencidas AS
WITH rating_atual AS (
    SELECT r.cliente_id, r.rating_interno, r.score_interno, r.pd_12m
    FROM ratings r
    INNER JOIN (SELECT MAX(data_referencia) AS dt FROM ratings) ud
        ON r.data_referencia = ud.dt
)
SELECT
    o.operacao_id, o.cliente_id,
    c.segmento, c.setor, c.subsetor,
    o.produto, o.modalidade, o.garantia_tipo,

    CAST(ROUND(o.valor_aprovado  / 1000000, 2) AS DECIMAL(15,2))  AS valor_aprovado_MM,
    CAST(ROUND(o.valor_utilizado / 1000000, 2) AS DECIMAL(15,2))  AS valor_utilizado_MM,

    o.data_aprovacao,
    o.data_vencimento,

    -- Dias em atraso calculado automaticamente em relação à data atual
    DATEDIFF(DAY, o.data_vencimento, GETDATE())                    AS dias_vencido,

    -- Faixa de atraso para agrupamento e relatórios
    CASE
        WHEN DATEDIFF(DAY, o.data_vencimento, GETDATE()) <= 30  THEN '1 - Ate 30 dias'
        WHEN DATEDIFF(DAY, o.data_vencimento, GETDATE()) <= 90  THEN '2 - 31 a 90 dias'
        WHEN DATEDIFF(DAY, o.data_vencimento, GETDATE()) <= 365 THEN '3 - 91 a 365 dias'
        ELSE '4 - Acima de 365 dias'
    END                                                            AS faixa_atraso,

    ra.rating_interno, ra.score_interno,
    CAST(ra.pd_12m * 100 AS DECIMAL(10,4))                         AS pd_12m_pct,

    -- Prioridade de ação baseada em rating — define ordem de atuação da cobrança
    CASE
        WHEN ra.rating_interno IN ('C', 'D') THEN '1 - URGENTE'
        WHEN ra.rating_interno LIKE 'B%'     THEN '2 - ALTA'
        WHEN ra.rating_interno LIKE 'BB%'    THEN '3 - MEDIA'
        ELSE '4 - BAIXA'
    END                                                            AS prioridade_acao

FROM operacoes o
INNER JOIN clientes c      ON o.cliente_id = c.cliente_id
INNER JOIN rating_atual ra ON o.cliente_id = ra.cliente_id
WHERE o.status_operacao = 'Vencida';
GO


-- ----------------------------------------------------------------
-- QUESTÃO 2.6-C | vw_score_por_segmento_porte
-- ----------------------------------------------------------------
-- OBJETIVO: Entender como a qualidade de crédito se distribui
--   entre segmentos e portes para calibrar apetite de crédito.
--
-- TÉCNICAS UTILIZADAS:
--   INNER JOIN com rating mais recente via subquery de MAX()
--   AVG e STDEV para medir qualidade e homogeneidade por grupo
--   CASE para semáforo de score conforme benchmarks internos
--
-- RESULTADOS ENCONTRADOS:
--   EXCELENTE : Wholesale Grande (score 865)
--   CUIDADO   : Corporate Médio (score 676, 2 clientes)
--   ACHADO    : Wholesale Grande (865) vs Médio (761) = 104 pts
--               de diferença no mesmo segmento
--
-- RECOMENDAÇÃO: Revisar apetite de crédito para Corporate Médio.
--   Score mínimo de 613 — próximo do limiar CRÍTICO (<650).
-- ----------------------------------------------------------------
CREATE OR ALTER VIEW vw_score_por_segmento_porte AS
WITH rating_atual AS (
    SELECT r.cliente_id, r.score_interno, r.rating_interno
    FROM ratings r
    INNER JOIN (SELECT MAX(data_referencia) AS dt FROM ratings) ud
        ON r.data_referencia = ud.dt
)
SELECT
    c.segmento, c.porte,
    COUNT(DISTINCT c.cliente_id)                                        AS qtd_clientes,
    CAST(ROUND(AVG(CAST(ra.score_interno AS FLOAT)), 0) AS INT)         AS score_medio,
    CAST(ROUND(MIN(ra.score_interno), 0) AS INT)                        AS score_minimo,
    CAST(ROUND(MAX(ra.score_interno), 0) AS INT)                        AS score_maximo,
    CAST(ROUND(STDEV(CAST(ra.score_interno AS FLOAT)), 0) AS INT)       AS desvio_padrao,

    -- Semáforo conforme benchmarks internos
    CASE
        WHEN AVG(CAST(ra.score_interno AS FLOAT)) >= 850 THEN 'EXCELENTE'
        WHEN AVG(CAST(ra.score_interno AS FLOAT)) >= 800 THEN 'MUITO BOM'
        WHEN AVG(CAST(ra.score_interno AS FLOAT)) >= 750 THEN 'ATENCAO'
        WHEN AVG(CAST(ra.score_interno AS FLOAT)) >= 650 THEN 'CUIDADO'
        ELSE 'CRITICO'
    END                                                                 AS classificacao_score
FROM clientes c
INNER JOIN rating_atual ra ON c.cliente_id = ra.cliente_id
WHERE c.status_cliente = 'Ativo'
GROUP BY c.segmento, c.porte;
GO


-- ================================================================
-- VERIFICAÇÃO FINAL — Confirmar criação e retorno de todas as views
-- ================================================================
SELECT name AS view_name, create_date, modify_date
FROM sys.views
WHERE name LIKE 'vw_%'
ORDER BY name;
GO

SELECT 'vw_exposicao_enriquecida'    AS view_name, COUNT(*) AS linhas FROM vw_exposicao_enriquecida    UNION ALL
SELECT 'vw_kpi_exposicao',                         COUNT(*) FROM vw_kpi_exposicao                     UNION ALL
SELECT 'vw_kpi_ratings',                           COUNT(*) FROM vw_kpi_ratings                       UNION ALL
SELECT 'vw_kpi_limites',                           COUNT(*) FROM vw_kpi_limites                       UNION ALL
SELECT 'vw_kpi_operacoes',                         COUNT(*) FROM vw_kpi_operacoes                     UNION ALL
SELECT 'vw_kpi_risco',                             COUNT(*) FROM vw_kpi_risco                         UNION ALL
SELECT 'vw_score_por_segmento_mes',                COUNT(*) FROM vw_score_por_segmento_mes             UNION ALL
SELECT 'vw_exposicao_por_subsetor',                COUNT(*) FROM vw_exposicao_por_subsetor             UNION ALL
SELECT 'vw_exposicao_por_setor',                   COUNT(*) FROM vw_exposicao_por_setor                UNION ALL
SELECT 'vw_clientes_em_atencao',                   COUNT(*) FROM vw_clientes_em_atencao                UNION ALL
SELECT 'vw_operacoes_vencidas',                    COUNT(*) FROM vw_operacoes_vencidas                 UNION ALL
SELECT 'vw_score_por_segmento_porte',              COUNT(*) FROM vw_score_por_segmento_porte           UNION ALL
SELECT 'vw_analise_estatistica',                   COUNT(*) FROM vw_analise_estatistica;
GO

-- ================================================================
-- FIM DO SCRIPT — VIEWS ANALÍTICAS CRÉDITO CORPORATIVO
-- ================================================================


USE credito_ibba;

-- Teste 1: View base com classificacao_risco e provisao_necessaria
SELECT TOP 5
    cliente_id,
    data_referencia,
    exposicao_total,
    classificacao_risco,
    provisao_necessaria,
    pct_provisao
FROM vw_exposicao_enriquecida
ORDER BY data_referencia DESC;


USE credito_ibba;

-- Teste 2: KPI Exposição
SELECT * FROM vw_kpi_exposicao;

-- Teste 3: KPI Ratings
SELECT * FROM vw_kpi_ratings;

-- Teste 4: KPI Limites
SELECT * FROM vw_kpi_limites;

-- Teste 5: KPI Operações
SELECT * FROM vw_kpi_operacoes;

-- Teste 6: KPI Risco
SELECT * FROM vw_kpi_risco;


USE credito_ibba;

-- Teste 7: Evolução score por segmento
SELECT * FROM vw_score_por_segmento_mes
ORDER BY segmento, data_referencia;

-- Teste 8: Exposição por subsetor
SELECT * FROM vw_exposicao_por_subsetor;

-- Teste 9: Exposição por setor
SELECT * FROM vw_exposicao_por_setor
ORDER BY pct_portfolio DESC;

-- Teste 10: Clientes em atenção
SELECT * FROM vw_clientes_em_atencao
ORDER BY exposicao_total_MM DESC;

-- Teste 11: Operações vencidas
SELECT * FROM vw_operacoes_vencidas
ORDER BY valor_utilizado_MM DESC;

-- Teste 12: Score por segmento e porte
SELECT * FROM vw_score_por_segmento_porte
ORDER BY score_medio DESC;

-- Teste 13: Análise estatística
SELECT * FROM vw_analise_estatistica
ORDER BY flag_outlier DESC, z_score DESC;