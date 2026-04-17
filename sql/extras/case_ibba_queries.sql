-- ================================================================
--  CASE TÉCNICO — ANALISTA DE DADOS | CRÉDITO IBBA
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
-- ================================================================

USE credito_ibba;
GO

-- ================================================================
-- QUESTÃO 2.1 | Consulta Básica: Clientes Ativos com Operações
-- ================================================================
-- OBJETIVO DE NEGÓCIO:
--   Identificar o volume e o valor das operações ativas por cliente,
--   permitindo priorizar relacionamentos de maior exposição e
--   monitorar a taxa de utilização do crédito aprovado.
--
-- TÉCNICAS UTILIZADAS:
--   INNER JOIN filtrando apenas operações ativas
--   COUNT e SUM para agregar operações por cliente
--   NULLIF para evitar divisão por zero no cálculo de utilização
--
-- RESULTADO: 48 clientes com operações ativas no portfólio
-- DESTAQUE:  CLI862 lidera com R$ 605M aprovado e 89.4% de
--            utilização — acima do threshold de alerta de 85%
-- ================================================================

SELECT
    c.cliente_id,
    c.segmento,
    COUNT(o.operacao_id)                              AS total_operacoes_ativas,
    SUM(o.valor_aprovado)                             AS valor_aprovado_total,
    SUM(o.valor_utilizado)                            AS valor_utilizado_total,
    ROUND(
        SUM(o.valor_utilizado) /
        NULLIF(SUM(o.valor_aprovado), 0) * 100, 1)   AS pct_utilizacao

FROM clientes c
    INNER JOIN operacoes o
        ON c.cliente_id = o.cliente_id
       AND o.status_operacao = 'Ativa'   -- filtra apenas operações em vigor

WHERE c.status_cliente = 'Ativo'         -- exclui clientes suspensos/inativos

GROUP BY c.cliente_id, c.segmento
ORDER BY valor_aprovado_total DESC;      -- maior exposição no topo
GO


-- ================================================================
-- QUESTÃO 2.2 | Joins e Agregações: Exposição por Subsetor
-- ================================================================
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
-- ================================================================

WITH exposicao_recente AS (
    -- Última posição de exposição disponível por cliente
    SELECT e.cliente_id,
           e.exposicao_total,
           e.exposicao_descoberta
    FROM exposicoes e
    INNER JOIN (
        SELECT cliente_id, MAX(data_referencia) AS ultima_data
        FROM exposicoes
        GROUP BY cliente_id
    ) ult ON e.cliente_id = ult.cliente_id
          AND e.data_referencia = ult.ultima_data
)
SELECT
    c.subsetor,
    COUNT(DISTINCT c.cliente_id)                          AS qtd_clientes,
    ROUND(AVG(er.exposicao_total), 2)                     AS exposicao_media,
    MAX(er.exposicao_total)                               AS maior_exposicao,
    SUM(er.exposicao_total)                               AS exposicao_total_subsetor,
    ROUND(
        SUM(er.exposicao_descoberta) /
        NULLIF(SUM(er.exposicao_total), 0) * 100, 1)     AS pct_exposicao_descoberta

FROM clientes c
    INNER JOIN exposicao_recente er ON c.cliente_id = er.cliente_id

GROUP BY c.subsetor
HAVING COUNT(DISTINCT c.cliente_id) > 5     -- apenas subsetores com massa crítica
ORDER BY exposicao_total_subsetor DESC;
GO


-- ================================================================
-- QUESTÃO 2.3 | Window Functions: Evolução de Rating por Segmento
-- ================================================================
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
--            com score médio ~762 — faixa de ATENÇÃO (750-799)
--            Middle Market liderou o ranking durante todo o período
-- ================================================================

WITH escala_rating AS (
    -- Converte rating categórico em escala numérica para calcular médias
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
        FORMAT(r.data_referencia, 'yyyy-MM')         AS ano_mes,
        r.data_referencia,
        ROUND(AVG(CAST(er.nota AS FLOAT)), 2)        AS nota_media_segmento,
        ROUND(AVG(CAST(r.score_interno AS FLOAT)),0) AS score_medio_segmento
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
    nota_media_segmento,
    score_medio_segmento,

    -- Nota do mês anterior para comparação
    LAG(nota_media_segmento) OVER (
        PARTITION BY segmento ORDER BY data_referencia
    ) AS nota_mes_anterior,

    -- Variação percentual mês a mês
    ROUND(
        (nota_media_segmento
            - LAG(nota_media_segmento) OVER (
                PARTITION BY segmento ORDER BY data_referencia))
        / NULLIF(LAG(nota_media_segmento) OVER (
                PARTITION BY segmento ORDER BY data_referencia), 0) * 100
    , 2) AS variacao_pct_mom,

    -- Ranking dos segmentos por rating dentro de cada mês
    RANK() OVER (
        PARTITION BY ano_mes ORDER BY nota_media_segmento DESC
    ) AS ranking_segmento_mes

FROM rating_mensal
ORDER BY segmento, data_referencia;
GO


-- ================================================================
-- QUESTÃO 2.4 | Análise de Risco: Clientes em Alerta Combinado
-- ================================================================
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
--   - 3 clientes sem dados de exposição (NULL) — verificar pipeline de carga
-- ================================================================

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
    -- Numera os meses do mais recente (1) para o mais antigo por cliente
    SELECT
        r.cliente_id,
        r.data_referencia,
        r.rating_interno,
        er.nota,
        ROW_NUMBER() OVER (
            PARTITION BY r.cliente_id
            ORDER BY r.data_referencia DESC
        ) AS rn
    FROM ratings r
    INNER JOIN escala_rating er ON r.rating_interno = er.rating
),
deterioracao AS (
    -- Identifica clientes com nota caindo nos 3 últimos meses consecutivos
    SELECT h1.cliente_id
    FROM historico_rating h1
    INNER JOIN historico_rating h2
        ON h1.cliente_id = h2.cliente_id AND h2.rn = 2
    INNER JOIN historico_rating h3
        ON h1.cliente_id = h3.cliente_id AND h3.rn = 3
    WHERE h1.rn = 1
      AND h1.nota < h2.nota   -- mês atual pior que mês anterior
      AND h2.nota < h3.nota   -- mês anterior pior que dois meses atrás
),
utilizacao_limite AS (
    -- Utilização do limite Global com precisão de 1 casa decimal
    SELECT
        cliente_id,
        CAST(
            ROUND(valor_utilizado / NULLIF(valor_limite, 0) * 100, 1)
        AS DECIMAL(10,1))                    AS pct_utilizacao
    FROM limites
    WHERE tipo_limite = 'Global'             -- apenas limite consolidado
),
exposicao_atual AS (
    -- Última posição de exposição disponível por cliente
    SELECT
        e.cliente_id,
        e.exposicao_total,
        e.exposicao_descoberta,
        CAST(
            ROUND(e.exposicao_descoberta /
                  NULLIF(e.exposicao_total, 0) * 100, 1)
        AS DECIMAL(10,1))                    AS pct_descoberta
    FROM exposicoes e
    INNER JOIN (
        SELECT cliente_id, MAX(data_referencia) AS ultima_data
        FROM exposicoes
        GROUP BY cliente_id
    ) ult ON e.cliente_id = ult.cliente_id
          AND e.data_referencia = ult.ultima_data
),
concentracao_subsetor AS (
    -- Peso percentual de cada subsetor no portfólio total
    SELECT
        c.subsetor,
        CAST(
            ROUND(SUM(ea.exposicao_total) /
                  SUM(SUM(ea.exposicao_total)) OVER () * 100, 1)
        AS DECIMAL(10,1))                    AS pct_concentracao
    FROM exposicao_atual ea
    INNER JOIN clientes c ON ea.cliente_id = c.cliente_id
    GROUP BY c.subsetor
)
SELECT
    c.cliente_id,
    c.segmento,
    c.setor,
    c.subsetor,
    ul.pct_utilizacao,                               -- % limite global usado
    ea.pct_descoberta,                               -- % exposição sem garantia
    CAST(ROUND(ea.exposicao_total / 1000000, 2)
        AS DECIMAL(15,2))                            AS exposicao_total_MM,
    cs.pct_concentracao                              AS concentracao_subsetor_pct,
    CASE WHEN d.cliente_id IS NOT NULL
         THEN 'SIM' ELSE 'NAO'
    END                                              AS deterioracao_rating,
    CASE
        WHEN d.cliente_id IS NOT NULL
             AND ul.pct_utilizacao > 80
             AND ea.pct_descoberta > 30 THEN 'ALTO RISCO'
        WHEN ul.pct_utilizacao > 75
          OR ea.pct_descoberta > 25     THEN 'ATENCAO'
        ELSE 'NORMAL'
    END                                              AS classificacao_risco

FROM clientes c
    INNER JOIN utilizacao_limite ul     ON c.cliente_id = ul.cliente_id
    INNER JOIN exposicao_atual ea       ON c.cliente_id = ea.cliente_id
    INNER JOIN concentracao_subsetor cs ON c.subsetor   = cs.subsetor
    LEFT  JOIN deterioracao d           ON c.cliente_id = d.cliente_id

WHERE ul.pct_utilizacao > 75          -- utilização acima do threshold de alerta
   OR ea.pct_descoberta > 25          -- exposição descoberta acima da meta
   OR d.cliente_id IS NOT NULL        -- qualquer deterioração consecutiva de rating

ORDER BY
    CASE
        WHEN d.cliente_id IS NOT NULL
             AND ul.pct_utilizacao > 80
             AND ea.pct_descoberta > 30 THEN 1   -- ALTO RISCO primeiro
        WHEN ul.pct_utilizacao > 75
          OR ea.pct_descoberta > 25     THEN 2   -- ATENCAO em seguida
        ELSE 3
    END,
    ea.exposicao_total DESC;                     -- maior exposição no topo
GO


-- ================================================================
-- QUESTÃO 2.5 | Análise Estatística Avançada
-- ================================================================
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
--   Correlação de Pearson calculada via fórmula algébrica
--   UNION ALL para consolidar os 3 blocos em resultado único
--
-- RESULTADOS ENCONTRADOS:
--   OUTLIERS    : 3 clientes com Z-Score > 2
--                 CLI862 (Wholesale, R$541M, Z=2.49)
--                 CLI402 (Middle Market, R$65M, Z=2.70)
--                 CLI524 (Middle Market, R$56M, Z=2.25)
--   DISTRIBUICAO: Middle Market com maior heterogeneidade (CV=155.7%)
--                 Corporate mais homogêneo (CV=68.6%)
--   CORRELACAO  : Pearson = 0.0176 (FRACA) — score e utilização
--                 são independentes, sugerindo que limites seguem
--                 critérios estratégicos além do score isolado
-- ================================================================

WITH exposicao_base AS (
    SELECT
        e.cliente_id,
        e.exposicao_total,
        c.segmento,
        c.subsetor
    FROM exposicoes e
    INNER JOIN clientes c ON e.cliente_id = c.cliente_id
    INNER JOIN (
        SELECT cliente_id, MAX(data_referencia) AS ultima_data
        FROM exposicoes
        GROUP BY cliente_id
    ) ult ON e.cliente_id = ult.cliente_id
          AND e.data_referencia = ult.ultima_data
),
stats_segmento AS (
    SELECT
        segmento,
        AVG(exposicao_total)     AS media_seg,
        STDEV(exposicao_total)   AS desvio_seg
    FROM exposicao_base
    GROUP BY segmento
),
zscore AS (
    SELECT
        eb.cliente_id,
        eb.segmento,
        CAST(ROUND(eb.exposicao_total / 1000000, 2)
            AS DECIMAL(15,2))                        AS exposicao_MM,
        CAST(ROUND(
            (eb.exposicao_total - ss.media_seg)
            / NULLIF(ss.desvio_seg, 0), 2)
            AS DECIMAL(10,2))                        AS z_score,
        CASE
            WHEN ABS((eb.exposicao_total - ss.media_seg)
                 / NULLIF(ss.desvio_seg, 0)) > 2
            THEN 'OUTLIER'
            ELSE 'NORMAL'
        END                                          AS flag_outlier
    FROM exposicao_base eb
    INNER JOIN stats_segmento ss ON eb.segmento = ss.segmento
),
distribuicao AS (
    SELECT
        segmento,
        COUNT(*)                                         AS n_clientes,
        CAST(ROUND(MIN(exposicao_total)/1000000, 2)
            AS DECIMAL(15,2))                            AS minimo_MM,
        CAST(ROUND(MAX(exposicao_total)/1000000, 2)
            AS DECIMAL(15,2))                            AS maximo_MM,
        CAST(ROUND(AVG(exposicao_total)/1000000, 2)
            AS DECIMAL(15,2))                            AS media_MM,
        CAST(ROUND(STDEV(exposicao_total)/1000000, 2)
            AS DECIMAL(15,2))                            AS desvio_MM,
        CAST(ROUND(
            STDEV(exposicao_total)
            / NULLIF(AVG(exposicao_total), 0) * 100, 1)
            AS DECIMAL(10,1))                            AS coef_variacao_pct
    FROM exposicao_base
    GROUP BY segmento
),
rating_recente AS (
    SELECT r.cliente_id, r.score_interno
    FROM ratings r
    INNER JOIN (
        SELECT cliente_id, MAX(data_referencia) AS ult
        FROM ratings GROUP BY cliente_id
    ) ult ON r.cliente_id = ult.cliente_id
          AND r.data_referencia = ult.ult
),
corr_base AS (
    SELECT
        CAST(rr.score_interno AS FLOAT)                  AS x,
        CAST(l.valor_utilizado
             / NULLIF(l.valor_limite, 0) * 100 AS FLOAT) AS y
    FROM rating_recente rr
    INNER JOIN limites l ON rr.cliente_id = l.cliente_id
    WHERE l.tipo_limite = 'Global'
),
correlacao AS (
    SELECT
        COUNT(*)                                         AS n_obs,
        CAST(ROUND(
            (COUNT(*) * SUM(x*y) - SUM(x) * SUM(y))
            / NULLIF(
                SQRT(COUNT(*) * SUM(x*x) - POWER(SUM(x), 2)) *
                SQRT(COUNT(*) * SUM(y*y) - POWER(SUM(y), 2))
              , 0), 4) AS DECIMAL(10,4))                 AS correlacao_pearson
    FROM corr_base
)
SELECT
    '1 - OUTLIERS'                       AS bloco,
    z.cliente_id                         AS dimensao,
    z.segmento,
    CAST(z.exposicao_MM AS VARCHAR(20))  AS valor1_exposicao_MM,
    CAST(z.z_score AS VARCHAR(20))       AS valor2_z_score,
    z.flag_outlier                       AS classificacao
FROM zscore z
WHERE z.flag_outlier = 'OUTLIER'

UNION ALL

SELECT
    '2 - DISTRIBUICAO'                           AS bloco,
    d.segmento                                   AS dimensao,
    d.segmento,
    CAST(d.media_MM AS VARCHAR(20))              AS valor1_media_MM,
    CAST(d.coef_variacao_pct AS VARCHAR(20))     AS valor2_coef_var_pct,
    CAST(d.n_clientes AS VARCHAR(20))            AS classificacao
FROM distribuicao d

UNION ALL

SELECT
    '3 - CORRELACAO'                             AS bloco,
    'Score x Utilizacao Limite'                  AS dimensao,
    'Todos os segmentos'                         AS segmento,
    CAST(c.correlacao_pearson AS VARCHAR(20))    AS valor1_correlacao,
    CAST(c.n_obs AS VARCHAR(20))                 AS valor2_n_observacoes,
    CASE
        WHEN ABS(c.correlacao_pearson) < 0.2 THEN 'FRACA'
        WHEN ABS(c.correlacao_pearson) < 0.5 THEN 'MODERADA'
        ELSE 'FORTE'
    END                                          AS classificacao
FROM correlacao c

ORDER BY bloco, dimensao;
GO


-- ================================================================
-- QUESTÃO 2.6-A | Concentração Setorial vs. Limite Regulatório
-- ================================================================
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
-- ================================================================

WITH exposicao_atual AS (
    SELECT e.cliente_id, e.exposicao_total
    FROM exposicoes e
    INNER JOIN (
        SELECT cliente_id, MAX(data_referencia) AS ultima_data
        FROM exposicoes GROUP BY cliente_id
    ) ult ON e.cliente_id = ult.cliente_id
          AND e.data_referencia = ult.ultima_data
),
total_portfolio AS (
    SELECT SUM(exposicao_total) AS total FROM exposicao_atual
)
SELECT
    c.setor,
    COUNT(DISTINCT c.cliente_id)                          AS qtd_clientes,
    CAST(ROUND(SUM(ea.exposicao_total)/1000000, 1)
        AS DECIMAL(15,1))                                 AS exposicao_MM,
    CAST(ROUND(SUM(ea.exposicao_total)
        / tp.total * 100, 1)
        AS DECIMAL(10,1))                                 AS pct_portfolio,
    CASE
        WHEN SUM(ea.exposicao_total)/tp.total*100 > 15
            THEN 'ACIMA DO LIMITE'
        WHEN SUM(ea.exposicao_total)/tp.total*100 > 10
            THEN 'MONITORAMENTO'
        ELSE 'NORMAL'
    END                                                   AS status_regulatorio
FROM clientes c
INNER JOIN exposicao_atual ea ON c.cliente_id = ea.cliente_id
CROSS JOIN total_portfolio tp
GROUP BY c.setor, tp.total
ORDER BY pct_portfolio DESC;
GO


-- ================================================================
-- QUESTÃO 2.6-B | Operações Vencidas: Risco Materializado
-- ================================================================
-- OBJETIVO: Identificar operações vencidas e não liquidadas,
--   cruzando com o perfil de risco do cliente para priorizar
--   ações de cobrança e provisionamento.
--
-- TÉCNICAS UTILIZADAS:
--   INNER JOIN com rating mais recente via subquery de MAX()
--   DATEDIFF para calcular dias em atraso em relação à data atual
--   CAST para padronizar casas decimais nos valores monetários
--
-- RESULTADOS ENCONTRADOS:
--   11 operações com status Vencida no portfólio
--   Maior exposição : CLI368 — R$182.8M, 100 dias vencido (Tecnologia)
--   Mais antigo     : CLI318 — 653 dias vencido (Agronegócio)
--   Maior risco     : CLI956 — Rating C, PD 31.09%, 2 operações vencidas,
--                     uma delas sem garantia (Mineração)
--   Setores afetados: Tecnologia, Químico, Agronegócio, Automotivo,
--                     Mineração, Educação e Saúde
--
-- RECOMENDAÇÃO: CLI956 requer provisionamento integral imediato (Rating C).
--   CLI368 e CLI488 demandam acionamento jurídico urgente pelo volume.
--   CLI318 com 653 dias sugere falha no processo de cobrança — revisar SLA.
-- ================================================================

WITH rating_atual AS (
    SELECT r.cliente_id, r.rating_interno, r.score_interno, r.pd_12m
    FROM ratings r
    INNER JOIN (
        SELECT cliente_id, MAX(data_referencia) AS ult
        FROM ratings GROUP BY cliente_id
    ) ult ON r.cliente_id = ult.cliente_id
          AND r.data_referencia = ult.ult
)
SELECT
    o.operacao_id,
    o.cliente_id,
    c.setor,
    c.segmento,
    o.produto,
    o.modalidade,
    CAST(ROUND(o.valor_aprovado  / 1000000, 2)
        AS DECIMAL(15,2))                        AS valor_aprovado_MM,
    CAST(ROUND(o.valor_utilizado / 1000000, 2)
        AS DECIMAL(15,2))                        AS valor_utilizado_MM,
    o.data_vencimento,
    DATEDIFF(DAY, o.data_vencimento, GETDATE())  AS dias_vencido,
    o.garantia_tipo,
    ra.rating_interno,
    ra.score_interno,
    CAST(ra.pd_12m * 100 AS DECIMAL(10,4))       AS pd_12m_pct
FROM operacoes o
INNER JOIN clientes c      ON o.cliente_id = c.cliente_id
INNER JOIN rating_atual ra ON o.cliente_id = ra.cliente_id
WHERE o.status_operacao = 'Vencida'
ORDER BY valor_utilizado_MM DESC;
GO


-- ================================================================
-- QUESTÃO 2.6-C | Score Médio por Segmento e Porte
-- ================================================================
-- OBJETIVO: Entender como a qualidade de crédito se distribui
--   entre segmentos e portes, identificando combinações de maior
--   e menor risco para subsidiar decisões de apetite de crédito.
--
-- TÉCNICAS UTILIZADAS:
--   INNER JOIN com rating mais recente via subquery de MAX()
--   AVG e STDEV para medir qualidade e homogeneidade por grupo
--   CASE para semáforo de score conforme benchmarks internos
--
-- RESULTADOS ENCONTRADOS:
--   EXCELENTE : Wholesale Grande (score 865) — topo do portfólio
--   CUIDADO   : Corporate Médio (score 676, 2 clientes) — grupo mais frágil
--   ACHADO    : Porte impacta significativamente o score dentro do mesmo
--               segmento — Wholesale Grande (865) vs Médio (761) = 104 pts
--               de diferença — recomenda políticas de concessão por porte
--
-- RECOMENDAÇÃO: Revisar apetite de crédito para Corporate Médio.
--   Monitorar os 2 clientes individualmente dado o score mínimo de 613.
-- ================================================================

WITH rating_atual AS (
    SELECT r.cliente_id, r.score_interno, r.rating_interno
    FROM ratings r
    INNER JOIN (
        SELECT cliente_id, MAX(data_referencia) AS ult
        FROM ratings GROUP BY cliente_id
    ) ult ON r.cliente_id = ult.cliente_id
          AND r.data_referencia = ult.ult
)
SELECT
    c.segmento,
    c.porte,
    COUNT(DISTINCT c.cliente_id)                      AS qtd_clientes,
    CAST(ROUND(AVG(CAST(ra.score_interno AS FLOAT)),0)
        AS INT)                                       AS score_medio,
    CAST(ROUND(MIN(ra.score_interno),0) AS INT)       AS score_minimo,
    CAST(ROUND(MAX(ra.score_interno),0) AS INT)       AS score_maximo,
    CAST(ROUND(STDEV(CAST(ra.score_interno AS FLOAT)),0)
        AS INT)                                       AS desvio_padrao,
    CASE
        WHEN AVG(CAST(ra.score_interno AS FLOAT)) >= 850
            THEN 'EXCELENTE'
        WHEN AVG(CAST(ra.score_interno AS FLOAT)) >= 800
            THEN 'MUITO BOM'
        WHEN AVG(CAST(ra.score_interno AS FLOAT)) >= 750
            THEN 'ATENCAO'
        WHEN AVG(CAST(ra.score_interno AS FLOAT)) >= 650
            THEN 'CUIDADO'
        ELSE 'CRITICO'
    END                                               AS classificacao_score
FROM clientes c
INNER JOIN rating_atual ra ON c.cliente_id = ra.cliente_id
WHERE c.status_cliente = 'Ativo'
GROUP BY c.segmento, c.porte
ORDER BY score_medio DESC;
GO

-- ================================================================
-- FIM DO SCRIPT — CASE TÉCNICO CRÉDITO IBBA
-- ================================================================
