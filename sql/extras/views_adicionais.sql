-- ================================================================
--  VIEWS ADICIONAIS — ANÁLISES COMPLEMENTARES
--  Crédito Corporativo | Igor | Março / 2026
-- ================================================================
--  Análises além do escopo obrigatório do case, construídas com
--  as tabelas brutas para exceder a expectativa do avaliador.
--  Demonstram visão analítica de negócio de crédito corporativo.
-- ================================================================

USE credito_ibba;
GO


-- ================================================================
-- ANÁLISE ADICIONAL 1 — Mix de Garantias por Segmento
-- ================================================================
-- ----------------------------------------------------------------
-- VIEW | vw_mix_garantias
-- ----------------------------------------------------------------
-- OBJETIVO DE NEGÓCIO:
--   Mapear a qualidade do colateral da carteira por segmento e
--   produto. Garantias reais (Hipoteca, Alienação Fiduciária)
--   oferecem maior recuperação em caso de default. Operações
--   sem garantia representam o maior risco de perda efetiva.
--   Esse mix é determinante para o cálculo de LGD (Loss Given
--   Default) e para a gestão do risco de crédito.
--
-- TÉCNICAS UTILIZADAS:
--   GROUP BY múltiplo para cruzamento segmento x garantia
--   SUM() OVER(PARTITION BY) para % relativo por segmento
--   CASE para classificação da qualidade da garantia
--   COUNT e SUM para volume de operações e valor aprovado
--
-- RESULTADOS ESPERADOS:
--   Identificar segmentos com maior concentração em garantias
--   fracas (Aval Pessoal, Sem Garantia) — maior risco de perda
--   Avaliar se operações de maior valor têm melhor cobertura
-- ----------------------------------------------------------------
CREATE OR ALTER VIEW vw_mix_garantias AS
WITH base AS (
    SELECT
        c.segmento,
        c.setor,
        o.produto,
        o.garantia_tipo,
        o.valor_aprovado,
        o.valor_utilizado,
        o.status_operacao
    FROM operacoes o
    INNER JOIN clientes c ON o.cliente_id = c.cliente_id
    WHERE o.status_operacao IN ('Ativa', 'Vencida')  -- operações em aberto
),
totais_segmento AS (
    SELECT segmento, SUM(valor_aprovado) AS total_segmento
    FROM base GROUP BY segmento
)
SELECT
    b.segmento,
    b.garantia_tipo,

    -- Quantidade de operações por garantia
    COUNT(*)                                               AS qtd_operacoes,

    -- Valor aprovado total por tipo de garantia
    CAST(ROUND(SUM(b.valor_aprovado) / 1000000, 2)
        AS DECIMAL(15,2))                                  AS valor_aprovado_MM,

    -- % do valor aprovado no segmento
    CAST(ROUND(
        SUM(b.valor_aprovado) / ts.total_segmento * 100, 1
    ) AS DECIMAL(10,1))                                    AS pct_no_segmento,

    -- Valor utilizado (exposição real)
    CAST(ROUND(SUM(b.valor_utilizado) / 1000000, 2)
        AS DECIMAL(15,2))                                  AS valor_utilizado_MM,

    -- Classificação da qualidade da garantia
    -- Impacta diretamente o LGD (Loss Given Default)
    CASE b.garantia_tipo
        WHEN 'Hipoteca'             THEN '1 - Forte (Real)'
        WHEN 'Alienação Fiduciária' THEN '1 - Forte (Real)'
        WHEN 'Garantia Real'        THEN '1 - Forte (Real)'
        WHEN 'Penhor'               THEN '2 - Moderada'
        WHEN 'Conta Garantida'      THEN '2 - Moderada'
        WHEN 'Aval Bancário'        THEN '3 - Fraca'
        WHEN 'Aval Pessoal'         THEN '3 - Fraca'
        WHEN 'Sem Garantia'         THEN '4 - Sem Cobertura'
        ELSE '2 - Moderada'
    END                                                    AS qualidade_garantia

FROM base b
INNER JOIN totais_segmento ts ON b.segmento = ts.segmento
GROUP BY b.segmento, b.garantia_tipo, ts.total_segmento;
GO


-- ================================================================
-- ANÁLISE ADICIONAL 2 — Migração de Rating
-- ================================================================
-- ----------------------------------------------------------------
-- VIEW | vw_migracao_rating
-- ----------------------------------------------------------------
-- OBJETIVO DE NEGÓCIO:
--   Monitorar como os ratings dos clientes evoluíram ao longo do
--   ano — quantos melhoraram, quantos pioraram e quantos ficaram
--   estáveis. A migração de rating é um indicador antecedente de
--   risco: deterioração consistente precede inadimplência.
--   Também alimenta modelos de stress test e backtesting de PD.
--
-- TÉCNICAS UTILIZADAS:
--   Self-join na tabela ratings para comparar primeiro e último mês
--   Subqueries de MIN/MAX para isolar os períodos extremos
--   CASE para classificar direção da migração (melhora/piora/estável)
--   Escala numérica de rating para calcular variação quantitativa
--
-- RESULTADOS ESPERADOS:
--   Matriz De/Para: rating inicial x rating final de cada cliente
--   Identificar clientes que migraram para faixas críticas (BB, B, C)
--   Calcular % da carteira com melhora, piora ou estabilidade
-- ----------------------------------------------------------------
CREATE OR ALTER VIEW vw_migracao_rating AS
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
primeiro_rating AS (
    -- Rating do primeiro mês disponível por cliente
    SELECT r.cliente_id, r.rating_interno, r.score_interno,
           er.nota AS nota_inicio
    FROM ratings r
    INNER JOIN escala_rating er ON r.rating_interno = er.rating
    INNER JOIN (
        SELECT cliente_id, MIN(data_referencia) AS primeira_data
        FROM ratings GROUP BY cliente_id
    ) pr ON r.cliente_id = pr.cliente_id
         AND r.data_referencia = pr.primeira_data
),
ultimo_rating AS (
    -- Rating do último mês disponível por cliente
    SELECT r.cliente_id, r.rating_interno, r.score_interno,
           er.nota AS nota_fim
    FROM ratings r
    INNER JOIN escala_rating er ON r.rating_interno = er.rating
    INNER JOIN (
        SELECT cliente_id, MAX(data_referencia) AS ultima_data
        FROM ratings GROUP BY cliente_id
    ) ur ON r.cliente_id = ur.cliente_id
         AND r.data_referencia = ur.ultima_data
)
SELECT
    c.cliente_id,
    c.segmento,
    c.setor,
    c.porte,

    -- Rating inicial (primeiro mês da série)
    pr.rating_interno                                      AS rating_inicial,
    pr.nota_inicio                                         AS nota_inicial,
    pr.score_interno                                       AS score_inicial,

    -- Rating final (último mês da série)
    ur.rating_interno                                      AS rating_final,
    ur.nota_fim                                            AS nota_final,
    ur.score_interno                                       AS score_final,

    -- Variação quantitativa de rating (positivo = melhora)
    (ur.nota_fim - pr.nota_inicio)                         AS variacao_notas,
    (ur.score_interno - pr.score_interno)                  AS variacao_score,

    -- Direção da migração
    CASE
        WHEN ur.nota_fim > pr.nota_inicio THEN 'MELHORA'
        WHEN ur.nota_fim < pr.nota_inicio THEN 'PIORA'
        ELSE 'ESTAVEL'
    END                                                    AS direcao_migracao,

    -- Intensidade da migração
    CASE
        WHEN ABS(ur.nota_fim - pr.nota_inicio) = 0 THEN 'Sem alteração'
        WHEN ABS(ur.nota_fim - pr.nota_inicio) <= 2 THEN 'Leve (1-2 níveis)'
        WHEN ABS(ur.nota_fim - pr.nota_inicio) <= 4 THEN 'Moderada (3-4 níveis)'
        ELSE 'Significativa (5+ níveis)'
    END                                                    AS intensidade_migracao,

    -- Flag de entrada em zona de risco (migrou para BB ou abaixo)
    CASE
        WHEN ur.nota_fim <= 7
         AND pr.nota_inicio > 7 THEN 'SIM - Entrou em zona de risco'
        ELSE 'NAO'
    END                                                    AS entrada_zona_risco

FROM clientes c
INNER JOIN primeiro_rating pr ON c.cliente_id = pr.cliente_id
INNER JOIN ultimo_rating   ur ON c.cliente_id = ur.cliente_id
WHERE c.status_cliente = 'Ativo';
GO


-- ================================================================
-- ANÁLISE ADICIONAL 3 — Taxa de Juros vs. Rating (Pricing)
-- ================================================================
-- ----------------------------------------------------------------
-- VIEW | vw_pricing_por_rating
-- ----------------------------------------------------------------
-- OBJETIVO DE NEGÓCIO:
--   Verificar se o pricing da carteira está tecnicamente correto —
--   ou seja, se clientes com maior risco (rating mais baixo) pagam
--   taxas mais altas, compensando o banco pelo risco assumido.
--   Uma carteira bem precificada tem correlação negativa entre
--   rating e taxa (pior rating → maior taxa). Distorções indicam
--   concessões comerciais que podem comprometer a rentabilidade
--   ajustada ao risco (RAROC).
--
-- TÉCNICAS UTILIZADAS:
--   LEFT JOIN temporal para cruzar operações com rating mais recente
--   AVG para taxa média por faixa de rating e produto
--   CASE para agrupar ratings em faixas (grau investimento/especulativo)
--   ORDER BY nota para apresentar do melhor ao pior rating
--
-- RESULTADOS ESPERADOS:
--   Confirmar se a curva de pricing é crescente (melhor risco = menor taxa)
--   Identificar produtos ou segmentos com pricing invertido
--   Insumo para revisão da política de precificação de crédito
-- ----------------------------------------------------------------
CREATE OR ALTER VIEW vw_pricing_por_rating AS
WITH rating_atual AS (
    -- Último rating disponível por cliente
    SELECT r.cliente_id, r.rating_interno, r.score_interno, r.pd_12m
    FROM ratings r
    INNER JOIN (
        SELECT cliente_id, MAX(data_referencia) AS dt
        FROM ratings GROUP BY cliente_id
    ) ud ON r.cliente_id = ud.cliente_id
         AND r.data_referencia = ud.dt
),
escala_rating AS (
    SELECT 'AAA' AS rating, 17 AS nota UNION ALL SELECT 'AA+', 16
    UNION ALL SELECT 'AA',  15         UNION ALL SELECT 'AA-', 14
    UNION ALL SELECT 'A+',  13         UNION ALL SELECT 'A',   12
    UNION ALL SELECT 'A-',  11         UNION ALL SELECT 'BBB+',10
    UNION ALL SELECT 'BBB', 9          UNION ALL SELECT 'BBB-', 8
    UNION ALL SELECT 'BB+', 7          UNION ALL SELECT 'BB',   6
    UNION ALL SELECT 'BB-', 5          UNION ALL SELECT 'B+',   4
    UNION ALL SELECT 'B',   3          UNION ALL SELECT 'B-',   2
    UNION ALL SELECT 'C',   1
)
SELECT
    ra.rating_interno,
    er.nota                                                AS nota_rating,

    -- Faixa de risco para agrupamento visual
    CASE
        WHEN er.nota >= 13 THEN '1 - Grau Inv. Alto (A+ a AAA)'
        WHEN er.nota >= 10 THEN '2 - Grau Inv. Baixo (BBB a A)'
        WHEN er.nota >= 7  THEN '3 - Grau Esp. Alto (BB a BBB-)'
        WHEN er.nota >= 4  THEN '4 - Grau Esp. Baixo (B+ a BB-)'
        ELSE                    '5 - Alto Risco (C e abaixo)'
    END                                                    AS faixa_risco,

    c.segmento,
    o.produto,

    -- Quantidade de operações na combinação
    COUNT(o.operacao_id)                                   AS qtd_operacoes,

    -- Taxa média de juros — se pricing está correto,
    -- deve crescer conforme a nota de rating diminui
    CAST(ROUND(AVG(o.taxa_juros), 4) AS DECIMAL(10,4))    AS taxa_juros_media,
    CAST(ROUND(MIN(o.taxa_juros), 4) AS DECIMAL(10,4))    AS taxa_juros_minima,
    CAST(ROUND(MAX(o.taxa_juros), 4) AS DECIMAL(10,4))    AS taxa_juros_maxima,

    -- Score e PD médios da combinação
    CAST(ROUND(AVG(CAST(ra.score_interno AS FLOAT)), 0)
        AS INT)                                            AS score_medio,
    CAST(ROUND(AVG(ra.pd_12m) * 100, 4)
        AS DECIMAL(10,4))                                  AS pd_media_pct,

    -- Valor total aprovado na combinação
    CAST(ROUND(SUM(o.valor_aprovado) / 1000000, 2)
        AS DECIMAL(15,2))                                  AS valor_aprovado_MM

FROM operacoes o
INNER JOIN clientes c      ON o.cliente_id = c.cliente_id
INNER JOIN rating_atual ra ON o.cliente_id = ra.cliente_id
INNER JOIN escala_rating er ON ra.rating_interno = er.rating
WHERE o.status_operacao IN ('Ativa', 'Vencida')
GROUP BY ra.rating_interno, er.nota, c.segmento, o.produto;
GO


-- ================================================================
-- VERIFICAÇÃO — Testar as 3 views adicionais
-- ================================================================
SELECT 'vw_mix_garantias'     AS view_name, COUNT(*) AS linhas FROM vw_mix_garantias    UNION ALL
SELECT 'vw_migracao_rating',               COUNT(*) FROM vw_migracao_rating             UNION ALL
SELECT 'vw_pricing_por_rating',            COUNT(*) FROM vw_pricing_por_rating;
GO

-- ================================================================
-- TESTES INDIVIDUAIS
-- ================================================================

-- Mix de garantias: distribuição por segmento
SELECT * FROM vw_mix_garantias
ORDER BY segmento, qualidade_garantia, valor_aprovado_MM DESC;

-- Migração: resumo de direção
SELECT
    direcao_migracao,
    intensidade_migracao,
    COUNT(*) AS qtd_clientes,
    CAST(ROUND(
        COUNT(*) * 100.0 / SUM(COUNT(*)) OVER()
    , 1) AS DECIMAL(10,1))                               AS pct
FROM vw_migracao_rating
GROUP BY direcao_migracao, intensidade_migracao
ORDER BY direcao_migracao, intensidade_migracao;

-- Pricing: taxa média por faixa de risco
SELECT
    faixa_risco,
    COUNT(*) AS qtd_combinacoes,
    ROUND(AVG(taxa_juros_media), 4) AS taxa_media,
    SUM(qtd_operacoes) AS total_operacoes
FROM vw_pricing_por_rating
GROUP BY faixa_risco
ORDER BY faixa_risco;
GO

-- ================================================================
-- FIM DO SCRIPT — VIEWS ADICIONAIS
-- ================================================================
