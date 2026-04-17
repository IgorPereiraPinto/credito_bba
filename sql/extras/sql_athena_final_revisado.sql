-- ================================================================
--  CASE TÉCNICO — ANALISTA DE DADOS | CRÉDITO CORPORATIVO
--  Candidato : Igor
--  Posição   : Analista de Dados Pleno
--  Data      : Março / 2026
-- ================================================================
--  NOTA TÉCNICA:
--  Solução desenvolvida com sintaxe Amazon Athena (Presto/Trino).
--  Stack de produção: Amazon S3 + AWS Glue + Athena + QuickSight.
--  Validada e testada localmente em SQL Server Developer Edition
--  com adaptações mínimas de sintaxe documentadas neste arquivo.
--
--  ESTRUTURA DESTE ARQUIVO:
--  SEÇÃO 1 — DDL: criação do banco, tabelas e verificações
--  SEÇÃO 2 — QUERIES ANALÍTICAS: consultas do case comentadas
--  SEÇÃO 3 — VIEWS OBRIGATÓRIAS: camada semântica para Amazon QuickSight
--  SEÇÃO 4 — VIEWS ADICIONAIS: análises além do escopo do case
--
--  COMPATIBILIDADE:
--  Sintaxe Athena (Presto/Trino). Para SQL Server, ajustar:
--  DATE_FORMAT → FORMAT  |  STDDEV → STDEV  |  CURRENT_DATE → CURRENT_DATE
--  DATE_DIFF   → DATEDIFF  |  CREATE OR REPLACE VIEW → CREATE OR REPLACE VIEW
-- ================================================================


-- SEÇÃO 1 — CRIAÇÃO DO BANCO DE DADOS E TABELAS (DDL)
-- ================================================================

-- ================================================================
-- ARQUITETURA DE DADOS NO AMBIENTE AWS
-- ================================================================
-- No Athena, as tabelas são EXTERNAS — os dados residem no Amazon S3
-- e o esquema é registrado no AWS Glue Data Catalog.
-- Não existem PKs ou FKs declaradas: a integridade é garantida
-- pelo pipeline de ETL (AWS Glue) antes da carga no S3.
--
-- FLUXO DE CARGA:
--   Fonte (Excel/Core bancário)
--     → AWS Glue Job (limpeza + deduplicação)
--     → Amazon S3 (formato Parquet, particionado por data)
--     → AWS Glue Crawler (atualiza catálogo automaticamente)
--     → Amazon Athena (tabelas externas prontas para consulta)
--     → Amazon QuickSight (consome via Athena como dataset)
-- ================================================================

-- Passo 1: Criar o banco de dados no Glue Data Catalog
CREATE DATABASE IF NOT EXISTS credito_ibba;

-- ----------------------------------------------------------------
-- Tabela 1: clientes — cadastro base de clientes corporativos
-- Dados no S3: s3://bucket-credito/trusted/clientes/
-- Formato: Parquet | Atualização: diária via Glue Job
-- ----------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS credito_ibba.clientes (
    cliente_id                 STRING,
    segmento                   STRING,
    porte                      STRING,
    setor                      STRING,
    subsetor                   STRING,
    data_inicio_relacionamento DATE,
    regiao                     STRING,
    status_cliente             STRING
)
STORED AS PARQUET
LOCATION 's3://bucket-credito/trusted/clientes/'
TBLPROPERTIES ('parquet.compress' = 'SNAPPY');

-- ----------------------------------------------------------------
-- Tabela 2: operacoes — operações de crédito por cliente
-- Dados no S3: s3://bucket-credito/trusted/operacoes/
-- Formato: Parquet | Atualização: near-realtime via Kinesis Firehose
-- ----------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS credito_ibba.operacoes (
    operacao_id      STRING,
    cliente_id       STRING,
    produto          STRING,
    modalidade       STRING,
    valor_aprovado   DECIMAL(15,2),
    valor_utilizado  DECIMAL(15,2),
    taxa_juros       DECIMAL(8,4),
    prazo_meses      INT,
    data_aprovacao   DATE,
    data_vencimento  DATE,
    garantia_tipo    STRING,
    status_operacao  STRING
)
STORED AS PARQUET
LOCATION 's3://bucket-credito/trusted/operacoes/'
TBLPROPERTIES ('parquet.compress' = 'SNAPPY');

-- ----------------------------------------------------------------
-- Tabela 3: ratings — histórico mensal de rating por cliente
-- Dados no S3: s3://bucket-credito/trusted/ratings/
-- Particionado por: ano_mes (ex: ano_mes=2026-02)
-- ----------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS credito_ibba.ratings (
    cliente_id       STRING,
    data_referencia  DATE,
    rating_interno   STRING,
    rating_externo   STRING,
    pd_12m           DECIMAL(8,6),
    score_interno    INT,
    observacao       STRING
)
PARTITIONED BY (ano_mes STRING)
STORED AS PARQUET
LOCATION 's3://bucket-credito/trusted/ratings/'
TBLPROPERTIES ('parquet.compress' = 'SNAPPY');

-- ----------------------------------------------------------------
-- Tabela 4: limites — limites de crédito aprovados por tipo
-- Dados no S3: s3://bucket-credito/trusted/limites/
-- Atualização: semestral ou por evento de revisão de limite
-- ----------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS credito_ibba.limites (
    cliente_id      STRING,
    tipo_limite     STRING,
    valor_limite    DECIMAL(15,2),
    valor_utilizado DECIMAL(15,2),
    data_aprovacao  DATE,
    data_revisao    DATE,
    aprovador       STRING,
    status_limite   STRING
)
STORED AS PARQUET
LOCATION 's3://bucket-credito/trusted/limites/'
TBLPROPERTIES ('parquet.compress' = 'SNAPPY');

-- ----------------------------------------------------------------
-- Tabela 5: exposicoes — posição consolidada mensal de exposição
-- Dados no S3: s3://bucket-credito/trusted/exposicoes/
-- Particionado por: ano_mes (ex: ano_mes=2026-02)
-- ----------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS credito_ibba.exposicoes (
    cliente_id            STRING,
    data_referencia       DATE,
    exposicao_total       DECIMAL(15,2),
    exposicao_garantida   DECIMAL(15,2),
    exposicao_descoberta  DECIMAL(15,2)
)
PARTITIONED BY (ano_mes STRING)
STORED AS PARQUET
LOCATION 's3://bucket-credito/trusted/exposicoes/'
TBLPROPERTIES ('parquet.compress' = 'SNAPPY');

-- ----------------------------------------------------------------
-- Após criar as tabelas particionadas, sincronizar as partições:
-- ----------------------------------------------------------------
MSCK REPAIR TABLE credito_ibba.ratings;
MSCK REPAIR TABLE credito_ibba.exposicoes;


-- ================================================================
-- SEÇÃO 2 — VERIFICAÇÕES DE ESTRUTURA E QUALIDADE
-- ================================================================


-- ----------------------------------------------------------------
-- 2.1 Confirmar que as 5 tabelas foram criadas corretamente
-- ----------------------------------------------------------------
-- Athena: listar tabelas do banco de dados
SHOW TABLES IN credito_ibba;

-- Descrever estrutura de uma tabela (equivalente ao sp_help do SQL Server)
DESCRIBE credito_ibba.clientes;
DESCRIBE credito_ibba.operacoes;
DESCRIBE credito_ibba.ratings;
DESCRIBE credito_ibba.limites;
DESCRIBE credito_ibba.exposicoes;

-- ----------------------------------------------------------------
-- 2.2 Inspecionar estrutura detalhada de cada tabela
-- ----------------------------------------------------------------

-- ----------------------------------------------------------------
-- 2.3 Contagem de registros por tabela após carga
--     Resultado esperado: 72 / 222 / 864 / 92 / 432
--     Nota: valores abaixo do Excel original (75/223/900/95/432)
--     refletem deduplicação por PK — registros duplicados foram
--     descartados para garantir integridade referencial.
-- ----------------------------------------------------------------
SELECT 'clientes'   AS tabela, COUNT(*) AS registros FROM clientes  UNION ALL
SELECT 'operacoes',             COUNT(*)              FROM operacoes UNION ALL
SELECT 'ratings',               COUNT(*)              FROM ratings   UNION ALL
SELECT 'limites',               COUNT(*)              FROM limites   UNION ALL
SELECT 'exposicoes',            COUNT(*)              FROM exposicoes;

-- ----------------------------------------------------------------
-- 2.4 Verificações de qualidade dos dados (Questão 1.2 do case)
-- ----------------------------------------------------------------

-- CHECK 1: valor_utilizado deve ser <= valor_aprovado em operacoes
SELECT operacao_id, cliente_id, valor_aprovado, valor_utilizado
FROM operacoes
WHERE valor_utilizado > valor_aprovado;

-- CHECK 2: exposicao_descoberta = exposicao_total - exposicao_garantida
SELECT cliente_id, data_referencia,
       exposicao_total, exposicao_garantida, exposicao_descoberta,
       ROUND(exposicao_total - exposicao_garantida, 2) AS descoberta_calculada,
       ROUND(ABS(exposicao_descoberta
             - (exposicao_total - exposicao_garantida)), 2) AS diferenca
FROM exposicoes
WHERE ABS(exposicao_descoberta
      - (exposicao_total - exposicao_garantida)) > 0.01;

-- CHECK 3: pd_12m deve estar entre 0 e 1
SELECT cliente_id, data_referencia, pd_12m
FROM ratings
WHERE pd_12m < 0 OR pd_12m > 1 OR pd_12m IS NULL;

-- CHECK 4: data_aprovacao deve ser anterior a data_vencimento
SELECT operacao_id, data_aprovacao, data_vencimento
FROM operacoes
WHERE data_aprovacao >= data_vencimento;

-- CHECK 5: integridade referencial — orphan records nas tabelas filhas
SELECT 'ratings sem cliente'    AS check_ref, COUNT(*) AS qtd
FROM ratings r
LEFT JOIN clientes c ON r.cliente_id = c.cliente_id
WHERE c.cliente_id IS NULL
UNION ALL
SELECT 'limites sem cliente',    COUNT(*)
FROM limites l
LEFT JOIN clientes c ON l.cliente_id = c.cliente_id
WHERE c.cliente_id IS NULL
UNION ALL
SELECT 'exposicoes sem cliente', COUNT(*)
FROM exposicoes e
LEFT JOIN clientes c ON e.cliente_id = c.cliente_id
WHERE c.cliente_id IS NULL
UNION ALL
SELECT 'operacoes sem cliente',  COUNT(*)
FROM operacoes o
LEFT JOIN clientes c ON o.cliente_id = c.cliente_id
WHERE c.cliente_id IS NULL;


-- ================================================================
-- SEÇÃO 2 — QUERIES ANALÍTICAS DO CASE
-- ================================================================

-- ----------------------------------------------------------------
-- QUESTÃO 2.1 | Consulta Básica: Clientes Ativos com Operações
-- ----------------------------------------------------------------
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
-- DESTAQUE:  CLI862 lidera com R$605M aprovado e 89.4% utilização
--            — acima do threshold de alerta de 85%
-- ----------------------------------------------------------------


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


-- ----------------------------------------------------------------
-- QUESTÃO 2.2 | Joins e Agregações: Exposição por Subsetor
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


WITH exposicao_recente AS (
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
HAVING COUNT(DISTINCT c.cliente_id) > 5
ORDER BY exposicao_total_subsetor DESC;


-- ----------------------------------------------------------------
-- QUESTÃO 2.3 | Window Functions: Evolução de Rating por Segmento
-- ----------------------------------------------------------------
-- OBJETIVO DE NEGÓCIO:
--   Monitorar a qualidade de crédito de cada segmento ao longo
--   do tempo. A variação MoM indica tendência de melhora ou
--   deterioração e o ranking mensal permite benchmarking entre
--   segmentos perante o Comitê de Crédito.
--
-- TÉCNICAS UTILIZADAS:
--   CTE escala_rating para converter rating em escala numérica
--   DATE_FORMAT() para agrupar por ano-mês (sintaxe Athena)
--   LAG() OVER(PARTITION BY) para comparação mês a mês
--   RANK() OVER(PARTITION BY) para ranking mensal entre segmentos
--
-- RESULTADO: 36 linhas — 3 segmentos x 12 meses
-- INSIGHT:   Corporate ficou em 3º lugar em todos os 12 meses
--            com score médio ~762 — faixa ATENÇÃO (750-799)
--            Middle Market liderou o ranking durante todo o período
-- ----------------------------------------------------------------


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
        DATE_FORMAT(r.data_referencia, '%Y-%m')         AS ano_mes,
        r.data_referencia,
        ROUND(AVG(CAST(er.nota AS FLOAT)), 2)        AS nota_media_segmento,
        ROUND(AVG(CAST(r.score_interno AS FLOAT)),0) AS score_medio_segmento
    FROM ratings r
        INNER JOIN clientes c       ON r.cliente_id     = c.cliente_id
        INNER JOIN escala_rating er ON r.rating_interno = er.rating
    GROUP BY c.segmento,
             DATE_FORMAT(r.data_referencia, '%Y-%m'),
             r.data_referencia
)
SELECT
    segmento,
    ano_mes,
    nota_media_segmento,
    score_medio_segmento,
    LAG(nota_media_segmento) OVER (
        PARTITION BY segmento ORDER BY data_referencia
    ) AS nota_mes_anterior,
    ROUND(
        (nota_media_segmento
            - LAG(nota_media_segmento) OVER (
                PARTITION BY segmento ORDER BY data_referencia))
        / NULLIF(LAG(nota_media_segmento) OVER (
                PARTITION BY segmento ORDER BY data_referencia), 0) * 100
    , 2) AS variacao_pct_mom,
    RANK() OVER (
        PARTITION BY ano_mes ORDER BY nota_media_segmento DESC
    ) AS ranking_segmento_mes
FROM rating_mensal
ORDER BY segmento, data_referencia;


-- ----------------------------------------------------------------
-- QUESTÃO 2.4 | Análise de Risco: Clientes em Alerta Combinado
-- ----------------------------------------------------------------
-- OBJETIVO DE NEGÓCIO:
--   Responder ao enunciado do case em duas camadas complementares:
--   (1) recorte estrito para localizar clientes com deterioração de
--       rating nos últimos 3 meses + utilização > 80% + exposição
--       descoberta > 30% + leitura de concentração por subsetor;
--   (2) camada operacional de monitoramento (> 75% OU > 25%), mantida
--       para enriquecer a priorização da carteira sem perder aderência
--       ao enunciado original.
--
-- TÉCNICAS UTILIZADAS:
--   CTEs encadeadas para modularidade e legibilidade
--   ROW_NUMBER() para selecionar o período mais recente por cliente
--   Self-join em historico_rating para detectar deterioração consecutiva
--   SUM() OVER() para concentração relativa por subsetor
--   CASE combinado separando regra estrita do case e monitoramento
--
-- RESULTADO:
--   - Regra estrita do case: nenhum cliente elegível no snapshot final,
--     pois a base tratada apresenta pct_descoberta de 25,0% no recorte.
--   - Camada operacional: 24 clientes em ATENCAO, úteis para gestão
--     preventiva e acompanhamento comercial.
-- ----------------------------------------------------------------


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
    SELECT h1.cliente_id
    FROM historico_rating h1
    INNER JOIN historico_rating h2
        ON h1.cliente_id = h2.cliente_id AND h2.rn = 2
    INNER JOIN historico_rating h3
        ON h1.cliente_id = h3.cliente_id AND h3.rn = 3
    WHERE h1.rn = 1
      AND h1.nota < h2.nota
      AND h2.nota < h3.nota
),
utilizacao_limite AS (
    SELECT
        cliente_id,
        CAST(
            ROUND(valor_utilizado / NULLIF(valor_limite, 0) * 100, 1)
        AS DECIMAL(10,1))                    AS pct_utilizacao
    FROM limites
    WHERE tipo_limite = 'Global'
),
exposicao_atual AS (
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
        FROM exposicoes GROUP BY cliente_id
    ) ult ON e.cliente_id = ult.cliente_id
          AND e.data_referencia = ult.ultima_data
),
concentracao_subsetor AS (
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
    ul.pct_utilizacao,
    ea.pct_descoberta,
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
WHERE ul.pct_utilizacao > 75
   OR ea.pct_descoberta > 25
   OR d.cliente_id IS NOT NULL
ORDER BY
    CASE
        WHEN d.cliente_id IS NOT NULL
             AND ul.pct_utilizacao > 80
             AND ea.pct_descoberta > 30 THEN 1
        WHEN ul.pct_utilizacao > 75
          OR ea.pct_descoberta > 25     THEN 2
        ELSE 3
    END,
    ea.exposicao_total DESC;


-- BLOCO COMPLEMENTAR 2.4-A | Recorte estrito do enunciado
-- Regra do case: deterioracao de rating nos ultimos 3 meses
-- + utilizacao > 80% + exposicao descoberta > 30%
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
    SELECT
        r.cliente_id,
        r.data_referencia,
        er.nota,
        ROW_NUMBER() OVER (
            PARTITION BY r.cliente_id
            ORDER BY r.data_referencia DESC
        ) AS rn
    FROM ratings r
    INNER JOIN escala_rating er ON r.rating_interno = er.rating
),
deterioracao_3m AS (
    SELECT h1.cliente_id
    FROM historico_rating h1
    INNER JOIN historico_rating h2
        ON h1.cliente_id = h2.cliente_id AND h2.rn = 2
    INNER JOIN historico_rating h3
        ON h1.cliente_id = h3.cliente_id AND h3.rn = 3
    WHERE h1.rn = 1
      AND h1.nota < h2.nota
      AND h2.nota < h3.nota
),
utilizacao_limite AS (
    SELECT
        cliente_id,
        CAST(ROUND(valor_utilizado / NULLIF(valor_limite, 0) * 100, 1) AS DECIMAL(10,1)) AS pct_utilizacao
    FROM limites
    WHERE tipo_limite = 'Global'
),
exposicao_atual AS (
    SELECT
        e.cliente_id,
        e.exposicao_total,
        CAST(ROUND(e.exposicao_descoberta / NULLIF(e.exposicao_total, 0) * 100, 1) AS DECIMAL(10,1)) AS pct_descoberta
    FROM exposicoes e
    INNER JOIN (
        SELECT cliente_id, MAX(data_referencia) AS ultima_data
        FROM exposicoes
        GROUP BY cliente_id
    ) ult ON e.cliente_id = ult.cliente_id
          AND e.data_referencia = ult.ultima_data
),
concentracao_subsetor AS (
    SELECT
        c.subsetor,
        CAST(ROUND(SUM(ea.exposicao_total) /
              SUM(SUM(ea.exposicao_total)) OVER () * 100, 1) AS DECIMAL(10,1)) AS pct_concentracao
    FROM exposicao_atual ea
    INNER JOIN clientes c ON ea.cliente_id = c.cliente_id
    GROUP BY c.subsetor
)
SELECT
    c.cliente_id,
    c.segmento,
    c.setor,
    c.subsetor,
    ul.pct_utilizacao,
    ea.pct_descoberta,
    CAST(ROUND(ea.exposicao_total / 1000000, 2) AS DECIMAL(15,2)) AS exposicao_total_MM,
    cs.pct_concentracao AS concentracao_subsetor_pct
FROM clientes c
INNER JOIN utilizacao_limite ul     ON c.cliente_id = ul.cliente_id
INNER JOIN exposicao_atual ea       ON c.cliente_id = ea.cliente_id
INNER JOIN concentracao_subsetor cs ON c.subsetor   = cs.subsetor
INNER JOIN deterioracao_3m d        ON c.cliente_id = d.cliente_id
WHERE ul.pct_utilizacao > 80
  AND ea.pct_descoberta > 30
ORDER BY ea.exposicao_total DESC;


-- ----------------------------------------------------------------
-- QUESTÃO 2.5 | Análise Estatística Avançada
-- ----------------------------------------------------------------
-- OBJETIVO DE NEGÓCIO:
--   Entregar a analise estatistica completa pedida no case em quatro
--   frentes: (1) outliers via Z-Score por segmento, (2) distribuicao
--   por percentis e dispersao, (3) leitura temporal com media movel
--   de 3 meses e volatilidade mensal de ratings, e (4) correlacoes e
--   padroes anômalos entre score e utilizacao de limite.
--
-- TÉCNICAS UTILIZADAS:
--   Z-Score para detecção de outliers (threshold: 2 desvios padrão)
--   APPROX_PERCENTILE para P25 / P50 / P75 / P95 por segmento
--   STDDEV() e coeficiente de variacao por subsetor
--   AVG() OVER(ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) para MM3
--   STDDEV() sobre a serie mensal para volatilidade de ratings
--   Correlação de Pearson calculada via fórmula algébrica
--
-- RESULTADOS ENCONTRADOS:
--   OUTLIERS    : clientes com |Z| > 2 no snapshot mais recente
--   DISTRIBUICAO: percentis e CV permitem comparar heterogeneidade
--   TEMPORAL    : media movel 3M suaviza ruido de ratings individuais
--   CORRELACAO  : score x utilizacao permanece fraca no recorte
-- ----------------------------------------------------------------


WITH exposicao_base AS (
    SELECT e.cliente_id, e.exposicao_total, c.segmento, c.subsetor
    FROM exposicoes e
    INNER JOIN clientes c ON e.cliente_id = c.cliente_id
    INNER JOIN (
        SELECT cliente_id, MAX(data_referencia) AS ultima_data
        FROM exposicoes GROUP BY cliente_id
    ) ult ON e.cliente_id = ult.cliente_id
          AND e.data_referencia = ult.ultima_data
),
stats_segmento AS (
    SELECT segmento,
           AVG(exposicao_total)   AS media_seg,
           STDDEV(exposicao_total) AS desvio_seg
    FROM exposicao_base GROUP BY segmento
),
zscore AS (
    SELECT
        eb.cliente_id, eb.segmento,
        CAST(ROUND(eb.exposicao_total/1000000,2) AS DECIMAL(15,2)) AS exposicao_MM,
        CAST(ROUND((eb.exposicao_total-ss.media_seg)/NULLIF(ss.desvio_seg,0),2)
            AS DECIMAL(10,2))                                       AS z_score,
        CASE WHEN ABS((eb.exposicao_total-ss.media_seg)
                  /NULLIF(ss.desvio_seg,0)) > 2
             THEN 'OUTLIER' ELSE 'NORMAL' END                       AS flag_outlier
    FROM exposicao_base eb
    INNER JOIN stats_segmento ss ON eb.segmento = ss.segmento
),
distribuicao AS (
    SELECT segmento, COUNT(*) AS n_clientes,
        CAST(ROUND(MIN(exposicao_total)/1000000,2) AS DECIMAL(15,2)) AS minimo_MM,
        CAST(ROUND(MAX(exposicao_total)/1000000,2) AS DECIMAL(15,2)) AS maximo_MM,
        CAST(ROUND(AVG(exposicao_total)/1000000,2) AS DECIMAL(15,2)) AS media_MM,
        CAST(ROUND(STDDEV(exposicao_total)/1000000,2) AS DECIMAL(15,2)) AS desvio_MM,
        CAST(ROUND(STDDEV(exposicao_total)/NULLIF(AVG(exposicao_total),0)*100,1)
            AS DECIMAL(10,1))                                          AS coef_variacao_pct
    FROM exposicao_base GROUP BY segmento
),
rating_recente AS (
    SELECT r.cliente_id, r.score_interno FROM ratings r
    INNER JOIN (SELECT cliente_id, MAX(data_referencia) AS ult
                FROM ratings GROUP BY cliente_id) ult
        ON r.cliente_id=ult.cliente_id AND r.data_referencia=ult.ult
),
corr_base AS (
    SELECT CAST(rr.score_interno AS FLOAT) AS x,
           CAST(l.valor_utilizado/NULLIF(l.valor_limite,0)*100 AS FLOAT) AS y
    FROM rating_recente rr
    INNER JOIN limites l ON rr.cliente_id=l.cliente_id
    WHERE l.tipo_limite='Global'
),
correlacao AS (
    SELECT COUNT(*) AS n_obs,
        CAST(ROUND(
            (COUNT(*)*SUM(x*y)-SUM(x)*SUM(y)) /
            NULLIF(SQRT(COUNT(*)*SUM(x*x)-POWER(SUM(x),2)) *
                   SQRT(COUNT(*)*SUM(y*y)-POWER(SUM(y),2)),0)
        ,4) AS DECIMAL(10,4)) AS correlacao_pearson
    FROM corr_base
)
SELECT '1 - OUTLIERS' AS bloco, z.cliente_id AS dimensao, z.segmento,
       CAST(z.exposicao_MM AS VARCHAR(20)) AS valor1_exposicao_MM,
       CAST(z.z_score AS VARCHAR(20))      AS valor2_z_score,
       z.flag_outlier                      AS classificacao
FROM zscore z WHERE z.flag_outlier='OUTLIER'
UNION ALL
SELECT '2 - DISTRIBUICAO', d.segmento, d.segmento,
       CAST(d.media_MM AS VARCHAR(20)),
       CAST(d.coef_variacao_pct AS VARCHAR(20)),
       CAST(d.n_clientes AS VARCHAR(20))
FROM distribuicao d
UNION ALL
SELECT '3 - CORRELACAO', 'Score x Utilizacao Limite', 'Todos os segmentos',
       CAST(c.correlacao_pearson AS VARCHAR(20)),
       CAST(c.n_obs AS VARCHAR(20)),
       CASE WHEN ABS(c.correlacao_pearson)<0.2 THEN 'FRACA'
            WHEN ABS(c.correlacao_pearson)<0.5 THEN 'MODERADA'
            ELSE 'FORTE' END
FROM correlacao c
ORDER BY bloco, dimensao;


-- BLOCO COMPLEMENTAR 2.5-A | Percentis de exposicao por segmento
WITH exposicao_atual AS (
    SELECT e.cliente_id, e.exposicao_total, c.segmento, c.subsetor
    FROM exposicoes e
    INNER JOIN clientes c ON e.cliente_id = c.cliente_id
    INNER JOIN (
        SELECT cliente_id, MAX(data_referencia) AS ultima_data
        FROM exposicoes
        GROUP BY cliente_id
    ) ult ON e.cliente_id = ult.cliente_id
          AND e.data_referencia = ult.ultima_data
)
SELECT
    segmento,
    CAST(ROUND(APPROX_PERCENTILE(exposicao_total, 0.25) / 1000000, 2) AS DECIMAL(15,2)) AS p25_exposicao_MM,
    CAST(ROUND(APPROX_PERCENTILE(exposicao_total, 0.50) / 1000000, 2) AS DECIMAL(15,2)) AS p50_exposicao_MM,
    CAST(ROUND(APPROX_PERCENTILE(exposicao_total, 0.75) / 1000000, 2) AS DECIMAL(15,2)) AS p75_exposicao_MM,
    CAST(ROUND(APPROX_PERCENTILE(exposicao_total, 0.95) / 1000000, 2) AS DECIMAL(15,2)) AS p95_exposicao_MM
FROM exposicao_atual
GROUP BY segmento
ORDER BY segmento;

-- BLOCO COMPLEMENTAR 2.5-B | Media movel de rating (3 meses) por cliente
WITH rating_ordenado AS (
    SELECT
        r.cliente_id,
        r.data_referencia,
        c.segmento,
        r.rating_interno,
        r.score_interno,
        CAST(ROUND(AVG(r.score_interno) OVER (
            PARTITION BY r.cliente_id
            ORDER BY r.data_referencia
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ), 2) AS DECIMAL(10,2)) AS media_movel_score_3m
    FROM ratings r
    INNER JOIN clientes c ON r.cliente_id = c.cliente_id
)
SELECT *
FROM rating_ordenado
ORDER BY cliente_id, data_referencia;

-- BLOCO COMPLEMENTAR 2.5-C | Volatilidade mensal de ratings por segmento
WITH rating_mensal AS (
    SELECT
        DATE_FORMAT(r.data_referencia, '%Y-%m') AS ano_mes,
        c.segmento,
        AVG(r.score_interno) AS score_medio_mes
    FROM ratings r
    INNER JOIN clientes c ON r.cliente_id = c.cliente_id
    GROUP BY DATE_FORMAT(r.data_referencia, '%Y-%m'), c.segmento
)
SELECT
    segmento,
    CAST(ROUND(AVG(score_medio_mes), 2) AS DECIMAL(10,2)) AS score_medio_periodo,
    CAST(ROUND(STDDEV(score_medio_mes), 2) AS DECIMAL(10,2)) AS volatilidade_mensal,
    COUNT(*) AS qtd_meses
FROM rating_mensal
GROUP BY segmento
ORDER BY volatilidade_mensal DESC;


-- ----------------------------------------------------------------
-- QUESTÃO 2.6-A | Concentração Setorial vs. Limite Regulatório
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


WITH exposicao_atual AS (
    SELECT e.cliente_id, e.exposicao_total FROM exposicoes e
    INNER JOIN (SELECT cliente_id, MAX(data_referencia) AS ultima_data
                FROM exposicoes GROUP BY cliente_id) ult
        ON e.cliente_id=ult.cliente_id AND e.data_referencia=ult.ultima_data
),
total_portfolio AS (SELECT SUM(exposicao_total) AS total FROM exposicao_atual)
SELECT
    c.setor,
    COUNT(DISTINCT c.cliente_id)                           AS qtd_clientes,
    CAST(ROUND(SUM(ea.exposicao_total)/1000000,1)
        AS DECIMAL(15,1))                                  AS exposicao_MM,
    CAST(ROUND(SUM(ea.exposicao_total)/tp.total*100,1)
        AS DECIMAL(10,1))                                  AS pct_portfolio,
    CASE
        WHEN SUM(ea.exposicao_total)/tp.total*100 > 15 THEN 'ACIMA DO LIMITE'
        WHEN SUM(ea.exposicao_total)/tp.total*100 > 10 THEN 'MONITORAMENTO'
        ELSE 'NORMAL'
    END                                                    AS status_regulatorio
FROM clientes c
INNER JOIN exposicao_atual ea ON c.cliente_id = ea.cliente_id
CROSS JOIN total_portfolio tp
GROUP BY c.setor, tp.total
ORDER BY pct_portfolio DESC;


-- ----------------------------------------------------------------
-- QUESTÃO 2.6-B | Operações Vencidas: Risco Materializado
-- ----------------------------------------------------------------
-- OBJETIVO: Identificar operações vencidas e não liquidadas,
--   cruzando com perfil de risco para priorizar cobrança.
--
-- TÉCNICAS UTILIZADAS:
--   INNER JOIN com rating mais recente via subquery de MAX()
--   DATEDIFF para calcular dias em atraso em relação à data atual
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


WITH rating_atual AS (
    SELECT r.cliente_id, r.rating_interno, r.score_interno, r.pd_12m
    FROM ratings r
    INNER JOIN (SELECT cliente_id, MAX(data_referencia) AS ult
                FROM ratings GROUP BY cliente_id) ult
        ON r.cliente_id=ult.cliente_id AND r.data_referencia=ult.ult
)
SELECT
    o.operacao_id, o.cliente_id, c.setor, c.segmento,
    o.produto, o.modalidade,
    CAST(ROUND(o.valor_aprovado/1000000,2)  AS DECIMAL(15,2)) AS valor_aprovado_MM,
    CAST(ROUND(o.valor_utilizado/1000000,2) AS DECIMAL(15,2)) AS valor_utilizado_MM,
    o.data_vencimento,
    DATE_DIFF('day', o.data_vencimento, CURRENT_DATE)                AS dias_vencido,
    o.garantia_tipo,
    ra.rating_interno, ra.score_interno,
    CAST(ra.pd_12m * 100 AS DECIMAL(10,4))                     AS pd_12m_pct
FROM operacoes o
INNER JOIN clientes c      ON o.cliente_id = c.cliente_id
INNER JOIN rating_atual ra ON o.cliente_id = ra.cliente_id
WHERE o.status_operacao = 'Vencida'
ORDER BY valor_utilizado_MM DESC;


-- ----------------------------------------------------------------
-- QUESTÃO 2.6-C | Score Médio por Segmento e Porte
-- ----------------------------------------------------------------
-- OBJETIVO: Entender como a qualidade de crédito se distribui
--   entre segmentos e portes para calibrar apetite de crédito.
--
-- TÉCNICAS UTILIZADAS:
--   INNER JOIN com rating mais recente via subquery de MAX()
--   AVG e STDDEV para medir qualidade e homogeneidade por grupo (sintaxe Athena)
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


WITH rating_atual AS (
    SELECT r.cliente_id, r.score_interno, r.rating_interno FROM ratings r
    INNER JOIN (SELECT cliente_id, MAX(data_referencia) AS ult
                FROM ratings GROUP BY cliente_id) ult
        ON r.cliente_id=ult.cliente_id AND r.data_referencia=ult.ult
)
SELECT
    c.segmento, c.porte,
    COUNT(DISTINCT c.cliente_id)                       AS qtd_clientes,
    CAST(ROUND(AVG(CAST(ra.score_interno AS FLOAT)),0)
        AS INT)                                        AS score_medio,
    CAST(ROUND(MIN(ra.score_interno),0) AS INT)        AS score_minimo,
    CAST(ROUND(MAX(ra.score_interno),0) AS INT)        AS score_maximo,
    CAST(ROUND(STDDEV(CAST(ra.score_interno AS FLOAT)),0)
        AS INT)                                        AS desvio_padrao,
    CASE
        WHEN AVG(CAST(ra.score_interno AS FLOAT)) >= 850 THEN 'EXCELENTE'
        WHEN AVG(CAST(ra.score_interno AS FLOAT)) >= 800 THEN 'MUITO BOM'
        WHEN AVG(CAST(ra.score_interno AS FLOAT)) >= 750 THEN 'ATENCAO'
        WHEN AVG(CAST(ra.score_interno AS FLOAT)) >= 650 THEN 'CUIDADO'
        ELSE 'CRITICO'
    END                                                AS classificacao_score
FROM clientes c
INNER JOIN rating_atual ra ON c.cliente_id = ra.cliente_id
WHERE c.status_cliente = 'Ativo'
GROUP BY c.segmento, c.porte
ORDER BY score_medio DESC;

-- ================================================================
-- FIM DO SCRIPT — CASE TÉCNICO CRÉDITO IBBA
-- ================================================================


-- ================================================================
-- SEÇÃO 3 — VIEWS ANALÍTICAS OBRIGATÓRIAS
-- Camada semântica para consumo direto pelo Amazon QuickSight.
-- Cada view é criada no AWS Glue Data Catalog via Athena.
-- No QuickSight: conectar ao Athena e selecionar as views como datasets.
-- ================================================================


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
CREATE OR REPLACE VIEW vw_exposicao_enriquecida AS
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
CREATE OR REPLACE VIEW vw_kpi_exposicao AS
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
CREATE OR REPLACE VIEW vw_kpi_ratings AS
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
CREATE OR REPLACE VIEW vw_kpi_limites AS
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
CREATE OR REPLACE VIEW vw_kpi_operacoes AS
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
CREATE OR REPLACE VIEW vw_kpi_risco AS
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
    (SELECT subsetor FROM conc_subsetor
     ORDER BY pct_concentracao DESC
     LIMIT 1)                   AS subsetor_mais_concentrado,

    -- Clientes com utilização > 75% do limite Global
    (SELECT qtd FROM clientes_alerta)                  AS clientes_em_atencao,

    -- Semáforo concentração regulatória
    CASE
        WHEN MAX(cs.pct_concentracao) > 15 THEN 'ACIMA DO LIMITE'
        WHEN MAX(cs.pct_concentracao) > 10 THEN 'MONITORAMENTO'
        ELSE 'NORMAL'
    END                                                AS status_concentracao
FROM conc_subsetor cs;


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
CREATE OR REPLACE VIEW vw_exposicao_por_subsetor AS
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
-- Nota: ORDER BY não é garantido em views no Athena — use ORDER BY na query de consumo
-- Use ORDER BY na query de consumo: SELECT * FROM vw_exposicao_por_subsetor ORDER BY exposicao_total_subsetor DESC
;


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
--   DATE_FORMAT() para agrupar por ano-mês (sintaxe Athena)
--   LAG() OVER(PARTITION BY) para comparação mês a mês
--   RANK() OVER(PARTITION BY) para ranking mensal entre segmentos
--
-- RESULTADO: 36 linhas — 3 segmentos x 12 meses
-- INSIGHT:   Corporate ficou em 3º lugar em todos os 12 meses
--            com score médio ~762 — faixa ATENÇÃO (750-799)
--            Middle Market liderou o ranking durante todo o período
-- ----------------------------------------------------------------
CREATE OR REPLACE VIEW vw_score_por_segmento_mes AS
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
        DATE_FORMAT(r.data_referencia, '%Y-%m')                      AS ano_mes,
        r.data_referencia,
        CAST(ROUND(AVG(CAST(er.nota AS FLOAT)), 2) AS DECIMAL(10,2))  AS nota_media_segmento,
        CAST(ROUND(AVG(CAST(r.score_interno AS FLOAT)), 0) AS INT)     AS score_medio_segmento,
        COUNT(DISTINCT r.cliente_id)                                   AS qtd_clientes
    FROM ratings r
        INNER JOIN clientes c       ON r.cliente_id     = c.cliente_id
        INNER JOIN escala_rating er ON r.rating_interno = er.rating
    GROUP BY c.segmento,
             DATE_FORMAT(r.data_referencia, '%Y-%m'),
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
-- RESULTADO:
--   - A view preserva a camada operacional (> 75% OU > 25% OU deterioracao)
--   - Também explicita a regra estrita do case por flags dedicadas
--   - Nenhum cliente atingiu ALTO RISCO no snapshot final com a regra
--     estrita, pois pct_descoberta ficou em 25,0% no recorte tratado
-- ----------------------------------------------------------------
CREATE OR REPLACE VIEW vw_clientes_em_atencao AS
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

    -- Regra estrita do case
    CASE
        WHEN d.cliente_id IS NOT NULL
             AND ul.pct_utilizacao > 80
             AND ea.pct_descoberta > 30 THEN 'SIM'
        ELSE 'NAO'
    END                                            AS flag_alto_risco_case,

    -- Camada operacional de monitoramento
    CASE
        WHEN ul.pct_utilizacao > 75
          OR ea.pct_descoberta > 25
          OR d.cliente_id IS NOT NULL THEN 'SIM'
        ELSE 'NAO'
    END                                            AS flag_atencao_monitoramento,

    CASE
        WHEN d.cliente_id IS NOT NULL
             AND ul.pct_utilizacao > 80
             AND ea.pct_descoberta > 30 THEN 'ALTO RISCO'
        WHEN ul.pct_utilizacao > 75
          OR ea.pct_descoberta > 25
          OR d.cliente_id IS NOT NULL THEN 'ATENCAO'
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
--   STDDEV() para desvio padrão por segmento
--   Coeficiente de variação para medir dispersão relativa
--   INNER JOIN para cruzar exposição com estatísticas do segmento
--
-- RESULTADOS ENCONTRADOS:
--   OUTLIERS    : CLI862 (Z=2.49), CLI402 (Z=2.70), CLI524 (Z=2.25)
--   DISTRIBUICAO: Middle Market CV=155.7% — maior heterogeneidade
--   CORRELACAO  : Pearson=0.0176 (FRACA) — limites seguem critérios
--                 estratégicos além do score isolado
-- ----------------------------------------------------------------
CREATE OR REPLACE VIEW vw_analise_estatistica AS
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
        STDDEV(exposicao_total) AS desvio_seg,
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
CREATE OR REPLACE VIEW vw_exposicao_por_setor AS
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
CREATE OR REPLACE VIEW vw_operacoes_vencidas AS
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
    DATE_DIFF('day', o.data_vencimento, CURRENT_DATE)                    AS dias_vencido,

    -- Faixa de atraso para agrupamento e relatórios
    CASE
        WHEN DATE_DIFF('day', o.data_vencimento, CURRENT_DATE) <= 30  THEN '1 - Ate 30 dias'
        WHEN DATE_DIFF('day', o.data_vencimento, CURRENT_DATE) <= 90  THEN '2 - 31 a 90 dias'
        WHEN DATE_DIFF('day', o.data_vencimento, CURRENT_DATE) <= 365 THEN '3 - 91 a 365 dias'
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


-- ----------------------------------------------------------------
-- QUESTÃO 2.6-C | vw_score_por_segmento_porte
-- ----------------------------------------------------------------
-- OBJETIVO: Entender como a qualidade de crédito se distribui
--   entre segmentos e portes para calibrar apetite de crédito.
--
-- TÉCNICAS UTILIZADAS:
--   INNER JOIN com rating mais recente via subquery de MAX()
--   AVG e STDDEV para medir qualidade e homogeneidade por grupo (sintaxe Athena)
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
CREATE OR REPLACE VIEW vw_score_por_segmento_porte AS
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
    CAST(ROUND(STDDEV(CAST(ra.score_interno AS FLOAT)), 0) AS INT)       AS desvio_padrao,

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


-- ----------------------------------------------------------------
-- QUESTÃO 2.5 | Views complementares para aderência integral ao case
-- ----------------------------------------------------------------

CREATE OR REPLACE VIEW vw_percentis_exposicao_segmento AS
WITH exposicao_atual AS (
    SELECT e.cliente_id, e.exposicao_total, c.segmento
    FROM exposicoes e
    INNER JOIN clientes c ON e.cliente_id = c.cliente_id
    INNER JOIN (
        SELECT cliente_id, MAX(data_referencia) AS ultima_data
        FROM exposicoes
        GROUP BY cliente_id
    ) ult ON e.cliente_id = ult.cliente_id
          AND e.data_referencia = ult.ultima_data
)
SELECT
    segmento,
    CAST(ROUND(APPROX_PERCENTILE(exposicao_total, 0.25) / 1000000, 2) AS DECIMAL(15,2)) AS p25_exposicao_MM,
    CAST(ROUND(APPROX_PERCENTILE(exposicao_total, 0.50) / 1000000, 2) AS DECIMAL(15,2)) AS p50_exposicao_MM,
    CAST(ROUND(APPROX_PERCENTILE(exposicao_total, 0.75) / 1000000, 2) AS DECIMAL(15,2)) AS p75_exposicao_MM,
    CAST(ROUND(APPROX_PERCENTILE(exposicao_total, 0.95) / 1000000, 2) AS DECIMAL(15,2)) AS p95_exposicao_MM
FROM exposicao_atual
GROUP BY segmento;

CREATE OR REPLACE VIEW vw_rating_media_movel_3m AS
SELECT
    r.cliente_id,
    c.segmento,
    r.data_referencia,
    r.rating_interno,
    r.score_interno,
    CAST(ROUND(AVG(r.score_interno) OVER (
        PARTITION BY r.cliente_id
        ORDER BY r.data_referencia
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ), 2) AS DECIMAL(10,2)) AS media_movel_score_3m
FROM ratings r
INNER JOIN clientes c ON r.cliente_id = c.cliente_id;

CREATE OR REPLACE VIEW vw_volatilidade_rating_segmento AS
WITH rating_mensal AS (
    SELECT
        DATE_FORMAT(r.data_referencia, '%Y-%m') AS ano_mes,
        c.segmento,
        AVG(r.score_interno) AS score_medio_mes
    FROM ratings r
    INNER JOIN clientes c ON r.cliente_id = c.cliente_id
    GROUP BY DATE_FORMAT(r.data_referencia, '%Y-%m'), c.segmento
)
SELECT
    segmento,
    CAST(ROUND(AVG(score_medio_mes), 2) AS DECIMAL(10,2)) AS score_medio_periodo,
    CAST(ROUND(STDDEV(score_medio_mes), 2) AS DECIMAL(10,2)) AS volatilidade_mensal,
    COUNT(*) AS qtd_meses
FROM rating_mensal
GROUP BY segmento;


-- ================================================================
-- VERIFICAÇÃO FINAL — Confirmar criação e retorno de todas as views
-- ================================================================
-- Observação:
--   A prototipagem foi executada em SQL Server e convertida para Athena.
--   Em Athena, nao utilizar sys.views. Quando o catalog/schema permitir,
--   a listagem de views pode ser feita por INFORMATION_SCHEMA.
SELECT table_schema, table_name
FROM information_schema.views
WHERE table_name LIKE 'vw_%'
ORDER BY table_schema, table_name;

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
SELECT 'vw_analise_estatistica',                   COUNT(*) FROM vw_analise_estatistica    UNION ALL
SELECT 'vw_percentis_exposicao_segmento',           COUNT(*) FROM vw_percentis_exposicao_segmento UNION ALL
SELECT 'vw_rating_media_movel_3m',                  COUNT(*) FROM vw_rating_media_movel_3m  UNION ALL
SELECT 'vw_volatilidade_rating_segmento',           COUNT(*) FROM vw_volatilidade_rating_segmento;

-- ================================================================
-- FIM DO SCRIPT — VIEWS ANALÍTICAS CRÉDITO CORPORATIVO
-- ================================================================


-- ================================================================
-- SEÇÃO 4 — VIEWS ADICIONAIS
-- Análises além do escopo obrigatório do case.
-- Demonstram profundidade analítica em crédito corporativo:
-- pricing de risco, qualidade de colateral e migração de rating.
-- ================================================================


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
CREATE OR REPLACE VIEW vw_mix_garantias AS
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
CREATE OR REPLACE VIEW vw_migracao_rating AS
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
CREATE OR REPLACE VIEW vw_pricing_por_rating AS
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


-- ================================================================
-- VERIFICAÇÃO — Testar as 3 views adicionais
-- ================================================================
SELECT 'vw_mix_garantias'     AS view_name, COUNT(*) AS linhas FROM vw_mix_garantias    UNION ALL
SELECT 'vw_migracao_rating',               COUNT(*) FROM vw_migracao_rating             UNION ALL
SELECT 'vw_pricing_por_rating',            COUNT(*) FROM vw_pricing_por_rating;

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
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) AS pct
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

-- ================================================================
-- FIM DO SCRIPT — VIEWS ADICIONAIS
-- ================================================================
