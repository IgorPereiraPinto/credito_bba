# 🏦 Case: Analista de Dados - Crédito IBBA

## 📋 Objetivo do Case

Avaliar candidatos para a posição de **Analista de Dados no Crédito IBBA** testando:

- 🔍 **Conhecimentos de SQL**: Consultas complexas, joins, agregações, window functions
- 📊 **Habilidades em QuickSight**: Dashboards, KPIs, visualizações interativas
- 🗂️ **Entendimento de Metadados**: Interpretação de estruturas e relacionamentos
- 💰 **Conceitos de Crédito**: Exposições, ratings, limites, subsetor
- 🧠 **Capacidade Analítica**: Interpretação de resultados e insights de negócio

---

## 🎯 Contextualização do Negócio

Você foi contratado como **Analista de Dados no Crédito IBBA** e precisa analisar o portfólio de clientes corporativos. Sua missão é:

1. **Avaliar exposições** de crédito por cliente e segmento
2. **Monitorar ratings** e sua evolução temporal
3. **Analisar limites** aprovados vs utilizados
4. **Identificar concentrações** em subsetor
5. **Criar dashboards** executivos para tomada de decisão

---

## 🗄️ Estrutura dos Dados

### Tabelas Disponíveis

#### 1. `clientes` - Cadastro de Clientes
```sql
CREATE TABLE clientes (
    cliente_id VARCHAR(10) PRIMARY KEY,
    segmento VARCHAR(20),
    porte VARCHAR(10),
    setor VARCHAR(30),
    sub_setor VARCHAR(50),
    data_inicio_relacionamento DATE,
    regiao VARCHAR(15),
    status_cliente VARCHAR(10)
);
```

#### 2. `operacoes` - Operações de Crédito
```sql
CREATE TABLE operacoes (
    operacao_id VARCHAR(15) PRIMARY KEY,
    cliente_id VARCHAR(10),
    produto VARCHAR(30),
    modalidade VARCHAR(20),
    valor_aprovado DECIMAL(15,2),
    valor_utilizado DECIMAL(15,2),
    taxa_juros DECIMAL(8,4),
    prazo_meses INTEGER,
    data_aprovacao DATE,
    data_vencimento DATE,
    garantia_tipo VARCHAR(20),
    status_operacao VARCHAR(15),
    FOREIGN KEY (cliente_id) REFERENCES clientes(cliente_id)
);
```

#### 3. `ratings` - Histórico de Ratings
```sql
CREATE TABLE ratings (
    cliente_id VARCHAR(10),
    data_referencia DATE,
    rating_interno VARCHAR(5),
    rating_externo VARCHAR(5),
    pd_12m DECIMAL(8,6),
    score_interno INTEGER,
    observacao VARCHAR(100),
    PRIMARY KEY (cliente_id, data_referencia),
    FOREIGN KEY (cliente_id) REFERENCES clientes(cliente_id)
);
```

#### 4. `limites` - Limites de Crédito
```sql
CREATE TABLE limites (
    cliente_id VARCHAR(10),
    tipo_limite VARCHAR(20),
    valor_limite DECIMAL(15,2),
    valor_utilizado DECIMAL(15,2),
    data_aprovacao DATE,
    data_revisao DATE,
    aprovador VARCHAR(50),
    status_limite VARCHAR(10),
    PRIMARY KEY (cliente_id, tipo_limite),
    FOREIGN KEY (cliente_id) REFERENCES clientes(cliente_id)
);
```

#### 5. `exposicoes` - Exposições Consolidadas
```sql
CREATE TABLE exposicoes (
    cliente_id VARCHAR(10),
    data_referencia DATE,
    exposicao_total DECIMAL(15,2),
    exposicao_garantida DECIMAL(15,2),
    exposicao_descoberta DECIMAL(15,2),
    provisao_necessaria DECIMAL(15,2),
    classificacao_risco VARCHAR(2),
    PRIMARY KEY (cliente_id, data_referencia),
    FOREIGN KEY (cliente_id) REFERENCES clientes(cliente_id)
);
```

---

## 🗂️ PARTE 1: Análise de Metadados

### Questão 1.1: Interpretação da Estrutura
**Analise o modelo de dados apresentado e responda:**

a) Identifique os **relacionamentos** entre as tabelas
b) Quais são as **chaves primárias e estrangeiras**?
c) Que **informações sensíveis** devem ter sido omitidas na geração do case?
d) Como você interpretaria a diferença entre `exposicao_total` e `exposicao_descoberta`?

### Questão 1.2: Qualidade dos Dados
**Baseado na estrutura, liste 5 verificações de qualidade que você implementaria:**

Exemplo: Verificar se `valor_utilizado` ≤ `valor_aprovado` na tabela operações

---

## 💻 PARTE 2: Consultas SQL

### Questão 2.1: Consulta Básica 
```sql
-- Escreva uma consulta que retorne:
-- - Nome do cliente (cliente_id)
-- - Segmento
-- - Total de operações ativas
-- - Soma do valor aprovado
-- Ordenado pelo valor aprovado (decrescente)
```

### Questão 2.2: Joins e Agregações 
```sql
-- Crie uma consulta que mostre para cada subsetor:
-- - Quantidade de clientes
-- - Exposição total média
-- - Maior exposição individual
-- - % de exposição descoberta sobre o total
-- Apenas para subsetor com mais de 5 clientes
```

### Questão 2.3: Window Functions 
```sql
-- Desenvolva uma query que identifique:
-- - Evolução mensal do rating médio por segmento
-- - Variação percentual em relação ao mês anterior
-- - Ranking dos segmentos por rating no último mês
-- Use window functions adequadas
```

### Questão 2.4: Análise de Risco 
```sql
-- Crie uma consulta que identifique clientes com:
-- - Deterioração de rating (piora nos últimos 3 meses)
-- - Utilização de limite > 80%
-- - Exposição descoberta > 30% da exposição total
-- Inclua métricas de concentração por subsetor
```

### Questão 2.5: Análise Estatística Avançada 
```sql
-- Desenvolva uma análise estatística completa que inclua:
-- 1. DETECÇÃO DE OUTLIERS:
--    - Z-Score para identificar clientes com exposição anômala (>2 desvios padrão)
--    - Análise de outliers por segmento

-- 2. ANÁLISE DE DISTRIBUIÇÃO:
--    - Percentis P25, P50 (mediana), P75, P95 de exposições por segmento
--    - Desvio padrão e coeficiente de variação por subsetor

-- 3. ANÁLISE TEMPORAL:
--    - Média móvel de ratings (3 meses) por cliente
--    - Volatilidade mensal de ratings por segmento

-- 4. CORRELAÇÕES E PADRÕES:
--    - Correlação entre score_interno e % utilização de limite
--    - Identificação de comportamentos estatisticamente anômalos

-- Use CTEs, funções estatísticas (STDDEV, PERCENTILE_CONT) e window functions
-- Apresente resultados consolidados por tipo de análise
```
### Questão 2.6: Quais os principais análises geradas a partir da análise dos dados em questão? Encontrou algo nos dados além do que foi construído acima?

---

## 🎯 BENCHMARKS E MÉTRICAS

### Parâmetros Fictícios para Avaliação de KPIs

Para suas análises, utilize os seguintes **benchmarks fictícios**:

#### **📊 Rating Ponderado (Escala 0-1000):**
- **850-1000**: ✅ **EXCELENTE** - Portfólio premium (AAA/AA)
- **800-849**: ✅ **MUITO BOM** - Meta interna alcançada (A+/A)
- **750-799**: ⚠️ **ATENÇÃO** - Monitoramento necessário (A-/BBB+)
- **650-749**: ❌ **CUIDADO** - Revisão de estratégia (BBB/BBB-)
- **<650**: 🚨 **CRÍTICO** - Ação imediata necessária

#### **💰 KPIs Operacionais:**
- **Utilização de Limites**: 60-75% (ideal), >85% (alerta)
- **Exposição Descoberta**: <30% (meta), >40% (atenção)
- **Concentração Subsetor**: <15% (limite regulatório), >10% (monitoramento)

#### **🚨 Alertas de Risco (Critérios Combinados):**
- **ALTO RISCO**: Utilização >80% + Exposição descoberta >30%
- **ATENÇÃO**: Utilização >75% OU Exposição descoberta >25%
- **DETERIORAÇÃO**: Rating decaindo por 2+ meses consecutivos

#### **⚡ Frequência de Revisão:**
- **Ratings**: Mensal (mínimo trimestral)
- **Limites**: Semestral (ou por evento)
- **Exposições**: Diário (consolidação mensal)

> **💡 Dica**: Use estes benchmarks para interpretar métricas e definir status dos KPIs em seus dashboards.

---

## 📊 PARTE 3: Dashboard QuickSight

### Cenário
Você precisa criar um dashboard executivo para o **Comitê de Crédito** apresentar mensalmente.

### Questão 3.1: Definição de KPIs
**Liste 8 KPIs essenciais para um dashboard de crédito IBBA, incluindo:**
- Métrica
- Fórmula de cálculo
- Meta/benchmark *(use os benchmarks fornecidos acima)*
- Frequência de atualização
- Status visual (cores/ícones para cada faixa)

### Questão 3.2: Estrutura do Dashboard
**Desenhe (ou descreva detalhadamente) a estrutura de 3 abas:**

**Proposta não obrigatória:**
#### Aba 1: Visão Geral
- KPIs principais
- Gráficos de tendência
- Filtros disponíveis

#### Aba 2: Análise de Riscos
- Distribuição por rating
- Concentração por subsetor
- Alertas de deterioração

#### Aba 3: Performance de Limites
- Utilização vs aprovado
- Análise temporal
- Drill-down por produto

### Questão 3.3: Alertas e Ações
**Configure 5 alertas automáticos no QuickSight que disparariam ações do negócio:**

*Use os critérios de alerta definidos nos benchmarks acima.*

Exemplo: "Se concentração em um subsetor > 15% do portfólio → Alert para diversificação"

---

## 🎯 PARTE 4: Análise de Negócio

### Cenário: Análise de Concentração
Você identificou que 3 subsetores que representam 45% da exposição total do banco.

### Questão 4.1: Riscos Identificados
**Liste os principais riscos dessa concentração e suas possíveis consequências**

### Questão 4.2: Plano de Ação
**Proponha 5 ações concretas para mitigar esses riscos**

### Questão 4.3: Métricas de Monitoramento
**Defina métricas para acompanhar a eficácia das ações propostas**

---

## 🔥 PARTE 5: Simulação de Crise

### Questão 5.1: Situação de Crise
**Cenário**: Uma crise setorial afetou o segmento "Varejo" que representa 12.9% do seu portfólio (7 clientes, R$ 2.2 bilhões).

**Sua missão:**
1. Quais queries SQL executaria primeiro?
2. Que visualizações criaria no QuickSight?
3. Que informações levaria para a reunião de crise?
4. Proposta de monitoramento intensivo

### Questão 5.2: Automatização
**Descreva como automatizaria o processo de:**
1. Coleta de dados
2. Cálculo de métricas
3. Geração de alertas
4. Distribuição de relatórios

## 📋 Entregáveis Esperados

1. **Documento com respostas** das questões teóricas
2. **Scripts SQL** comentados e testáveis
3. **Mockup/wireframe** dos dashboards QuickSight
4. **Apresentação** sobre os principais insights
5. **Plano de implementação** das melhorias sugeridas

---

*Este case foi desenvolvido para avaliar de forma abrangente as competências necessárias para um Analista de Dados no Crédito IBBA, simulando cenários reais com dados sintéticos.*