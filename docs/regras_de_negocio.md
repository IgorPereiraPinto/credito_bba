# Regras de Negócio — analise_de_credito

## KPIs e benchmarks

### Score ponderado do portfólio (escala 0-1000)

Calcula o score médio da carteira ponderado pela exposição de cada cliente.
Clientes com maior exposição têm mais peso na média.

```
Score Ponderado = Σ(score_interno × exposicao_total) / Σ(exposicao_total)
```

**Benchmarks internos:**

| Faixa       | Status      | Ação recomendada                        |
|-------------|-------------|-----------------------------------------|
| 850 – 1000  | EXCELENTE   | Manter estratégia                       |
| 800 – 849   | MUITO BOM   | Meta interna alcançada                  |
| 750 – 799   | ATENÇÃO     | Monitoramento mensal reforçado          |
| 650 – 749   | CUIDADO     | Revisão de estratégia de carteira       |
| < 650       | CRÍTICO     | Ação imediata — Comitê de Crédito       |

---

### Taxa de utilização de limite

```
% Utilização = valor_utilizado / valor_limite × 100
```

**Benchmarks:**

| Faixa         | Status   | Interpretação                              |
|---------------|----------|--------------------------------------------|
| 60% – 75%     | IDEAL    | Limite bem dimensionado e utilizado        |
| 75% – 85%     | ATENÇÃO  | Revisão de limite recomendada              |
| > 85%         | ALERTA   | Risco de estouro — revisão urgente         |
| < 40%         | SUBUTILIZADO | Revisar necessidade ou ajustar limite  |

---

### Exposição descoberta

```
Exposição Descoberta = Exposição Total − Exposição Garantida
% Descoberta = Exposição Descoberta / Exposição Total × 100
```

**Benchmarks:**

| Faixa    | Status   | Interpretação                                |
|----------|----------|----------------------------------------------|
| < 30%    | META     | Cobertura de garantias adequada              |
| 30%–40%  | ATENÇÃO  | Reforço de garantias recomendado             |
| > 40%    | CRÍTICO  | Risco elevado sem cobertura adequada         |

---

### Concentração por subsetor

```
% Concentração = Exposição Subsetor / Exposição Total Portfólio × 100
```

**Benchmarks regulatórios:**

| Faixa   | Status               | Base regulatória                            |
|---------|----------------------|---------------------------------------------|
| < 10%   | OK                   | Portfólio bem diversificado                 |
| 10%–15% | MONITORAMENTO        | Acompanhamento mensal recomendado           |
| > 15%   | LIMITE REGULATÓRIO   | Ação obrigatória — risco sistêmico elevado  |

---

## Regras de validação de dados

Aplicadas em `python/03_validate.py`:

| Regra                                          | Tabela    | Severidade | Ação           |
|------------------------------------------------|-----------|------------|----------------|
| `valor_utilizado` ≤ `valor_aprovado`           | operacoes | warning    | Registra       |
| `exposicao_descoberta` = `total` − `garantida` | exposicoes| warning    | Registra       |
| `pd_12m` entre 0 e 1                           | ratings   | error      | Remove linha   |
| `data_vencimento` ≥ `data_aprovacao`           | operacoes | error      | Remove linha   |
| `valor_limite` > 0                             | limites   | error      | Remove linha   |
| Unicidade de PK por tabela                     | todas     | warning    | Remove duplicata|

---

## Escala de rating numérica

Usada nas views `vw_stage_escala_rating` para habilitar cálculos quantitativos.

| Rating | Nota | Categoria          |
|--------|------|--------------------|
| AAA    | 17   | Investment Grade   |
| AA+    | 16   | Investment Grade   |
| AA     | 15   | Investment Grade   |
| AA-    | 14   | Investment Grade   |
| A+     | 13   | Investment Grade   |
| A      | 12   | Investment Grade   |
| A-     | 11   | Investment Grade   |
| BBB+   | 10   | Investment Grade   |
| BBB    | 9    | Investment Grade   |
| BBB-   | 8    | Investment Grade   |
| BB+    | 7    | Sub-Investment     |
| BB     | 6    | Sub-Investment     |
| BB-    | 5    | Sub-Investment     |
| B+     | 4    | Sub-Investment     |
| B      | 3    | Sub-Investment     |
| B-     | 2    | Sub-Investment     |
| C      | 1    | Distressed         |

---

## Critérios de alerta combinado (Matriz de Risco)

Definidos em `vw_matriz_risco`:

**ALTO_RISCO:** 2 ou mais dos 3 flags ativos simultaneamente:

- Flag 1: Deterioração de rating por 2+ meses consecutivos
- Flag 2: Utilização de limite > 80%
- Flag 3: Exposição descoberta > 30%

**ATENÇÃO:** 1 flag ativo:

- Utilização > 75% OU Exposição descoberta > 25%

---

## Classificação de risco (CMN 2.682)

Base para o campo `classificacao_risco` em `exposicoes`:

| Classificação | PD aproximada | Provisão mínima |
|---------------|---------------|-----------------|
| AA            | ~0%           | 0%              |
| A             | 0,5%          | 0,5%            |
| B             | 1%            | 1%              |
| C             | 3%            | 3%              |
| D             | 10%           | 10%             |
| E             | 30%           | 30%             |
| F             | 50%           | 50%             |
| G             | 70%           | 70%             |
| H             | 100%          | 100%            |
