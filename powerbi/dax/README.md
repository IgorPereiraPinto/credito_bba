# Power BI DAX - analise_de_credito

Pacote de DAX alinhado ao layout atual do dashboard do projeto.

Arquivos desta pasta:

- `medidas_credito_bba.dax`
  Conjunto principal de medidas para cards, graficos e status de KPI
- `tabela_calendario.dax`
  Tabela calendario para analises temporais
- `tabelas_suporte_dashboard.dax`
  Tabela auxiliar para aging de operacoes vencidas

---

## Como usar

1. Conecte o Power BI ao `SQL Server` ou ao `Athena`
2. Importe as tabelas e views do modelo
3. Crie uma tabela vazia chamada `_Medidas`
4. Cole as medidas de `medidas_credito_bba.dax` nessa tabela
5. Crie `dCalendario` usando `tabela_calendario.dax`
6. Crie `dAgingVencimento` usando `tabelas_suporte_dashboard.dax`
7. Ajuste os `Display Folders` conforme os blocos comentados no arquivo

---

## Fontes esperadas no modelo

- `vw_stage_cliente_enriquecido`
- `vw_stage_exposicao_recente`
- `vw_stage_rating_recente`
- `vw_stage_limite_consolidado`
- `vw_stage_operacoes_ativas`
- `vw_kpi_por_subsetor`
- `vw_matriz_risco`
- `vw_evolucao_rating_segmento`
- `operacoes`
- `ratings`
- `dCalendario`

---

## Decisoes de metodologia

**PD Media Pct**
Calcula PD ponderada pela exposicao de cada cliente (`SUMX(pd * exposicao) / SUM(exposicao)`).
Evita distorcao por clientes pequenos com PD elevada — padrao para carteiras corporativas.
Alinhado com `pd_ponderado_pct` nas views SQL. A versao simples (`pd_medio_simples_pct`) fica disponivel nas views para comparacao.

**Provisao Total Estimada (DAX)**
Usa `SUM(provisao_necessaria)` da view SQL, que aplica taxas regulatorias por faixa de rating (CMN 2.682) sobre `exposicao_total`. Esta e a abordagem regulatoria correta para Power BI.

O dashboard HTML usa metodologia diferente (PD ponderada x exposicao descoberta, LGD=100%), documentada no glossario do dashboard como premissa conservadora. Os dois valores serao diferentes — isso e esperado e intencional.

---

## Organizacao sugerida de pastas

```text
_Medidas/
|-- 0. Base
|-- 1. Visao Geral
|-- 2. Aprof. KPIs
|-- 3. Provisao e Classificacao
|-- 4. Evolucao e Distribuicao
|-- 5. Concentracao
|-- 6. Analise de Riscos
|-- 7. Performance de Limites
|-- 8. Analises Adicionais
|-- 9. Tempo
`-- 10. Status KPI
```

---

## Cobertura do dashboard

As medidas foram organizadas para cobrir o layout atual do repositorio:

- `Visao Geral`
  - Exposicao Total
  - % Exposicao Descoberta
  - Score Medio
  - PD Media
  - Operacoes Vencidas
  - Provisao Total Estimada
- `Aprof. KPIs`
  - Valor Vencido
  - Utilizacao Media
  - Concentracao Top 5
  - Aging de vencidos
  - Ranking de top clientes
- `Analise de Riscos`
  - classificacao de risco
  - flags
  - deterioracao
- `Performance de Limites`
  - limite total
  - utilizado
  - headroom
  - clientes acima de thresholds
- `Analises Adicionais`
  - percentis
  - media movel
  - volatilidade

---

## Observacoes

- As medidas foram escritas para um modelo em estrela leve, usando `vw_stage_cliente_enriquecido` como base principal
- O aging de vencidos requer a tabela `dAgingVencimento`
- Para visuais temporais, relacione `dCalendario` com as colunas de data relevantes do modelo
- Se optar por consumir apenas as views `vw_kpi_*`, parte das medidas pode se tornar redundante, mas o pacote DAX continua util para formatacao, status e analises adicionais
