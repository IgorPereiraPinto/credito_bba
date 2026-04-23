# 00 - Setup Local e Git

## Objetivo da etapa

Comecar o projeto da forma correta: criar a pasta local, alinhar o nome com o repositorio, preparar o ambiente, abrir no VS Code, entender quando usar terminal e quando atuar no codigo, e iniciar o versionamento sem pular etapas.

---

## Onde o processo comeca

O processo comeca antes do Python e antes do SQL.

Ele comeca em 4 decisoes iniciais:

1. qual sera o nome do projeto
2. onde ele sera criado na sua maquina
3. como ele sera versionado no Git
4. qual sera o fluxo de trabalho entre terminal, editor e documentacao

---

## Passo 1 - Criar a pasta do projeto

Regra recomendada:

- o nome da pasta local deve ser o mesmo nome do repositorio
- isso reduz erro de caminho, erro em scripts e confusao em documentacao

Exemplo:

```text
Projeto local: analise_de_credito
Repositorio GitHub: analise_de_credito
```

Se o projeto ja existe no GitHub, prefira clonar:

```bash
git clone https://github.com/IgorPereiraPinto/analise_de_credito.git
cd analise_de_credito
```

Por que comecar assim:

- o Git ja traz o historico do projeto
- voce evita criar arquivos fora do lugar
- o nome da pasta ja nasce padronizado

Se o projeto ainda nao existe no GitHub e vai nascer do zero:

```bash
mkdir analise_de_credito
cd analise_de_credito
git init
```

Por que `git init` entra aqui:

- para versionar desde a primeira decisao
- para separar alteracao experimental de alteracao aprovada
- para permitir rollback e colaboracao

---

## Passo 2 - Abrir e analisar as bases de dados

Antes de escrever codigo, entenda o dado e o problema.

O que voce precisa enxergar na base:

- quais tabelas existem
- qual a granularidade de cada uma
- quais campos identificam entidades
- quais campos representam valor, data, status, score e classificacao
- quais relacionamentos parecem existir entre as tabelas
- quais colunas parecem essenciais para tomada de decisao

Perguntas de negocio que ajudam a identificar o problema:

1. quem e a entidade principal da analise: cliente, operacao, limite ou exposicao?
2. qual decisao de negocio o dashboard precisa apoiar?
3. quais riscos o negocio quer monitorar?
4. quais indicadores mostram deterioracao ou melhoria da carteira?
5. quais regras parecem criticas: vencimento, limite, score, rating, exposicao descoberta?
6. o que seria um comportamento normal, de atencao e critico?

Saida esperada desta etapa:

- mapa das tabelas
- lista de perguntas de negocio
- lista inicial de KPIs
- hipoteses de risco, eficiencia ou concentracao

---

## Passo 3 - Abrir a pasta no VS Code

Comandos:

```bash
code .
```

Se o comando `code` nao estiver habilitado:

- abra o VS Code manualmente
- use `File > Open Folder`
- selecione a pasta do projeto

Por que abrir no VS Code neste ponto:

- para navegar estrutura, docs e scripts
- para ler o projeto antes de alterar
- para manter terminal e codigo no mesmo contexto

---

## Passo 4 - Criar e ativar o ambiente Python

Comandos:

```bash
python -m venv .venv
```

Windows:

```bash
.venv\Scripts\activate
```

Linux/Mac:

```bash
source .venv/bin/activate
```

Por que isso vem antes das bibliotecas:

- isola as dependencias do projeto
- evita conflito com outros projetos
- torna o setup reproduzivel

---

## Passo 5 - Instalar as bibliotecas

Bibliotecas do projeto:

```bash
pip install -r requirements.txt
pip install -r requirements-dev.txt
```

O que cada arquivo representa:

- `requirements.txt`: dependencias para rodar o projeto
- `requirements-dev.txt`: dependencias para testar, validar e desenvolver

Por que instalar nessa ordem:

- primeiro o que faz o projeto funcionar
- depois o que apoia qualidade, testes e manutencao

---

## Passo 6 - Configurar o projeto

Comando:

```bash
copy .env.example .env
```

No Linux/Mac:

```bash
cp .env.example .env
```

Depois disso:

- revisar caminhos de entrada e saida
- revisar configuracoes de exportacao
- revisar conexao SQL Server ou Athena, se aplicavel

Por que isso acontece antes do ETL:

- o ETL depende de caminho correto
- o export depende do formato configurado
- a camada SQL depende do ambiente certo

---

## Passo 7 - Quando usar terminal e quando usar codigo

No terminal voce faz:

- criar ambiente virtual
- ativar ambiente
- instalar bibliotecas
- rodar scripts
- rodar testes
- executar comandos Git

No codigo voce faz:

- ajustar regras de extracao
- ajustar tipos e limpeza
- implementar validacoes
- comentar trechos editaveis para reutilizacao
- adaptar SQL, KPIs e dashboard

Regra pratica:

- terminal executa o fluxo
- codigo define a logica

---

## Passo 8 - Qual e a primeira etapa tecnica do pipeline

Depois do setup e da analise da base, o primeiro bloco tecnico e o Python ETL.

Sequencia recomendada:

1. `01_extract.py`
2. `02_clean.py`
3. `03_validate.py`
4. `04_export.py`
5. testes
6. SQL `raw`
7. SQL `stage`
8. SQL `dw`
9. queries analiticas
10. dashboard

Por que comecar pelo ETL:

- sem dado confiavel, o SQL so replica problema
- sem padronizacao, os joins ficam fragilizados
- sem validacao, os KPIs perdem credibilidade

---

## Passo 9 - Quais comandos Git executar e por que

Se o projeto foi clonado, voce ja comeca com historico.

Comandos mais importantes no dia a dia:

```bash
git status
```

Por que usar:

- ver o que mudou
- confirmar se voce esta mexendo no arquivo certo

```bash
git add .
```

Ou, preferencialmente:

```bash
git add README.md docs/como_executar.md roadmap/00_setup_local_e_git.md
```

Por que usar:

- preparar o que vai para o proximo commit
- evitar commitar arquivo errado

```bash
git commit -m "docs: melhora onboarding do projeto"
```

Por que usar:

- registrar uma unidade clara de trabalho
- facilitar revisao e rollback

```bash
git pull origin master
```

Por que usar:

- trazer atualizacoes do remoto antes de continuar
- reduzir conflito quando o projeto esta sendo alterado em mais de um lugar

```bash
git push origin master
```

Por que usar:

- publicar sua alteracao
- disparar atualizacoes do GitHub Pages quando a branch de publicacao usa `master`

Ordem segura no dia a dia:

1. `git status`
2. editar arquivos
3. `git status`
4. `git add ...`
5. `git commit -m "..."`
6. `git pull origin master`
7. resolver conflitos, se houver
8. `git push origin master`

---

## Checklist de entrada da etapa

- [ ] pasta criada com o mesmo nome do repositorio
- [ ] projeto aberto no VS Code
- [ ] ambiente virtual criado
- [ ] dependencias instaladas
- [ ] `.env` configurado
- [ ] base de dados analisada
- [ ] perguntas de negocio registradas
- [ ] Git funcionando localmente

---

## Onde esta etapa termina

Esta etapa termina quando:

- o ambiente esta pronto
- voce entende minimamente a base
- sabe qual problema quer resolver
- consegue rodar comandos no terminal sem improviso
- esta pronto para entrar no entendimento do case e do pipeline

Proximo passo: [01_visao_geral_do_projeto.md](01_visao_geral_do_projeto.md)
