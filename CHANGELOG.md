# Changelog

Todas as alterações notáveis neste projeto serão documentadas neste arquivo.

O formato é baseado no [Keep a Changelog](https://keepachangelog.com/pt-BR/1.0.0/),
e este projeto adere ao [Semantic Versioning (SemVer)](https://semver.org/lang/pt-BR/).

---

## [1.1.0] - 2026-09-12

### Adicionado
- **Expurgo Físico Definitivo de Rodadas (Purge & Disk Reclamation):**
  - Implementação de modal interativo de confirmação com visualização de volumetria prévia (`/api/rodadas/purge-preview`).
  - Remoção em cascata transacional de registros das tabelas `leituras`, `regressoes` e `rodadas` (`/api/rodadas/purge`).
  - Limpeza física e cirúrgica de arquivos de evidência forense em disco (`evidencias_raw/`) pertencentes à rodada, sem comandos de shell ou wildcards.
  - Execução imediata de `VACUUM` no SQLite para deflacionar e recuperar o espaço em disco do arquivo `tdtot_auditoria.db`.
  - Salvaguarda rígida impedindo a exclusão ou expurgo da rodada atualmente ativa.

## [1.0.1] - 2026-09-12

### Modificado
- **Identificação Cronológica em Combos de Rodada:**
  - As caixas de seleção de rodadas no Dossiê (`/report`) agora exibem o nome da rodada concatenado com a respectiva data e hora de início (`dd/MM/yyyy HH:mm:ss`), com destaque para a rodada `[ATIVA]`.
  - Migração e saneamento dos nomes de todas as rodadas existentes no banco de dados, removendo redundâncias de data/hora inseridas no texto do nome.
  - Atualização do gerador automático de rodadas diárias e manuais para nomes limpos e padronizados.

## [1.0.0] - 2026-09-12 - Versão Estável

### Adicionado
- **Identificação Visual de Versão:**
  - Badge oficial `v1.0` adicionado no topo de todas as telas (Dashboard Principal e Dossiê Técnico Forense).
- **Dossiê Técnico Forense Unificado e Reconstituição Histórica:**
  - Recuperação completa de rodadas históricas diretamente do banco SQLite (`tdtot_auditoria.db`).
  - Cálculo retroativo exato de SLA de propagação (Média, P90, P95, P99) e atributos de cache (TTL médio `max-age` e Hit Rate de borda CDN).
  - Isolamento seguro de checkpoints e segregação de rodadas de testes.
- **Ajuda Forense Contextual nos KPIs:**
  - Botão informativo `ℹ️` puro em cada um dos 5 cards de KPI.
  - Popover flutuante inteligente posicionado logo abaixo do card clicado, sem obstruir as métricas ou rolar a página para o rodapé.
  - Explicação pericial didática para Arquivos Monitorados, Regressões, Cache Atrasado, SLA e Atributos HTTP.
- **Pausa Inteligente de Auto-Refresh:**
  - O auto-refresh periódico de 10s é pausado automaticamente ao visualizar rodadas passadas/inativas, mantendo a tela estável e imutável.
  - Indicador dinâmico no cabeçalho sinalizando `🟢 Dossiê Dinâmico (ao vivo)` vs `⚪ Rodada Histórica (Auto-Refresh Pausado)`.
- **Otimização de Desempenho e Tráfego Forense:**
  - Implementação do modo leve (`light=1`) na rota `/api/regressoes`, reduzindo o payload de 77 MB para menos de 1,5 MB com carregamento instantâneo no navegador.
  - Eliminação de exceções de manipulação do DOM e blindagem com checagens defensivas em todos os seletores.
- **Motor de Auditoria Cronológica e Regressão de Dados:**
  - Verificação monótona de data e hora de geração (`dg` e `hg`) entre arquivos JSON de totalização.
  - Detecção imediata de anomalias temporais e regressões de borda na camada de CDN/Cache (SIM).
  - Rastreabilidade independente da sequence `idg` para contornar intercalação nativa em clusters Oracle RAC.
- **Topologia Multi-Nós Flexível (N-Vias):**
  - Suporte a arquitetura Master/Replica dinâmica com nós customizados (HMG, SIM, Akamai, instâncias regionais).
  - Gerenciador de Servidores no dashboard web com CRUD, teste de latência HTTP e promoção instantânea de Origem.
  - Inspeção e comparação side-by-side via Matriz Multi-Nós com badges de status e deltas individuais.
- **Persistência Forense em SQLite:**
  - Armazenamento transacional de alta velocidade utilizando o módulo nativo `node:sqlite` com modo WAL (*Write-Ahead Logging*).
  - Registro detalhado de histórico de leituras, detecções de anomalias e metadados de propagação.
