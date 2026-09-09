# Changelog

Todas as alterações notáveis neste projeto serão documentadas neste arquivo.

O formato é baseado no [Keep a Changelog](https://keepachangelog.com/pt-BR/1.0.0/),
e este projeto adere ao [Semantic Versioning (SemVer)](https://semver.org/lang/pt-BR/).

---

## [1.0.0] - 2026-09-09

### Adicionado
- **Motor de Auditoria Cronológica e Regressão de Dados:**
  - Verificação monótona de data e hora de geração (`dg` e `hg`) entre arquivos JSON de totalização.
  - Detecção imediata de anomalias temporais e regressões de borda na camada de CDN/Cache (SIM).
  - Rastreabilidade independente da sequence `idg` para contornar intercalação nativa em clusters Oracle RAC.
- **Topologia Multi-Nós Flexível (N-Vias):**
  - Suporte a arquitetura Master/Replica dinâmica com nós customizados (HMG, SIM, Akamai, instâncias regionais).
  - Gerenciador de Servidores no dashboard web com CRUD, teste de latência HTTP e promoção instantânea de Origem.
  - Inspeção e comparação side-by-side via Matriz Multi-Nós com badges de status e deltas individuais.
- **Painel em Tempo Real (Dashboard Web):**
  - Servidor HTTP nativo na porta `3333` com atualização reativa via Server-Sent Events (SSE).
  - KPIs consolidados: Arquivos monitorados, regressões temporais na rodada e estatísticas de SLA de propagação (Média, P90, P95, P100).
  - Tabela comparativa com filtros instantâneos por Eleição, UF, Tipo de Arquivo, Cargo e Status de Integridade.
  - Inspecionador de cabeçalhos HTTP (`Cache-Control`, `Age`, `ETag`, `Server`, IPs de instâncias).
- **Gerenciador de Rodadas (Checkpoints Lógicos):**
  - Delimitação de janelas de teste com reinício automático de contadores sem expurgo do histórico pericial no banco.
  - Transição diária automática à meia-noite e possibilidade de criação manual de novas rodadas.
- **Dossiê Técnico de Evidências Forenses:**
  - Geração de relatório HTML autônomo com sumário executivo, KPIs periciais e listagem completa de evidências raw.
  - Salvamento estruturado de snapshots na pasta `versoes/` replicando a hierarquia de URLs.
  - Assistente de exportação em lote (.ZIP) com cálculo de tamanho e barra de progresso em tempo real.
- **Coleta Híbrida de Alta Eficiência:**
  - Integração via Chrome DevTools Protocol (CDP `:9222`) para interceptação de sessões ativas no navegador.
  - Pool assíncrono concorrente de sondas HTTP com reaproveitamento de conexões e suporte a descompressão gzip nativa.
- **Persistência Forense em SQLite:**
  - Armazenamento transacional de alta velocidade utilizando o módulo nativo `node:sqlite` com modo WAL (*Write-Ahead Logging*).
  - Registro detalhado de histórico de leituras, detecções de anomalias e metadados de propagação.
