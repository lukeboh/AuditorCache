# Changelog

Todas as alterações notáveis neste projeto serão documentadas neste arquivo.

O formato é baseado no [Keep a Changelog](https://keepachangelog.com/pt-BR/1.0.0/),
e este projeto adere ao [Semantic Versioning (SemVer)](https://semver.org/lang/pt-BR/).

## [1.0.0.4] - 2026-09-15 - Linha do Tempo Forense Interativa e Decodificação Ghost Akamai

### Adicionado
- **Interatividade Total na Linha do Tempo de Requisições:**
  - Inspeção pontual de qualquer leitura cronológica (preliminar, intermediária ou posterior) com atualização em tempo real do **Painel Forense & Detalhes Técnicos**.
  - Exibição sob demanda dos cabeçalhos HTTP completos (`Request Headers` e `Response Headers`), latência (RTT), instante de chamada e diretivas RFC 7234 para cada requisição da timeline.
- **Destaque Forense à Requisição Causadora do Caso:**
  - Destaque visual com badge pulsante vermelho `🚨 REQUISIÇÃO CAUSADORA DO CASO FORENSE` na timeline e banner pericial dedicado no painel técnico.
  - Botão de ação rápida `[🚨 Voltar à Requisição Causadora]` para restauração imediata do foco no ponto de anomalia durante a navegação entre passos.
- **Trilha Visual de Versões (Taxonomia V1, V2, ...):**
  - Mapeamento e vinculação cromática consistente das versões de arquivos ao longo de todo o dossiê e cabeçalho técnico.
- **Decodificação e Persistência da Lâmina Ghost Akamai Edge (`ghost_ip`):**
  - Decodificação pontual e retroativa de endereços IPv4 da lâmina Ghost a partir do *Ghost Reference Number* (GRN) no formato Little-Endian hexadecimal.
  - Exibição transparente tanto do VIP TCP de Conexão quanto da Lâmina Ghost no Dashboard Web e nos Dossiês Forenses.
- **Paridade Completa no Relatório Offline:**
  - Espelhamento de todas as rotinas interativas e decodificadores para relatórios HTML estáticos exportados (`relatorio_evidencias.html`).

---

## [1.0.0.2] - 2026-09-14 - Hotfix Dossiê Forense de Regressões Detectadas

### Corrigido
- **Correção de ReferenceError (`dateHttp is not defined`):**
  - Declarada a variável `const dateHttp = rawHeaders['date'] || '-'` no painel técnico de detalhes de auditoria forense (`renderTechDetailsInPanel`).
  - Corrigida a renderização e abertura do modal "Dossiê Forense de Regressões Detectadas", garantindo a visualização íntegra da lista de ocorrências e de seus metadados de cabeçalho HTTP RFC 7234.
- **Detecção Resiliente de Nó de Origem:**
  - Atualizadas as referências ao estado global da API (`latestApiData.originKey`) na renderização dos cards de regressão e esquema cronológico, assegurando a estilização visual roxa (`tag-hmg-title` / `🟣 Origem Primária`).

---

## [1.0.0.1] - 2026-09-14 - Hotfix Novos Servidores & Ciclos Dinâmicos

### Corrigido
- **Suporte Dinâmico a Ciclos de Eleição (`pl.c`):**
  - Removido o prefixo hardcoded `tdtot2026/` das rotas e do catálogo de arquivos.
  - O sistema agora extrai dinamicamente o diretório do ciclo (`ele2026`, `tdtot2026`, etc.) diretamente da propriedade `"c"` do arquivo mestre `ele-c.json`.
  - Os 229 arquivos de totalização e configuração de urnas (`-cs.json`) dos novos servidores de teste passam a ser descobertos e monitorados com sucesso (HTTP 200).
- **Descoberta de Eleições em Servidores Ativos:**
  - A função de auto-descoberta (`discoverAvailableElections`) foi corrigida para usar o servidor de Origem ativo (`HMG_SIMULADO`) ou a réplica ativa (`SIMINTERESSADOS`), em vez de procurar cegamente a chave legada `'SIM'` mesmo inativa.
- **Interceptador CDP do Chrome DevTools:**
  - Atualizado o listener de rede do Chrome para fazer matching dinâmico das requisições com a `baseUrl` de todos os servidores ativos cadastrados, extraindo corretamente os caminhos relativos para URLs em `/simulado/` ou `/simulado/simulado/`.
- **Cálculo de SLA Agnosticista a Nomes de Servidores:**
  - As consultas SQLite de cálculo e reconstituição de SLA foram migradas para verificar `papel_servidor = 'ORIGEM'` e `papel_servidor = 'REPLICA'`, mantendo total retrocompatibilidade com bases legadas.
- **Remoção de Falso-Positivo por Regressão de Seções (ST):**
  - Removida a detecção de regressão baseada na contagem de seções totalizadas (`st`), uma vez que variações regressivas nessa métrica são negocialmente aceitas (decorrentes de reprocessamento, anulação de urnas ou totalizações suplementares).
  - Atualizados os critérios combinados (`TEMPO + TOTALIZAÇÃO + ST` e `TEMPO + ST`) para manter unicamente as regressões temporais de geração (`dg/hg`) e totalização (`dt/ht`).
  - Higienização da base SQLite (`tdtot_auditoria.db`) com expurgo de 677 registros exclusivos de ST, atualização de 456 registros combinados e normalização de 2.872 leituras na tabela `leituras`.
  - Atualizada a interface do Dossiê e da Matriz de Auditoria, ajustando os filtros e badges de inversão para `INVERSÃO: DG ↗ | DT ↘`.
- **Script de Inicialização `iniciar_monitor.ps1`:**
  - Atualizadas as abas abertas no Chrome em depuração para o novo simulador (`/simulado/simulado/app/index.html`).

---

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
