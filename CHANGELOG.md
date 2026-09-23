# Changelog

Todas as alterações notáveis neste projeto serão documentadas neste arquivo.

O formato é baseado no [Keep a Changelog](https://keepachangelog.com/pt-BR/1.0.0/),
e este projeto adere ao [Semantic Versioning (SemVer)](https://semver.org/lang/pt-BR/).

## [1.0.0.9] - 2026-09-22 - Redesenho Unificado de SLA & Sincronização, Matriz de Convergência e Segregação de Incidentes de Dessincronia

### Adicionado
- **Redesenho Unificado da Matriz de SLA (2 Linhas x 7 Colunas) no Dashboard, Console e Dossiê:**
  - Padronização em formato matricial visual (`STATUS | QTD | MÉDIA | P90 | P95 | P99 | P100`) cobrindo as duas dimensões operacionais de sincronização:
    - **Linha 1: `DESSINCRONIZADOS` (Tolerância TSE: 90s):** Arquivos gerados na Origem (`dg/hg`) pendentes de entrega pela réplica (`Last-Modified`).
    - **Linha 2: `EM PROPAGAÇÃO` (Tolerância Akamai: 60s):** Arquivos recém-detectados na réplica em convergência Anycast entre PoPs/lâminas de borda.
  - Box pericial idêntico renderizado no console terminal (moldura ASCII alinhada em 87 caracteres) na inicialização do monitor e ciclicamente a cada 5 minutos.
- **Segregação Forense Estrita entre Incidentes de Dessincronia e Regressões de Cache:**
  - **Incidente de Dessincronia (Critério TSE):** Quando o tempo decorrido entre a geração na Origem e a entrega da réplica ultrapassa a tolerância de 90s, é registrado formalmente como **Incidente de Dessincronia**.
  - Criada tabela dedicada `incidentes_dessincronia` no SQLite (com índices em `timestamp_unix` e `arquivo`) e log contínuo em `incidentes_dessincronia.csv`.
  - **Isolamento de Causa Raiz:** Ocorrências de estouro de SLA de entrega da Origem não poluem a tabela `regressoes` nem o dossiê de anomalias de borda, garantindo precisão pericial na identificação de gargalos.
  - **Regressão de Cache (Critério Akamai):** Ocorrências em que nós de borda retrocedem para versões antigas após decorridos mais de 60s da detecção da nova versão continuam sendo registradas estritamente em `regressoes` como Ghost Cache.
- **Interface Web e Endpoints REST para Auditoria de Dessincronia:**
  - Novo modal interativo no Dashboard: **`⚠️ Incidentes de Dessincronia de Cache (Tolerância TSE > 90s)`**, com busca dinâmica, badges de tempo de atraso e links para os arquivos na Origem e Réplica.
  - Novos endpoints: `GET /api/incidentes-dessincronia` e `GET /download/csv-incidentes-dessincronia` (alias `/export/incidentes-dessincronia-csv`).
  - Badge visual clicável no Card 4 indicando o total de incidentes de dessincronia com atalho para abertura do modal.
- **Renomeação Sistêmica de Parâmetros e Harmonização de Textos:**
  - Parâmetro 3: Renomeado de *Tolerância para Regressão de Cache (Critério Akamai)* para `3) Tolerância de Propagação (Critério Akamai)` (padrão: 60s).
  - Parâmetro 4: Renomeado de *SLA Máximo de Propagação (Critério TSE)* para `4) Tolerância de Dessincronia (Critério TSE)` (padrão: 90s).
  - Atualização do resumo do cabeçalho: `Matriz: 10s | Réplicas: 10s | Propagação Akamai: 60s | Dessincronia TSE: 90s`.
  - Atualização dos textos de ajuda metodológica nos tooltips do Dashboard e no laudo pericial `relatorio_evidencias.html`.
- **Validação e Instalação Automatizada de Dependências no Script de Inicialização:**
  - Implementada verificação inteligente de presença e versão do Node.js (requer >= 22.5.0 para `node:sqlite`) em `iniciar_monitor.ps1`.
  - Instalação automatizada com fallback duplo (`winget` e download assistido do `.msi` oficial da Node.js Foundation).
  - Recarregamento transparente do `$env:PATH` na sessão ativa do PowerShell.

---

## [1.0.0.8] - 2026-09-22 - Correção de Rastreamento de Réplicas no fileSyncTracker e Suporte a URLs de Aplicação

### Corrigido
- **Correção no Rastreamento de Versão Alvo em Réplicas (`fileSyncTracker`):**
  - Correção crítica no método `processVersion`: anteriormente, leituras de réplica servindo versões antigas (ex: V1) gravavam indevidamente `tracker.firstSeenCdnAt` e marcavam a sincronização no nó antes da chegada da nova versão (V2).
  - Implementada a validação estrita `isTargetOrNewer`: `firstSeenCdnAt` e a conclusão de sincronização só são carimbados quando a réplica efetivamente entrega a versão alvo ou superior.
  - Correção na resolução de tolerância de cache da Akamai para priorizar `Math.max(cdnFirstSeen, tracker.firstSeenCdnAt)` e validar com precisão o tempo decorrido do cabeçalho `Last-Modified` (`elapsedLmSec <= 60s`).
  - Eliminação de alarmes falsos de regressão provocados por alternância rápida entre lâminas Ghost Anycast durante o ciclo normal de TTL (23s de oscilação real vs 320s calculados incorretamente no passado).

### Adicionado
- **Detecção e Ajuste Automático de URLs de Aplicação Web no Cadastro de Servidores:**
  - Auto-detecção de sufixos de front-end SPA/HTML como `/app/index.html` em `/api/servidores/testar` e `/api/servidores/salvar`.
  - Sugestão instantânea da URL base correta do backend de dados com substituição amigável no modal do Dashboard, prevenindo falhas de teste de conectividade.

---

## [1.0.0.7] - 2026-09-21 - Duplo Critério de Atraso (Akamai vs TSE) e SLA Individualizado no Dashboard

### Adicionado
- **Desacoplamento do Duplo Critério de Avaliação de Atraso e Integridade:**
  - **Critério 1: Falha de Integridade / Regressão de Cache (Critério Akamai):**
    - Medição de tolerância de 60s (ciclo de cache TTL CDN) contados estritamente a partir do momento em que a CDN/borda detecta e serve pela primeira vez a nova versão (`firstSeenCdnAt`).
    - Se durante o ciclo de 60s um nó de borda entregar versão anterior, o evento é classificado como convergência normal de cache (`EM_PROPAGACAO` / `CONVERGENCIA_CACHE_AKAMAI`), gravando telemetria em `leituras` sem disparar alarme falso de regressão nem poluir a tabela `regressoes`.
    - Se após 60s da detecção da nova versão pela CDN um nó de borda retroceder para uma versão defasada, confirma-se Falha de Integridade de Cache Akamai (Ghost Cache / regressão real).
  - **Critério 2: SLA de Latência de Propagação (Critério TSE):**
    - Medição de SLA ponta a ponta (padrão: 90s), calculado a partir da publicação do arquivo na Origem (HMG).
    - Permite auditar com precisão o tempo total que as réplicas levaram para disponibilizar o dado aos usuários finais, identificando estouros de SLA contratual/operacional.
  - **Parâmetros Independentes de Configuração e Hot-Reload:**
    - Novos parâmetros `tolerancia_cache_akamai_segundos` (60s) e `sla_propagacao_tse_segundos` (90s), persistidos no SQLite com hot-reload sem reiniciar o processo.
    - Modal de configurações no Dashboard e resumo de parâmetros no cabeçalho atualizados para: `Matriz: 10s | Réplicas: 10s | Cache Akamai: 60s | SLA TSE: 90s`.
    - Endpoints REST `GET /api/configuracoes` e `POST /api/configuracoes` atualizados para gerenciar ambos os limiares.

- **Evolução do Card 4 de KPIs: "Cache (SLAs Acumulados)":**
  - **Suporte a Visão Consolidada e Nós Individuais:**
    - Botões interativos em formato de *pill* (`Consolidado`, `SIM-UNIFICADO`, `SIM-INTERESSADOS`, etc.) no cabeçalho do Card 4.
    - Alternância em tempo real com 1 clique: os percentis do card (Média, P90, P95, P99, P100) e a contagem de arquivos desincronizados recalculam imediatamente para refletir o nó selecionado ou a média de todos os nós.
    - Barra inferior de resumo permanente (`Nós: SIM-UNIFICADO: 0m 14s (P95: 0m 19s) | ...`) exibindo todos os nós simultaneamente com atalho para focar no nó desejado.

### Corrigido
- **Cálculo de SLA Resiliente para Nós com Nomes Customizados:**
  - Correção na query SQLite de cálculo de SLA que antes comparava apenas nomes legados `'HMG'` e `'SIM'`, ignorando servidores configurados como `HMG-UNIFICADO`, `SIM-UNIFICADO` e `SIM-INTERESSADOS`.
  - Implementado pattern matching resiliente (`papel_servidor LIKE '%ORIGEM%'` vs `NOT (...)`) com agrupamento dinâmico por réplica, gerando tanto `todaySlaStats` (consolidado) quanto `replicaSlaStats` (por nó).

---

## [1.0.0.6] - 2026-09-16 - Exportação do Dossiê Pericial Offline Autônomo (Padrão Rodada #20)

### Adicionado
- **Exportação do Dossiê Pericial Offline Autônomo (Padrão Rodada #20):**
  - Implementação do motor gerador `dossie_offline_generator.js` integrado à rota `/export/dossie-html`.
  - Ao clicar no botão `📥 Baixar HTML Offline` no Dossiê Técnico Forense (`/report`), o arquivo HTML gerado reproduz com fidelidade pericial estrita todos os requisitos de `relatorio_regressoes_rodada_20.html`.
  - Reconstrução da cadeia causal de cada incidente com janela pericial completa de 7 leituras (Origem HMG, Histórico Prévio, Referência Anterior / Pico, Regressão Detectada, Recuperação / Normalização).
  - Grid pericial de 25 colunas técnicas organizadas por grupos funcionais com reorganização Drag & Drop dos cabeçalhos e botão de restauração.
  - Exportação inteligente para CSV em UTF-8 com BOM respeitando rigorosamente a ordem visual reorganizada das colunas na tela.
  - Painel executivo com 4 cards de KPIs calculados em tempo real (Ocorrências Auditadas com faixa de IDs, Requisições no Contexto com média calculada, % SIM e % Conformidade da Origem HMG).
  - Modal forense escuro de headers HTTP brutos (Request e Response headers completos em JSON formatado).
  - Arquivo 100% autocontido e portável (Zero chamadas externas ou scripts de CDNs, dados brutos incorporados em `const RAW_DATA` e `const OCCURRENCES_DATA`).

### Corrigido
- **Classificação Agnóstica de Servidores Origem vs Réplica no Dossiê Offline:**
  - Correção na identificação de nós monitorados com sufixos dinâmicos (`HMG-UNIFICADO`, `HMG-INTERESSADOS`, `SIM-UNIFICADO`, `SIM-INTERESSADOS`).
  - Anteriormente, a verificação por igualdade estrita (`=== 'HMG'`) classificava requisições do servidor de origem com outro nome como réplicas simuladas, rotulando com badge âmbar `SIM` em vez de roxo `HMG`, atribuindo status de histórico prévio/recuperação em vez de `🟣 ORIGEM` e zerando o filtro "Apenas HMG (Origem)".
  - Suporte completo tanto às rodadas históricas (ex: Rodada #20 com nós `HMG`/`SIM`) quanto às novas rodadas unificadas (`HMG-UNIFICADO`/`SIM-UNIFICADO`), com contadores dinâmicos nos botões de filtro (`Apenas HMG (227)` e `Apenas SIM (319)`).
  - Inclusão do acionador `📥 Exportar HTML Offline` no modal de regressões do Dashboard com repasse automático do ID da rodada ativa e termos de busca.

---

## [1.0.0.5] - 2026-09-16 - Parametrização de Polling Desacoplado e Tolerância de 90s Akamai CDN

### Adicionado
- **Parametrização Independente dos Ciclos de Busca (Polling Desacoplado):**
  - Desacoplamento da varredura contínua entre Servidor Matriz (Origem) e Servidores Replicados (Cache / Borda / CDN).
  - Dois novos parâmetros com persistência no SQLite e hot-reload em tempo de execução:
    - `intervalo_matriz_segundos`: Frequência de consulta ao nó de origem (padrão: 10s).
    - `intervalo_replicas_segundos`: Frequência de consulta aos nós de cache/distribuição (padrão: 10s).
  - Pools de workers concorrentes isolados protegidos contra sobreposição de ciclos via timers assíncronos.
- **Janela de Tolerância Akamai de 90s para Falha de Integridade Registrável:**
  - Novo parâmetro `tolerancia_propagacao_segundos` (padrão: 90s), baseado nas premissas técnicas da Akamai (30s para término do pipeline de publicação do TSE + 60s de ciclo de cache CDN).
  - As leituras em intervalos curtos (10s) continuam ocorrendo para alimentação de métricas e medição exata do tempo de convergência.
  - Leituras divergentes/intermediárias observadas durante a janela de tolerância de 90s são classificadas como `EM_PROPAGACAO`, gravando telemetria em `leituras`, mas sem registrar falso positivo em `regressoes` nem no arquivo `regressoes_detectadas.csv`.
  - Apenas divergências que persistirem após extrapolar o limite de 90s da publicação são registradas como falha de integridade confirmada com evidência raw.
- **Gestão de Parâmetros no Dashboard Web & API REST:**
  - Novos endpoints `GET /api/configuracoes` e `POST /api/configuracoes` com propagação de eventos via Server-Sent Events (SSE).
  - Novo modal interativo **`⏱️ Parâmetros de Polling e Tolerância Akamai CDN`** no cabeçalho do Dashboard.
  - Indicador visual permanente dos parâmetros ativos na barra superior (`headerParamsSummary`).



---

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
