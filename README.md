# 🗳️ AuditorCache / Monitor TDTot TSE

[![Release](https://img.shields.io/github/v/release/lukeboh/AuditorCache?include_prereleases&color=brightgreen)](https://github.com/lukeboh/AuditorCache/releases)
[![Node.js](https://img.shields.io/badge/node-%3E%3D22.5.0-blue)](https://nodejs.org)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Repository](https://img.shields.io/badge/GitHub-lukeboh%2FAuditorCache-181717?logo=github)](https://github.com/lukeboh/AuditorCache)

Sistema autônomo e contínuo de **auditoria forense, validação cronológica e medição de SLA de propagação** entre o ambiente de geração primária (**HMG - Homologação**) e a camada de distribuição em cache/CDN (**SIM - Simulado**) dos arquivos da totalização de eleições do Tribunal Superior Eleitoral (TSE).

---

## 📌 Sumário
1. [Objetivo e Contexto](#-objetivo-e-contexto)
2. [Topologia e Arquitetura](#-topologia-e-arquitetura)
3. [Pré-requisitos](#-pré-requisitos)
4. [Como Executar](#-como-executar)
5. [Painel em Tempo Real (Dashboard Web)](#-painel-em-tempo-real-dashboard-web)
6. [Dossiê Técnico de Evidências Forenses](#-dossiê-técnico-de-evidências-forenses)
7. [Padrão de Salvamento Contínuo de Versões (Pasta versoes/)](#-padrão-de-salvamento-contínuo-de-versões-pasta-versoes)
8. [Exportação Compactada de Versões (.ZIP) com Barra de Progresso](#-exportação-compactada-de-versões-zip-com-barra-de-progresso)
9. [Delimitador Lógico de Rodadas (Checkpoints de Teste)](#-delimitador-lógico-de-rodadas-checkpoints-de-teste)
10. [Descoberta e Monitoramento Multi-Eleições](#-descoberta-e-monitoramento-multi-eleições)
11. [Topologia Flexível e Auditoria Multi-Nós (N-Vias)](#-topologia-flexível-e-auditoria-multi-nós-n-vias)
12. [Decisões de Engenharia e Peculiaridades Técnicas](#-decisões-de-engenharia-e-peculiaridades-técnicas)
13. [Estrutura do Repositório](#-estrutura-do-repositório)

---

## 🎯 Objetivo e Contexto

Durante os testes e simulados de totalização das Eleições, os sistemas do TSE geram arquivos JSON consolidados de boletins, votos e configurações (arquivos `-u.json`, `-ab.json`, etc.). 

Este monitor foi concebido para responder, em tempo real e com rigor pericial, a três perguntas fundamentais:
1. **Integridade Cronológica:** Ocorreu algum retrocesso temporal no conteúdo dos arquivos publicados? (Ex: um arquivo publicado com horário `17:40` voltar a exibir dados de `16:43`?).
2. **SLA e Propagação de Cache:** Quanto tempo leva para um arquivo gerado na fonte (HMG) se tornar visível aos eleitores e sistemas externos na camada de cache/CDN (SIM)?
3. **Defasagem Instantânea:** Quantos arquivos estão desatualizados no cache neste exato segundo?

---

## 🏗️ Topologia e Arquitetura

```
+---------------------------------------------------------------------------------+
|                                 SISTEMA TDTOT                                   |
|                                                                                 |
|   +-----------------------+                    +----------------------------+   |
|   |  FONTE (HMG)          |                    |  CACHE / CDN (SIM)         |   |
|   |  Origem Primária      | == Propagação ==>  |  Borda de Distribuição     |   |
|   |  resultados-hmg       |                    |  resultados-sim (PoPs)     |   |
|   +-----------------------+                    +----------------------------+   |
+---------------------------------------------------------------------------------+
           |                                                    |
           +--------------------+         +---------------------+
                                |         |
                                v         v
                   +-------------------------------+
                   |     COLETA HÍBRIDA            |
                   | - Chrome DevTools (CDP :9222) |
                   | - HTTP Poller Assíncrono      |
                   +-------------------------------+
                                   |
                                   v
                   +-------------------------------+
                   |    MOTOR DE AUDITORIA         |
                   | - Validação Temporal (dg/hg)  |
                   | - SLA First-Seen desde 00h    |
                   | - Detecção de Regressão Borda |
                   +-------------------------------+
                                   |
         +-------------------------+-------------------------+
         |                                                   |
         v                                                   v
+-------------------------------+                   +-------------------------------+
|     PERSISTÊNCIA FORENSE      |                   |       INTERFACE DO USUÁRIO    |
| - SQLite WAL (tdtot_auditoria)|                   | - Dashboard Web (:3333)       |
| - Raw JSONs (evidencias_raw/) |                   | - Dossiê Executivo (/dossie)  |
| - CSVs de Auditoria           |                   | - Alertas Sonoros e Filtros   |
+-------------------------------+                   +-------------------------------+
```

### 1. Servidores Monitorados
* **FONTE / ORIGEM (`HMG`):** `https://resultados-hmg.tse.jus.br/homologa/teste/`  
  Onde os arquivos são totalizados e gravados diretamente pelos geradores do TDTot.
* **CACHE / CDN (`SIM`):** `https://resultados-sim.tse.jus.br/simulado/teste/`  
  Servidores de borda (CloudFront/CDN/Nginx) responsáveis por entregar o arquivo aos usuários finais.

### 2. Coleta Híbrida
* **Chrome DevTools Protocol (CDP na porta 9222):** Intercepta passivamente todo tráfego de rede gerado pela aplicação oficial do TSE rodando no navegador, capturando novos arquivos e rotas dinâmicas em tempo real.
* **HTTP Poller Paralelo:** Realiza requisições periódicas (`GET` com `Cache-Control: no-cache` e timestamp anti-cache) para todos os arquivos descobertos no catálogo em ambos os servidores, garantindo atualização segundo a segundo.

### 3. Persistência Forense e Histórico de Versões
* **Banco SQLite em modo WAL (`tdtot_auditoria.db`):** Registro de alta concorrência contendo:
  * `leituras`: Histórico completo de cada leitura feita (timestamp unix, servidor, data, hora, idg, hash).
  * `comparativos`: Último estado comparativo consolidado de cada arquivo.
  * `regressoes`: Log imutável de todas as regressões temporais reais identificadas.
* **Repositório de Versões (`versoes/`):** Gravação contínua de todos os snapshots de versões coletadas, organizada na árvore de pastas da URL com nome carimbado por ambiente (`HMG`/`SIM`), data (`dg`), hora (`hg`) e `idg`.
* **Evidências Brutas de Incidentes (`evidencias_raw/`):** Quando uma regressão temporal é detectada, o pacote com headers e metadados técnicos é assinado e salvo para perícia.
* **Logs CSV/Texto:** `historico_comparativo.csv`, `regressoes_detectadas.csv` e `regressoes_detectadas.log`.

---

## 💻 Pré-requisitos

1. **Node.js**: Versão **22.x** ou **24.x** (requer suporte nativo ao módulo embutido `node:sqlite`).
2. **Google Chrome**: Instalado no caminho padrão (`C:\Program Files\Google\Chrome\Application\chrome.exe`).
3. **PowerShell**: Para execução dos scripts de inicialização (Windows).

---

## 🚀 Como Executar

### Opção 1: Inicialização Automática (Recomendado)
Abra o PowerShell na pasta do projeto e execute:
```powershell
.\iniciar_monitor.ps1
```
O script irá:
1. Verificar se o Chrome de depuração está ativo na porta `9222`. Se não estiver, inicializa o Chrome com perfil dedicado de debug apontando para o sistema de Resultados.
2. Iniciar o servidor Node.js `monitor_tdtot.js`.
3. Abrir o Dashboard em `http://127.0.0.1:3333`.

### Opção 2: Inicialização Manual
1. Inicie o Chrome em modo de depuração remota:
```powershell
& "C:\Program Files\Google\Chrome\Application\chrome.exe" --remote-debugging-port=9222 --user-data-dir="$env:LOCALAPPDATA\Google\Chrome\DebugProfile" --remote-allow-origins=* "https://resultados-sim.tse.jus.br/simulado/teste/app/index.html"
```
2. Inicie o monitor:
```powershell
node monitor_tdtot.js
```
3. Acesse no navegador:
   * **Dashboard em Tempo Real:** [http://127.0.0.1:3333](http://127.0.0.1:3333)
   * **Dossiê Forense de Auditoria:** [http://127.0.0.1:3333/dossie](http://127.0.0.1:3333/dossie)
   * **API de Métricas:** [http://127.0.0.1:3333/api/comparison](http://127.0.0.1:3333/api/comparison)

---

## 📊 Painel em Tempo Real (Dashboard Web)

O Dashboard (`http://127.0.0.1:3333`) atualiza automaticamente a cada 4 segundos via polling à API `/api/comparison`.

### Indicadores (Cards de KPI)
| Card | Indicador | Descrição |
| :--- | :--- | :--- |
| **Arquivos Monitorados** | Total | Total de arquivos do catálogo monitorados ativamente na sessão. |
| **Exibidos / Filtrados** | Total Visível | Número de arquivos exibidos no momento conforme os filtros ativos. |
| **Regressão Hora** | **Quantidade** | Total de regressões temporais reais detectadas no período. |
| | **Última Ocorrência** | Horário exato (`HH:mm:ss`) em que ocorreu a última regressão. |
| **Cache (SLAs Acumulados)** | **Atrasados Agora** | Quantidade instantânea de arquivos desatualizados no cache neste segundo. |
| | **Média** | Tempo médio que os arquivos levam para sincronizar da fonte ao cache. |
| | **P90** | 90% dos arquivos sincronizam neste tempo ou menos. |
| | **P95** | 95% dos arquivos sincronizam neste tempo ou menos. |
| | **P99** | 99% dos arquivos sincronizam neste tempo ou menos. |
| | **P100 (Máx)** | Tempo máximo de propagação registrado no dia (caso mais lento). |

> 💡 **Nota sobre os SLAs:** As métricas de SLA (`Média`, `P90`, `P95`, `P99`, `P100`) consideram **todos os eventos de sincronização ocorridos desde a 00:00:00 do dia atual**, recalculando dinamicamente ao aplicar filtros de UF ou busca na tabela.

### Colunas da Tabela de Auditoria
1. **Classificação:** Badge visual de criticidade (`NORMAL`, `CACHE_ATRASADO`, `REGRESSAO_TEMPORAL`, `ERRO_COLETA`).
2. **Caminho do Arquivo & Ações:** Caminho relativo (ex: `tdtot2026/21272/dados/sp/...json`) e botões rápidos para inspecionar o JSON diretamente na Fonte ou no Cache.
3. **FONTE (HMG) Data / Hora:** Data e hora de geração registradas na origem primária.
4. **CACHE (SIM) Data / Hora:** Data e hora de geração presentes no cache/CDN.
5. **Δ Tempo DG/HG (Min:Seg):** Diferença cronológica entre os dados da Fonte e do Cache em formato legível (ex: `0m 45s`).
6. **SLA Sync (DG/HG):** Tempo efetivo que a versão atual levou para propagar até o cache.
7. **⚡ Cache & TTL (HMG ➔ SIM):** Comparativo direto de headers HTTP que influenciam o cache (`Cache-Control`, `TTL/max-age`, status da CDN `Hit from child`/`Miss`, `ETag`, `Server`). Inclui botão **"🔍 Comparar Headers"** para inspeção side-by-side em modal.
8. **Integridade:** Status resumido (`OK`, `ATRASADO`, `REGRESSÃO`).

---

## 🌐 Monitoramento de Atributos de Cache HTTP e TTL (HMG vs SIM)

Para diagnosticar com precisão a causa de retenções de versão e inconsistências de entrega, o painel agora extrai e compara ativamente todos os atributos de cabeçalho HTTP responsáveis pelas políticas de cache:

### 1. KPI Card "Atributos de Cache & TTL (HMG vs SIM)"
Localizado no topo do dashboard, o card apresenta três métricas consolidadas em tempo real:
* **SIM Edge TTL:** Exibe o tempo de vida remanescente na borda Akamai (`max-age` médio, além da faixa mínima e máxima observada entre os arquivos, ex: `43s (35-59s)`).
* **CDN Hit Rate:** Porcentagem de requisições atendidas diretamente pelos PoPs de borda (`Hit from child`).
* **HMG Origem:** Status da diretiva na fonte primária (Apache sem header explícito de `Cache-Control`).

### 2. Inspeção Detalhada por Arquivo (Modal de Headers com Tooltips & Instâncias)
Ao clicar em **"🔍 Comparar Headers"** em qualquer linha da tabela, um modal abre o comparativo completo com **tooltips/hints interativos** (`?`) explicando a função técnica de cada atributo:
* **`Cache-Control`:** Instruções para caches intermediários (ex: `max-age`, `no-cache`). Mostra a ausência na fonte primária vs política na borda.
* **`TTL Estimado / Remanescente`:** Tempo de vida máximo e contagem regressiva de segundos até o descarte da réplica no Edge.
* **`CDN / Status`:** Rota de atendimento (`ORIGIN` direto ou `Hit from child`).
* **`ETag`:** Assinatura hash do conteúdo do arquivo, permitindo revalidação condicional rápida.
* **`Last-Modified`:** Carimbo de data/hora física da última alteração no storage.
* **`Instância (IP)`:** **IP real da máquina/servidor que atendeu o GET** (capturado via socket TCP na camada de rede e pelo Chrome DevTools CDP):
  * **HMG:** `192.168.218.33` (Servidor Apache de Origem).
  * **SIM:** IPs dos PoPs de borda Akamai (ex: `2.20.139.141`, `2.20.139.146`, `2.23.98.236`).
  * **Diagnóstico de Regressões:** Permite identificar com certeza matemática se uma versão retrocedeu porque a requisição seguinte caiu em um nó/PoP diferente da CDN que ainda retinha o arquivo antigo em cache.
* **`Server`:** Identificação do software web (`Apache` vs `Akamai CDN`).
* **💡 Análise de Impacto Diagnóstica:** Diagnóstico contextualizado e automático correlacionando os cabeçalhos e a instância física que atendeu a requisição.

---

## 📑 Dossiê Técnico de Evidências Forenses

Ao acessar a rota `/dossie` no navegador ou abrir o arquivo `relatorio_evidencias.html`, o sistema gera um laudo pericial contendo:
* Cabeçalho de auditoria com metodologia aplicada, data/hora da emissão e resumo de ambiente.
* Quadro com resumo consolidado de regressões reais encontradas na Fonte vs no Cache.
* Tabela de arquivos com seus deltas e SLAs de sincronização.
* Seção de evidências materiais vinculando cada regressão ao seu arquivo bruto correspondente em `evidencias_raw/`.

---

---

## 💾 Padrão de Salvamento Contínuo de Versões (Pasta `versoes/`)

Para possibilitar depuração visual imediata e análise comparativa direta entre a **Fonte (`HMG`)** e a **Borda/Cache (`SIM`)**, cada nova versão de arquivo identificada durante o monitoramento é gravada imediatamente em disco na pasta `versoes/`.

### 1. Hierarquia de Diretórios (Espelhamento da URL)
A estrutura de pastas reproduz com exatidão o caminho relativo da URL pública do arquivo (sem o domínio e sem os prefixos `/homologa/teste/` ou `/simulado/teste/`):

| URL de Origem | Estrutura de Diretório Gerada |
| :--- | :--- |
| `https://.../teste/tdtot2026/21272/dados/sp/sp...-u.json` | `versoes/tdtot2026/21272/dados/sp/` |
| `https://.../teste/tdtot2026/21270/dados/to/to-...-ab.json` | `versoes/tdtot2026/21270/dados/to/` |
| `https://.../teste/comum/ele-c.json` | `versoes/comum/` |

### 2. Nomenclatura dos Arquivos
O nome de cada arquivo incorpora os 4 metadados essenciais para auditoria cronológica e identificação de ambiente:

$\mathbf{\{nome\_base\}\_\{AMBIENTE\}\_dg\{DG\}\_hg\{HG\}\_idg\{IDG\}\{ext\}}$

* **`{nome_base}`:** Nome original do arquivo sem extensão (ex.: `sp71072-z0001-c0007-e021272-u` ou `to-e021272-ab`).
* **`{AMBIENTE}`:** Servidor que forneceu os dados no momento da coleta (`HMG` para Fonte, `SIM` para Cache/CDN).
* **`dg{DG}`:** Data de geração sanitizada com hífens no padrão brasileiro (ex.: `dg08-09-2026`).
* **`hg{HG}`:** Hora de geração sanitizada com hífens (ex.: `hg17-44-28`).
* **`idg{IDG}`:** Identificador sequencial global do arquivo (ex.: `idg139882161`).
* **`{ext}`:** Extensão do arquivo original (ex.: `.json`).

### 3. Exemplo Prático em Disco
```text
versoes/
└── tdtot2026/
    └── 21270/
        └── dados/
            └── ac/
                ├── ac-c0001-e021270-u_HMG_dg08-09-2026_hg18-18-18_idg139169269.json
                ├── ac-c0001-e021270-u_HMG_dg08-09-2026_hg18-18-42_idg139173777.json
                ├── ac-c0001-e021270-u_SIM_dg08-09-2026_hg18-17-01_idg139168988.json
                ├── ac-c0001-e021270-u_SIM_dg08-09-2026_hg18-18-18_idg139169269.json
                ├── ac-e021270-ab_HMG_dg08-09-2026_hg18-18-19_idg139170676.json
                └── ac-e021270-ab_SIM_dg08-09-2026_hg18-17-01_idg139167241.json
```

### 4. Benefícios Operacionais para a Equipe:
1. **Comparação *Side-by-Side* Imediata:** Como os arquivos da Fonte (`HMG`) e do Cache (`SIM`) residem na mesma pasta temática regional (`dados/sp/`, `dados/ac/`), os desenvolvedores podem selecioná-los e disparar um `diff` no VS Code, Beyond Compare ou WinMerge diretamente.
2. **Auditoria da Linha do Tempo:** Cada evolução ou retificação de totalização gera um novo arquivo com o carimbo temporal no próprio nome, preservando o histórico integral de cada passo da totalização.
3. **JSON Pronto e Formatado:** Cada arquivo contém o JSON integral indentado (2 espaços), facilitando a leitura imediata de seções, votos, candidatos e metadados.
4. **Distinção entre `versoes/` e `evidencias_raw/`:**
   * **`versoes/`**: Repositório completo e organizado de **todos os snapshots de versão** gerados ao longo da sessão.
   * **`evidencias_raw/`**: Dossiê focado em **incidentes de anomalia / regressão**, contendo pacotes assinados com headers HTTP completos para instrução pericial.

---


## 📦 Exportação Compactada de Versões (.ZIP) com Barra de Progresso

Para agilizar o download massivo e a análise offline das milhares de versões de arquivos capturados na pasta `versoes/`, o painel disponibiliza a funcionalidade **"Baixar Versões (ZIP)"**.

### 1. Funcionalidades da Exportação
* **Filtro por Eleição:** O usuário pode optar por baixar o pacote consolidado de **"Todas as Eleições"** ou filtrar apenas uma eleição específica (ex: `21270`, `21272`).
* **Estimativa Dinâmica:** Antes de iniciar, o modal exibe a quantidade exata de arquivos a serem compactados e a estimativa de tamanho final do arquivo compactado.
* **Barra de Progresso em Tempo Real:** O processo de leitura e compressão reporta periodicamente o estágio atual, porcentagem calculada (0% a 100%), contagem de arquivos processados e o nome do arquivo sendo empacotado no exato instante.
* **Download Automático:** Assim que a geração atinge 100%, o navegador inicia o download do arquivo `.zip` automaticamente, mantendo também um link direto para novo download caso necessário.

### 2. Arquitetura de Streaming e Desempenho (Zero Dependências Externas)
* **Motor Zip Nativo:** A compressão utiliza a API nativa de streaming do Node.js (`node:zlib` com `deflateRawSync` nível 1 e cálculo de CRC32), sem necessidade de pacotes externos do npm.
* **Gravador em Disco com Baixa Pegada de Memória:** O fluxo de dados é descarregado diretamente no disco em `temp_zips/` por meio de `fs.createWriteStream`, evitando sobrecarga de memória RAM mesmo ao compactar mais de 5.000 arquivos.
* **Não Bloqueante (Event-Loop Friendly):** O gerador cede controle ciclicamente ao *event loop* do Node.js a cada bloco de arquivos via `setImmediate`, garantindo que as rotinas de varredura contínua de rede e a atualização do dashboard não sofram interrupção.

---


## 📍 Delimitador Lógico de Rodadas (Checkpoints de Teste)

Para permitir a realização de novos testes e simulados em dias subsequentes ou em diferentes horários do mesmo dia sem misturar métricas nem gerar falsos alarmes de regressão, o sistema adota o conceito de **Rodadas como Delimitadores Lógicos Temporais**.

### 1. Como Funciona
* **Zero Perda de Dados:** Nenhum registro é apagado do banco de dados SQLite nem da pasta de versões. O histórico permanece 100% preservado para perícia.
* **Marco Zero Instantâneo:** Ao iniciar ou trocar de rodada, o sistema estabelece um novo carimbo temporal de corte. Apenas leituras, regressões e sincronizações ocorridas **a partir do início da rodada ativa** são computadas nos KPIs do painel.
* **Eliminação de Falsas Regressões:** Quando uma nova rodada começa, a memória de comparação é reiniciada. A primeira leitura de cada arquivo na nova rodada é tratada como "leitura inicial", evitando que uma eleição reiniciada com horários anteriores acione alarmes indevidos contra o final do teste anterior.
* **Novo Dia Automático:** Por padrão, a virada de dia (00:00:00) já cria e ativa automaticamente uma nova rodada do dia.

### 2. Controles Disponíveis no Painel Web
* **Badge no Cabeçalho:** Mostra a rodada ativa no momento e o horário exato em que foi iniciada.
* **Botão `➕ Nova`:** Cria instantaneamente uma nova rodada com o horário do clique, zerando os contadores para um novo teste.
* **Botão `⚙️ Gerenciar`:** Abre o modal de gerenciamento permitindo:
  * Ativar rodadas anteriores para rever métricas passadas.
  * Editar o nome da rodada (ex: *"Carga 50% Urnas"* ou *"Simulado Tarde"*).
  * Ajustar retroativamente o horário de início (formato ISO).
  * Excluir marcos de rodadas desnecessários.
* **Integração com Exportação ZIP:** No modal de download ZIP, é possível marcar a opção **"Compactar apenas arquivos gerados na rodada ativa"**, gerando um pacote exclusivo da rodada em andamento.

---


## 🗳️ Descoberta e Monitoramento Multi-Eleições

Para atender à totalidade dos pleitos simultâneos disponibilizados no combo oficial de **Resultados do TSE**, o monitor conta com um motor autônomo de **Descoberta Dinâmica de Eleições**.

### 1. Descoberta Automática de Catálogo
* Ao iniciar (e ciclicamente a cada 60 segundos), o sistema consulta o arquivo mestre `comum/config/ele-c.json` e os arquivos de configuração municipal (`mun-e<codigo>-cm.json`).
* O sistema identifica automaticamente todos os pleitos e eleições configurados no ambiente (eleições federais, estaduais e municipais, como `21270`, `21272`, `21274`, `21125`, `21127`, `21129`, `10143`, etc.).

### 2. Escolha e Ativação no Painel Web
* **Badge no Cabeçalho:** Mostra a contagem de eleições ativas no momento (ex.: `🗳️ Eleições: 2 de 7 ativas (201 arqs)`).
* **Botão `⚙️ Escolher`:** Abre o modal de configuração de eleições permitindo:
  * **Monitorar Todas Simultaneamente:** O botão *"✓ Selecionar Todas"* ativa instantaneamente a varredura concorrente de todas as eleições existentes (expandindo o catálogo para mais de 500 arquivos).
  * **Foco em Eleições Específicas:** O usuário pode marcar ou desmarcar checkboxes individuais para monitorar apenas as eleições de interesse na rodada (ex: focar apenas nas eleições municipais suplementares ou apenas na eleição federal).
  * **Persistência das Escolhas:** A seleção de eleições ativas é salva no banco SQLite (`eleicoes_monitoradas`), sendo mantida entre reinicializações do servidor.
* **Filtros Sincronizados:** O combo de filtro *"Eleição"* da tabela do Dashboard é populado automaticamente com os nomes oficiais e códigos de todas as eleições descobertas.

---

## 🖥️ Topologia Flexível e Auditoria Multi-Nós (N-Vias)

O sistema suporta a **auditoria contínua de múltiplos servidores em paralelo (N-Vias)**, permitindo cadastrar, ativar/desativar e comparar quaisquer nós da infraestrutura do TSE (ex.: `HMG (Origem)`, `SIM (Akamai CDN)`, `EDGE-SP`, `EDGE-DF`, `Slave 1`, `Slave 2`, `Interno` e `Externo`).

```
                              [ NÓ DE ORIGEM (Master) ]
                                (Ex: HMG / Homologação)
                                         |
               +-------------------------+-------------------------+
               |                         |                         |
               v                         v                         v
     [ RÉPLICA 1 (SIM) ]       [ RÉPLICA 2 (SLAVE1) ]     [ RÉPLICA 3 (EDGE) ]
      (Akamai Borda SP)         (Servidor Interno DF)     (Nó Secundário RJ)
```

### 1. Papéis Arquiteturais (Origem vs Réplicas)
* **⭐ ORIGEM (Master de Referência):** Nó canônico onde os arquivos são gerados na totalização. Todos os deltas temporais ($\Delta$ DG/HG) e SLAs de sincronização são calculados em relação a ele. Qualquer servidor cadastrado pode ser promovido a Origem com 1 clique.
* **📡 RÉPLICA (Caches, Bordas e Slaves):** Servidores de distribuição que replicam a origem. O motor afere individualmente se a réplica está sincronizada, pendente, atrasada ou com regressão de borda.

### 2. Funcionalidades do Gerenciador de Servidores (`🖥️ Servidores`)
* **Cadastro e Edição (CRUD):** Formulário no modal do Dashboard para adicionar novos nós com Chave, Nome Descritivo, Base URL e Papel.
* **Teste de Conectividade em Tempo Real:** Botão *"📡 Testar"* que realiza um GET direto na URL do servidor, calculando a latência (ms), o status HTTP retornado e resolvendo o **IP da instância servidora** e os cabeçalhos de resposta (`Server`, `Cache-Control`).
* **Promoção Instantânea de Origem:** Botão *"⭐ Tornar Origem"* que transfere o papel master para outro nó, recalibrando automaticamente todos os cálculos de SLAs.
* **Pausar / Ativar Coleta:** Controle individual de polling sem necessidade de deletar o servidor configurado.

### 3. Exibição Adaptativa na Tabela de Auditoria
* **Modo Padrão (2 Servidores):** Mantém a visualização clássica com colunas dedicadas para a Origem (`HMG`) e a Réplica (`SIM`).
* **Modo Multi-Nós (3 ou mais Servidores):** A tabela sintetiza os nós em uma coluna consolidada de réplicas com badges individuais por nó (`SIM: ⚡ 0m 00s`, `SLAVE1: ⏱️ -15s`), além do botão **`🔍 Matriz Multi-Nós`** que abre uma tabela comparativa ampliada inspecionando todos os servidores simultaneamente lado a lado para aquele arquivo (IP, DG/HG, IDG, Delta, SLA, TTL e ETag).

### 4. Endpoints REST da API
* `GET /api/servidores`: Retorna a lista de servidores cadastrados e indica qual é a Origem ativa.
* `POST /api/servidores/salvar`: Cria ou atualiza um nó (`chave`, `nome`, `baseUrl`, `papel`, `ativo`).
* `POST /api/servidores/toggle`: Ativa ou pausa o polling de um servidor.
* `POST /api/servidores/definir-origem`: Define qual nó será o Master de Referência.
* `POST /api/servidores/excluir`: Remove nós secundários customizados.
* `POST /api/servidores/testar`: Dispara sonda HTTP contra a URL e retorna latência, IP resolvido e status.

---

## 🔬 Decisões de Engenharia e Peculiaridades Técnicas

### 1. O Caso da Sequence `IDG` no Cluster Oracle RAC
* **Comportamento Observado:** Durante os primeiros testes, o sistema acusava milhares de falsas "regressões" no campo `idg` (Identificador Global).
* **Causa Raiz Identificada:** O banco de dados de totalização opera em cluster **Oracle Real Application Clusters (RAC)** com instâncias ativas simultâneas (Exadata). Sequences em cluster utilizam blocos de cache em memória independentes por nó (ex: `CACHE 20` ou `50`). Quando o Nó 1 e o Nó 2 geram arquivos em paralelo, os números da sequence sofrem intercalação natural (ex: Nó 1 emite `101`, Nó 2 emite `121`, e depois Nó 1 emite `102`).
* **Decisão Arquitetural:** Alterar a sequence para `NOCACHE` ou `ORDER` em produção geraria forte contenção de enfileiramento (`row cache lock`), derrubando a performance de escrita da totalização. Por conseguinte, **o IDG foi mantido nos arquivos apenas para rastreabilidade, sendo removido da lógica de regressão**. A verdade cronológica estrita do versionamento é orientada unicamente pelos campos **`dg` (Data de Geração)** e **`hg` (Hora de Geração)**.

### 2. Regressões Reais de Borda na CDN (SIM)
* **Comportamento Observado:** O ambiente de homologação (HMG) teve apenas 2 regressões pontuais em todo o dia, enquanto a camada de cache (SIM) registrou centenas de oscilações retroativas.
* **Causa Raiz Identificada:** A camada de CDN/Cache (SIM) é composta por múltiplos nós e servidores de borda (*Edge Points of Presence - PoPs*). Uma requisição em um segundo atinge um PoP já invalidado e atualizado; a requisição seguinte pode atingir um nó com cache intermediário ainda não expirado, servindo uma versão retroativa de minutos antes.
* **Solução:** O sistema audita essas ocorrências como **Regressão de Borda no Cache**, registrando-as como evidência pericial no dossiê.

### 3. Blindagem da Métrica de SLA (*First-Seen*)
* Para evitar que uma regressão de borda posterior (ocorrida, por exemplo, 50 minutos após o arquivo ter sido publicado) seja erroneamente computada como "tempo de propagação inicial do arquivo", a métrica de SLA utiliza uma query com CTE (*Common Table Expression*) que cruza estritamente a **primeira aparição (*First Seen*) daquela versão no HMG com a primeira aparição no SIM**.

---

## 📂 Estrutura do Repositório

```text
├── monitor_tdtot.js             # Aplicação principal (Coletor, Motor de Auditoria e Web Server)
├── iniciar_monitor.ps1          # Script PowerShell de inicialização rápida (Chrome CDP + Node)
├── terminal_feed.ps1            # Script auxiliar para acompanhamento via console
├── inspect_page_state.js        # Utilitário de inspeção de estado do DOM via CDP
├── inspect_tabs.js              # Utilitário de listagem de abas ativas do Chrome
├── test_parse.js                # Validador de parsing de data/hora cronológica
├── relatorio_evidencias.html    # Dossiê estático e viewer web de auditoria forense
├── package.json                 # Manifesto da aplicação (versão, scripts e metadados)
├── CHANGELOG.md                 # Histórico de alterações e releases (SemVer)
├── LICENSE                      # Licença de uso MIT
├── MINI_MANUAL_ACESSO_REDE.md   # Manual de acesso em rede interna e firewall
├── .gitignore                   # Regras de exclusão de artefatos temporários e bancos
├── .gitattributes              # Normalização de finais de linha e binários
└── README.md                    # Documentação técnica completa

# Artefatos gerados em tempo de execução (protegidos pelo .gitignore):
├── tdtot_auditoria.db           # Banco de dados SQLite persistente (WAL mode)
├── historico_comparativo.csv    # Exportação em CSV do histórico comparativo
├── regressoes_detectadas.csv    # Exportação em CSV das regressões registradas
├── regressoes_detectadas.log    # Log textual das anomalias cronológicas
├── evidencias_raw/              # Dossiê contendo os payloads JSON brutos das anomalias
├── versoes/                     # Repositório contínuo de snapshots na estrutura de URL
└── temp_zips/                   # Armazenamento temporário de arquivos ZIP de download
```

---

**Auditoria Técnica de Resultados TDTot TSE**  
*Desenvolvido para monitoramento de alta precisão, garantia de integridade de dados e medição de SLAs de entrega das eleições.*
