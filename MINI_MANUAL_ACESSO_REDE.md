# 🗳️ Mini Manual de Acesso e Uso - Monitor TDTot TSE

> **Servidor Anfitrião:** Estação `tsesetot48` (Tribunal Superior Eleitoral)  
> **IP da Estação:** `10.30.64.17`  
> **Porta de Serviço:** `3333`

---

## 🌐 1. Como Acessar a Aplicação

Qualquer pessoa conectada à rede interna do TSE (ou via VPN corporativa) pode acessar o monitor diretamente pelo navegador web (Google Chrome, Microsoft Edge, Firefox, etc.) através de um dos links abaixo:

* **Pelo nome da máquina (Recomendado):**  
  👉 [http://tsesetot48:3333](http://tsesetot48:3333)

* **Pelo nome de domínio completo (FQDN):**  
  👉 [http://tsesetot48.tse.jus.br:3333](http://tsesetot48.tse.jus.br:3333)

* **Pelo endereço IP direto:**  
  👉 [http://10.30.64.17:3333](http://10.30.64.17:3333)

---

## 🎯 2. O que é o Monitor TDTot?

O **Monitor TDTot** é uma ferramenta de auditoria pericial contínua e em tempo real desenvolvida para acompanhar a publicação e a propagação dos arquivos da totalização das Eleições.

A aplicação audita:
1. **Integridade Cronológica (DG/HG):** Garante que os arquivos publicados avançam sempre para a frente no tempo, alertando imediatamente caso ocorra qualquer retrocesso temporal (regressão de borda).
2. **SLA de Propagação do Cache:** Mede com precisão de segundos quanto tempo leva para um arquivo gerado na **Origem (HMG)** se tornar visível aos eleitores e sistemas externos na camada de **Cache/CDN (SIM / Akamai)**.
3. **Auditoria Multi-Nós (N-Vias):** Permite acompanhar múltiplos servidores em paralelo (HMG, SIM, réplicas secundárias, nós internos e externos).

---

## 🖥️ 3. Como Usar o Dashboard (Passo a Passo)

### 3.1 Painel Superior de Indicadores (KPIs)
Ao abrir a tela, o cabeçalho apresenta os seguintes números consolidados em tempo real:
* **Arquivos Monitorados:** Quantidade total de arquivos no catálogo ativo de varredura.
* **Exibidos / Filtrados:** Quantidade de arquivos visíveis após aplicação de filtros.
* **Regressão Hora:** Quantidade de regressões temporais reais identificadas na rodada atual e o horário do último evento.
* **Cache (SLAs Acumulados):**
  * **Atrasados:** Quantidade de arquivos desatualizados no cache no exato segundo atual.
  * **Média / P90 / P95 / P100:** Tempos percentílicos de propagação de todos os arquivos desde o início da rodada.
* **Atributos de Cache & TTL:** TTL remanescente na CDN, taxa de acerto de cache (*CDN Hit Rate*) e headers da origem.

---

### 3.2 Tabela Comparativa de Arquivos
Cada linha da tabela representa um arquivo de totalização (`-u.json`, `-ab.json`, etc.):
* **Classificação:** Eleição, UF, Cargo e Nível (Nacional, Estadual, Municipal ou Zona).
* **Caminho do Arquivo:** Nome do arquivo com botões diretos para abrir o JSON bruto ou copiar o link.
* **Origem (Master):** Data e hora (`DG/HG`) de geração na fonte oficial, com o identificador de totalização (`IDG`).
* **Réplica (Cache / SIM):** Data e hora recebidas no nó de distribuição.
* **Δ Tempo DG/HG:** Diferença entre a data/hora da Origem e da Réplica (`0m 00s` = perfeitamente sincronizado; `-15s` = réplica 15 segundos atrasada).
* **SLA Sync:** Tempo real que a réplica levou para receber a versão gerada na origem (`⚡ 0m 00s`).
* **Cache & TTL:** TTL configurado, IP da instância que atendeu a requisição e botão **`🔍 Comparar Headers`**.
* **Integridade:** Status de integridade (`OK` ou `🚨 REGRESSÃO DETECTADA`).

---

### 3.3 Barra de Ações Rápidas no Cabeçalho

1. **🗳️ Eleições (`⚙️ Escolher`):**
   * Permite selecionar quais eleições monitorar (Federal 1º Turno `21270`, Estadual `21272`, Municipais, etc.).
   * O botão *"✓ Selecionar Todas"* ativa o monitoramento concorrente de todas as eleições cadastradas.

2. **📍 Rodada (`⚙️ Gerenciar` / `➕ Nova`):**
   * Delimita um novo marco lógico de testes (zerando contadores de regressão e médias de SLA) sem apagar o histórico persistido no banco SQLite.
   * Testes em dias diferentes iniciam automaticamente uma nova rodada.

3. **🖥️ Servidores (`⚙️ Gerenciar`):**
   * Permite cadastrar novos servidores/nós (ex.: nós internos, externos, slaves e bordas regionais).
   * Botão *"📡 Testar"* para testar latência HTTP e descobrir o IP da instância.
   * Botão *"⭐ Tornar Origem"* para definir qual nó atuará como Master de Referência.

4. **📦 Baixar Versões (ZIP):**
   * Abre o assistente para empacotar e baixar todos os arquivos JSON de snapshots salvos na pasta `versoes/`.
   * Inclui filtro por eleição e barra de progresso em tempo real durante a compactação.

5. **⚙️ Exportar / Dados:**
   * **📄 Abrir Dossiê Forense (HTML):** Relatório pericial formatado para impressão ou arquivamento técnico.
   * **💾 Baixar Banco SQLite (`tdtot_auditoria.db`):** Base completa de leituras em formato SQLite para análises em BI/Python/R.
   * **📊 Baixar CSVs:** Planilhas tabulares de regressões e de propagação minuto a minuto.

---

## 🔍 4. Resolução de Problemas (Troubleshooting)

### A página não carrega no navegador de outro computador?
1. **Verifique se a estação `tsesetot48` está ligada e com o monitor rodando:**  
   O processo Node.js precisa estar em execução na estação anfitriã.
2. **Teste o acesso pelo IP direto:**  
   Se o DNS corporativo não resolver o nome `tsesetot48`, tente diretamente pelo endereço:  
   `http://10.30.64.17:3333`
3. **Firewall do Windows na estação `tsesetot48`:**  
   Caso o Windows Defender Firewall bloqueie conexões de entrada na porta 3333, execute no PowerShell como Administrador:  
   ```powershell
   New-NetFirewallRule -DisplayName "Monitor TDTot TSE (3333)" -Direction Inbound -LocalPort 3333 -Protocol TCP -Action Allow
   ```
