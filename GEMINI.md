# Diretrizes Críticas de Segurança do Sistema (Regra Permanente)

## 🚨 Proibição Absoluta de Comandos Destrutivos

1. **Proibição Total de `cmd.exe /c` e `rmdir /s`**:
   - É terminantemente **PROIBIDO** executar comandos de exclusão recursiva em lote via `cmd.exe /c`, `rmdir /s /q`, `del /s`, ou qualquer wrapper de shell com caminhos interpolados ou variáveis de ambiente.
   - Motivo: O parser do PowerShell e do CMD interpretam incorretamente aspas escapadas, transformando a barra invertida inicial no diretório raiz da unidade (`D:\`), gerando risco de varredura e deleção em massa do disco.

2. **Deleção e Limpeza de Arquivos**:
   - O agente **NUNCA** deve executar exclusões de pastas ou diretórios inteiros por conta própria.
   - Toda e qualquer limpeza de arquivos de cache, logs ou dados temporários deve ser:
     - **Delegada ao usuário** para execução manual; OU
     - Se explicitamente solicitada pelo usuário, restrita a arquivos individuais estritamente identificados, utilizando `Remove-Item -LiteralPath` com validação de caminho absoluto único, **JAMAIS** utilizando wildcards (`*`) ou exclusões recursivas em lote.

3. **Validação Rígida de Escopo**:
   - Nenhuma operação de sistema de arquivos pode ultrapassar os limites do diretório específico do projeto (`D:\OneDrive - TRIBUNAL SUPERIOR ELEITORAL\_Work\TDTot`).
