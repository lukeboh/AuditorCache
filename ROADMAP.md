# 🗺️ Roadmap de Evoluções e Melhorias Futuras

Este documento centraliza as funcionalidades planejadas, melhorias de interface e evoluções arquiteturais para o **AuditorCache / Monitor TDTot TSE**.

---

## 📋 Backlog / Em Planejamento

### 🎛️ Interface e Filtros

- [ ] **Comboboxes com Checkboxes (Seleção Múltipla) nos Filtros de Coluna**
  - **Contexto:** Atualmente, os seletores/filtros de coluna operam em modo de seleção única (ou valor exato / "Todos").
  - **Objetivo:** Permitir que os comboboxes de filtro das colunas (ex.: Eleição, UF, Cargo, Tipo de Arquivo, Status) passem a ter checkboxes integrados em dropdown multi-seleção (*multiselect dropdown with checkboxes*).
  - **Benefício:** Permitir combinações arbitrárias de seleção na auditoria (por exemplo: visualizar simultaneamente as UFs `SP`, `RJ` e `MG`, ou combinar múltiplos status de integridade como `Atrasado` + `Regressão`).
  - **Impacto:** Alteração no componente de filtro da tabela do Dashboard (`monitor_tdtot.js`) e sincronização dos predicados de filtragem na renderização da matriz.

---

## 💡 Sugestões e Próximos Passos
*Novas propostas e solicitações podem ser adicionadas neste documento antes de entrarem nas sprints de implementação.*
