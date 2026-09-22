const fs = require('fs');
const path = require('path');

function getBaseUrl(serverKey, serverMap) {
  if (serverMap && serverMap[serverKey] && serverMap[serverKey].base_url) return serverMap[serverKey].base_url;
  const k = (serverKey || '').toUpperCase();
  if (k.startsWith('HMG')) return 'https://resultados-hmg.tse.jus.br/simulado2026/';
  if (k.startsWith('SIM')) return 'https://resultados-sim.tse.jus.br/simulado/simulado2026/';
  return 'https://resultados-sim.tse.jus.br/simulado/simulado2026/';
}

function isOriginServer(serverKey, serverMap, papelServidor) {
  if (papelServidor && (papelServidor.includes('ORIGEM') || papelServidor.includes('FONTE'))) return true;
  if (serverMap && serverMap[serverKey] && serverMap[serverKey].papel === 'ORIGEM') return true;
  if (!serverKey) return false;
  const s = String(serverKey).toUpperCase();
  return s.startsWith('HMG');
}

function isReplicaServer(serverKey, serverMap, papelServidor) {
  return !isOriginServer(serverKey, serverMap, papelServidor);
}

function getServerRoleLabel(serverKey, serverMap, papelServidor) {
  if (isOriginServer(serverKey, serverMap, papelServidor)) return 'FONTE / ORIGEM';
  if (papelServidor) return papelServidor;
  if (serverMap && serverMap[serverKey] && serverMap[serverKey].nome) return serverMap[serverKey].nome;
  return 'Simulado (Cache Akamai)';
}

function parseFileMetadata(relPath) {
  let pleito = '-';
  let eleicao = 'Geral';
  let uf = 'BR';
  let tipo = 'Outro';
  let sufixo = '-';
  let cargo = '-';
  let nivel = 'Geral';

  if (relPath.includes('ele-c.json')) {
    return { pleito: 'Todos', eleicao: 'Comum', uf: 'BR', tipo: 'Configuração Geral', sufixo: '-c.json', cargo: '-', nivel: 'Geral' };
  }

  const mCs = relPath.match(/arquivo-urna\/(\d+)\/config\/([a-z]{2})\/([a-z]{2})-p\d+-cs\.json/i);
  if (mCs) {
    pleito = mCs[1];
    uf = mCs[2].toUpperCase();
    tipo = 'Comum Simples (cs)';
    sufixo = '-cs.json';
    cargo = '-';
    nivel = 'Seções / Urnas';
    eleicao = pleito;
    return { pleito, eleicao, uf, tipo, sufixo, cargo, nivel };
  }

  const mEle = relPath.match(/\/(\d{5})\//);
  if (mEle) {
    eleicao = mEle[1];
    if (eleicao === '21270' || eleicao === '21272' || eleicao === '21274') {
      pleito = '17801';
    } else if (eleicao === '21125' || eleicao === '21127' || eleicao === '21129') {
      pleito = '17696';
    } else if (eleicao === '10143') {
      pleito = '8758';
    } else if (eleicao === '9240' || eleicao === '9238') {
      pleito = '8267';
    }
  }

  const mUf = relPath.match(/\/dados\/([a-z]{2})\//);
  if (mUf) uf = mUf[1].toUpperCase();

  const fn = relPath.split('/').pop();

  if (fn.includes('-cm.json')) {
    tipo = 'Configuração Municípios';
    sufixo = '-cm.json';
    cargo = '-';
    nivel = 'Eleição';
    uf = '-';
  } else if (fn.includes('-cs.json')) {
    tipo = 'Comum Simples (cs)';
    sufixo = '-cs.json';
    cargo = '-';
    nivel = 'Seções / Urnas';
  } else if (fn.includes('-ab.json')) {
    tipo = 'Boletim Aberto (ab)';
    sufixo = '-ab.json';
    if (fn.startsWith('br-')) { nivel = 'Nacional'; uf = 'BR'; }
    else if (fn.includes('-z')) nivel = 'Zona';
    else nivel = 'Estadual';
  } else if (fn.includes('-u.json')) {
    tipo = 'Unificada (u)';
    sufixo = '-u.json';
    const mCargo = fn.match(/-c(\d{4})-/);
    if (mCargo) {
      const cCode = parseInt(mCargo[1], 10);
      const cargos = {
        1: 'Presidente',
        3: 'Governador',
        5: 'Senador',
        6: 'Dep. Federal',
        7: 'Dep. Estadual',
        8: 'Dep. Distrital',
        11: 'Prefeito',
        13: 'Vereador',
        25: 'Conselheiro Distrital'
      };
      cargo = cargos[cCode] || ('Cargo ' + cCode);
    }
    if (fn.startsWith('br-')) { nivel = 'Nacional'; uf = 'BR'; }
    else if (fn.includes('-z')) nivel = 'Zona';
    else if (fn.match(/^[a-z]{2}\d{5}/)) nivel = 'Município';
    else nivel = 'Estadual';
  } else if (fn.includes('-c.json')) {
    tipo = 'Configuração';
    sufixo = '-c.json';
  }

  return { pleito, eleicao, uf, tipo, sufixo, cargo, nivel };
}

function buildCausalChainDataset(db, targetRodada, options = {}) {
  const rodadaStartIso = targetRodada.inicio_iso;
  const rodadaEndIso = targetRodada.fim_iso;

  const servers = db.prepare('SELECT * FROM servidores_monitorados').all();
  const serverMap = {};
  servers.forEach(s => { serverMap[s.chave] = s; });

  let regSql = 'SELECT * FROM regressoes WHERE timestamp_iso >= ? ';
  const regParams = [rodadaStartIso];
  if (rodadaEndIso) {
    regSql += 'AND timestamp_iso <= ? ';
    regParams.push(rodadaEndIso);
  }
  if (options.criterio) {
    regSql += 'AND criterio LIKE ? ';
    regParams.push('%' + options.criterio + '%');
  } else {
    regSql += "AND (criterio LIKE '%TEMPO (DG/HG)%') ";
  }
  if (options.servidor) {
    regSql += 'AND servidor = ? ';
    regParams.push(options.servidor);
  }
  if (options.q) {
    const qClean = options.q.replace(/^#/, '');
    regSql += 'AND (id = ? OR arquivo LIKE ? OR motivo LIKE ? OR akamai_grn LIKE ?) ';
    regParams.push(parseInt(qClean, 10) || -1, '%' + qClean + '%', '%' + qClean + '%', '%' + qClean + '%');
  }

  regSql += 'ORDER BY id ASC';

  const regressoesRows = db.prepare(regSql).all(...regParams);

  const allRegs = db.prepare('SELECT timestamp_iso, arquivo FROM regressoes').all();
  const regSet = new Set(allRegs.map(r => r.arquivo + '|' + r.timestamp_iso));

  const stmtLeituras = db.prepare('SELECT * FROM leituras WHERE arquivo = ? ORDER BY id ASC');
  const leiturasCache = new Map();

  const occurrences = [];

  for (let i = 0; i < regressoesRows.length; i++) {
    const reg = regressoesRows[i];
    const occIndex = i + 1;
    const meta = parseFileMetadata(reg.arquivo);

    let allL = leiturasCache.get(reg.arquivo);
    if (!allL) {
      allL = stmtLeituras.all(reg.arquivo);
      leiturasCache.set(reg.arquivo, allL);
    }

    let regLIdx = allL.findIndex(l => l.arquivo === reg.arquivo && (l.timestamp_iso === reg.timestamp_iso || l.call_time_iso === reg.timestamp_iso));
    if (regLIdx === -1) {
      const regTime = new Date(reg.timestamp_iso).getTime();
      let bestDiff = Infinity;
      allL.forEach((l, idx) => {
        const lTime = l.timestamp_unix || new Date(l.timestamp_iso || l.call_time_iso).getTime();
        const diff = Math.abs(lTime - regTime);
        if (diff < bestDiff && l.servidor === reg.servidor) {
          bestDiff = diff;
          regLIdx = idx;
        }
      });
    }

    const windowIndices = [];
    if (regLIdx !== -1) {
      for (let w = regLIdx - 4; w <= regLIdx + 2; w++) {
        if (w >= 0 && w < allL.length) windowIndices.push(w);
      }
    }

    const requests = [];

    let peakSimIdxInWindow = -1;
    for (let wIdx = 0; wIdx < windowIndices.length; wIdx++) {
      const globalIdx = windowIndices[wIdx];
      if (globalIdx < regLIdx) {
        const item = allL[globalIdx];
        if (isReplicaServer(item.servidor, serverMap, item.papel_servidor) && !regSet.has(item.arquivo + '|' + item.timestamp_iso)) {
          peakSimIdxInWindow = wIdx;
        }
      }
    }

    for (let wIdx = 0; wIdx < windowIndices.length; wIdx++) {
      const globalIdx = windowIndices[wIdx];
      const l = allL[globalIdx];
      let respHeaders = {};
      try { if (l.headers_json) respHeaders = JSON.parse(l.headers_json); } catch(e) {}
      let reqHeaders = {};
      try { if (l.request_headers_json) reqHeaders = JSON.parse(l.request_headers_json); } catch(e) {}

      let status_category = '';
      let status_badge = '';
      let status_class = '';
      let is_regression = false;

      const isOrigin = isOriginServer(l.servidor, serverMap, l.papel_servidor);

      if (isOrigin) {
        status_category = 'ORIGEM';
        const serverDisplay = l.servidor.startsWith('HMG') ? 'HMG' : l.servidor;
        status_badge = '🟣 ORIGEM (' + serverDisplay + ')';
        status_class = 'badge-origin';
      } else if (regSet.has(l.arquivo + '|' + l.timestamp_iso)) {
        status_category = 'REGRESSAO';
        status_badge = '🚨 REGRESSÃO DETECTADA';
        status_class = 'badge-regression';
        is_regression = true;
      } else if (globalIdx < regLIdx) {
        if (wIdx === peakSimIdxInWindow) {
          status_category = 'PICO_ANTERIOR';
          status_badge = '📌 REF. ANTERIOR (PICO)';
          status_class = 'badge-peak';
        } else {
          status_category = 'HISTORICO';
          status_badge = '⚪ HISTÓRICO PRÉVIO';
          status_class = 'badge-history';
        }
      } else {
        status_category = 'RECUPERACAO';
        status_badge = '✓ RECUPERAÇÃO / NORMALIZAÇÃO';
        status_class = 'badge-recovery';
      }

      const rawMaxAge = respHeaders['cache-control'] ? respHeaders['cache-control'].match(/max-age=(\d+)/) : null;
      const maxAgeStr = rawMaxAge ? (rawMaxAge[1] + 's') : (l.max_age ? (l.max_age + 's') : '-');

      const baseUrl = getBaseUrl(l.servidor, serverMap);
      const url = baseUrl + l.arquivo;

      let cdnCacheStatus = '-';
      if (isOrigin) {
        cdnCacheStatus = 'ORIGIN (Apache)';
      } else {
        cdnCacheStatus = respHeaders['cdn-cache-status'] || l.cdn_status || '-';
      }

      let contentLen = '-';
      if (respHeaders['content-length']) {
        contentLen = respHeaders['content-length'] + ' B';
      }

      requests.push({
        occurrence_id: reg.id,
        occurrence_index: occIndex,
        leitura_id: l.id,
        servidor: l.servidor,
        papel_servidor: getServerRoleLabel(l.servidor, serverMap, l.papel_servidor),
        status_badge,
        status_class,
        status_category,
        is_regression,
        hg: l.hg || '-',
        dg: l.dg || '-',
        idg: l.idg || '-',
        ht: l.ht || '-',
        dt: l.dt || '-',
        secoes: (l.secoes !== null && l.secoes !== undefined && l.secoes !== '') ? String(l.secoes) : '-',
        timestamp_iso: l.timestamp_iso || l.call_time_iso,
        timestamp_unix: l.timestamp_unix || l.call_time_unix || new Date(l.timestamp_iso || l.call_time_iso).getTime(),
        url,
        arquivo: l.arquivo,
        last_modified: respHeaders['last-modified'] || '-',
        etag: respHeaders['etag'] || l.etag || '-',
        cache_control: respHeaders['cache-control'] || l.cache_control || '-',
        max_age: maxAgeStr,
        akamai_grn: respHeaders['akamai-grn'] || l.akamai_grn || '-',
        cdn_cache_status: cdnCacheStatus,
        x_server_ip: respHeaders['x-server-ip'] || l.server_ip || '-',
        age: respHeaders['age'] || '-',
        expires: respHeaders['expires'] || '-',
        date_http: respHeaders['date'] || '-',
        server_http: respHeaders['server'] || '-',
        content_length: contentLen,
        content_encoding: respHeaders['content-encoding'] || '-',
        latency_ms: l.latency_ms || 0,
        all_response_headers: respHeaders,
        all_request_headers: reqHeaders,
        file_meta: {
          arquivo: l.arquivo,
          eleicao: meta.eleicao,
          uf: meta.uf,
          cargo: meta.cargo,
          tipo: meta.tipo
        }
      });
    }

    occurrences.push({
      id: reg.id,
      index: occIndex,
      arquivo: reg.arquivo,
      servidor: reg.servidor,
      timestamp_iso: reg.timestamp_iso,
      timestamp_unix: reg.call_time_unix || new Date(reg.timestamp_iso).getTime(),
      criterio: reg.criterio,
      motivo: reg.motivo,
      detalhes: reg.detalhes,
      dg_anterior: reg.dg_anterior,
      hg_anterior: reg.hg_anterior,
      idg_anterior: reg.idg_anterior,
      dt_anterior: reg.dt_anterior,
      ht_anterior: reg.ht_anterior,
      secoes_anterior: reg.secoes_anterior ? String(reg.secoes_anterior) : '0',
      dg_recebido: reg.dg_recebido,
      hg_recebido: reg.hg_recebido,
      idg_recebido: reg.idg_recebido,
      dt_recebido: reg.dt_recebido,
      ht_recebido: reg.ht_recebido,
      secoes_recebido: reg.secoes_recebido ? String(reg.secoes_recebido) : '0',
      file_meta: {
        arquivo: reg.arquivo,
        eleicao: meta.eleicao,
        uf: meta.uf,
        cargo: meta.cargo,
        tipo: meta.tipo
      },
      requests
    });
  }

  const rawData = occurrences.flatMap(o => o.requests);

  return { occurrences, rawData, serverMap };
}

function generateOfflineForensicReportHtml(db, targetRodada, options = {}) {
  const { occurrences, rawData, serverMap } = buildCausalChainDataset(db, targetRodada, options);

  const rodadaNum = targetRodada.id || (targetRodada.nome ? targetRodada.nome.replace(/\D+/g, '') : 1);
  let rodadaInicioFormatted = targetRodada.inicio_iso;
  try {
    const d = new Date(targetRodada.inicio_iso);
    rodadaInicioFormatted = d.toLocaleString('pt-BR', { timeZone: 'America/Sao_Paulo' }).replace(',', '');
  } catch(e) {}

  const totalOcorrencias = occurrences.length;
  const minId = totalOcorrencias > 0 ? occurrences[0].id : '-';
  const maxId = totalOcorrencias > 0 ? occurrences[totalOcorrencias - 1].id : '-';
  const incidentesSubtext = totalOcorrencias > 1 ? ('Incidentes (#' + minId + ' a #' + maxId + ')') : (totalOcorrencias === 1 ? ('Incidente #' + minId) : 'Nenhum incidente');

  const totalRequisicoes = rawData.length;
  const totalRequisicoesFormatted = totalRequisicoes.toLocaleString('pt-BR');
  const avgReqs = totalOcorrencias > 0 ? Math.round(totalRequisicoes / totalOcorrencias) : 0;

  const totalSimReg = occurrences.filter(o => isReplicaServer(o.servidor, serverMap)).length;
  const pctSim = totalOcorrencias > 0 ? Math.round((totalSimReg / totalOcorrencias) * 100) : 100;

  const totalHmgReg = occurrences.filter(o => isOriginServer(o.servidor, serverMap)).length;
  const pctHmgOk = totalOcorrencias > 0 ? Math.round(((totalOcorrencias - totalHmgReg) / totalOcorrencias) * 100) : 100;
  const hmgSubText = totalHmgReg === 0 ? 'Zero regressões em HMG' : (totalHmgReg + ' anomalias detectadas');

  const totalSimReqs = rawData.filter(r => isReplicaServer(r.servidor, serverMap, r.papel_servidor)).length;
  const totalHmgReqs = rawData.filter(r => isOriginServer(r.servidor, serverMap, r.papel_servidor)).length;

  const totalInversoes = occurrences.filter(o => (o.criterio && o.criterio.includes('TOTALIZAÇÃO')) || (o.motivo && o.motivo.includes('TOTALIZAÇÃO')) || (o.dt_recebido && o.dt_anterior && (o.dt_recebido < o.dt_anterior || (o.dt_recebido === o.dt_anterior && o.ht_recebido < o.ht_anterior)))).length;

  let part1 = "<!DOCTYPE html>\n<html lang=\"pt-BR\">\n<head>\n  <meta charset=\"UTF-8\">\n  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">\n  <title>Dossiê Pericial de Integridade — Regressões DG/HG Rodada #20</title>\n  <style>\n    :root {\n      --bg-main: #0a0e17;\n      --bg-card: #0f172a;\n      --bg-card-hover: #1e293b;\n      --border: #334155;\n      --border-accent: #38bdf8;\n      --text-main: #f8fafc;\n      --text-muted: #94a3b8;\n      --danger: #ef4444;\n      --danger-bg: rgba(239, 68, 68, 0.15);\n      --warning: #f59e0b;\n      --success: #10b981;\n      --success-bg: rgba(16, 185, 129, 0.15);\n      --primary: #3b82f6;\n      --primary-bg: rgba(59, 130, 246, 0.15);\n      --purple: #a855f7;\n      --purple-bg: rgba(168, 85, 247, 0.15);\n    }\n\n    * { box-sizing: border-box; margin: 0; padding: 0; }\n    body {\n      font-family: -apple-system, BlinkMacSystemFont, \"Segoe UI\", Roboto, Helvetica, Arial, sans-serif;\n      background-color: var(--bg-main);\n      color: var(--text-main);\n      line-height: 1.4;\n      padding: 24px;\n      font-size: 13px;\n    }\n\n    .header-panel {\n      background: linear-gradient(180deg, #111827 0%, #0b132b 100%);\n      border: 1px solid var(--border);\n      border-radius: 12px;\n      padding: 20px 24px;\n      margin-bottom: 20px;\n      box-shadow: 0 4px 20px rgba(0,0,0,0.4);\n    }\n    .header-top {\n      display: flex;\n      justify-content: space-between;\n      align-items: center;\n      flex-wrap: wrap;\n      gap: 16px;\n      margin-bottom: 16px;\n    }\n    .title-area h1 {\n      font-size: 1.45rem;\n      font-weight: 800;\n      color: #fff;\n      display: flex;\n      align-items: center;\n      gap: 10px;\n    }\n    .title-area p {\n      color: var(--text-muted);\n      font-size: 0.85rem;\n      margin-top: 4px;\n    }\n    .badge-rodada {\n      background: rgba(56, 189, 248, 0.15);\n      border: 1px solid var(--border-accent);\n      color: #38bdf8;\n      font-weight: 700;\n      padding: 4px 12px;\n      border-radius: 999px;\n      font-size: 0.82rem;\n    }\n\n    .kpis-grid {\n      display: grid;\n      grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));\n      gap: 12px;\n      margin-bottom: 18px;\n    }\n    .kpi-card {\n      background: #090e1c;\n      border: 1px solid rgba(255,255,255,0.08);\n      border-radius: 8px;\n      padding: 12px 16px;\n      display: flex;\n      flex-direction: column;\n    }\n    .kpi-label { font-size: 0.72rem; text-transform: uppercase; color: var(--text-muted); font-weight: 700; }\n    .kpi-val { font-size: 1.4rem; font-weight: 800; color: #f8fafc; margin-top: 4px; font-family: monospace; }\n    .kpi-sub { font-size: 0.72rem; color: var(--text-muted); margin-top: 2px; }\n\n    .controls-panel {\n      background: #0f172a;\n      border: 1px solid var(--border);\n      border-radius: 10px;\n      padding: 14px 18px;\n      margin-bottom: 20px;\n      display: flex;\n      flex-direction: column;\n      gap: 12px;\n    }\n    .controls-row-1 {\n      display: flex;\n      gap: 12px;\n      align-items: center;\n      flex-wrap: wrap;\n    }\n    .search-box {\n      flex: 1;\n      min-width: 280px;\n      position: relative;\n    }\n    .search-box input {\n      width: 100%;\n      background: #020617;\n      border: 1px solid #475569;\n      color: #fff;\n      padding: 9px 12px 9px 36px;\n      border-radius: 6px;\n      font-size: 0.86rem;\n      outline: none;\n    }\n    .search-box input:focus {\n      border-color: var(--border-accent);\n      box-shadow: 0 0 0 2px rgba(56, 189, 248, 0.2);\n    }\n    .search-icon {\n      position: absolute;\n      left: 11px;\n      top: 9px;\n      color: #64748b;\n      pointer-events: none;\n    }\n\n    .filter-btn-group {\n      display: flex;\n      gap: 6px;\n      flex-wrap: wrap;\n    }\n    .btn-filter {\n      background: #1e293b;\n      border: 1px solid #475569;\n      color: #cbd5e1;\n      padding: 6px 12px;\n      border-radius: 6px;\n      font-size: 0.78rem;\n      cursor: pointer;\n      font-weight: 600;\n      transition: all 0.15s;\n    }\n    .btn-filter:hover { background: #334155; color: #fff; }\n    .btn-filter.active {\n      background: var(--primary);\n      border-color: #60a5fa;\n      color: #fff;\n    }\n    .btn-export {\n      background: #059669;\n      border: 1px solid #10b981;\n      color: #fff;\n      padding: 7px 14px;\n      border-radius: 6px;\n      font-size: 0.80rem;\n      font-weight: 700;\n      cursor: pointer;\n      display: inline-flex;\n      align-items: center;\n      gap: 6px;\n    }\n    .btn-export:hover { background: #047857; }\n\n    .btn-reset-cols {\n      background: #1e293b;\n      border: 1px dashed #64748b;\n      color: #94a3b8;\n      padding: 6px 12px;\n      border-radius: 6px;\n      font-size: 0.76rem;\n      cursor: pointer;\n      transition: all 0.15s;\n    }\n    .btn-reset-cols:hover { color: #fff; border-color: #94a3b8; background: #334155; }\n\n    .drag-hint-banner {\n      background: rgba(56, 189, 248, 0.08);\n      border: 1px solid rgba(56, 189, 248, 0.25);\n      border-radius: 6px;\n      padding: 6px 12px;\n      font-size: 0.75rem;\n      color: #38bdf8;\n      display: flex;\n      align-items: center;\n      gap: 8px;\n    }\n\n    .status-summary-bar {\n      display: flex;\n      justify-content: space-between;\n      align-items: center;\n      font-size: 0.80rem;\n      color: var(--text-muted);\n      padding: 0 4px;\n      flex-wrap: wrap;\n      gap: 8px;\n    }\n\n    /* Tabela de Requisições */\n    .table-container {\n      background: #0f172a;\n      border: 1px solid var(--border);\n      border-radius: 10px;\n      overflow-x: auto;\n      box-shadow: 0 4px 16px rgba(0,0,0,0.3);\n      position: relative;\n    }\n    table {\n      width: 100%;\n      border-collapse: collapse;\n      font-size: 0.76rem;\n      text-align: left;\n      white-space: nowrap;\n    }\n    thead th {\n      background: #111827;\n      color: #94a3b8;\n      padding: 10px 10px;\n      font-weight: 700;\n      font-size: 0.72rem;\n      text-transform: uppercase;\n      letter-spacing: 0.03em;\n      border-bottom: 2px solid #334155;\n      user-select: none;\n      position: sticky;\n      top: 0;\n      z-index: 10;\n      transition: background 0.15s, border-color 0.15s;\n    }\n    thead th[draggable=\"true\"] {\n      cursor: grab;\n    }\n    thead th[draggable=\"true\"]:active {\n      cursor: grabbing;\n    }\n    thead th:hover {\n      background: #1e293b;\n      color: #38bdf8;\n    }\n    thead th.dragging {\n      opacity: 0.35;\n      background: #334155 !important;\n    }\n    thead th.drag-over-left {\n      border-left: 3px solid #38bdf8 !important;\n      background: rgba(56, 189, 248, 0.15) !important;\n    }\n    thead th.drag-over-right {\n      border-right: 3px solid #38bdf8 !important;\n      background: rgba(56, 189, 248, 0.15) !important;\n    }\n    .th-grip {\n      color: #64748b;\n      margin-right: 4px;\n      font-size: 0.75rem;\n      display: inline-block;\n    }\n    .sort-icon {\n      margin-left: 4px;\n      font-size: 0.68rem;\n      opacity: 0.4;\n    }\n    thead th.sorted .sort-icon {\n      opacity: 1;\n      color: #38bdf8;\n    }\n\n    tbody tr {\n      border-bottom: 1px solid rgba(255,255,255,0.04);\n      transition: background 0.1s ease;\n    }\n    tbody tr:hover {\n      background: rgba(255,255,255,0.03);\n    }\n\n    /* Destaques visuais para os tipos de linha */\n    tr.row-regression {\n      background: rgba(239, 68, 68, 0.12) !important;\n      border-left: 4px solid #ef4444;\n    }\n    tr.row-regression:hover {\n      background: rgba(239, 68, 68, 0.18) !important;\n    }\n    tr.row-peak {\n      background: rgba(56, 189, 248, 0.05);\n      border-left: 4px solid #38bdf8;\n    }\n    tr.row-origin {\n      background: rgba(168, 85, 247, 0.04);\n      border-left: 4px solid #a855f7;\n    }\n    tr.row-recovery {\n      background: rgba(16, 185, 129, 0.04);\n      border-left: 4px solid #10b981;\n    }\n    tr.row-history {\n      border-left: 4px solid transparent;\n    }\n\n    /* Divisória de grupo por ocorrência */\n    tr.occurrence-start {\n      border-top: 2px solid #334155;\n    }\n\n    td {\n      padding: 7px 10px;\n      color: #cbd5e1;\n      font-family: monospace;\n      vertical-align: middle;\n      white-space: nowrap;\n    }\n\n    .badge-occ {\n      font-family: monospace;\n      font-weight: 800;\n      font-size: 0.78rem;\n      padding: 2px 7px;\n      border-radius: 4px;\n      background: #1e293b;\n      color: #fca5a5;\n      border: 1px solid rgba(239, 68, 68, 0.4);\n      display: inline-flex;\n      align-items: center;\n      gap: 3px;\n    }\n\n    .badge-env-sim {\n      background: rgba(245, 158, 11, 0.2);\n      color: #fbbf24;\n      border: 1px solid rgba(245, 158, 11, 0.4);\n      padding: 2px 6px;\n      border-radius: 4px;\n      font-weight: 700;\n      font-size: 0.70rem;\n    }\n    .badge-env-hmg {\n      background: rgba(168, 85, 247, 0.2);\n      color: #d8b4fe;\n      border: 1px solid rgba(168, 85, 247, 0.4);\n      padding: 2px 6px;\n      border-radius: 4px;\n      font-weight: 700;\n      font-size: 0.70rem;\n    }\n\n    .badge-status {\n      padding: 2px 8px;\n      border-radius: 4px;\n      font-size: 0.68rem;\n      font-weight: 800;\n      letter-spacing: 0.02em;\n      display: inline-block;\n    }\n    .badge-regression {\n      background: #dc2626;\n      color: #fff;\n      box-shadow: 0 0 6px rgba(220, 38, 38, 0.5);\n    }\n    .badge-peak {\n      background: rgba(56, 189, 248, 0.2);\n      color: #38bdf8;\n      border: 1px solid #38bdf8;\n    }\n    .badge-origin {\n      background: rgba(168, 85, 247, 0.2);\n      color: #c084fc;\n      border: 1px solid rgba(168, 85, 247, 0.5);\n    }\n    .badge-recovery {\n      background: rgba(16, 185, 129, 0.2);\n      color: #34d399;\n      border: 1px solid #10b981;\n    }\n    .badge-history {\n      background: rgba(148, 163, 184, 0.15);\n      color: #94a3b8;\n      border: 1px solid #475569;\n    }\n\n    .url-cell {\n      white-space: nowrap;\n    }\n    .url-cell a {\n      color: #38bdf8;\n      text-decoration: none;\n      display: inline-block;\n    }\n    .url-cell a:hover {\n      text-decoration: underline;\n      color: #7dd3fc;\n    }\n\n    .btn-raw {\n      background: #1e293b;\n      border: 1px solid #475569;\n      color: #93c5fd;\n      padding: 2px 7px;\n      border-radius: 4px;\n      font-size: 0.68rem;\n      cursor: pointer;\n      font-weight: 600;\n    }\n    .btn-raw:hover {\n      background: #3b82f6;\n      color: #fff;\n      border-color: #60a5fa;\n    }\n\n    /* Modal de Headers Brutos */\n    .modal-overlay {\n      display: none;\n      position: fixed;\n      top: 0; left: 0; right: 0; bottom: 0;\n      background: rgba(0,0,0,0.8);\n      z-index: 9999;\n      justify-content: center;\n      align-items: center;\n      padding: 20px;\n    }\n    .modal-content {\n      background: #0f172a;\n      border: 1px solid #38bdf8;\n      border-radius: 12px;\n      width: 100%;\n      max-width: 800px;\n      max-height: 85vh;\n      display: flex;\n      flex-direction: column;\n      box-shadow: 0 10px 40px rgba(0,0,0,0.7);\n    }\n    .modal-header {\n      padding: 16px 20px;\n      border-bottom: 1px solid #334155;\n      display: flex;\n      justify-content: space-between;\n      align-items: center;\n    }\n    .modal-header h3 {\n      font-size: 1.05rem;\n      color: #fff;\n      display: flex;\n      align-items: center;\n      gap: 8px;\n    }\n    .modal-close {\n      background: none;\n      border: none;\n      color: #94a3b8;\n      font-size: 1.5rem;\n      cursor: pointer;\n      padding: 0 6px;\n    }\n    .modal-close:hover { color: #fff; }\n    .modal-body {\n      padding: 20px;\n      overflow-y: auto;\n      font-family: monospace;\n      font-size: 0.78rem;\n    }\n    .modal-section { margin-bottom: 16px; }\n    .modal-section h4 {\n      font-size: 0.82rem;\n      color: #38bdf8;\n      margin-bottom: 6px;\n      text-transform: uppercase;\n      border-bottom: 1px solid rgba(255,255,255,0.08);\n      padding-bottom: 4px;\n    }\n    pre.json-box {\n      background: #020617;\n      padding: 12px;\n      border-radius: 6px;\n      border: 1px solid #1e293b;\n      overflow-x: auto;\n      color: #cbd5e1;\n    }\n\n    /* Paginação e Rodapé */\n    .pagination-bar {\n      display: flex;\n      justify-content: space-between;\n      align-items: center;\n      padding: 12px 18px;\n      background: #0b132b;\n      border: 1px solid var(--border);\n      border-top: none;\n      border-radius: 0 0 10px 10px;\n      font-size: 0.80rem;\n      color: var(--text-muted);\n    }\n    .pagination-controls {\n      display: flex;\n      gap: 6px;\n      align-items: center;\n    }\n    .btn-page {\n      background: #1e293b;\n      border: 1px solid #475569;\n      color: #fff;\n      padding: 4px 10px;\n      border-radius: 4px;\n      cursor: pointer;\n      font-size: 0.75rem;\n    }\n    .btn-page:disabled {\n      opacity: 0.3;\n      cursor: not-allowed;\n    }\n  </style>\n</head>\n<body>\n\n  <div class=\"header-panel\">\n    <div class=\"header-top\">\n      <div class=\"title-area\">\n        <h1><span>🚨</span> Dossiê Pericial de Integridade — Regressões DG/HG da Rodada #20</h1>\n        <p>Inspeção pericial de todas as ocorrências de regressão temporal (DG/HG) e contexto completo das requisições associadas.</p>\n      </div>\n      <div class=\"badge-rodada\">\n        Rodada #20 • Início: 13/09/2026 13:00:53\n      </div>\n    </div>\n\n    <div class=\"kpis-grid\">\n      <div class=\"kpi-card\">\n        <div class=\"kpi-label\">Ocorrências Auditadas (#)</div>\n        <div class=\"kpi-val\" id=\"kpiOcorrencias\">242</div>\n        <div class=\"kpi-sub\">Incidentes (#31076 a #31682)</div>\n      </div>\n      <div class=\"kpi-card\">\n        <div class=\"kpi-label\">Requisições no Contexto</div>\n        <div class=\"kpi-val\" id=\"kpiRequisicoes\">1.694</div>\n        <div class=\"kpi-sub\">Média de 7 reqs por incidente</div>\n      </div>\n      <div class=\"kpi-card\">\n        <div class=\"kpi-label\">Ambiente da Regressão</div>\n        <div class=\"kpi-val\" style=\"color:#fbbf24;\">100% SIM</div>\n        <div class=\"kpi-sub\">Nó de Cache Borda Akamai</div>\n      </div>\n      <div class=\"kpi-card\">\n        <div class=\"kpi-label\">Integridade da Origem</div>\n        <div class=\"kpi-val\" style=\"color:#10b981;\">100% HMG OK</div>\n        <div class=\"kpi-sub\">Zero regressões em HMG</div>\n      </div>\n    </div>\n  </div>\n\n  <div class=\"controls-panel\">\n    <div class=\"controls-row-1\">\n      <div class=\"search-box\">\n        <span class=\"search-icon\">🔍</span>\n        <input type=\"text\" id=\"globalSearch\" placeholder=\"Filtrar por qualquer campo (ex: #31453, 2.23.98.230, ro-c0001, ETag, GRN, DG, HG)\" oninput=\"handleSearch()\">\n      </div>\n\n      <div class=\"filter-btn-group\">\n        <button type=\"button\" class=\"btn-filter active\" onclick=\"setPresetFilter('all', this)\">Todas as Requisições (1694)</button>\n        <button type=\"button\" class=\"btn-filter\" onclick=\"setPresetFilter('reg_only', this)\">Apenas Regressões (242)</button>\n        <button type=\"button\" class=\"btn-filter\" onclick=\"setPresetFilter('sim_only', this)\">Apenas SIM</button>\n        <button type=\"button\" class=\"btn-filter\" onclick=\"setPresetFilter('hmg_only', this)\">Apenas HMG (Origem)</button>\n        <button type=\"button\" class=\"btn-filter\" onclick=\"setPresetFilter('inversion_tot', this)\">Envolve DT/HT (30)</button>\n      </div>\n\n      <button type=\"button\" class=\"btn-reset-cols\" onclick=\"resetColumnOrder()\">\n        ↺ Restaurar Colunas\n      </button>\n\n      <button type=\"button\" class=\"btn-export\" onclick=\"exportToCSV()\">\n        <span>⬇️</span> Exportar CSV (Ordem da Tela)\n      </button>\n    </div>\n\n    <div class=\"drag-hint-banner\">\n      <span>✋ <strong>Reorganização de Colunas (Drag & Drop):</strong> Arraste e solte o cabeçalho de qualquer coluna para mudar a sua posição e priorizar as informações que deseja analisar. A exportação em CSV respeitará automaticamente a nova ordem visual!</span>\n    </div>\n\n    <div class=\"status-summary-bar\">\n      <div>\n        <span id=\"filteredCountText\">Exibindo 1694 de 1694 requisições</span>\n        <span id=\"occurrenceCountText\" style=\"margin-left: 10px; color: #38bdf8;\">(242 ocorrências representadas)</span>\n      </div>\n      <div>\n        <span>💡 Clique no cabeçalho para ordenar. Digite o número da ocorrência (ex: <code>#31453</code>) para isolar o incidente.</span>\n      </div>\n    </div>\n  </div>\n\n  <div class=\"table-container\">\n    <table id=\"mainTable\">\n      <thead id=\"tableHead\">\n        <!-- Renderizado dinamicamente com Drag & Drop -->\n      </thead>\n      <tbody id=\"tableBody\">\n        <!-- Renderizado dinamicamente -->\n      </tbody>\n    </table>\n  </div>\n\n  <div class=\"pagination-bar\">\n    <div>\n      <span>Exibindo página <strong id=\"currentPageNum\">1</strong> de <strong id=\"totalPagesNum\">1</strong></span>\n    </div>\n    <div class=\"pagination-controls\">\n      <button type=\"button\" class=\"btn-page\" id=\"btnPrevPage\" onclick=\"changePage(-1)\">◀ Anterior</button>\n      <select id=\"pageSizeSelect\" onchange=\"changePageSize(this.value)\" style=\"background:#020617; color:#fff; border:1px solid #475569; padding:3px 6px; border-radius:4px; font-size:0.75rem;\">\n        <option value=\"100\">100 por página</option>\n        <option value=\"250\">250 por página</option>\n        <option value=\"500\">500 por página</option>\n        <option value=\"1000\">1000 por página</option>\n        <option value=\"99999\">Mostrar Todos</option>\n      </select>\n      <button type=\"button\" class=\"btn-page\" id=\"btnNextPage\" onclick=\"changePage(1)\">Próxima ▶</button>\n    </div>\n  </div>\n\n  <!-- Modal de Headers Brutos -->\n  <div class=\"modal-overlay\" id=\"rawHeadersModal\" onclick=\"closeModal(event)\">\n    <div class=\"modal-content\" onclick=\"event.stopPropagation()\">\n      <div class=\"modal-header\">\n        <h3><span id=\"modalHeaderIcon\">📋</span> Metadados e Headers HTTP Completos</h3>\n        <button type=\"button\" class=\"modal-close\" onclick=\"closeModal()\">&times;</button>\n      </div>\n      <div class=\"modal-body\">\n        <div style=\"margin-bottom:12px; display:flex; gap:8px; flex-wrap:wrap; font-size:0.80rem;\">\n          <span style=\"color:#94a3b8;\">Ocorrência: <strong style=\"color:#fca5a5;\" id=\"modalOccId\">-</strong></span> |\n          <span style=\"color:#94a3b8;\">Ambiente: <strong style=\"color:#38bdf8;\" id=\"modalServidor\">-</strong></span> |\n          <span style=\"color:#94a3b8;\">Leitura ID: <strong style=\"color:#fff;\" id=\"modalLeituraId\">-</strong></span> |\n          <span style=\"color:#94a3b8;\">Timestamp: <strong style=\"color:#fff;\" id=\"modalTimestamp\">-</strong></span>\n        </div>\n        <div class=\"modal-section\">\n          <h4>URL e Requisição</h4>\n          <pre class=\"json-box\" id=\"modalUrlBox\"></pre>\n        </div>\n        <div class=\"modal-section\">\n          <h4>Request Headers Enviados</h4>\n          <pre class=\"json-box\" id=\"modalRequestHeadersBox\"></pre>\n        </div>\n        <div class=\"modal-section\">\n          <h4>Response Headers Recebidos</h4>\n          <pre class=\"json-box\" id=\"modalResponseHeadersBox\"></pre>\n        </div>\n      </div>\n    </div>\n  </div>\n\n  <script>\n    ";
  let part2 = "\n    // Definição das colunas com Timestamp na 2ª coluna por padrão\n    const DEFAULT_COLUMN_KEYS = [\n      'occurrence_id',\n      'timestamp_unix',\n      'servidor',\n      'status_category',\n      'hg',\n      'dg',\n      'idg',\n      'ht',\n      'dt',\n      'arquivo',\n      'last_modified',\n      'etag',\n      'cache_control',\n      'max_age',\n      'akamai_grn',\n      'cdn_cache_status',\n      'x_server_ip',\n      'age',\n      'expires',\n      'date_http',\n      'server_http',\n      'content_length',\n      'content_encoding',\n      'latency_ms',\n      'headers_btn'\n    ];\n\n    let columnOrder = [...DEFAULT_COLUMN_KEYS];\n\n    const COLUMNS_DEF = {\n      occurrence_id: {\n        label: 'Ocorrência (#)',\n        sortKey: 'occurrence_id',\n        renderCell: (r) => '<td><span class=\"badge-occ\">#' + r.occurrence_id + '</span></td>',\n        csvValue: (r) => '#' + r.occurrence_id\n      },\n      timestamp_unix: {\n        label: 'Timestamp Requisição',\n        sortKey: 'timestamp_unix',\n        renderCell: (r) => {\n          const d = new Date(r.timestamp_iso);\n          const f = d.toLocaleTimeString('pt-BR') + '.' + String(d.getMilliseconds()).padStart(3, '0') + ' (' + d.toLocaleDateString('pt-BR') + ')';\n          return '<td title=\"' + r.timestamp_iso + '\">' + f + '</td>';\n        },\n        csvValue: (r) => r.timestamp_iso\n      },\n      servidor: {\n        label: 'Ambiente',\n        sortKey: 'servidor',\n        renderCell: (r) => {\n          const isHmg = (r.servidor && r.servidor.toUpperCase().startsWith('HMG')) || (r.papel_servidor && r.papel_servidor.includes('ORIGEM'));\n          const badgeClass = isHmg ? 'badge-env-hmg' : 'badge-env-sim';\n          return '<td><span class=\"' + badgeClass + '\">' + r.servidor + '</span></td>';\n        },\n        csvValue: (r) => r.servidor\n      },\n      status_category: {\n        label: 'Status / Integridade',\n        sortKey: 'status_category',\n        renderCell: (r) => '<td><span class=\"badge-status ' + r.status_class + '\">' + r.status_badge + '</span></td>',\n        csvValue: (r) => r.status_badge.replace(/^[^\\w]+/, '')\n      },\n      hg: {\n        label: 'HG (Geração)',\n        sortKey: 'hg',\n        renderCell: (r) => '<td style=\"color:' + (r.is_regression ? '#ef4444; font-weight:bold;' : '#fff;') + '\">' + r.hg + '</td>',\n        csvValue: (r) => r.hg\n      },\n      dg: {\n        label: 'DG (Geração)',\n        sortKey: 'dg',\n        renderCell: (r) => '<td>' + r.dg + '</td>',\n        csvValue: (r) => r.dg\n      },\n      idg: {\n        label: 'IDG',\n        sortKey: 'idg',\n        renderCell: (r) => '<td style=\"color:' + (r.is_regression ? '#fca5a5;' : '#94a3b8;') + '\">' + r.idg + '</td>',\n        csvValue: (r) => r.idg\n      },\n      ht: {\n        label: 'HT (Totaliz.)',\n        sortKey: 'ht',\n        renderCell: (r) => '<td>' + r.ht + '</td>',\n        csvValue: (r) => r.ht\n      },\n      dt: {\n        label: 'DT (Totaliz.)',\n        sortKey: 'dt',\n        renderCell: (r) => '<td>' + r.dt + '</td>',\n        csvValue: (r) => r.dt\n      },\n      arquivo: {\n        label: 'URL / Arquivo',\n        sortKey: 'arquivo',\n        renderCell: (r) => '<td class=\"url-cell\"><a href=\"' + r.url + '\" target=\"_blank\" title=\"Abrir URL real: ' + r.url + '\">' + r.arquivo + '</a></td>',\n        csvValue: (r) => r.url\n      },\n      last_modified: {\n        label: 'Last-Modified',\n        sortKey: 'last_modified',\n        renderCell: (r) => '<td>' + r.last_modified + '</td>',\n        csvValue: (r) => r.last_modified\n      },\n      etag: {\n        label: 'ETag',\n        sortKey: 'etag',\n        renderCell: (r) => '<td>' + r.etag + '</td>',\n        csvValue: (r) => r.etag\n      },\n      cache_control: {\n        label: 'Cache-Control',\n        sortKey: 'cache_control',\n        renderCell: (r) => '<td>' + r.cache_control + '</td>',\n        csvValue: (r) => r.cache_control\n      },\n      max_age: {\n        label: 'Max-Age',\n        sortKey: 'max_age',\n        renderCell: (r) => '<td>' + r.max_age + '</td>',\n        csvValue: (r) => r.max_age\n      },\n      akamai_grn: {\n        label: 'Akamai-GRN',\n        sortKey: 'akamai_grn',\n        renderCell: (r) => '<td>' + r.akamai_grn + '</td>',\n        csvValue: (r) => r.akamai_grn\n      },\n      cdn_cache_status: {\n        label: 'CDN-Cache-Status',\n        sortKey: 'cdn_cache_status',\n        renderCell: (r) => '<td>' + r.cdn_cache_status + '</td>',\n        csvValue: (r) => r.cdn_cache_status\n      },\n      x_server_ip: {\n        label: 'X-Server-IP',\n        sortKey: 'x_server_ip',\n        renderCell: (r) => '<td style=\"color:#38bdf8;\">' + r.x_server_ip + '</td>',\n        csvValue: (r) => r.x_server_ip\n      },\n      age: {\n        label: 'Age',\n        sortKey: 'age',\n        renderCell: (r) => '<td>' + r.age + '</td>',\n        csvValue: (r) => r.age\n      },\n      expires: {\n        label: 'Expires',\n        sortKey: 'expires',\n        renderCell: (r) => '<td>' + r.expires + '</td>',\n        csvValue: (r) => r.expires\n      },\n      date_http: {\n        label: 'Date (HTTP)',\n        sortKey: 'date_http',\n        renderCell: (r) => '<td>' + r.date_http + '</td>',\n        csvValue: (r) => r.date_http\n      },\n      server_http: {\n        label: 'Server',\n        sortKey: 'server_http',\n        renderCell: (r) => '<td>' + r.server_http + '</td>',\n        csvValue: (r) => r.server_http\n      },\n      content_length: {\n        label: 'Content-Length',\n        sortKey: 'content_length',\n        renderCell: (r) => '<td>' + r.content_length + '</td>',\n        csvValue: (r) => r.content_length\n      },\n      content_encoding: {\n        label: 'Content-Encoding',\n        sortKey: 'content_encoding',\n        renderCell: (r) => '<td>' + r.content_encoding + '</td>',\n        csvValue: (r) => r.content_encoding\n      },\n      latency_ms: {\n        label: 'Latência (ms)',\n        sortKey: 'latency_ms',\n        renderCell: (r) => '<td>' + r.latency_ms + '</td>',\n        csvValue: (r) => r.latency_ms\n      },\n      headers_btn: {\n        label: 'Headers Brutos',\n        sortKey: null,\n        renderCell: (r) => '<td><button type=\"button\" class=\"btn-raw\" onclick=\"openHeadersModal(' + r.leitura_id + ', ' + r.occurrence_id + ')\">🔍 Headers</button></td>',\n        csvValue: null\n      }\n    };\n\n    let currentFilterPreset = 'all';\n    let currentSearchTerm = '';\n    let sortColumn = 'occurrence_id';\n    let sortAsc = true;\n    let currentPage = 1;\n    let pageSize = 100;\n\n    let filteredData = [...RAW_DATA];\n\n    function init() {\n      renderTableHeader();\n      applyFilters();\n    }\n\n    // ==========================================\n    // DRAG AND DROP PARA REORDENAR COLUNAS\n    // ==========================================\n    let draggedColKey = null;\n\n    function renderTableHeader() {\n      const thead = document.getElementById('tableHead');\n      let trHtml = '<tr>';\n\n      for (let i = 0; i < columnOrder.length; i++) {\n        const key = columnOrder[i];\n        const colDef = COLUMNS_DEF[key];\n        if (!colDef) continue;\n\n        const isSorted = (sortColumn === colDef.sortKey);\n        const sortedClass = isSorted ? 'sorted' : '';\n        const sortIcon = colDef.sortKey ? (isSorted ? (sortAsc ? '▲' : '▼') : '▲▼') : '';\n\n        trHtml += '<th ' +\n          'id=\"th_col_' + key + '\" ' +\n          'class=\"' + sortedClass + '\" ' +\n          'draggable=\"true\" ' +\n          'ondragstart=\"handleDragStart(event, \\'' + key + '\\')\" ' +\n          'ondragover=\"handleDragOver(event, \\'' + key + '\\')\" ' +\n          'ondragleave=\"handleDragLeave(event, \\'' + key + '\\')\" ' +\n          'ondrop=\"handleDrop(event, \\'' + key + '\\')\" ' +\n          'ondragend=\"handleDragEnd(event)\" ' +\n          'onclick=\"handleHeaderClick(event, \\'' + key + '\\')\" ' +\n          'title=\"Arraste para mover de posição / Clique para ordenar\">' +\n          '<span class=\"th-grip\">⋮⋮</span>' +\n          colDef.label +\n          (sortIcon ? '<span class=\"sort-icon\">' + sortIcon + '</span>' : '') +\n          '</th>';\n      }\n\n      trHtml += '</tr>';\n      thead.innerHTML = trHtml;\n    }\n\n    let isDraggingActive = false;\n\n    function handleDragStart(e, key) {\n      draggedColKey = key;\n      isDraggingActive = true;\n      e.dataTransfer.effectAllowed = 'move';\n      e.dataTransfer.setData('text/plain', key);\n      setTimeout(() => {\n        const el = document.getElementById('th_col_' + key);\n        if (el) el.classList.add('dragging');\n      }, 0);\n    }\n\n    function handleDragOver(e, key) {\n      e.preventDefault();\n      e.dataTransfer.dropEffect = 'move';\n      if (!draggedColKey || draggedColKey === key) return;\n\n      const th = document.getElementById('th_col_' + key);\n      if (!th) return;\n\n      const rect = th.getBoundingClientRect();\n      const mid = rect.left + rect.width / 2;\n      if (e.clientX < mid) {\n        th.classList.add('drag-over-left');\n        th.classList.remove('drag-over-right');\n      } else {\n        th.classList.add('drag-over-right');\n        th.classList.remove('drag-over-left');\n      }\n    }\n\n    function handleDragLeave(e, key) {\n      const th = document.getElementById('th_col_' + key);\n      if (th) {\n        th.classList.remove('drag-over-left');\n        th.classList.remove('drag-over-right');\n      }\n    }\n\n    function handleDrop(e, targetKey) {\n      e.preventDefault();\n      const th = document.getElementById('th_col_' + targetKey);\n      if (th) {\n        th.classList.remove('drag-over-left');\n        th.classList.remove('drag-over-right');\n      }\n\n      if (!draggedColKey || draggedColKey === targetKey) return;\n\n      const srcIdx = columnOrder.indexOf(draggedColKey);\n      let tgtIdx = columnOrder.indexOf(targetKey);\n\n      if (srcIdx !== -1 && tgtIdx !== -1) {\n        const rect = th ? th.getBoundingClientRect() : null;\n        if (rect && e.clientX >= (rect.left + rect.width / 2)) {\n          tgtIdx++;\n        }\n        columnOrder.splice(srcIdx, 1);\n        if (srcIdx < tgtIdx) tgtIdx--;\n        columnOrder.splice(tgtIdx, 0, draggedColKey);\n\n        renderTableHeader();\n        renderTable();\n      }\n    }\n\n    function handleDragEnd(e) {\n      document.querySelectorAll('thead th').forEach(th => {\n        th.classList.remove('dragging');\n        th.classList.remove('drag-over-left');\n        th.classList.remove('drag-over-right');\n      });\n      setTimeout(() => {\n        isDraggingActive = false;\n        draggedColKey = null;\n      }, 50);\n    }\n\n    function handleHeaderClick(e, key) {\n      if (isDraggingActive) return;\n      const colDef = COLUMNS_DEF[key];\n      if (!colDef || !colDef.sortKey) return;\n      sortTable(colDef.sortKey);\n    }\n\n    function resetColumnOrder() {\n      columnOrder = [...DEFAULT_COLUMN_KEYS];\n      renderTableHeader();\n      renderTable();\n    }\n\n    // ==========================================\n    // FILTROS, ORDENAÇÃO E BUSCA\n    // ==========================================\n    function setPresetFilter(preset, btn) {\n      currentFilterPreset = preset;\n      document.querySelectorAll('.btn-filter').forEach(b => b.classList.remove('active'));\n      if (btn) btn.classList.add('active');\n      currentPage = 1;\n      applyFilters();\n    }\n\n    function handleSearch() {\n      currentSearchTerm = document.getElementById('globalSearch').value.trim().toLowerCase();\n      currentPage = 1;\n      applyFilters();\n    }\n\n    function applyFilters() {\n      filteredData = RAW_DATA.filter(item => {\n        if (currentFilterPreset === 'reg_only' && !item.is_regression) return false;\n        if (currentFilterPreset === 'sim_only') {\n          const isSim = (item.servidor && item.servidor.toUpperCase().startsWith('SIM')) || (item.papel_servidor && !item.papel_servidor.includes('ORIGEM'));\n          if (!isSim) return false;\n        }\n        if (currentFilterPreset === 'hmg_only') {\n          const isHmg = (item.servidor && item.servidor.toUpperCase().startsWith('HMG')) || (item.papel_servidor && item.papel_servidor.includes('ORIGEM'));\n          if (!isHmg) return false;\n        }\n        if (currentFilterPreset === 'inversion_tot') {\n          const occ = OCCURRENCES_DATA.find(o => o.id === item.occurrence_id);\n          if (!occ || !occ.dt_recebido || !occ.dt_anterior || (occ.dt_recebido >= occ.dt_anterior && occ.ht_recebido >= occ.ht_anterior)) {\n            return false;\n          }\n        }\n\n        if (currentSearchTerm) {\n          const cleanSearch = currentSearchTerm.replace(/^#/, '');\n          const occMatch = String(item.occurrence_id).includes(cleanSearch);\n          const servMatch = item.servidor.toLowerCase().includes(cleanSearch);\n          const statusMatch = item.status_badge.toLowerCase().includes(cleanSearch);\n          const fileMatch = item.arquivo.toLowerCase().includes(cleanSearch);\n          const urlMatch = item.url.toLowerCase().includes(cleanSearch);\n          const ipMatch = item.x_server_ip.toLowerCase().includes(cleanSearch);\n          const grnMatch = item.akamai_grn.toLowerCase().includes(cleanSearch);\n          const etagMatch = item.etag.toLowerCase().includes(cleanSearch);\n          const hgMatch = item.hg.toLowerCase().includes(cleanSearch);\n          const dgMatch = item.dg.toLowerCase().includes(cleanSearch);\n          const idgMatch = String(item.idg).includes(cleanSearch);\n          const ccMatch = item.cache_control.toLowerCase().includes(cleanSearch);\n          const lmMatch = item.last_modified.toLowerCase().includes(cleanSearch);\n\n          if (!occMatch && !servMatch && !statusMatch && !fileMatch && !urlMatch && !ipMatch && !grnMatch && !etagMatch && !hgMatch && !dgMatch && !idgMatch && !ccMatch && !lmMatch) {\n            return false;\n          }\n        }\n\n        return true;\n      });\n\n      filteredData.sort((a, b) => {\n        let valA = a[sortColumn];\n        let valB = b[sortColumn];\n\n        if (valA === undefined || valA === null) valA = '';\n        if (valB === undefined || valB === null) valB = '';\n\n        if (typeof valA === 'number' && typeof valB === 'number') {\n          return sortAsc ? (valA - valB) : (valB - valA);\n        }\n\n        const strA = String(valA).toLowerCase();\n        const strB = String(valB).toLowerCase();\n        if (strA < strB) return sortAsc ? -1 : 1;\n        if (strA > strB) return sortAsc ? 1 : -1;\n        return 0;\n      });\n\n      const totalOccs = new Set(filteredData.map(d => d.occurrence_id)).size;\n      document.getElementById('filteredCountText').textContent = 'Exibindo ' + filteredData.length + ' de ' + RAW_DATA.length + ' requisições';\n      document.getElementById('occurrenceCountText').textContent = '(' + totalOccs + ' ocorrências representadas)';\n\n      renderTableHeader();\n      renderTable();\n    }\n\n    function sortTable(sortKey) {\n      if (sortColumn === sortKey) {\n        sortAsc = !sortAsc;\n      } else {\n        sortColumn = sortKey;\n        sortAsc = true;\n      }\n      applyFilters();\n    }\n\n    function renderTable() {\n      const tbody = document.getElementById('tableBody');\n      const totalPages = Math.ceil(filteredData.length / pageSize) || 1;\n      if (currentPage > totalPages) currentPage = totalPages;\n      if (currentPage < 1) currentPage = 1;\n\n      document.getElementById('currentPageNum').textContent = currentPage;\n      document.getElementById('totalPagesNum').textContent = totalPages;\n      document.getElementById('btnPrevPage').disabled = (currentPage === 1);\n      document.getElementById('btnNextPage').disabled = (currentPage === totalPages);\n\n      const startIndex = (currentPage - 1) * pageSize;\n      const pageItems = filteredData.slice(startIndex, startIndex + pageSize);\n\n      if (pageItems.length === 0) {\n        tbody.innerHTML = '<tr><td colspan=\"' + columnOrder.length + '\" style=\"text-align:center; padding:40px; color:#94a3b8; font-size:0.9rem;\">Nenhuma requisição encontrada com os filtros informados.</td></tr>';\n        return;\n      }\n\n      let html = '';\n      let lastOccId = null;\n\n      for (let i = 0; i < pageItems.length; i++) {\n        const item = pageItems[i];\n        const isNewOcc = (lastOccId !== null && item.occurrence_id !== lastOccId);\n        lastOccId = item.occurrence_id;\n\n        let rowClass = 'row-history';\n        if (item.is_regression) rowClass = 'row-regression';\n        else if (item.status_category === 'PICO_ANTERIOR') rowClass = 'row-peak';\n        else if (item.status_category === 'ORIGEM') rowClass = 'row-origin';\n        else if (item.status_category === 'RECUPERACAO') rowClass = 'row-recovery';\n\n        if (isNewOcc) rowClass += ' occurrence-start';\n\n        html += '<tr class=\"' + rowClass + '\">';\n\n        for (let c = 0; c < columnOrder.length; c++) {\n          const key = columnOrder[c];\n          const colDef = COLUMNS_DEF[key];\n          if (colDef && colDef.renderCell) {\n            html += colDef.renderCell(item);\n          }\n        }\n\n        html += '</tr>';\n      }\n\n      tbody.innerHTML = html;\n    }\n\n    function changePage(delta) {\n      currentPage += delta;\n      renderTable();\n      window.scrollTo({ top: 380, behavior: 'smooth' });\n    }\n\n    function changePageSize(size) {\n      pageSize = parseInt(size, 10);\n      currentPage = 1;\n      renderTable();\n    }\n\n    function openHeadersModal(leituraId, occId) {\n      const item = RAW_DATA.find(r => r.leitura_id === leituraId && r.occurrence_id === occId) || RAW_DATA.find(r => r.leitura_id === leituraId);\n      if (!item) return;\n\n      document.getElementById('modalOccId').textContent = '#' + item.occurrence_id;\n      document.getElementById('modalServidor').textContent = item.servidor + ' (' + item.papel_servidor + ')';\n      document.getElementById('modalLeituraId').textContent = item.leitura_id;\n      document.getElementById('modalTimestamp').textContent = item.timestamp_iso;\n\n      document.getElementById('modalUrlBox').textContent = item.url;\n      document.getElementById('modalRequestHeadersBox').textContent = JSON.stringify(item.all_request_headers, null, 2);\n      document.getElementById('modalResponseHeadersBox').textContent = JSON.stringify(item.all_response_headers, null, 2);\n\n      document.getElementById('rawHeadersModal').style.display = 'flex';\n    }\n\n    function closeModal() {\n      document.getElementById('rawHeadersModal').style.display = 'none';\n    }\n\n    // ==========================================\n    // EXPORTAÇÃO CSV RESPEITANDO A ORDEM DA TELA\n    // ==========================================\n    function exportToCSV() {\n      if (!filteredData || filteredData.length === 0) {\n        alert('Nenhum dado para exportar');\n        return;\n      }\n\n      const exportableKeys = columnOrder.filter(k => COLUMNS_DEF[k] && COLUMNS_DEF[k].csvValue !== null);\n      const headers = exportableKeys.map(k => COLUMNS_DEF[k].label);\n\n      const rows = filteredData.map(r => {\n        return exportableKeys.map(k => {\n          const colDef = COLUMNS_DEF[k];\n          return colDef.csvValue(r);\n        });\n      });\n\n      let csvContent = \"\\uFEFF\" + headers.map(h => '\"' + String(h).replace(/\"/g, '\"\"') + '\"').join(\";\") + \"\\r\\n\";\n      rows.forEach(row => {\n        csvContent += row.map(val => '\"' + String(val).replace(/\"/g, '\"\"') + '\"').join(\";\") + \"\\r\\n\";\n      });\n\n      const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });\n      const url = URL.createObjectURL(blob);\n      const a = document.createElement('a');\n      a.href = url;\n      a.download = 'regressoes_dghg_rodada_20_com_contexto.csv';\n      document.body.appendChild(a);\n      a.click();\n      document.body.removeChild(a);\n      URL.revokeObjectURL(url);\n    }\n\n    window.onload = init;\n  </script>\n</body>\n</html>\n";

  // Replace dynamic values in Part 1
  part1 = part1.replace(/Rodada #20/g, 'Rodada #' + rodadaNum);
  part1 = part1.replace(/13\/09\/2026 13:00:53/g, rodadaInicioFormatted);
  part1 = part1.replace(/id="kpiOcorrencias">242</, 'id="kpiOcorrencias">' + totalOcorrencias + '<');
  part1 = part1.replace(/Incidentes \(#31076 a #31682\)/, incidentesSubtext);
  part1 = part1.replace(/id="kpiRequisicoes">1\.694</, 'id="kpiRequisicoes">' + totalRequisicoesFormatted + '<');
  part1 = part1.replace(/Média de 7 reqs por incidente/, 'Média de ' + avgReqs + ' reqs por incidente');
  part1 = part1.replace(/100% SIM/, pctSim + '% SIM');
  part1 = part1.replace(/100% HMG OK/, pctHmgOk + '% HMG OK');
  part1 = part1.replace(/Zero regressões em HMG/, hmgSubText);

  // Filter button counts
  part1 = part1.replace(/Todas as Requisições \(1694\)/, 'Todas as Requisições (' + totalRequisicoes + ')');
  part1 = part1.replace(/Apenas Regressões \(242\)/, 'Apenas Regressões (' + totalOcorrencias + ')');
  part1 = part1.replace(/Apenas SIM/, 'Apenas SIM (' + totalSimReqs + ')');
  part1 = part1.replace(/Apenas HMG \(Origem\)/, 'Apenas HMG (' + totalHmgReqs + ')');
  part1 = part1.replace(/Envolve DT\/HT \(30\)/, 'Envolve DT/HT (' + totalInversoes + ')');

  // Summary bar counts
  part1 = part1.replace(/Exibindo 1694 de 1694 requisições/, 'Exibindo ' + totalRequisicoes + ' de ' + totalRequisicoes + ' requisições');
  part1 = part1.replace(/\(242 ocorrências representadas\)/, '(' + totalOcorrencias + ' ocorrências representadas)');

  // In Part 2, replace the CSV filename
  part2 = part2.replace(/regressoes_dghg_rodada_20_com_contexto\.csv/, 'regressoes_dghg_rodada_' + rodadaNum + '_com_contexto.csv');

  return part1 + 'const RAW_DATA = ' + JSON.stringify(rawData) + ';\n    const OCCURRENCES_DATA = ' + JSON.stringify(occurrences) + ';\n' + part2;
}

module.exports = {
  buildCausalChainDataset,
  generateOfflineForensicReportHtml
};
