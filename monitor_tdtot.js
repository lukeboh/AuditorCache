const https = require('https');
const http = require('http');
const zlib = require('node:zlib');
const fs = require('fs');
const path = require('path');
const { DatabaseSync } = require('node:sqlite');

/**
 * Monitor TDTot / Resultados TSE - Sistema de Auditoria, Comparação e Persistência Forense
 * 
 * Topologia da Arquitetura Multi-Nós:
 * - Master de Referência (ORIGEM): Onde os arquivos são totalizados e gerados pelo TDTot (ex: HMG).
 * - Réplicas / Borda (REPLICA):    Servidores de cache, distribuição e CDNs que replicam os dados da origem (ex: SIM, Akamai, Slaves).
 */

const CDP_PORT = 9222;
const DASHBOARD_PORT = 3333;
const WORKSPACE_DIR = __dirname;
const DB_FILE = path.join(WORKSPACE_DIR, 'tdtot_auditoria.db');
const EVIDENCIAS_DIR = path.join(WORKSPACE_DIR, 'evidencias_raw');
const VERSOES_DIR = path.join(WORKSPACE_DIR, 'versoes');
const TEMP_ZIPS_DIR = path.join(WORKSPACE_DIR, 'temp_zips');
if (!fs.existsSync(TEMP_ZIPS_DIR)) { fs.mkdirSync(TEMP_ZIPS_DIR, { recursive: true }); }
const LOG_CSV = path.join(WORKSPACE_DIR, 'historico_comparativo.csv');
const REGRESSIONS_CSV = path.join(WORKSPACE_DIR, 'regressoes_detectadas.csv');
const ALERT_LOG = path.join(WORKSPACE_DIR, 'regressoes_detectadas.log');
const REPORT_HTML = path.join(WORKSPACE_DIR, 'relatorio_evidencias.html');

if (!fs.existsSync(EVIDENCIAS_DIR)) {
  fs.mkdirSync(EVIDENCIAS_DIR, { recursive: true });
}
if (!fs.existsSync(VERSOES_DIR)) {
  fs.mkdirSync(VERSOES_DIR, { recursive: true });
}

// Inicializa Banco de Dados SQLite com WAL para alta concorrência
const db = new DatabaseSync(DB_FILE);
try {
  db.exec('PRAGMA journal_mode = WAL;');
  db.exec('PRAGMA synchronous = NORMAL;');
} catch (e) {}

var cachedTodaySync = null;
var lastTodaySyncFetch = 0;

// =====================================================================
// GERENCIADOR DE RODADAS (CHECKPOINTS LÓGICOS)
// =====================================================================
let currentActiveRodada = null;

function getTodayMidnightUnix() {
  const now = new Date();
  return new Date(now.getFullYear(), now.getMonth(), now.getDate(), 0, 0, 0).getTime();
}

function syncRodadasChain() {
  try {
    const rodadas = db.prepare('SELECT id, inicio_unix FROM rodadas ORDER BY inicio_unix ASC, id ASC').all();
    const updateFim = db.prepare('UPDATE rodadas SET fim_unix = ?, fim_iso = ? WHERE id = ?');
    for (let i = 0; i < rodadas.length; i++) {
      const cur = rodadas[i];
      const next = rodadas[i + 1];
      if (next) {
        const fimUnix = next.inicio_unix;
        const fimIso = new Date(fimUnix).toISOString();
        updateFim.run(fimUnix, fimIso, cur.id);
      } else {
        db.prepare('UPDATE rodadas SET fim_unix = NULL, fim_iso = NULL WHERE id = ?').run(cur.id);
      }
    }
  } catch (err) {
    console.error('Erro ao sincronizar cadeia de rodadas:', err.message);
  }
}

function findRodadaForTimestamp(timestampUnix) {
  try {
    const t = Number(timestampUnix);
    const r = db.prepare(`
      SELECT * FROM rodadas 
      WHERE inicio_unix <= ? AND (fim_unix IS NULL OR fim_unix > ?)
      ORDER BY inicio_unix DESC LIMIT 1
    `).get(t, t);
    return r || null;
  } catch (err) {
    return null;
  }
}

function initRodadas() {
  try {
    let row = db.prepare('SELECT * FROM rodadas WHERE ativo = 1 ORDER BY id DESC LIMIT 1').get();
    const todayMidnight = getTodayMidnightUnix();

    if (!row || row.inicio_unix < todayMidnight) {
      const now = new Date();
      const dateStr = now.toLocaleDateString('pt-BR');
      const nome = !row ? 'Rodada Inicial - ' + dateStr : 'Rodada do Dia - ' + dateStr;
      
      const inicioUnix = todayMidnight;
      const inicioIso = new Date(inicioUnix).toISOString();

      if (row) {
        db.prepare('UPDATE rodadas SET ativo = 0, fim_iso = ?, fim_unix = ? WHERE id = ?')
          .run(inicioIso, inicioUnix, row.id);
      } else {
        db.prepare('UPDATE rodadas SET ativo = 0 WHERE ativo = 1').run();
      }

      const stmt = db.prepare('INSERT INTO rodadas (nome, inicio_iso, inicio_unix, ativo) VALUES (?, ?, ?, 1)');
      const res = stmt.run(nome, inicioIso, inicioUnix);
      row = db.prepare('SELECT * FROM rodadas WHERE id = ?').get(res.lastInsertRowid);
      console.log(`📍 [RODADA CRIADA AUTOMATICAMENTE] ID=${row.id} "${row.nome}" (Início: ${new Date(row.inicio_unix).toLocaleTimeString('pt-BR')})`);
    } else {
      console.log(`📍 [RODADA ATIVA CARREGADA] ID=${row.id} "${row.nome}" (Início: ${new Date(row.inicio_unix).toLocaleTimeString('pt-BR')})`);
    }
    currentActiveRodada = row;
    syncRodadasChain();
  } catch (err) {
    console.error('Erro ao inicializar rodadas:', err.message);
  }
}

function checkDayRollover() {
  const active = currentActiveRodada;
  const todayMidnight = getTodayMidnightUnix();
  if (!active || active.inicio_unix < todayMidnight) {
    console.log('\n🌙 [VIRADA DO DIA DETECTADA] Transição automática de rodada diária (00:00:00)...');
    initRodadas();
  }
}

function getActiveRodada() {
  if (!currentActiveRodada) initRodadas();
  return currentActiveRodada;
}

function createNewRodada(customName, customInicioUnix = null) {
  const now = new Date();
  const dateStr = now.toLocaleDateString('pt-BR');
  const timeStr = now.toLocaleTimeString('pt-BR');
  const nome = customName && customName.trim() ? customName.trim() : ('Nova Rodada - ' + dateStr + ' ' + timeStr);
  const inicioUnix = customInicioUnix ? Number(customInicioUnix) : now.getTime();
  const inicioIso = new Date(inicioUnix).toISOString();

  if (currentActiveRodada) {
    db.prepare('UPDATE rodadas SET ativo = 0, fim_iso = ?, fim_unix = ? WHERE id = ?')
      .run(inicioIso, inicioUnix, currentActiveRodada.id);
  }

  const stmt = db.prepare('INSERT INTO rodadas (nome, inicio_iso, inicio_unix, ativo) VALUES (?, ?, ?, 1)');
  const res = stmt.run(nome, inicioIso, inicioUnix);
  currentActiveRodada = db.prepare('SELECT * FROM rodadas WHERE id = ?').get(res.lastInsertRowid);
  syncRodadasChain();

  cachedTodaySync = null;
  clearServerStates();
  fileSyncTracker.clear();
  hydrateStateFromDb();

  console.log(`🚀 [NOVA RODADA ATIVADA] ID=${currentActiveRodada.id} "${currentActiveRodada.nome}" a partir de ${inicioIso}`);
  return currentActiveRodada;
}

function updateRodada(id, nome, inicioUnix) {
  const numId = Number(id);
  const inicioIso = new Date(inicioUnix).toISOString();
  db.prepare('UPDATE rodadas SET nome = ?, inicio_iso = ?, inicio_unix = ? WHERE id = ?')
    .run(nome, inicioIso, inicioUnix, numId);

  syncRodadasChain();

  if (currentActiveRodada && currentActiveRodada.id === numId) {
    currentActiveRodada = db.prepare('SELECT * FROM rodadas WHERE id = ?').get(numId);
    cachedTodaySync = null;
    clearServerStates();
    fileSyncTracker.clear();
    hydrateStateFromDb();
    console.log(`✏️ [RODADA ATUALIZADA] ID=${numId} "${currentActiveRodada.nome}" novo marco: ${inicioIso} (${new Date(inicioUnix).toLocaleTimeString('pt-BR')})`);
  }
}

function deleteRodada(id) {
  db.prepare('DELETE FROM rodadas WHERE id = ?').run(id);
  syncRodadasChain();
  if (currentActiveRodada && currentActiveRodada.id === Number(id)) {
    currentActiveRodada = null;
    initRodadas();
    clearServerStates();
    fileSyncTracker.clear();
    hydrateStateFromDb();
  }
}

// =====================================================================
// DDL DO BANCO DE DADOS E TABELAS
// =====================================================================
db.exec(`
  CREATE TABLE IF NOT EXISTS rodadas (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    nome TEXT NOT NULL,
    inicio_iso TEXT NOT NULL,
    inicio_unix INTEGER NOT NULL,
    fim_iso TEXT,
    fim_unix INTEGER,
    ativo INTEGER DEFAULT 1
  );

  CREATE TABLE IF NOT EXISTS servidores_monitorados (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    chave TEXT UNIQUE NOT NULL,
    nome TEXT NOT NULL,
    papel TEXT NOT NULL,
    base_url TEXT NOT NULL,
    ativo INTEGER DEFAULT 1,
    ordem INTEGER DEFAULT 0,
    criado_em TEXT NOT NULL
  );

  CREATE TABLE IF NOT EXISTS eleicoes_monitoradas (
    cd TEXT PRIMARY KEY,
    nome TEXT NOT NULL,
    tipo TEXT,
    pleito TEXT,
    ativo INTEGER DEFAULT 1
  );

  CREATE TABLE IF NOT EXISTS leituras (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp_iso TEXT NOT NULL,
    timestamp_unix INTEGER NOT NULL,
    servidor TEXT NOT NULL,
    papel_servidor TEXT NOT NULL,
    arquivo TEXT NOT NULL,
    idg TEXT,
    dg TEXT,
    hg TEXT,
    gen_time INTEGER,
    secoes TEXT,
    secoes_pct TEXT,
    votos TEXT,
    etag TEXT,
    status_ordem TEXT,
    detalhes TEXT,
    evidencia_raw_path TEXT
  );

  CREATE TABLE IF NOT EXISTS regressoes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp_iso TEXT NOT NULL,
    servidor TEXT NOT NULL,
    papel_servidor TEXT NOT NULL,
    arquivo TEXT NOT NULL,
    criterio TEXT,
    motivo TEXT NOT NULL,
    idg_anterior TEXT,
    dg_anterior TEXT,
    hg_anterior TEXT,
    secoes_anterior TEXT,
    idg_recebido TEXT,
    dg_recebido TEXT,
    hg_recebido TEXT,
    secoes_recebido TEXT,
    evidencia_raw_path TEXT,
    detalhes TEXT
  );

  CREATE TABLE IF NOT EXISTS comparativos (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp_iso TEXT NOT NULL,
    arquivo TEXT NOT NULL,
    hmg_fonte_dg TEXT,
    hmg_fonte_hg TEXT,
    hmg_fonte_idg TEXT,
    hmg_fonte_secoes TEXT,
    sim_cache_dg TEXT,
    sim_cache_hg TEXT,
    sim_cache_idg TEXT,
    sim_cache_secoes TEXT,
    atraso_segundos INTEGER,
    defasagem_idg INTEGER,
    tempo_sync_segundos REAL,
    status_comparacao TEXT,
    descricao TEXT
  );
`);

try { db.exec('ALTER TABLE regressoes ADD COLUMN criterio TEXT;'); } catch (e) {}
try { db.exec('ALTER TABLE eleicoes_monitoradas ADD COLUMN ciclo TEXT;'); } catch (e) {}
try { db.exec('ALTER TABLE comparativos ADD COLUMN defasagem_idg INTEGER;'); } catch (e) {}
try { db.exec('ALTER TABLE comparativos ADD COLUMN tempo_sync_segundos REAL;'); } catch (e) {}
try { db.exec('ALTER TABLE leituras ADD COLUMN dt TEXT;'); } catch (e) {}
try { db.exec('ALTER TABLE leituras ADD COLUMN ht TEXT;'); } catch (e) {}
try { db.exec('ALTER TABLE leituras ADD COLUMN tot_time INTEGER;'); } catch (e) {}
try { db.exec('ALTER TABLE regressoes ADD COLUMN dt_anterior TEXT;'); } catch (e) {}
try { db.exec('ALTER TABLE regressoes ADD COLUMN ht_anterior TEXT;'); } catch (e) {}
try { db.exec('ALTER TABLE regressoes ADD COLUMN dt_recebido TEXT;'); } catch (e) {}
try { db.exec('ALTER TABLE regressoes ADD COLUMN ht_recebido TEXT;'); } catch (e) {}
try { db.exec('ALTER TABLE leituras ADD COLUMN headers_json TEXT;'); } catch (e) {}
try { db.exec('ALTER TABLE leituras ADD COLUMN request_headers_json TEXT;'); } catch (e) {}
try { db.exec('ALTER TABLE leituras ADD COLUMN server_ip TEXT;'); } catch (e) {}
try { db.exec('ALTER TABLE leituras ADD COLUMN cache_control TEXT;'); } catch (e) {}
try { db.exec('ALTER TABLE leituras ADD COLUMN cdn_status TEXT;'); } catch (e) {}
try { db.exec('ALTER TABLE leituras ADD COLUMN max_age INTEGER;'); } catch (e) {}
try { db.exec('ALTER TABLE leituras ADD COLUMN akamai_grn TEXT;'); } catch (e) {}
try { db.exec('ALTER TABLE regressoes ADD COLUMN akamai_grn TEXT;'); } catch (e) {}
try { db.exec('ALTER TABLE regressoes ADD COLUMN headers_json TEXT;'); } catch (e) {}
try { db.exec('ALTER TABLE regressoes ADD COLUMN request_headers_json TEXT;'); } catch (e) {}
try { db.exec('ALTER TABLE leituras ADD COLUMN call_time_iso TEXT;'); } catch (e) {}
try { db.exec('ALTER TABLE leituras ADD COLUMN call_time_unix INTEGER;'); } catch (e) {}
try { db.exec('ALTER TABLE leituras ADD COLUMN latency_ms INTEGER;'); } catch (e) {}
try { db.exec('ALTER TABLE regressoes ADD COLUMN call_time_iso TEXT;'); } catch (e) {}
try { db.exec('ALTER TABLE regressoes ADD COLUMN call_time_unix INTEGER;'); } catch (e) {}
try { db.exec('ALTER TABLE regressoes ADD COLUMN latency_ms INTEGER;'); } catch (e) {}
try { db.exec('CREATE INDEX IF NOT EXISTS idx_leituras_arquivo_time ON leituras (arquivo, timestamp_unix);'); } catch (e) {}
try { db.exec('CREATE INDEX IF NOT EXISTS idx_leituras_call_time ON leituras (arquivo, call_time_unix);'); } catch (e) {}
try { db.exec('CREATE INDEX IF NOT EXISTS idx_regressoes_time ON regressoes (timestamp_iso);'); } catch (e) {}
try { db.exec('CREATE INDEX IF NOT EXISTS idx_regressoes_grn ON regressoes (akamai_grn);'); } catch (e) {}
try { db.exec('CREATE INDEX IF NOT EXISTS idx_leituras_grn ON leituras (akamai_grn);'); } catch (e) {}
try { db.exec('CREATE INDEX IF NOT EXISTS idx_regressoes_headers ON regressoes (headers_json);'); } catch (e) {}
try { db.exec('CREATE INDEX IF NOT EXISTS idx_leituras_req_headers ON leituras (request_headers_json);'); } catch (e) {}

const stmtInsertLeitura = db.prepare(`
  INSERT INTO leituras (timestamp_iso, timestamp_unix, servidor, papel_servidor, arquivo, idg, dg, hg, gen_time, secoes, secoes_pct, votos, etag, status_ordem, detalhes, evidencia_raw_path, dt, ht, tot_time, headers_json, server_ip, cache_control, cdn_status, max_age, akamai_grn, call_time_iso, call_time_unix, latency_ms, request_headers_json)
  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`);

const stmtInsertRegressao = db.prepare(`
  INSERT INTO regressoes (timestamp_iso, servidor, papel_servidor, arquivo, criterio, motivo, idg_anterior, dg_anterior, hg_anterior, secoes_anterior, idg_recebido, dg_recebido, hg_recebido, secoes_recebido, evidencia_raw_path, detalhes, dt_anterior, ht_anterior, dt_recebido, ht_recebido, akamai_grn, call_time_iso, call_time_unix, latency_ms, headers_json, request_headers_json)
  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`);

const stmtInsertComparativo = db.prepare(`
  INSERT INTO comparativos (timestamp_iso, arquivo, hmg_fonte_dg, hmg_fonte_hg, hmg_fonte_idg, hmg_fonte_secoes, sim_cache_dg, sim_cache_hg, sim_cache_idg, sim_cache_secoes, atraso_segundos, defasagem_idg, tempo_sync_segundos, status_comparacao, descricao)
  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`);

// =====================================================================
// GERENCIADOR DE SERVIDORES (TOPOLOGIA MULTI-NÓS)
// =====================================================================
const knownServers = new Map();

const serverStates = new Proxy({
  HMG: new Map(),
  SIM: new Map()
}, {
  get(target, prop) {
    if (typeof prop !== 'string') return undefined;
    if (!target[prop]) {
      target[prop] = new Map();
    }
    return target[prop];
  }
});

function clearServerStates() {
  for (const key of Object.keys(serverStates)) {
    if (serverStates[key] && typeof serverStates[key].clear === 'function') {
      serverStates[key].clear();
    }
  }
}

function initServidores() {
  try {
    const rows = db.prepare('SELECT * FROM servidores_monitorados ORDER BY ordem ASC, id ASC').all();
    if (rows.length === 0) {
      const nowIso = new Date().toISOString();
      db.prepare(`
        INSERT INTO servidores_monitorados (chave, nome, papel, base_url, ativo, ordem, criado_em)
        VALUES (?, ?, ?, ?, 1, ?, ?)
      `).run('HMG', 'Homologação (Fonte Oficial)', 'ORIGEM', 'https://resultados-hmg.tse.jus.br/teste/', 1, nowIso);
      db.prepare(`
        INSERT INTO servidores_monitorados (chave, nome, papel, base_url, ativo, ordem, criado_em)
        VALUES (?, ?, ?, ?, 1, ?, ?)
      `).run('SIM', 'Simulado (Cache Akamai)', 'REPLICA', 'https://resultados-sim.tse.jus.br/simulado/teste/', 2, nowIso);
      return initServidores();
    }
    knownServers.clear();
    for (const r of rows) {
      const baseUrlClean = r.base_url.endsWith('/') ? r.base_url : (r.base_url + '/');
      knownServers.set(r.chave, {
        id: r.id,
        chave: r.chave,
        nome: r.nome,
        papel: r.papel,
        baseUrl: baseUrlClean,
        base_url: baseUrlClean,
        ativo: r.ativo === 1,
        ordem: r.ordem,
        criado_em: r.criado_em
      });
      if (!serverStates[r.chave]) {
        serverStates[r.chave] = new Map();
      }
    }
    console.log(`🖥️ [SERVIDORES ATIVOS] ${getActiveServers().length} de ${knownServers.size} nós configurados. Origem: ${getOriginServer()?.chave || 'N/A'}`);
  } catch (err) {
    console.error('Erro ao inicializar servidores_monitorados:', err.message);
  }
}

function getActiveServers() {
  return Array.from(knownServers.values()).filter(s => s.ativo);
}

function getOriginServer() {
  const active = getActiveServers();
  return active.find(s => s.papel === 'ORIGEM') || active[0] || null;
}

function getReplicaServers() {
  const origin = getOriginServer();
  return getActiveServers().filter(s => !origin || s.chave !== origin.chave);
}

// Proxy retrocompatível transparente para código legado que consome SERVERS
const SERVERS = new Proxy({}, {
  get(target, prop) {
    if (typeof prop !== 'string') return undefined;
    const origin = getOriginServer();
    const replica = getReplicaServers()[0];
    if (prop === 'HMG') {
      const s = (origin && origin.ativo) ? origin : knownServers.get('HMG');
      if (s) return { key: s.chave, role: s.papel === 'ORIGEM' ? 'FONTE / ORIGEM' : (s.nome || 'FONTE / ORIGEM'), baseUrl: s.baseUrl };
      return { key: 'HMG', role: 'FONTE / ORIGEM', baseUrl: 'https://resultados-hmg.tse.jus.br/simulado/' };
    }
    if (prop === 'SIM') {
      const s = (replica && replica.ativo) ? replica : knownServers.get('SIM');
      if (s) return { key: s.chave, role: s.papel === 'ORIGEM' ? 'FONTE / ORIGEM' : (s.nome || 'CACHE / DISTRIBUIÇÃO'), baseUrl: s.baseUrl };
      return { key: 'SIM', role: 'CACHE / DISTRIBUIÇÃO', baseUrl: 'https://resultados-sim.tse.jus.br/simulado/simulado/' };
    }
    const s = knownServers.get(prop);
    if (s) {
      return {
        key: s.chave,
        role: s.papel === 'ORIGEM' ? 'FONTE / ORIGEM' : (s.nome || 'CACHE / DISTRIBUIÇÃO'),
        baseUrl: s.baseUrl
      };
    }
    return undefined;
  }
});

initServidores();

function hydrateStateFromDb() {
  try {
    const rodada = getActiveRodada();
    const rodadaInicio = rodada ? rodada.inicio_unix : 0;
    const rodadaFim = rodada && rodada.fim_unix ? rodada.fim_unix : null;
    clearServerStates();

    const sqlHydrate = rodadaFim ? `
      SELECT l.* FROM leituras l
      INNER JOIN (
        SELECT servidor, arquivo, MAX(id) as max_id
        FROM leituras
        WHERE timestamp_unix >= ? AND timestamp_unix < ?
        GROUP BY servidor, arquivo
      ) latest ON l.id = latest.max_id
    ` : `
      SELECT l.* FROM leituras l
      INNER JOIN (
        SELECT servidor, arquivo, MAX(id) as max_id
        FROM leituras
        WHERE timestamp_unix >= ?
        GROUP BY servidor, arquivo
      ) latest ON l.id = latest.max_id
    `;

    const rows = rodadaFim 
      ? db.prepare(sqlHydrate).all(rodadaInicio, rodadaFim)
      : db.prepare(sqlHydrate).all(rodadaInicio);

    for (const r of rows) {
      const serverKey = r.servidor;
      if (!serverStates[serverKey]) {
        serverStates[serverKey] = new Map();
      }
      serverStates[serverKey].set(r.arquivo, {
        rodadaId: rodada ? rodada.id : null,
        timestampIso: r.call_time_iso || r.timestamp_iso,
        timestampUnix: r.call_time_unix || r.timestamp_unix,
        callTimeIso: r.call_time_iso || r.timestamp_iso,
        callTimeUnix: r.call_time_unix || r.timestamp_unix,
        latencyMs: r.latency_ms ?? null,
        serverKey: r.servidor,
        relPath: r.arquivo,
        filename: getFilename(r.arquivo),
        idg: r.idg,
        idgNum: r.idg ? Number(r.idg) : null,
        dg: r.dg,
        hg: r.hg,
        genTime: parseDgHg(r.dg, r.hg),
        st: (r.secoes !== null && r.secoes !== undefined && String(r.secoes).trim() !== '') ? Number(r.secoes) : null,
        pst: r.secoes_pct,
        vTot: r.votos,
        dt: r.dt || null,
        ht: r.ht || null,
        totTime: parseDgHg(r.dt, r.ht),
        etag: r.etag,
        maxAge: r.max_age ?? null,
        cdnCacheStatus: r.cdn_status || null,
        source: 'Banco_SQLite',
        status: r.status_ordem || 'CARREGADO_DB',
        details: r.detalhes || 'Histórico SQLite restaurado'
      });
    }
    
    try {
      const slaRows = db.prepare(`
        SELECT 
          h.arquivo,
          ROUND((s.timestamp_unix - h.timestamp_unix) / 1000.0) as sync_delay_sec
        FROM leituras h
        JOIN leituras s ON h.arquivo = s.arquivo AND h.dg = s.dg AND h.hg = s.hg
        WHERE (h.papel_servidor = 'ORIGEM' OR h.servidor = 'HMG') AND (s.papel_servidor = 'REPLICA' OR s.servidor = 'SIM') AND s.timestamp_unix >= h.timestamp_unix AND h.timestamp_unix >= ?
        GROUP BY h.arquivo
        ORDER BY h.id DESC
      `).all(rodadaInicio);
      for (const row of slaRows) {
        fileSyncTracker.set(row.arquivo, {
          targetIdg: null,
          targetHg: null,
          targetDg: null,
          targetGenTime: null,
          originDetectedAt: null,
          hmgDetectedAt: null,
          simSyncedAt: null,
          replicas: {},
          lastSyncSec: row.sync_delay_sec,
          isWaiting: false
        });
      }
      console.log(`⚡ [SLA Inicializado] ${slaRows.length} tempos de SLA de sincronização restaurados do banco.`);
    } catch (e) {}
    console.log(`💾 [SQLite Restaurado] ${rows.length} estados prévios carregados do banco.`);
  } catch (e) {
    console.error('Falha na restauração do SQLite:', e.message);
  }
}

// =====================================================================
// CATÁLOGO DE ARQUIVOS E DESCOBERTA DE ELEIÇÕES
// =====================================================================
const UFS = ['ac', 'al', 'am', 'ap', 'ba', 'ce', 'df', 'es', 'go', 'ma', 'mg', 'ms', 'mt', 'pa', 'pb', 'pe', 'pi', 'pr', 'rj', 'rn', 'ro', 'rr', 'rs', 'sc', 'se', 'sp', 'to', 'zz'];
const CARGOS_ESTADUAL = [
  { cod: '0003', nome: 'Governador' },
  { cod: '0005', nome: 'Senador' },
  { cod: '0006', nome: 'Deputado Federal' },
  { cod: '0007', nome: 'Deputado Estadual' }
];

function buildCatalog() {
  if (knownElections && knownElections.size > 0) {
    return buildCatalogFromElections();
  }
  const list = [];
  list.push('comum/config/ele-c.json');
  list.push('ele2026/21270/config/mun-e021270-cm.json');
  list.push('ele2026/21272/config/mun-e021272-cm.json');

  // Configuração de Seções (-cs.json) do Pleito 17801
  for (const uf of UFS) {
    list.push(`ele2026/arquivo-urna/17801/config/${uf}/${uf}-p017801-cs.json`);
  }

  list.push('ele2026/21270/dados/br/br-c0001-e021270-u.json');
  list.push('ele2026/21270/dados/br/br-e021270-ab.json');
  for (const uf of UFS) {
    list.push(`ele2026/21270/dados/${uf}/${uf}-c0001-e021270-u.json`);
    list.push(`ele2026/21270/dados/${uf}/${uf}-e021270-ab.json`);
  }
  for (const uf of UFS) {
    if (uf === 'zz') continue;
    list.push(`ele2026/21272/dados/${uf}/${uf}-e021272-ab.json`);
    for (const c of CARGOS_ESTADUAL) {
      if (uf === 'df' && c.cod === '0007') {
        list.push(`ele2026/21272/dados/df/df-c0008-e021272-u.json`);
      } else {
        list.push(`ele2026/21272/dados/${uf}/${uf}-c${c.cod}-e021272-u.json`);
      }
    }
  }
  return list;
}

const knownElections = new Map();

function fetchJsonHttps(urlStr) {
  return new Promise((resolve) => {
    try {
      const u = new URL(urlStr);
      const lib = u.protocol === 'https:' ? https : http;
      lib.get(u, { headers: { 'User-Agent': 'Mozilla/5.0' } }, (res) => {
        if (res.statusCode !== 200) return resolve({ status: res.statusCode });
        const chunks = [];
        res.on('data', c => chunks.push(c));
        res.on('end', () => {
          try {
            const parsed = JSON.parse(Buffer.concat(chunks).toString('utf8'));
            resolve({ status: 200, json: parsed });
          } catch {
            resolve({ status: 500 });
          }
        });
      }).on('error', () => resolve({ status: 500 }));
    } catch {
      resolve({ status: 500 });
    }
  });
}

async function discoverAvailableElections() {
  try {
    const saved = db.prepare('SELECT cd, ativo FROM eleicoes_monitoradas').all();
    const savedMap = new Map(saved.map(s => [s.cd, s.ativo === 1]));

    const origin = getOriginServer();
    const replica = getReplicaServers()[0];
    const discoveryUrl = (origin && origin.ativo ? origin.baseUrl : (replica && replica.ativo ? replica.baseUrl : 'https://resultados-hmg.tse.jus.br/simulado/'));
    const eleRes = await fetchJsonHttps(discoveryUrl + 'comum/config/ele-c.json');
    if (eleRes.status !== 200 || !eleRes.json || !eleRes.json.pl) return;

    for (const pl of eleRes.json.pl) {
      const ciclo = pl.c || 'ele2026';
      for (const el of (pl.e || [])) {
        const cd = String(el.cd);
        const padded = cd.padStart(6, '0');
        const cmRes = await fetchJsonHttps(discoveryUrl + ciclo + '/' + cd + '/config/mun-e' + padded + '-cm.json');
        if (cmRes.status === 200 && cmRes.json && cmRes.json.abr) {
          const ufs = cmRes.json.abr.map(a => a.cd);
          const nm = el.nm.replace(/&#186;/g, 'º');

          let ativo;
          if (savedMap.has(cd)) {
            ativo = savedMap.get(cd);
          } else {
            ativo = (cd === '21270' || cd === '21272');
          }

          // Se todas as eleições salvas estiverem inativas no banco, ativa as principais (21270 e 21272)
          const anySavedActive = Array.from(savedMap.values()).some(v => v === true);
          if (!anySavedActive && (cd === '21270' || cd === '21272')) {
            ativo = true;
          }

          db.prepare('INSERT OR REPLACE INTO eleicoes_monitoradas (cd, nome, tipo, pleito, ativo, ciclo) VALUES (?, ?, ?, ?, ?, ?)')
            .run(cd, nm, String(el.tp), String(pl.cd), ativo ? 1 : 0, ciclo);

          knownElections.set(cd, {
            cd,
            padded,
            nm,
            tp: String(el.tp),
            pleito: String(pl.cd),
            ciclo,
            ufs,
            abr: cmRes.json.abr,
            ativo
          });
        }
      }
    }
    console.log(`🎯 [DESCOBERTA DE ELEIÇÕES] ${knownElections.size} eleições catalogadas do sistema TSE.`);
  } catch (err) {
    console.error('Erro na descoberta de eleições:', err.message);
  }
}

function buildCatalogFromElections() {
  const list = [];
  list.push('comum/config/ele-c.json');

  for (const [cd, el] of knownElections.entries()) {
    if (!el.ativo) continue;
    const ciclo = el.ciclo || 'ele2026';
    const padded = el.padded;
    list.push(`${ciclo}/${cd}/config/mun-e${padded}-cm.json`);

    if (el.tp === '8') {
      list.push(`${ciclo}/${cd}/dados/br/br-e${padded}-ab.json`);
      list.push(`${ciclo}/${cd}/dados/br/br-c0001-e${padded}-u.json`);
      for (const uf of el.ufs) {
        list.push(`${ciclo}/${cd}/dados/${uf}/${uf}-e${padded}-ab.json`);
        list.push(`${ciclo}/${cd}/dados/${uf}/${uf}-c0001-e${padded}-u.json`);
      }
    } else if (el.tp === '1') {
      for (const uf of el.ufs) {
        if (uf === 'zz') continue;
        list.push(`${ciclo}/${cd}/dados/${uf}/${uf}-e${padded}-ab.json`);
        list.push(`${ciclo}/${cd}/dados/${uf}/${uf}-c0003-e${padded}-u.json`);
        list.push(`${ciclo}/${cd}/dados/${uf}/${uf}-c0005-e${padded}-u.json`);
        list.push(`${ciclo}/${cd}/dados/${uf}/${uf}-c0006-e${padded}-u.json`);
        if (uf === 'df') {
          list.push(`${ciclo}/${cd}/dados/df/df-c0008-e${padded}-u.json`);
        } else {
          list.push(`${ciclo}/${cd}/dados/${uf}/${uf}-c0007-e${padded}-u.json`);
        }
      }
    } else if (el.tp === '3') {
      for (const uf of el.ufs) {
        list.push(`${ciclo}/${cd}/dados/${uf}/${uf}-e${padded}-ab.json`);
      }
      if (el.abr) {
        for (const abr of el.abr) {
          const uf = abr.cd;
          for (const mu of (abr.mu || []).slice(0, 5)) {
            if (cd === '21274' && mu.cd === '30015') {
              list.push(`${ciclo}/${cd}/dados/${uf}/${uf}${mu.cd}-c0025-e${padded}-u.json`);
            } else {
              list.push(`${ciclo}/${cd}/dados/${uf}/${uf}${mu.cd}-c0011-e${padded}-u.json`);
              list.push(`${ciclo}/${cd}/dados/${uf}/${uf}${mu.cd}-c0013-e${padded}-u.json`);
            }
          }
        }
      }
    }
  }

  // Adiciona arquivos de configuração de seção (-cs.json) para os pleitos ativos
  const activePleitos = new Map();
  for (const [cd, el] of knownElections.entries()) {
    if (el.ativo && el.pleito) activePleitos.set(el.pleito, el.ciclo || 'ele2026');
  }
  if (activePleitos.size === 0) activePleitos.set('17801', 'ele2026');
  for (const [pl, ciclo] of activePleitos.entries()) {
    const paddedPl = String(pl).padStart(6, '0');
    for (const uf of UFS) {
      list.push(`${ciclo}/arquivo-urna/${pl}/config/${uf}/${uf}-p${paddedPl}-cs.json`);
    }
  }

  if (knownElections.get('21270')?.ativo) {
    const ciclo = knownElections.get('21270')?.ciclo || 'ele2026';
    list.push(`${ciclo}/21270/dados/sp/sp71072-z0001-c0001-e021270-u.json`);
  }
  if (knownElections.get('21272')?.ativo) {
    const ciclo = knownElections.get('21272')?.ciclo || 'ele2026';
    list.push(`${ciclo}/21272/dados/sp/sp71072-z0001-c0003-e021272-u.json`);
    list.push(`${ciclo}/21272/dados/sp/sp71072-z0001-c0005-e021272-u.json`);
    list.push(`${ciclo}/21272/dados/sp/sp71072-z0001-c0006-e021272-u.json`);
    list.push(`${ciclo}/21272/dados/sp/sp71072-z0001-c0007-e021272-u.json`);
  }

  return list;
}

function updateTrackedCatalog() {
  const newList = buildCatalogFromElections();
  trackedFiles.clear();
  for (const f of newList) trackedFiles.add(f);
  console.log(`📋 [CATÁLOGO ATUALIZADO] ${trackedFiles.size} arquivos em monitoramento ativo.`);
}

const trackedFiles = new Set(buildCatalog());

// =====================================================================
// RASTREAMENTO DE SLA E ESTRUTURAS DE ESTADO
// =====================================================================
const fileSyncTracker = new Map();

const recentLogs = [];
let totalChecksCount = 0;
const sseClients = new Set();
const activeWsConnections = new Map();

// Cores ANSI para o console
const RED = '\x1b[31m';
const GREEN = '\x1b[32m';
const YELLOW = '\x1b[33m';
const BLUE = '\x1b[34m';
const MAGENTA = '\x1b[35m';
const CYAN = '\x1b[36m';
const BOLD = '\x1b[1m';
const RESET = '\x1b[0m';

function initLogs() {
  if (!fs.existsSync(LOG_CSV)) {
    fs.writeFileSync(
      LOG_CSV,
      'timestamp_iso,arquivo,hmg_fonte_dg,hmg_fonte_hg,hmg_fonte_idg,hmg_fonte_secoes,sim_cache_dg,sim_cache_hg,sim_cache_idg,sim_cache_secoes,atraso_tempo_segundos,defasagem_idg,status_tempo,status_idg,detalhes\n',
      'utf8'
    );
  }
  if (!fs.existsSync(REGRESSIONS_CSV)) {
    fs.writeFileSync(
      REGRESSIONS_CSV,
      'timestamp_iso,servidor,papel_servidor,arquivo,criterio,motivo,idg_anterior,dg_hg_anterior,secoes_anterior,idg_recebido,dg_hg_recebido,secoes_recebido,caminho_evidencia_raw\n',
      'utf8'
    );
  }
}

function parseDgHg(dg, hg) {
  if (!dg || !hg) return null;
  const dParts = String(dg).trim().split('/').map(Number);
  const hParts = String(hg).trim().split(':').map(Number);
  if (dParts.length < 3 || hParts.length < 2) return null;
  const [d, m, y] = dParts;
  const [hh, mm, ss] = [hParts[0], hParts[1], hParts[2] !== undefined ? hParts[2] : 0];
  if (isNaN(d) || isNaN(m) || isNaN(y) || isNaN(hh) || isNaN(mm) || isNaN(ss)) return null;
  if (d < 1 || d > 31 || m < 1 || m > 12 || y < 2000 || y > 2100) return null;
  if (hh < 0 || hh > 23 || mm < 0 || mm > 59 || ss < 0 || ss > 59) return null;
  return new Date(y, m - 1, d, hh, mm, ss).getTime();
}

function formatMinSec(sec) {
  if (sec === null || sec === undefined) return '-';
  const sign = sec < 0 ? '-' : '';
  const total = Math.abs(Math.round(sec));
  const m = Math.floor(total / 60);
  const s = total % 60;
  return `${sign}${m}m ${s.toString().padStart(2, '0')}s`;
}

function formatDuration(sec) {
  if (sec === 0) return '0s';
  const s = Math.abs(sec);
  const m = Math.floor(s / 60);
  const remS = s % 60;
  if (m === 0) return `${remS}s`;
  const h = Math.floor(m / 60);
  const remM = m % 60;
  if (h === 0) return `${m}m ${remS}s`;
  return `${h}h ${remM}m ${remS}s`;
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

  // 1. Configuração de Seção (-cs.json)
  // Ex: tdtot2026/arquivo-urna/17801/config/sp/sp-p017801-cs.json
  const mCs = relPath.match(/arquivo-urna\/(\d+)\/config\/([a-z]{2})\/([a-z]{2})-p\d+-cs\.json/i);
  if (mCs) {
    pleito = mCs[1];
    uf = mCs[2].toUpperCase();
    tipo = 'Configuração de Seção';
    sufixo = '-cs.json';
    cargo = '-';
    nivel = 'Seções / Urnas';
    eleicao = 'Pleito ' + pleito;
    return { pleito, eleicao, uf, tipo, sufixo, cargo, nivel };
  }

  const mEle = relPath.match(/\/(\d{5})\//);
  if (mEle) {
    eleicao = mEle[1];
    const elObj = (typeof knownElections !== 'undefined' && knownElections) ? knownElections.get(eleicao) : null;
    if (elObj && elObj.pleito) {
      pleito = elObj.pleito;
    } else if (eleicao === '21270' || eleicao === '21272' || eleicao === '21274') {
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
    tipo = 'Configuração de Seção';
    sufixo = '-cs.json';
    cargo = '-';
    nivel = 'Seções / Urnas';
  } else if (fn.includes('-ab.json')) {
    tipo = 'Abrangência';
    sufixo = '-ab.json';
    if (fn.startsWith('br-')) { nivel = 'Nacional'; uf = 'BR'; }
    else if (fn.includes('-z')) nivel = 'Zona';
    else nivel = 'Estadual';
  } else if (fn.includes('-u.json')) {
    tipo = 'Totalização';
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
      cargo = cargos[cCode] || (`Cargo ${cCode}`);
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

function decodeJwsOrJson(text) {
  try {
    const trimmed = text.trim();
    if (trimmed.startsWith('{')) {
      return JSON.parse(trimmed);
    }
    const parts = trimmed.split('.');
    if (parts.length >= 2) {
      const payloadStr = Buffer.from(parts[1], 'base64url').toString('utf8');
      return JSON.parse(payloadStr);
    }
  } catch {
    return null;
  }
  return null;
}

function getFilename(relPath) {
  return relPath.split('/').pop();
}

function broadcastUpdate(type, data) {
  const payload = `data: ${JSON.stringify({ type, data })}\n\n`;
  for (const res of sseClients) {
    try {
      res.write(payload);
    } catch {
      sseClients.delete(res);
    }
  }
}

function getComparison(relPath) {
  const origin = getOriginServer();
  const originKey = origin ? origin.chave : 'HMG';
  const replicas = getReplicaServers();
  const originState = serverStates[originKey]?.get(relPath);

  const primaryReplicaKey = replicas.length > 0 ? replicas[0].chave : (originKey === 'HMG' ? 'SIM' : 'REPLICA');
  const primaryReplicaState = primaryReplicaKey ? serverStates[primaryReplicaKey]?.get(relPath) : null;

  if (!originState || !primaryReplicaState) {
    return {
      status: 'AGUARDANDO_DADOS',
      delaySec: 0,
      diffIdg: 0,
      textTime: 'Aguardando leitura',
      textIdg: 'Aguardando leitura',
      inconsistency: false,
      text: 'Aguardando leitura dos servidores',
      originKey,
      primaryReplicaKey,
      replicasCount: replicas.length,
      isMultiServer: replicas.length > 1,
      replicas: []
    };
  }

  // 1. Comparação Temporal (DG/HG) contra Réplica Primária
  let delaySec = null;
  let statusTime = 'SEM_TIMESTAMP';
  let textTime = 'Sem timestamp de geração';

  if (originState.genTime !== null && primaryReplicaState.genTime !== null) {
    delaySec = Math.round((originState.genTime - primaryReplicaState.genTime) / 1000);
    if (delaySec === 0) {
      statusTime = 'SINCRONIZADO';
      textTime = '0m 00s';
    } else if (delaySec > 0) {
      statusTime = 'CACHE_ATRASADO';
      textTime = `-${formatMinSec(delaySec)}`;
    } else {
      statusTime = 'CACHE_A_FRENTE';
      textTime = `+${formatMinSec(-delaySec)}`;
    }
  }

  // 2. Comparação Sequencial (IDG)
  let diffIdg = null;
  let statusIdg = 'SEM_IDG';
  let textIdg = 'Sem IDG';

  if (originState.idgNum !== null && primaryReplicaState.idgNum !== null) {
    diffIdg = originState.idgNum - primaryReplicaState.idgNum;
    if (diffIdg === 0) {
      statusIdg = 'SINCRONIZADO';
      textIdg = '0 (idg idêntico)';
    } else if (diffIdg > 0) {
      statusIdg = 'CACHE_ATRASADO';
      textIdg = `-${diffIdg} idg`;
    } else {
      statusIdg = 'CACHE_A_FRENTE';
      textIdg = `+${-diffIdg} idg`;
    }
  }

  let status = 'SINCRONIZADO';
  if (statusTime === 'CACHE_ATRASADO') {
    status = 'CACHE_ATRASADO';
  } else if (statusTime === 'CACHE_A_FRENTE') {
    status = 'CACHE_A_FRENTE';
  }

  let textSummary = `Tempo: ${textTime} | IDG: ${textIdg}`;

  // 3. SLA de Sincronização
  const tracker = fileSyncTracker.get(relPath);
  let syncSlaSec = null;
  let syncSlaText = '-';
  let syncSlaStatus = 'SEM_DADOS';

  const isInSync = Boolean(originState && primaryReplicaState && originState.dg === primaryReplicaState.dg && originState.hg === primaryReplicaState.hg);

  if (isInSync) {
    if (tracker) {
      tracker.isWaiting = false;
      const refTime = tracker.originDetectedAt || tracker.hmgDetectedAt;
      if (tracker.lastSyncSec === null && refTime) {
        tracker.lastSyncSec = Math.max(0, Math.round((Date.now() - refTime) / 1000));
      }
      syncSlaSec = tracker.lastSyncSec ?? 0;
    } else {
      syncSlaSec = 0;
    }
    syncSlaStatus = syncSlaSec > 30 ? 'ALERTA' : 'OK';
    syncSlaText = formatMinSec(syncSlaSec);
  } else if (tracker) {
    const refTime = tracker.originDetectedAt || tracker.hmgDetectedAt;
    if (tracker.isWaiting && refTime) {
      const elapsed = Math.max(0, Math.round((Date.now() - refTime) / 1000));
      syncSlaSec = elapsed;
      syncSlaStatus = elapsed > 30 ? 'CRITICO' : 'AGUARDANDO';
      syncSlaText = `⏱️ ${formatMinSec(elapsed)} (em sync)`;
    } else if (tracker.lastSyncSec !== null) {
      syncSlaSec = tracker.lastSyncSec;
      syncSlaStatus = tracker.lastSyncSec > 30 ? 'ALERTA' : 'OK';
      syncSlaText = formatMinSec(tracker.lastSyncSec);
    }
  }

  // 4. Rastreamento e Comparação Individual de Todas as Réplicas Ativas
  const replicaComparisons = [];
  let maxReplicaDelay = 0;
  let anyReplicaLagged = false;

  for (const rep of replicas) {
    const rState = serverStates[rep.chave]?.get(relPath);
    let rDelaySec = null;
    let rStatusTime = 'PENDENTE';
    let rTextTime = 'Pendente';
    let rSyncSlaSec = 0;
    let rSyncSlaText = '-';
    let rSyncSlaStatus = 'SEM_DADOS';

    if (rState && originState) {
      if (originState.genTime !== null && rState.genTime !== null) {
        rDelaySec = Math.round((originState.genTime - rState.genTime) / 1000);
        if (rDelaySec === 0) {
          rStatusTime = 'SINCRONIZADO';
          rTextTime = '0m 00s';
        } else if (rDelaySec > 0) {
          rStatusTime = 'CACHE_ATRASADO';
          rTextTime = `-${formatMinSec(rDelaySec)}`;
          anyReplicaLagged = true;
          if (rDelaySec > maxReplicaDelay) maxReplicaDelay = rDelaySec;
        } else {
          rStatusTime = 'CACHE_A_FRENTE';
          rTextTime = `+${formatMinSec(-rDelaySec)}`;
        }
      }

      const repInSync = (originState.dg === rState.dg && originState.hg === rState.hg);
      if (repInSync) {
        const repTracker = tracker?.replicas?.[rep.chave];
        rSyncSlaSec = repTracker?.syncSec ?? tracker?.lastSyncSec ?? 0;
        rSyncSlaStatus = rSyncSlaSec > 30 ? 'ALERTA' : 'OK';
        rSyncSlaText = formatMinSec(rSyncSlaSec);
      } else if (tracker && (tracker.originDetectedAt || tracker.hmgDetectedAt)) {
        const refTime = tracker.originDetectedAt || tracker.hmgDetectedAt;
        const elapsed = Math.max(0, Math.round((Date.now() - refTime) / 1000));
        rSyncSlaSec = elapsed;
        rSyncSlaStatus = elapsed > 30 ? 'CRITICO' : 'AGUARDANDO';
        rSyncSlaText = `⏱️ ${formatMinSec(elapsed)}`;
      }
    }

    replicaComparisons.push({
      chave: rep.chave,
      nome: rep.nome,
      baseUrl: rep.baseUrl,
      url: rep.baseUrl + relPath,
      exists: Boolean(rState),
      hg: rState?.hg || '-',
      dg: rState?.dg || '-',
      idg: rState?.idg || '-',
      genTime: rState?.genTime || null,
      dt: rState?.dt || '-',
      ht: rState?.ht || '-',
      totTime: rState?.totTime || null,
      st: rState?.st ?? null,
      pst: rState?.pst ?? null,
      delaySec: rDelaySec ?? 0,
      statusTime: rStatusTime,
      textTime: rTextTime,
      syncSlaSec: rSyncSlaSec,
      syncSlaText: rSyncSlaText,
      syncSlaStatus: rSyncSlaStatus,
      cacheControl: rState?.cacheControl || '(nenhum)',
      maxAge: rState?.maxAge ?? null,
      cdnStatus: rState?.cdnCacheStatus || '-',
      etag: rState?.etag || '-',
      serverIp: rState?.serverIp || '-',
      server: rState?.serverHeader || '-',
      akamaiGrn: rState?.akamaiGrn || '-'
    });
  }

  // 5. Comparativo de Atributos de Cache
  const cacheDiff = {
    hmg: {
      serverKey: originKey,
      cacheControl: originState.cacheControl || '(nenhum / default)',
      maxAge: originState.maxAge,
      cdnStatus: originState.cdnCacheStatus || 'ORIGIN',
      etag: originState.etag || '-',
      server: originState.serverHeader || 'Web Server',
      serverIp: originState.serverIp || '-',
      lastModified: originState.lastModifiedHeader || '-'
    },
    sim: {
      serverKey: primaryReplicaKey || 'REPLICA',
      cacheControl: primaryReplicaState.cacheControl || '(nenhum)',
      maxAge: primaryReplicaState.maxAge,
      cdnStatus: primaryReplicaState.cdnCacheStatus || (primaryReplicaState.maxAge !== null ? 'Hit (Edge)' : '-'),
      etag: primaryReplicaState.etag || '-',
      server: primaryReplicaState.serverHeader || 'Edge/CDN',
      serverIp: primaryReplicaState.serverIp || '-',
      akamaiGrn: primaryReplicaState.akamaiGrn || '-',
      lastModified: primaryReplicaState.lastModifiedHeader || '-'
    },
    ttlMismatch: (originState.maxAge !== primaryReplicaState.maxAge),
    simTtlText: primaryReplicaState.maxAge !== null ? `${primaryReplicaState.maxAge}s` : (primaryReplicaState.cacheControl ? primaryReplicaState.cacheControl : '-'),
    cdnHit: (primaryReplicaState.cdnCacheStatus && primaryReplicaState.cdnCacheStatus.toLowerCase().includes('hit')) || false
  };

  // 6. Comparativo de Totalização (DT/HT) e Seções (ST)
  let delayTotSec = null;
  let statusTot = 'SEM_TOTALIZACAO';
  let textTot = '-';
  if (originState.totTime !== null && primaryReplicaState.totTime !== null) {
    delayTotSec = Math.round((originState.totTime - primaryReplicaState.totTime) / 1000);
    if (delayTotSec === 0) {
      statusTot = 'SINCRONIZADO';
      textTot = '0m 00s';
    } else if (delayTotSec > 0) {
      statusTot = 'CACHE_ATRASADO';
      textTot = `-${formatMinSec(delayTotSec)}`;
    } else {
      statusTot = 'CACHE_A_FRENTE';
      textTot = `+${formatMinSec(-delayTotSec)}`;
    }
  }

  let diffSt = null;
  let statusSt = 'SEM_SECOES';
  let textSt = '-';
  if (originState.st !== null && primaryReplicaState.st !== null) {
    diffSt = originState.st - primaryReplicaState.st;
    if (diffSt === 0) {
      statusSt = 'SINCRONIZADO';
      textSt = '0 seç';
    } else if (diffSt > 0) {
      statusSt = 'CACHE_DEFASADO';
      textSt = `-${diffSt} seç`;
    } else {
      statusSt = 'CACHE_A_FRENTE';
      textSt = `+${-diffSt} seç`;
    }
  }

  return {
    status,
    delaySec: delaySec ?? 0,
    diffIdg: diffIdg ?? 0,
    syncSlaSec: syncSlaSec ?? 0,
    syncSlaText,
    syncSlaStatus,
    statusTime,
    statusIdg,
    textTime,
    textIdg,
    delayTotSec,
    statusTot,
    textTot,
    diffSt,
    statusSt,
    textSt,
    inconsistency: false,
    inconsistencyDetails: '',
    cacheDiff,
    originKey,
    primaryReplicaKey,
    isMultiServer: replicas.length > 1,
    anyReplicaLagged,
    maxReplicaDelay,
    replicas: replicaComparisons,
    text: textSummary + (syncSlaText !== '-' ? ` | SLA Sync: ${syncSlaText}` : '')
  };
}

function generateHtmlReport(embeddedData = null) {
  const html = `<!DOCTYPE html>
<html lang="pt-BR">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Dossiê Técnico Forense v1.0: Fonte HMG vs Cache SIM (TDTot TSE)${embeddedData ? ' [OFFLINE]' : ''}</title>
  <script>
    (function() {
      try {
        if (localStorage.getItem('tdtot_theme') === 'light') {
          document.documentElement.setAttribute('data-theme', 'light');
        }
      } catch(e) {}
    })();
  </script>
  <style>
    :root {
      --bg: #0b132b;
      --card-bg: #1c2541;
      --border: #3a506b;
      --text: #f8fafc;
      --text-muted: #94a3b8;
      --accent-green: #10b981;
      --accent-red: #ef4444;
      --accent-blue: #38bdf8;
      --accent-yellow: #f59e0b;
      --accent-purple: #a855f7;
      --accent-pink: #ec4899;
    }
    html[data-theme="light"] {
      --bg: #f1f5f9;
      --card-bg: #ffffff;
      --border: #cbd5e1;
      --text: #0f172a;
      --text-muted: #475569;
      --accent-green: #059669;
      --accent-red: #dc2626;
      --accent-blue: #0284c7;
      --accent-yellow: #d97706;
      --accent-purple: #7c3aed;
      --accent-pink: #db2777;
    }
    html[data-theme="light"] body { background: var(--bg); color: var(--text); }
    html[data-theme="light"] h1, html[data-theme="light"] h2, html[data-theme="light"] h3, html[data-theme="light"] .card-title { color: #0f172a !important; }
    html[data-theme="light"] .stat-card { background: #ffffff; border-color: #cbd5e1; }
    html[data-theme="light"] .card { background: #ffffff; border-color: #cbd5e1; }
    html[data-theme="light"] .filter-panel { background: #f8fafc; border-color: #cbd5e1; }
    html[data-theme="light"] .filter-label { color: #475569; }
    html[data-theme="light"] .filter-select, html[data-theme="light"] .search-input { background: #ffffff !important; color: #0f172a !important; border-color: #cbd5e1 !important; }
    html[data-theme="light"] .filter-select:focus, html[data-theme="light"] .search-input:focus { border-color: #0284c7 !important; }
    html[data-theme="light"] .filter-select option { background: #ffffff; color: #0f172a; }
    html[data-theme="light"] .btn-outline { color: #334155 !important; border-color: #cbd5e1 !important; }
    html[data-theme="light"] .btn-outline:hover { background: #e2e8f0 !important; color: #0f172a !important; }
    html[data-theme="light"] .btn-reset { background: #e2e8f0; color: #1e293b; border: 1px solid #cbd5e1; }
    html[data-theme="light"] .btn-reset:hover { background: #cbd5e1; }
    html[data-theme="light"] .btn-copy { background: #e2e8f0; color: #1e293b; border-color: #cbd5e1; }
    html[data-theme="light"] .btn-copy:hover { background: #cbd5e1; }
    html[data-theme="light"] th { background: #f8fafc; color: #475569; border-bottom: 1px solid #cbd5e1; }
    html[data-theme="light"] th.sortable:hover { background: #e0f2fe; color: #0369a1; }
    html[data-theme="light"] td { border-bottom: 1px solid #e2e8f0; color: #1e293b; }
    html[data-theme="light"] tr:hover { background: #f8fafc; }
    html[data-theme="light"] .tab-btn { color: #64748b; }
    html[data-theme="light"] .tab-btn:hover { color: #0f172a; background: #e2e8f0; }
    html[data-theme="light"] .tab-btn.active { color: #0284c7; background: #e0f2fe; border-color: #bae6fd; }
    html[data-theme="light"] .regression-card { background: #ffffff !important; border-color: rgba(239, 68, 68, 0.3) !important; box-shadow: 0 2px 8px rgba(0,0,0,0.06) !important; color: #0f172a !important; }
    html[data-theme="light"] .regression-card:hover { border-color: #ef4444 !important; }
    html[data-theme="light"] .modal-content { background: #ffffff; border-color: #cbd5e1; color: #0f172a; box-shadow: 0 20px 25px -5px rgba(0,0,0,0.15); }
    html[data-theme="light"] .modal-header { border-bottom-color: #e2e8f0; }
    html[data-theme="light"] .modal-header h3 { color: #0f172a; }
    html[data-theme="light"] .modal-footer { border-top-color: #e2e8f0; background: #f8fafc; }
    html[data-theme="light"] #colSelectorDropdown { background: #ffffff !important; border-color: #cbd5e1 !important; box-shadow: 0 12px 32px rgba(0,0,0,0.15) !important; color: #0f172a !important; }
    html[data-theme="light"] #colSelectorDropdown strong { color: #0f172a !important; }
    html[data-theme="light"] #dossieTechPanel { background: #ffffff !important; border-color: #cbd5e1 !important; box-shadow: 0 4px 12px rgba(0,0,0,0.05) !important; }
    html[data-theme="light"] #dossieTechPanel > div:first-child { background: #f8fafc !important; border-bottom-color: #e2e8f0 !important; }
    html[data-theme="light"] #dossieTechPanelContent td { border-bottom-color: #e2e8f0 !important; color: #1e293b !important; }
    html[data-theme="light"] #dossieTechPanelContent tr { border-bottom-color: #e2e8f0 !important; }
    html[data-theme="light"] #dossieTechPanelContent div[style*="background:#1e293b"] { background: #f8fafc !important; border-color: #cbd5e1 !important; }
    html[data-theme="light"] #dossieTechPanelContent table { background: #ffffff !important; border-color: #cbd5e1 !important; }
    html[data-theme="light"] .timeline-box { background: #f8fafc !important; border-color: #cbd5e1 !important; }
    html[data-theme="light"] .timeline-step-normal { background: #ffffff !important; border-color: #e2e8f0 !important; }
    html[data-theme="light"] .timeline-step-normal strong { color: #0f172a !important; }
    html[data-theme="light"] .timeline-step-normal span { color: #475569 !important; }
    html[data-theme="light"] div[style*="background:#0b132b"] { background: #f8fafc !important; border-color: #cbd5e1 !important; }
    html[data-theme="light"] div[style*="background:rgba(15,23,42,0.6)"] { background: #ffffff !important; border-color: #e2e8f0 !important; }
    html[data-theme="light"] div[style*="background:rgba(15,23,42,0.6)"] strong { color: #0f172a !important; }
    html[data-theme="light"] div[style*="background:rgba(15,23,42,0.6)"] span { color: #475569 !important; }
    html[data-theme="light"] .code,
    html[data-theme="light"] div[style*="color:#f8fafc"],
    html[data-theme="light"] div[style*="color: #f8fafc"],
    html[data-theme="light"] strong[style*="color:#f8fafc"],
    html[data-theme="light"] strong[style*="color: #f8fafc"],
    html[data-theme="light"] span[style*="color:#f8fafc"],
    html[data-theme="light"] span[style*="color: #f8fafc"],
    html[data-theme="light"] .filter-header strong,
    html[data-theme="light"] .filter-label span {
      color: #0f172a !important;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; }
    body { background: var(--bg); color: var(--text); padding: 24px; line-height: 1.5; }
    header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px; padding-bottom: 16px; border-bottom: 1px solid var(--border); }
    h1 { font-size: 1.5rem; display: flex; align-items: center; gap: 10px; color: #f8fafc; }
    h2 { font-size: 1.15rem; font-weight: 700; color: #f8fafc; margin-bottom: 12px; display: flex; align-items: center; justify-content: space-between; }
    .card { background: var(--card-bg); border: 1px solid var(--border); border-radius: 12px; padding: 20px; margin-bottom: 20px; }

    /* KPIS */
    .stats-grid { display: grid; grid-template-columns: 0.8fr 0.8fr 1.1fr 2.4fr 2.2fr; gap: 10px; margin-bottom: 18px; align-items: stretch; }
    @media (max-width: 1300px) { .stats-grid { grid-template-columns: 1fr 1fr; } }
    @media (max-width: 700px) { .stats-grid { grid-template-columns: 1fr; } }
    .stat-card { background: var(--card-bg); border: 1px solid var(--border); border-radius: 10px; padding: 12px 16px; }
    .stat-label { color: var(--text-muted); font-size: 0.70rem; text-transform: uppercase; font-weight: 700; margin-bottom: 4px; }
    .stat-value { font-size: 1.5rem; font-weight: 700; }

    /* FILTROS */
    .filter-panel { background: rgba(0,0,0,0.25); border: 1px solid var(--border); border-radius: 10px; padding: 16px; margin-bottom: 18px; display: flex; flex-wrap: wrap; gap: 12px; align-items: flex-end; }
    .filter-group { display: flex; flex-direction: column; gap: 4px; }
    .filter-label { font-size: 0.72rem; text-transform: uppercase; color: var(--text-muted); font-weight: 700; letter-spacing: 0.05em; }
    .filter-select, .search-input { background: #0f172a; color: #f8fafc; border: 1px solid var(--border); border-radius: 6px; padding: 7px 11px; font-size: 0.84rem; outline: none; transition: border-color 0.2s; }
    .filter-select:focus, .search-input:focus { border-color: var(--accent-blue); }
    .search-input { min-width: 240px; }
    .btn-reset { background: #334155; color: #f8fafc; border: none; padding: 7px 14px; border-radius: 6px; cursor: pointer; font-size: 0.80rem; font-weight: 600; }
    .btn-reset:hover { background: #475569; }

    /* TABELA */
    table { width: 100%; border-collapse: collapse; text-align: left; font-size: 0.84rem; }
    th { color: var(--text-muted); padding: 11px 9px; border-bottom: 1px solid var(--border); font-weight: 600; background: rgba(0,0,0,0.25); user-select: none; }
    th.sortable { cursor: pointer; transition: all 0.15s; }
    th.sortable:hover { background: rgba(56, 189, 248, 0.15); color: #fff; }
    .sort-icon { font-size: 0.70rem; margin-left: 4px; opacity: 0.5; }
    th.sorted-asc .sort-icon, th.sorted-desc .sort-icon { opacity: 1; color: var(--accent-blue); font-weight: bold; }
    td { padding: 10px 9px; border-bottom: 1px solid rgba(255,255,255,0.05); vertical-align: middle; }
    tr:hover { background: rgba(255,255,255,0.025); }

    .badge { padding: 3px 7px; border-radius: 5px; font-size: 0.72rem; font-weight: 700; display: inline-block; white-space: nowrap; }
    .badge-sync { background: #065f46; color: #a7f3d0; }
    .badge-lag { background: #78350f; color: #fde68a; border: 1px solid #d97706; }
    .badge-ahead { background: #1e3a8a; color: #bfdbfe; border: 1px solid #3b82f6; }
    .badge-danger { background: #991b1b; color: #fecaca; }
    .badge-purple { background: rgba(168, 85, 247, 0.2); color: #c084fc; border: 1px solid rgba(168, 85, 247, 0.4); }
    .badge-yellow { background: rgba(245, 158, 11, 0.2); color: #fbbf24; border: 1px solid rgba(245, 158, 11, 0.4); }

    .tag-pill { display: inline-block; padding: 2px 6px; border-radius: 4px; font-size: 0.70rem; font-weight: 600; margin-right: 4px; }
    .tag-eleicao { background: rgba(56, 189, 248, 0.15); color: #38bdf8; border: 1px solid rgba(56, 189, 248, 0.3); }
    .tag-uf { background: rgba(168, 85, 247, 0.15); color: #c084fc; border: 1px solid rgba(168, 85, 247, 0.3); font-weight: bold; }
    .tag-tipo { background: rgba(16, 185, 129, 0.15); color: #34d399; border: 1px solid rgba(16, 185, 129, 0.3); }
    .tag-pleito { background: rgba(236, 72, 153, 0.15); color: #f472b6; border: 1px solid rgba(236, 72, 153, 0.35); font-weight: 700; }
    .tag-cargo { background: rgba(245, 158, 11, 0.15); color: #fbbf24; border: 1px solid rgba(245, 158, 11, 0.3); }
    .code { font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace; }
    .tag-hmg-title { color: var(--accent-purple); font-weight: 700; background: rgba(168,85,247,0.15); padding: 1px 6px; border-radius: 3px; }
    .tag-sim-title { color: var(--accent-blue); font-weight: 700; background: rgba(56,189,248,0.15); padding: 1px 6px; border-radius: 3px; }

    .btn { background: #7c3aed; color: white; border: none; padding: 7px 14px; border-radius: 6px; font-weight: 600; cursor: pointer; text-decoration: none; display: inline-flex; align-items: center; gap: 6px; font-size: 0.80rem; }
    .btn:hover { background: #6d28d9; }
    .btn-copy { cursor: pointer; border: 1px solid #475569; background: #1e293b; color: #cbd5e1; border-radius: 5px; padding: 2px 7px; font-size: 0.72rem; font-weight: 500; }
    .btn-copy:hover { background: #334155; color: #fff; border-color: #64748b; }

    .regression-card:hover { border-color: #38bdf8 !important; }
    .kpi-info-btn { background: transparent !important; border: none !important; color: inherit; cursor: pointer; padding: 0 2px !important; margin: 0 !important; font-size: 0.95rem !important; display: inline-flex; align-items: center; justify-content: center; opacity: 0.75; transition: opacity 0.15s ease, transform 0.15s ease; line-height: 1; vertical-align: middle; outline: none; }
    .kpi-info-btn:hover { opacity: 1; transform: scale(1.18); background: transparent !important; border: none !important; }

    /* ABAS DO DOSSIÊ */
    .tabs-nav { display: flex; gap: 8px; margin-bottom: 16px; border-bottom: 1px solid var(--border); padding-bottom: 8px; }
    .tab-btn { background: transparent; border: none; color: var(--text-muted); font-size: 0.90rem; font-weight: 600; padding: 8px 16px; border-radius: 6px; cursor: pointer; transition: all 0.15s; }
    .tab-btn:hover { color: #fff; background: rgba(255,255,255,0.05); }
    .tab-btn.active { color: #38bdf8; background: rgba(56, 189, 248, 0.15); border: 1px solid rgba(56, 189, 248, 0.3); }

    /* AUTO REFRESH TOGGLE */
    .live-pulse { width: 8px; height: 8px; border-radius: 50%; background: #10b981; display: inline-block; box-shadow: 0 0 8px #10b981; animation: pulseLive 2s infinite; }
    @keyframes pulseLive { 0%, 100% { opacity: 1; transform: scale(1); } 50% { opacity: 0.4; transform: scale(1.2); } }
    /* BARRA DE PROGRESSO GLOBAL SUPERIOR */
    #topProgressBar {
      position: fixed;
      top: 0;
      left: 0;
      height: 3px;
      width: 0%;
      background: linear-gradient(90deg, #38bdf8, #818cf8, #a855f7, #10b981);
      z-index: 100000;
      opacity: 0;
      transition: width 0.25s ease, opacity 0.3s ease;
      box-shadow: 0 0 10px rgba(56, 189, 248, 0.85);
      pointer-events: none;
    }
    #topProgressBar.active {
      opacity: 1;
    }
    #topProgressBar.indeterminate {
      opacity: 1;
      width: 100% !important;
      background: linear-gradient(90deg, transparent, #38bdf8, #a855f7, #10b981, transparent);
      background-size: 200% 100%;
      animation: indeterminateBarAnim 1.1s cubic-bezier(0.4, 0, 0.2, 1) infinite;
    }
    @keyframes indeterminateBarAnim {
      0% { background-position: 200% 0; }
      100% { background-position: -200% 0; }
    }

    /* BADGE COM SPINNER DE ATUALIZAÇÃO */
    .updating-badge {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      font-size: 0.72rem;
      font-weight: 600;
      color: var(--accent-blue);
      background: rgba(56, 189, 248, 0.12);
      border: 1px solid rgba(56, 189, 248, 0.35);
      padding: 2px 9px;
      border-radius: 9999px;
      letter-spacing: 0.02em;
      transition: opacity 0.2s ease, transform 0.2s ease;
    }
    .spinner-icon {
      width: 11px;
      height: 11px;
      border: 2px solid rgba(56, 189, 248, 0.25);
      border-top-color: var(--accent-blue);
      border-radius: 50%;
      animation: spinIndicator 0.65s linear infinite;
      display: inline-block;
      flex-shrink: 0;
    }
    @keyframes spinIndicator {
      to { transform: rotate(360deg); }
    }
    .table-updating {
      opacity: 0.65;
      pointer-events: none;
      transition: opacity 0.15s ease;
    }
  </style>
</head>
<body>
  <!-- BARRA DE PROGRESSO SUPERIOR -->
  <div id="topProgressBar"></div>
  <header>
    <div>
      <h1>🗳️ Dossiê Técnico Forense <span class="badge badge-sync" style="font-size: 0.72rem; vertical-align: middle; margin-left: 6px; letter-spacing: 0.5px;">v1.0</span></h1>
      <div style="font-size: 0.80rem; color: var(--text-muted); margin-top: 4px;">
        Comparativo Contínuo: <strong style="color: #c084fc;">HMG (Fonte/Origem)</strong> vs <strong style="color: #38bdf8;">SIM (Cache/CDN Akamai)</strong> | Repositório: <code>tdtot_auditoria.db</code>
      </div>
    </div>
    <div style="display: flex; align-items: center; gap: 10px;">
      <button id="themeToggleBtn" onclick="toggleTheme()" class="btn btn-outline" style="padding: 6px 12px; font-size: 0.78rem; display: inline-flex; align-items: center; gap: 6px; cursor: pointer; border-radius: 6px;" title="Alternar entre modo escuro e claro">
        ☀️ Modo Claro
      </button>
      ${embeddedData ? `
      <div style="display: flex; align-items: center; gap: 6px; background: rgba(56, 189, 248, 0.15); border: 1px solid rgba(56, 189, 248, 0.4); padding: 4px 10px; border-radius: 6px; font-size: 0.78rem;">
        <span style="font-size: 0.9rem;">📦</span>
        <span style="color: #38bdf8; font-weight: 700;">Dossiê Autônomo Offline</span>
        <span id="lastRefreshTime" style="color: var(--text-muted); font-size: 0.72rem;">(${embeddedData.exportedAt || 'Exportado'})</span>
      </div>
      <button onclick="window.print()" class="btn" style="background: #334155; padding: 6px 12px; font-size: 0.78rem;" title="Imprimir / Salvar PDF">🖨️ Imprimir PDF</button>
      ` : `
      <div id="liveRefreshBadge" style="display: flex; align-items: center; gap: 6px; background: rgba(16, 185, 129, 0.1); border: 1px solid rgba(16, 185, 129, 0.3); padding: 4px 10px; border-radius: 6px; font-size: 0.78rem;">
        <span class="live-pulse"></span>
        <span style="color: #34d399; font-weight: 600;">Dossiê Dinâmico</span>
        <span id="lastRefreshTime" style="color: var(--text-muted); font-size: 0.72rem;">(atualizado agora)</span>
      </div>
      <button onclick="loadReportData()" class="btn" style="background: #334155; padding: 6px 12px; font-size: 0.78rem;" title="Atualizar dados do dossiê">↺ Atualizar</button>
      <a href="/export/dossie-html" class="btn" style="background: #0284c7; padding: 6px 12px; font-size: 0.78rem; text-decoration: none;" title="Baixar arquivo HTML autônomo offline para compartilhamento">📥 Baixar HTML Offline</a>
      <a href="/" target="_blank" class="btn" style="background: #475569; padding: 6px 12px; font-size: 0.78rem; text-decoration: none;" title="Abrir Dashboard Principal">📊 Abrir Dashboard</a>
      `}
    </div>
  </header>

  <!-- PAINEL DE FILTROS UNIFICADO -->
  <div class="card" style="padding: 14px 18px; margin-bottom: 16px;">
    <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 12px; flex-wrap: wrap; gap: 8px;">
      <div style="display: flex; align-items: center; gap: 8px;">
        <strong style="font-size: 0.90rem; color: #f8fafc;">🔍 Filtros Multidimensionais</strong>
        <span id="activeFiltersBadge" style="font-size: 0.72rem; background: rgba(56, 189, 248, 0.2); color: var(--accent-blue); padding: 2px 8px; border-radius: 9999px; display: none;">Filtros Ativos</span>
        <span id="dossieFilterUpdatingBadge" class="updating-badge" style="display: none;">
          <span class="spinner-icon"></span>
          <span>Filtrando...</span>
        </span>
      </div>
      <button class="btn-reset" onclick="resetFilters()" style="padding: 4px 10px; font-size: 0.75rem;">↺ Limpar Filtros</button>
    </div>

    <div class="filter-panel" style="margin-bottom: 0; padding: 0; border: none; background: transparent;">
      <div class="filter-group">
        <label class="filter-label">Busca Textual</label>
        <input type="text" id="searchInput" class="search-input" placeholder="Buscar por arquivo, UF, cargo..." oninput="applyFilters()">
      </div>

      <div class="filter-group">
        <label class="filter-label">Servidor</label>
        <select id="filterServidor" class="filter-select" onchange="applyFilters()">
          <option value="">Todos os Servidores</option>
          <option value="HMG">HMG (Origem Primária)</option>
          <option value="SIM">SIM (Cache Akamai CDN)</option>
        </select>
      </div>

      <div class="filter-group">
        <label class="filter-label" style="display:flex; justify-content:space-between; align-items:center;">
          <span>Rodada</span>
          <button type="button" onclick="openRodadasModal()" class="btn-copy" style="font-size:0.68rem; padding:1px 6px; cursor:pointer;" title="Gerenciar e Editar Parâmetros da Rodada">⚙️ Gerenciar</button>
        </label>
        <select id="filterRodada" class="filter-select" onchange="onRodadaFilterChanged()">
          <option value="">Carregando rodadas...</option>
        </select>
      </div>

      <div class="filter-group">
        <label class="filter-label">Pleito</label>
        <select id="filterPleito" class="filter-select" onchange="applyFilters()">
          <option value="">Todos os Pleitos</option>
        </select>
      </div>

      <div class="filter-group">
        <label class="filter-label">Eleição</label>
        <select id="filterEleicao" class="filter-select" onchange="applyFilters()">
          <option value="">Todas as Eleições</option>
          <option value="21270">21270 - Federal 1º T</option>
          <option value="21272">21272 - Estadual 1º T</option>
          <option value="Comum">Comum (Geral)</option>
        </select>
      </div>

      <div class="filter-group">
        <label class="filter-label">UF</label>
        <select id="filterUf" class="filter-select" onchange="applyFilters()">
          <option value="">Todas as UFs</option>
          <option value="BR">BR (Brasil Geral)</option>
          <option value="AC">AC</option><option value="AL">AL</option><option value="AM">AM</option><option value="AP">AP</option>
          <option value="BA">BA</option><option value="CE">CE</option><option value="DF">DF</option><option value="ES">ES</option>
          <option value="GO">GO</option><option value="MA">MA</option><option value="MG">MG</option><option value="MS">MS</option>
          <option value="MT">MT</option><option value="PA">PA</option><option value="PB">PB</option><option value="PE">PE</option>
          <option value="PI">PI</option><option value="PR">PR</option><option value="RJ">RJ</option><option value="RN">RN</option>
          <option value="RO">RO</option><option value="RR">RR</option><option value="RS">RS</option><option value="SC">SC</option>
          <option value="SE">SE</option><option value="SP">SP</option><option value="TO">TO</option><option value="ZZ">ZZ (Exterior)</option>
        </select>
      </div>

      <div class="filter-group">
        <label class="filter-label">Tipo de Arquivo</label>
        <select id="filterTipo" class="filter-select" onchange="applyFilters()">
          <option value="">Todos os Tipos</option>
          <option value="Totalização">Totalização / Resultados (-u.json)</option>
          <option value="Abrangência">Abrangência / Resumo Geral (-ab.json)</option>
          <option value="Configuração de Seção">Configuração de Seção (-cs.json)</option>
          <option value="Configuração">Configurações Gerais (-cm / -c.json)</option>
        </select>
      </div>

      <div class="filter-group">
        <label class="filter-label">Cargo</label>
        <select id="filterCargo" class="filter-select" onchange="applyFilters()">
          <option value="">Todos os Cargos</option>
          <option value="Presidente">Presidente</option>
          <option value="Governador">Governador</option>
          <option value="Senador">Senador</option>
          <option value="Dep. Federal">Deputado Federal</option>
          <option value="Dep. Estadual">Deputado Estadual</option>
          <option value="Dep. Distrital">Deputado Distrital</option>
        </select>
      </div>

      <div class="filter-group">
        <label class="filter-label">Status / Integridade</label>
        <select id="filterStatus" class="filter-select" onchange="applyFilters()">
          <option value="">Todos os Status</option>
          <option value="REG_ALL">🚨 Qualquer Regressão Detectada</option>
          <option value="REG_INVERSAO">🚨 Inversão de Dados (DG ↗, DT/ST ↘)</option>
          <option value="REG_TIME">🚨 Regressão de Geração (DG/HG)</option>
          <option value="REG_TOT">🚨 Regressão de Totalização (DT/HT)</option>
          <option value="REG_ST">🚨 Regressão de Seções (ST)</option>
          <option value="ATRASADO">⏳ Apenas Cache Atrasado</option>
          <option value="SINCRONIZADO">✅ Apenas Sincronizados</option>
          <option value="SIM_TTL_LOW">⚡ SIM TTL Baixo (≤ 15s)</option>
        </select>
      </div>
    </div>
  </div>

  <!-- KPIS PROPORCIONAIS DO DOSSIÊ -->
  <div class="stats-grid">
    <div class="stat-card" style="border-left: 3px solid #64748b;">
      <div class="stat-label" style="display: flex; justify-content: space-between; align-items: center;">
        <span>Arquivos Monitorados</span>
        <button type="button" class="kpi-info-btn" onclick="openKpiHelp(event, 'files')" title="Definição do indicador">ℹ️</button>
      </div>
      <div class="stat-value" id="kpiFiles">0</div>
      <div style="font-size: 0.72rem; color: var(--text-muted); margin-top: 2px;" id="kpiFilesSub">Total do catálogo</div>
    </div>
    <div class="stat-card" style="border-left: 3px solid #ef4444;">
      <div class="stat-label" style="display: flex; justify-content: space-between; align-items: center;">
        <span>Regressões (Rodada)</span>
        <button type="button" class="kpi-info-btn" onclick="openKpiHelp(event, 'regs')" title="Definição do indicador">ℹ️</button>
      </div>
      <div class="stat-value" id="kpiRegs" style="color: #f87171;">0</div>
      <div style="font-size: 0.72rem; color: var(--text-muted); margin-top: 2px;">Última: <span id="kpiLastReg" style="color: #fca5a5;">-</span></div>
    </div>
    <div class="stat-card" style="border-left: 3px solid #f59e0b;">
      <div class="stat-label" style="display: flex; justify-content: space-between; align-items: center;">
        <span>Cache Atrasado</span>
        <button type="button" class="kpi-info-btn" onclick="openKpiHelp(event, 'desync')" title="Definição do indicador">ℹ️</button>
      </div>
      <div class="stat-value" id="kpiDesync" style="color: #fbbf24;">0</div>
      <div style="font-size: 0.72rem; color: var(--text-muted); margin-top: 2px;" id="kpiDesyncSub">Arquivos desatualizados</div>
    </div>
    <div class="stat-card" style="border-left: 3px solid #10b981;">
      <div class="stat-label" style="display: flex; justify-content: space-between; align-items: center;">
        <span>SLA Sync Cache (DG/HG)</span>
        <div style="display: flex; align-items: center; gap: 6px;">
          <span id="slaScopeLabel" style="color: #34d399;">Hoje</span>
          <button type="button" class="kpi-info-btn" onclick="openKpiHelp(event, 'sla')" title="Definição do indicador">ℹ️</button>
        </div>
      </div>
      <div style="display: flex; justify-content: space-between; gap: 8px; align-items: baseline; margin-top: 4px;">
        <div><div style="font-size: 0.65rem; color: var(--text-muted);">MÉDIA</div><div id="slaAvg" style="font-size: 1.05rem; font-weight: 700; color: #fbbf24;">0m 00s</div></div>
        <div style="border-left: 1px solid var(--border); padding-left: 6px;"><div style="font-size: 0.65rem; color: var(--text-muted);">P90</div><div id="slaP90" style="font-size: 1.05rem; font-weight: 700; color: #fb923c;">0m 00s</div></div>
        <div style="border-left: 1px solid var(--border); padding-left: 6px;"><div style="font-size: 0.65rem; color: var(--text-muted);">P95</div><div id="slaP95" style="font-size: 1.05rem; font-weight: 700; color: #f87171;">0m 00s</div></div>
        <div style="border-left: 1px solid var(--border); padding-left: 6px;"><div style="font-size: 0.65rem; color: var(--text-muted);">P99</div><div id="slaP99" style="font-size: 1.05rem; font-weight: 700; color: #ef4444;">0m 00s</div></div>
      </div>
    </div>
    <div class="stat-card" style="border-left: 3px solid #38bdf8;">
      <div class="stat-label" style="display: flex; justify-content: space-between; align-items: center;">
        <span>Atributos HTTP & Edge Cache</span>
        <div style="display: flex; align-items: center; gap: 6px;">
          <span style="color: #38bdf8;">AKAMAI</span>
          <button type="button" class="kpi-info-btn" onclick="openKpiHelp(event, 'http')" title="Definição do indicador">ℹ️</button>
        </div>
      </div>
      <div style="display: flex; justify-content: space-between; gap: 8px; align-items: baseline; margin-top: 4px;">
        <div><div style="font-size: 0.65rem; color: var(--text-muted);">SIM TTL</div><div id="kpiTtl" style="font-size: 1.15rem; font-weight: 700; color: #38bdf8;">~60s</div></div>
        <div style="border-left: 1px solid var(--border); padding-left: 8px;"><div style="font-size: 0.65rem; color: var(--text-muted);">HIT RATE</div><div id="kpiHitRate" style="font-size: 1.15rem; font-weight: 700; color: #34d399;">100%</div></div>
        <div style="border-left: 1px solid var(--border); padding-left: 8px;"><div style="font-size: 0.65rem; color: var(--text-muted);">ORIGEM</div><div style="font-size: 0.82rem; font-weight: 600; color: #c084fc;">Apache</div></div>
      </div>
    </div>
  </div>

  <!-- NAVEGAÇÃO ENTRE SEÇÕES -->
  <div class="tabs-nav">
    <button id="tabBtnComp" class="tab-btn active" onclick="switchTab('comp')">📊 1. Comparativo de Propagação e SLA de Sincronização</button>
    <button id="tabBtnRegs" class="tab-btn" onclick="switchTab('regs')">🚨 2. Registro Detalhado de Regressões Temporais (<span id="tabRegsBadge">0</span>)</button>
  </div>

  <!-- SEÇÃO 1: TABELA COMPARATIVA DE PROPAGAÇÃO -->
  <div id="sectionComp" class="card" style="padding: 14px 18px; position: relative;">
    <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 12px; flex-wrap: wrap; gap: 8px;">
      <div style="display: flex; align-items: center; gap: 8px;">
        <strong style="font-size: 0.95rem; color: #f8fafc;">📊 1. Matriz de Propagação e SLA de Sincronização</strong>
        <span id="compTableCountBadge" style="font-size: 0.75rem; background: rgba(255,255,255,0.06); color: var(--text-muted); padding: 2px 8px; border-radius: 6px;">(carregando...)</span>
        <span id="dossieCompUpdatingBadge" class="updating-badge" style="display: none;">
          <span class="spinner-icon"></span>
          <span>Atualizando tabela...</span>
        </span>
      </div>

      <!-- SELETOR DINÂMICO DE COLUNAS -->
      <div style="display: flex; gap: 8px; align-items: center; position: relative;">
        <button id="btnColSelector" onclick="toggleColumnModal(event)" class="btn" style="background: transparent; border: 1px solid rgba(56,189,248,0.4); color: #38bdf8; height: 30px; padding: 0 12px; font-size: 0.78rem; cursor: pointer;">
          <span>⚙️ Colunas (<span id="visibleColsCount">8</span>/<span id="totalColsCount">15</span>)</span>
          <span style="font-size: 0.70rem;">▾</span>
        </button>

        <div id="colSelectorDropdown" style="display:none; position:absolute; right:0; top:36px; z-index:9999; background:#0f172a; border:1px solid rgba(255,255,255,0.15); box-shadow:0 12px 32px rgba(0,0,0,0.7); border-radius:8px; width:340px; padding:14px; font-size:0.82rem; text-align:left;">
          <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:8px; border-bottom:1px solid rgba(255,255,255,0.08); padding-bottom:8px;">
            <strong style="color:#f8fafc; font-size:0.88rem;">⚙️ Configurar Colunas do Dossiê</strong>
            <div style="display:flex; gap:5px;">
              <button onclick="resetDefaultColumns()" class="btn-copy" style="font-size:0.68rem; padding:2px 6px;">Padrão</button>
              <button onclick="toggleAllColumns(true)" class="btn-copy" style="font-size:0.68rem; padding:2px 6px;">Todas</button>
              <button onclick="toggleColumnModal(event)" style="background:transparent; border:none; color:var(--text-muted); cursor:pointer; font-size:0.9rem; margin-left:4px;">✕</button>
            </div>
          </div>
          <div style="color:var(--text-muted); font-size:0.72rem; margin-bottom:10px;">
            Ative ou desative as colunas exibidas no dossiê de auditoria. Suas opções são salvas automaticamente no navegador.
          </div>
          <div id="colCheckboxesList" style="display:flex; flex-direction:column; gap:6px; max-height:360px; overflow-y:auto; padding-right:4px;">
            <!-- Renderizado via JS -->
          </div>
        </div>
      </div>
    </div>

    <!-- Tabela Comparativa Estrita -->
    <table id="compTable">
      <thead>
        <tr>
          <th data-col="classificacao" class="sortable" onclick="setCompSort('classificacao')">Classificação <span id="sort_classificacao" class="sort-icon">⇅</span></th>
          <th data-col="arquivo" class="sortable" onclick="setCompSort('arquivo')">Caminho do Arquivo & Inspeção <span id="sort_arquivo" class="sort-icon">⇅</span></th>
          <th data-col="hmg_time" class="sortable" onclick="setCompSort('hmg_time')" title="Data/Hora de Geração na FONTE (DG/HG)"><span class="tag-hmg-title">FONTE (HMG)</span><br>Geração (DG/HG) <span id="sort_hmg_time" class="sort-icon">⇅</span></th>
          <th data-col="sim_time" class="sortable" onclick="setCompSort('sim_time')" title="Data/Hora de Geração no CACHE (DG/HG)"><span class="tag-sim-title">CACHE (SIM)</span><br>Geração (DG/HG) <span id="sort_sim_time" class="sort-icon">⇅</span></th>
          <th data-col="delay_time" class="sortable" onclick="setCompSort('delay_time')" title="Diferença na data/hora de geração">Δ Tempo DG/HG <span id="sort_delay_time" class="sort-icon">⇅</span></th>
          <th data-col="sync_sla" class="sortable" onclick="setCompSort('sync_sla')" title="Tempo decorrido até replicação efetiva no cache">SLA Sync (DG/HG) <span id="sort_sync_sla" class="sort-icon">⇅</span></th>
          <th data-col="cache_ttl" class="sortable" onclick="setCompSort('cache_ttl')" title="Cache-Control, TTL, CDN e Headers HTTP">⚡ Cache & TTL <span id="sort_cache_ttl" class="sort-icon">⇅</span></th>
          <th data-col="status" class="sortable" onclick="setCompSort('status')">Integridade <span id="sort_status" class="sort-icon">⇅</span></th>
          <th data-col="origin_tot" class="sortable" onclick="setCompSort('origin_tot')" title="Data/Hora da Totalização na FONTE (DT/HT)"><span class="tag-hmg-title">FONTE (HMG)</span><br>Totalização (DT/HT) <span id="sort_origin_tot" class="sort-icon">⇅</span></th>
          <th data-col="replica_tot" class="sortable" onclick="setCompSort('replica_tot')" title="Data/Hora da Totalização no CACHE (DT/HT)"><span class="tag-sim-title">CACHE (SIM)</span><br>Totalização (DT/HT) <span id="sort_replica_tot" class="sort-icon">⇅</span></th>
          <th data-col="delay_tot" class="sortable" onclick="setCompSort('delay_tot')" title="Diferença no horário de totalização">Δ Tempo DT/HT <span id="sort_delay_tot" class="sort-icon">⇅</span></th>
          <th data-col="origin_st" class="sortable" onclick="setCompSort('origin_st')" title="Seções apuradas na FONTE (ST / %)"><span class="tag-hmg-title">FONTE (HMG)</span><br>Seções (ST / %) <span id="sort_origin_st" class="sort-icon">⇅</span></th>
          <th data-col="replica_st" class="sortable" onclick="setCompSort('replica_st')" title="Seções apuradas no CACHE (ST / %)"><span class="tag-sim-title">CACHE (SIM)</span><br>Seções (ST / %) <span id="sort_replica_st" class="sort-icon">⇅</span></th>
          <th data-col="diff_st" class="sortable" onclick="setCompSort('diff_st')" title="Diferença na quantidade de seções apuradas">Δ Seções <span id="sort_diff_st" class="sort-icon">⇅</span></th>
          <th data-col="idg" class="sortable" onclick="setCompSort('idg')" title="Identificador Sequencial de Geração (IDG)">IDG <span id="sort_idg" class="sort-icon">⇅</span></th>
        </tr>
      </thead>
      <tbody id="compTableBody">
        <tr><td colspan="15" style="text-align: center; color: var(--text-muted); padding: 30px;">Carregando dossiê comparativo...</td></tr>
      </tbody>
    </table>
  </div>

  <!-- SEÇÃO 2: REGISTRO DETALHADO DE REGRESSÕES TEMPORAIS (LAYOUT FORENSE SPLIT) -->
  <div id="sectionRegs" class="card" style="padding: 16px 20px; display: none;">
    <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 12px; flex-wrap: wrap; gap: 8px;">
      <div style="display: flex; align-items: center; gap: 8px;">
        <strong style="font-size: 1.05rem; color: #f8fafc;">🚨 2. Dossiê Forense de Regressões Temporais Reais (Auditoria Estrita)</strong>
        <span id="regsTableCountBadge" style="font-size: 0.75rem; background: rgba(239,68,68,0.15); color: #f87171; border: 1px solid rgba(239,68,68,0.3); padding: 2px 8px; border-radius: 6px;">(carregando...)</span>
        <span id="dossieRegsUpdatingBadge" class="updating-badge" style="display: none;">
          <span class="spinner-icon"></span>
          <span>Atualizando ocorrências...</span>
        </span>
      </div>
      <div style="font-size: 0.78rem; color: var(--text-muted);">
        Clique em qualquer ocorrência na coluna esquerda para fixar e auditar seus cabeçalhos HTTP e atributos de rede no painel direito.
      </div>
    </div>

    <!-- Barra de Filtros Internos da Seção 2 -->
    <div style="background:#0f172a; border:1px solid #334155; border-radius:8px; padding:10px 14px; margin-bottom:14px; display:flex; flex-wrap:wrap; gap:10px; align-items:center;">
      <div style="flex:1; min-width:260px; display:flex; gap:6px;">
        <input type="text" id="dossieRegSearchInput" placeholder="🔍 Buscar por ID (#26374), arquivo, UF, cargo, IP, GRN ou motivo..." oninput="applyFilters()" style="flex:1; background:#1e293b; border:1px solid #475569; color:#f8fafc; padding:6px 12px; border-radius:6px; font-size:0.82rem; outline:none;" />
      </div>
      <div style="display:flex; align-items:center; gap:6px;">
        <label style="font-size:0.75rem; color:#94a3b8; font-weight:700;">GRN:</label>
        <input type="text" id="dossieRegFilterGrn" placeholder="Filtrar por Akamai-GRN..." oninput="applyFilters()" style="background:#1e293b; border:1px solid #475569; color:#f8fafc; padding:6px 8px; border-radius:6px; font-size:0.78rem; width:160px; outline:none;" />
      </div>
      <div style="display:flex; align-items:center; gap:6px;">
        <label style="font-size:0.75rem; color:#94a3b8; font-weight:700;">Servidor:</label>
        <select id="dossieRegFilterServer" onchange="applyFilters()" style="background:#1e293b; border:1px solid #475569; color:#f8fafc; padding:6px 8px; border-radius:6px; font-size:0.78rem;">
          <option value="">Todos os Servidores</option>
          <option value="SIM">SIM (Cache Akamai)</option>
          <option value="HMG">HMG (Fonte Oficial)</option>
        </select>
      </div>
      <div style="display:flex; align-items:center; gap:6px;">
        <label style="font-size:0.75rem; color:#94a3b8; font-weight:700;">UF:</label>
        <select id="dossieRegFilterUf" onchange="applyFilters()" style="background:#1e293b; border:1px solid #475569; color:#f8fafc; padding:6px 8px; border-radius:6px; font-size:0.78rem;">
          <option value="">Todas as UFs</option>
        </select>
      </div>
      <div style="display:flex; align-items:center; gap:6px;">
        <label style="font-size:0.75rem; color:#94a3b8; font-weight:700;">Critério:</label>
        <select id="dossieRegFilterCriterion" onchange="applyFilters()" style="background:#1e293b; border:1px solid #475569; color:#f8fafc; padding:6px 8px; border-radius:6px; font-size:0.78rem;">
          <option value="">Todos os Critérios</option>
          <option value="INVERSAO_DG_DT_ST">🚨 Inversão de Dados (DG ↗, DT/ST ↘)</option>
          <option value="TEMPO">DG/HG (Tempo Geração)</option>
          <option value="TOTALIZAÇÃO">DT/HT (Totalização)</option>
          <option value="SEÇÕES">ST (Seções Apuradas)</option>
          <option value="SEQUENCIAL">IDG (Sequencial)</option>
        </select>
      </div>
      <div style="margin-left:auto; display:flex; align-items:center; gap:10px;">
        <button id="btnToggleAllDossieRegs" type="button" onclick="toggleAllDossieRegCards()" class="btn-copy" style="padding:5px 12px; font-size:0.78rem; background:#1e293b; border:1px solid #475569; color:#cbd5e1; display:flex; align-items:center; gap:6px; cursor:pointer;" title="Expandir ou colapsar todas as ocorrências">
          <span id="toggleAllDossieRegsIcon">↕️</span> <span id="toggleAllDossieRegsText">Expandir Todos</span>
        </button>
        <div id="dossieRegShowingCount" style="font-size:0.78rem; color:#64748b;">
          Exibindo 0 de 0
        </div>
      </div>
    </div>

    <!-- CORPO DA SEÇÃO: LAYOUT SPLIT EM 2 COLUNAS COM ROLAGEM INDEPENDENTE -->
    <div style="display:flex; gap:16px; min-height:650px; height:calc(88vh - 220px); max-height:920px; overflow:hidden;">
      
      <!-- Coluna Esquerda: Feed / Lista de Ocorrências com Scroll Independente -->
      <div id="dossieRegsListContainer" style="flex:1.05; min-width:0; overflow-y:auto; padding-right:8px; display:flex; flex-direction:column; gap:12px;">
        <!-- Preenchido dinamicamente via JS -->
      </div>

      <!-- Coluna Direita: Painel Lateral Fixo com Detalhes Técnicos e Scroll Independente -->
      <div id="dossieTechPanel" style="flex:0.95; min-width:460px; max-width:720px; background:#0f172a; border:1px solid #334155; border-radius:10px; display:flex; flex-direction:column; overflow:hidden; box-shadow:inset 0 2px 8px rgba(0,0,0,0.3);">
        <div style="background:#1e293b; padding:10px 14px; border-bottom:1px solid #334155; display:flex; justify-content:space-between; align-items:center; flex-shrink:0;">
          <div style="font-size:0.85rem; font-weight:700; color:#38bdf8; display:flex; align-items:center; gap:6px;">
            <span>🌐</span> Painel Forense & Detalhes Técnicos
          </div>
          <div id="dossieTechPanelSelectedBadge" style="font-family:monospace; font-size:0.75rem; color:#94a3b8;">
            Nenhum selecionado
          </div>
        </div>
        <div id="dossieTechPanelContent" style="flex:1; min-height:0; overflow-y:auto; padding:14px; font-size:0.78rem;">
          <div style="text-align:center; padding:60px 20px; color:#64748b;">
            <div style="font-size:2rem; margin-bottom:8px;">👈</div>
            <div>Selecione qualquer ocorrência na lista à esquerda para auditar aqui seus metadados de rede, cabeçalhos de solicitação, resposta e controle de cache.</div>
          </div>
        </div>
      </div>

    </div>
  </div>

  <footer style="margin-top: 30px; padding-top: 14px; border-top: 1px solid var(--border); font-size: 0.78rem; color: var(--text-muted); display: flex; justify-content: space-between; align-items: center;">
    <div>Auditoria Técnica de Resultados TDTot TSE - Comparativo Contínuo HMG (Fonte) vs SIM (Distribuição/CDN).</div>
    <div>Banco de Dados: <code>tdtot_auditoria.db</code> | Atualização Automática: <strong>Ativa (10s)</strong></div>
  </footer>

  <script>
    // =====================================================================
    // GERENCIADOR DE TEMAS (DARK / LIGHT)
    // =====================================================================
    var selectedDossieRegressionId = null;

    function initTheme() {
      const saved = localStorage.getItem('tdtot_theme') || 'dark';
      applyTheme(saved);
    }

    function applyTheme(theme) {
      const btn = document.getElementById('themeToggleBtn');
      if (theme === 'light') {
        document.documentElement.setAttribute('data-theme', 'light');
        if (btn) btn.innerHTML = '🌙 Modo Escuro';
      } else {
        document.documentElement.removeAttribute('data-theme');
        if (btn) btn.innerHTML = '☀️ Modo Claro';
      }
      localStorage.setItem('tdtot_theme', theme);
      if (typeof selectDossieRegression === 'function' && selectedDossieRegressionId) {
        selectDossieRegression(selectedDossieRegressionId);
      }
    }

    function toggleTheme() {
      const isLight = document.documentElement.getAttribute('data-theme') === 'light';
      applyTheme(isLight ? 'dark' : 'light');
    }

    initTheme();

    // =====================================================================
    // SELETOR DINÂMICO DE COLUNAS (COMPARTILHADO COM O DASHBOARD)
    // =====================================================================
    const AVAILABLE_COLUMNS = [
      { id: 'classificacao', label: 'Classificação', default: true, desc: 'Pleito, Eleição, UF, Tipo e Cargo' },
      { id: 'arquivo',       label: 'Caminho do Arquivo & Ações', default: true, desc: 'Caminho relativo e links de inspeção' },
      { id: 'origin_time',   label: 'FONTE: Geração (DG/HG)', default: true, desc: 'Data/Hora de geração na Origem' },
      { id: 'replica_time',  label: 'CACHE: Geração (DG/HG)', default: true, desc: 'Data/Hora de geração no Cache' },
      { id: 'delay_time',    label: 'Δ Tempo DG/HG', default: true, desc: 'Diferença de geração em Minutos e Segundos' },
      { id: 'sync_sla',      label: 'SLA Sync (DG/HG)', default: true, desc: 'Tempo real até replicação na borda/cache' },
      { id: 'cache_ttl',     label: '⚡ Cache & TTL', default: true, desc: 'Cache-Control, max-age e CDN status' },
      { id: 'status',        label: 'Integridade', default: true, desc: 'Auditoria forense e integridade temporal' },
      { id: 'origin_tot',    label: 'FONTE: Totalização (DT/HT)', default: false, desc: 'Data/Hora de apuração consolidada na Origem' },
      { id: 'replica_tot',   label: 'CACHE: Totalização (DT/HT)', default: false, desc: 'Data/Hora de apuração consolidada no Cache' },
      { id: 'delay_tot',     label: 'Δ Tempo DT/HT', default: false, desc: 'Diferença de fechamento entre Origem e Cache' },
      { id: 'origin_st',     label: 'FONTE: Seções (ST / %)', default: false, desc: 'Seções apuradas e percentual na Origem' },
      { id: 'replica_st',    label: 'CACHE: Seções (ST / %)', default: false, desc: 'Seções apuradas e percentual no Cache' },
      { id: 'diff_st',       label: 'Δ Seções', default: false, desc: 'Diferença de seções apuradas entre Origem e Cache' },
      { id: 'idg',           label: 'IDG Sequencial', default: false, desc: 'Identificador Sequencial de Geração' }
    ];

    function getVisibleColumns() {
      try {
        const stored = localStorage.getItem('tdtot_visible_columns');
        if (stored) {
          const parsed = JSON.parse(stored);
          if (Array.isArray(parsed) && parsed.length > 0) return parsed;
        }
      } catch (e) {}
      return AVAILABLE_COLUMNS.filter(c => c.default).map(c => c.id);
    }

    function saveVisibleColumns(cols) {
      try {
        localStorage.setItem('tdtot_visible_columns', JSON.stringify(cols));
      } catch (e) {}
      applyColumnVisibility();
    }

    function applyColumnVisibility() {
      const visible = getVisibleColumns();
      const visibleSet = new Set(visible);

      let styleEl = document.getElementById('dynamicColumnsCss');
      if (!styleEl) {
        styleEl = document.createElement('style');
        styleEl.id = 'dynamicColumnsCss';
        document.head.appendChild(styleEl);
      }

      let css = '';
      for (const col of AVAILABLE_COLUMNS) {
        if (!visibleSet.has(col.id)) {
          css += '[data-col="' + col.id + '"] { display: none !important; } ';
        }
      }
      styleEl.textContent = css;

      const countEl = document.getElementById('visibleColsCount');
      if (countEl) countEl.textContent = visible.length;
      const totalEl = document.getElementById('totalColsCount');
      if (totalEl) totalEl.textContent = AVAILABLE_COLUMNS.length;

      renderColumnCheckboxes();
    }

    function renderColumnCheckboxes() {
      const listEl = document.getElementById('colCheckboxesList');
      if (!listEl) return;
      const visible = new Set(getVisibleColumns());

      listEl.innerHTML = AVAILABLE_COLUMNS.map(col => {
        const isChecked = visible.has(col.id);
        const isDefault = col.default;
        return '<label style="display:flex; align-items:flex-start; gap:8px; padding:6px 8px; border-radius:6px; cursor:pointer; background:' + (isChecked ? 'rgba(56,189,248,0.08)' : 'rgba(255,255,255,0.02)') + '; border:1px solid ' + (isChecked ? 'rgba(56,189,248,0.25)' : 'rgba(255,255,255,0.05)') + ';">' +
          '<input type="checkbox" ' + (isChecked ? 'checked' : '') + ' data-col-id="' + col.id + '" onchange="toggleColumn(this.dataset.colId, this.checked)" style="margin-top:3px; accent-color:#38bdf8; cursor:pointer;">' +
          '<div style="flex:1;">' +
            '<div style="display:flex; align-items:center; justify-content:space-between;">' +
              '<span style="font-weight:600; color:' + (isChecked ? '#f8fafc' : '#94a3b8') + '; font-size:0.80rem;">' + col.label + '</span>' +
              (isDefault ? '<span style="font-size:0.64rem; color:var(--text-muted); background:rgba(255,255,255,0.06); padding:1px 4px; border-radius:3px;">Padrão</span>' : '<span style="font-size:0.64rem; color:#38bdf8; background:rgba(56,189,248,0.1); padding:1px 4px; border-radius:3px;">Opcional</span>') +
            '</div>' +
            '<div style="font-size:0.68rem; color:var(--text-muted); line-height:1.2; margin-top:2px;">' + col.desc + '</div>' +
          '</div>' +
        '</label>';
      }).join('');
    }

    function toggleColumn(colId, enable) {
      let current = getVisibleColumns();
      if (enable) {
        if (!current.includes(colId)) current.push(colId);
      } else {
        if (current.length <= 1) {
          alert('Pelo menos uma coluna deve permanecer visível.');
          applyColumnVisibility();
          return;
        }
        current = current.filter(id => id !== colId);
      }
      saveVisibleColumns(current);
    }

    function resetDefaultColumns() {
      const defaults = AVAILABLE_COLUMNS.filter(c => c.default).map(c => c.id);
      saveVisibleColumns(defaults);
    }

    function toggleAllColumns(enable) {
      if (enable) {
        saveVisibleColumns(AVAILABLE_COLUMNS.map(c => c.id));
      } else {
        resetDefaultColumns();
      }
    }

    function toggleColumnModal(e) {
      if (e) e.stopPropagation();
      const dd = document.getElementById('colSelectorDropdown');
      if (!dd) return;
      dd.style.display = (dd.style.display === 'none' || !dd.style.display) ? 'block' : 'none';
    }

    document.addEventListener('click', (e) => {
      const dd = document.getElementById('colSelectorDropdown');
      const btn = document.getElementById('btnColSelector');
      if (dd && dd.style.display === 'block') {
        if (!dd.contains(e.target) && !btn.contains(e.target)) {
          dd.style.display = 'none';
        }
      }
    });

    // =====================================================================
    // NAVEGAÇÃO DE ABAS
    // =====================================================================
    let currentTab = 'comp';
    function switchTab(tab) {
      currentTab = tab;
      document.getElementById('tabBtnComp').className = 'tab-btn ' + (tab === 'comp' ? 'active' : '');
      document.getElementById('tabBtnRegs').className = 'tab-btn ' + (tab === 'regs' ? 'active' : '');
      document.getElementById('sectionComp').style.display = tab === 'comp' ? 'block' : 'none';
      document.getElementById('sectionRegs').style.display = tab === 'regs' ? 'block' : 'none';
    }

    // =====================================================================
    // INDICADORES VISUAIS DE CARREGAMENTO / CONSULTA / FILTRAGEM (DOSSIÊ)
    // =====================================================================
    function showLoading(msg = 'Atualizando dados...') {
      const bar = document.getElementById('topProgressBar');
      if (bar) {
        bar.classList.add('active', 'indeterminate');
      }
      const compBadge = document.getElementById('dossieCompUpdatingBadge');
      if (compBadge) {
        compBadge.style.display = 'inline-flex';
        const span = compBadge.querySelector('span:last-child');
        if (span) span.textContent = msg;
      }
      const regsBadge = document.getElementById('dossieRegsUpdatingBadge');
      if (regsBadge) {
        regsBadge.style.display = 'inline-flex';
        const span = regsBadge.querySelector('span:last-child');
        if (span) span.textContent = msg;
      }
      const filterBadge = document.getElementById('dossieFilterUpdatingBadge');
      if (filterBadge) {
        filterBadge.style.display = 'inline-flex';
        const span = filterBadge.querySelector('span:last-child');
        if (span) span.textContent = msg.includes('Filtrando') ? msg : 'Consultando...';
      }
      const compTable = document.getElementById('compTableBody');
      if (compTable) compTable.classList.add('table-updating');
    }

    function hideLoading() {
      const bar = document.getElementById('topProgressBar');
      if (bar) {
        bar.classList.remove('indeterminate');
        bar.style.width = '100%';
        setTimeout(() => {
          bar.classList.remove('active');
          bar.style.width = '0%';
        }, 220);
      }
      const compBadge = document.getElementById('dossieCompUpdatingBadge');
      if (compBadge) compBadge.style.display = 'none';
      const regsBadge = document.getElementById('dossieRegsUpdatingBadge');
      if (regsBadge) regsBadge.style.display = 'none';
      const filterBadge = document.getElementById('dossieFilterUpdatingBadge');
      if (filterBadge) filterBadge.style.display = 'none';
      const compTable = document.getElementById('compTableBody');
      if (compTable) compTable.classList.remove('table-updating');
    }

    let filterDebounceTimer = null;
    function debounceApplyFilters(delay = 120, customMsg = 'Filtrando...') {
      showLoading(customMsg);
      if (filterDebounceTimer) clearTimeout(filterDebounceTimer);
      filterDebounceTimer = setTimeout(() => {
        applyFiltersInternal();
      }, delay);
    }

    function applyFilters() {
      debounceApplyFilters(40, 'Filtrando...');
    }

    // =====================================================================
    // ESTADO E ORDENAÇÃO
    // =====================================================================
    let rawComparisonList = [];
    let rawRegressionsList = [];
    let rawRodadasList = [];
    let activeRodada = null;
    let latestApiData = null;

    let compSortCol = 'delay_time';
    let compSortDir = 'desc';

    let regsSortCol = 'id';
    let regsSortDir = 'desc';

    function setCompSort(col) {
      if (compSortCol === col) {
        compSortDir = (compSortDir === 'asc') ? 'desc' : 'asc';
      } else {
        compSortCol = col;
        compSortDir = (col === 'classificacao' || col === 'arquivo') ? 'asc' : 'desc';
      }
      updateCompSortIcons();
      applyFilters();
    }

    function setRegsSort(col) {
      if (regsSortCol === col) {
        regsSortDir = (regsSortDir === 'asc') ? 'desc' : 'asc';
      } else {
        regsSortCol = col;
        regsSortDir = (col === 'id' || col === 'timestamp_iso') ? 'desc' : 'asc';
      }
      updateRegsSortIcons();
      applyFilters();
    }

    function updateCompSortIcons() {
      const cols = ['classificacao', 'arquivo', 'hmg_time', 'sim_time', 'delay_time', 'sync_sla', 'cache_ttl', 'status', 'origin_tot', 'replica_tot', 'delay_tot', 'origin_st', 'replica_st', 'diff_st', 'idg'];
      for (const c of cols) {
        const el = document.getElementById('sort_' + c);
        const th = el ? el.closest('th') : null;
        if (!el || !th) continue;
        th.classList.remove('sorted-asc', 'sorted-desc');
        if (c === compSortCol) {
          el.textContent = compSortDir === 'asc' ? '▲' : '▼';
          th.classList.add(compSortDir === 'asc' ? 'sorted-asc' : 'sorted-desc');
        } else {
          el.textContent = '⇅';
        }
      }
    }

    function updateRegsSortIcons() {
      const cols = ['id', 'timestamp_iso', 'servidor', 'criterio', 'arquivo', 'motivo'];
      for (const c of cols) {
        const el = document.getElementById('sort_reg_' + c);
        const th = el ? el.closest('th') : null;
        if (!el || !th) continue;
        th.classList.remove('sorted-asc', 'sorted-desc');
        if (c === regsSortCol) {
          el.textContent = regsSortDir === 'asc' ? '▲' : '▼';
          th.classList.add(regsSortDir === 'asc' ? 'sorted-asc' : 'sorted-desc');
        } else {
          el.textContent = '⇅';
        }
      }
    }

    function formatMinSec(sec) {
      if (sec === null || sec === undefined || isNaN(sec)) return '-';
      const s = Math.round(Number(sec));
      const sign = s < 0 ? '-' : '';
      const abs = Math.abs(s);
      const m = Math.floor(abs / 60);
      const remS = abs % 60;
      return sign + m + 'm ' + String(remS).padStart(2, '0') + 's';
    }

    // =====================================================================
    // CARREGAMENTO DINÂMICO DOS DADOS
    // =====================================================================
    let hasLoadedInversions = false;
    async function loadReportData(isAuto = false) {
      try {
        if (!isAuto) showLoading('Atualizando dossiê forense...');
        const sel = document.getElementById('filterRodada');
        const fRod = sel ? sel.value : '';

        const compUrl = fRod ? ('/api/comparison?rodadaId=' + encodeURIComponent(fRod)) : '/api/comparison';
        const regsUrl = fRod ? ('/api/regressoes?rodadaId=' + encodeURIComponent(fRod) + '&limit=300&light=1') : '/api/regressoes?all=1&limit=300&light=1';

        const fetchPromises = [
          fetch(compUrl),
          fetch(regsUrl),
          fetch('/api/rodadas')
        ];

        const responses = await Promise.all(fetchPromises);
        const compData = await responses[0].json();
        const regsData = await responses[1].json();
        const rodadasData = await responses[2].json();

        latestApiData = compData;
        rawComparisonList = compData.comparison || [];
        rawRegressionsList = regsData.regressoes || [];
        rawRodadasList = rodadasData.list || [];
        activeRodada = rodadasData.active || null;

        updateRodadasDropdown();
        updatePleitosDropdown(rawComparisonList);
        updateAutoRefreshIndicator();
        applyFiltersInternal();

        const lrEl = document.getElementById('lastRefreshTime');
        if (lrEl) lrEl.textContent = '(' + new Date().toLocaleTimeString('pt-BR') + ')';
      } catch(e) {
        console.error('Erro ao carregar dados do dossiê:', e);
      } finally {
        if (!isAuto) hideLoading();
      }
    }

        function formatDateTimeFull(dt) {
      if (!dt || dt === '-') return '-';
      const d = new Date(dt);
      if (isNaN(d.getTime())) return String(dt);
      const pad = n => String(n).padStart(2, '0');
      const dia = pad(d.getDate());
      const mes = pad(d.getMonth() + 1);
      const ano = d.getFullYear();
      const hora = pad(d.getHours());
      const min = pad(d.getMinutes());
      const seg = pad(d.getSeconds());
      return dia + '/' + mes + '/' + ano + ' - ' + hora + ':' + min + ':' + seg;
    }

    const KPI_HELP_TEXTS = {
      files: {
        title: '📁 Arquivos Monitorados',
        body: '<p><strong>O que representa:</strong> Quantidade de artefatos JSON de totalização eleitoral atualmente sendo inspecionados sob os filtros ativos (Rodada, Pleito, Eleição, UF, Cargo ou Busca).</p><p><strong>Comportamento:</strong> O valor principal exibe o total de arquivos filtrados. O subtexto indica a proporção em relação ao catálogo cadastrado (ex.: <em>"de 229 no catálogo"</em> quando há filtros, ou <em>"Total do catálogo"</em> sem filtros).</p>'
      },
      regs: {
        title: '🚨 Regressões Temporais e Inversões',
        body: '<p><strong>O que representa:</strong> Total de anomalias periciais detectadas no ciclo/rodada em que o nó de cache (SIM/Akamai) ou a própria origem retornou dados cronologicamente ou quantitativamente inferiores a um estado anterior já validado (recuo de DG/HG, redução de votos/seções ou retrocesso de IDG).</p><p><strong>Carimbo "Última":</strong> Exibe a data e o horário exato da ocorrência mais recente registrada no banco de dados no formato estrito <code>dd/MM/yyyy - HH:mm:ss</code>.</p>'
      },
      desync: {
        title: '⏳ Cache Atrasado (Defasagem Borda vs Origem)',
        body: '<p><strong>O que representa:</strong> Quantidade de arquivos em que a <strong>Origem (HMG)</strong> já disponibilizou uma versão mais recente (data/hora de geração DG/HG mais nova ou número sequencial IDG superior), porém a <strong>Réplica de Cache (SIM / Akamai)</strong> continua entregando uma versão anterior ao eleitor/usuário.</p><p><strong>Por que pode marcar 100% (ex: 142/142)?</strong><br>Quando a Origem (HMG) totaliza uma nova rodada e os nós de Cache (SIM) deixam de receber atualizações ou o simulador é paralisado (por exemplo, na noite de 11/09 às 20:04 a origem atualizou, enquanto o nó SIM parou às 19:31), todos os arquivos que tiveram novos dados na origem ficam pendentes de sincronização na borda, resultando em 100% de defasagem legítima detectada pela auditoria.</p>'
      },
      sla: {
        title: '⚡ SLA de Sincronização de Cache (DG/HG)',
        body: '<p><strong>O que representa:</strong> Mede o tempo decorrido (em segundos/minutos) entre a geração do arquivo na Origem HMG e sua efetiva disponibilidade capturada na Réplica SIM (Cache Akamai).</p><p><strong>Métricas:</strong><ul><li><strong>MÉDIA:</strong> Tempo médio de propagação dos arquivos na rodada.</li><li><strong>P90, P95, P99:</strong> Percentis de latência (90%, 95% e 99% das atualizações foram sincronizadas dentro deste limite de tempo).</li></ul></p><p><em>Nota:</em> Em rodadas sem alterações concomitantes nos dois ambientes, o indicador exibe 0m 00s.</p>'
      },
      http: {
        title: '🌐 Atributos HTTP & Edge Cache (Akamai)',
        body: '<p><strong>O que representa:</strong> Métricas de eficiência e configuração de entrega na borda da CDN:<ul><li><strong>SIM TTL (max-age):</strong> Tempo de vida estipulado no cabeçalho HTTP <code>Cache-Control</code> indicando por quantos segundos o nó de cache mantém o arquivo em memória antes de consultar a origem.</li><li><strong>HIT RATE:</strong> Percentual de requisições atendidas diretamente pelo cache de borda (Akamai Cache HIT) sem onerar a infraestrutura de origem.</li><li><strong>ORIGEM:</strong> Servidor web responsável pela geração primária dos dados (Apache / HMG).</li></ul></p>'
      }
    };

    function isFilteringActiveRodada() {
      const sel = document.getElementById('filterRodada');
      const fRod = sel ? sel.value : '';
      if (!fRod) return true; // Todas as rodadas
      return activeRodada && String(activeRodada.id) === String(fRod);
    }

    function updateAutoRefreshIndicator() {
      const badge = document.getElementById('liveRefreshBadge');
      const sel = document.getElementById('filterRodada');
      const fRod = sel ? sel.value : '';
      if (!badge) return;

      if (!isFilteringActiveRodada()) {
        badge.style.background = 'rgba(148, 163, 184, 0.15)';
        badge.style.borderColor = 'rgba(148, 163, 184, 0.4)';
        badge.style.color = '#94a3b8';
        badge.innerHTML = '⚪ Rodada Histórica #' + fRod + ' <span style="font-size:0.70rem; opacity:0.8;">(Auto-Refresh Pausado)</span>';
      } else {
        badge.style.background = 'rgba(16, 185, 129, 0.15)';
        badge.style.borderColor = 'rgba(16, 185, 129, 0.35)';
        badge.style.color = '#34d399';
        badge.innerHTML = '<span class="live-pulse"></span> Dossiê Dinâmico <span id="lastRefreshTime" style="font-size:0.72rem; color:var(--text-muted);">(' + new Date().toLocaleTimeString('pt-BR') + ')</span>';
      }
    }

    function openKpiHelp(event, kpiKey) {
      if (event) event.stopPropagation();
      const popover = document.getElementById('kpiHelpPopover');
      if (!popover) return;

      const info = KPI_HELP_TEXTS[kpiKey];
      if (!info) return;

      document.getElementById('kpiHelpTitle').innerHTML = info.title;
      document.getElementById('kpiHelpBody').innerHTML = info.body;

      popover.style.display = 'block';

      const trigger = event ? event.currentTarget : null;
      const card = trigger ? trigger.closest('.stat-card') : null;

      if (card) {
        const rect = card.getBoundingClientRect();
        const scrollY = window.pageYOffset || document.documentElement.scrollTop;
        const scrollX = window.pageXOffset || document.documentElement.scrollLeft;

        popover.style.top = (rect.bottom + scrollY + 8) + 'px';

        let leftPos = rect.left + scrollX;
        const popWidth = 420;
        if (leftPos + popWidth > window.innerWidth) {
          leftPos = Math.max(10, window.innerWidth - popWidth - 20 + scrollX);
        }
        popover.style.left = leftPos + 'px';
      } else {
        popover.style.top = '140px';
        popover.style.left = '50%';
        popover.style.transform = 'translateX(-50%)';
      }
    }

    function closeKpiHelp() {
      const popover = document.getElementById('kpiHelpPopover');
      if (popover) popover.style.display = 'none';
    }

    document.addEventListener('click', function(e) {
      const pop = document.getElementById('kpiHelpPopover');
      if (pop && pop.style.display === 'block' && !pop.contains(e.target) && !e.target.closest('.kpi-info-btn')) {
        closeKpiHelp();
      }
    });

    async function onRodadaFilterChanged() {
      const sel = document.getElementById('filterRodada');
      const fRod = sel ? sel.value : '';
      showLoading('Carregando dados da rodada ' + (fRod ? ('#' + fRod) : 'completa') + '...');
      try {
        const urlComp = fRod ? ('/api/comparison?rodadaId=' + encodeURIComponent(fRod)) : '/api/comparison';
        const urlRegs = fRod ? ('/api/regressoes?rodadaId=' + encodeURIComponent(fRod) + '&limit=300&light=1') : '/api/regressoes?all=1&limit=300&light=1';
        const [resComp, resRegs] = await Promise.all([fetch(urlComp), fetch(urlRegs)]);
        const compData = await resComp.json();
        const regsData = await resRegs.json();

        latestApiData = compData;
        rawComparisonList = compData.comparison || [];
        rawRegressionsList = regsData.regressoes || [];

        updatePleitosDropdown(rawComparisonList);
        updateAutoRefreshIndicator();
        applyFiltersInternal();
      } catch(e) {
        console.error('Erro ao alternar rodada:', e);
      } finally {
        hideLoading();
      }
    }

    function updateKpis(filteredComp, filteredRegs, compData) {
      if (!filteredComp) filteredComp = rawComparisonList;
      if (!filteredRegs) filteredRegs = rawRegressionsList;
      if (!compData) compData = latestApiData || {};

      const totalFiles = rawComparisonList.length;
      const totalRegs = rawRegressionsList.length;
      const isFiltered = filteredComp.length !== totalFiles || filteredRegs.length !== totalRegs;

      // 1. Arquivos Monitorados
      const kpiFilesEl = document.getElementById('kpiFiles');
      if (kpiFilesEl) kpiFilesEl.textContent = filteredComp.length;
      const kpiFilesSubEl = document.getElementById('kpiFilesSub');
      if (kpiFilesSubEl) {
        kpiFilesSubEl.innerHTML = isFiltered 
          ? ('de <strong style="color:var(--text);">' + totalFiles + '</strong> no catálogo')
          : ('Total do catálogo (' + totalFiles + ')');
      }

      // 2. Regressões (Rodada / Filtradas)
      const kpiRegsEl = document.getElementById('kpiRegs');
      if (kpiRegsEl) kpiRegsEl.textContent = filteredRegs.length;
      const tabRegsBadge = document.getElementById('tabRegsBadge');
      if (tabRegsBadge) tabRegsBadge.textContent = filteredRegs.length;

      const kpiLastRegEl = document.getElementById('kpiLastReg');
      if (kpiLastRegEl) {
        if (filteredRegs.length > 0) {
          const firstReg = filteredRegs[0];
          const timeStr = firstReg.timestamp_iso || firstReg.call_time_iso;
          kpiLastRegEl.textContent = formatDateTimeFull(timeStr || compData.lastRegressionTime);
        } else if (compData.lastRegressionTime && compData.lastRegressionTime !== '-') {
          kpiLastRegEl.textContent = formatDateTimeFull(compData.lastRegressionTime);
        } else {
          kpiLastRegEl.textContent = '-';
        }
      }

      // 3. Cache Atrasado
      let desync = 0;
      for (const r of filteredComp) {
        if (r.comparison?.status === 'CACHE_ATRASADO') desync++;
      }
      const kpiDesyncEl = document.getElementById('kpiDesync');
      if (kpiDesyncEl) kpiDesyncEl.textContent = desync;
      const kpiDesyncSubEl = document.getElementById('kpiDesyncSub');
      if (kpiDesyncSubEl) {
        const pct = filteredComp.length ? Math.round((desync / filteredComp.length) * 100) : 0;
        kpiDesyncSubEl.textContent = pct + '% dos filtrados (' + desync + '/' + filteredComp.length + ')';
      }

      // 4. SLA Sync Cache (DG/HG)
      const slaScopeEl = document.getElementById('slaScopeLabel');
      const slaTimes = [];
      for (const r of filteredComp) {
        if (r.comparison?.syncSlaSec !== undefined && r.comparison?.syncSlaSec !== null && !isNaN(r.comparison.syncSlaSec) && Number(r.comparison.syncSlaSec) > 0) {
          slaTimes.push(Number(r.comparison.syncSlaSec));
        }
      }
      slaTimes.sort((a, b) => a - b);

      if (isFiltered && slaTimes.length > 0) {
        if (slaScopeEl) slaScopeEl.textContent = 'Filtrados';
        const avg = Math.round(slaTimes.reduce((a, b) => a + b, 0) / slaTimes.length);
        const p90 = slaTimes[Math.floor(slaTimes.length * 0.90)];
        const p95 = slaTimes[Math.floor(slaTimes.length * 0.95)];
        const p99 = slaTimes[Math.floor(slaTimes.length * 0.99)];
        document.getElementById('slaAvg').textContent = formatMinSec(avg);
        document.getElementById('slaP90').textContent = formatMinSec(p90);
        document.getElementById('slaP95').textContent = formatMinSec(p95);
        document.getElementById('slaP99').textContent = formatMinSec(p99);
      } else if (compData.todaySlaStats && compData.todaySlaStats.totalEvents > 0) {
        if (slaScopeEl) slaScopeEl.textContent = compData.activeRodada ? ('#' + compData.activeRodada.id) : 'Rodada';
        document.getElementById('slaAvg').textContent = formatMinSec(compData.todaySlaStats.avgSec);
        document.getElementById('slaP90').textContent = formatMinSec(compData.todaySlaStats.p90Sec);
        document.getElementById('slaP95').textContent = formatMinSec(compData.todaySlaStats.p95Sec);
        document.getElementById('slaP99').textContent = formatMinSec(compData.todaySlaStats.p99Sec);
      } else if (slaTimes.length > 0) {
        if (slaScopeEl) slaScopeEl.textContent = 'Rodada';
        const avg = Math.round(slaTimes.reduce((a, b) => a + b, 0) / slaTimes.length);
        const p90 = slaTimes[Math.floor(slaTimes.length * 0.90)];
        const p95 = slaTimes[Math.floor(slaTimes.length * 0.95)];
        const p99 = slaTimes[Math.floor(slaTimes.length * 0.99)];
        document.getElementById('slaAvg').textContent = formatMinSec(avg);
        document.getElementById('slaP90').textContent = formatMinSec(p90);
        document.getElementById('slaP95').textContent = formatMinSec(p95);
        document.getElementById('slaP99').textContent = formatMinSec(p99);
      } else {
        if (slaScopeEl) slaScopeEl.textContent = isFiltered ? 'Filtrados' : 'Rodada';
        document.getElementById('slaAvg').textContent = '0m 00s';
        document.getElementById('slaP90').textContent = '0m 00s';
        document.getElementById('slaP95').textContent = '0m 00s';
        document.getElementById('slaP99').textContent = '0m 00s';
      }

      // 5. Atributos HTTP & Edge Cache
      let simTtlSum = 0, simTtlCount = 0;
      let cdnHits = 0, cdnTotal = 0;
      for (const r of filteredComp) {
        const ma = (r.sim && r.sim.maxAge !== null && r.sim.maxAge !== undefined) ? Number(r.sim.maxAge) : (r.comparison?.cacheDiff?.sim?.maxAge ?? null);
        if (ma !== null && !isNaN(ma)) {
          simTtlSum += ma;
          simTtlCount++;
        }
        const cdn = r.sim?.cdnCacheStatus || r.sim?.cdnStatus || r.comparison?.cacheDiff?.sim?.cdnStatus || r.comparison?.cdnStatus;
        if (cdn && cdn !== '-') {
          cdnTotal++;
          if (String(cdn).toLowerCase().includes('hit')) cdnHits++;
        }
      }
      if (simTtlCount > 0) {
        document.getElementById('kpiTtl').textContent = Math.round(simTtlSum / simTtlCount) + 's';
      } else if (!isFiltered && compData.cacheStats?.simAvgTtl) {
        document.getElementById('kpiTtl').textContent = compData.cacheStats.simAvgTtl + 's';
      } else {
        document.getElementById('kpiTtl').textContent = '-';
      }

      if (cdnTotal > 0) {
        document.getElementById('kpiHitRate').textContent = Math.round((cdnHits / cdnTotal) * 100) + '%';
      } else if (!isFiltered && compData.cacheStats?.cdnHitRate !== undefined) {
        document.getElementById('kpiHitRate').textContent = compData.cacheStats.cdnHitRate + '%';
      } else {
        document.getElementById('kpiHitRate').textContent = '-';
      }
    }

    function updateRodadasDropdown() {
      const sel = document.getElementById('filterRodada');
      if (!sel) return;
      const curVal = sel.value;
      sel.innerHTML = '<option value="">Todas as Rodadas</option>';
      for (const r of rawRodadasList) {
        const opt = document.createElement('option');
        opt.value = String(r.id);
        const ativoTag = r.ativo ? ' [ATIVA]' : '';
        opt.textContent = '#' + r.id + ' ' + r.nome + ativoTag;
        sel.appendChild(opt);
      }
      if (curVal !== undefined && curVal !== null && curVal !== '') sel.value = curVal;
      else if (activeRodada) sel.value = String(activeRodada.id);
    }

    function updatePleitosDropdown(list) {
      const sel = document.getElementById('filterPleito');
      if (!sel) return;
      const curVal = sel.value;
      const pleitos = new Set();
      for (const r of list) {
        if (r.meta?.pleito && r.meta.pleito !== '-' && r.meta.pleito !== 'Todos') {
          pleitos.add(r.meta.pleito);
        }
      }
      const sorted = Array.from(pleitos).sort();
      sel.innerHTML = '<option value="">Todos os Pleitos</option>';
      for (const p of sorted) {
        const opt = document.createElement('option');
        opt.value = p;
        opt.textContent = 'Pleito ' + p;
        sel.appendChild(opt);
      }
      if (curVal && sorted.includes(curVal)) sel.value = curVal;
    }

    function resetFilters() {
      document.getElementById('searchInput').value = '';
      document.getElementById('filterServidor').value = '';
      if (activeRodada) document.getElementById('filterRodada').value = String(activeRodada.id);
      else document.getElementById('filterRodada').value = '';
      document.getElementById('filterPleito').value = '';
      document.getElementById('filterEleicao').value = '';
      document.getElementById('filterUf').value = '';
      document.getElementById('filterTipo').value = '';
      document.getElementById('filterCargo').value = '';
      document.getElementById('filterStatus').value = '';
      onRodadaFilterChanged();
    }

    // =====================================================================
    // APLICAÇÃO DE FILTROS E ORDENAÇÃO
    // =====================================================================
    function applyFiltersInternal() {
      const q = document.getElementById('searchInput').value.toLowerCase().trim();
      const fSrv = document.getElementById('filterServidor').value;
      const fRod = document.getElementById('filterRodada').value;
      const fPleito = document.getElementById('filterPleito').value;
      const fEle = document.getElementById('filterEleicao').value;
      const fUf = document.getElementById('filterUf').value;
      const fTipo = document.getElementById('filterTipo').value;
      const fCargo = document.getElementById('filterCargo').value;
      const fStatus = document.getElementById('filterStatus').value;

      // 1. Filtra Matriz Comparativa
      const filteredComp = rawComparisonList.filter(row => {
        const meta = row.meta || {};
        if (fPleito && meta.pleito !== fPleito && meta.pleito !== 'Todos') return false;
        if (fEle && meta.eleicao !== fEle) return false;
        if (fUf && meta.uf !== fUf) return false;
        if (fTipo) {
          if (fTipo === 'Configuração' && !meta.tipo.includes('Configuração')) return false;
          else if (fTipo !== 'Configuração' && meta.tipo !== fTipo) return false;
        }
        if (fCargo && meta.cargo !== fCargo) return false;

        if (fSrv) {
          if (fSrv === 'HMG' && (!row.hmg || !row.hmg.genTime)) return false;
          if (fSrv === 'SIM' && (!row.sim || !row.sim.genTime)) return false;
        }

        if (fStatus === 'REG_ALL') {
          const hasReg = (row.hmg?.status === 'REGRESSAO_DETECTADA' || row.sim?.status === 'REGRESSAO_DETECTADA');
          if (!hasReg) return false;
        } else if (fStatus === 'REG_TIME') {
          const hmgReg = row.hmg?.criterion?.includes('TEMPO') || row.hmg?.criterion?.includes('DG/HG');
          const simReg = row.sim?.criterion?.includes('TEMPO') || row.sim?.criterion?.includes('DG/HG');
          if (!hmgReg && !simReg) return false;
        } else if (fStatus === 'REG_TOT') {
          const hmgRegTot = row.hmg?.criterion?.includes('TOTALIZAÇÃO');
          const simRegTot = row.sim?.criterion?.includes('TOTALIZAÇÃO');
          if (!hmgRegTot && !simRegTot) return false;
        } else if (fStatus === 'REG_ST') {
          const hmgRegSt = row.hmg?.criterion?.includes('SEÇÕES');
          const simRegSt = row.sim?.criterion?.includes('SEÇÕES');
          if (!hmgRegSt && !simRegSt) return false;
        } else if (fStatus === 'ATRASADO') {
          if (row.comparison?.status !== 'CACHE_ATRASADO') return false;
        } else if (fStatus === 'SINCRONIZADO') {
          if (row.comparison?.status !== 'SINCRONIZADO') return false;
        } else if (fStatus === 'SIM_TTL_LOW') {
          if (!row.sim || row.sim.maxAge === null || row.sim.maxAge > 15) return false;
        }

        if (q) {
          const matchPath = row.relPath.toLowerCase().includes(q);
          const matchCargo = (meta.cargo || '').toLowerCase().includes(q);
          const matchUf = (meta.uf || '').toLowerCase().includes(q);
          if (!matchPath && !matchCargo && !matchUf) return false;
        }

        return true;
      });

      // 2. Filtra Regressões Forenses
      let rodadaObj = null;
      if (fRod) {
        rodadaObj = rawRodadasList.find(r => String(r.id) === String(fRod));
      }

      // Filtros internos específicos da Seção 2 (Dossiê)
      const dQ = (document.getElementById('dossieRegSearchInput') ? document.getElementById('dossieRegSearchInput').value : '').toLowerCase().trim();
      const dGrn = (document.getElementById('dossieRegFilterGrn') ? document.getElementById('dossieRegFilterGrn').value : '').toLowerCase().trim();
      const dSrv = document.getElementById('dossieRegFilterServer') ? document.getElementById('dossieRegFilterServer').value : '';
      const dUf = document.getElementById('dossieRegFilterUf') ? document.getElementById('dossieRegFilterUf').value.toLowerCase().trim() : '';
      const dCrit = document.getElementById('dossieRegFilterCriterion') ? document.getElementById('dossieRegFilterCriterion').value.toUpperCase().trim() : '';

      const filteredRegs = rawRegressionsList.filter(reg => {
        if (rodadaObj) {
          const regUnix = new Date(reg.timestamp_iso).getTime();
          if (regUnix < rodadaObj.inicio_unix) return false;
          if (rodadaObj.fim_unix && regUnix > rodadaObj.fim_unix) return false;
        }

        if (fSrv && reg.servidor !== fSrv) return false;
        if (dSrv && reg.servidor !== dSrv) return false;

        const meta = reg.fileMeta || {};
        if (fPleito && meta.pleito && meta.pleito !== fPleito && meta.pleito !== 'Todos') return false;
        if (fEle && meta.eleicao && meta.eleicao !== fEle) return false;
        if (fUf && meta.uf && meta.uf !== fUf) return false;
        if (dUf && (meta.uf || '').toLowerCase() !== dUf) return false;

        if (fTipo && meta.tipo) {
          if (fTipo === 'Configuração' && !meta.tipo.includes('Configuração')) return false;
          else if (fTipo !== 'Configuração' && meta.tipo !== fTipo) return false;
        }
        if (fCargo && meta.cargo && meta.cargo !== fCargo) return false;

        if (dCrit) {
          const c = (reg.criterio || '').toUpperCase();
          const m = (reg.motivo || '').toUpperCase();
          const d = (reg.detalhes || '').toUpperCase();
          if (dCrit === 'INVERSAO_DG_DT_ST') {
            const hasInversionTag = c.includes('INVERSÃO') || m.includes('INVERSÃO') || d.includes('INVERSÃO') || m.includes('INVERSÃO_DG_DT_ST') || d.includes('INVERSÃO_DG_DT_ST');
            const dgAdv = Boolean(reg.dg_anterior && reg.dg_recebido && reg.hg_anterior && reg.hg_recebido && (
              reg.dg_recebido > reg.dg_anterior || (reg.dg_recebido === reg.dg_anterior && reg.hg_recebido >= reg.hg_anterior)
            ));
            const totReg = Boolean(reg.dt_recebido && reg.dt_anterior && (reg.dt_recebido < reg.dt_anterior || (reg.dt_recebido === reg.dt_anterior && reg.ht_recebido < reg.ht_anterior)));
            const stReg = Boolean(reg.secoes_recebido !== null && reg.secoes_anterior !== null && Number(reg.secoes_recebido) < Number(reg.secoes_anterior));
            if (!hasInversionTag && !(dgAdv && (totReg || stReg))) return false;
          } else {
            if (!c.includes(dCrit) && !m.includes(dCrit)) return false;
          }
        }

        if (fStatus === 'REG_INVERSAO') {
          const c = (reg.criterio || '').toUpperCase();
          const m = (reg.motivo || '').toUpperCase();
          const d = (reg.detalhes || '').toUpperCase();
          const hasInversionTag = c.includes('INVERSÃO') || m.includes('INVERSÃO') || d.includes('INVERSÃO') || m.includes('INVERSÃO_DG_DT_ST') || d.includes('INVERSÃO_DG_DT_ST');
          const dgAdv = Boolean(reg.dg_anterior && reg.dg_recebido && reg.hg_anterior && reg.hg_recebido && (
            reg.dg_recebido > reg.dg_anterior || (reg.dg_recebido === reg.dg_anterior && reg.hg_recebido >= reg.hg_anterior)
          ));
          const totReg = Boolean(reg.dt_recebido && reg.dt_anterior && (reg.dt_recebido < reg.dt_anterior || (reg.dt_recebido === reg.dt_anterior && reg.ht_recebido < reg.ht_anterior)));
          const stReg = Boolean(reg.secoes_recebido !== null && reg.secoes_anterior !== null && Number(reg.secoes_recebido) < Number(reg.secoes_anterior));
          if (!hasInversionTag && !(dgAdv && (totReg || stReg))) return false;
        } else if (fStatus === 'REG_TIME') {
          if (!reg.criterio?.includes('TEMPO') && !reg.motivo?.includes('TEMPORAL')) return false;
        } else if (fStatus === 'REG_TOT') {
          if (!reg.criterio?.includes('TOTALIZAÇÃO') && !reg.motivo?.includes('TOTALIZAÇÃO')) return false;
        } else if (fStatus === 'REG_ST') {
          if (!reg.criterio?.includes('SEÇÕES') && !reg.motivo?.includes('SEÇÕES')) return false;
        }

        if (dGrn) {
          const rGrn = (reg.akamai_grn || '').toLowerCase();
          const rRawGrn = (reg.rawMeta?.headers?.['akamai-grn'] || reg.rawMeta?.headers?.['x-akamai-grn'] || '').toLowerCase();
          const matchTimelineGrn = (reg.timeline || []).some(step => (step.akamai_grn || '').toLowerCase().includes(dGrn));
          if (!rGrn.includes(dGrn) && !rRawGrn.includes(dGrn) && !matchTimelineGrn) return false;
        }

        if (dQ) {
          const cleanQ = dQ.replace(/^#/, '');
          const matchId = String(reg.id || '').includes(cleanQ);
          const matchIdg = String(reg.idg_recebido || '').includes(cleanQ) || String(reg.idg_anterior || '').includes(cleanQ);
          const matchFile = (reg.arquivo || '').toLowerCase().includes(cleanQ);
          const matchMotivo = (reg.motivo || '').toLowerCase().includes(cleanQ);
          const matchServer = (reg.servidor || '').toLowerCase().includes(cleanQ);
          const matchUf = (meta.uf || '').toLowerCase().includes(cleanQ);
          const matchCargo = (meta.cargo || '').toLowerCase().includes(cleanQ);
          const matchGrn = (reg.akamai_grn || '').toLowerCase().includes(cleanQ);
          const matchIp = (reg.server_ip || '').toLowerCase().includes(cleanQ) || (reg.timeline || []).some(s => (s.server_ip || '').toLowerCase().includes(cleanQ));
          if (!matchId && !matchIdg && !matchFile && !matchMotivo && !matchServer && !matchUf && !matchCargo && !matchGrn && !matchIp) return false;
        }

        if (q) {
          const matchPath = reg.arquivo.toLowerCase().includes(q);
          const matchMotivo = (reg.motivo || '').toLowerCase().includes(q);
          const matchUf = (meta.uf || '').toLowerCase().includes(q);
          if (!matchPath && !matchMotivo && !matchUf) return false;
        }

        return true;
      });

      const hasActiveFilters = Boolean(q || fSrv || fPleito || fEle || fUf || fTipo || fCargo || fStatus || dQ || dGrn || dSrv || dUf || dCrit || (fRod && activeRodada && fRod !== String(activeRodada.id)));
      const afb = document.getElementById('activeFiltersBadge');
      if (afb) afb.style.display = hasActiveFilters ? 'inline-block' : 'none';

      const kpiVisEl = document.getElementById('kpiFilesVisible');
      if (kpiVisEl) kpiVisEl.textContent = filteredComp.length;
      updateKpis(filteredComp, filteredRegs, latestApiData);
      const compBadge = document.getElementById('compTableCountBadge');
      if (compBadge) compBadge.textContent = '(' + filteredComp.length + ' de ' + rawComparisonList.length + ' arquivos)';
      const regsBadge = document.getElementById('regsTableCountBadge');
      if (regsBadge) regsBadge.textContent = '(' + filteredRegs.length + ' de ' + rawRegressionsList.length + ' ocorrências)';
      
      const dossieCountEl = document.getElementById('dossieRegShowingCount');
      if (dossieCountEl) dossieCountEl.textContent = 'Exibindo ' + filteredRegs.length + ' de ' + rawRegressionsList.length;

      const kpiRegsEl = document.getElementById('kpiRegs');
      if (kpiRegsEl) kpiRegsEl.textContent = filteredRegs.length;
      const tabRegsBadge = document.getElementById('tabRegsBadge');
      if (tabRegsBadge) tabRegsBadge.textContent = filteredRegs.length;

      renderCompTable(sortCompData(filteredComp));
      renderRegsTable(sortRegsData(filteredRegs));
      hideLoading();
    }

    function sortCompData(rows) {
      return [...rows].sort((a, b) => {
        let vA, vB;
        switch(compSortCol) {
          case 'classificacao':
            vA = (a.meta?.uf || '') + '_' + (a.meta?.cargo || '') + '_' + (a.meta?.tipo || '');
            vB = (b.meta?.uf || '') + '_' + (b.meta?.cargo || '') + '_' + (b.meta?.tipo || '');
            break;
          case 'arquivo':
            vA = a.relPath.toLowerCase();
            vB = b.relPath.toLowerCase();
            break;
          case 'hmg_time':
            vA = a.hmg?.genTime ?? -1;
            vB = b.hmg?.genTime ?? -1;
            break;
          case 'sim_time':
            vA = a.sim?.genTime ?? -1;
            vB = b.sim?.genTime ?? -1;
            break;
          case 'delay_time':
            vA = Number(a.comparison?.delaySec ?? 0);
            vB = Number(b.comparison?.delaySec ?? 0);
            break;
          case 'sync_sla':
            vA = Number(a.comparison?.syncSlaSec ?? 0);
            vB = Number(b.comparison?.syncSlaSec ?? 0);
            break;
          case 'cache_ttl':
            vA = Number(a.sim?.maxAge ?? 0);
            vB = Number(b.sim?.maxAge ?? 0);
            break;
          case 'status':
            const regA = (a.hmg?.status === 'REGRESSAO_DETECTADA' || a.sim?.status === 'REGRESSAO_DETECTADA') ? 3 : (a.comparison?.status === 'CACHE_ATRASADO' ? 2 : 1);
            const regB = (b.hmg?.status === 'REGRESSAO_DETECTADA' || b.sim?.status === 'REGRESSAO_DETECTADA') ? 3 : (b.comparison?.status === 'CACHE_ATRASADO' ? 2 : 1);
            vA = regA;
            vB = regB;
            break;
          case 'origin_tot':
            vA = (a.origin?.totTime ?? a.hmg?.totTime ?? -1);
            vB = (b.origin?.totTime ?? b.hmg?.totTime ?? -1);
            break;
          case 'replica_tot':
            vA = (a.sim?.totTime ?? -1);
            vB = (b.sim?.totTime ?? -1);
            break;
          case 'delay_tot':
            vA = (a.comparison?.delayTotSec ?? 0);
            vB = (b.comparison?.delayTotSec ?? 0);
            break;
          case 'origin_st':
            vA = Number(a.origin?.st ?? a.hmg?.st ?? -1);
            vB = Number(b.origin?.st ?? b.hmg?.st ?? -1);
            break;
          case 'replica_st':
            vA = Number(a.sim?.st ?? -1);
            vB = Number(b.sim?.st ?? -1);
            break;
          case 'diff_st':
            vA = Number(a.comparison?.diffSt ?? 0);
            vB = Number(b.comparison?.diffSt ?? 0);
            break;
          case 'idg':
            vA = Number(a.origin?.idgNum ?? a.hmg?.idgNum ?? 0);
            vB = Number(b.origin?.idgNum ?? b.hmg?.idgNum ?? 0);
            break;
          default:
            vA = a.relPath;
            vB = b.relPath;
        }
        if (vA < vB) return compSortDir === 'asc' ? -1 : 1;
        if (vA > vB) return compSortDir === 'asc' ? 1 : -1;
        return a.relPath.localeCompare(b.relPath);
      });
    }

    function sortRegsData(rows) {
      return [...rows].sort((a, b) => {
        let vA, vB;
        switch(regsSortCol) {
          case 'id':
            vA = Number(a.id);
            vB = Number(b.id);
            break;
          case 'timestamp_iso':
            vA = new Date(a.timestamp_iso).getTime();
            vB = new Date(b.timestamp_iso).getTime();
            break;
          case 'servidor':
            vA = a.servidor || '';
            vB = b.servidor || '';
            break;
          case 'criterio':
            vA = a.criterio || '';
            vB = b.criterio || '';
            break;
          case 'arquivo':
            vA = a.arquivo.toLowerCase();
            vB = b.arquivo.toLowerCase();
            break;
          case 'motivo':
            vA = a.motivo || '';
            vB = b.motivo || '';
            break;
          default:
            vA = a.id;
            vB = b.id;
        }
        if (vA < vB) return regsSortDir === 'asc' ? -1 : 1;
        if (vA > vB) return regsSortDir === 'asc' ? 1 : -1;
        return 0;
      });
    }

    // =====================================================================
    // RENDERIZAÇÃO DAS TABELAS
    // =====================================================================
    function renderCompTable(rows) {
      const tbody = document.getElementById('compTableBody');
      tbody.innerHTML = '';
      const visibleCols = getVisibleColumns();

      if (rows.length === 0) {
        tbody.innerHTML = '<tr><td colspan="' + visibleCols.length + '" style="text-align: center; color: var(--text-muted); padding: 30px;">Nenhum arquivo corresponde aos filtros aplicados.</td></tr>';
        return;
      }

      for (const row of rows) {
        const meta = row.meta || {};
        const tr = document.createElement('tr');

        // Badge Atraso DG/HG
        let delayTimeBadge = '<span class="badge badge-sync">0m 00s</span>';
        if (row.comparison?.statusTime === 'CACHE_ATRASADO') {
          delayTimeBadge = '<span class="badge badge-lag">' + row.comparison.textTime + '</span>';
        } else if (row.comparison?.statusTime === 'CACHE_A_FRENTE') {
          delayTimeBadge = '<span class="badge badge-ahead">' + row.comparison.textTime + '</span>';
        } else if (row.comparison?.statusTime === 'SEM_TIMESTAMP') {
          delayTimeBadge = '<span class="badge" style="background:#334155">-</span>';
        }

        // Origem
        const originMeta = row.origin || row.hmg;
        const originIdgTag = originMeta && originMeta.idg ? '<br><span style="font-size:0.70rem; color:var(--text-muted); font-family:monospace;">IDG: ' + originMeta.idg + '</span>' : '';
        const originHg = originMeta ? '<a href="' + (row.originUrl || row.hmgUrl) + '" target="_blank" style="color:var(--accent-purple); text-decoration:none;" title="Abrir JSON da Origem"><strong style="font-size:0.95rem;">' + originMeta.hg + ' ↗</strong></a><br><span style="font-size:0.75rem;color:var(--text-muted);">' + originMeta.dg + '</span>' + originIdgTag : '<span style="color:#64748b">Pendente</span>';

        // Réplica
        const simMeta = row.sim;
        const simIdgTag = simMeta && simMeta.idg ? '<br><span style="font-size:0.70rem; color:var(--text-muted); font-family:monospace;">IDG: ' + simMeta.idg + '</span>' : '';
        const simHg = simMeta ? '<a href="' + row.simUrl + '" target="_blank" style="color:var(--accent-blue); text-decoration:none;" title="Abrir JSON do Cache"><strong style="font-size:0.95rem;">' + simMeta.hg + ' ↗</strong></a><br><span style="font-size:0.75rem;color:var(--text-muted);">' + simMeta.dg + '</span>' + simIdgTag : '<span style="color:#64748b">Pendente</span>';

        // SLA Sync
        let syncSlaBadge = '<span class="badge" style="background:#334155">-</span>';
        if (row.comparison?.syncSlaStatus === 'OK') syncSlaBadge = '<span class="badge badge-sync">⚡ ' + row.comparison.syncSlaText + '</span>';
        else if (row.comparison?.syncSlaStatus === 'ALERTA') syncSlaBadge = '<span class="badge badge-lag">⚠️ ' + row.comparison.syncSlaText + '</span>';
        else if (row.comparison?.syncSlaStatus === 'CRITICO') syncSlaBadge = '<span class="badge badge-danger">' + row.comparison.syncSlaText + '</span>';

        // Status Integridade
        let orderStatus = '<span class="badge badge-sync">OK</span>';
        const hasReg = (row.hmg?.status === 'REGRESSAO_DETECTADA' || row.sim?.status === 'REGRESSAO_DETECTADA');
        if (hasReg) {
          const regSrv = row.hmg?.status === 'REGRESSAO_DETECTADA' ? 'HMG' : 'SIM';
          const regCrit = (row.hmg?.status === 'REGRESSAO_DETECTADA' ? row.hmg?.criterion : row.sim?.criterion) || 'REGRESSÃO';
          orderStatus = '<span class="badge badge-danger">REGRESSÃO (' + regSrv + '): ' + regCrit + '</span>';
        }

        // Totalização DT/HT
        let originTotCell = '<span style="color:var(--text-muted); font-size:0.75rem;">-</span>';
        if (originMeta?.dt && originMeta?.ht) {
          originTotCell = '<div style="display:flex; flex-direction:column; gap:1px;"><strong style="color:var(--accent-purple); font-size:0.88rem;">' + originMeta.ht + '</strong><span style="font-size:0.72rem; color:var(--text-muted);">' + originMeta.dt + '</span></div>';
        }

        let replicaTotCell = '<span style="color:var(--text-muted); font-size:0.75rem;">-</span>';
        if (row.sim?.dt && row.sim?.ht) {
          replicaTotCell = '<div style="display:flex; flex-direction:column; gap:1px;"><strong style="color:var(--accent-blue); font-size:0.88rem;">' + row.sim.ht + '</strong><span style="font-size:0.72rem; color:var(--text-muted);">' + row.sim.dt + '</span></div>';
        }

        let delayTotCell = '<span style="color:var(--text-muted); font-size:0.75rem;">-</span>';
        if (originMeta?.totTime !== null && originMeta?.totTime !== undefined && row.sim?.totTime !== null && row.sim?.totTime !== undefined) {
          const diffTot = Math.round((originMeta.totTime - row.sim.totTime) / 1000);
          if (diffTot === 0) delayTotCell = '<span class="badge badge-sync">0m 00s</span>';
          else if (diffTot > 0) delayTotCell = '<span class="badge badge-lag">-' + formatMinSec(diffTot) + '</span>';
          else delayTotCell = '<span class="badge badge-ahead">+' + formatMinSec(-diffTot) + '</span>';
        }

        // Seções ST
        let originStCell = '<span style="color:var(--text-muted); font-size:0.75rem;">-</span>';
        if (originMeta?.st !== null && originMeta?.st !== undefined) {
          originStCell = '<div style="display:flex; flex-direction:column; gap:1px;"><strong style="color:#10b981; font-size:0.88rem;">' + Number(originMeta.st).toLocaleString('pt-BR') + '</strong><span class="badge badge-sync" style="font-size:0.65rem; padding:1px 4px; width:fit-content;">' + (originMeta.pst || '0,00') + '%</span></div>';
        }

        let replicaStCell = '<span style="color:var(--text-muted); font-size:0.75rem;">-</span>';
        if (row.sim?.st !== null && row.sim?.st !== undefined) {
          replicaStCell = '<div style="display:flex; flex-direction:column; gap:1px;"><strong style="color:#38bdf8; font-size:0.88rem;">' + Number(row.sim.st).toLocaleString('pt-BR') + '</strong><span class="badge badge-sync" style="font-size:0.65rem; padding:1px 4px; width:fit-content;">' + (row.sim.pst || '0,00') + '%</span></div>';
        }

        let diffStCell = '<span style="color:var(--text-muted); font-size:0.75rem;">-</span>';
        if (originMeta?.st !== null && originMeta?.st !== undefined && row.sim?.st !== null && row.sim?.st !== undefined) {
          const diffSt = Number(originMeta.st) - Number(row.sim.st);
          if (diffSt === 0) diffStCell = '<span class="badge badge-sync">0 seç</span>';
          else if (diffSt > 0) diffStCell = '<span class="badge badge-lag">-' + diffSt + ' seç</span>';
          else diffStCell = '<span class="badge badge-ahead">+' + (-diffSt) + ' seç</span>';
        }

        // IDG
        let idgCell = '<span style="color:var(--text-muted); font-size:0.75rem;">-</span>';
        if (originMeta?.idg || row.sim?.idg) {
          idgCell = '<div style="font-family:monospace; font-size:0.76rem;"><div>HMG: <strong style="color:var(--accent-purple);">' + (originMeta?.idg || '-') + '</strong></div><div>SIM: <strong style="color:var(--accent-blue);">' + (row.sim?.idg || '-') + '</strong></div></div>';
        }

        // Cache TTL
        const cacheTtlCell = '<div style="display:flex; flex-direction:column; gap:2px; font-size:0.75rem;">' +
          '<div><span class="tag-pill tag-uf" style="font-size:0.65rem;">HMG</span><span style="font-family:monospace; color:#c084fc;">' + (row.hmg?.cacheControl || '(sem header)') + '</span></div>' +
          '<div><span class="tag-pill tag-eleicao" style="font-size:0.65rem;">SIM</span><span style="font-family:monospace; color:#38bdf8; font-weight:bold;">' + (row.sim?.cacheControl || (row.sim?.maxAge != null ? 'max-age=' + row.sim.maxAge : '-')) + '</span></div>' +
        '</div>';

        tr.innerHTML = 
          '<td data-col="classificacao" style="min-width: 130px;">' +
            '<div style="margin-bottom: 4px; display: flex; gap: 4px; flex-wrap: wrap;">' +
              '<span class="tag-pill tag-pleito">Pl: ' + (meta.pleito || '-') + '</span>' +
              '<span class="tag-pill tag-eleicao">' + meta.eleicao + '</span>' +
              '<span class="tag-pill tag-uf">' + meta.uf + '</span>' +
            '</div>' +
            '<div>' +
              '<span class="tag-pill tag-tipo">' + meta.tipo + '</span>' +
              (meta.cargo !== '-' ? '<span class="tag-pill tag-cargo">' + meta.cargo + '</span>' : '') +
            '</div>' +
          '</td>' +
          '<td data-col="arquivo" style="min-width: 250px;">' +
            '<div class="code" style="font-weight:bold; color:var(--text); font-size:0.82rem; margin-bottom:4px; word-break:break-all;">' + row.relPath + '</div>' +
            '<div style="display:flex; gap:6px;">' +
              '<a href="' + row.hmgUrl + '" target="_blank" class="btn-copy" style="text-decoration:none; color:#c084fc;">↗ HMG</a>' +
              '<a href="' + row.simUrl + '" target="_blank" class="btn-copy" style="text-decoration:none; color:#38bdf8;">↗ SIM</a>' +
            '</div>' +
          '</td>' +
          '<td data-col="origin_time">' + originHg + '</td>' +
          '<td data-col="replica_time">' + simHg + '</td>' +
          '<td data-col="delay_time">' + delayTimeBadge + '</td>' +
          '<td data-col="sync_sla">' + syncSlaBadge + '</td>' +
          '<td data-col="cache_ttl">' + cacheTtlCell + '</td>' +
          '<td data-col="status">' + orderStatus + '</td>' +
          '<td data-col="origin_tot">' + originTotCell + '</td>' +
          '<td data-col="replica_tot">' + replicaTotCell + '</td>' +
          '<td data-col="delay_tot">' + delayTotCell + '</td>' +
          '<td data-col="origin_st">' + originStCell + '</td>' +
          '<td data-col="replica_st">' + replicaStCell + '</td>' +
          '<td data-col="diff_st">' + diffStCell + '</td>' +
          '<td data-col="idg">' + idgCell + '</td>';

        tbody.appendChild(tr);
      }
    }

    // =====================================================================
    // SEÇÃO 2: RENDERIZAÇÃO FORENSE SPLIT COM TRILHA DE VERSÕES E PAINEL
    // =====================================================================
    const VERSION_PALETTES = [
      { bg: 'rgba(6, 182, 212, 0.18)', border: '#0891b2', text: '#22d3ee', badgeBg: '#0891b2', name: 'Ciano' },
      { bg: 'rgba(16, 185, 129, 0.18)', border: '#059669', text: '#34d399', badgeBg: '#059669', name: 'Esmeralda' },
      { bg: 'rgba(245, 158, 11, 0.18)', border: '#d97706', text: '#fbbf24', badgeBg: '#d97706', name: 'Âmbar' },
      { bg: 'rgba(168, 85, 247, 0.18)', border: '#9333ea', text: '#c084fc', badgeBg: '#9333ea', name: 'Roxo' },
      { bg: 'rgba(249, 115, 22, 0.18)', border: '#ea580c', text: '#fb923c', badgeBg: '#ea580c', name: 'Laranja' },
      { bg: 'rgba(236, 72, 153, 0.18)', border: '#db2777', text: '#f472b6', badgeBg: '#db2777', name: 'Rosa' },
      { bg: 'rgba(59, 130, 246, 0.18)', border: '#2563eb', text: '#60a5fa', badgeBg: '#2563eb', name: 'Azul' },
      { bg: 'rgba(132, 204, 22, 0.18)', border: '#65a30d', text: '#a3e635', badgeBg: '#65a30d', name: 'Lima' },
      { bg: 'rgba(99, 102, 241, 0.18)', border: '#4f46e5', text: '#818cf8', badgeBg: '#4f46e5', name: 'Índigo' },
      { bg: 'rgba(244, 63, 94, 0.18)', border: '#e11d48', text: '#fb7185', badgeBg: '#e11d48', name: 'Rubi' },
      { bg: 'rgba(20, 184, 166, 0.18)', border: '#0d9488', text: '#2dd4bf', badgeBg: '#0d9488', name: 'Teal' },
      { bg: 'rgba(234, 179, 8, 0.18)', border: '#ca8a04', text: '#fde047', badgeBg: '#ca8a04', name: 'Dourado' }
    ];

    function getVersionKey(dg, hg, idg) {
      const d = (dg || '').trim();
      const h = (hg || '').trim();
      const i = (idg || '').trim();
      if (!d && !h && !i) return '';
      return (d + ' ' + h).trim() + (i ? ('#' + i) : '');
    }

    function escapeHtml(str) {
      if (!str) return '';
      return String(str).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
    }

    selectedDossieRegressionId = null;
    const expandedDossieCardIds = new Set();

    function toggleDossieCard(id, event) {
      if (event) event.stopPropagation();
      if (expandedDossieCardIds.has(id)) {
        expandedDossieCardIds.delete(id);
      } else {
        expandedDossieCardIds.add(id);
      }
      updateDossieCardCollapsedState(id);
      updateToggleAllDossieBtn();
    }

    function toggleAllDossieRegCards() {
      const allVisibleIds = Array.from(document.querySelectorAll('#dossieRegsListContainer .regression-card')).map(function(el) {
        return parseInt(el.getAttribute('data-regression-id'), 10);
      }).filter(Boolean);
      const allExpanded = allVisibleIds.length > 0 && allVisibleIds.every(function(id) { return expandedDossieCardIds.has(id); });
      if (allExpanded) {
        expandedDossieCardIds.clear();
      } else {
        allVisibleIds.forEach(function(id) { expandedDossieCardIds.add(id); });
      }
      allVisibleIds.forEach(function(id) { updateDossieCardCollapsedState(id); });
      updateToggleAllDossieBtn();
    }

    function updateDossieCardCollapsedState(id) {
      const bodyEl = document.getElementById('dossieCardBody_' + id);
      const chevronEl = document.getElementById('dossieChevron_' + id);
      const btnTextEl = document.getElementById('dossieBtnText_' + id);
      const cardEl = document.getElementById('dossieCard_' + id);
      const isExp = expandedDossieCardIds.has(id);

      if (bodyEl) bodyEl.style.display = isExp ? 'block' : 'none';
      if (chevronEl) chevronEl.textContent = isExp ? '▼' : '▶';
      if (btnTextEl) btnTextEl.textContent = isExp ? 'Recolher' : 'Detalhes';
      if (cardEl) {
        cardEl.style.padding = isExp ? '14px' : '9px 14px';
      }
    }

    function updateToggleAllDossieBtn() {
      const text = document.getElementById('toggleAllDossieRegsText');
      if (!text) return;
      const allVisibleIds = Array.from(document.querySelectorAll('#dossieRegsListContainer .regression-card')).map(function(el) {
        return parseInt(el.getAttribute('data-regression-id'), 10);
      }).filter(Boolean);
      const allExpanded = allVisibleIds.length > 0 && allVisibleIds.every(function(id) { return expandedDossieCardIds.has(id); });
      text.textContent = allExpanded ? 'Colapsar Todos' : 'Expandir Todos';
    }

    function renderRegsTable(rows) {
      const container = document.getElementById('dossieRegsListContainer');
      if (!container) return;

      if (!rows || rows.length === 0) {
        container.innerHTML = '<div style="text-align: center; color: var(--text-muted); padding: 40px; background:#0f172a; border-radius:8px; border:1px solid #334155;">Nenhuma ocorrência de regressão encontrada para os filtros aplicados.</div>';
        const panelContent = document.getElementById('dossieTechPanelContent');
        if (panelContent) {
          panelContent.innerHTML = '<div style="text-align:center; padding:60px 20px; color:#64748b;"><div style="font-size:2rem; margin-bottom:8px;">🔍</div><div>Nenhuma ocorrência selecionada.</div></div>';
        }
        const badge = document.getElementById('dossieTechPanelSelectedBadge');
        if (badge) badge.textContent = 'Nenhum selecionado';
        selectedDossieRegressionId = null;
        return;
      }

      let html = '';
      for (let i = 0; i < rows.length; i++) {
        const r = rows[i];
        const timeStr = r.timestamp_iso ? new Date(r.timestamp_iso).toLocaleTimeString('pt-BR') : '-';
        const elapsedText = r.timestamp_iso ? formatMinSec(Math.round((Date.now() - new Date(r.timestamp_iso).getTime()) / 1000)) + ' atrás' : '-';

        const serverRoleDesc = r.papel_servidor || (r.servidor === 'HMG' ? 'Origem / Primário' : 'Cache / Réplica');
        const serverBadgeClass = r.servidor === 'HMG' ? 'badge-purple' : 'badge-yellow';

        const critBadges = [];
        if (r.criterio) {
          if (r.criterio.includes('TEMPO') || r.criterio.includes('DG/HG')) critBadges.push('<span class="badge badge-danger">DG/HG TEMPO</span>');
          if (r.criterio.includes('TOTALIZAÇÃO')) critBadges.push('<span class="badge badge-danger">DT/HT TOTALIZAÇÃO</span>');
          if (r.criterio.includes('SEÇÕES')) critBadges.push('<span class="badge badge-danger">ST SEÇÕES</span>');
          if (r.criterio.includes('SEQUENCIAL') || r.criterio.includes('IDG')) critBadges.push('<span class="badge badge-danger">IDG SEQUENCIAL</span>');
        }

        const prevTotStr = (r.dt_anterior ? r.dt_anterior + ' ' + (r.ht_anterior || '') : '-');
        const currTotStr = (r.dt_recebido ? r.dt_recebido + ' ' + (r.ht_recebido || '') : '-');
        const isTotRegression = Boolean(r.dt_recebido && r.dt_anterior && (r.dt_recebido < r.dt_anterior || (r.dt_recebido === r.dt_anterior && r.ht_recebido < r.ht_anterior)));

        const prevStStr = (r.secoes_anterior !== null && r.secoes_anterior !== undefined ? r.secoes_anterior + ' seç' : '-');
        const currStStr = (r.secoes_recebido !== null && r.secoes_recebido !== undefined ? r.secoes_recebido + ' seç' : '-');
        const isStRegression = Boolean(r.secoes_recebido !== null && r.secoes_anterior !== null && Number(r.secoes_recebido) < Number(r.secoes_anterior));

        const dgAdvOrSame = Boolean(r.dg_anterior && r.dg_recebido && r.hg_anterior && r.hg_recebido && (
          r.dg_recebido > r.dg_anterior || (r.dg_recebido === r.dg_anterior && r.hg_recebido >= r.hg_anterior)
        ));
        const isInversion = Boolean((r.criterio && r.criterio.includes('INVERSÃO')) || (r.motivo && r.motivo.includes('INVERSÃO')) || (r.detalhes && r.detalhes.includes('INVERSÃO')) || (dgAdvOrSame && (isTotRegression || isStRegression)));

        if (isInversion) {
          if (isTotRegression && isStRegression) {
            critBadges.push('<span class="badge" style="background:rgba(244,63,94,0.3); color:#fda4af; border:1px solid #f43f5e; font-weight:800;" title="Arquivo mais novo em DG/HG, porém DT/HT e ST retrocederam!">🚨 INVERSÃO: DG ↗ | DT/ST ↘</span>');
          } else if (isTotRegression) {
            critBadges.push('<span class="badge" style="background:rgba(244,63,94,0.3); color:#fda4af; border:1px solid #f43f5e; font-weight:800;" title="Arquivo mais novo em DG/HG, porém Totalização (DT/HT) retrocedeu!">🚨 INVERSÃO: DG ↗ | DT ↘</span>');
          } else if (isStRegression) {
            critBadges.push('<span class="badge" style="background:rgba(244,63,94,0.3); color:#fda4af; border:1px solid #f43f5e; font-weight:800;" title="Arquivo mais novo em DG/HG, porém Seções Apuradas (ST) diminuíram!">🚨 INVERSÃO: DG ↗ | ST ↘</span>');
          }
        }

        if (critBadges.length === 0) critBadges.push('<span class="badge badge-danger">REGRESSÃO FORENSE</span>');
        const critBadgesHtml = critBadges.join(' ');

        const uf = (r.fileMeta && r.fileMeta.uf) ? r.fileMeta.uf : '-';
        const cargo = (r.fileMeta && r.fileMeta.cargo) ? r.fileMeta.cargo : '-';
        const tipo = (r.fileMeta && r.fileMeta.tipo) ? r.fileMeta.tipo : '-';
        const eleicao = (r.fileMeta && r.fileMeta.eleicao) ? r.fileMeta.eleicao : '-';
        const motivoTexto = escapeHtml(r.motivo || r.detalhes || '');

        // Construção do Esquema Cronológico (Linha do Tempo de Requisições)
        const timelineList = r.timeline || [];
        const versionMap = new Map();
        let verCounter = 1;

        for (let tIdx = 0; tIdx < timelineList.length; tIdx++) {
          const step = timelineList[tIdx];
          const vKey = getVersionKey(step.dg, step.hg, step.idg);
          if (vKey && !versionMap.has(vKey)) {
            const paletteIndex = (verCounter - 1) % VERSION_PALETTES.length;
            versionMap.set(vKey, {
              index: verCounter,
              label: 'V' + verCounter,
              palette: VERSION_PALETTES[paletteIndex],
              dg: step.dg || '',
              hg: step.hg || '',
              idg: step.idg || '',
              count: 0
            });
            verCounter++;
          }
          if (vKey && versionMap.has(vKey)) {
            versionMap.get(vKey).count++;
          }
        }

        const prevVKey = getVersionKey(r.dg_anterior, r.hg_anterior, r.idg_anterior);
        const currVKey = getVersionKey(r.dg_recebido, r.hg_recebido, r.idg_recebido);

        if (prevVKey && !versionMap.has(prevVKey)) {
          const paletteIndex = (verCounter - 1) % VERSION_PALETTES.length;
          versionMap.set(prevVKey, {
            index: verCounter,
            label: 'V' + verCounter,
            palette: VERSION_PALETTES[paletteIndex],
            dg: r.dg_anterior || '',
            hg: r.hg_anterior || '',
            idg: r.idg_anterior || '',
            count: 0
          });
          verCounter++;
        }

        if (currVKey && !versionMap.has(currVKey)) {
          const paletteIndex = (verCounter - 1) % VERSION_PALETTES.length;
          versionMap.set(currVKey, {
            index: verCounter,
            label: 'V' + verCounter,
            palette: VERSION_PALETTES[paletteIndex],
            dg: r.dg_recebido || '',
            hg: r.hg_recebido || '',
            idg: r.idg_recebido || '',
            count: 0
          });
          verCounter++;
        }

        let timelineHtml = '';
        if (timelineList.length > 0) {
          let versionLegendHtml = '';
          if (versionMap.size > 0) {
            versionLegendHtml += '<div style="display:flex; align-items:center; gap:6px; flex-wrap:wrap; margin-top:8px; padding-top:8px; border-top:1px dashed rgba(255,255,255,0.08); font-size:0.73rem;">' +
              '<span style="color:#94a3b8; font-weight:700; display:inline-flex; align-items:center; gap:4px;"><span>🏷️</span> Trilha Visual de Versões:</span>';
            
            versionMap.forEach(function(v) {
              const vPal = v.palette;
              const vDgHg = (v.hg || v.dg) ? ((v.dg ? v.dg.substring(0, 5) + ' ' : '') + v.hg) : '-';
              const vIdg = v.idg ? (' • IDG ' + v.idg) : '';
              versionLegendHtml += '<span style="display:inline-flex; align-items:center; gap:5px; background:' + vPal.bg + '; border:1px solid ' + vPal.border + '; color:' + vPal.text + '; padding:2px 8px; border-radius:5px; font-family:monospace; font-weight:700;">' +
                '<span style="background:' + vPal.badgeBg + '; color:#fff; font-size:0.65rem; padding:1px 5px; border-radius:3px; font-weight:800;">' + v.label + '</span>' +
                '<span>' + vDgHg + vIdg + '</span>' +
                '<span style="opacity:0.65; font-size:0.68rem;">(' + v.count + 'x)</span>' +
              '</span>';
            });

            versionLegendHtml += '</div>';
          }

          timelineHtml += '<div class="timeline-box" style="margin-bottom:12px; background:#0b132b; border:1px solid #334155; border-radius:8px; padding:12px 14px;">' +
            '<div style="margin-bottom:10px; border-bottom:1px solid rgba(255,255,255,0.08); padding-bottom:8px;">' +
              '<div style="display:flex; justify-content:space-between; align-items:center; flex-wrap:wrap; gap:6px;">' +
                '<strong style="font-size:0.84rem; color:#38bdf8; display:flex; align-items:center; gap:6px;">' +
                  '<span>⏱️</span> Esquema Cronológico de Eventos (Linha do Tempo das Requisições):' +
                '</strong>' +
                '<span style="font-size:0.72rem; color:#94a3b8;">' + timelineList.length + ' leituras no período</span>' +
              '</div>' +
              versionLegendHtml +
            '</div>' +
            '<div style="display:flex; flex-direction:column; gap:8px;">';

          for (let tIdx = 0; tIdx < timelineList.length; tIdx++) {
            const step = timelineList[tIdx];
            const stepTime = new Date(step.call_time_iso || step.timestamp_iso).toLocaleTimeString('pt-BR');
            const latencyBadge = (step.latency_ms !== null && step.latency_ms !== undefined) 
              ? ('<span style="color:#94a3b8; font-size:0.68rem; font-family:monospace;" title="Latência de ida e volta da requisição: ' + step.latency_ms + 'ms">(' + step.latency_ms + 'ms)</span>')
              : '';
            const isReg = step.isRegressionPoint;
            const isOrigin = step.servidor === 'HMG';
            
            const itemClass = isReg ? 'timeline-step-reg' : (isOrigin ? 'timeline-step-origin' : 'timeline-step-normal');
            const itemBg = isReg 
              ? 'background:rgba(239,68,68,0.14); border:1px solid #ef4444;' 
              : (isOrigin ? 'background:rgba(168,85,247,0.08); border:1px solid rgba(168,85,247,0.3);' : 'background:rgba(15,23,42,0.6); border:1px solid #1e293b;');
            
            const badgeServidor = isOrigin ? 'tag-hmg-title' : 'tag-sim-title';
            const statusLabel = isReg 
              ? '<span style="background:#dc2626; color:#fff; font-weight:700; padding:2px 8px; border-radius:4px; font-size:0.72rem; animation:pulse 1s infinite;">🚨 DETECÇÃO DE REVERSÃO!</span>'
              : (isOrigin ? '<span style="color:#c084fc; font-weight:600; font-size:0.72rem;">🟣 Origem Primária</span>' : '<span style="color:#10b981; font-weight:600; font-size:0.72rem;">✓ Leitura Normal</span>');

            const stepDgHg = (step.dg || '-') + ' ' + (step.hg || '-');
            const stepIdg = step.idg ? 'IDG: ' + step.idg : '';
            const stepSt = (step.secoes !== null && step.secoes !== undefined) ? 'ST: ' + step.secoes + ' seç' : '';
            const stepTot = (step.dt && step.ht) ? 'Tot: ' + step.dt + ' ' + step.ht : '';

            const stepVKey = getVersionKey(step.dg, step.hg, step.idg);
            const verInfo = versionMap.get(stepVKey);
            const pal = verInfo ? verInfo.palette : { bg: 'rgba(255,255,255,0.05)', border: '#475569', text: '#cbd5e1', badgeBg: '#475569' };
            const verBadge = verInfo ? ('<span style="background:' + pal.badgeBg + '; color:#fff; font-size:0.65rem; padding:1px 5px; border-radius:3px; font-weight:800; font-family:monospace;">' + verInfo.label + '</span>') : '';

            const chipDgHg = '<span style="display:inline-flex; align-items:center; gap:5px; background:' + pal.bg + '; border:1px solid ' + pal.border + '; color:' + pal.text + '; padding:2px 8px; border-radius:5px; font-family:monospace; font-size:0.74rem; font-weight:700; box-shadow:0 1px 2px rgba(0,0,0,0.2);">' +
              verBadge +
              '<span>DG/HG: ' + escapeHtml(stepDgHg) + '</span>' +
            '</span>';

            const chipIdg = step.idg ? ('<span style="display:inline-flex; align-items:center; background:' + pal.bg + '; border:1px solid ' + pal.border + '; color:' + pal.text + '; padding:2px 7px; border-radius:5px; font-family:monospace; font-size:0.72rem; font-weight:700; box-shadow:0 1px 2px rgba(0,0,0,0.2);">' +
              escapeHtml(stepIdg) +
            '</span>') : '';

            timelineHtml += '<div class="' + itemClass + '" style="' + itemBg + ' border-radius:6px; padding:8px 12px; font-size:0.78rem;">' +
              '<div style="display:flex; justify-content:space-between; align-items:center; flex-wrap:wrap; gap:6px; margin-bottom:4px;">' +
                '<div style="display:flex; align-items:center; gap:8px; flex-wrap:wrap;">' +
                  '<strong style="font-family:monospace; color:#f8fafc; font-size:0.82rem;" title="Instante de envio da requisição (Disparo)">• ' + stepTime + '</strong>' +
                  latencyBadge +
                  '<span class="' + badgeServidor + '" style="font-size:0.70rem; padding:1px 6px; border-radius:3px;">' + step.servidor + '</span>' +
                  chipDgHg +
                  (chipIdg ? chipIdg : '') +
                  (stepSt ? '<span style="font-family:monospace; color:#34d399; font-size:0.72rem;">' + stepSt + '</span>' : '') +
                  (stepTot ? '<span style="font-family:monospace; color:#fbbf24; font-size:0.72rem;">' + stepTot + '</span>' : '') +
                '</div>' +
                '<div>' + statusLabel + '</div>' +
              '</div>' +

              '<div style="display:flex; align-items:center; gap:12px; flex-wrap:wrap; font-size:0.72rem; color:#94a3b8; font-family:monospace; border-top:1px solid rgba(255,255,255,0.05); padding-top:4px; margin-top:4px;">' +
                '<span>⏱️ Chamada: <strong style="color:#e2e8f0;">' + ((step.call_time_iso ? step.call_time_iso.slice(11, 19) : stepTime)) + '</strong></span>' +
                '<span>🌐 IP Borda: <strong style="color:#38bdf8;">' + (step.server_ip || '-') + '</strong></span>' +
                '<span>⚡ Cache-Control: <strong style="color:#f8fafc;">' + (step.cache_control || '-') + '</strong></span>' +
                '<span>📦 CDN Cache: <strong style="color:#34d399;">' + (step.cdn_status || '-') + '</strong></span>' +
                '<span>🏷️ ETag: <span style="color:#cbd5e1;">' + (step.etag || '-') + '</span></span>' +
                (step.akamai_grn && step.akamai_grn !== '-' ? '<span>🆔 GRN: <strong style="color:#c084fc;" title="Akamai Global Request Number">' + escapeHtml(step.akamai_grn) + '</strong></span>' : '') +
                (step.age && step.age !== '-' ? '<span>⏳ Age: ' + step.age + '</span>' : '') +
              '</div>' +
            '</div>';
          }

          timelineHtml += '</div></div>';
        }

        const caseGrn = r.akamai_grn || (r.rawMeta && r.rawMeta.headers && (r.rawMeta.headers['akamai-grn'] || r.rawMeta.headers['x-akamai-grn'])) || null;
        const caseGrnBadge = caseGrn ? ('<span style="font-family:monospace; font-size:0.72rem; background:rgba(168,85,247,0.15); color:#c084fc; border:1px solid rgba(168,85,247,0.3); padding:2px 8px; border-radius:4px;" title="Akamai Global Request Number (GRN)">🆔 GRN: ' + escapeHtml(caseGrn) + '</span> ') : '';

        const prevVerInfo = versionMap.get(prevVKey);
        const prevPal = prevVerInfo ? prevVerInfo.palette : null;
        const prevVerBadge = prevVerInfo ? ('<span style="background:' + prevPal.badgeBg + '; color:#fff; font-size:0.62rem; padding:1px 5px; border-radius:3px; font-weight:800; font-family:monospace; margin-right:4px;">' + prevVerInfo.label + '</span>') : '';
        const prevDgHgHtml = prevPal ? (
          '<span style="display:inline-flex; align-items:center; background:' + prevPal.bg + '; border:1px solid ' + prevPal.border + '; color:' + prevPal.text + '; padding:2px 8px; border-radius:4px; font-weight:700;">' +
            prevVerBadge + (r.dg_anterior || '-') + ' ' + (r.hg_anterior || '-') +
          '</span>'
        ) : ('<strong style="color:#f8fafc;">' + (r.dg_anterior || '-') + ' ' + (r.hg_anterior || '-') + '</strong>');

        const prevIdgHtml = (r.idg_anterior && prevPal) ? (
          '<span style="background:' + prevPal.bg + '; border:1px solid ' + prevPal.border + '; color:' + prevPal.text + '; padding:1px 6px; border-radius:4px; font-weight:700;">' +
            r.idg_anterior +
          '</span>'
        ) : (r.idg_anterior || '-');

        const currVerInfo = versionMap.get(currVKey);
        const currPal = currVerInfo ? currVerInfo.palette : null;
        const currVerBadge = currVerInfo ? ('<span style="background:' + currPal.badgeBg + '; color:#fff; font-size:0.62rem; padding:1px 5px; border-radius:3px; font-weight:800; font-family:monospace; margin-right:4px;">' + currVerInfo.label + '</span>') : '';
        const currDgHgHtml = currPal ? (
          '<span style="display:inline-flex; align-items:center; background:' + currPal.bg + '; border:1px solid ' + currPal.border + '; color:' + currPal.text + '; padding:2px 8px; border-radius:4px; font-weight:700;">' +
            currVerBadge + (r.dg_recebido || '-') + ' ' + (r.hg_recebido || '-') +
          '</span>'
        ) : ('<strong style="color:#ef4444;">' + (r.dg_recebido || '-') + ' ' + (r.hg_recebido || '-') + '</strong>');

        const currIdgHtml = (r.idg_recebido && currPal) ? (
          '<span style="background:' + currPal.bg + '; border:1px solid ' + currPal.border + '; color:' + currPal.text + '; padding:1px 6px; border-radius:4px; font-weight:700;">' +
            r.idg_recebido +
          '</span>'
        ) : (r.idg_recebido || '-');

        const isExpanded = expandedDossieCardIds.has(r.id);

        html += '<div id="dossieCard_' + r.id + '" class="regression-card" data-regression-id="' + r.id + '" onclick="selectDossieRegression(' + r.id + ')" style="background:#0f172a; border:1px solid rgba(239,68,68,0.35); border-left:4px solid #ef4444; border-radius:8px; padding:' + (isExpanded ? '14px' : '9px 14px') + '; box-shadow:0 4px 12px rgba(0,0,0,0.25); cursor:pointer; transition:all 0.15s ease;">' +
          '<div style="display:flex; justify-content:space-between; align-items:center; flex-wrap:wrap; gap:8px;">' +
            '<div style="display:flex; align-items:center; gap:8px; flex-wrap:wrap;">' +
              '<button type="button" onclick="toggleDossieCard(' + r.id + ', event)" class="btn-copy" style="padding:2px 8px; font-size:0.75rem; background:#1e293b; border:1px solid #475569; color:#38bdf8; display:flex; align-items:center; gap:4px; cursor:pointer;" title="Expandir/Recolher ocorrência">' +
                '<span id="dossieChevron_' + r.id + '">' + (isExpanded ? '▼' : '▶') + '</span>' +
                '<span id="dossieBtnText_' + r.id + '" style="font-size:0.70rem; font-weight:700;">' + (isExpanded ? 'Recolher' : 'Detalhes') + '</span>' +
              '</button>' +
              '<span style="font-family:monospace; font-weight:700; font-size:0.82rem; background:rgba(239,68,68,0.2); color:#fca5a5; padding:2px 8px; border-radius:4px; border:1px solid rgba(239,68,68,0.4);">#' + r.id + '</span>' +
              '<span style="font-size:0.80rem; color:#94a3b8; font-family:monospace;">⏱️ ' + timeStr + ' (' + elapsedText + ')</span>' +
              '<span class="' + serverBadgeClass + '" style="font-size:0.75rem; padding:2px 8px; border-radius:4px;">' + r.servidor + ' (' + serverRoleDesc + ')</span>' +
              critBadgesHtml +
              caseGrnBadge +
            '</div>' +
            '<div style="display:flex; align-items:center; gap:6px;">' +
              (!window.STANDALONE_DOSSIER_DATA ? (
                '<a href="/api/evidencia?id=' + r.id + '" target="_blank" onclick="event.stopPropagation();" class="btn-copy" style="font-size:0.72rem; padding:3px 8px; text-decoration:none;" title="Ver payload JSON raw">🔍 Ver JSON</a>' +
                '<a href="/api/evidencia?id=' + r.id + '&download=1" target="_blank" onclick="event.stopPropagation();" class="btn-copy" style="font-size:0.72rem; padding:3px 8px; text-decoration:none;" title="Baixar JSON da evidência">⬇️ Baixar JSON</a>'
              ) : '') +
              '<button type="button" onclick="event.stopPropagation(); selectDossieRegression(' + r.id + ');" class="btn-copy" style="font-size:0.72rem; padding:3px 10px; background:#1e293b; border:1px solid #38bdf8; color:#38bdf8;" id="btnDossieInspect_' + r.id + '">🌐 Inspecionar Painel 👉</button>' +
            '</div>' +
          '</div>' +

          '<div style="display:flex; justify-content:space-between; align-items:center; gap:8px; margin-top:6px; flex-wrap:wrap; font-size:0.80rem;">' +
            '<div style="display:flex; align-items:center; gap:8px; flex-wrap:wrap;">' +
              '<span style="color:#38bdf8; font-weight:700; font-family:monospace; word-break:break-all;">' + r.arquivo + '</span>' +
              '<span style="background:rgba(255,255,255,0.06); padding:2px 6px; border-radius:4px; font-size:0.72rem; color:#cbd5e1;">UF: <strong>' + uf + '</strong></span>' +
              '<span style="background:rgba(255,255,255,0.06); padding:2px 6px; border-radius:4px; font-size:0.72rem; color:#cbd5e1;">Cargo: <strong>' + cargo + '</strong></span>' +
              '<span style="background:rgba(255,255,255,0.06); padding:2px 6px; border-radius:4px; font-size:0.72rem; color:#cbd5e1;">Eleição: <strong>' + eleicao + '</strong></span>' +
            '</div>' +
            '<div style="display:flex; align-items:center; gap:6px; font-family:monospace; font-size:0.74rem; flex-wrap:wrap;">' +
              '<span style="color:#94a3b8;">DG/HG:</span> ' +
              '<span style="color:#10b981;">' + (r.dg_anterior || '-') + ' ' + (r.hg_anterior || '-') + '</span>' +
              '<span style="' + (dgAdvOrSame ? 'color:#10b981;' : 'color:#ef4444; font-weight:bold;') + '">➔ ' + (r.dg_recebido || '-') + ' ' + (r.hg_recebido || '-') + '</span>' +
              (isTotRegression ? (' <span style="background:rgba(249,115,22,0.15); border:1px solid rgba(249,115,22,0.3); padding:1px 5px; border-radius:3px; color:#fb923c;"><span style="color:#94a3b8;">DT:</span> ' + (r.dt_anterior ? r.dt_anterior.substring(0, 5) + ' ' + (r.ht_anterior || '') : '-') + ' ➔ <strong style="color:#ef4444;">' + (r.dt_recebido ? r.dt_recebido.substring(0, 5) + ' ' + (r.ht_recebido || '') : '-') + ' ↘</strong></span>') : '') +
              (isStRegression ? (' <span style="background:rgba(236,72,153,0.15); border:1px solid rgba(236,72,153,0.3); padding:1px 5px; border-radius:3px; color:#f472b6;"><span style="color:#94a3b8;">ST:</span> ' + (r.secoes_anterior !== null && r.secoes_anterior !== undefined ? r.secoes_anterior : '-') + ' ➔ <strong style="color:#ef4444;">' + (r.secoes_recebido !== null && r.secoes_recebido !== undefined ? r.secoes_recebido : '-') + ' ↘</strong></span>') : '') +
              (r.idg_anterior ? ('<span style="color:#94a3b8; margin-left:4px;">(IDG: ' + r.idg_anterior + ' ➔ <strong style="color:#fca5a5;">' + (r.idg_recebido || '-') + '</strong>)</span>') : '') +
            '</div>' +
          '</div>' +

          '<div id="dossieCardBody_' + r.id + '" class="card-collapsible-body" style="display:' + (isExpanded ? 'block' : 'none') + '; margin-top:10px; border-top:1px dashed rgba(255,255,255,0.1); padding-top:10px;">' +
            timelineHtml +

            '<div style="display:grid; grid-template-columns:repeat(auto-fit, minmax(240px, 1fr)); gap:12px; background:#1e293b; border:1px solid #334155; border-radius:8px; padding:10px 14px; margin-bottom:8px; font-size:0.80rem;">' +
              '<div>' +
                '<div style="font-size:0.72rem; text-transform:uppercase; color:#10b981; font-weight:700; margin-bottom:6px; display:flex; align-items:center; gap:4px;">' +
                  '<span>✓</span> Versão Anterior Mais Recente:' +
                '</div>' +
                '<div style="display:grid; grid-template-columns:120px 1fr; gap:4px 8px; font-family:monospace; align-items:center;">' +
                  '<span style="color:#94a3b8;">Geração (DG/HG):</span>' +
                  '<div>' + prevDgHgHtml + '</div>' +
                  '<span style="color:#94a3b8;">Totalização:</span>' +
                  '<span style="color:#f8fafc;">' + prevTotStr + '</span>' +
                  '<span style="color:#94a3b8;">Seções Apuradas:</span>' +
                  '<span style="color:#f8fafc;">' + prevStStr + '</span>' +
                  '<span style="color:#94a3b8;">IDG (Sequencial):</span>' +
                  '<div>' + prevIdgHtml + '</div>' +
                '</div>' +
              '</div>' +

              '<div>' +
                '<div style="font-size:0.72rem; text-transform:uppercase; color:#ef4444; font-weight:700; margin-bottom:6px; display:flex; align-items:center; gap:4px;">' +
                  '<span>🚨</span> Versão Recebida (Retrocesso):' +
                '</div>' +
                '<div style="display:grid; grid-template-columns:120px 1fr; gap:4px 8px; font-family:monospace; align-items:center;">' +
                  '<span style="color:#94a3b8;">Geração (DG/HG):</span>' +
                  '<div>' + currDgHgHtml + '</div>' +
                  '<span style="color:#94a3b8;">Totalização:</span>' +
                  '<span style="' + (isTotRegression ? 'color:#ef4444; font-weight:bold;' : 'color:#f8fafc;') + '">' + currTotStr + '</span>' +
                  '<span style="color:#94a3b8;">Seções Apuradas:</span>' +
                  '<span style="' + (isStRegression ? 'color:#ef4444; font-weight:bold;' : 'color:#f8fafc;') + '">' + currStStr + '</span>' +
                  '<span style="color:#94a3b8;">IDG (Sequencial):</span>' +
                  '<div>' + currIdgHtml + '</div>' +
                '</div>' +
              '</div>' +
            '</div>' +

            '<div style="font-size:0.78rem; color:#fca5a5; background:rgba(239,68,68,0.12); border:1px solid rgba(239,68,68,0.25); border-radius:6px; padding:8px 12px;">' +
              '<strong>⚠️ Diagnóstico:</strong> ' + motivoTexto +
            '</div>' +
          '</div>' +
        '</div>';
      }

      container.innerHTML = html;
      updateToggleAllDossieBtn();

      // Auto-seleciona a ocorrência corrente ou o primeiro item da lista
      if (rows.length > 0) {
        const exists = selectedDossieRegressionId && rows.some(item => item.id === selectedDossieRegressionId);
        const targetId = exists ? selectedDossieRegressionId : rows[0].id;
        selectDossieRegression(targetId);
      }
    }

    function selectDossieRegression(id) {
      selectedDossieRegressionId = id;
      const r = rawRegressionsList.find(item => item.id === id);
      if (!r) return;

      const isLight = document.documentElement.getAttribute('data-theme') === 'light';
      const allCards = document.querySelectorAll('#dossieRegsListContainer .regression-card');
      allCards.forEach(function(card) {
        card.style.borderColor = 'rgba(239,68,68,0.35)';
        card.style.background = isLight ? '#ffffff' : '#0f172a';
        card.style.boxShadow = isLight ? '0 2px 8px rgba(0,0,0,0.06)' : '0 4px 12px rgba(0,0,0,0.25)';
      });

      const selectedCard = document.getElementById('dossieCard_' + id);
      if (selectedCard) {
        selectedCard.style.borderColor = '#0284c7';
        selectedCard.style.background = isLight ? '#f0f9ff' : '#132338';
        selectedCard.style.boxShadow = isLight ? '0 0 16px rgba(2,132,199,0.25)' : '0 0 16px rgba(56,189,248,0.25)';
      }

      const allInspectBtns = document.querySelectorAll('[id^="btnDossieInspect_"]');
      allInspectBtns.forEach(function(btn) {
        btn.textContent = '🌐 Inspecionar Painel 👉';
        btn.style.background = '#1e293b';
        btn.style.borderColor = '#38bdf8';
        btn.style.color = '#38bdf8';
      });
      const currentInspectBtn = document.getElementById('btnDossieInspect_' + id);
      if (currentInspectBtn) {
        currentInspectBtn.textContent = '🔍 INSPECIONANDO ATIVO';
        currentInspectBtn.style.background = '#0284c7';
        currentInspectBtn.style.borderColor = '#38bdf8';
        currentInspectBtn.style.color = '#ffffff';
      }

      const badge = document.getElementById('dossieTechPanelSelectedBadge');
      if (badge) {
        badge.innerHTML = '<span style="color:#fca5a5; font-weight:bold;">#' + r.id + '</span> | ' + escapeHtml(r.arquivo);
      }

      renderDossieTechDetails(r);
    }

    function renderDossieTechDetails(r) {
      const panelContent = document.getElementById('dossieTechPanelContent');
      if (!panelContent) return;

      const rawHeaders = (r.rawMeta && (r.rawMeta.response_headers || r.rawMeta.headers)) || r.response_headers || {};
      const rawReqHeaders = (r.rawMeta && r.rawMeta.request_headers) || r.request_headers || {};
      const serverIp = rawHeaders['x-server-ip'] || (r.rawMeta && r.rawMeta.serverIp) || r.server_ip || '-';
      const cdnCache = rawHeaders['cdn-cache-status'] || rawHeaders['x-cache'] || '-';
      const cacheControl = rawHeaders['cache-control'] || '-';
      const expires = rawHeaders['expires'] || '-';
      const age = rawHeaders['age'] !== undefined ? (rawHeaders['age'] + 's') : '-';
      const etag = rawHeaders['etag'] || '-';
      const lastModified = rawHeaders['last-modified'] || '-';
      const dateHttp = rawHeaders['date'] || '-';
      const webServer = rawHeaders['server'] || (r.servidor === 'SIM' ? 'Akamai CDN' : 'Apache Origin');
      const originUrl = (r.rawMeta && r.rawMeta.url_origem) || '-';
      const reqCacheControl = rawReqHeaders['cache-control'] || rawReqHeaders['Cache-Control'] || '-';
      const reqPragma = rawReqHeaders['pragma'] || rawReqHeaders['Pragma'] || '-';
      const caseGrn = r.akamai_grn || rawHeaders['akamai-grn'] || rawHeaders['x-akamai-grn'] || null;
      const rawPath = r.evidencia_raw_path ? r.evidencia_raw_path : '(salvo no buffer SQLite)';

      let reqHeadersRowsHtml = '';
      const reqEntries = Object.entries(rawReqHeaders);
      if (reqEntries.length > 0) {
        for (let j = 0; j < reqEntries.length; j++) {
          const k = reqEntries[j][0];
          const v = reqEntries[j][1];
          const lk = k.toLowerCase();
          const isCacheHdr = ['cache-control', 'pragma', 'if-modified-since', 'if-none-match'].includes(lk);
          reqHeadersRowsHtml += '<tr style="border-bottom:1px solid rgba(255,255,255,0.05);">' +
            '<td style="padding:5px 8px; color:' + (isCacheHdr ? '#38bdf8; font-weight:700;' : '#94a3b8;') + '; font-family:monospace; width:200px;">' +
              (isCacheHdr ? '<span style="background:rgba(56,189,248,0.15); color:#38bdf8; border:1px solid rgba(56,189,248,0.3); padding:1px 4px; border-radius:3px; font-size:0.62rem; margin-right:4px; font-weight:bold;">CACHE</span>' : '') +
              escapeHtml(k) +
            '</td>' +
            '<td style="padding:5px 8px; color:' + (isCacheHdr ? '#f8fafc; font-weight:600;' : '#cbd5e1;') + '; font-family:monospace; word-break:break-all;">' +
              escapeHtml(String(v)) +
            '</td>' +
          '</tr>';
        }
      } else {
        reqHeadersRowsHtml = '<tr><td colspan="2" style="padding:6px 8px; color:#64748b; font-style:italic;">Cabeçalhos padrão registrados.</td></tr>';
      }

      let respHeadersRowsHtml = '';
      const respEntries = Object.entries(rawHeaders);
      if (respEntries.length > 0) {
        for (let j = 0; j < respEntries.length; j++) {
          const k = respEntries[j][0];
          const v = respEntries[j][1];
          const lk = k.toLowerCase();
          const isCacheHdr = ['cache-control', 'pragma', 'expires', 'age', 'etag', 'last-modified', 'date', 'vary'].includes(lk);
          const isCdnHdr = ['akamai-grn', 'x-akamai-grn', 'cdn-cache-status', 'x-cache', 'x-cache-lookup', 'x-cache-hits', 'x-check-cacheable', 'x-true-cache-key', 'x-cache-key', 'server-timing'].includes(lk);
          const isIpOrServer = ['x-server-ip', 'server'].includes(lk);

          let tagBadge = '';
          let valColor = '#cbd5e1';
          let keyColor = '#94a3b8';

          if (isCacheHdr) {
            tagBadge = '<span style="background:rgba(16,185,129,0.15); color:#34d399; border:1px solid rgba(16,185,129,0.3); padding:1px 4px; border-radius:3px; font-size:0.62rem; margin-right:4px; font-weight:bold;">CACHE</span>';
            keyColor = '#34d399';
            valColor = '#f8fafc; font-weight:bold';
          } else if (isCdnHdr) {
            tagBadge = '<span style="background:rgba(168,85,247,0.15); color:#c084fc; border:1px solid rgba(168,85,247,0.3); padding:1px 4px; border-radius:3px; font-size:0.62rem; margin-right:4px; font-weight:bold;">CDN</span>';
            keyColor = '#c084fc';
            valColor = '#f8fafc; font-weight:bold';
          } else if (isIpOrServer) {
            tagBadge = '<span style="background:rgba(56,189,248,0.15); color:#38bdf8; border:1px solid rgba(56,189,248,0.3); padding:1px 4px; border-radius:3px; font-size:0.62rem; margin-right:4px; font-weight:bold;">REDE</span>';
            keyColor = '#38bdf8';
            valColor = '#f8fafc';
          }

          respHeadersRowsHtml += '<tr style="border-bottom:1px solid rgba(255,255,255,0.05);">' +
            '<td style="padding:5px 8px; color:' + keyColor + '; font-family:monospace; width:200px;">' +
              tagBadge + escapeHtml(k) +
            '</td>' +
            '<td style="padding:5px 8px; color:' + valColor + '; font-family:monospace; word-break:break-all;">' +
              escapeHtml(String(v)) +
            '</td>' +
          '</tr>';
        }
      } else {
        respHeadersRowsHtml = '<tr><td colspan="2" style="padding:6px 8px; color:#64748b; font-style:italic;">Nenhum cabeçalho de resposta registrado.</td></tr>';
      }

      panelContent.innerHTML = 
        '<div style="background:#1e293b; border:1px solid #334155; border-radius:8px; padding:10px 12px; margin-bottom:12px;">' +
          '<div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:4px;">' +
            '<span style="font-weight:700; color:#38bdf8; font-size:0.85rem;">Caso Forense #' + r.id + '</span>' +
            '<span style="font-size:0.75rem; color:#94a3b8; font-family:monospace;">' + (r.servidor || '') + ' (' + (r.papel_servidor || (r.servidor === 'HMG' ? 'Fonte Oficial' : 'Cache Akamai')) + ')</span>' +
          '</div>' +
          '<div style="font-family:monospace; color:#f8fafc; font-size:0.78rem; word-break:break-all;">' + escapeHtml(r.arquivo) + '</div>' +
        '</div>' +

        '<div style="margin-bottom:14px;">' +
          '<div style="font-size:0.72rem; text-transform:uppercase; color:#94a3b8; font-weight:700; margin-bottom:6px; display:flex; align-items:center; gap:4px;">' +
            '<span>📍</span> Metadados de Rede e Conexão:' +
          '</div>' +
          '<table style="width:100%; border-collapse:collapse; font-size:0.74rem; background:#1e293b; border-radius:6px; overflow:hidden; border:1px solid #334155;">' +
            '<tbody>' +
              '<tr style="border-bottom:1px solid #334155;">' +
                '<td style="color:#94a3b8; padding:5px 8px; width:190px;">Instância / IP Borda (x-server-ip)</td>' +
                '<td style="color:#38bdf8; font-weight:bold; font-family:monospace; padding:5px 8px;">' + serverIp + '</td>' +
              '</tr>' +
              '<tr style="border-bottom:1px solid #334155;">' +
                '<td style="color:#94a3b8; padding:5px 8px;">Akamai-GRN</td>' +
                '<td style="color:#c084fc; font-weight:bold; font-family:monospace; padding:5px 8px; word-break:break-all;">' + (caseGrn || '-') + '</td>' +
              '</tr>' +
              '<tr style="border-bottom:1px solid #334155;">' +
                '<td style="color:#94a3b8; padding:5px 8px;">Instante de Disparo (T_call)</td>' +
                '<td style="color:#e2e8f0; font-family:monospace; padding:5px 8px;">' + (r.call_time_iso || r.timestamp_iso) + '</td>' +
              '</tr>' +
              '<tr style="border-bottom:1px solid #334155;">' +
                '<td style="color:#94a3b8; padding:5px 8px;">Latência de Rede (RTT)</td>' +
                '<td style="color:#34d399; font-family:monospace; padding:5px 8px;">' + (r.latency_ms !== null && r.latency_ms !== undefined ? (r.latency_ms + ' ms') : '-') + '</td>' +
              '</tr>' +
              '<tr style="border-bottom:1px solid #334155;">' +
                '<td style="color:#94a3b8; padding:5px 8px;">Camada Web (Server)</td>' +
                '<td style="color:#94a3b8; font-family:monospace; padding:5px 8px;">' + webServer + '</td>' +
              '</tr>' +
              '<tr style="border-bottom:1px solid #334155;">' +
                '<td style="color:#94a3b8; padding:5px 8px;">URL da Requisição</td>' +
                '<td style="color:#38bdf8; font-family:monospace; padding:5px 8px; word-break:break-all;">' + (originUrl !== '-' ? ('<a href="' + originUrl + '" target="_blank" style="color:#38bdf8;">' + originUrl + '</a>') : '-') + '</td>' +
              '</tr>' +
              '<tr>' +
                '<td style="color:#94a3b8; padding:5px 8px;">Arquivo Raw Gravado</td>' +
                '<td style="color:#64748b; font-family:monospace; padding:5px 8px; word-break:break-all;">' + rawPath + '</td>' +
              '</tr>' +
            '</tbody>' +
          '</table>' +
        '</div>' +

        '<div style="margin-bottom:14px;">' +
          '<div style="font-size:0.72rem; text-transform:uppercase; color:#34d399; font-weight:700; margin-bottom:6px; display:flex; align-items:center; gap:4px;">' +
            '<span>⚡</span> Diretivas e Headers de Controle de Cache (RFC 7234 & Akamai CDN):' +
          '</div>' +
          '<table style="width:100%; border-collapse:collapse; font-size:0.74rem; background:#1e293b; border-radius:6px; overflow:hidden; border:1px solid rgba(16,185,129,0.3);">' +
            '<tbody>' +
              '<tr style="border-bottom:1px solid #334155;">' +
                '<td style="color:#94a3b8; padding:5px 8px; width:190px;">Cache-Control (Resposta)</td>' +
                '<td style="color:#f8fafc; font-weight:bold; font-family:monospace; padding:5px 8px;">' + cacheControl + '</td>' +
              '</tr>' +
              '<tr style="border-bottom:1px solid #334155;">' +
                '<td style="color:#94a3b8; padding:5px 8px;">Cache-Control (Solicitação)</td>' +
                '<td style="color:#38bdf8; font-weight:bold; font-family:monospace; padding:5px 8px;">' + reqCacheControl + '</td>' +
              '</tr>' +
              '<tr style="border-bottom:1px solid #334155;">' +
                '<td style="color:#94a3b8; padding:5px 8px;">CDN Cache Status</td>' +
                '<td style="color:#34d399; font-weight:bold; font-family:monospace; padding:5px 8px;">' + cdnCache + '</td>' +
              '</tr>' +
              '<tr style="border-bottom:1px solid #334155;">' +
                '<td style="color:#94a3b8; padding:5px 8px;">Idade em Cache (Age)</td>' +
                '<td style="color:#f8fafc; font-family:monospace; padding:5px 8px;">' + age + '</td>' +
              '</tr>' +
              '<tr style="border-bottom:1px solid #334155;">' +
                '<td style="color:#94a3b8; padding:5px 8px;">Expiração HTTP (Expires)</td>' +
                '<td style="color:#f8fafc; font-family:monospace; padding:5px 8px;">' + expires + '</td>' +
              '</tr>' +
              '<tr style="border-bottom:1px solid #334155;">' +
                '<td style="color:#94a3b8; padding:5px 8px;">Hash de Integridade (ETag)</td>' +
                '<td style="color:#94a3b8; font-family:monospace; padding:5px 8px; word-break:break-all;">' + etag + '</td>' +
              '</tr>' +
              '<tr style="border-bottom:1px solid #334155;">' +
                '<td style="color:#94a3b8; padding:5px 8px;">Última Modificação (Last-Modified)</td>' +
                '<td style="color:#94a3b8; font-family:monospace; padding:5px 8px;">' + lastModified + '</td>' +
              '</tr>' +
              '<tr style="border-bottom:1px solid #334155;">' +
                '<td style="color:#94a3b8; padding:5px 8px;">Data do Servidor HTTP (Date)</td>' +
                '<td style="color:#94a3b8; font-family:monospace; padding:5px 8px;">' + dateHttp + '</td>' +
              '</tr>' +
              '<tr style="border-bottom:1px solid #334155;">' +
                '<td style="color:#94a3b8; padding:5px 8px;">Diretiva Pragma</td>' +
                '<td style="color:#94a3b8; font-family:monospace; padding:5px 8px;">' + (rawHeaders['pragma'] || reqPragma || '-') + '</td>' +
              '</tr>' +
              '<tr>' +
                '<td style="color:#94a3b8; padding:5px 8px;">Diretiva de Variação (Vary)</td>' +
                '<td style="color:#94a3b8; font-family:monospace; padding:5px 8px;">' + (rawHeaders['vary'] || '-') + '</td>' +
              '</tr>' +
            '</tbody>' +
          '</table>' +
        '</div>' +

        '<div style="margin-bottom:14px;">' +
          '<div style="font-size:0.72rem; text-transform:uppercase; color:#38bdf8; font-weight:700; margin-bottom:6px; display:flex; align-items:center; gap:4px;">' +
            '<span>📤</span> Cabeçalhos da Solicitação Enviada (HTTP Request Headers):' +
          '</div>' +
          '<table style="width:100%; border-collapse:collapse; font-size:0.74rem; background:#1e293b; border-radius:6px; overflow:hidden; border:1px solid rgba(56,189,248,0.25);">' +
            '<thead>' +
              '<tr style="background:#0f172a; border-bottom:1px solid #334155; text-align:left;">' +
                '<th style="padding:5px 8px; color:#94a3b8; font-weight:600; width:190px;">Header</th>' +
                '<th style="padding:5px 8px; color:#94a3b8; font-weight:600;">Valor Enviado</th>' +
              '</tr>' +
            '</thead>' +
            '<tbody>' +
              reqHeadersRowsHtml +
            '</tbody>' +
          '</table>' +
        '</div>' +

        '<div>' +
          '<div style="font-size:0.72rem; text-transform:uppercase; color:#c084fc; font-weight:700; margin-bottom:6px; display:flex; align-items:center; gap:4px;">' +
            '<span>📥</span> Cabeçalhos da Resposta Recebida (HTTP Response Headers - Todos):' +
          '</div>' +
          '<table style="width:100%; border-collapse:collapse; font-size:0.74rem; background:#1e293b; border-radius:6px; overflow:hidden; border:1px solid rgba(168,85,247,0.25);">' +
            '<thead>' +
              '<tr style="background:#0f172a; border-bottom:1px solid #334155; text-align:left;">' +
                '<th style="padding:5px 8px; color:#94a3b8; font-weight:600; width:190px;">Header</th>' +
                '<th style="padding:5px 8px; color:#94a3b8; font-weight:600;">Valor Recebido</th>' +
              '</tr>' +
            '</thead>' +
            '<tbody>' +
              respHeadersRowsHtml +
            '</tbody>' +
          '</table>' +
        '</div>';
    }

    function updateUfDropdowns(regs) {
      const ufs = Array.from(new Set(regs.map(r => (r.fileMeta && r.fileMeta.uf) ? r.fileMeta.uf.toUpperCase() : '').filter(Boolean))).sort();
      const dossieUfSel = document.getElementById('dossieRegFilterUf');
      if (dossieUfSel && ufs.length > 0) {
        const cur = dossieUfSel.value;
        dossieUfSel.innerHTML = '<option value="">Todas as UFs</option>' + ufs.map(u => '<option value="' + u + '">' + u + '</option>').join('');
        dossieUfSel.value = cur;
      }
    }

    // Inicialização (Suporta Modo Online com Auto-Refresh e Modo Offline Autônomo)
    window.STANDALONE_DOSSIER_DATA = ${embeddedData ? JSON.stringify(embeddedData) : 'null'};

    applyColumnVisibility();

    if (window.STANDALONE_DOSSIER_DATA) {
      const d = window.STANDALONE_DOSSIER_DATA;
      rawComparisonList = d.comparison || [];
      rawRegressionsList = d.regressoes || [];
      rawRodadasList = d.rodadas || [];
      activeRodada = d.activeRodada || null;
      latestApiData = d;
      updateRodadasDropdown();
      updatePleitosDropdown(rawComparisonList);
      updateUfDropdowns(rawRegressionsList);
      updateKpis({
        comparison: rawComparisonList,
        regressionsTimeCount: rawRegressionsList.length,
        lastRegressionTime: d.lastRegressionTime || '-',
        todaySlaStats: d.todaySlaStats,
        cacheStats: d.cacheStats
      }, { total: rawRegressionsList.length, regressoes: rawRegressionsList });
      applyFilters();
    } else {
      loadReportData();
      setInterval(() => {
        if (isFilteringActiveRodada()) {
          loadReportData(true);
        }
      }, 10000);
    }

    // =====================================================================
    // GERENCIAMENTO E EDIÇÃO DINÂMICA DE RODADAS NO DOSSIÊ
    // =====================================================================
    function openRodadasModal() {
      document.getElementById('rodadasModal').style.display = 'flex';
      loadRodadasList();
    }

    function closeRodadasModal() {
      document.getElementById('rodadasModal').style.display = 'none';
    }

    async function loadRodadasList() {
      const container = document.getElementById('rodadasListContainer');
      if (!container) return;
      try {
        const res = await fetch('/api/rodadas');
        const data = await res.json();
        renderRodadasList(data.list || [], data.active);
      } catch (err) {
        container.innerHTML = '<div style="color:#ef4444; font-size:0.82rem;">Erro ao carregar rodadas: ' + err.message + '</div>';
      }
    }

    function renderRodadasList(list, active) {
      const container = document.getElementById('rodadasListContainer');
      if (!container) return;
      if (!list || !list.length) {
        container.innerHTML = '<div style="color:#94a3b8; font-size:0.82rem;">Nenhuma rodada cadastrada.</div>';
        return;
      }

      container.innerHTML = '';
      for (const r of list) {
        const isActive = active && active.id === r.id;
        const d = new Date(r.inicio_unix);
        const item = document.createElement('div');
        item.style.cssText = 'display:flex; justify-content:space-between; align-items:center; background:' + (isActive ? 'rgba(16, 185, 129, 0.15)' : '#0f172a') + '; border:1px solid ' + (isActive ? '#10b981' : '#334155') + '; padding:10px 14px; border-radius:8px; gap:10px;';
        
        const info = document.createElement('div');
        info.innerHTML = '<div style="font-size:0.88rem; font-weight:700; color:' + (isActive ? '#10b981' : '#f8fafc') + ';">' + (isActive ? '🟢 ' : '') + r.nome + '</div>' +
                         '<div style="font-size:0.74rem; color:#94a3b8;">Início: ' + d.toLocaleDateString('pt-BR') + ' ' + d.toLocaleTimeString('pt-BR') + '</div>';
        item.appendChild(info);

        const actions = document.createElement('div');
        actions.style.cssText = 'display:flex; gap:6px; align-items:center;';

        if (!isActive) {
          const btnAtivar = document.createElement('button');
          btnAtivar.className = 'btn';
          btnAtivar.style.cssText = 'background:#0284c7; padding:4px 10px; font-size:0.75rem;';
          btnAtivar.innerText = 'Ativar';
          btnAtivar.onclick = () => activateRodada(r.id);
          actions.appendChild(btnAtivar);
        }

        const btnEdit = document.createElement('button');
        btnEdit.className = 'btn-copy';
        btnEdit.innerText = '✏️';
        btnEdit.title = 'Editar nome, data e hora da rodada';
        btnEdit.onclick = () => openEditRodadaModal(r);
        actions.appendChild(btnEdit);

        const btnDel = document.createElement('button');
        btnDel.className = 'btn-copy';
        btnDel.style.color = '#ef4444';
        btnDel.innerText = '🗑️';
        btnDel.title = 'Excluir marco desta rodada';
        btnDel.onclick = () => deleteRodadaPrompt(r.id);
        actions.appendChild(btnDel);

        item.appendChild(actions);
        container.appendChild(item);
      }
    }

    async function activateRodada(id) {
      await fetch('/api/rodadas/ativar', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ id })
      });
      loadRodadasList();
      loadReportData();
    }

    async function submitNewRodada() {
      const input = document.getElementById('newRodadaInput');
      const nome = input.value.trim();
      await fetch('/api/rodadas/nova', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ nome })
      });
      input.value = '';
      loadRodadasList();
      loadReportData();
    }

    let currentEditingRodada = null;

    function openEditRodadaModal(r) {
      currentEditingRodada = r;
      document.getElementById('editRodadaId').value = r.id;
      document.getElementById('editRodadaNome').value = r.nome || '';
      
      const d = new Date(r.inicio_unix);
      const pad = n => String(n).padStart(2, '0');
      const localIso = d.getFullYear() + '-' + pad(d.getMonth()+1) + '-' + pad(d.getDate()) + 'T' + pad(d.getHours()) + ':' + pad(d.getMinutes()) + ':' + pad(d.getSeconds());
      document.getElementById('editRodadaInicio').value = localIso;

      const badge = document.getElementById('editRodadaBadge');
      if (badge) {
        const isActive = Boolean(r.ativo) || (typeof activeRodada !== 'undefined' && activeRodada && activeRodada.id === r.id);
        badge.textContent = isActive ? '🟢 RODADA ATIVA' : ('#' + r.id);
        badge.style.color = isActive ? '#10b981' : '#38bdf8';
      }

      document.getElementById('editRodadaModal').style.display = 'flex';
      setTimeout(() => document.getElementById('editRodadaNome').focus(), 50);
    }

    function closeEditRodadaModal() {
      document.getElementById('editRodadaModal').style.display = 'none';
      currentEditingRodada = null;
    }

    function setEditRodadaToMidnight() {
      const now = new Date();
      const pad = n => String(n).padStart(2, '0');
      document.getElementById('editRodadaInicio').value = now.getFullYear() + '-' + pad(now.getMonth()+1) + '-' + pad(now.getDate()) + 'T00:00:00';
    }

    function setEditRodadaToNow() {
      const now = new Date();
      const pad = n => String(n).padStart(2, '0');
      document.getElementById('editRodadaInicio').value = now.getFullYear() + '-' + pad(now.getMonth()+1) + '-' + pad(now.getDate()) + 'T' + pad(now.getHours()) + ':' + pad(now.getMinutes()) + ':' + pad(now.getSeconds());
    }

    async function submitEditRodadaModal() {
      const id = document.getElementById('editRodadaId').value;
      const nome = document.getElementById('editRodadaNome').value.trim();
      const inicioStr = document.getElementById('editRodadaInicio').value;

      if (!nome) {
        alert('Por favor, informe um nome para a rodada.');
        return;
      }
      if (!inicioStr) {
        alert('Por favor, informe a data e hora de início.');
        return;
      }

      const inicioDate = new Date(inicioStr);
      if (isNaN(inicioDate.getTime())) {
        alert('Data/hora inválida!');
        return;
      }

      const inicioUnix = inicioDate.getTime();

      try {
        const res = await fetch('/api/rodadas/editar', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ id, nome, inicioUnix })
        });
        const data = await res.json();
        if (data.ok) {
          closeEditRodadaModal();
          loadRodadasList();
          loadReportData();
        } else {
          alert('Erro ao editar rodada: ' + (data.error || 'Erro desconhecido'));
        }
      } catch (err) {
        alert('Erro de conexão ao editar rodada: ' + err.message);
      }
    }

    async function deleteRodadaPrompt(id) {
      if (!confirm('Tem certeza que deseja excluir este marco de rodada?')) return;
      await fetch('/api/rodadas/excluir', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ id })
      });
      loadRodadasList();
      loadReportData();
    }
  </script>

  <!-- MODAL DE GERENCIAMENTO DE RODADAS -->
  <div id="rodadasModal" style="display:none; position:fixed; top:0; left:0; width:100%; height:100%; background:rgba(0,0,0,0.75); z-index:9999; align-items:center; justify-content:center; backdrop-filter:blur(3px);">
    <div style="background:#1e293b; border:1px solid #475569; border-radius:14px; width:92%; max-width:620px; padding:24px; box-shadow:0 25px 50px -12px rgba(0,0,0,0.6); color:#f8fafc; max-height:90vh; overflow-y:auto;">
      <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:14px; border-bottom:1px solid #334155; padding-bottom:12px;">
        <h3 style="margin:0; font-size:1.15rem; display:flex; align-items:center; gap:8px;">
          📍 Delimitador Lógico de Rodadas
        </h3>
        <button onclick="closeRodadasModal()" style="background:transparent; border:none; color:#94a3b8; font-size:1.4rem; cursor:pointer; line-height:1;">&times;</button>
      </div>

      <p style="font-size:0.83rem; color:#94a3b8; margin-bottom:16px; line-height:1.4;">
        As rodadas definem o <strong>marco zero</strong> para contagem de regressões e cálculo de SLAs, sem apagar nenhum dado do banco SQLite.
      </p>

      <!-- CRIAR NOVA RODADA -->
      <div style="background:#0f172a; border:1px solid #334155; border-radius:8px; padding:14px; margin-bottom:20px;">
        <h4 style="margin:0 0 10px 0; font-size:0.9rem; color:#38bdf8;">➕ Iniciar Nova Rodada</h4>
        <div style="display:flex; gap:10px; flex-wrap:wrap;">
          <input type="text" id="newRodadaInput" placeholder="Nome da rodada (Ex: Simulado Tarde, Carga 50%)..." style="flex:1; min-width:200px; background:#1e293b; border:1px solid #475569; color:#f8fafc; padding:8px 12px; border-radius:6px; font-size:0.85rem; outline:none;" />
          <button onclick="submitNewRodada()" class="btn" style="background:#10b981; padding:8px 16px;">🚀 Iniciar Rodada Agora</button>
        </div>
      </div>

      <!-- LISTA DE RODADAS HISTÓRICAS -->
      <div>
        <h4 style="margin:0 0 10px 0; font-size:0.9rem; color:#f8fafc;">Histórico de Rodadas Registradas</h4>
        <div id="rodadasListContainer" style="display:flex; flex-direction:column; gap:8px; max-height:280px; overflow-y:auto;">
          Carregando histórico...
        </div>
      </div>

      <div style="display:flex; justify-content:flex-end; margin-top:20px;">
        <button onclick="closeRodadasModal()" class="btn btn-outline" style="padding:8px 18px;">Fechar</button>
      </div>
    </div>
  </div>

  <!-- MODAL DE EDIÇÃO DE ESCOPO DA RODADA (NOME + DATA E HORA COM SEGUNDOS) -->
  <div id="editRodadaModal" style="display:none; position:fixed; top:0; left:0; width:100%; height:100%; background:rgba(0,0,0,0.8); z-index:10000; align-items:center; justify-content:center; backdrop-filter:blur(4px);">
    <div style="background:#1e293b; border:1px solid #38bdf8; border-radius:14px; width:92%; max-width:520px; padding:24px; box-shadow:0 25px 50px -12px rgba(0,0,0,0.7); color:#f8fafc;">
      <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:14px; border-bottom:1px solid #334155; padding-bottom:12px;">
        <h3 style="margin:0; font-size:1.15rem; display:flex; align-items:center; gap:8px; color:#38bdf8;">
          ✏️ Editar Escopo da Rodada <span id="editRodadaBadge" style="font-size:0.75rem; font-weight:700; padding:2px 8px; border-radius:4px; background:rgba(255,255,255,0.08);"></span>
        </h3>
        <button onclick="closeEditRodadaModal()" style="background:transparent; border:none; color:#94a3b8; font-size:1.4rem; cursor:pointer; line-height:1;">&times;</button>
      </div>

      <input type="hidden" id="editRodadaId" value="" />

      <div style="margin-bottom:16px;">
        <label style="display:block; font-size:0.80rem; color:#94a3b8; font-weight:700; margin-bottom:6px;">Nome de Identificação da Rodada:</label>
        <input type="text" id="editRodadaNome" placeholder="Ex: Simulado Tarde, Carga 50%..." style="width:100%; background:#0f172a; border:1px solid #475569; color:#f8fafc; padding:8px 12px; border-radius:6px; font-size:0.88rem; outline:none; box-sizing:border-box;" />
      </div>

      <div style="margin-bottom:16px;">
        <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:6px;">
          <label style="font-size:0.80rem; color:#94a3b8; font-weight:700;">Data e Hora de Início (Marco Zero com Segundos):</label>
          <div style="display:flex; gap:6px;">
            <button type="button" onclick="setEditRodadaToMidnight()" class="btn-copy" style="font-size:0.68rem; padding:2px 6px;">Hoje 00:00:00</button>
            <button type="button" onclick="setEditRodadaToNow()" class="btn-copy" style="font-size:0.68rem; padding:2px 6px;">Agora</button>
          </div>
        </div>
        <input type="datetime-local" step="1" id="editRodadaInicio" style="width:100%; background:#0f172a; border:1px solid #475569; color:#f8fafc; padding:8px 12px; border-radius:6px; font-size:0.88rem; outline:none; box-sizing:border-box; color-scheme:dark;" />
        <div style="font-size:0.72rem; color:#94a3b8; margin-top:5px; line-height:1.4;">
          💡 Define o instante exato com segundos (<strong style="color:#e2e8f0;">HH:mm:ss</strong>) considerado para o marco zero das regressões e cálculo do SLA de propagação. Ocorrências anteriores são preservadas no histórico geral.
        </div>
      </div>

      <div style="display:flex; justify-content:flex-end; gap:10px; margin-top:20px;">
        <button onclick="closeEditRodadaModal()" class="btn btn-outline" style="padding:8px 16px;">Cancelar</button>
        <button onclick="submitEditRodadaModal()" class="btn" style="background:#0284c7; padding:8px 18px; font-weight:700;">💾 Salvar Alterações</button>
      </div>
    </div>
  </div>

  <!-- POPOVER FLUTUANTE DE DEFINIÇÃO FORENSE DOS KPIS (PRÓXIMO AO CARD) -->
  <div id="kpiHelpPopover" style="display: none; position: absolute; z-index: 10000; width: 420px; max-width: 90vw; background: #0f172a; border: 1px solid #38bdf8; border-radius: 8px; padding: 14px 16px; box-shadow: 0 12px 30px rgba(0,0,0,0.85), 0 0 15px rgba(56,189,248,0.25); color: var(--text); font-size: 0.84rem; line-height: 1.55;">
    <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 10px; border-bottom: 1px solid rgba(56,189,248,0.25); padding-bottom: 6px;">
      <h3 id="kpiHelpTitle" style="margin: 0; font-size: 0.98rem; color: #38bdf8; font-weight: 700;">ℹ️ Definição do Indicador</h3>
      <button onclick="closeKpiHelp()" style="background: transparent; border: none; color: var(--text-muted); font-size: 1.1rem; cursor: pointer; padding: 0 4px; line-height: 1;">✕</button>
    </div>
    <div id="kpiHelpBody" style="font-size: 0.82rem; color: #e2e8f0; line-height: 1.5;">
    </div>
    <div style="text-align: right; margin-top: 12px; border-top: 1px solid rgba(255,255,255,0.08); padding-top: 8px;">
      <button onclick="closeKpiHelp()" class="btn-copy" style="padding: 3px 12px; font-size: 0.75rem; background: #0284c7; color: #fff; border: 1px solid #38bdf8;">Fechar</button>
    </div>
  </div>
</body>
</html>`;

  if (!embeddedData) {
    fs.writeFileSync(REPORT_HTML, html, 'utf8');
  }
  return html;
}

function saveVersionInUrlStructure(serverKey, relPath, payload, rawText, meta) {
  try {
    const cleanRel = relPath.replace(/^[\\/\/]+/, '');
    const dirPart = path.dirname(cleanRel);
    const ext = path.extname(cleanRel) || '.json';
    const baseName = path.basename(cleanRel, ext);

    const safeDg = (meta.dg || 'sem_dg').replace(/[/\\:]/g, '-');
    const safeHg = (meta.hg || 'sem_hg').replace(/[/\\:]/g, '-');
    const safeIdg = meta.idg ? String(meta.idg) : 'sem_idg';
    const env = serverKey || 'SRV';

    const versionFileName = `${baseName}_${env}_dg${safeDg}_hg${safeHg}_idg${safeIdg}${ext}`;
    const targetDir = path.join(VERSOES_DIR, dirPart);

    if (!fs.existsSync(targetDir)) {
      fs.mkdirSync(targetDir, { recursive: true });
    }

    const targetPath = path.join(targetDir, versionFileName);

    if (!fs.existsSync(targetPath)) {
      const dataObj = payload || decodeJwsOrJson(rawText);
      const fileContent = dataObj ? JSON.stringify(dataObj, null, 2) : (rawText || '{}');
      fs.writeFileSync(targetPath, fileContent, 'utf8');
    }
  } catch (err) {
    console.error(`Erro ao salvar versao em estrutura de URL para ${relPath}:`, err.message);
  }
}

function recordVersionAndEvidence(serverKey, relPath, payload, rawText, source, headers, meta, isRegression, regressionDetails = null, criterion = 'GERAL') {
  saveVersionInUrlStructure(serverKey, relPath, payload, rawText, meta);
  const filename = getFilename(relPath);
  const timestampIso = meta.callTimeIso || (meta.timestampIso ? meta.timestampIso : new Date().toISOString());
  const timestampUnix = meta.callTimeUnix || (meta.timestampUnix ? meta.timestampUnix : new Date(timestampIso).getTime());
  const callTimeIso = meta.callTimeIso || timestampIso;
  const callTimeUnix = meta.callTimeUnix || timestampUnix;
  const latencyMs = meta.latencyMs !== undefined ? meta.latencyMs : null;
  const role = SERVERS[serverKey]?.role || serverKey;

  const requestHeaders = meta.requestHeaders || {};
  const responseHeaders = headers || {};

  // Resumo de todas as diretivas e headers de controle de cache (RFC 7234, HTTP 1.1 e Akamai CDN)
  const cacheControlSummary = {
    request: {
      'cache-control': requestHeaders['cache-control'] || requestHeaders['Cache-Control'] || null,
      'pragma': requestHeaders['pragma'] || requestHeaders['Pragma'] || null,
      'if-modified-since': requestHeaders['if-modified-since'] || requestHeaders['If-Modified-Since'] || null,
      'if-none-match': requestHeaders['if-none-match'] || requestHeaders['If-None-Match'] || null
    },
    response: {
      'cache-control': responseHeaders['cache-control'] || responseHeaders['Cache-Control'] || null,
      'pragma': responseHeaders['pragma'] || responseHeaders['Pragma'] || null,
      'expires': responseHeaders['expires'] || responseHeaders['Expires'] || null,
      'age': responseHeaders['age'] !== undefined ? responseHeaders['age'] : null,
      'etag': responseHeaders['etag'] || responseHeaders['ETag'] || null,
      'last-modified': responseHeaders['last-modified'] || responseHeaders['Last-Modified'] || null,
      'date': responseHeaders['date'] || responseHeaders['Date'] || null,
      'vary': responseHeaders['vary'] || responseHeaders['Vary'] || null,
      'cdn-cache-status': responseHeaders['cdn-cache-status'] || responseHeaders['x-cache'] || null,
      'x-cache': responseHeaders['x-cache'] || null,
      'x-cache-lookup': responseHeaders['x-cache-lookup'] || null,
      'x-cache-hits': responseHeaders['x-cache-hits'] || null,
      'x-check-cacheable': responseHeaders['x-check-cacheable'] || null,
      'x-cache-key': responseHeaders['x-cache-key'] || responseHeaders['x-true-cache-key'] || null,
      'akamai-grn': meta.akamaiGrn || responseHeaders['akamai-grn'] || responseHeaders['x-akamai-grn'] || null,
      'server': responseHeaders['server'] || null
    }
  };

  let rawFilePath = null;

  if (isRegression) {
    const prefix = 'REGRESSAO';
    const safeDate = timestampIso.replace(/[:.]/g, '-');
    const rawFilename = `${prefix}_${serverKey}_${filename.replace('.json', '')}_idg${meta.idg || 'na'}_${safeDate}.json`;
    rawFilePath = path.join(EVIDENCIAS_DIR, rawFilename);

    try {
      fs.writeFileSync(rawFilePath, JSON.stringify({
        metadata: {
          servidor: serverKey,
          papel_servidor: role,
          arquivo: filename,
          url_origem: (SERVERS[serverKey]?.baseUrl || '') + relPath,
          instante_chamada_iso: callTimeIso,
          instante_chamada_unix: callTimeUnix,
          latencia_ms: latencyMs,
          timestamp_coleta: timestampIso,
          source,
          headers: responseHeaders, // compatibilidade retroativa
          response_headers: responseHeaders,
          request_headers: requestHeaders,
          cache_control_headers: cacheControlSummary,
          akamai_grn: meta.akamaiGrn || (responseHeaders && (responseHeaders['akamai-grn'] || responseHeaders['x-akamai-grn'])) || null,
          isRegression,
          criterion,
          regressionDetails
        },
        payload
      }, null, 2), 'utf8');
    } catch {
      rawFilePath = null;
    }
  }

  // 1. Grava na tabela leituras
  try {
    stmtInsertLeitura.run(
      timestampIso,
      timestampUnix,
      serverKey,
      role,
      relPath,
      meta.idg || null,
      meta.dg || null,
      meta.hg || null,
      meta.genTime || null,
      meta.st !== null && meta.st !== undefined ? String(meta.st) : null,
      meta.pst || null,
      meta.vTot || null,
      meta.etag || null,
      meta.status || 'NORMAL',
      meta.details || 'Leitura realizada',
      rawFilePath || null,
      meta.dt || null,
      meta.ht || null,
      meta.totTime || null,
      JSON.stringify(responseHeaders || {}),
      meta.serverIp || null,
      meta.cacheControl || null,
      meta.cdnCacheStatus || null,
      meta.maxAge !== null && meta.maxAge !== undefined ? Number(meta.maxAge) : null,
      meta.akamaiGrn || (responseHeaders && (responseHeaders['akamai-grn'] || responseHeaders['x-akamai-grn'])) || null,
      callTimeIso,
      callTimeUnix,
      latencyMs,
      JSON.stringify(requestHeaders || {})
    );
  } catch (e) {
    console.error('Erro ao gravar leitura no SQLite:', e.message);
  }

  // 2. Se for regressão, grava na tabela regressoes e no CSV
  if (isRegression) {
    try {
      stmtInsertRegressao.run(
        timestampIso,
        serverKey,
        role,
        relPath,
        criterion || 'REGRESSAO',
        meta.details || 'Regressão detectada',
        meta.prevIdg || null,
        meta.prevDg || null,
        meta.prevHg || null,
        meta.prevSt !== null && meta.prevSt !== undefined ? String(meta.prevSt) : null,
        meta.idg || null,
        meta.dg || null,
        meta.hg || null,
        meta.st !== null && meta.st !== undefined ? String(meta.st) : null,
        rawFilePath || null,
        meta.details || null,
        meta.prevDt || null,
        meta.prevHt || null,
        meta.dt || null,
        meta.ht || null,
        meta.akamaiGrn || (responseHeaders && (responseHeaders['akamai-grn'] || responseHeaders['x-akamai-grn'])) || null,
        callTimeIso,
        callTimeUnix,
        latencyMs,
        JSON.stringify(responseHeaders || {}),
        JSON.stringify(requestHeaders || {})
      );

      const regLine = [
        timestampIso,
        serverKey,
        `"${role}"`,
        filename,
        `"${criterion}"`,
        `"${(meta.details || '').replace(/"/g, '""')}"`,
        meta.prevIdg ?? '',
        `"${meta.prevDg || ''} ${meta.prevHg || ''}".trim()`,
        `"${meta.prevDt || ''} ${meta.prevHt || ''}".trim()`,
        meta.prevSt ?? '',
        meta.idg ?? '',
        `"${meta.dg || ''} ${meta.hg || ''}".trim()`,
        `"${meta.dt || ''} ${meta.ht || ''}".trim()`,
        meta.st ?? '',
        `"${rawFilePath || ''}"`
      ].join(',') + '\n';
      fs.appendFileSync(REGRESSIONS_CSV, regLine, 'utf8');

      const alertMsg = `\n======================================================================\n` +
        `[${timestampIso}] REGRESSÃO DETECTADA (${criterion}) NO SERVIDOR: ${serverKey} (${role})!\n` +
        `  Instância / IP: ${meta.serverIp || 'N/A'}\n` +
        `  Arquivo: ${relPath}\n` +
        `  Motivo: ${meta.details}\n` +
        `  Versão Anterior: dg=${meta.prevDg} hg=${meta.prevHg} | dt=${meta.prevDt || '-'} ht=${meta.prevHt || '-'} | st=${meta.prevSt ?? '-'} (idg: ${meta.prevIdg})\n` +
        `  Versão Recebida: dg=${meta.dg} hg=${meta.hg} | dt=${meta.dt || '-'} ht=${meta.ht || '-'} | st=${meta.st ?? '-'} (idg: ${meta.idg})\n` +
        `  Evidência Raw Gravada: ${rawFilePath}\n` +
        `======================================================================\n`;
      fs.appendFileSync(ALERT_LOG, alertMsg, 'utf8');
    } catch (e) {
      console.error('Erro ao gravar regressão no SQLite:', e.message);
    }
  }

  totalChecksCount++;
  recentLogs.unshift({
    time: new Date(callTimeUnix).toLocaleTimeString(),
    timestampIso,
    serverKey,
    role,
    filename,
    criterion,
    status: meta.status,
    details: meta.details,
    isRegression
  });
  if (recentLogs.length > 100) recentLogs.pop();

  broadcastUpdate('log', { serverKey, role, filename, ...meta, criterion });
  generateHtmlReport();
}

/**
 * COMPARAÇÃO DUPLA DE MONOTONICIDADE (DG/HG e IDG INDEPENDENTES)
 */
function processVersion(serverKey, relPath, payload, rawText, source, headers = {}, timing = {}) {
  const filename = getFilename(relPath);
  const callTimeUnix = timing.callTimeUnix || Date.now();
  const callTimeIso = timing.callTimeIso || new Date(callTimeUnix).toISOString();
  const latencyMs = timing.latencyMs !== undefined ? timing.latencyMs : null;
  const localTime = new Date(callTimeUnix).toLocaleTimeString();

  const idg = payload.idg ? String(payload.idg) : null;
  const idgNum = idg ? Number(idg) : null;
  const dg = payload.dg ? payload.dg.trim() : null;
  const hg = payload.hg ? payload.hg.trim() : null;
  const genTime = parseDgHg(dg, hg);
  const dt = (payload.dt && String(payload.dt).trim() !== '') ? String(payload.dt).trim() : null;
  const ht = (payload.ht && String(payload.ht).trim() !== '') ? String(payload.ht).trim() : null;
  const totTime = parseDgHg(dt, ht);
  const stRaw = payload.s?.st ?? payload.st ?? null;
  const st = (stRaw !== null && stRaw !== undefined && String(stRaw).trim() !== '') ? Number(stRaw) : null;
  const pst = payload.s?.pst ?? payload.pst ?? null;
  const ts = payload.s?.ts ?? payload.ts ?? null;
  const vTot = payload.s?.tv ?? payload.vTot ?? null;
  const etag = headers['etag'] || headers['ETag'] || null;

  // Extração de atributos de Cache HTTP e CDN
  const cacheControl = headers['cache-control'] || headers['Cache-Control'] || null;
  let maxAge = null;
  if (cacheControl) {
    const maMatch = cacheControl.match(/max-age=(\d+)/i);
    if (maMatch) maxAge = parseInt(maMatch[1], 10);
  }
  const cdnCacheStatus = headers['cdn-cache-status'] || headers['x-cache'] || headers['x-cache-lookup'] || (serverKey === 'HMG' ? 'ORIGIN (Apache)' : null);
  const ageHeader = headers['age'] || null;
  const expiresHeader = headers['expires'] || headers['Expires'] || null;
  const lastModifiedHeader = headers['last-modified'] || headers['Last-Modified'] || null;
  const serverHeader = headers['server'] || (serverKey === 'SIM' ? 'Akamai CDN' : 'Apache Origin');
  const serverIp = headers['x-server-ip'] || timing.serverIp || null;
  const akamaiGrn = headers['akamai-grn'] || headers['x-akamai-grn'] || headers['x-akamai-request-id'] || headers['akamai-request-id'] || null;
  const requestHeaders = timing.requestHeaders || {
    'cache-control': 'no-cache, no-store, must-revalidate',
    'pragma': 'no-cache',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) TSE-Audit/2.0',
    'accept': 'application/json, text/plain, */*'
  };

  const currentMeta = {
    cacheControl,
    maxAge,
    cdnCacheStatus,
    ageHeader,
    expiresHeader,
    lastModifiedHeader,
    serverHeader,
    serverIp,
    akamaiGrn,
    requestHeaders,
    timestampIso: callTimeIso,
    timestampUnix: callTimeUnix,
    callTimeIso,
    callTimeUnix,
    latencyMs,
    serverKey,
    relPath,
    filename,
    idg,
    idgNum,
    dg,
    hg,
    genTime,
    dt,
    ht,
    totTime,
    st,
    pst,
    ts,
    vTot,
    etag,
    source,
    rodadaId: null
  };

  const activeRodada = getActiveRodada();
  if (activeRodada) {
    currentMeta.rodadaId = activeRodada.id;
  }

  const prev = serverStates[serverKey].get(relPath);

  // Isolamento estrito por rodada: se não há estado anterior ou se o estado anterior
  // pertence a outra rodada ou foi registrado antes do início da rodada ativa,
  // esta primeira leitura é a VERSÃO INICIAL (baseline v1) da rodada atual.
  const prevOutOfRodada = prev && (
    (activeRodada && (prev.callTimeUnix || prev.timestampUnix || 0) < activeRodada.inicio_unix) ||
    (prev.rodadaId && activeRodada && prev.rodadaId !== activeRodada.id)
  );

  if (!prev || prevOutOfRodada) {
    currentMeta.status = 'INICIAL';
    currentMeta.details = prevOutOfRodada ? 'Versão inicial (novo ciclo de rodada)' : 'Versão inicial';
    serverStates[serverKey].set(relPath, currentMeta);
    recordVersionAndEvidence(serverKey, relPath, payload, rawText, source, headers, currentMeta, false, null, 'INICIAL');
    console.log(`[${localTime}] ${CYAN}[${serverKey} (${SERVERS[serverKey]?.role})]${RESET} ${BOLD}${filename}${RESET}: Leitura inicial${prevOutOfRodada ? ' [Nova Rodada]' : ''} -> dg=${dg} hg=${hg} | idg=${idg || 'N/A'}`);
    logComparisonRow(relPath);
    return;
  }

  // Gating 1: Respostas que devem ignorar teste de regressão (ex: cache interno de navegador)
  if (timing.skipRegression) {
    recordVersionAndEvidence(serverKey, relPath, payload, rawText, source, headers, currentMeta, false, null, 'CACHE_NAVEGADOR');
    return;
  }

  // Gating 2 (DUPLA CHECAGEM): Resposta fora de ordem cronológica de disparo
  // Se esta requisição foi disparada ANTERIORMENTE ao disparo que originou o estado atual (prev),
  // a resposta chegou com atraso de trânsito. NUNCA pode ser avaliada como regressão!
  if (prev.callTimeUnix && callTimeUnix < prev.callTimeUnix) {
    currentMeta.status = 'RESPOSTA_FORA_DE_ORDEM';
    currentMeta.details = `Chegada fora de ordem: chamada em ${callTimeIso} anterior à leitura atual (${prev.callTimeIso})`;
    recordVersionAndEvidence(serverKey, relPath, payload, rawText, source, headers, currentMeta, false, null, 'FORA_DE_ORDEM');
    return;
  }

  const isSameGen = Boolean(dg && prev.dg && hg && prev.hg && dg === prev.dg && hg === prev.hg);
  const isSameTot = Boolean((!dt && !prev.dt && !ht && !prev.ht) || (dt && prev.dt && ht && prev.ht && dt === prev.dt && ht === prev.ht));
  const isSameSt = Boolean((st === null && prev.st === null) || (st !== null && prev.st !== null && st === prev.st));
  const isSameIdg = Boolean((!idg && !prev.idg) || (idg && prev.idg && idg === prev.idg));

  if (isSameGen && isSameTot && isSameSt && isSameIdg) {
    // Atualiza metadados dinâmicos de cache (ex: TTL/max-age decrescente do SIM) mesmo sem nova versão
    prev.cacheControl = currentMeta.cacheControl;
    prev.maxAge = currentMeta.maxAge;
    prev.cdnCacheStatus = currentMeta.cdnCacheStatus;
    prev.ageHeader = currentMeta.ageHeader;
    prev.expiresHeader = currentMeta.expiresHeader;
    prev.lastModifiedHeader = currentMeta.lastModifiedHeader;
    prev.serverHeader = currentMeta.serverHeader;
    if (currentMeta.serverIp) prev.serverIp = currentMeta.serverIp;
    if (currentMeta.akamaiGrn) prev.akamaiGrn = currentMeta.akamaiGrn;
    prev.callTimeUnix = currentMeta.callTimeUnix;
    prev.callTimeIso = currentMeta.callTimeIso;
    prev.latencyMs = currentMeta.latencyMs;
    return;
  }

  let isTimeRegression = false;
  let isTotTimeRegression = false;
  let isStRegression = false;
  let isIdgRegression = false;
  const reasons = [];

  // Critério 1: Monotonicidade Temporal de Geração (DG/HG)
  // Se dg e hg forem idênticos em texto, por definição matemática NÃO HÁ REGRESSÃO TEMPORAL
  if (!isSameGen && genTime !== null && prev.genTime !== null && genTime < prev.genTime) {
    isTimeRegression = true;
    const diffSec = Math.round((prev.genTime - genTime) / 1000);
    reasons.push(`REGRESSÃO TEMPORAL (DG/HG): retrocedeu de ${prev.dg} ${prev.hg} para ${dg} ${hg} (-${diffSec}s)`);
  }

  // Critério 2: Monotonicidade Temporal de Totalização (DT/HT)
  // Se dt e ht forem idênticos em texto, por definição matemática NÃO HÁ REGRESSÃO DE TOTALIZAÇÃO
  if (!isSameTot && totTime !== null && prev.totTime !== null && totTime < prev.totTime) {
    isTotTimeRegression = true;
    const diffTotSec = Math.round((prev.totTime - totTime) / 1000);
    reasons.push(`REGRESSÃO DE TOTALIZAÇÃO (DT/HT): retrocedeu de ${prev.dt} ${prev.ht} para ${dt} ${ht} (-${diffTotSec}s)`);
  }

  // Critério 3: Monotonicidade de Seções Totalizadas (ST)
  // A quantidade acumulada de seções apuradas nunca pode diminuir no mesmo arquivo
  if (!isSameSt && st !== null && prev.st !== null && st < prev.st) {
    isStRegression = true;
    const diffSt = prev.st - st;
    reasons.push(`REGRESSÃO DE SEÇÕES APURADAS (ST): retrocedeu de ${prev.st} para ${st} seções (-${diffSt})`);
  }

  // Critério 4: Monotonicidade Sequencial (IDG) - Apenas rastreio/anomalia (não bloqueante no Oracle RAC)
  if (!isSameIdg && idgNum !== null && prev.idgNum !== null && idgNum < prev.idgNum) {
    isIdgRegression = true;
    const diffIdg = prev.idgNum - idgNum;
    reasons.push(`ANOMALIA SEQUENCIAL (IDG): retrocedeu de ${prev.idg} para ${idg} (-${diffIdg})`);
  }

  // Incoerência de Geração vs Totalização
  const isDgAdvancedOrSame = (!isTimeRegression && genTime !== null && prev.genTime !== null && genTime >= prev.genTime);
  const isInversion = isDgAdvancedOrSame && (isTotTimeRegression || isStRegression);

  if (isInversion) {
    if (isTotTimeRegression && isStRegression) {
      reasons.push(`[INVERSÃO_DG_DT_ST] 🚨 INVERSÃO DE TOTALIZAÇÃO: Arquivo gerado mais recentemente (DG/HG avançou), mas a Totalização (DT/HT) e Seções Apuradas (ST) retrocederam!`);
    } else if (isTotTimeRegression) {
      reasons.push(`[INVERSÃO_DG_DT_ST] 🚨 INVERSÃO DE TOTALIZAÇÃO: Arquivo gerado mais recentemente (DG/HG avançou), mas a Totalização (DT/HT) retrocedeu!`);
    } else if (isStRegression) {
      reasons.push(`[INVERSÃO_DG_DT_ST] 🚨 INVERSÃO DE SEÇÕES: Arquivo gerado mais recentemente (DG/HG avançou), mas as Seções Apuradas (ST) diminuíram!`);
    }
  } else if (!isTimeRegression && isIdgRegression && genTime !== null && prev.genTime !== null && genTime > prev.genTime) {
    reasons.push(`🚨 ANOMALIA DE GERAÇÃO: DG/HG avançou no tempo, mas IDG retrocedeu sequencialmente!`);
  }

  // Auditoria Estrita: Qualquer regressão em DG/HG, DT/HT ou ST é tratada como REGRESSÃO FORENSE!
  const isRegression = isTimeRegression || isTotTimeRegression || isStRegression;

  const violatedCriteria = [];
  if (isTimeRegression) violatedCriteria.push('TEMPO (DG/HG)');
  if (isTotTimeRegression) {
    if (isInversion && !isStRegression) {
      violatedCriteria.push('TOTALIZAÇÃO (DT/HT) [INVERSÃO: DG ↗, DT ↘]');
    } else {
      violatedCriteria.push('TOTALIZAÇÃO (DT/HT)');
    }
  }
  if (isStRegression) {
    if (isInversion && !isTotTimeRegression) {
      violatedCriteria.push('SEÇÕES (ST) [INVERSÃO: DG ↗, ST ↘]');
    } else {
      violatedCriteria.push('SEÇÕES (ST)');
    }
  }

  let criterion = isRegression ? violatedCriteria.join(' + ') : 'NORMAL';
  if (isInversion && isTotTimeRegression && isStRegression) {
    criterion = 'TOTALIZAÇÃO (DT/HT) + SEÇÕES (ST) [INVERSÃO: DG ↗, DT/ST ↘]';
  }

  if (isRegression) {
    currentMeta.status = 'REGRESSAO_DETECTADA';
    currentMeta.criterion = criterion;
    currentMeta.prevIdg = prev.idg;
    currentMeta.prevDg = prev.dg;
    currentMeta.prevHg = prev.hg;
    currentMeta.prevDt = prev.dt;
    currentMeta.prevHt = prev.ht;
    currentMeta.prevSt = prev.st;
    currentMeta.details = reasons.join(' | ');

    console.log(`\n${RED}${BOLD}======================================================================${RESET}`);
    console.log(`${RED}${BOLD}🚨🚨 [ALERTA: REGRESSÃO NO SERVIDOR ${serverKey} (${SERVERS[serverKey]?.role})!] 🚨🚨${RESET}`);
    if (isInversion) {
      console.log(`${RED}${BOLD}🚨🚨 [HIPÓTESE DETECTADA: INVERSÃO DE DADOS (DG/HG AVANÇOU, DT/ST RETROCEDEU)!] 🚨🚨${RESET}`);
    }
    console.log(`${RED}Critério Violado:${RESET} ${BOLD}${criterion}${RESET}`);
    console.log(`${RED}Arquivo:${RESET}          ${BOLD}${filename}${RESET} (${source})`);
    console.log(`${RED}Motivo:${RESET}           ${reasons.join(' | ')}`);
    console.log(`  Versão Anterior (Mais Nova): dg=${prev.dg} hg=${prev.hg} | idg=${prev.idg}`);
    console.log(`  Versão Recebida (Retrocedeu): dg=${dg} hg=${hg} | idg=${idg}`);
    console.log(`${RED}${BOLD}>> EVIDÊNCIA GRAVADA NO BANCO SQLITE E EM ARQUIVO RAW! <<${RESET}`);
    console.log(`${RED}${BOLD}======================================================================\n${RESET}`);

    recordVersionAndEvidence(serverKey, relPath, payload, rawText, source, headers, currentMeta, true, currentMeta.details, criterion);
    serverStates[serverKey].set(relPath, currentMeta);
  } else {
    let progressionInfo = `Novo dg/hg: ${dg} ${hg} | idg: ${idg}`;
    if (prev.genTime && genTime) {
      const advanceSec = Math.round((genTime - prev.genTime) / 1000);
      progressionInfo += ` (+${advanceSec}s)`;
    }
    currentMeta.status = 'PROGRESSAO_OK';
    currentMeta.criterion = 'OK';
    currentMeta.details = progressionInfo;
    serverStates[serverKey].set(relPath, currentMeta);

    recordVersionAndEvidence(serverKey, relPath, payload, rawText, source, headers, currentMeta, false, null, 'OK');

    
    // Rastreia SLA de Sincronização relativo à ORIGEM definida:
    const originServer = getOriginServer();
    const isOriginNode = originServer && (originServer.chave === serverKey);

    if (isOriginNode && genTime !== null) {
      let tracker = fileSyncTracker.get(relPath);
      if (!tracker) {
        tracker = {
          targetHg: hg,
          targetDg: dg,
          targetGenTime: genTime,
          originDetectedAt: Date.now(),
          hmgDetectedAt: Date.now(),
          replicas: {},
          lastSyncSec: 0,
          isWaiting: false
        };
        fileSyncTracker.set(relPath, tracker);
      } else if (tracker.targetGenTime === null || genTime > tracker.targetGenTime || tracker.targetHg !== hg || tracker.targetDg !== dg) {
        tracker.targetHg = hg;
        tracker.targetDg = dg;
        tracker.targetGenTime = genTime;
        tracker.originDetectedAt = Date.now();
        tracker.hmgDetectedAt = Date.now();
        tracker.replicas = {};
        tracker.isWaiting = true;
      }
    } else if (!isOriginNode && genTime !== null) {
      const tracker = fileSyncTracker.get(relPath);
      if (tracker && tracker.targetGenTime && genTime >= tracker.targetGenTime) {
        if (!tracker.replicas) tracker.replicas = {};
        if (!tracker.replicas[serverKey]) {
          const syncSec = tracker.originDetectedAt ? Math.max(0, Math.round((Date.now() - tracker.originDetectedAt) / 1000)) : 0;
          tracker.replicas[serverKey] = {
            syncedAt: Date.now(),
            syncSec,
            isWaiting: false
          };
          tracker.lastSyncSec = syncSec;
          tracker.isWaiting = false;
          console.log(`   ${CYAN}⚡ [SLA SYNC DG/HG CONCLUÍDO]${RESET} ${filename}: Sincronizou no nó ${serverKey} em ${BOLD}${formatMinSec(syncSec)}${RESET}!`);
        }
      }
    }

    console.log(`[${localTime}] ${GREEN}✅ [${serverKey} ATUALIZOU]${RESET} ${BOLD}${filename}${RESET}: dg=${dg} hg=${hg} | idg=${idg}`);
    logComparisonRow(relPath);
  }
}

function logComparisonRow(relPath) {
  const comp = getComparison(relPath);
  const hmg = serverStates.HMG.get(relPath); // FONTE
  const sim = serverStates.SIM.get(relPath); // CACHE

  if (hmg && sim) {
    let color = GREEN;
    if (comp.status === 'CACHE_ATRASADO') color = YELLOW;
    if (comp.status === 'CACHE_A_FRENTE') color = CYAN;
    if (comp.inconsistency) color = MAGENTA;

    console.log(`   ${color}📡 [PROPAGAÇÃO]${RESET} ${relPath}: Tempo=${comp.textTime} | IDG=${comp.textIdg} | HMG(${hmg.hg}/idg:${hmg.idg}) -> SIM(${sim.hg}/idg:${sim.idg})`);

    try {
      stmtInsertComparativo.run(
        new Date().toISOString(),
        relPath,
        hmg.dg,
        hmg.hg,
        hmg.idg,
        hmg.st,
        sim.dg,
        sim.hg,
        sim.idg,
        sim.st,
        comp.delaySec,
        comp.diffIdg,
        comp.syncSlaSec,
        comp.status,
        comp.text
      );
    } catch {}

    const csvLine = [
      new Date().toISOString(),
      relPath,
      hmg.dg,
      hmg.hg,
      hmg.idg,
      hmg.st,
      sim.dg,
      sim.hg,
      sim.idg,
      sim.st,
      comp.delaySec,
      comp.diffIdg,
      comp.statusTime,
      comp.statusIdg,
      `"${comp.text}"`
    ].join(',') + '\n';
    fs.appendFileSync(LOG_CSV, csvLine, 'utf8');
  }
}

function httpRequestWithIp(urlStr) {
  return new Promise((resolve) => {
    let resolved = false;
    const finish = (result) => {
      if (!resolved) {
        resolved = true;
        resolve(result);
      }
    };

    try {
      const u = new URL(urlStr);
      const lib = u.protocol === 'https:' ? https : http;
      const callTimeUnix = Date.now();
      const callTimeIso = new Date(callTimeUnix).toISOString();
      const requestHeaders = {
        'Cache-Control': 'no-cache, no-store, must-revalidate',
        'Pragma': 'no-cache',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) TSE-Audit/2.0',
        'Accept': 'application/json, text/plain, */*',
        'Accept-Encoding': 'gzip, deflate, br'
      };
      const req = lib.request(u, {
        method: 'GET',
        timeout: 5000,
        headers: requestHeaders
      }, (res) => {
        const serverIp = req.socket?.remoteAddress || null;
        const encoding = (res.headers['content-encoding'] || '').toLowerCase().trim();
        let stream = res;
        if (encoding === 'gzip') {
          stream = res.pipe(zlib.createGunzip());
        } else if (encoding === 'deflate') {
          stream = res.pipe(zlib.createInflate());
        } else if (encoding === 'br') {
          stream = res.pipe(zlib.createBrotliDecompress());
        }

        const chunks = [];
        stream.on('data', chunk => chunks.push(chunk));
        stream.on('end', () => {
          const latencyMs = Date.now() - callTimeUnix;
          const text = Buffer.concat(chunks).toString('utf8');
          const headersObj = {};
          for (const [k, v] of Object.entries(res.headers)) {
            headersObj[k.toLowerCase()] = v;
          }
          if (serverIp) headersObj['x-server-ip'] = serverIp;
          finish({
            ok: res.statusCode >= 200 && res.statusCode < 300,
            statusCode: res.statusCode,
            text,
            headers: headersObj,
            responseHeaders: headersObj,
            requestHeaders,
            serverIp,
            callTimeUnix,
            callTimeIso,
            latencyMs
          });
        });
        stream.on('error', (err) => {
          // Se falhar na descompressão, tenta fallback para corpo bruto se houver chunks
          finish({
            ok: false,
            error: 'Decompression error: ' + err.message,
            statusCode: res.statusCode,
            text: '',
            headers: {},
            responseHeaders: {},
            requestHeaders,
            callTimeUnix,
            callTimeIso,
            latencyMs: Date.now() - callTimeUnix
          });
        });
      });
      req.on('timeout', () => {
        req.destroy(new Error('ETIMEDOUT'));
      });
      req.on('error', (e) => finish({
        ok: false,
        error: e.message,
        headers: {},
        responseHeaders: {},
        requestHeaders,
        callTimeUnix,
        callTimeIso,
        latencyMs: Date.now() - callTimeUnix
      }));
      req.end();
    } catch (err) {
      finish({ ok: false, error: err.message, headers: {}, responseHeaders: {}, requestHeaders: {} });
    }
  });
}

const inFlightPollFiles = new Set();

async function pollFile(relPath) {
  if (inFlightPollFiles.has(relPath)) return;
  inFlightPollFiles.add(relPath);
  try {
    const cacheBust = `?nocache=${Date.now()}`;
    const activeServers = getActiveServers();

    for (const srv of activeServers) {
      const fullUrl = srv.baseUrl + relPath + cacheBust;
      try {
        const resp = await httpRequestWithIp(fullUrl);
        if (!resp.ok || !resp.text) continue;

        const payload = decodeJwsOrJson(resp.text);
        if (!payload) continue;

        processVersion(srv.chave, relPath, payload, resp.text, 'Polling_Ativo', resp.headers, {
          callTimeUnix: resp.callTimeUnix,
          callTimeIso: resp.callTimeIso,
          latencyMs: resp.latencyMs,
          requestHeaders: resp.requestHeaders,
          serverIp: resp.serverIp
        });
      } catch {}
    }
  } finally {
    inFlightPollFiles.delete(relPath);
  }
}

async function runWorkerPool(filesArray, concurrency = 15) {
  const queue = [...filesArray];
  const workers = Array.from({ length: concurrency }, async () => {
    while (queue.length > 0) {
      const relPath = queue.shift();
      if (relPath) {
        try {
          await pollFile(relPath);
        } catch {}
      }
    }
  });
  await Promise.all(workers);
}

async function attachTabObserver(tab) {
  const wsUrl = tab.webSocketDebuggerUrl;
  if (!wsUrl || activeWsConnections.has(tab.id)) return;

  const ws = new WebSocket(wsUrl);
  let reqCounter = 1;
  const pendingRequests = new Map();
  const cdpRequestHeadersMap = new Map();

  ws.onopen = () => {
    activeWsConnections.set(tab.id, ws);
    console.log(`${BLUE}🔗 [CDP Conectado]${RESET} Aba "${tab.title}" (${tab.id.slice(0, 8)})`);
    ws.send(JSON.stringify({ id: reqCounter++, method: 'Network.enable' }));
  };

  ws.onmessage = (event) => {
    try {
      const data = JSON.parse(event.data);

      if (data.method === 'Network.requestWillBeSent') {
        const req = data.params?.request;
        const requestId = data.params?.requestId;
        if (requestId && req?.headers) {
          const reqHeaders = {};
          for (const [k, v] of Object.entries(req.headers)) {
            reqHeaders[k.toLowerCase()] = v;
          }
          cdpRequestHeadersMap.set(requestId, reqHeaders);
          if (cdpRequestHeadersMap.size > 2000) {
            const firstKey = cdpRequestHeadersMap.keys().next().value;
            cdpRequestHeadersMap.delete(firstKey);
          }
        }
      }

      if (data.method === 'Network.responseReceived') {
        const resp = data.params.response;
        const url = resp.url;

        if (url.includes('.jws') || (url.includes('/dados/') && url.includes('.json')) || url.includes('/config/')) {
          let serverKey = null;
          let relPath = null;

          const activeServers = getActiveServers();
          for (const srv of activeServers) {
            const cleanBase = srv.baseUrl.endsWith('/') ? srv.baseUrl : (srv.baseUrl + '/');
            if (url.startsWith(cleanBase)) {
              serverKey = srv.chave;
              relPath = url.slice(cleanBase.length).split('?')[0].replace(/\.jws$/, '.json');
              break;
            }
          }

          if (!serverKey || !relPath) {
            for (const srv of activeServers) {
              try {
                const uObj = new URL(srv.baseUrl);
                if (url.includes(uObj.host) && url.includes(uObj.pathname)) {
                  serverKey = srv.chave;
                  const idx = url.indexOf(uObj.pathname);
                  relPath = url.slice(idx + uObj.pathname.length).split('?')[0].replace(/\.jws$/, '.json');
                  break;
                }
              } catch {}
            }
          }

          // Fallback retrocompatível para nós legados
          if (!serverKey || !relPath) {
            if (url.includes('resultados-hmg.tse.jus.br')) {
              serverKey = getOriginServer()?.chave || 'HMG';
              const m = url.match(/\/(?:teste|simulado)\/((?:ele\d{4}|tdtot\d{4}|comum)\/.*?\.(?:json|jws))/);
              if (m) relPath = m[1].replace(/\.jws$/, '.json');
            } else if (url.includes('resultados-sim.tse.jus.br')) {
              serverKey = getReplicaServers()[0]?.chave || 'SIM';
              const m = url.match(/\/(?:simulado\/teste|simulado\/simulado|simulado)\/((?:ele\d{4}|tdtot\d{4}|comum)\/.*?\.(?:json|jws))/);
              if (m) relPath = m[1].replace(/\.jws$/, '.json');
            }
          }

          if (serverKey && relPath) {
            if (!trackedFiles.has(relPath)) {
              trackedFiles.add(relPath);
              console.log(`${YELLOW}🔍 [Auto-descoberta via Navegador]${RESET} Novo arquivo monitorado: ${getFilename(relPath)}`);
            }

            const requestId = data.params.requestId;
            const bodyCmdId = reqCounter++;
            const headersWithIp = { ...(resp.headers || {}) };
            if (resp.remoteIPAddress) headersWithIp['x-server-ip'] = resp.remoteIPAddress;
            const isFromCache = Boolean(resp.fromDiskCache || resp.fromPrefetchCache || resp.fromServiceWorker);
            const callTimeUnix = Date.now();
            const callTimeIso = new Date(callTimeUnix).toISOString();

            let cdpReqHeaders = {};
            if (resp.requestHeaders && Object.keys(resp.requestHeaders).length > 0) {
              for (const [k, v] of Object.entries(resp.requestHeaders)) {
                cdpReqHeaders[k.toLowerCase()] = v;
              }
            } else if (cdpRequestHeadersMap.has(requestId)) {
              cdpReqHeaders = cdpRequestHeadersMap.get(requestId);
            } else {
              cdpReqHeaders = {
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/CDP',
                'accept': 'application/json, text/plain, */*'
              };
            }

            pendingRequests.set(bodyCmdId, {
              serverKey,
              relPath,
              headers: headersWithIp,
              requestHeaders: cdpReqHeaders,
              isFromCache,
              callTimeUnix,
              callTimeIso
            });

            ws.send(JSON.stringify({
              id: bodyCmdId,
              method: 'Network.getResponseBody',
              params: { requestId }
            }));
          }
        }
      } else if (data.id && pendingRequests.has(data.id)) {
        const reqInfo = pendingRequests.get(data.id);
        pendingRequests.delete(data.id);

        if (data.result && data.result.body) {
          const bodyText = data.result.base64Encoded
            ? Buffer.from(data.result.body, 'base64').toString('utf8')
            : data.result.body;

          const payload = decodeJwsOrJson(bodyText);
          if (payload) {
            processVersion(
              reqInfo.serverKey,
              reqInfo.relPath,
              payload,
              bodyText,
              reqInfo.isFromCache ? 'Browser_CDP_Cache' : 'Browser_CDP',
              reqInfo.headers,
              {
                callTimeUnix: reqInfo.callTimeUnix,
                callTimeIso: reqInfo.callTimeIso,
                skipRegression: reqInfo.isFromCache,
                requestHeaders: reqInfo.requestHeaders,
                serverIp: reqInfo.headers?.['x-server-ip'] || null
              }
            );
          }
        }
      }
    } catch {}
  };

  ws.onclose = () => {
    activeWsConnections.delete(tab.id);
  };
}

async function syncTabs() {
  try {
    const tabs = await fetch(`http://127.0.0.1:${CDP_PORT}/json/list`).then(r => r.json());
    const targetTabs = tabs.filter(t => t.type === 'page' && t.url && (t.url.includes('resultados-hmg') || t.url.includes('resultados-sim')));
    for (const t of targetTabs) {
      if (!activeWsConnections.has(t.id)) {
        await attachTabObserver(t);
      }
    }
  } catch {}
}

// -------------------------------------------------------------
// SERVIDOR WEB DO DASHBOARD COM DUPLO CRITÉRIO (DG/HG vs IDG)
// -------------------------------------------------------------
const DASHBOARD_HTML = `<!DOCTYPE html>
<html lang="pt-BR">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>TDTot v1.0 - Auditoria Dupla de Propagação: HMG vs SIM</title>
  <script>
    (function() {
      try {
        if (localStorage.getItem('tdtot_theme') === 'light') {
          document.documentElement.setAttribute('data-theme', 'light');
        }
      } catch(e) {}
    })();
  </script>
  <style>
    :root {
      --bg: #0b132b;
      --card-bg: #1c2541;
      --border: #3a506b;
      --text: #f8fafc;
      --text-muted: #94a3b8;
      --accent-green: #10b981;
      --accent-red: #ef4444;
      --accent-blue: #38bdf8;
      --accent-yellow: #f59e0b;
      --accent-purple: #a855f7;
      --accent-pink: #ec4899;
    }
    html[data-theme="light"] {
      --bg: #f1f5f9;
      --card-bg: #ffffff;
      --border: #cbd5e1;
      --text: #0f172a;
      --text-muted: #475569;
      --accent-green: #059669;
      --accent-red: #dc2626;
      --accent-blue: #0284c7;
      --accent-yellow: #d97706;
      --accent-purple: #7c3aed;
      --accent-pink: #db2777;
    }
    html[data-theme="light"] body { background: var(--bg); color: var(--text); }
    html[data-theme="light"] h1, html[data-theme="light"] h2, html[data-theme="light"] h3, html[data-theme="light"] .card-title { color: #0f172a !important; }
    html[data-theme="light"] .filter-card { background: #ffffff !important; border-color: #cbd5e1 !important; }
    html[data-theme="light"] .filter-header { background: #f8fafc !important; color: #0f172a !important; }
    html[data-theme="light"] .filter-header:hover { background: #f1f5f9 !important; }
    html[data-theme="light"] .filter-body { border-top-color: #cbd5e1 !important; }
    html[data-theme="light"] .filter-panel { background: #f8fafc !important; border-color: #cbd5e1 !important; }
    html[data-theme="light"] .filter-label { color: #475569 !important; }
    html[data-theme="light"] .filter-select, html[data-theme="light"] .search-input { background: #ffffff !important; color: #0f172a !important; border-color: #cbd5e1 !important; }
    html[data-theme="light"] .filter-select:focus, html[data-theme="light"] .search-input:focus { border-color: #0284c7 !important; }
    html[data-theme="light"] .filter-select option { background: #ffffff; color: #0f172a; }
    html[data-theme="light"] .btn-outline { color: #334155 !important; border-color: #cbd5e1 !important; }
    html[data-theme="light"] .btn-outline:hover { background: #e2e8f0 !important; color: #0f172a !important; }
    html[data-theme="light"] .btn-reset { background: #e2e8f0 !important; color: #1e293b !important; border: 1px solid #cbd5e1 !important; }
    html[data-theme="light"] .btn-reset:hover { background: #cbd5e1 !important; }
    html[data-theme="light"] .btn-copy { background: #e2e8f0 !important; color: #1e293b !important; border-color: #cbd5e1 !important; }
    html[data-theme="light"] .btn-copy:hover { background: #cbd5e1 !important; }
    html[data-theme="light"] th { background: #f8fafc !important; color: #475569 !important; border-bottom: 1px solid #cbd5e1 !important; }
    html[data-theme="light"] th.sortable:hover { background: #e0f2fe !important; color: #0369a1 !important; }
    html[data-theme="light"] td { border-bottom: 1px solid #e2e8f0 !important; color: #1e293b !important; }
    html[data-theme="light"] tr:hover { background: #f8fafc !important; }
    html[data-theme="light"] .menu-content { background: #ffffff !important; border-color: #cbd5e1 !important; box-shadow: 0 10px 25px -5px rgba(0,0,0,0.12) !important; }
    html[data-theme="light"] .menu-content a { color: #1e293b !important; border-bottom: 1px solid #f1f5f9 !important; }
    html[data-theme="light"] .menu-content a:hover { background: #f1f5f9 !important; color: #0284c7 !important; }
    html[data-theme="light"] .modal-content { background: #ffffff !important; border-color: #cbd5e1 !important; color: #0f172a !important; box-shadow: 0 20px 25px -5px rgba(0,0,0,0.15) !important; }
    html[data-theme="light"] .modal-header { border-bottom-color: #e2e8f0 !important; }
    html[data-theme="light"] .modal-header h3 { color: #0f172a !important; }
    html[data-theme="light"] .modal-footer { border-top-color: #e2e8f0 !important; background: #f8fafc !important; }
    html[data-theme="light"] .log-feed { background: #f8fafc !important; border-color: #cbd5e1 !important; color: #334155 !important; }
    html[data-theme="light"] .log-row { border-bottom-color: #e2e8f0 !important; }
    html[data-theme="light"] #headerRodadaName { color: #0f172a !important; }
    html[data-theme="light"] #colSelectorDropdown { background: #ffffff !important; border-color: #cbd5e1 !important; box-shadow: 0 12px 32px rgba(0,0,0,0.15) !important; color: #0f172a !important; }
    html[data-theme="light"] #colSelectorDropdown strong { color: #0f172a !important; }
    html[data-theme="light"] .code,
    html[data-theme="light"] div[style*="color:#f8fafc"],
    html[data-theme="light"] div[style*="color: #f8fafc"],
    html[data-theme="light"] strong[style*="color:#f8fafc"],
    html[data-theme="light"] strong[style*="color: #f8fafc"],
    html[data-theme="light"] span[style*="color:#f8fafc"],
    html[data-theme="light"] span[style*="color: #f8fafc"],
    html[data-theme="light"] .filter-header strong,
    html[data-theme="light"] .filter-label span {
      color: #0f172a !important;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; }
    body { background: var(--bg); color: var(--text); padding: 24px; }
    header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 24px; padding-bottom: 16px; border-bottom: 1px solid var(--border); }
    h1 { font-size: 1.5rem; display: flex; align-items: center; gap: 10px; }
    .status-badge { padding: 8px 16px; border-radius: 9999px; font-weight: 700; font-size: 0.95rem; text-transform: uppercase; letter-spacing: 0.05em; display: inline-flex; align-items: center; gap: 8px; cursor: pointer; transition: all 0.2s cubic-bezier(0.4, 0, 0.2, 1); user-select: none; }
    .status-badge:hover { transform: scale(1.04); filter: brightness(1.15); }
    .status-badge:active { transform: scale(0.98); }
    .status-ok { background: rgba(16, 185, 129, 0.2); color: var(--accent-green); border: 1px solid var(--accent-green); }
    .status-danger { background: rgba(239, 68, 68, 0.25); color: var(--accent-red); border: 2px solid var(--accent-red); animation: pulse 1.2s infinite; box-shadow: 0 0 14px rgba(239, 68, 68, 0.45); }
    .status-danger:hover { box-shadow: 0 0 22px rgba(239, 68, 68, 0.85); }
    @keyframes pulse { 0%, 100% { transform: scale(1); } 50% { transform: scale(1.04); } }
    
    .actions-bar { display: flex; gap: 12px; margin-bottom: 24px; flex-wrap: wrap; }
    .btn { background: #7c3aed; color: white; border: none; padding: 9px 16px; border-radius: 8px; font-weight: 600; cursor: pointer; text-decoration: none; display: inline-flex; align-items: center; gap: 8px; font-size: 0.88rem; transition: background 0.2s; }
    .btn:hover { background: #6d28d9; }
    .btn-danger { background: #dc2626; }
    .btn-danger:hover { background: #b91c1c; }
    .btn-outline { background: transparent; border: 1px solid var(--border); color: var(--text); }
    .btn-outline:hover { background: rgba(255,255,255,0.05); }

    /* MENU DROPDOWN DE EXPORTAÇÃO */
    .menu-dropdown { position: relative; display: inline-block; }
    .menu-content { display: none; position: absolute; right: 0; top: 100%; margin-top: 6px; background: #1e293b; min-width: 250px; border: 1px solid var(--border); border-radius: 8px; box-shadow: 0 10px 25px -5px rgba(0,0,0,0.5); z-index: 100; overflow: hidden; }
    .menu-content a { color: var(--text); padding: 10px 14px; text-decoration: none; display: flex; align-items: center; gap: 8px; font-size: 0.84rem; border-bottom: 1px solid rgba(255,255,255,0.05); transition: background 0.15s; }
    .menu-content a:hover { background: rgba(56, 189, 248, 0.15); color: #fff; }
    .menu-content a:last-child { border-bottom: none; }
    .menu-dropdown.show .menu-content { display: block; }

    /* FILTRO COLAPSÁVEL */
    .filter-card { background: var(--card-bg); border: 1px solid var(--border); border-radius: 12px; margin-bottom: 16px; overflow: hidden; }
    .filter-header { padding: 12px 18px; display: flex; justify-content: space-between; align-items: center; cursor: pointer; user-select: none; background: rgba(0,0,0,0.15); }
    .filter-header:hover { background: rgba(0,0,0,0.25); }
    .filter-body { padding: 14px 18px; border-top: 1px solid var(--border); }
    .filter-body.collapsed { display: none; }
    .toggle-icon { font-size: 0.8rem; transition: transform 0.2s; }
    .toggle-icon.collapsed { transform: rotate(-90deg); }

    .stats-grid { 
      display: grid; 
      grid-template-columns: 0.72fr 0.72fr 1.05fr 2.55fr 2.35fr; 
      gap: 10px; 
      margin-bottom: 16px; 
      align-items: stretch;
    }
    @media (max-width: 1400px) {
      .stats-grid {
        grid-template-columns: 0.9fr 0.9fr 1.2fr 2.6fr;
      }
      .stat-card-ttl {
        grid-column: span 4;
      }
    }
    @media (max-width: 1050px) {
      .stats-grid {
        grid-template-columns: 1fr 1fr;
      }
      .stat-card-cache, .stat-card-ttl {
        grid-column: span 2;
      }
    }
    @media (max-width: 650px) {
      .stats-grid {
        grid-template-columns: 1fr;
      }
      .stat-card-cache, .stat-card-ttl {
        grid-column: span 1;
      }
    }
    .stat-card { background: var(--card-bg); border: 1px solid var(--border); border-radius: 12px; padding: 16px; }
    .stat-label { color: var(--text-muted); font-size: 0.80rem; text-transform: uppercase; margin-bottom: 6px; }
    .stat-value { font-size: 1.7rem; font-weight: bold; }
    
    .card { background: var(--card-bg); border: 1px solid var(--border); border-radius: 12px; padding: 20px; margin-bottom: 24px; }
    .card-title { font-size: 1.15rem; font-weight: 600; margin-bottom: 16px; display: flex; justify-content: space-between; align-items: center; }
    
    /* FILTROS E USABILIDADE */
    .filter-panel { background: rgba(0,0,0,0.25); border: 1px solid var(--border); border-radius: 10px; padding: 16px; margin-bottom: 20px; display: flex; flex-wrap: wrap; gap: 14px; align-items: center; }
    .filter-group { display: flex; flex-direction: column; gap: 4px; }
    .filter-label { font-size: 0.75rem; text-transform: uppercase; color: var(--text-muted); font-weight: 700; letter-spacing: 0.05em; }
    .filter-select, .search-input { background: #0f172a; color: #f8fafc; border: 1px solid var(--border); border-radius: 6px; padding: 8px 12px; font-size: 0.86rem; outline: none; transition: border-color 0.2s; }
    .filter-select:focus, .search-input:focus { border-color: var(--accent-blue); }
    .search-input { min-width: 260px; }
    .btn-reset { background: #334155; color: #f8fafc; border: none; padding: 8px 14px; border-radius: 6px; cursor: pointer; font-size: 0.82rem; font-weight: 600; margin-top: 18px; }
    .btn-reset:hover { background: #475569; }

    table { width: 100%; border-collapse: collapse; text-align: left; font-size: 0.85rem; }
    th { color: var(--text-muted); padding: 12px 10px; border-bottom: 1px solid var(--border); font-weight: 600; background: rgba(0,0,0,0.2); user-select: none; }
    th.sortable { cursor: pointer; transition: all 0.15s; }
    th.sortable:hover { background: rgba(56, 189, 248, 0.15); color: #fff; }
    .sort-icon { font-size: 0.72rem; margin-left: 4px; opacity: 0.5; }
    th.sorted-asc .sort-icon, th.sorted-desc .sort-icon { opacity: 1; color: var(--accent-blue); font-weight: bold; }
    td { padding: 12px 10px; border-bottom: 1px solid rgba(255,255,255,0.05); vertical-align: middle; }
    tr:hover { background: rgba(255,255,255,0.025); }
    
    .badge { padding: 4px 8px; border-radius: 6px; font-size: 0.74rem; font-weight: 700; display: inline-block; white-space: nowrap; }
    .badge-sync { background: #065f46; color: #a7f3d0; }
    .badge-lag { background: #78350f; color: #fde68a; border: 1px solid #d97706; }
    .badge-ahead { background: #1e3a8a; color: #bfdbfe; border: 1px solid #3b82f6; }
    .badge-inconsistency { background: #831843; color: #fbcfe8; border: 1px solid #db2777; font-weight: bold; }
    .badge-danger { background: #991b1b; color: #fecaca; animation: blink 1s infinite; }
    @keyframes blink { 0%, 100% { opacity: 1; } 50% { opacity: 0.4; } }

    .tag-pill { display: inline-block; padding: 2px 7px; border-radius: 4px; font-size: 0.72rem; font-weight: 600; margin-right: 4px; }
    .tag-eleicao { background: rgba(56, 189, 248, 0.15); color: #38bdf8; border: 1px solid rgba(56, 189, 248, 0.3); }
    .tag-uf { background: rgba(168, 85, 247, 0.15); color: #c084fc; border: 1px solid rgba(168, 85, 247, 0.3); font-weight: bold; }
    .tag-tipo { background: rgba(16, 185, 129, 0.15); color: #34d399; border: 1px solid rgba(16, 185, 129, 0.3); }
    .tag-pleito { background: rgba(236, 72, 153, 0.15); color: #f472b6; border: 1px solid rgba(236, 72, 153, 0.35); font-weight: 700; }
    .tag-cargo { background: rgba(245, 158, 11, 0.15); color: #fbbf24; border: 1px solid rgba(245, 158, 11, 0.3); }
    
    .code { font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace; }
    .tag-hmg-title { color: var(--accent-purple); font-weight: 700; }
    .tag-sim-title { color: var(--accent-blue); font-weight: 700; }

    /* HINT / TOOLTIP DE HEADERS */
    .header-hint { display: inline-flex; align-items: center; gap: 4px; border-bottom: 1px dashed rgba(255,255,255,0.3); cursor: help; }
    .hint-icon { display: inline-flex; align-items: center; justify-content: center; width: 14px; height: 14px; font-size: 0.65rem; border-radius: 50%; background: #334155; color: #38bdf8; font-weight: 700; margin-left: 2px; }
    .hint-desc { font-size: 0.70rem; color: #94a3b8; font-style: italic; margin-top: 2px; }
    .tag-ip { background: rgba(56, 189, 248, 0.12); color: #38bdf8; border: 1px solid rgba(56, 189, 248, 0.3); font-family: monospace; font-size: 0.68rem; padding: 1px 5px; border-radius: 4px; }
    .log-feed { max-height: 280px; overflow-y: auto; font-family: monospace; font-size: 0.8rem; line-height: 1.6; }
    .log-item { padding: 4px 0; border-bottom: 1px solid rgba(255,255,255,0.04); display: flex; gap: 12px; }
    .log-time { color: var(--text-muted); min-width: 75px; }
    .log-src { min-width: 140px; font-weight: 600; }
    .log-file { min-width: 260px; }
    .log-desc { flex: 1; }
    
    .url-link { color: #38bdf8; text-decoration: none; display: inline-flex; align-items: center; gap: 4px; font-size: 0.76rem; padding: 3px 8px; background: rgba(56, 189, 248, 0.12); border-radius: 5px; border: 1px solid rgba(56, 189, 248, 0.3); font-weight: 600; transition: all 0.2s; }
    .url-link:hover { background: rgba(56, 189, 248, 0.25); color: #fff; transform: translateY(-1px); }
    .url-link-hmg { color: #c084fc; background: rgba(168, 85, 247, 0.12); border-color: rgba(168, 85, 247, 0.3); }
    .url-link-hmg:hover { background: rgba(168, 85, 247, 0.25); color: #fff; }
    .btn-copy { cursor: pointer; border: 1px solid #475569; background: #1e293b; color: #cbd5e1; border-radius: 5px; padding: 3px 8px; font-size: 0.74rem; font-weight: 500; transition: all 0.15s; }
    .btn-copy:hover { background: #334155; color: #fff; border-color: #64748b; }

    /* BARRA DE PROGRESSO GLOBAL SUPERIOR */
    #topProgressBar {
      position: fixed;
      top: 0;
      left: 0;
      height: 3px;
      width: 0%;
      background: linear-gradient(90deg, #38bdf8, #818cf8, #a855f7, #10b981);
      z-index: 100000;
      opacity: 0;
      transition: width 0.25s ease, opacity 0.3s ease;
      box-shadow: 0 0 10px rgba(56, 189, 248, 0.85);
      pointer-events: none;
    }
    #topProgressBar.active {
      opacity: 1;
    }
    #topProgressBar.indeterminate {
      opacity: 1;
      width: 100% !important;
      background: linear-gradient(90deg, transparent, #38bdf8, #a855f7, #10b981, transparent);
      background-size: 200% 100%;
      animation: indeterminateBarAnim 1.1s cubic-bezier(0.4, 0, 0.2, 1) infinite;
    }
    @keyframes indeterminateBarAnim {
      0% { background-position: 200% 0; }
      100% { background-position: -200% 0; }
    }

    /* BADGE COM SPINNER DE ATUALIZAÇÃO */
    .updating-badge {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      font-size: 0.72rem;
      font-weight: 600;
      color: var(--accent-blue);
      background: rgba(56, 189, 248, 0.12);
      border: 1px solid rgba(56, 189, 248, 0.35);
      padding: 2px 9px;
      border-radius: 9999px;
      letter-spacing: 0.02em;
      transition: opacity 0.2s ease, transform 0.2s ease;
    }
    .spinner-icon {
      width: 11px;
      height: 11px;
      border: 2px solid rgba(56, 189, 248, 0.25);
      border-top-color: var(--accent-blue);
      border-radius: 50%;
      animation: spinIndicator 0.65s linear infinite;
      display: inline-block;
      flex-shrink: 0;
    }
    @keyframes spinIndicator {
      to { transform: rotate(360deg); }
    }
    .table-updating {
      opacity: 0.65;
      pointer-events: none;
      transition: opacity 0.15s ease;
    }
  </style>
</head>
<body>
  <!-- BARRA DE PROGRESSO SUPERIOR -->
  <div id="topProgressBar"></div>
  <header style="margin-bottom: 14px; padding-bottom: 12px; display: flex; flex-direction: column; gap: 10px; border-bottom: 1px solid var(--border);">
    <!-- LINHA 1: Título, Status, Eleições, Servidores e Botões de Exportação à Direita -->
    <div style="display: flex; justify-content: space-between; align-items: center; gap: 12px; flex-wrap: nowrap;">
      <div style="display: flex; align-items: center; gap: 10px; flex-wrap: wrap;">
        <h1 style="font-size: 1.25rem; font-weight: 700; margin: 0; display: flex; align-items: center; gap: 8px;">🗳️ Auditoria Dupla: HMG ➔ SIM <span class="status-badge status-ok" style="font-size: 0.72rem; padding: 2px 7px; border-radius: 6px; font-weight: 700; letter-spacing: 0.5px;">v1.0</span></h1>
        
        <!-- STATUS BADGE (30px) -->
        <span id="statusBadge" class="status-badge status-ok" onclick="openRegressoesModal()" title="Clique para abrir a auditoria forense detalhada de todas as regressões detectadas" style="height: 30px; box-sizing: border-box; padding: 0 12px; font-size: 0.78rem; display: inline-flex; align-items: center; gap: 6px; cursor: pointer;">
          <span>●</span> <span id="statusText">CARREGANDO...</span>
        </span>

        <!-- SELETOR ELEIÇÕES (30px) -->
        <div style="height: 30px; box-sizing: border-box; display: inline-flex; align-items: center; gap: 6px; background: rgba(168, 85, 247, 0.12); border: 1px solid rgba(168, 85, 247, 0.35); padding: 0 10px; border-radius: 8px;">
          <span style="font-size: 0.76rem; color: #c084fc; font-weight: 700;">🗳️ Eleições:</span>
          <span id="headerEleicoesCount" style="font-size: 0.80rem; font-weight: 600; color: #f8fafc;">Calculando...</span>
          <button onclick="openEleicoesModal()" class="btn-copy" style="padding: 1px 7px; font-size: 0.72rem; height: 22px; display: inline-flex; align-items: center;" title="Configurar quais eleições monitorar">⚙️ Escolher</button>
        </div>

        <!-- SELETOR SERVIDORES (30px) -->
        <div style="height: 30px; box-sizing: border-box; display: inline-flex; align-items: center; gap: 6px; background: rgba(59, 130, 246, 0.12); border: 1px solid rgba(59, 130, 246, 0.35); padding: 0 10px; border-radius: 8px;">
          <span style="font-size: 0.76rem; color: #60a5fa; font-weight: 700;">🖥️ Servidores:</span>
          <span id="headerServidoresCount" style="font-size: 0.80rem; font-weight: 600; color: #f8fafc;">2 ativos</span>
          <button onclick="openServidoresModal()" class="btn-copy" style="padding: 1px 7px; font-size: 0.72rem; height: 22px; display: inline-flex; align-items: center;" title="Gerenciar nós e servidores de auditoria">⚙️ Gerenciar</button>
        </div>
      </div>

      <!-- BOTÕES DE EXPORTAÇÃO (Harmonizados na mesma altura de 30px) -->
      <div style="display: flex; align-items: center; gap: 8px; flex-shrink: 0;">
        <button id="themeToggleBtn" onclick="toggleTheme()" class="btn btn-outline" style="height: 30px; box-sizing: border-box; padding: 0 12px; font-size: 0.76rem; font-weight: 600; border-radius: 8px; border: 1px solid var(--border); display: inline-flex; align-items: center; gap: 6px; cursor: pointer; transition: all 0.15s;" title="Alternar entre modo escuro e modo claro">
          ☀️ Modo Claro
        </button>
        <button onclick="openZipModal()" class="btn" style="background: #0284c7; height: 30px; box-sizing: border-box; padding: 0 12px; font-size: 0.76rem; font-weight: 600; border-radius: 8px; border: 1px solid rgba(56, 189, 248, 0.4); display: inline-flex; align-items: center; gap: 6px; transition: all 0.15s;" title="Baixar arquivos capturados em ZIP">
          📦 Baixar Versões (ZIP)
        </button>
        <a href="/report" target="_blank" class="btn btn-danger" style="height: 30px; box-sizing: border-box; padding: 0 12px; font-size: 0.76rem; font-weight: 600; border-radius: 8px; border: 1px solid rgba(239, 68, 68, 0.4); display: inline-flex; align-items: center; gap: 6px; text-decoration: none; transition: all 0.15s;" title="Abrir Dossiê Forense HTML">
          📄 Dossiê HTML
        </a>
        <div class="menu-dropdown" id="exportMenuDropdown">
          <button onclick="toggleMenu()" class="btn btn-outline" style="height: 30px; box-sizing: border-box; padding: 0 12px; font-size: 0.76rem; font-weight: 600; border-radius: 8px; border: 1px solid var(--border); display: inline-flex; align-items: center; gap: 6px; transition: all 0.15s;" title="Menu de exportações e downloads">
            ⚙️ Exportar / Dados ▾
          </button>
          <div class="menu-content" id="exportMenuContent">
            <a href="javascript:void(0)" onclick="openRegressoesModal()">🚨 Inspecionar Regressões da Rodada (Modal)</a>
            <a href="/report" target="_blank">📄 Abrir Dossiê Forense (HTML)</a>
            <a href="/download/db">💾 Baixar Banco SQLite (tdtot_auditoria.db)</a>
            <a href="/download/csv-regressoes">📊 Baixar CSV de Regressões</a>
            <a href="/download/csv-comparativo">📈 Baixar CSV de Propagação</a>
            <a href="javascript:void(0)" onclick="openZipModal()">📦 Baixar Versões Salvas (ZIP)</a>
          </div>
        </div>
      </div>
    </div>

    <!-- LINHA 2: Seletor de Rodada Ativa -->
    <div style="display: flex; align-items: center; gap: 10px;">
      <div style="height: 30px; box-sizing: border-box; display: inline-flex; align-items: center; gap: 6px; background: rgba(56, 189, 248, 0.12); border: 1px solid rgba(56, 189, 248, 0.35); padding: 0 10px; border-radius: 8px;">
        <span style="font-size: 0.76rem; color: #38bdf8; font-weight: 700;">📍 Rodada:</span>
        <span id="headerRodadaName" style="font-size: 0.80rem; font-weight: 600; color: #f8fafc;">Carregando...</span>
        <button onclick="openRodadasModal()" class="btn-copy" style="padding: 1px 7px; font-size: 0.72rem; height: 22px; display: inline-flex; align-items: center; margin-left: 2px;" title="Gerenciar Rodadas e Checkpoints">⚙️ Gerenciar</button>
        <button onclick="quickNewRodada()" class="btn" style="background: #10b981; padding: 0 8px; font-size: 0.72rem; height: 22px; line-height: 1; border-radius: 4px; display: inline-flex; align-items: center; margin-left: 2px;" title="Iniciar nova rodada zerada imediatamente">➕ Nova</button>
      </div>
    </div>
  </header>

  <!-- GRID DE KPIS PROPORCIONAL: 3 MENORES (COMPACTOS) E 2 DENSOS (EXPANDIDOS) -->
  <div class="stats-grid">
    <!-- Card 1: Arquivos Monitorados (Menor) -->
    <div class="stat-card" style="padding: 10px 14px; border-left: 3px solid #64748b; display: flex; flex-direction: column; justify-content: space-between;">
      <div class="stat-label" style="font-size: 0.70rem; margin-bottom: 2px;">Arquivos Monitorados</div>
      <div class="stat-value" id="countFiles" style="font-size: 1.35rem; line-height: 1.2;">0</div>
    </div>

    <!-- Card 2: Exibidos/Filtrados (Menor) -->
    <div class="stat-card" style="padding: 10px 14px; border-left: 3px solid var(--accent-blue); display: flex; flex-direction: column; justify-content: space-between;">
      <div class="stat-label" style="font-size: 0.70rem; margin-bottom: 2px;">Exibidos / Filtrados</div>
      <div class="stat-value" id="countVisible" style="font-size: 1.35rem; line-height: 1.2; color: var(--accent-blue);">0</div>
    </div>

    <!-- Card 3: Regressão Hora (Menor/Médio) -->
    <div class="stat-card" style="padding: 10px 14px; border-left: 3px solid var(--accent-red); display: flex; flex-direction: column; justify-content: space-between;">
      <div class="stat-label" style="font-size: 0.70rem; margin-bottom: 4px;">Regressão Hora</div>
      <div style="display: flex; gap: 12px; align-items: baseline;">
        <div>
          <div style="font-size: 0.65rem; color: var(--text-muted); text-transform: uppercase;">Quantidade</div>
          <div id="countRegTime" style="font-size: 1.3rem; font-weight: 700; line-height: 1.2; color: var(--accent-red);">0</div>
        </div>
        <div style="border-left: 1px solid var(--border); padding-left: 10px;">
          <div style="font-size: 0.65rem; color: var(--text-muted); text-transform: uppercase;">Última Ocorrência</div>
          <div id="lastRegTime" style="font-size: 1.05rem; font-weight: 600; line-height: 1.2; color: #f87171; font-family: monospace;">-</div>
        </div>
      </div>
    </div>

    <!-- Card 4: Cache (SLAs Acumulados - Denso) -->
    <div class="stat-card stat-card-cache" style="padding: 10px 14px; border-left: 3px solid var(--accent-yellow); display: flex; flex-direction: column; justify-content: space-between;">
      <div class="stat-label" style="font-size: 0.70rem; margin-bottom: 4px;">Cache (SLAs Acumulados)</div>
      <div style="display: flex; justify-content: space-between; gap: 6px; align-items: baseline; flex-wrap: nowrap;">
        <div>
          <div style="font-size: 0.65rem; color: var(--text-muted); text-transform: uppercase;" title="Quantidade de arquivos com cache desatualizado neste exato instante">Atrasados</div>
          <div id="countDesync" style="font-size: 1.25rem; font-weight: 700; line-height: 1.2; color: var(--accent-yellow);">0</div>
        </div>
        <div style="border-left: 1px solid var(--border); padding-left: 8px;">
          <div style="font-size: 0.65rem; color: var(--text-muted); text-transform: uppercase;" title="Tempo médio de sincronização dos arquivos">Média</div>
          <div id="cacheAvg" style="font-size: 1.0rem; font-weight: 600; line-height: 1.2; color: #fbbf24;">0m 00s</div>
        </div>
        <div style="border-left: 1px solid var(--border); padding-left: 8px;">
          <div style="font-size: 0.65rem; color: var(--text-muted); text-transform: uppercase;" title="90% dos arquivos sincronizam neste tempo ou menos">P90</div>
          <div id="cacheP90" style="font-size: 1.0rem; font-weight: 600; line-height: 1.2; color: #fb923c;">0m 00s</div>
        </div>
        <div style="border-left: 1px solid var(--border); padding-left: 8px;">
          <div style="font-size: 0.65rem; color: var(--text-muted); text-transform: uppercase;" title="95% dos arquivos sincronizam neste tempo ou menos">P95</div>
          <div id="cacheP95" style="font-size: 1.0rem; font-weight: 600; line-height: 1.2; color: #f87171;">0m 00s</div>
        </div>
        <div style="border-left: 1px solid var(--border); padding-left: 8px;">
          <div style="font-size: 0.65rem; color: var(--text-muted); text-transform: uppercase;" title="99% dos arquivos sincronizam neste tempo ou menos">P99</div>
          <div id="cacheP99" style="font-size: 1.0rem; font-weight: 600; line-height: 1.2; color: #ef4444;">0m 00s</div>
        </div>
        <div style="border-left: 1px solid var(--border); padding-left: 8px;">
          <div style="font-size: 0.65rem; color: var(--text-muted); text-transform: uppercase;" title="Tempo máximo de sincronização (100% dos arquivos)">P100</div>
          <div id="cacheP100" style="font-size: 1.0rem; font-weight: 600; line-height: 1.2; color: #b91c1c;">0m 00s</div>
        </div>
      </div>
    </div>

    <!-- Card 5: KPI Atributos de Cache & TTL (Denso) -->
    <div class="stat-card stat-card-ttl" style="padding: 10px 14px; border-left: 3px solid #38bdf8; display: flex; flex-direction: column; justify-content: space-between;">
      <div class="stat-label" style="font-size: 0.70rem; margin-bottom: 4px; display: flex; justify-content: space-between;">
        <span>🌐 Atributos de Cache & TTL (HMG vs SIM)</span>
        <span style="color: #38bdf8; font-weight: 700;">HTTP/CDN</span>
      </div>
      <div style="display: flex; justify-content: space-between; gap: 10px; align-items: baseline; flex-wrap: nowrap;">
        <div>
          <div style="font-size: 0.65rem; color: var(--text-muted); text-transform: uppercase;" title="TTL configurado ou remanescente no SIM (Akamai CDN)">SIM Edge TTL</div>
          <div style="display: flex; align-items: baseline; gap: 4px;">
            <span id="kpiSimTtl" style="font-size: 1.25rem; font-weight: 700; line-height: 1.2; color: #38bdf8;">~60s</span>
            <span id="kpiSimTtlRange" style="font-size: 0.72rem; color: #94a3b8;">(0-60s)</span>
          </div>
        </div>
        <div style="border-left: 1px solid var(--border); padding-left: 10px;">
          <div style="font-size: 0.65rem; color: var(--text-muted); text-transform: uppercase;" title="Percentual de respostas servidas pelo cache da CDN">CDN Hit Rate</div>
          <div id="kpiCdnHits" style="font-size: 1.25rem; font-weight: 700; line-height: 1.2; color: #10b981;">100%</div>
        </div>
        <div style="border-left: 1px solid var(--border); padding-left: 10px;">
          <div style="font-size: 0.65rem; color: var(--text-muted); text-transform: uppercase;" title="Header Cache-Control da Origem HMG">HMG Origem</div>
          <div id="kpiHmgHeader" style="font-size: 0.82rem; font-weight: 600; line-height: 1.2; color: #c084fc; font-family: monospace;" title="Apache não envia Cache-Control (depende de Last-Modified/ETag)">Sem Header</div>
        </div>
      </div>
    </div>
  </div>

  <div class="filter-card">
    <div class="filter-header" onclick="toggleFilterPanel()">
      <div style="display: flex; align-items: center; gap: 8px;">
        <span style="font-size: 0.95rem; font-weight: 600;">🔍 Filtros & Busca Rápida</span>
        <span id="activeFilterBadge" style="font-size: 0.72rem; background: rgba(56, 189, 248, 0.2); color: var(--accent-blue); padding: 2px 8px; border-radius: 9999px; display: none;">Filtros Ativos</span>
        <span id="filterUpdatingBadge" class="updating-badge" style="display: none;">
          <span class="spinner-icon"></span>
          <span>Filtrando...</span>
        </span>
      </div>
      <span class="toggle-icon" id="filterToggleIcon">▾ Recolher Filtros</span>
    </div>

    <div class="filter-body" id="filterBody">
      <div class="filter-panel" style="margin-bottom: 0; padding: 0; border: none; background: transparent;">
      <div class="filter-group">
        <label class="filter-label">Busca Textual</label>
        <input type="text" id="searchInput" class="search-input" placeholder="Buscar por arquivo, município, zona, seção..." oninput="applyFilters()">
      </div>

      <div class="filter-group">
        <label class="filter-label">Pleito</label>
        <select id="filterPleito" class="filter-select" onchange="applyFilters()">
          <option value="">Todos os Pleitos</option>
        </select>
      </div>

      <div class="filter-group">
        <label class="filter-label">Eleição</label>
        <select id="filterEleicao" class="filter-select" onchange="applyFilters()">
          <option value="">Todas as Eleições</option>
          <option value="21270">21270 - Federal 1º T</option>
          <option value="21272">21272 - Estadual 1º T</option>
          <option value="Comum">Comum (Geral)</option>
        </select>
      </div>

      <div class="filter-group">
        <label class="filter-label">UF</label>
        <select id="filterUf" class="filter-select" onchange="applyFilters()">
          <option value="">Todas as UFs</option>
          <option value="BR">BR (Brasil Geral)</option>
          <option value="AC">AC</option><option value="AL">AL</option><option value="AM">AM</option><option value="AP">AP</option>
          <option value="BA">BA</option><option value="CE">CE</option><option value="DF">DF</option><option value="ES">ES</option>
          <option value="GO">GO</option><option value="MA">MA</option><option value="MG">MG</option><option value="MS">MS</option>
          <option value="MT">MT</option><option value="PA">PA</option><option value="PB">PB</option><option value="PE">PE</option>
          <option value="PI">PI</option><option value="PR">PR</option><option value="RJ">RJ</option><option value="RN">RN</option>
          <option value="RO">RO</option><option value="RR">RR</option><option value="RS">RS</option><option value="SC">SC</option>
          <option value="SE">SE</option><option value="SP">SP</option><option value="TO">TO</option><option value="ZZ">ZZ (Exterior)</option>
        </select>
      </div>

      <div class="filter-group">
        <label class="filter-label">Tipo de Arquivo</label>
        <select id="filterTipo" class="filter-select" onchange="applyFilters()">
          <option value="">Todos os Tipos</option>
          <option value="Totalização">Totalização / Resultados (-u.json)</option>
          <option value="Abrangência">Abrangência / Resumo Geral (-ab.json)</option>
          <option value="Configuração de Seção">Configuração de Seção (-cs.json)</option>
          <option value="Configuração">Configurações Gerais (-cm / -c.json)</option>
        </select>
      </div>

      <div class="filter-group">
        <label class="filter-label">Cargo</label>
        <select id="filterCargo" class="filter-select" onchange="applyFilters()">
          <option value="">Todos os Cargos</option>
          <option value="Presidente">Presidente</option>
          <option value="Governador">Governador</option>
          <option value="Senador">Senador</option>
          <option value="Dep. Federal">Deputado Federal</option>
          <option value="Dep. Estadual">Deputado Estadual</option>
          <option value="Dep. Distrital">Deputado Distrital</option>
        </select>
      </div>

      <div class="filter-group">
        <label class="filter-label">Critério de Auditoria</label>
        <select id="filterStatus" class="filter-select" onchange="applyFilters()">
          <option value="">Todos os Status</option>
          <option value="REG_ALL">🚨 Qualquer Regressão Detectada</option>
          <option value="REG_TIME">🚨 Regressão de Geração (DG/HG)</option>
          <option value="REG_TOT">🚨 Regressão de Totalização (DT/HT)</option>
          <option value="REG_ST">🚨 Regressão de Seções (ST)</option>
          <option value="ATRASADO">⏳ Apenas Cache Atrasado</option>
          <option value="SINCRONIZADO">✅ Apenas Sincronizados</option>
          <option value="SIM_TTL_LOW">⚡ SIM TTL Baixo (≤ 15s)</option>
        </select>
      </div>

      <button class="btn-reset" onclick="resetFilters()" style="margin-top: 18px;">↺ Limpar</button>
      </div>
    </div>
  </div>

  <div class="card" style="padding: 14px 18px; position: relative;">
    <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 12px; flex-wrap: wrap; gap: 8px;">
      <div style="display: flex; align-items: center; gap: 8px;">
        <strong style="font-size: 0.95rem; color: #f8fafc;">📊 Matriz Comparativa de Arquivos</strong>
        <span id="tableCountBadge" style="font-size: 0.75rem; background: rgba(255,255,255,0.06); color: var(--text-muted); padding: 2px 8px; border-radius: 6px; border: 1px solid rgba(255,255,255,0.08);">(...)</span>
        <span id="tableUpdatingBadge" class="updating-badge" style="display: none;">
          <span class="spinner-icon"></span>
          <span>Atualizando dados...</span>
        </span>
      </div>
      <div style="display: flex; gap: 8px; align-items: center; position: relative;">
        <button id="btnColSelector" onclick="toggleColumnModal(event)" class="btn-export" style="height: 30px; padding: 0 12px; font-size: 0.80rem; display: flex; align-items: center; gap: 6px; border-color: rgba(56,189,248,0.4); color: #38bdf8; cursor: pointer;">
          <span>⚙️ Colunas (<span id="visibleColsCount">8</span>/<span id="totalColsCount">11</span>)</span>
          <span style="font-size: 0.70rem;">▾</span>
        </button>

        <!-- Popover / Dropdown de Seleção de Colunas -->
        <div id="colSelectorDropdown" style="display:none; position:absolute; right:0; top:36px; z-index:9999; background:#0f172a; border:1px solid rgba(255,255,255,0.15); box-shadow:0 12px 32px rgba(0,0,0,0.7); border-radius:8px; width:340px; padding:14px; font-size:0.82rem; text-align:left;">
          <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:8px; border-bottom:1px solid rgba(255,255,255,0.08); padding-bottom:8px;">
            <strong style="color:#f8fafc; font-size:0.88rem;">⚙️ Configurar Colunas</strong>
            <div style="display:flex; gap:5px;">
              <button onclick="resetDefaultColumns()" class="btn-copy" style="font-size:0.68rem; padding:2px 6px;">Padrão</button>
              <button onclick="toggleAllColumns(true)" class="btn-copy" style="font-size:0.68rem; padding:2px 6px;">Todas</button>
              <button onclick="toggleColumnModal(event)" style="background:transparent; border:none; color:var(--text-muted); cursor:pointer; font-size:0.9rem; margin-left:4px;">✕</button>
            </div>
          </div>
          <div style="color:var(--text-muted); font-size:0.72rem; margin-bottom:10px;">
            Ative ou desative as colunas exibidas na tabela. A preferência é salva automaticamente no navegador.
          </div>
          <div id="colCheckboxesList" style="display:flex; flex-direction:column; gap:6px; max-height:360px; overflow-y:auto; padding-right:4px;">
          </div>
        </div>
      </div>
    </div>

    <table>
      <thead id="compareTableHead">
        <tr>
          <th data-col="classificacao" class="sortable" onclick="setSort('classificacao')">Classificação <span id="sort_classificacao" class="sort-icon">⇅</span></th>
          <th data-col="arquivo" class="sortable" onclick="setSort('arquivo')">Caminho do Arquivo & Ações <span id="sort_arquivo" class="sort-icon">⇅</span></th>
          <th data-col="origin_time" class="sortable" onclick="setSort('hmg_time')"><span id="thOriginTitle" class="tag-hmg-title">FONTE (HMG)</span><br>Geração (DG/HG) <span id="sort_hmg_time" class="sort-icon">⇅</span></th>
          <th data-col="replica_time" class="sortable" onclick="setSort('sim_time')"><span id="thReplicaTitle" class="tag-sim-title">CACHE (SIM)</span><br>Geração (DG/HG) <span id="sort_sim_time" class="sort-icon">⇅</span></th>
          <th data-col="delay_time" class="sortable" onclick="setSort('delay_time')" title="Diferença entre a data/hora de geração da FONTE e do CACHE em minutos e segundos">Δ Tempo DG/HG <span id="sort_delay_time" class="sort-icon">⇅</span></th>
          <th data-col="sync_sla" class="sortable" onclick="setSort('sync_sla')" title="Tempo real decorrido entre a detecção do DG/HG na Origem até sua chegada no Cache">SLA Sync (DG/HG) <span id="sort_sync_sla" class="sort-icon">⇅</span></th>
          <th data-col="cache_ttl" class="sortable" onclick="setSort('cache_ttl')" title="Comparativo de Headers HTTP de Cache: TTL / max-age, Cache-Control, CDN status e ETag">⚡ Cache & TTL <span id="sort_cache_ttl" class="sort-icon">⇅</span></th>
          <th data-col="status" class="sortable" onclick="setSort('status')">Integridade <span id="sort_status" class="sort-icon">⇅</span></th>
          <th data-col="origin_tot" class="sortable" onclick="setSort('origin_tot')" title="Data e Hora da Totalização/fechamento apurada na FONTE (DT/HT)"><span id="thOriginTotTitle" class="tag-hmg-title">FONTE (HMG)</span><br>Totalização (DT/HT) <span id="sort_origin_tot" class="sort-icon">⇅</span></th>
          <th data-col="replica_tot" class="sortable" onclick="setSort('replica_tot')" title="Data e Hora da Totalização/fechamento apurada no CACHE (DT/HT)"><span id="thReplicaTotTitle" class="tag-sim-title">CACHE (SIM)</span><br>Totalização (DT/HT) <span id="sort_replica_tot" class="sort-icon">⇅</span></th>
          <th data-col="delay_tot" class="sortable" onclick="setSort('delay_tot')" title="Diferença entre o horário de totalização da FONTE e do CACHE em minutos e segundos">Δ Tempo DT/HT <span id="sort_delay_tot" class="sort-icon">⇅</span></th>
          <th data-col="origin_st" class="sortable" onclick="setSort('origin_st')" title="Quantidade e percentual de seções apuradas na FONTE (ST / %)"><span id="thOriginStTitle" class="tag-hmg-title">FONTE (HMG)</span><br>Seções (ST / %) <span id="sort_origin_st" class="sort-icon">⇅</span></th>
          <th data-col="replica_st" class="sortable" onclick="setSort('replica_st')" title="Quantidade e percentual de seções apuradas no CACHE (ST / %)"><span id="thReplicaStTitle" class="tag-sim-title">CACHE (SIM)</span><br>Seções (ST / %) <span id="sort_replica_st" class="sort-icon">⇅</span></th>
          <th data-col="diff_st" class="sortable" onclick="setSort('diff_st')" title="Diferença na quantidade de seções apuradas entre FONTE e CACHE">Δ Seções <span id="sort_diff_st" class="sort-icon">⇅</span></th>
          <th data-col="idg" class="sortable" onclick="setSort('idg')" title="Identificador Sequencial de Geração (IDG)">IDG <span id="sort_idg" class="sort-icon">⇅</span></th>
        </tr>
      </thead>
      <tbody id="compareTableBody">
        <tr><td colspan="11" style="text-align: center; color: var(--text-muted); padding: 20px;">Carregando dados comparativos...</td></tr>
      </tbody>
    </table>
  </div>

  <div class="card">
    <div class="card-title">
      <span>📝 Feed em Tempo Real das Requisições e Regressões</span>
      <button onclick="clearLogs()" style="background: transparent; border: 1px solid var(--border); color: var(--text-muted); padding: 4px 8px; border-radius: 4px; cursor: pointer;">Limpar</button>
    </div>
    <div class="log-feed" id="logFeed"></div>
  </div>

  <script>
    // =====================================================================
    // GERENCIADOR DE TEMAS (DARK / LIGHT)
    // =====================================================================
    function initTheme() {
      const saved = localStorage.getItem('tdtot_theme') || 'dark';
      applyTheme(saved);
    }

    function applyTheme(theme) {
      const btn = document.getElementById('themeToggleBtn');
      if (theme === 'light') {
        document.documentElement.setAttribute('data-theme', 'light');
        if (btn) btn.innerHTML = '🌙 Modo Escuro';
      } else {
        document.documentElement.removeAttribute('data-theme');
        if (btn) btn.innerHTML = '☀️ Modo Claro';
      }
      localStorage.setItem('tdtot_theme', theme);
    }

    function toggleTheme() {
      const isLight = document.documentElement.getAttribute('data-theme') === 'light';
      applyTheme(isLight ? 'dark' : 'light');
    }

    initTheme();

    function toggleMenu() {
      const menu = document.getElementById('exportMenuDropdown');
      menu.classList.toggle('show');
    }

    window.addEventListener('click', (e) => {
      const menu = document.getElementById('exportMenuDropdown');
      if (menu && !menu.contains(e.target)) {
        menu.classList.remove('show');
      }
    });

    function toggleFilterPanel() {
      const body = document.getElementById('filterBody');
      const icon = document.getElementById('filterToggleIcon');
      body.classList.toggle('collapsed');
      if (body.classList.contains('collapsed')) {
        icon.textContent = '▸ Expandir Filtros';
      } else {
        icon.textContent = '▾ Recolher Filtros';
      }
    }

    // =====================================================================
    // INDICADORES VISUAIS DE CARREGAMENTO / CONSULTA / FILTRAGEM
    // =====================================================================
    let loadingTimer = null;
    function showLoading(msg = 'Atualizando dados...') {
      const bar = document.getElementById('topProgressBar');
      if (bar) {
        bar.classList.add('active', 'indeterminate');
      }
      const tableBadge = document.getElementById('tableUpdatingBadge');
      if (tableBadge) {
        tableBadge.style.display = 'inline-flex';
        const span = tableBadge.querySelector('span:last-child');
        if (span) span.textContent = msg;
      }
      const filterBadge = document.getElementById('filterUpdatingBadge');
      if (filterBadge) {
        filterBadge.style.display = 'inline-flex';
        const span = filterBadge.querySelector('span:last-child');
        if (span) span.textContent = msg.includes('Filtrando') ? msg : 'Consultando...';
      }
      const tbl = document.getElementById('compareTableBody');
      if (tbl) tbl.classList.add('table-updating');
    }

    function hideLoading() {
      const bar = document.getElementById('topProgressBar');
      if (bar) {
        bar.classList.remove('indeterminate');
        bar.style.width = '100%';
        setTimeout(() => {
          bar.classList.remove('active');
          bar.style.width = '0%';
        }, 220);
      }
      const tableBadge = document.getElementById('tableUpdatingBadge');
      if (tableBadge) tableBadge.style.display = 'none';
      const filterBadge = document.getElementById('filterUpdatingBadge');
      if (filterBadge) filterBadge.style.display = 'none';
      const tbl = document.getElementById('compareTableBody');
      if (tbl) tbl.classList.remove('table-updating');
    }

    let filterDebounceTimer = null;
    function debounceApplyFilters(delay = 120, customMsg = 'Filtrando...') {
      showLoading(customMsg);
      if (filterDebounceTimer) clearTimeout(filterDebounceTimer);
      filterDebounceTimer = setTimeout(() => {
        applyFiltersInternal();
      }, delay);
    }

    function applyFilters() {
      debounceApplyFilters(40, 'Filtrando...');
    }

    let rawComparisonList = [];
    let currentSortCol = "delay_time";
    let currentSortDir = "desc";

    function setSort(col) {
      if (currentSortCol === col) {
        currentSortDir = (currentSortDir === "asc") ? "desc" : "asc";
      } else {
        currentSortCol = col;
        currentSortDir = (col === "classificacao" || col === "arquivo") ? "asc" : "desc";
      }
      updateSortIcons();
      applyFilters();
    }

    function updateSortIcons() {
      const cols = ["classificacao", "arquivo", "hmg_time", "sim_time", "delay_time", "sync_sla", "cache_ttl", "status", "origin_tot", "replica_tot", "delay_tot", "origin_st", "replica_st", "diff_st", "idg"];
      for (const c of cols) {
        const el = document.getElementById("sort_" + c);
        const th = el ? el.closest("th") : null;
        if (!el || !th) continue;
        th.classList.remove("sorted-asc", "sorted-desc");
        if (c === currentSortCol) {
          el.textContent = currentSortDir === "asc" ? "▲" : "▼";
          th.classList.add(currentSortDir === "asc" ? "sorted-asc" : "sorted-desc");
        } else {
          el.textContent = "⇅";
        }
      }
    }

    function sortData(rows) {
      return [...rows].sort((a, b) => {
        let vA, vB;
        switch(currentSortCol) {
          case "classificacao":
            vA = (a.meta?.uf || "") + "_" + (a.meta?.cargo || "") + "_" + (a.meta?.tipo || "");
            vB = (b.meta?.uf || "") + "_" + (b.meta?.cargo || "") + "_" + (b.meta?.tipo || "");
            break;
          case "arquivo":
            vA = a.relPath.toLowerCase();
            vB = b.relPath.toLowerCase();
            break;
          case "hmg_time":
            vA = a.hmg?.genTime ?? -1;
            vB = b.hmg?.genTime ?? -1;
            break;
          case "sim_time":
            vA = a.sim?.genTime ?? -1;
            vB = b.sim?.genTime ?? -1;
            break;
          case "delay_time":
            vA = Number(a.comparison?.delaySec ?? 0);
            vB = Number(b.comparison?.delaySec ?? 0);
            break;
          case "sync_sla":
            vA = Number(a.comparison?.syncSlaSec ?? 0);
            vB = Number(b.comparison?.syncSlaSec ?? 0);
            break;
          case "cache_ttl":
            vA = Number(a.sim?.maxAge ?? 0);
            vB = Number(b.sim?.maxAge ?? 0);
            break;
          case "status":
            const regA = (a.hmg?.status === "REGRESSAO_DETECTADA" || a.sim?.status === "REGRESSAO_DETECTADA") ? 3 : (a.comparison?.status === "CACHE_ATRASADO" ? 2 : 1);
            const regB = (b.hmg?.status === "REGRESSAO_DETECTADA" || b.sim?.status === "REGRESSAO_DETECTADA") ? 3 : (b.comparison?.status === "CACHE_ATRASADO" ? 2 : 1);
            vA = regA;
            vB = regB;
            break;
          case "origin_tot":
            vA = (a.origin?.totTime ?? a.hmg?.totTime ?? -1);
            vB = (b.origin?.totTime ?? b.hmg?.totTime ?? -1);
            break;
          case "replica_tot":
            vA = (a.sim?.totTime ?? -1);
            vB = (b.sim?.totTime ?? -1);
            break;
          case "delay_tot":
            vA = (a.comparison?.delayTotSec ?? 0);
            vB = (b.comparison?.delayTotSec ?? 0);
            break;
          case "origin_st":
            vA = Number(a.origin?.st ?? a.hmg?.st ?? -1);
            vB = Number(b.origin?.st ?? b.hmg?.st ?? -1);
            break;
          case "replica_st":
            vA = Number(a.sim?.st ?? -1);
            vB = Number(b.sim?.st ?? -1);
            break;
          case "diff_st":
            vA = Number(a.comparison?.diffSt ?? 0);
            vB = Number(b.comparison?.diffSt ?? 0);
            break;
          case "idg":
            vA = Number(a.origin?.idgNum ?? a.hmg?.idgNum ?? 0);
            vB = Number(b.origin?.idgNum ?? b.hmg?.idgNum ?? 0);
            break;
          default:
            vA = a.relPath;
            vB = b.relPath;
        }
        if (vA < vB) return currentSortDir === "asc" ? -1 : 1;
        if (vA > vB) return currentSortDir === "asc" ? 1 : -1;
        return a.relPath.localeCompare(b.relPath);
      });
    }

    
    function formatMinSec(sec) {
      if (sec === null || sec === undefined || isNaN(sec)) return '-';
      const s = Math.round(Number(sec));
      const sign = s < 0 ? '-' : '';
      const abs = Math.abs(s);
      const m = Math.floor(abs / 60);
      const remS = abs % 60;
      return sign + m + 'm ' + String(remS).padStart(2, '0') + 's';
    }

    let latestApiData = null;
function setElText(id, val) {
      const el = document.getElementById(id);
      if (el) el.textContent = val;
    }

    async function loadData() {
      try {
        showLoading('Atualizando matriz...');
        const res = await fetch('/api/comparison');
        const data = await res.json();
        latestApiData = data;
        rawComparisonList = data.comparison;
        applyFiltersInternal();
      } catch(e) {
        console.error('Erro em loadData:', e);
        hideLoading();
      }
    }

    function renderHeaderStats(data, filteredRows) {
      if (data.activeRodada) renderRodadasHeader(data.activeRodada);
      if (data.servers) {
        const srvCountEl = document.getElementById('headerServidoresCount');
        if (srvCountEl) srvCountEl.textContent = data.servers.length + ' nós (' + (data.activeServers?.length || data.servers.filter(s=>s.ativo).length) + ' ativos)';
        
        const thOrig = document.getElementById('thOriginTitle');
        const thRepl = document.getElementById('thReplicaTitle');
        const thOrigTot = document.getElementById('thOriginTotTitle');
        const thReplTot = document.getElementById('thReplicaTotTitle');
        const thOrigSt = document.getElementById('thOriginStTitle');
        const thReplSt = document.getElementById('thReplicaStTitle');
        
        const origName = data.servers.find(s => s.chave === data.originKey)?.chave || data.originKey || 'ORIGEM';
        const repName = (data.servers.length > 2) ? 'RÉPLICAS (' + (data.servers.length - 1) + ' nós)' : (data.servers.find(s => s.chave !== data.originKey)?.chave || 'RÉPLICA');
        
        if (thOrig && thRepl) {
          thOrig.innerHTML = origName + ' <span style="font-size:0.7rem; color:#c084fc;">(Origem)</span>';
          thRepl.innerHTML = repName + ' <span style="font-size:0.7rem; color:#38bdf8;">(Réplica)</span>';
        }
        if (thOrigTot && thReplTot) {
          thOrigTot.innerHTML = origName + ' <span style="font-size:0.7rem; color:#c084fc;">(Origem)</span>';
          thReplTot.innerHTML = repName + ' <span style="font-size:0.7rem; color:#38bdf8;">(Réplica)</span>';
        }
        if (thOrigSt && thReplSt) {
          thOrigSt.innerHTML = origName + ' <span style="font-size:0.7rem; color:#c084fc;">(Origem)</span>';
          thReplSt.innerHTML = repName + ' <span style="font-size:0.7rem; color:#38bdf8;">(Réplica)</span>';
        }
      }
      document.getElementById('countFiles').textContent = data.comparison.length;
      document.getElementById('countVisible').textContent = filteredRows ? filteredRows.length : data.comparison.length;
      document.getElementById('countRegTime').textContent = data.regressionsTimeCount || 0;
      document.getElementById('lastRegTime').textContent = data.lastRegressionTime || '-';

      // Atualiza KPI de Atributos de Cache & TTL
      if (data.cacheStats) {
        const cs = data.cacheStats;
        const kpiSimTtlEl = document.getElementById('kpiSimTtl');
        const kpiSimTtlRangeEl = document.getElementById('kpiSimTtlRange');
        const kpiCdnHitsEl = document.getElementById('kpiCdnHits');
        const kpiHmgHeaderEl = document.getElementById('kpiHmgHeader');

        if (kpiSimTtlEl) kpiSimTtlEl.textContent = cs.simAvgTtl + 's';
        if (kpiSimTtlRangeEl) kpiSimTtlRangeEl.textContent = '(' + cs.simTtlMin + '-' + cs.simTtlMax + 's)';
        if (kpiCdnHitsEl) kpiCdnHitsEl.textContent = cs.cdnHitRate + '%';
        if (kpiHmgHeaderEl) kpiHmgHeaderEl.textContent = 'Sem Header (Apache)';
      }

      // 1. Quantidade de arquivos com cache atrasado AGORA
      const list = filteredRows || data.comparison;
      let desyncCount = 0;
      for (const row of list) {
        if (row.comparison.status === 'CACHE_ATRASADO') desyncCount++;
      }
      document.getElementById('countDesync').textContent = desyncCount;

      // 2. SLAs de todas as atualizações desde 00h (não apenas as últimas)
      const syncMap = data.todaySyncByFile || {};
      let slas = [];
      if (filteredRows && filteredRows.length < data.comparison.length) {
        for (const row of filteredRows) {
          const fileSlas = syncMap[row.relPath];
          if (fileSlas && fileSlas.length) {
            slas.push(...fileSlas);
          }
        }
        slas.sort((a, b) => a - b);
      } else if (data.todaySlaStats) {
        // Estatísticas pré-calculadas de todas as sincronizações desde 00h
        setElText('cacheAvg', formatMinSec(data.todaySlaStats.avgSec));
        setElText('cacheP90', formatMinSec(data.todaySlaStats.p90Sec));
        setElText('cacheP95', formatMinSec(data.todaySlaStats.p95Sec));
        setElText('cacheP99', formatMinSec(data.todaySlaStats.p99Sec));
        setElText('cacheP100', formatMinSec(data.todaySlaStats.p100Sec));
        slas = null;
      }

      if (slas && slas.length > 0) {
        const avgSec = Math.round(slas.reduce((a, b) => a + b, 0) / slas.length);
        setElText('cacheAvg', formatMinSec(avgSec));

        const idx90 = Math.min(slas.length - 1, Math.floor(slas.length * 0.90));
        setElText('cacheP90', formatMinSec(slas[idx90]));

        const idx95 = Math.min(slas.length - 1, Math.floor(slas.length * 0.95));
        setElText('cacheP95', formatMinSec(slas[idx95]));

        const idx99 = Math.min(slas.length - 1, Math.floor(slas.length * 0.99));
        setElText('cacheP99', formatMinSec(slas[idx99]));

        const idx100 = slas.length - 1;
        setElText('cacheP100', formatMinSec(slas[idx100]));
      } else if (slas !== null) {
        setElText('cacheAvg', '0m 00s');
        setElText('cacheP90', '0m 00s');
        setElText('cacheP95', '0m 00s');
        setElText('cacheP99', '0m 00s');
        setElText('cacheP100', '0m 00s');
      }

      const badge = document.getElementById('statusBadge');
      const badgeText = document.getElementById('statusText');

      const totalRegs = data.regressionsTimeCount || 0;
      if (totalRegs > 0) {
        badge.className = 'status-badge status-danger';
        badgeText.innerHTML = '🚨 ' + totalRegs + ' REGRESSÃO(ÕES) TEMPORAL(IS)! <span style="font-size:0.70rem; opacity:0.85; margin-left:3px; font-weight:normal;">[Ver Detalhes ↗]</span>';
        badge.title = 'Clique para abrir o Dossiê Forense com todos os detalhes das ' + totalRegs + ' regressões detectadas!';
      } else {
        badge.className = 'status-badge status-ok';
        badgeText.textContent = '✅ SEM REGRESSÕES TEMPORAIS';
        badge.title = 'Nenhuma regressão detectada nesta rodada. Clique para inspecionar histórico.';
      }

      const feed = document.getElementById('logFeed');
      feed.innerHTML = '';
      for (const log of data.recentLogs) {
        const item = document.createElement('div');
        item.className = 'log-item';
        const srcColor = log.serverKey === 'HMG' ? 'tag-hmg-title' : 'tag-sim-title';
        const alertStyle = log.isRegression ? 'color: var(--accent-red); font-weight: bold;' : '';
        const critBadge = log.criterion && log.criterion !== 'NORMAL' && log.criterion !== 'OK' && log.criterion !== 'INICIAL' 
          ? \`<span style="background:#dc2626; color:#fff; padding:1px 5px; border-radius:3px; font-size:0.7rem; margin-right:4px;">\${log.criterion}</span>\`
          : '';
        item.innerHTML = \`
          <span class="log-time">\${log.time || ''}</span>
          <span class="log-src \${srcColor}">[\${log.serverKey} (\${log.role || ''})]</span>
          <span class="log-file code">\${log.filename}</span>
          <span class="log-desc" style="\${alertStyle}">\${critBadge}\${log.status}: \${log.details}</span>
        \`;
        feed.appendChild(item);
      }
    }

    function resetFilters() {
      document.getElementById('searchInput').value = '';
      if (document.getElementById('filterPleito')) document.getElementById('filterPleito').value = '';
      document.getElementById('filterEleicao').value = '';
      document.getElementById('filterUf').value = '';
      document.getElementById('filterTipo').value = '';
      document.getElementById('filterCargo').value = '';
      document.getElementById('filterStatus').value = '';
      applyFilters();
    }

    function updatePleitosDropdown(list) {
      const sel = document.getElementById('filterPleito');
      if (!sel) return;
      const curVal = sel.value;
      const pleitos = new Set();
      for (const r of list) {
        if (r.meta && r.meta.pleito && r.meta.pleito !== '-' && r.meta.pleito !== 'Todos') {
          pleitos.add(r.meta.pleito);
        }
      }
      const sorted = Array.from(pleitos).sort();
      if (sel.options.length <= 1 || (sel.options.length - 1) !== sorted.length) {
        sel.innerHTML = '<option value="">Todos os Pleitos</option>';
        for (const p of sorted) {
          const opt = document.createElement('option');
          opt.value = p;
          opt.textContent = 'Pleito ' + p;
          sel.appendChild(opt);
        }
        if (curVal && sorted.includes(curVal)) {
          sel.value = curVal;
        }
      }
    }

    function applyFiltersInternal() {
      if (rawComparisonList) updatePleitosDropdown(rawComparisonList);

      const q = document.getElementById('searchInput').value.toLowerCase().trim();
      const fPleito = document.getElementById('filterPleito')?.value;
      const fEle = document.getElementById('filterEleicao').value;
      const fUf = document.getElementById('filterUf').value;
      const fTipo = document.getElementById('filterTipo').value;
      const fCargo = document.getElementById('filterCargo').value;
      const fStatus = document.getElementById('filterStatus').value;

      const filtered = rawComparisonList.filter(row => {
        const meta = row.meta || {};
        if (fPleito && meta.pleito !== fPleito && meta.pleito !== 'Todos') return false;
        if (fEle && meta.eleicao !== fEle) return false;
        if (fUf && meta.uf !== fUf) return false;
        if (fTipo) {
          if (fTipo === 'Configuração' && !meta.tipo.includes('Configuração')) return false;
          else if (fTipo !== 'Configuração' && meta.tipo !== fTipo) return false;
        }
        if (fCargo && meta.cargo !== fCargo) return false;
        if (fCargo && meta.cargo !== fCargo) return false;

        if (fStatus === 'REG_ALL') {
          const hasReg = (row.hmg?.status === 'REGRESSAO_DETECTADA' || row.sim?.status === 'REGRESSAO_DETECTADA');
          if (!hasReg) return false;
        } else if (fStatus === 'REG_TIME') {
          const hmgReg = row.hmg?.criterion?.includes('TEMPO') || row.hmg?.criterion?.includes('DG/HG');
          const simReg = row.sim?.criterion?.includes('TEMPO') || row.sim?.criterion?.includes('DG/HG');
          if (!hmgReg && !simReg) return false;
        } else if (fStatus === 'REG_TOT') {
          const hmgRegTot = row.hmg?.criterion?.includes('TOTALIZAÇÃO');
          const simRegTot = row.sim?.criterion?.includes('TOTALIZAÇÃO');
          if (!hmgRegTot && !simRegTot) return false;
        } else if (fStatus === 'REG_ST') {
          const hmgRegSt = row.hmg?.criterion?.includes('SEÇÕES');
          const simRegSt = row.sim?.criterion?.includes('SEÇÕES');
          if (!hmgRegSt && !simRegSt) return false;
        } else if (fStatus === 'ATRASADO') {
          if (row.comparison.status !== 'CACHE_ATRASADO') return false;
        } else if (fStatus === 'SINCRONIZADO') {
          if (row.comparison.status !== 'SINCRONIZADO') return false;
        } else if (fStatus === 'SIM_TTL_LOW') {
          if (!row.sim || row.sim.maxAge === null || row.sim.maxAge > 15) return false;
        }

        if (q) {
          const matchPath = row.relPath.toLowerCase().includes(q);
          const matchCargo = (meta.cargo || '').toLowerCase().includes(q);
          const matchUf = (meta.uf || '').toLowerCase().includes(q);
          if (!matchPath && !matchCargo && !matchUf) return false;
        }

        return true;
      });

      document.getElementById('countVisible').textContent = filtered.length;
      const tcb = document.getElementById('tableCountBadge');
      if (tcb) tcb.textContent = '(' + filtered.length + ' de ' + rawComparisonList.length + ' arquivos)';
      const hasActive = Boolean(q || fPleito || fEle || fUf || fTipo || fCargo || fStatus);
      const afb = document.getElementById('activeFilterBadge');
      if (afb) afb.style.display = hasActive ? 'inline-block' : 'none';

      if (latestApiData) {
        renderHeaderStats(latestApiData, filtered);
      }

      renderTable(sortData(filtered));
      hideLoading();
    }

    // =====================================================================
    // SELETOR DINÂMICO DE COLUNAS COM PERSISTÊNCIA LOCAL
    // =====================================================================
    const AVAILABLE_COLUMNS = [
      { id: 'classificacao', label: 'Classificação', default: true, desc: 'Pleito, Eleição, UF, Tipo e Cargo' },
      { id: 'arquivo',       label: 'Caminho do Arquivo & Ações', default: true, desc: 'Caminho relativo e links de inspeção' },
      { id: 'origin_time',   label: 'FONTE: Geração (DG/HG)', default: true, desc: 'Data/Hora de geração na Origem' },
      { id: 'replica_time',  label: 'CACHE: Geração (DG/HG)', default: true, desc: 'Data/Hora de geração no Cache' },
      { id: 'delay_time',    label: 'Δ Tempo DG/HG', default: true, desc: 'Diferença de geração em Minutos e Segundos' },
      { id: 'sync_sla',      label: 'SLA Sync (DG/HG)', default: true, desc: 'Tempo real até replicação na borda/cache' },
      { id: 'cache_ttl',     label: '⚡ Cache & TTL', default: true, desc: 'Cache-Control, max-age e CDN status' },
      { id: 'status',        label: 'Integridade', default: true, desc: 'Auditoria forense e integridade temporal' },
      { id: 'origin_tot',    label: 'FONTE: Totalização (DT/HT)', default: false, desc: 'Data/Hora de apuração consolidada na Origem' },
      { id: 'replica_tot',   label: 'CACHE: Totalização (DT/HT)', default: false, desc: 'Data/Hora de apuração consolidada no Cache' },
      { id: 'delay_tot',     label: 'Δ Tempo DT/HT', default: false, desc: 'Diferença de fechamento entre Origem e Cache' },
      { id: 'origin_st',     label: 'FONTE: Seções (ST / %)', default: false, desc: 'Seções apuradas e percentual na Origem' },
      { id: 'replica_st',    label: 'CACHE: Seções (ST / %)', default: false, desc: 'Seções apuradas e percentual no Cache' },
      { id: 'diff_st',       label: 'Δ Seções', default: false, desc: 'Diferença de seções apuradas entre Origem e Cache' },
      { id: 'idg',           label: 'IDG Sequencial', default: false, desc: 'Identificador Sequencial de Geração' }
    ];

    function getVisibleColumns() {
      try {
        const stored = localStorage.getItem('tdtot_visible_columns');
        if (stored) {
          const parsed = JSON.parse(stored);
          if (Array.isArray(parsed) && parsed.length > 0) return parsed;
        }
      } catch (e) {}
      return AVAILABLE_COLUMNS.filter(c => c.default).map(c => c.id);
    }

    function saveVisibleColumns(cols) {
      try {
        localStorage.setItem('tdtot_visible_columns', JSON.stringify(cols));
      } catch (e) {}
      applyColumnVisibility();
    }

    function applyColumnVisibility() {
      const visible = getVisibleColumns();
      const visibleSet = new Set(visible);

      let styleEl = document.getElementById('dynamicColumnsCss');
      if (!styleEl) {
        styleEl = document.createElement('style');
        styleEl.id = 'dynamicColumnsCss';
        document.head.appendChild(styleEl);
      }

      let css = '';
      for (const col of AVAILABLE_COLUMNS) {
        if (!visibleSet.has(col.id)) {
          css += '[data-col="' + col.id + '"] { display: none !important; } ';
        }
      }
      styleEl.textContent = css;

      const countEl = document.getElementById('visibleColsCount');
      if (countEl) countEl.textContent = visible.length;
      const totalEl = document.getElementById('totalColsCount');
      if (totalEl) totalEl.textContent = AVAILABLE_COLUMNS.length;

      renderColumnCheckboxes();
    }

    function renderColumnCheckboxes() {
      const listEl = document.getElementById('colCheckboxesList');
      if (!listEl) return;
      const visible = new Set(getVisibleColumns());

      listEl.innerHTML = AVAILABLE_COLUMNS.map(col => {
        const isChecked = visible.has(col.id);
        const isDefault = col.default;
        return '<label style="display:flex; align-items:flex-start; gap:8px; padding:6px 8px; border-radius:6px; cursor:pointer; background:' + (isChecked ? 'rgba(56,189,248,0.08)' : 'rgba(255,255,255,0.02)') + '; border:1px solid ' + (isChecked ? 'rgba(56,189,248,0.25)' : 'rgba(255,255,255,0.05)') + ';">' +
          '<input type="checkbox" ' + (isChecked ? 'checked' : '') + ' data-col-id="' + col.id + '" onchange="toggleColumn(this.dataset.colId, this.checked)" style="margin-top:3px; accent-color:#38bdf8; cursor:pointer;">' +
          '<div style="flex:1;">' +
            '<div style="display:flex; align-items:center; justify-content:space-between;">' +
              '<span style="font-weight:600; color:' + (isChecked ? '#f8fafc' : '#94a3b8') + '; font-size:0.80rem;">' + col.label + '</span>' +
              (isDefault ? '<span style="font-size:0.64rem; color:var(--text-muted); background:rgba(255,255,255,0.06); padding:1px 4px; border-radius:3px;">Padrão</span>' : '<span style="font-size:0.64rem; color:#38bdf8; background:rgba(56,189,248,0.1); padding:1px 4px; border-radius:3px;">Opcional</span>') +
            '</div>' +
            '<div style="font-size:0.68rem; color:var(--text-muted); line-height:1.2; margin-top:2px;">' + col.desc + '</div>' +
          '</div>' +
        '</label>';
      }).join('');
    }

    function toggleColumn(colId, enable) {
      let current = getVisibleColumns();
      if (enable) {
        if (!current.includes(colId)) current.push(colId);
      } else {
        if (current.length <= 1) {
          alert('Pelo menos uma coluna deve permanecer visível.');
          applyColumnVisibility();
          return;
        }
        current = current.filter(id => id !== colId);
      }
      saveVisibleColumns(current);
    }

    function resetDefaultColumns() {
      const defaults = AVAILABLE_COLUMNS.filter(c => c.default).map(c => c.id);
      saveVisibleColumns(defaults);
    }

    function toggleAllColumns(enable) {
      if (enable) {
        saveVisibleColumns(AVAILABLE_COLUMNS.map(c => c.id));
      } else {
        resetDefaultColumns();
      }
    }

    function toggleColumnModal(e) {
      if (e) e.stopPropagation();
      const dd = document.getElementById('colSelectorDropdown');
      if (!dd) return;
      const isHidden = dd.style.display === 'none' || !dd.style.display;
      dd.style.display = isHidden ? 'block' : 'none';
    }

    document.addEventListener('click', (e) => {
      const dd = document.getElementById('colSelectorDropdown');
      const btn = document.getElementById('btnColSelector');
      if (dd && dd.style.display === 'block') {
        if (!dd.contains(e.target) && !btn.contains(e.target)) {
          dd.style.display = 'none';
        }
      }
    });

    function renderTable(rows) {
      const tbody = document.getElementById('compareTableBody');
      tbody.innerHTML = '';

      const visibleCols = getVisibleColumns();

      if (rows.length === 0) {
        tbody.innerHTML = '<tr><td colspan="' + visibleCols.length + '" style="text-align: center; color: var(--text-muted); padding: 24px;">Nenhum arquivo corresponde aos filtros aplicados.</td></tr>';
        return;
      }

      const isMulti = latestApiData && latestApiData.servers && latestApiData.servers.length > 2;

      for (const row of rows) {
        const meta = row.meta || {};
        const tr = document.createElement('tr');

        // Badge Atraso por Data/Hora (DG/HG)
        let delayTimeBadge = '<span class="badge badge-sync">0m 00s</span>';
        if (row.comparison.statusTime === 'CACHE_ATRASADO') {
          delayTimeBadge = \`<span class="badge badge-lag">\${row.comparison.textTime}</span>\`;
        } else if (row.comparison.statusTime === 'CACHE_A_FRENTE') {
          delayTimeBadge = \`<span class="badge badge-ahead">\${row.comparison.textTime}</span>\`;
        } else if (row.comparison.statusTime === 'SEM_TIMESTAMP') {
          delayTimeBadge = '<span class="badge" style="background:#334155">-</span>';
        }

        // Origem (Master de Referência)
        const originMeta = row.origin || row.hmg;
        const originIdgTag = originMeta && originMeta.idg ? \`<br><span style="font-size:0.70rem; color:var(--text-muted); font-family:monospace;" title="Identificador de Geração (IDG)">IDG: \${originMeta.idg}</span>\` : '';
        const originHg = originMeta ? \`<a href="\${row.originUrl || row.hmgUrl}" target="_blank" style="color:var(--accent-purple); text-decoration:none;" title="Abrir JSON do Servidor de ORIGEM"><strong style="font-size:0.98rem;">\${originMeta.hg} ↗</strong></a><br><span style="font-size:0.75rem;color:var(--text-muted);">\${originMeta.dg}</span>\${originIdgTag}\` : '<span style="color:#64748b">Pendente</span>';

        // Réplicas / Nós
        let replicaCellContent = '';
        if (isMulti && row.comparison.replicas && row.comparison.replicas.length > 1) {
          // Visualização Multi-Nós (3+ servidores)
          replicaCellContent = \`
            <div style="display:flex; flex-direction:column; gap:4px; min-width:210px;">
              \${row.comparison.replicas.map(r => \`
                <div style="display:flex; align-items:center; justify-content:space-between; gap:6px; background:rgba(0,0,0,0.25); border:1px solid rgba(255,255,255,0.06); padding:3px 7px; border-radius:5px;">
                  <div style="display:flex; align-items:center; gap:4px;">
                    <span class="tag-pill tag-eleicao" style="font-size:0.67rem; padding:1px 4px;">\${r.chave}</span>
                    <strong style="font-size:0.80rem; color:\${r.delaySec > 0 ? '#fbbf24' : '#38bdf8'};">\${r.hg}</strong>
                  </div>
                  <div style="display:flex; align-items:center; gap:4px;">
                    <span class="badge \${r.delaySec > 0 ? 'badge-lag' : 'badge-sync'}" style="font-size:0.64rem; padding:1px 4px;">\${r.textTime}</span>
                    <span class="badge \${r.syncSlaStatus === 'CRITICO' ? 'badge-danger' : (r.syncSlaStatus === 'ALERTA' ? 'badge-lag' : 'badge-sync')}" style="font-size:0.64rem; padding:1px 4px;">⚡ \${r.syncSlaText}</span>
                  </div>
                </div>
              \`).join('')}
              <button onclick="openMultiNodeModal('\${encodeURIComponent(row.relPath)}')" class="btn-copy" style="font-size:0.68rem; padding:2px 6px; margin-top:2px; text-align:center; background:rgba(56,189,248,0.1); border-color:rgba(56,189,248,0.3); color:#38bdf8;">🔍 Matriz Multi-Nós (\${row.comparison.replicas.length} réplicas)</button>
            </div>
          \`;
        } else {
          // Visualização Clássica (2 servidores)
          const simIdgTag = row.sim && row.sim.idg ? \`<br><span style="font-size:0.70rem; color:var(--text-muted); font-family:monospace;" title="Identificador de Geração (IDG)">IDG: \${row.sim.idg}</span>\` : '';
          replicaCellContent = row.sim ? \`<a href="\${row.simUrl}" target="_blank" style="color:var(--accent-blue); text-decoration:none;" title="Abrir JSON do CACHE"><strong style="font-size:0.98rem;">\${row.sim.hg} ↗</strong></a><br><span style="font-size:0.75rem;color:var(--text-muted);">\${row.sim.dg}</span>\${simIdgTag}\` : '<span style="color:#64748b">Pendente</span>';
        }

        // Status de Integridade e Regressão
        let orderStatus = '<span class="badge badge-sync">OK</span>';
        let hasRegression = false;
        let regSrv = '';
        let regCrit = '';

        if (latestApiData?.servers) {
          for (const s of latestApiData.servers) {
            const sMeta = row.allServers?.[s.chave]?.meta;
            if (sMeta?.status === 'REGRESSAO_DETECTADA') {
              hasRegression = true;
              regSrv = s.chave;
              regCrit = sMeta.criterion || 'REGRESSÃO';
              break;
            }
          }
        } else {
          hasRegression = (row.hmg?.status === 'REGRESSAO_DETECTADA' || row.sim?.status === 'REGRESSAO_DETECTADA');
          regSrv = row.hmg?.status === 'REGRESSAO_DETECTADA' ? 'HMG' : 'SIM';
          regCrit = (row.hmg?.status === 'REGRESSAO_DETECTADA' ? row.hmg?.criterion : row.sim?.criterion) || 'REGRESSÃO';
        }

        if (hasRegression) {
          orderStatus = \`<span class="badge badge-danger" title="Violou critério de integridade: \${regCrit}">REGRESSÃO (\${regSrv}): \${regCrit}</span>\`;
        }

        // Badge SLA Sync
        let syncSlaBadge = '<span class="badge" style="background:#334155">-</span>';
        if (row.comparison.syncSlaStatus === 'OK') {
          syncSlaBadge = \`<span class="badge badge-sync">⚡ \${row.comparison.syncSlaText}</span>\`;
        } else if (row.comparison.syncSlaStatus === 'ALERTA') {
          syncSlaBadge = \`<span class="badge badge-lag">⚠️ \${row.comparison.syncSlaText}</span>\`;
        } else if (row.comparison.syncSlaStatus === 'AGUARDANDO') {
          syncSlaBadge = \`<span class="badge badge-ahead">\${row.comparison.syncSlaText}</span>\`;
        } else if (row.comparison.syncSlaStatus === 'CRITICO') {
          syncSlaBadge = \`<span class="badge badge-danger">\${row.comparison.syncSlaText}</span>\`;
        }

        // Links de todos os nós para inspeção rápida
        let serverLinks = '';
        if (latestApiData && latestApiData.servers) {
          serverLinks = latestApiData.servers.map(s => {
            const u = s.baseUrl + row.relPath;
            const isOrig = s.chave === row.originKey;
            const linkClass = isOrig ? 'url-link url-link-hmg' : 'url-link';
            return \`<a href="\${u}" target="_blank" class="\${linkClass}" style="font-size:0.72rem; padding:2px 6px;" title="Abrir no nó \${s.chave}">↗ \${s.chave}</a>\`;
          }).join('');
        } else {
          serverLinks = \`
            <a href="\${row.hmgUrl}" target="_blank" class="url-link url-link-hmg">↗ HMG</a>
            <a href="\${row.simUrl}" target="_blank" class="url-link">↗ SIM</a>
          \`;
        }

        // 1. Células de Totalização Simétricas por Servidor (Origem, Réplica e Delta)
        let originTotCell = '<span style="color:var(--text-muted); font-size:0.75rem;">-</span>';
        if (originMeta?.dt && originMeta?.ht) {
          originTotCell = \`<div style="display:flex; flex-direction:column; gap:1px;"><strong style="color:var(--accent-purple); font-size:0.90rem;">\${originMeta.ht}</strong><span style="font-size:0.72rem; color:var(--text-muted);">\${originMeta.dt}</span></div>\`;
        }

        let replicaTotCell = '<span style="color:var(--text-muted); font-size:0.75rem;">-</span>';
        if (row.sim?.dt && row.sim?.ht) {
          replicaTotCell = \`<div style="display:flex; flex-direction:column; gap:1px;"><strong style="color:var(--accent-blue); font-size:0.90rem;">\${row.sim.ht}</strong><span style="font-size:0.72rem; color:var(--text-muted);">\${row.sim.dt}</span></div>\`;
        }

        let delayTotCell = '<span style="color:var(--text-muted); font-size:0.75rem;">-</span>';
        if (originMeta?.totTime !== null && originMeta?.totTime !== undefined && row.sim?.totTime !== null && row.sim?.totTime !== undefined) {
          const diffTot = Math.round((originMeta.totTime - row.sim.totTime) / 1000);
          if (diffTot === 0) {
            delayTotCell = '<span class="badge badge-sync" style="font-size:0.72rem; padding:2px 6px;">0m 00s</span>';
          } else if (diffTot > 0) {
            delayTotCell = \`<span class="badge badge-lag" style="font-size:0.72rem; padding:2px 6px;" title="Totalização no cache atrasada em \${formatMinSec(diffTot)}">-\${formatMinSec(diffTot)}</span>\`;
          } else {
            delayTotCell = \`<span class="badge badge-ahead" style="font-size:0.72rem; padding:2px 6px;">+\${formatMinSec(-diffTot)}</span>\`;
          }
        }

        // 2. Células de Seções Apuradas Simétricas por Servidor (Origem, Réplica e Delta)
        let originStCell = '<span style="color:var(--text-muted); font-size:0.75rem;">-</span>';
        if (originMeta?.st !== null && originMeta?.st !== undefined) {
          const stNum = Number(originMeta.st);
          const tsNum = originMeta.ts ? Number(originMeta.ts) : null;
          const pstText = originMeta.pst || (tsNum ? ((stNum / tsNum) * 100).toFixed(2).replace('.', ',') : '0,00');
          originStCell = \`<div style="display:flex; flex-direction:column; gap:2px;"><div style="display:flex; align-items:baseline; gap:4px;"><strong style="color:#10b981; font-size:0.88rem;">\${stNum.toLocaleString('pt-BR')}</strong>\${tsNum ? ('<span style="font-size:0.70rem; color:var(--text-muted);">/ ' + tsNum.toLocaleString('pt-BR') + '</span>') : ''}</div><span class="badge badge-sync" style="font-size:0.65rem; padding:1px 4px; width:fit-content;">\${pstText}%</span></div>\`;
        }

        let replicaStCell = '<span style="color:var(--text-muted); font-size:0.75rem;">-</span>';
        if (row.sim?.st !== null && row.sim?.st !== undefined) {
          const simStNum = Number(row.sim.st);
          const simTsNum = row.sim.ts ? Number(row.sim.ts) : (originMeta?.ts ? Number(originMeta.ts) : null);
          const simPstText = row.sim.pst || (simTsNum ? ((simStNum / simTsNum) * 100).toFixed(2).replace('.', ',') : '0,00');
          replicaStCell = \`<div style="display:flex; flex-direction:column; gap:2px;"><div style="display:flex; align-items:baseline; gap:4px;"><strong style="color:#38bdf8; font-size:0.88rem;">\${simStNum.toLocaleString('pt-BR')}</strong>\${simTsNum ? ('<span style="font-size:0.70rem; color:var(--text-muted);">/ ' + simTsNum.toLocaleString('pt-BR') + '</span>') : ''}</div><span class="badge badge-sync" style="font-size:0.65rem; padding:1px 4px; width:fit-content;">\${simPstText}%</span></div>\`;
        }

        let diffStCell = '<span style="color:var(--text-muted); font-size:0.75rem;">-</span>';
        if (originMeta?.st !== null && originMeta?.st !== undefined && row.sim?.st !== null && row.sim?.st !== undefined) {
          const diffSt = Number(originMeta.st) - Number(row.sim.st);
          if (diffSt === 0) {
            diffStCell = '<span class="badge badge-sync" style="font-size:0.72rem; padding:2px 6px;">0 seç</span>';
          } else if (diffSt > 0) {
            diffStCell = \`<span class="badge badge-lag" style="font-size:0.72rem; padding:2px 6px;" title="Cache com \${diffSt} seções a menos que a Origem">-\${diffSt} seç</span>\`;
          } else {
            diffStCell = \`<span class="badge badge-ahead" style="font-size:0.72rem; padding:2px 6px;">+\${-diffSt} seç</span>\`;
          }
        }
        // 3. Célula IDG Sequencial
        const origIdg = originMeta?.idg;
        const simIdg = row.sim?.idg;
        let idgCell = '<span style="color:var(--text-muted); font-size:0.75rem;">-</span>';
        if (origIdg || simIdg) {
          idgCell = \`
            <div style="min-width:115px; font-family:monospace; font-size:0.78rem; display:flex; flex-direction:column; gap:2px;">
              <div><span style="color:var(--text-muted); font-size:0.68rem;">\${row.originKey || 'FONTE'}:</span> <strong style="color:var(--accent-purple);">\${origIdg || '-'}</strong></div>
              <div><span style="color:var(--text-muted); font-size:0.68rem;">\${row.comparison.primaryReplicaKey || 'CACHE'}:</span> <strong style="color:var(--accent-blue);">\${simIdg || '-'}</strong></div>
              \${(row.comparison.diffIdg !== null && row.comparison.diffIdg !== 0) ? ('<span class="badge ' + (row.comparison.diffIdg > 0 ? 'badge-lag' : 'badge-ahead') + '" style="font-size:0.64rem; padding:1px 4px; margin-top:2px;">Δ ' + row.comparison.textIdg + '</span>') : ''}
            </div>
          \`;
        }

        tr.innerHTML = \`
          <td data-col="classificacao" style="min-width: 140px;">
            <div style="margin-bottom: 4px; display: flex; gap: 4px; flex-wrap: wrap;">
              <span class="tag-pill tag-pleito" title="Dimensão Pleito">Pl: \${meta.pleito || '-'}</span>
              <span class="tag-pill tag-eleicao">\${meta.eleicao}</span>
              <span class="tag-pill tag-uf">\${meta.uf}</span>
            </div>
            <div>
              <span class="tag-pill tag-tipo">\${meta.tipo} (\${meta.sufixo || ''})</span>
              \${meta.cargo !== '-' ? ('<span class="tag-pill tag-cargo">' + meta.cargo + '</span>') : ''}
            </div>
          </td>
          <td data-col="arquivo" style="min-width: 280px;">
            <div class="code" style="font-weight:700; color:var(--text); font-size:0.82rem; margin-bottom:6px; word-break:break-all;">\${row.relPath}</div>
            <div style="display:flex; gap:5px; flex-wrap:wrap; align-items:center;">
              \${serverLinks}
              <button onclick="openMultiNodeModal('\${encodeURIComponent(row.relPath)}')" class="btn-copy" style="font-size:0.70rem; padding:2px 6px;">📋 Inspecionar Nós</button>
            </div>
          </td>
          <td data-col="origin_time">\${originHg}</td>
          <td data-col="replica_time">\${replicaCellContent}</td>
          <td data-col="delay_time">\${delayTimeBadge}</td>
          <td data-col="sync_sla">\${syncSlaBadge}</td>
          <td data-col="cache_ttl">
            <div style="display:flex; flex-direction:column; gap:4px; font-size:0.75rem;">
              <div style="display:flex; align-items:center; gap:6px;">
                <span class="tag-pill tag-uf" style="font-size:0.68rem; padding:1px 5px;">\${row.originKey || 'ORIGEM'}</span>
                <span style="font-family:monospace; color:#c084fc;" title="Header Cache-Control da Origem">\${row.hmg?.cacheControl || '(sem Cache-Control)'}</span>
              </div>
              <div style="display:flex; align-items:center; gap:6px;">
                <span class="tag-pill tag-eleicao" style="font-size:0.68rem; padding:1px 5px;">\${row.comparison.primaryReplicaKey || 'RÉPLICA'}</span>
                <span style="font-family:monospace; font-weight:700; color:#38bdf8;" title="Cache-Control e TTL">\${row.sim?.cacheControl || (row.sim?.maxAge != null ? 'max-age=' + row.sim.maxAge : '-')}</span>
                \${row.sim?.serverIp ? ('<span class="tag-ip" title="Instância: ' + row.sim.serverIp + '">📍 ' + row.sim.serverIp + '</span>') : ''}
              </div>
              <div style="display:flex; align-items:center; gap:6px; margin-top:2px;">
                <button onclick="openMultiNodeModal('\${encodeURIComponent(row.relPath)}')" class="btn-copy" style="font-size:0.68rem; padding:1px 6px;" title="Comparar todos os nós e headers">🔍 Matriz Completa</button>
              </div>
            </div>
          </td>
          <td data-col="status">\${orderStatus}</td>
          <td data-col="origin_tot">\${originTotCell}</td>
          <td data-col="replica_tot">\${replicaTotCell}</td>
          <td data-col="delay_tot">\${delayTotCell}</td>
          <td data-col="origin_st">\${originStCell}</td>
          <td data-col="replica_st">\${replicaStCell}</td>
          <td data-col="diff_st">\${diffStCell}</td>
          <td data-col="idg">\${idgCell}</td>
        \`;
        tbody.appendChild(tr);
      }
    }

    const evtSource = new EventSource('/api/events');
    evtSource.onmessage = () => loadData();

    setInterval(loadData, 4000);
    applyColumnVisibility();
    updateSortIcons();
    loadData();

    
    
    
    // --- GERENCIAMENTO DE SERVIDORES NA INTERFACE ---
    let cachedServidoresData = null;
    let editingServerChave = null;

    async function loadServidoresData() {
      try {
        const resp = await fetch('/api/servidores');
        const data = await resp.json();
        cachedServidoresData = data;
        renderServidoresList(data.servers, data.originKey);
      } catch (err) {
        console.error('Erro ao listar servidores:', err);
      }
    }

    function renderServidoresList(list, originKey) {
      const container = document.getElementById('servidoresListContainer');
      if (!container) return;
      if (!list || !list.length) {
        container.innerHTML = '<div style="color:#94a3b8; font-size:0.85rem;">Nenhum servidor cadastrado.</div>';
        return;
      }

      container.innerHTML = '';
      for (const s of list) {
        const isOrig = s.papel === 'ORIGEM' || s.chave === originKey;
        const item = document.createElement('div');
        item.style.cssText = 'display:flex; justify-content:space-between; align-items:center; background:' + (isOrig ? 'rgba(168, 85, 247, 0.12)' : '#0f172a') + '; border:1px solid ' + (isOrig ? '#a855f7' : '#334155') + '; padding:12px 16px; border-radius:8px; gap:12px; flex-wrap:wrap;';

        const left = document.createElement('div');
        left.style.cssText = 'display:flex; flex-direction:column; gap:4px; flex:1; min-width:240px;';
        
        const papelBadge = isOrig 
          ? '<span class="tag-pill tag-uf" style="font-size:0.70rem; padding:2px 6px;">⭐ ORIGEM (Master)</span>' 
          : '<span class="tag-pill tag-eleicao" style="font-size:0.70rem; padding:2px 6px;">📡 RÉPLICA</span>';
        
        const statusPill = s.ativo 
          ? '<span style="color:#10b981; font-weight:700; font-size:0.72rem;">● ATIVO NO POLLING</span>' 
          : '<span style="color:#94a3b8; font-weight:700; font-size:0.72rem;">○ INATIVO / PAUSADO</span>';

        left.innerHTML = \`
          <div style="display:flex; align-items:center; gap:8px;">
            <strong style="font-size:0.95rem; font-family:monospace; color:\${isOrig ? '#c084fc' : '#38bdf8'};">\${s.chave}</strong>
            <span style="font-weight:600; color:#f8fafc; font-size:0.88rem;">\${s.nome}</span>
            \${papelBadge}
            \${statusPill}
          </div>
          <div class="code" style="font-size:0.76rem; color:#94a3b8; word-break:break-all;">\${s.baseUrl || s.base_url}</div>
          <div id="srv_status_\${s.chave}" style="font-size:0.72rem; color:#64748b; margin-top:2px;"></div>
        \`;
        item.appendChild(left);

        const actions = document.createElement('div');
        actions.style.cssText = 'display:flex; gap:6px; align-items:center; flex-wrap:wrap;';

        // Botão Ativar / Desativar
        const btnToggle = document.createElement('button');
        btnToggle.className = 'btn-copy';
        btnToggle.innerText = s.ativo ? '⏸️ Pausar' : '▶️ Ativar';
        btnToggle.onclick = () => toggleServidor(s.chave, !s.ativo);
        actions.appendChild(btnToggle);

        // Botão Tornar Origem
        if (!isOrig) {
          const btnOrig = document.createElement('button');
          btnOrig.className = 'btn';
          btnOrig.style.cssText = 'background:#a855f7; padding:4px 10px; font-size:0.75rem;';
          btnOrig.innerText = '⭐ Tornar Origem';
          btnOrig.title = 'Definir este nó como referência master para cálculo de propagação e SLAs';
          btnOrig.onclick = () => setOrigem(s.chave);
          actions.appendChild(btnOrig);
        }

        // Botão Testar Conexão
        const btnTest = document.createElement('button');
        btnTest.className = 'btn-copy';
        btnTest.innerText = '📡 Testar';
        btnTest.title = 'Testar conexão HTTP e obter IP';
        btnTest.onclick = () => testSingleServer(s.chave, s.baseUrl || s.base_url);
        actions.appendChild(btnTest);

        // Botão Editar
        const btnEdit = document.createElement('button');
        btnEdit.className = 'btn-copy';
        btnEdit.innerText = '✏️';
        btnEdit.title = 'Editar nó';
        btnEdit.onclick = () => editServidor(s);
        actions.appendChild(btnEdit);

        // Botão Excluir
        if (!isOrig && list.length > 1) {
          const btnDel = document.createElement('button');
          btnDel.className = 'btn-copy';
          btnDel.style.color = '#ef4444';
          btnDel.innerText = '🗑️';
          btnDel.title = 'Remover nó da auditoria';
          btnDel.onclick = () => deleteServidor(s.chave);
          actions.appendChild(btnDel);
        }

        item.appendChild(actions);
        container.appendChild(item);
      }
    }

    function openServidoresModal() {
      document.getElementById('servidoresModal').style.display = 'flex';
      resetServidorForm();
      loadServidoresData();
    }

    function closeServidoresModal() {
      document.getElementById('servidoresModal').style.display = 'none';
      loadData();
    }

    function resetServidorForm() {
      editingServerChave = null;
      document.getElementById('srvFormTitle').textContent = '➕ Cadastrar Novo Nó / Servidor';
      document.getElementById('srvChaveInput').value = '';
      document.getElementById('srvChaveInput').disabled = false;
      document.getElementById('srvNomeInput').value = '';
      document.getElementById('srvUrlInput').value = '';
      document.getElementById('srvPapelSelect').value = 'REPLICA';
      document.getElementById('srvCancelBtn').style.display = 'none';
      document.getElementById('srvTestFeedback').textContent = '';
    }

    function editServidor(s) {
      editingServerChave = s.chave;
      document.getElementById('srvFormTitle').textContent = '✏️ Editando Servidor: ' + s.chave;
      document.getElementById('srvChaveInput').value = s.chave;
      document.getElementById('srvChaveInput').disabled = true;
      document.getElementById('srvNomeInput').value = s.nome;
      document.getElementById('srvUrlInput').value = s.baseUrl || s.base_url;
      document.getElementById('srvPapelSelect').value = s.papel;
      document.getElementById('srvCancelBtn').style.display = 'inline-block';
      document.getElementById('srvTestFeedback').textContent = '';
    }

    async function submitServidorForm() {
      const chave = document.getElementById('srvChaveInput').value.trim().toUpperCase();
      const nome = document.getElementById('srvNomeInput').value.trim();
      const baseUrl = document.getElementById('srvUrlInput').value.trim();
      const papel = document.getElementById('srvPapelSelect').value;

      if (!chave || !nome || !baseUrl) {
        alert('Por favor, preencha Chave, Nome e URL Base.');
        return;
      }

      try {
        const resp = await fetch('/api/servidores/salvar', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ chave, nome, baseUrl, papel, ativo: true })
        });
        const data = await resp.json();
        if (data.ok) {
          resetServidorForm();
          loadServidoresData();
          loadData();
        } else {
          alert('Erro ao salvar: ' + (data.error || 'Falha na requisição'));
        }
      } catch (err) {
        alert('Erro ao comunicar com o servidor: ' + err.message);
      }
    }

    async function toggleServidor(chave, ativo) {
      await fetch('/api/servidores/toggle', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ chave, ativo })
      });
      loadServidoresData();
      loadData();
    }

    async function setOrigem(chave) {
      if (!confirm('Deseja definir o nó "' + chave + '" como a ORIGEM de referência?\\n\\nTodos os cálculos de propagação e SLAs passarão a usar este nó como master.')) return;
      await fetch('/api/servidores/definir-origem', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ chave })
      });
      loadServidoresData();
      loadData();
    }

    async function deleteServidor(chave) {
      if (!confirm('Deseja realmente remover o servidor "' + chave + '" da auditoria?')) return;
      const resp = await fetch('/api/servidores/excluir', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ chave })
      });
      const data = await resp.json();
      if (!data.ok) {
        alert('Não foi possível excluir: ' + data.error);
      }
      loadServidoresData();
      loadData();
    }

    async function testFormServer() {
      const url = document.getElementById('srvUrlInput').value.trim();
      const feedback = document.getElementById('srvTestFeedback');
      if (!url) {
        feedback.textContent = 'Informe a URL para testar.';
        feedback.style.color = '#f87171';
        return;
      }
      feedback.textContent = 'Testando conexão HTTP...';
      feedback.style.color = '#38bdf8';

      try {
        const resp = await fetch('/api/servidores/testar', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ baseUrl: url })
        });
        const res = await resp.json();
        if (res.ok) {
          feedback.innerHTML = \`<span style="color:#10b981;">✓ HTTP \${res.statusCode}</span> (\${res.timeMs}ms) | IP: \${res.serverIp || 'N/A'} | Server: \${res.server || '-'}\`;
        } else {
          feedback.innerHTML = \`<span style="color:#f87171;">✗ Falha: HTTP \${res.statusCode || '0'}</span> (\${res.error || 'Sem resposta'})\`;
        }
      } catch (err) {
        feedback.textContent = 'Erro ao disparar teste: ' + err.message;
        feedback.style.color = '#f87171';
      }
    }

    async function testSingleServer(chave, url) {
      const el = document.getElementById('srv_status_' + chave);
      if (el) el.innerHTML = '<span style="color:#38bdf8;">Testando...</span>';
      try {
        const resp = await fetch('/api/servidores/testar', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ baseUrl: url })
        });
        const res = await resp.json();
        if (el) {
          if (res.ok) {
            el.innerHTML = \`<span style="color:#10b981;">✓ Respondeu em \${res.timeMs}ms</span> | IP Instância: <strong>\${res.serverIp || 'N/A'}</strong> | Server: \${res.server || '-'}\`;
          } else {
            el.innerHTML = \`<span style="color:#f87171;">✗ HTTP \${res.statusCode || '0'} (\${res.error || 'Erro'})</span>\`;
          }
        }
      } catch (err) {
        if (el) el.innerHTML = '<span style="color:#f87171;">Erro no teste: ' + err.message + '</span>';
      }
    }

    // --- MATRIZ COMPARATIVA MULTI-NÓS ---
    function openMultiNodeModal(relPathEncoded) {
      const relPath = decodeURIComponent(relPathEncoded);
      document.getElementById('multiNodePath').textContent = relPath;
      document.getElementById('multiNodeModal').style.display = 'flex';

      const tbody = document.getElementById('multiNodeTableBody');
      tbody.innerHTML = '';

      if (!latestApiData) return;

      const row = (latestApiData.comparison || []).find(c => c.relPath === relPath);
      if (!row) {
        tbody.innerHTML = '<tr><td colspan="11" style="text-align:center; padding:18px;">Arquivo não encontrado na coleta atual.</td></tr>';
        return;
      }

      const serversList = latestApiData.servers || [];
      const originKey = latestApiData.originKey || 'HMG';
      const originMeta = row.origin || row.hmg;

      for (const srv of serversList) {
        const srvState = row.allServers?.[srv.chave]?.meta || (srv.chave === originKey ? row.hmg : (srv.chave === row.comparison?.primaryReplicaKey ? row.sim : null));
        const srvUrl = (srv.baseUrl || '') + relPath;
        const isOrig = srv.chave === originKey;

        let deltaText = 'Origem (Referência)';
        let deltaBadge = '<span class="badge badge-sync">0m 00s (Master)</span>';
        let syncSlaBadge = '<span class="badge badge-sync">0m 00s</span>';

        if (!isOrig) {
          if (originMeta && srvState && originMeta.genTime !== null && srvState.genTime !== null) {
            const delaySec = Math.round((originMeta.genTime - srvState.genTime) / 1000);
            if (delaySec === 0) {
              deltaBadge = '<span class="badge badge-sync">0m 00s</span>';
            } else if (delaySec > 0) {
              deltaBadge = \`<span class="badge badge-lag">-\${formatMinSec(delaySec)}</span>\`;
            } else {
              deltaBadge = \`<span class="badge badge-ahead">+\${formatMinSec(-delaySec)}</span>\`;
            }
          } else {
            deltaBadge = '<span class="badge" style="background:#334155">Pendente</span>';
          }

          // Encontra SLA da réplica
          const repComp = row.comparison?.replicas?.find(r => r.chave === srv.chave);
          if (repComp) {
            syncSlaBadge = \`<span class="badge \${repComp.syncSlaStatus === 'CRITICO' ? 'badge-danger' : (repComp.syncSlaStatus === 'ALERTA' ? 'badge-lag' : 'badge-sync')}">⚡ \${repComp.syncSlaText}</span>\`;
          } else {
            syncSlaBadge = '<span class="badge" style="background:#334155">-</span>';
          }
        }

        const tr = document.createElement('tr');
        tr.style.background = isOrig ? 'rgba(168, 85, 247, 0.08)' : 'transparent';
        tr.style.borderBottom = '1px solid rgba(255,255,255,0.06)';

        tr.innerHTML = \`
          <td style="padding:10px 8px;">
            <strong style="color:\${isOrig ? '#c084fc' : '#38bdf8'}; font-family:monospace; font-size:0.92rem;">\${srv.chave}</strong>
            <br><span style="font-size:0.72rem; color:#94a3b8;">\${srv.nome}</span>
            <br>\${isOrig ? '<span class="tag-pill tag-uf" style="font-size:0.65rem;">ORIGEM</span>' : '<span class="tag-pill tag-eleicao" style="font-size:0.65rem;">RÉPLICA</span>'}
          </td>
          <td style="padding:10px 8px;">
            \${srvState?.serverIp ? \`<span class="tag-ip" title="Instância: \${srvState.serverIp}">📍 \${srvState.serverIp}</span>\` : '<span style="color:#64748b">-</span>'}
            <div style="font-size:0.70rem; color:#94a3b8; margin-top:2px;">\${srvState?.serverHeader || '-'}</div>
          </td>
          <td style="padding:10px 8px;">
            <strong style="font-size:0.95rem; color:\${isOrig ? '#c084fc' : '#f8fafc'};">\${srvState?.hg || 'Pendente'}</strong>
            <br><span style="font-size:0.74rem; color:#94a3b8;">\${srvState?.dg || '-'}</span>
          </td>
          <td style="padding:10px 8px;">
            \${(srvState?.dt && srvState?.ht) ? ('<strong style="font-size:0.90rem; color:#c084fc;">' + srvState.ht + '</strong><br><span style="font-size:0.72rem; color:#94a3b8;">' + srvState.dt + '</span>') : '<span style="color:#64748b">-</span>'}
          </td>
          <td style="padding:10px 8px;">
            \${(srvState?.st !== null && srvState?.st !== undefined) ? ('<strong style="font-size:0.88rem; color:#10b981;">' + Number(srvState.st).toLocaleString('pt-BR') + '</strong>' + (srvState.pst ? ('<br><span class="badge badge-sync" style="font-size:0.65rem; padding:1px 4px;">' + srvState.pst + '%</span>') : '')) : '<span style="color:#64748b">-</span>'}
          </td>
          <td style="padding:10px 8px; font-family:monospace; font-size:0.78rem; color:#94a3b8;">
            \${srvState?.idg || '-'}
          </td>
          <td style="padding:10px 8px;">
            \${deltaBadge}
          </td>
          <td style="padding:10px 8px;">
            \${syncSlaBadge}
          </td>
          <td style="padding:10px 8px;">
            <div class="code" style="font-size:0.72rem; color:\${srvState?.maxAge !== null ? '#10b981' : '#94a3b8'};">\${srvState?.cacheControl || '(sem Cache-Control)'}</div>
            <div style="font-size:0.70rem; color:#94a3b8;">TTL: \${srvState?.maxAge !== null && srvState?.maxAge !== undefined ? srvState.maxAge + 's' : '-'}</div>
          </td>
          <td style="padding:10px 8px;">
            <div style="font-size:0.72rem; color:#34d399;">\${srvState?.cdnCacheStatus || '-'}</div>
            <div class="code" style="font-size:0.68rem; color:#94a3b8; word-break:break-all;">\${srvState?.etag || '-'}</div>
          </td>
          <td style="padding:10px 8px; text-align:center;">
            <a href="\${srvUrl}" target="_blank" class="url-link" style="font-size:0.72rem; padding:2px 7px;">↗ Abrir</a>
            <button onclick="navigator.clipboard.writeText('\${srvUrl}'); this.innerText='✓'; setTimeout(()=>this.innerText='📋', 1200);" class="btn-copy" style="font-size:0.72rem; padding:2px 6px; margin-left:4px;" title="Copiar URL deste nó">📋</button>
          </td>
        \`;
        tbody.appendChild(tr);
      }
    }

    function closeMultiNodeModal() {
      document.getElementById('multiNodeModal').style.display = 'none';
    }

    // --- GERENCIAMENTO DE ELEIÇÕES NA INTERFACE ---
    let cachedEleicoesData = null;

    async function loadEleicoesData() {
      try {
        const resp = await fetch('/api/eleicoes');
        const data = await resp.json();
        cachedEleicoesData = data;
        renderEleicoesHeader(data.eleicoes, data.totalTracked);
        renderEleicoesFilterDropdown(data.eleicoes);
        renderEleicoesModalList(data.eleicoes);
      } catch (err) {
        console.error('Erro ao carregar eleicoes:', err);
      }
    }

    function renderEleicoesHeader(list, totalTracked) {
      const el = document.getElementById('headerEleicoesCount');
      if (!el) return;
      const ativas = list.filter(e => e.ativo).length;
      el.innerHTML = '<span style="color:#c084fc; font-weight:700;">' + ativas + ' de ' + list.length + ' ativas</span> <span style="font-size:0.72rem; color:#94a3b8;">(' + totalTracked + ' arqs)</span>';
    }

    function renderEleicoesFilterDropdown(list) {
      const sel = document.getElementById('filterEleicao');
      if (!sel) return;
      const curVal = sel.value;
      sel.innerHTML = '<option value="">Todas as Eleições</option>';
      for (const e of list) {
        const opt = document.createElement('option');
        opt.value = e.cd;
        opt.innerText = e.cd + ' - ' + e.nome;
        sel.appendChild(opt);
      }
      const optComum = document.createElement('option');
      optComum.value = 'Comum';
      optComum.innerText = 'Comum (Geral)';
      sel.appendChild(optComum);
      sel.value = curVal;
    }

    function renderEleicoesModalList(list) {
      const container = document.getElementById('eleicoesListContainer');
      const statsEl = document.getElementById('eleicoesModalStats');
      if (!container) return;

      const ativas = list.filter(e => e.ativo).length;
      if (statsEl) statsEl.innerText = ativas + ' de ' + list.length + ' eleições ativas no monitoramento';

      container.innerHTML = '';
      for (const e of list) {
        const item = document.createElement('div');
        item.style.cssText = 'display:flex; justify-content:space-between; align-items:center; background:' + (e.ativo ? 'rgba(168, 85, 247, 0.12)' : '#0f172a') + '; border:1px solid ' + (e.ativo ? '#a855f7' : '#334155') + '; padding:10px 14px; border-radius:8px; gap:12px; cursor:pointer; transition:all 0.15s;';
        
        item.onclick = (evt) => {
          if (evt.target.tagName !== 'INPUT') {
            const chk = item.querySelector('input[type="checkbox"]');
            chk.checked = !chk.checked;
            toggleEleicao(e.cd, chk.checked);
          }
        };

        const left = document.createElement('div');
        left.style.cssText = 'display:flex; align-items:center; gap:12px;';

        const chk = document.createElement('input');
        chk.type = 'checkbox';
        chk.checked = e.ativo;
        chk.style.cssText = 'width:18px; height:18px; cursor:pointer; accent-color:#a855f7;';
        chk.onchange = (evt) => {
          evt.stopPropagation();
          toggleEleicao(e.cd, chk.checked);
        };
        left.appendChild(chk);

        const info = document.createElement('div');
        const tpBadge = e.tipo === '8' ? '<span class="tag-pill tag-eleicao">Federal</span>' : (e.tipo === '1' ? '<span class="tag-pill tag-uf">Estadual</span>' : '<span class="tag-pill tag-cargo">Municipal</span>');
        info.innerHTML = '<div style="font-size:0.9rem; font-weight:700; color:' + (e.ativo ? '#f8fafc' : '#94a3b8') + ';">' + tpBadge + ' <strong>' + e.cd + '</strong> - ' + e.nome + '</div>' +
                         '<div style="font-size:0.75rem; color:#94a3b8; margin-top:2px;">Pleito: ' + e.pleito + ' | ' + e.ufsCount + ' Unidades Federativas atendidas</div>';
        left.appendChild(info);
        item.appendChild(left);

        const statusBadge = document.createElement('span');
        statusBadge.className = 'badge ' + (e.ativo ? 'badge-sync' : '');
        statusBadge.style.cssText = e.ativo ? '' : 'background:#334155; color:#94a3b8;';
        statusBadge.innerText = e.ativo ? 'MONITORANDO' : 'INATIVO';
        item.appendChild(statusBadge);

        container.appendChild(item);
      }
    }

    function openEleicoesModal() {
      document.getElementById('eleicoesModal').style.display = 'flex';
      loadEleicoesData();
    }

    function closeEleicoesModal() {
      document.getElementById('eleicoesModal').style.display = 'none';
      loadData();
    }

    async function toggleEleicao(cd, ativo) {
      await fetch('/api/eleicoes/toggle', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ cd, ativo })
      });
      loadEleicoesData();
    }

    async function toggleAllEleicoes(ativar) {
      await fetch('/api/eleicoes/selecionar-todas', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ ativar })
      });
      loadEleicoesData();
    }

    // --- LÓGICA DE CLIENTE PARA RODADAS ---
    let currentActiveRodadaInfo = null;

    async function loadRodadasList() {
      try {
        const resp = await fetch('/api/rodadas');
        const data = await resp.json();
        currentActiveRodadaInfo = data.active;
        renderRodadasHeader(data.active);
        renderRodadasList(data.list, data.active);
      } catch (err) {
        console.error('Erro ao listar rodadas:', err);
      }
    }

    function renderRodadasHeader(active) {
      const el = document.getElementById('headerRodadaName');
      if (!el) return;
      if (active) {
        const d = new Date(active.inicio_unix);
        const time = d.toLocaleTimeString('pt-BR');
        el.innerHTML = '<span style="color:#10b981; font-weight:700;">' + active.nome + '</span> <span style="font-size:0.72rem; color:#94a3b8;">(início ' + time + ')</span>';
      } else {
        el.innerText = 'Sem rodada ativa';
      }
    }

    function renderRodadasList(list, active) {
      const container = document.getElementById('rodadasListContainer');
      if (!container) return;
      if (!list || !list.length) {
        container.innerHTML = '<div style="color:#94a3b8; font-size:0.82rem;">Nenhuma rodada cadastrada.</div>';
        return;
      }

      container.innerHTML = '';
      for (const r of list) {
        const isActive = active && active.id === r.id;
        const d = new Date(r.inicio_unix);
        const item = document.createElement('div');
        item.style.cssText = 'display:flex; justify-content:space-between; align-items:center; background:' + (isActive ? 'rgba(16, 185, 129, 0.15)' : '#0f172a') + '; border:1px solid ' + (isActive ? '#10b981' : '#334155') + '; padding:10px 14px; border-radius:8px; gap:10px;';
        
        const info = document.createElement('div');
        info.innerHTML = '<div style="font-size:0.88rem; font-weight:700; color:' + (isActive ? '#10b981' : '#f8fafc') + ';">' + (isActive ? '🟢 ' : '') + r.nome + '</div>' +
                         '<div style="font-size:0.74rem; color:#94a3b8;">Início: ' + d.toLocaleDateString('pt-BR') + ' ' + d.toLocaleTimeString('pt-BR') + '</div>';
        item.appendChild(info);

        const actions = document.createElement('div');
        actions.style.cssText = 'display:flex; gap:6px; align-items:center;';

        if (!isActive) {
          const btnAtivar = document.createElement('button');
          btnAtivar.className = 'btn';
          btnAtivar.style.cssText = 'background:#0284c7; padding:4px 10px; font-size:0.75rem;';
          btnAtivar.innerText = 'Ativar';
          btnAtivar.onclick = () => activateRodada(r.id);
          actions.appendChild(btnAtivar);
        }

        const btnEdit = document.createElement('button');
        btnEdit.className = 'btn-copy';
        btnEdit.innerText = '✏️';
        btnEdit.title = 'Editar nome, data e hora da rodada';
        btnEdit.onclick = () => openEditRodadaModal(r);
        actions.appendChild(btnEdit);

        const btnDel = document.createElement('button');
        btnDel.className = 'btn-copy';
        btnDel.style.color = '#ef4444';
        btnDel.innerText = '🗑️';
        btnDel.title = 'Excluir marco desta rodada';
        btnDel.onclick = () => deleteRodadaPrompt(r.id);
        actions.appendChild(btnDel);

        item.appendChild(actions);
        container.appendChild(item);
      }
    }

    function openRodadasModal() {
      document.getElementById('rodadasModal').style.display = 'flex';
      loadRodadasList();
    }

    function closeRodadasModal() {
      document.getElementById('rodadasModal').style.display = 'none';
    }

    async function quickNewRodada() {
      if (!confirm('Deseja iniciar uma nova rodada do zero agora?\\n\\nO sistema passará a considerar este instante como marco zero para regressões e SLAs.')) return;
      await fetch('/api/rodadas/nova', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({}) });
      loadRodadasList();
      loadData();
    }

    async function submitNewRodada() {
      const input = document.getElementById('newRodadaInput');
      const nome = input.value.trim();
      await fetch('/api/rodadas/nova', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ nome })
      });
      input.value = '';
      loadRodadasList();
      loadData();
    }

    async function activateRodada(id) {
      await fetch('/api/rodadas/ativar', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ id })
      });
      loadRodadasList();
      loadData();
    }

    let currentEditingRodada = null;

    function openEditRodadaModal(r) {
      currentEditingRodada = r;
      document.getElementById('editRodadaId').value = r.id;
      document.getElementById('editRodadaNome').value = r.nome || '';
      
      const d = new Date(r.inicio_unix);
      const pad = n => String(n).padStart(2, '0');
      const localIso = d.getFullYear() + '-' + pad(d.getMonth()+1) + '-' + pad(d.getDate()) + 'T' + pad(d.getHours()) + ':' + pad(d.getMinutes()) + ':' + pad(d.getSeconds());
      document.getElementById('editRodadaInicio').value = localIso;

      const badge = document.getElementById('editRodadaBadge');
      if (badge) {
        const isActive = Boolean(r.ativo) || (currentActiveRodadaInfo && currentActiveRodadaInfo.id === r.id);
        badge.textContent = isActive ? '🟢 RODADA ATIVA' : ('#' + r.id);
        badge.style.color = isActive ? '#10b981' : '#38bdf8';
      }

      document.getElementById('editRodadaModal').style.display = 'flex';
      setTimeout(() => document.getElementById('editRodadaNome').focus(), 50);
    }

    function closeEditRodadaModal() {
      document.getElementById('editRodadaModal').style.display = 'none';
      currentEditingRodada = null;
    }

    function setEditRodadaToMidnight() {
      const now = new Date();
      const pad = n => String(n).padStart(2, '0');
      document.getElementById('editRodadaInicio').value = now.getFullYear() + '-' + pad(now.getMonth()+1) + '-' + pad(now.getDate()) + 'T00:00:00';
    }

    function setEditRodadaToNow() {
      const now = new Date();
      const pad = n => String(n).padStart(2, '0');
      document.getElementById('editRodadaInicio').value = now.getFullYear() + '-' + pad(now.getMonth()+1) + '-' + pad(now.getDate()) + 'T' + pad(now.getHours()) + ':' + pad(now.getMinutes()) + ':' + pad(now.getSeconds());
    }

    async function submitEditRodadaModal() {
      const id = document.getElementById('editRodadaId').value;
      const nome = document.getElementById('editRodadaNome').value.trim();
      const inicioStr = document.getElementById('editRodadaInicio').value;

      if (!nome) {
        alert('Por favor, informe um nome para a rodada.');
        return;
      }
      if (!inicioStr) {
        alert('Por favor, informe a data e hora de início.');
        return;
      }

      const inicioDate = new Date(inicioStr);
      if (isNaN(inicioDate.getTime())) {
        alert('Data/hora inválida!');
        return;
      }

      const inicioUnix = inicioDate.getTime();

      try {
        const res = await fetch('/api/rodadas/editar', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ id, nome, inicioUnix })
        });
        const data = await res.json();
        if (data.ok) {
          closeEditRodadaModal();
          loadRodadasList();
          loadData();
        } else {
          alert('Erro ao editar rodada: ' + (data.error || 'Erro desconhecido'));
        }
      } catch (err) {
        alert('Erro de conexão ao editar rodada: ' + err.message);
      }
    }

    async function deleteRodadaPrompt(id) {
      if (!confirm('Tem certeza que deseja excluir este marco de rodada?')) return;
      await fetch('/api/rodadas/excluir', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ id })
      });
      loadRodadasList();
      loadData();
    }

    let zipOptionsData = null;
    let currentZipJobId = null;
    let zipPollTimer = null;

    async function openZipModal() {
      const modal = document.getElementById('zipModal');
      modal.style.display = 'flex';
      document.getElementById('zipFormArea').style.display = 'block';
      document.getElementById('zipProgressArea').style.display = 'none';
      document.getElementById('zipFinishedActions').style.display = 'none';
      document.getElementById('zipStartBtn').disabled = false;
      document.getElementById('zipProgressBar').style.width = '0%';
      document.getElementById('zipProgressPercent').innerText = '0%';

      try {
        const resp = await fetch('/api/zip/options');
        zipOptionsData = await resp.json();
        
        const sel = document.getElementById('zipElectionSelect');
        sel.innerHTML = '<option value="ALL">📦 Todas as Eleições</option>';
        if (zipOptionsData && zipOptionsData.elections) {
          for (const el of Object.keys(zipOptionsData.elections).sort()) {
            const count = zipOptionsData.elections[el];
            const opt = document.createElement('option');
            opt.value = el;
            opt.innerText = 'Eleição ' + el + ' (' + count + ' arquivos)';
            sel.appendChild(opt);
          }
        }
        updateZipEst();
      } catch (err) {
        console.error('Erro ao carregar opcoes de ZIP:', err);
      }
    }

    function closeZipModal() {
      if (zipPollTimer) clearInterval(zipPollTimer);
      document.getElementById('zipModal').style.display = 'none';
    }

    async function updateZipEst() {
      const apenasRodada = document.getElementById('zipApenasRodadaCheck') && document.getElementById('zipApenasRodadaCheck').checked;
      try {
        const resp = await fetch('/api/zip/options?apenasRodada=' + (apenasRodada ? 'true' : 'false'));
        zipOptionsData = await resp.json();
      } catch {}

      if (!zipOptionsData) return;
      const el = document.getElementById('zipElectionSelect').value;
      let count = 0;
      if (el === 'ALL') {
        count = zipOptionsData.totalFiles || 0;
      } else {
        count = (zipOptionsData.elections && zipOptionsData.elections[el]) || 0;
      }
      document.getElementById('zipEstFiles').innerText = count.toLocaleString('pt-BR') + ' arquivos';
      const estMb = ((count * 30) / 1024).toFixed(1);
      document.getElementById('zipEstSize').innerText = '~' + estMb + ' MB (estimado compactado)';
    }

    async function startZipExport() {
      const el = document.getElementById('zipElectionSelect').value;
      document.getElementById('zipFormArea').style.display = 'none';
      document.getElementById('zipProgressArea').style.display = 'block';
      document.getElementById('zipProgressStage').innerText = 'Inicializando compactador...';
      document.getElementById('zipProgressPercent').innerText = '0%';
      document.getElementById('zipProgressBar').style.width = '0%';
      document.getElementById('zipFinishedActions').style.display = 'none';

      try {
        const resp = await fetch('/api/zip/start', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ eleicao: el, apenasRodada: document.getElementById('zipApenasRodadaCheck')?.checked || false })
        });
        const data = await resp.json();
        if (!data.jobId) {
          alert('Erro ao iniciar compactação: ' + (data.error || 'Desconhecido'));
          closeZipModal();
          return;
        }

        currentZipJobId = data.jobId;
        pollZipProgress();
      } catch (err) {
        alert('Falha na requisição de compactação: ' + err.message);
        closeZipModal();
      }
    }

    function pollZipProgress() {
      if (zipPollTimer) clearInterval(zipPollTimer);
      zipPollTimer = setInterval(async () => {
        if (!currentZipJobId) return;
        try {
          const resp = await fetch('/api/zip/status?jobId=' + encodeURIComponent(currentZipJobId));
          const status = await resp.json();

          if (status.status === 'SCANNING') {
            document.getElementById('zipProgressStage').innerText = 'Localizando arquivos no disco...';
            document.getElementById('zipProgressCounts').innerText = status.done + ' encontrados';
          } else if (status.status === 'PROCESSING') {
            document.getElementById('zipProgressStage').innerText = 'Compactando em streaming (.zip)...';
            const pct = status.percent || 0;
            document.getElementById('zipProgressBar').style.width = pct + '%';
            document.getElementById('zipProgressPercent').innerText = pct + '%';
            document.getElementById('zipProgressCounts').innerText = (status.done || 0).toLocaleString('pt-BR') + ' / ' + (status.total || 0).toLocaleString('pt-BR') + ' arquivos';
            if (status.currentFile) {
              document.getElementById('zipProgressCurrentFile').innerText = status.currentFile;
            }
          } else if (status.status === 'READY') {
            clearInterval(zipPollTimer);
            document.getElementById('zipProgressBar').style.width = '100%';
            document.getElementById('zipProgressPercent').innerText = '100%';
            document.getElementById('zipProgressStage').innerText = '✅ Concluído com sucesso!';
            document.getElementById('zipProgressCounts').innerText = (status.total || 0).toLocaleString('pt-BR') + ' arquivos compactados (' + (status.fileSizeMb || '0') + ' MB)';
            document.getElementById('zipProgressCurrentFile').innerText = 'Download iniciado automaticamente!';
            
            const downloadUrl = '/api/zip/download?jobId=' + encodeURIComponent(currentZipJobId);
            document.getElementById('zipDirectDownloadLink').href = downloadUrl;
            document.getElementById('zipFinishedActions').style.display = 'flex';

            // Trigger download
            window.location.href = downloadUrl;
          } else if (status.status === 'ERROR') {
            clearInterval(zipPollTimer);
            document.getElementById('zipProgressStage').innerText = '❌ Erro na compactação';
            document.getElementById('zipProgressCurrentFile').innerText = status.error || 'Erro desconhecido';
            document.getElementById('zipFinishedActions').style.display = 'flex';
          }
        } catch (err) {
          console.error('Erro no polling de ZIP:', err);
        }
      }, 300);
    }

    function clearLogs() {
      document.getElementById('logFeed').innerHTML = '';
    }

    // --- MODAL DE COMPARATIVO DE CACHE & HEADERS ---
    function openCacheModal(encodedRelPath) {
      const relPath = decodeURIComponent(encodedRelPath);
      const row = rawComparisonList.find(r => r.relPath === relPath);
      if (!row) return;

      document.getElementById('cacheModalPath').innerText = row.relPath;
      const hmg = row.hmg || {};
      const sim = row.sim || {};

      document.getElementById('mHmgCc').innerText = hmg.cacheControl || '(nenhum)';
      document.getElementById('mHmgTtl').innerText = hmg.maxAge !== null && hmg.maxAge !== undefined ? hmg.maxAge + 's' : 'Sem TTL (Origin Default)';
      document.getElementById('mHmgCdn').innerText = hmg.cdnCacheStatus || 'ORIGIN (Apache)';
      document.getElementById('mHmgEtag').innerText = hmg.etag || '-';
      document.getElementById('mHmgLm').innerText = hmg.lastModifiedHeader || '-';
      document.getElementById('mHmgIp').innerText = hmg.serverIp || '192.168.218.33';
      document.getElementById('mHmgServer').innerText = hmg.serverHeader || 'Apache';

      document.getElementById('mSimCc').innerText = sim.cacheControl || '(nenhum)';
      document.getElementById('mSimTtl').innerText = sim.maxAge !== null && sim.maxAge !== undefined ? sim.maxAge + 's' : '-';
      document.getElementById('mSimCdn').innerText = sim.cdnCacheStatus || 'Hit from child';
      document.getElementById('mSimEtag').innerText = sim.etag || '-';
      document.getElementById('mSimLm').innerText = sim.lastModifiedHeader || '-';
      document.getElementById('mSimIp').innerText = sim.serverIp || 'Aguardando coleta...';
      document.getElementById('mSimServer').innerText = sim.serverHeader || 'Akamai CDN';
      if (document.getElementById('mSimGrn')) {
        document.getElementById('mSimGrn').innerText = sim.akamaiGrn || '-';
      }

      // Análise automática
      let analysis = '';
      if (!hmg.cacheControl || hmg.cacheControl.includes('nenhum')) {
        analysis += '⚠️ <strong>Origem HMG sem Cache-Control explícito:</strong> A origem Apache não define diretiva de cache no payload. A Akamai (SIM) assume a política configurada no Edge com TTL de ~60s (<code>max-age=60</code> decrescente).<br>';
      }
      if (sim.maxAge !== null) {
        analysis += '⏱️ <strong>TTL de Borda (Edge):</strong> O SIM reporta <code>max-age=' + sim.maxAge + 's</code>, significando que a réplica da CDN expira em ' + sim.maxAge + ' segundos antes de revalidar com a origem HMG.<br>';
      }
      if (sim.serverIp) {
        analysis += '📍 <strong>Diagnóstico de Instância Borda:</strong> Esta resposta foi servida pelo nó/PoP Akamai <code>' + sim.serverIp + '</code>. Se outra requisição for atendida por um PoP diferente que ainda retém versão anterior em cache, ocorrerá uma regressão percebida pelo usuário.<br>';
      }
      if (hmg.etag && sim.etag && hmg.etag !== sim.etag) {
        analysis += '🏷️ <strong>ETags Distintos:</strong> HMG utiliza ETag de Apache (<code>' + hmg.etag + '</code>) enquanto SIM utiliza ETag de CDN/S3 (<code>' + sim.etag + '</code>). A revalidação condicional pode agir de forma independente.';
      } else {
        analysis += '✅ <strong>Consistência de Entrega:</strong> Objeto servido com sucesso pela borda Akamai com entrega aos usuários via CDN.';
      }
      document.getElementById('cacheAnalysisText').innerHTML = analysis;

      document.getElementById('cacheModal').style.display = 'flex';
    }

    function closeCacheModal() {
      document.getElementById('cacheModal').style.display = 'none';
    }

    function escapeHtml(str) {
      if (str === null || str === undefined) return '';
      return String(str)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#039;');
    }

    let allRegressoesData = [];

    const VERSION_PALETTES = [
      { bg: 'rgba(6, 182, 212, 0.18)', border: '#0891b2', text: '#22d3ee', badgeBg: '#0891b2', name: 'Ciano' },
      { bg: 'rgba(16, 185, 129, 0.18)', border: '#059669', text: '#34d399', badgeBg: '#059669', name: 'Esmeralda' },
      { bg: 'rgba(245, 158, 11, 0.18)', border: '#d97706', text: '#fbbf24', badgeBg: '#d97706', name: 'Âmbar' },
      { bg: 'rgba(168, 85, 247, 0.18)', border: '#9333ea', text: '#c084fc', badgeBg: '#9333ea', name: 'Roxo' },
      { bg: 'rgba(249, 115, 22, 0.18)', border: '#ea580c', text: '#fb923c', badgeBg: '#ea580c', name: 'Laranja' },
      { bg: 'rgba(236, 72, 153, 0.18)', border: '#db2777', text: '#f472b6', badgeBg: '#db2777', name: 'Rosa' },
      { bg: 'rgba(59, 130, 246, 0.18)', border: '#2563eb', text: '#60a5fa', badgeBg: '#2563eb', name: 'Azul' },
      { bg: 'rgba(132, 204, 22, 0.18)', border: '#65a30d', text: '#a3e635', badgeBg: '#65a30d', name: 'Lima' },
      { bg: 'rgba(99, 102, 241, 0.18)', border: '#4f46e5', text: '#818cf8', badgeBg: '#4f46e5', name: 'Índigo' },
      { bg: 'rgba(244, 63, 94, 0.18)', border: '#e11d48', text: '#fb7185', badgeBg: '#e11d48', name: 'Rubi' },
      { bg: 'rgba(20, 184, 166, 0.18)', border: '#0d9488', text: '#2dd4bf', badgeBg: '#0d9488', name: 'Teal' },
      { bg: 'rgba(234, 179, 8, 0.18)', border: '#ca8a04', text: '#fde047', badgeBg: '#ca8a04', name: 'Dourado' }
    ];

    function getVersionKey(dg, hg, idg) {
      const d = (dg || '').trim();
      const h = (hg || '').trim();
      const i = (idg || '').trim();
      if (!d && !h && !i) return '';
      return (d + ' ' + h).trim() + (i ? ('#' + i) : '');
    }

    const expandedRegCardIds = new Set();

    function toggleRegCard(id, event) {
      if (event) event.stopPropagation();
      if (expandedRegCardIds.has(id)) {
        expandedRegCardIds.delete(id);
      } else {
        expandedRegCardIds.add(id);
      }
      updateCardCollapsedState(id);
      updateToggleAllBtn();
    }

    function toggleAllRegCards() {
      const allVisibleIds = Array.from(document.querySelectorAll('#regressoesListContainer .regression-card')).map(function(el) {
        return parseInt(el.getAttribute('data-regression-id'), 10);
      }).filter(Boolean);
      const allExpanded = allVisibleIds.length > 0 && allVisibleIds.every(function(id) { return expandedRegCardIds.has(id); });
      if (allExpanded) {
        expandedRegCardIds.clear();
      } else {
        allVisibleIds.forEach(function(id) { expandedRegCardIds.add(id); });
      }
      allVisibleIds.forEach(function(id) { updateCardCollapsedState(id); });
      updateToggleAllBtn();
    }

    function updateCardCollapsedState(id) {
      const bodyEl = document.getElementById('regCardBody_' + id);
      const chevronEl = document.getElementById('regChevron_' + id);
      const btnTextEl = document.getElementById('regBtnText_' + id);
      const cardEl = document.getElementById('regCard_' + id);
      const isExp = expandedRegCardIds.has(id);

      if (bodyEl) bodyEl.style.display = isExp ? 'block' : 'none';
      if (chevronEl) chevronEl.textContent = isExp ? '▼' : '▶';
      if (btnTextEl) btnTextEl.textContent = isExp ? 'Recolher' : 'Detalhes';
      if (cardEl) {
        cardEl.style.padding = isExp ? '14px' : '10px 14px';
      }
    }

    function updateToggleAllBtn() {
      const text = document.getElementById('toggleAllRegsText');
      if (!text) return;
      const allVisibleIds = Array.from(document.querySelectorAll('#regressoesListContainer .regression-card')).map(function(el) {
        return parseInt(el.getAttribute('data-regression-id'), 10);
      }).filter(Boolean);
      const allExpanded = allVisibleIds.length > 0 && allVisibleIds.every(function(id) { return expandedRegCardIds.has(id); });
      text.textContent = allExpanded ? 'Colapsar Todos' : 'Expandir Todos';
    }

    async function openRegressoesModal() {
      document.getElementById('regressoesModal').style.display = 'flex';
      await loadRegressoesData();
    }

    function closeRegressoesModal() {
      document.getElementById('regressoesModal').style.display = 'none';
    }

    let regModalPageSize = 50;

    async function loadRegressoesData() {
      const container = document.getElementById('regressoesListContainer');
      container.innerHTML = '<div style="text-align:center; padding:40px; color:#94a3b8; font-size:0.9rem;">⏳ Carregando ocorrências de regressão e evidências forenses...</div>';
      try {
        const res = await fetch('/api/regressoes?limit=500');
        const data = await res.json();
        allRegressoesData = data.regressoes || [];
        
        document.getElementById('regModalRodadaNome').textContent = (data.rodada && data.rodada.nome) ? data.rodada.nome : 'Rodada Atual';
        document.getElementById('regModalTotalCount').textContent = data.total !== undefined ? data.total : allRegressoesData.length;
        
        // Popula seletor de UFs se disponível
        const ufSelect = document.getElementById('regFilterUf');
        if (ufSelect) {
          const currentUf = ufSelect.value;
          const ufs = Array.from(new Set(allRegressoesData.map(r => (r.fileMeta && r.fileMeta.uf) ? r.fileMeta.uf.toUpperCase() : '').filter(Boolean))).sort();
          ufSelect.innerHTML = '<option value="">Todas as UFs</option>' + ufs.map(u => '<option value="' + u + '">' + u + '</option>').join('');
          ufSelect.value = currentUf;
        }

        renderFilteredRegressoes(true);
      } catch(err) {
        container.innerHTML = '<div style="text-align:center; padding:40px; color:#ef4444; font-size:0.9rem;">❌ Erro ao carregar regressões: ' + err.message + '</div>';
      }
    }

    async function buscarRegressoesRemoto() {
      const searchInput = document.getElementById('regSearchInput');
      const grnInput = document.getElementById('regFilterGrn');
      const serverInput = document.getElementById('regFilterServer');
      const ufInput = document.getElementById('regFilterUf');
      const critInput = document.getElementById('regFilterCriterion');

      const q = (searchInput ? searchInput.value : '').replace(/^#/, '').trim();
      const grn = (grnInput ? grnInput.value : '').trim();
      const server = (serverInput ? serverInput.value : '').trim();
      const uf = (ufInput ? ufInput.value : '').trim();
      const crit = (critInput ? critInput.value : '').trim();

      if (!q && !grn && !server && !uf && !crit) {
        await loadRegressoesData();
        return;
      }

      const container = document.getElementById('regressoesListContainer');
      container.innerHTML = '<div style="text-align:center; padding:40px; color:#94a3b8; font-size:0.9rem;">⏳ Buscando no histórico completo do banco SQLite...</div>';
      try {
        let fetchUrl = '/api/regressoes?limit=5000&all=1';
        if (q) fetchUrl += '&q=' + encodeURIComponent(q);
        if (grn) fetchUrl += '&grn=' + encodeURIComponent(grn);
        if (server) fetchUrl += '&servidor=' + encodeURIComponent(server);
        if (uf) fetchUrl += '&uf=' + encodeURIComponent(uf);
        if (crit) fetchUrl += '&criterio=' + encodeURIComponent(crit);

        const res = await fetch(fetchUrl);
        const data = await res.json();
        if (data.regressoes && data.regressoes.length > 0) {
          allRegressoesData = data.regressoes;
          const searchTerms = [q ? '#' + q : '', grn ? 'GRN:' + grn : '', server, uf, crit].filter(Boolean).join(' | ');
          document.getElementById('regModalRodadaNome').textContent = 'Busca Histórica (' + (searchTerms || 'Filtros') + ')';
          document.getElementById('regModalTotalCount').textContent = data.regressoes.length;
          renderFilteredRegressoes(true);
        } else {
          container.innerHTML = '<div style="text-align:center; padding:40px; color:#ef4444;">Nenhuma ocorrência encontrada no banco para os filtros informados.</div>';
          const badge = document.getElementById('regShowingCount');
          if (badge) badge.textContent = 'Exibindo 0 de 0';
        }
      } catch(e) {
        container.innerHTML = '<div style="text-align:center; padding:40px; color:#ef4444;">Erro na busca remota: ' + e.message + '</div>';
      }
    }

    function renderFilteredRegressoes(resetPage) {
      if (resetPage) regModalPageSize = 50;
      const container = document.getElementById('regressoesListContainer');
      const search = (document.getElementById('regSearchInput') ? document.getElementById('regSearchInput').value : '').toLowerCase().trim();
      const server = document.getElementById('regFilterServer') ? document.getElementById('regFilterServer').value : '';
      const ufFilter = document.getElementById('regFilterUf') ? document.getElementById('regFilterUf').value.toLowerCase() : '';
      const criterion = document.getElementById('regFilterCriterion') ? document.getElementById('regFilterCriterion').value : '';
      const grnFilter = (document.getElementById('regFilterGrn') ? document.getElementById('regFilterGrn').value : '').toLowerCase().trim();

      const cleanSearch = search.replace(/^#/, '');

      const filtered = allRegressoesData.filter(function(r) {
        if (server && r.servidor !== server) return false;
        if (ufFilter) {
          const rUf = (r.fileMeta && r.fileMeta.uf ? r.fileMeta.uf : '').toLowerCase();
          if (rUf !== ufFilter) return false;
        }
        if (criterion) {
          const c = (r.criterio || '').toUpperCase();
          const m = (r.motivo || '').toUpperCase();
          const d = (r.detalhes || '').toUpperCase();
          if (criterion === 'INVERSAO_DG_DT_ST') {
            const hasInversionTag = c.includes('INVERSÃO') || m.includes('INVERSÃO') || d.includes('INVERSÃO') || m.includes('INVERSÃO_DG_DT_ST') || d.includes('INVERSÃO_DG_DT_ST');
            const dgAdv = Boolean(r.dg_anterior && r.dg_recebido && r.hg_anterior && r.hg_recebido && (
              r.dg_recebido > r.dg_anterior || (r.dg_recebido === r.dg_anterior && r.hg_recebido >= r.hg_anterior)
            ));
            const totReg = Boolean(r.dt_recebido && r.dt_anterior && (r.dt_recebido < r.dt_anterior || (r.dt_recebido === r.dt_anterior && r.ht_recebido < r.ht_anterior)));
            const stReg = Boolean(r.secoes_recebido !== null && r.secoes_anterior !== null && Number(r.secoes_recebido) < Number(r.secoes_anterior));
            if (!hasInversionTag && !(dgAdv && (totReg || stReg))) return false;
          } else {
            if (!c.includes(criterion) && !m.includes(criterion)) return false;
          }
        }
        if (grnFilter) {
          const rGrn = (r.akamai_grn || '').toLowerCase();
          const rRawGrn = (r.rawMeta && r.rawMeta.headers && (r.rawMeta.headers['akamai-grn'] || r.rawMeta.headers['x-akamai-grn']) ? String(r.rawMeta.headers['akamai-grn'] || r.rawMeta.headers['x-akamai-grn']) : '').toLowerCase();
          const matchTimelineGrn = (r.timeline || []).some(function(step) {
            return step.akamai_grn && String(step.akamai_grn).toLowerCase().includes(grnFilter);
          });
          if (!rGrn.includes(grnFilter) && !rRawGrn.includes(grnFilter) && !matchTimelineGrn) return false;
        }
        if (cleanSearch) {
          const matchId = String(r.id || '') === cleanSearch || String(r.id || '').includes(cleanSearch);
          const matchIdg = String(r.idg_recebido || '').includes(cleanSearch) || String(r.idg_anterior || '').includes(cleanSearch);
          const matchFile = (r.arquivo || '').toLowerCase().includes(cleanSearch);
          const matchMotivo = (r.motivo || '').toLowerCase().includes(cleanSearch);
          const matchServer = (r.servidor || '').toLowerCase().includes(cleanSearch);
          const matchUf = (r.fileMeta && r.fileMeta.uf ? r.fileMeta.uf : '').toLowerCase().includes(cleanSearch);
          const matchCargo = (r.fileMeta && r.fileMeta.cargo ? r.fileMeta.cargo : '').toLowerCase().includes(cleanSearch);
          const matchIp = (r.rawMeta && r.rawMeta.headers && r.rawMeta.headers['x-server-ip'] ? r.rawMeta.headers['x-server-ip'] : (r.rawMeta && r.rawMeta.serverIp ? r.rawMeta.serverIp : '')).toLowerCase().includes(cleanSearch);
          const matchGrn = (r.akamai_grn || '').toLowerCase().includes(cleanSearch) || (r.rawMeta && r.rawMeta.headers && String(r.rawMeta.headers['akamai-grn'] || '').toLowerCase().includes(cleanSearch));
          const matchTimelineGrn = (r.timeline || []).some(function(step) {
            return step.akamai_grn && String(step.akamai_grn).toLowerCase().includes(cleanSearch);
          });
          if (!matchId && !matchIdg && !matchFile && !matchMotivo && !matchServer && !matchUf && !matchCargo && !matchIp && !matchGrn && !matchTimelineGrn) return false;
        }
        return true;
      });

      const countEl = document.getElementById('regShowingCount');
      const isFiltered = Boolean(search || server || ufFilter || criterion || grnFilter);
      const limitToShow = isFiltered ? Math.max(regModalPageSize, filtered.length) : regModalPageSize;
      const displayItems = filtered.slice(0, limitToShow);

      if (countEl) {
        countEl.textContent = 'Exibindo ' + displayItems.length + ' de ' + filtered.length + ' caso(s)' + (allRegressoesData.length !== filtered.length ? ' (filtrado de ' + allRegressoesData.length + ')' : '');
      }

      if (filtered.length === 0) {
        if (allRegressoesData.length === 0) {
          container.innerHTML = '<div style="text-align:center; padding:50px 20px; color:#10b981;">' +
            '<div style="font-size:2.5rem; margin-bottom:10px;">✅</div>' +
            '<div style="font-size:1.1rem; font-weight:700; color:#f8fafc;">Nenhuma regressão detectada nesta rodada!</div>' +
            '<div style="font-size:0.84rem; color:#94a3b8; margin-top:6px; max-width:500px; margin-left:auto; margin-right:auto;">' +
              'Todos os arquivos consultados em HMG, SIM e nós adicionais mantiveram estrita monotonicidade cronológica e sequencial.' +
            '</div>' +
          '</div>';
        } else {
          container.innerHTML = '<div style="text-align:center; padding:40px; color:#94a3b8;">' +
            '<div style="font-size:1rem; margin-bottom:10px;">Nenhum caso na rodada atual corresponde aos filtros selecionados.</div>' +
            (cleanSearch ? '<button onclick="buscarRegressoesRemoto()" class="btn-copy" style="padding:8px 16px; font-size:0.82rem; background:#2563eb; color:#fff; cursor:pointer;">🔍 Buscar termo no histórico completo do banco SQLite</button>' : '') +
          '</div>';
        }
        return;
      }

      let html = '';
      for (let i = 0; i < displayItems.length; i++) {
        const r = displayItems[i];
        const rawHeaders = (r.rawMeta && (r.rawMeta.response_headers || r.rawMeta.headers)) || r.response_headers || {};

        const dateObj = new Date(r.timestamp_iso);
        const timeStr = dateObj.toLocaleTimeString('pt-BR') + ' (' + dateObj.toLocaleDateString('pt-BR') + ')';
        const elapsedMinutes = Math.round((Date.now() - dateObj.getTime()) / 60000);
        const elapsedText = elapsedMinutes <= 0 ? 'agora há pouco' : ('há ' + elapsedMinutes + ' min');

        const crit = (r.criterio || 'TEMPO (DG/HG)').toUpperCase();
        let critBadgesHtml = '';
        if (crit.includes('TEMPO') || crit.includes('TEMPORAL') || (r.motivo && r.motivo.includes('REGRESSÃO TEMPORAL'))) {
          critBadgesHtml += '<span style="background:rgba(239,68,68,0.25); color:#ef4444; border:1px solid #ef4444; padding:2px 8px; border-radius:4px; font-size:0.72rem; font-weight:700;">🚨 TEMPO (DG/HG)</span> ';
        }
        if (crit.includes('TOTALIZAÇÃO') || (r.motivo && r.motivo.includes('TOTALIZAÇÃO'))) {
          critBadgesHtml += '<span style="background:rgba(249,115,22,0.25); color:#f97316; border:1px solid #f97316; padding:2px 8px; border-radius:4px; font-size:0.72rem; font-weight:700;">🚨 TOTALIZAÇÃO (DT/HT)</span> ';
        }
        if (crit.includes('SEÇÕES') || (r.motivo && r.motivo.includes('SEÇÕES APURADAS'))) {
          critBadgesHtml += '<span style="background:rgba(236,72,153,0.25); color:#ec4899; border:1px solid #ec4899; padding:2px 8px; border-radius:4px; font-size:0.72rem; font-weight:700;">🚨 SEÇÕES (ST)</span> ';
        }
        if (r.motivo && r.motivo.includes('ANOMALIA SEQUENCIAL (IDG)')) {
          critBadgesHtml += '<span style="background:rgba(168,85,247,0.25); color:#c084fc; border:1px solid #c084fc; padding:2px 8px; border-radius:4px; font-size:0.72rem; font-weight:600;">⚠️ IDG</span> ';
        }

        const serverBadgeClass = r.servidor === 'HMG' ? 'tag-hmg-title' : 'tag-sim-title';
        const serverRoleDesc = r.papel_servidor || (r.servidor === 'HMG' ? 'Fonte Oficial' : 'Cache Akamai');

        const prevTotStr = (r.dt_anterior ? r.dt_anterior + ' ' + (r.ht_anterior || '') : '-');
        const currTotStr = (r.dt_recebido ? r.dt_recebido + ' ' + (r.ht_recebido || '') : '-');
        const isTotRegression = Boolean(r.dt_recebido && r.dt_anterior && (r.dt_recebido < r.dt_anterior || (r.dt_recebido === r.dt_anterior && r.ht_recebido < r.ht_anterior)));

        const prevStStr = (r.secoes_anterior !== null && r.secoes_anterior !== undefined ? r.secoes_anterior + ' seç' : '-');
        const currStStr = (r.secoes_recebido !== null && r.secoes_recebido !== undefined ? r.secoes_recebido + ' seç' : '-');
        const isStRegression = Boolean(r.secoes_recebido !== null && r.secoes_anterior !== null && Number(r.secoes_recebido) < Number(r.secoes_anterior));

        const dgAdvOrSame = Boolean(r.dg_anterior && r.dg_recebido && r.hg_anterior && r.hg_recebido && (
          r.dg_recebido > r.dg_anterior || (r.dg_recebido === r.dg_anterior && r.hg_recebido >= r.hg_anterior)
        ));
        const isInversion = Boolean(crit.includes('INVERSÃO') || (r.motivo && r.motivo.includes('INVERSÃO')) || (r.detalhes && r.detalhes.includes('INVERSÃO')) || (dgAdvOrSame && (isTotRegression || isStRegression)));

        if (isInversion) {
          if (isTotRegression && isStRegression) {
            critBadgesHtml += '<span style="background:rgba(244,63,94,0.3); color:#fda4af; border:1px solid #f43f5e; padding:2px 8px; border-radius:4px; font-size:0.72rem; font-weight:800;" title="Arquivo mais novo em DG/HG, porém DT/HT e ST retrocederam!">🚨 INVERSÃO: DG ↗ | DT/ST ↘</span> ';
          } else if (isTotRegression) {
            critBadgesHtml += '<span style="background:rgba(244,63,94,0.3); color:#fda4af; border:1px solid #f43f5e; padding:2px 8px; border-radius:4px; font-size:0.72rem; font-weight:800;" title="Arquivo mais novo em DG/HG, porém Totalização (DT/HT) retrocedeu!">🚨 INVERSÃO: DG ↗ | DT ↘</span> ';
          } else if (isStRegression) {
            critBadgesHtml += '<span style="background:rgba(244,63,94,0.3); color:#fda4af; border:1px solid #f43f5e; padding:2px 8px; border-radius:4px; font-size:0.72rem; font-weight:800;" title="Arquivo mais novo em DG/HG, porém Seções Apuradas (ST) diminuíram!">🚨 INVERSÃO: DG ↗ | ST ↘</span> ';
          }
        }

        const uf = (r.fileMeta && r.fileMeta.uf) ? r.fileMeta.uf : '-';
        const cargo = (r.fileMeta && r.fileMeta.cargo) ? r.fileMeta.cargo : '-';
        const tipo = (r.fileMeta && r.fileMeta.tipo) ? r.fileMeta.tipo : '-';
        const eleicao = (r.fileMeta && r.fileMeta.eleicao) ? r.fileMeta.eleicao : '-';
        const motivoTexto = escapeHtml(r.motivo || r.detalhes || '');

        // Construção do Esquema Cronológico (Linha do Tempo de Requisições)
        const timelineList = r.timeline || [];

        // Mapeamento e identificação de versões distintas para a trilha visual
        const versionMap = new Map();
        let verCounter = 1;

        for (let tIdx = 0; tIdx < timelineList.length; tIdx++) {
          const step = timelineList[tIdx];
          const vKey = getVersionKey(step.dg, step.hg, step.idg);
          if (vKey && !versionMap.has(vKey)) {
            const paletteIndex = (verCounter - 1) % VERSION_PALETTES.length;
            versionMap.set(vKey, {
              index: verCounter,
              label: 'V' + verCounter,
              palette: VERSION_PALETTES[paletteIndex],
              dg: step.dg || '',
              hg: step.hg || '',
              idg: step.idg || '',
              count: 0
            });
            verCounter++;
          }
          if (vKey && versionMap.has(vKey)) {
            versionMap.get(vKey).count++;
          }
        }

        const prevVKey = getVersionKey(r.dg_anterior, r.hg_anterior, r.idg_anterior);
        const currVKey = getVersionKey(r.dg_recebido, r.hg_recebido, r.idg_recebido);

        if (prevVKey && !versionMap.has(prevVKey)) {
          const paletteIndex = (verCounter - 1) % VERSION_PALETTES.length;
          versionMap.set(prevVKey, {
            index: verCounter,
            label: 'V' + verCounter,
            palette: VERSION_PALETTES[paletteIndex],
            dg: r.dg_anterior || '',
            hg: r.hg_anterior || '',
            idg: r.idg_anterior || '',
            count: 0
          });
          verCounter++;
        }

        if (currVKey && !versionMap.has(currVKey)) {
          const paletteIndex = (verCounter - 1) % VERSION_PALETTES.length;
          versionMap.set(currVKey, {
            index: verCounter,
            label: 'V' + verCounter,
            palette: VERSION_PALETTES[paletteIndex],
            dg: r.dg_recebido || '',
            hg: r.hg_recebido || '',
            idg: r.idg_recebido || '',
            count: 0
          });
          verCounter++;
        }

        let timelineHtml = '';
        if (timelineList.length > 0) {
          let versionLegendHtml = '';
          if (versionMap.size > 0) {
            versionLegendHtml += '<div style="display:flex; align-items:center; gap:6px; flex-wrap:wrap; margin-top:8px; padding-top:8px; border-top:1px dashed rgba(255,255,255,0.08); font-size:0.73rem;">' +
              '<span style="color:#94a3b8; font-weight:700; display:inline-flex; align-items:center; gap:4px;"><span>🏷️</span> Trilha Visual de Versões:</span>';
            
            versionMap.forEach(function(v) {
              const vPal = v.palette;
              const vDgHg = (v.hg || v.dg) ? ((v.dg ? v.dg.substring(0, 5) + ' ' : '') + v.hg) : '-';
              const vIdg = v.idg ? (' • IDG ' + v.idg) : '';
              versionLegendHtml += '<span style="display:inline-flex; align-items:center; gap:5px; background:' + vPal.bg + '; border:1px solid ' + vPal.border + '; color:' + vPal.text + '; padding:2px 8px; border-radius:5px; font-family:monospace; font-weight:700;">' +
                '<span style="background:' + vPal.badgeBg + '; color:#fff; font-size:0.65rem; padding:1px 5px; border-radius:3px; font-weight:800;">' + v.label + '</span>' +
                '<span>' + vDgHg + vIdg + '</span>' +
                '<span style="opacity:0.65; font-size:0.68rem;">(' + v.count + 'x)</span>' +
              '</span>';
            });

            versionLegendHtml += '</div>';
          }

          timelineHtml += '<div style="margin-bottom:12px; background:#0b132b; border:1px solid #334155; border-radius:8px; padding:12px 14px;">' +
            '<div style="margin-bottom:10px; border-bottom:1px solid rgba(255,255,255,0.08); padding-bottom:8px;">' +
              '<div style="display:flex; justify-content:space-between; align-items:center; flex-wrap:wrap; gap:6px;">' +
                '<strong style="font-size:0.84rem; color:#38bdf8; display:flex; align-items:center; gap:6px;">' +
                  '<span>⏱️</span> Esquema Cronológico de Eventos (Linha do Tempo das Requisições):' +
                '</strong>' +
                '<span style="font-size:0.72rem; color:#94a3b8;">' + timelineList.length + ' leituras no período</span>' +
              '</div>' +
              versionLegendHtml +
            '</div>' +
            '<div style="display:flex; flex-direction:column; gap:8px;">';

          for (let tIdx = 0; tIdx < timelineList.length; tIdx++) {
            const step = timelineList[tIdx];
            const stepTime = new Date(step.call_time_iso || step.timestamp_iso).toLocaleTimeString('pt-BR');
            const latencyBadge = (step.latency_ms !== null && step.latency_ms !== undefined) 
              ? ('<span style="color:#94a3b8; font-size:0.68rem; font-family:monospace;" title="Latência de ida e volta da requisição: ' + step.latency_ms + 'ms">(' + step.latency_ms + 'ms)</span>')
              : '';
            const isReg = step.isRegressionPoint;
            const isOrigin = step.servidor === 'HMG';
            
            const itemBg = isReg 
              ? 'background:rgba(239,68,68,0.14); border:1px solid #ef4444;' 
              : (isOrigin ? 'background:rgba(168,85,247,0.08); border:1px solid rgba(168,85,247,0.3);' : 'background:rgba(15,23,42,0.6); border:1px solid #1e293b;');
            
            const badgeServidor = isOrigin ? 'tag-hmg-title' : 'tag-sim-title';
            const statusLabel = isReg 
              ? '<span style="background:#dc2626; color:#fff; font-weight:700; padding:2px 8px; border-radius:4px; font-size:0.72rem; animation:pulse 1s infinite;">🚨 DETECÇÃO DE REVERSÃO!</span>'
              : (isOrigin ? '<span style="color:#c084fc; font-weight:600; font-size:0.72rem;">🟣 Origem Primária</span>' : '<span style="color:#10b981; font-weight:600; font-size:0.72rem;">✓ Leitura Normal</span>');

            const stepDgHg = (step.dg || '-') + ' ' + (step.hg || '-');
            const stepIdg = step.idg ? 'IDG: ' + step.idg : '';
            const stepSt = (step.secoes !== null && step.secoes !== undefined) ? 'ST: ' + step.secoes + ' seç' : '';
            const stepTot = (step.dt && step.ht) ? 'Tot: ' + step.dt + ' ' + step.ht : '';

            const stepVKey = getVersionKey(step.dg, step.hg, step.idg);
            const verInfo = versionMap.get(stepVKey);
            const pal = verInfo ? verInfo.palette : { bg: 'rgba(255,255,255,0.05)', border: '#475569', text: '#cbd5e1', badgeBg: '#475569' };
            const verBadge = verInfo ? ('<span style="background:' + pal.badgeBg + '; color:#fff; font-size:0.65rem; padding:1px 5px; border-radius:3px; font-weight:800; font-family:monospace;">' + verInfo.label + '</span>') : '';

            // Chips com a cor exclusiva da versão para DG/HG e IDG
            const chipDgHg = '<span style="display:inline-flex; align-items:center; gap:5px; background:' + pal.bg + '; border:1px solid ' + pal.border + '; color:' + pal.text + '; padding:2px 8px; border-radius:5px; font-family:monospace; font-size:0.74rem; font-weight:700; box-shadow:0 1px 2px rgba(0,0,0,0.2);">' +
              verBadge +
              '<span>DG/HG: ' + escapeHtml(stepDgHg) + '</span>' +
            '</span>';

            const chipIdg = step.idg ? ('<span style="display:inline-flex; align-items:center; background:' + pal.bg + '; border:1px solid ' + pal.border + '; color:' + pal.text + '; padding:2px 7px; border-radius:5px; font-family:monospace; font-size:0.72rem; font-weight:700; box-shadow:0 1px 2px rgba(0,0,0,0.2);">' +
              escapeHtml(stepIdg) +
            '</span>') : '';

            timelineHtml += '<div style="' + itemBg + ' border-radius:6px; padding:8px 12px; font-size:0.78rem;">' +
              '<div style="display:flex; justify-content:space-between; align-items:center; flex-wrap:wrap; gap:6px; margin-bottom:4px;">' +
                '<div style="display:flex; align-items:center; gap:8px; flex-wrap:wrap;">' +
                  '<strong style="font-family:monospace; color:#f8fafc; font-size:0.82rem;" title="Instante de envio da requisição (Disparo)">• ' + stepTime + '</strong>' +
                  latencyBadge +
                  '<span class="' + badgeServidor + '" style="font-size:0.70rem; padding:1px 6px; border-radius:3px;">' + step.servidor + '</span>' +
                  chipDgHg +
                  (chipIdg ? chipIdg : '') +
                  (stepSt ? '<span style="font-family:monospace; color:#34d399; font-size:0.72rem;">' + stepSt + '</span>' : '') +
                  (stepTot ? '<span style="font-family:monospace; color:#fbbf24; font-size:0.72rem;">' + stepTot + '</span>' : '') +
                '</div>' +
                '<div>' + statusLabel + '</div>' +
              '</div>' +

              '<div style="display:flex; align-items:center; gap:12px; flex-wrap:wrap; font-size:0.72rem; color:#94a3b8; font-family:monospace; border-top:1px solid rgba(255,255,255,0.05); padding-top:4px; margin-top:4px;">' +
                '<span>⏱️ Chamada: <strong style="color:#e2e8f0;">' + ((step.call_time_iso ? step.call_time_iso.slice(11, 19) : stepTime)) + '</strong></span>' +
                '<span>🌐 IP Borda: <strong style="color:#38bdf8;">' + (step.server_ip || '-') + '</strong></span>' +
                '<span>⚡ Cache-Control: <strong style="color:#f8fafc;">' + (step.cache_control || '-') + '</strong></span>' +
                '<span>📦 CDN Cache: <strong style="color:#34d399;">' + (step.cdn_status || '-') + '</strong></span>' +
                '<span>🏷️ ETag: <span style="color:#cbd5e1;">' + (step.etag || '-') + '</span></span>' +
                (step.akamai_grn && step.akamai_grn !== '-' ? '<span>🆔 GRN: <strong style="color:#c084fc;" title="Akamai Global Request Number">' + escapeHtml(step.akamai_grn) + '</strong></span>' : '') +
                (step.age && step.age !== '-' ? '<span>⏳ Age: ' + step.age + '</span>' : '') +
              '</div>' +
            '</div>';
          }

          timelineHtml += '</div></div>';
        }

        const caseGrn = r.akamai_grn || rawHeaders['akamai-grn'] || rawHeaders['x-akamai-grn'] || null;
        const caseGrnBadge = caseGrn ? ('<span style="font-family:monospace; font-size:0.72rem; background:rgba(168,85,247,0.15); color:#c084fc; border:1px solid rgba(168,85,247,0.3); padding:2px 8px; border-radius:4px;" title="Akamai Global Request Number (GRN)">🆔 GRN: ' + escapeHtml(caseGrn) + '</span> ') : '';

        const isExpanded = expandedRegCardIds.has(r.id);

        html += '<div id="regCard_' + r.id + '" class="regression-card" data-regression-id="' + r.id + '" onclick="selectRegressionForDetails(' + r.id + ')" style="background:#0f172a; border:1px solid rgba(239,68,68,0.35); border-left:4px solid #ef4444; border-radius:8px; padding:' + (isExpanded ? '14px' : '9px 14px') + '; box-shadow:0 4px 12px rgba(0,0,0,0.25); cursor:pointer; transition:all 0.15s ease;">' +
          '<div style="display:flex; justify-content:space-between; align-items:center; flex-wrap:wrap; gap:8px;">' +
            '<div style="display:flex; align-items:center; gap:8px; flex-wrap:wrap;">' +
              '<button type="button" onclick="toggleRegCard(' + r.id + ', event)" class="btn-copy" style="padding:2px 8px; font-size:0.75rem; background:#1e293b; border:1px solid #475569; color:#38bdf8; display:flex; align-items:center; gap:4px; cursor:pointer;" title="Expandir/Recolher ocorrência">' +
                '<span id="regChevron_' + r.id + '">' + (isExpanded ? '▼' : '▶') + '</span>' +
                '<span id="regBtnText_' + r.id + '" style="font-size:0.70rem; font-weight:700;">' + (isExpanded ? 'Recolher' : 'Detalhes') + '</span>' +
              '</button>' +
              '<span style="font-family:monospace; font-weight:700; font-size:0.82rem; background:rgba(239,68,68,0.2); color:#fca5a5; padding:2px 8px; border-radius:4px; border:1px solid rgba(239,68,68,0.4);">#' + r.id + '</span>' +
              '<span style="font-size:0.80rem; color:#94a3b8; font-family:monospace;">⏱️ ' + timeStr + ' (' + elapsedText + ')</span>' +
              '<span class="' + serverBadgeClass + '" style="font-size:0.75rem; padding:2px 8px; border-radius:4px;">' + r.servidor + ' (' + serverRoleDesc + ')</span>' +
              critBadgesHtml +
              caseGrnBadge +
            '</div>' +
            '<div style="display:flex; align-items:center; gap:6px;">' +
              '<a href="/api/evidencia?id=' + r.id + '" target="_blank" onclick="event.stopPropagation();" class="btn-copy" style="font-size:0.72rem; padding:3px 8px; text-decoration:none;" title="Ver payload JSON raw">🔍 Ver JSON</a>' +
              '<a href="/api/evidencia?id=' + r.id + '&download=1" target="_blank" onclick="event.stopPropagation();" class="btn-copy" style="font-size:0.72rem; padding:3px 8px; text-decoration:none;" title="Baixar JSON da evidência">⬇️ Baixar JSON</a>' +
              '<button type="button" onclick="event.stopPropagation(); selectRegressionForDetails(' + r.id + ');" class="btn-copy" style="font-size:0.72rem; padding:3px 10px; background:#1e293b; border:1px solid #38bdf8; color:#38bdf8;" id="btnInspect_' + r.id + '">🌐 Inspecionar Painel 👉</button>' +
            '</div>' +
          '</div>' +

          '<div style="display:flex; justify-content:space-between; align-items:center; gap:8px; margin-top:6px; flex-wrap:wrap; font-size:0.80rem;">' +
            '<div style="display:flex; align-items:center; gap:8px; flex-wrap:wrap;">' +
              '<span style="color:#38bdf8; font-weight:700; font-family:monospace; word-break:break-all;">' + r.arquivo + '</span>' +
              '<span style="background:rgba(255,255,255,0.06); padding:2px 6px; border-radius:4px; font-size:0.72rem; color:#cbd5e1;">UF: <strong>' + uf + '</strong></span>' +
              '<span style="background:rgba(255,255,255,0.06); padding:2px 6px; border-radius:4px; font-size:0.72rem; color:#cbd5e1;">Cargo: <strong>' + cargo + '</strong></span>' +
              '<span style="background:rgba(255,255,255,0.06); padding:2px 6px; border-radius:4px; font-size:0.72rem; color:#cbd5e1;">Eleição: <strong>' + eleicao + '</strong></span>' +
            '</div>' +
            '<div style="display:flex; align-items:center; gap:6px; font-family:monospace; font-size:0.74rem; flex-wrap:wrap;">' +
              '<span style="color:#94a3b8;">DG/HG:</span> ' +
              '<span style="color:#10b981;">' + (r.dg_anterior || '-') + ' ' + (r.hg_anterior || '-') + '</span>' +
              '<span style="' + (dgAdvOrSame ? 'color:#10b981;' : 'color:#ef4444; font-weight:bold;') + '">➔ ' + (r.dg_recebido || '-') + ' ' + (r.hg_recebido || '-') + '</span>' +
              (isTotRegression ? (' <span style="background:rgba(249,115,22,0.15); border:1px solid rgba(249,115,22,0.3); padding:1px 5px; border-radius:3px; color:#fb923c;"><span style="color:#94a3b8;">DT:</span> ' + (r.dt_anterior ? r.dt_anterior.substring(0, 5) + ' ' + (r.ht_anterior || '') : '-') + ' ➔ <strong style="color:#ef4444;">' + (r.dt_recebido ? r.dt_recebido.substring(0, 5) + ' ' + (r.ht_recebido || '') : '-') + ' ↘</strong></span>') : '') +
              (isStRegression ? (' <span style="background:rgba(236,72,153,0.15); border:1px solid rgba(236,72,153,0.3); padding:1px 5px; border-radius:3px; color:#f472b6;"><span style="color:#94a3b8;">ST:</span> ' + (r.secoes_anterior !== null && r.secoes_anterior !== undefined ? r.secoes_anterior : '-') + ' ➔ <strong style="color:#ef4444;">' + (r.secoes_recebido !== null && r.secoes_recebido !== undefined ? r.secoes_recebido : '-') + ' ↘</strong></span>') : '') +
              (r.idg_anterior ? ('<span style="color:#94a3b8; margin-left:4px;">(IDG: ' + r.idg_anterior + ' ➔ <strong style="color:#fca5a5;">' + (r.idg_recebido || '-') + '</strong>)</span>') : '') +
            '</div>' +
          '</div>' +

          '<div id="regCardBody_' + r.id + '" class="card-collapsible-body" style="display:' + (isExpanded ? 'block' : 'none') + '; margin-top:10px; border-top:1px dashed rgba(255,255,255,0.1); padding-top:10px;">' +
            timelineHtml;

        const prevVerInfo = versionMap.get(prevVKey);
        const prevPal = prevVerInfo ? prevVerInfo.palette : null;
        const prevVerBadge = prevVerInfo ? ('<span style="background:' + prevPal.badgeBg + '; color:#fff; font-size:0.62rem; padding:1px 5px; border-radius:3px; font-weight:800; font-family:monospace; margin-right:4px;">' + prevVerInfo.label + '</span>') : '';
        const prevDgHgHtml = prevPal ? (
          '<span style="display:inline-flex; align-items:center; background:' + prevPal.bg + '; border:1px solid ' + prevPal.border + '; color:' + prevPal.text + '; padding:2px 8px; border-radius:4px; font-weight:700;">' +
            prevVerBadge + (r.dg_anterior || '-') + ' ' + (r.hg_anterior || '-') +
          '</span>'
        ) : ('<strong style="color:#f8fafc;">' + (r.dg_anterior || '-') + ' ' + (r.hg_anterior || '-') + '</strong>');

        const prevIdgHtml = (r.idg_anterior && prevPal) ? (
          '<span style="background:' + prevPal.bg + '; border:1px solid ' + prevPal.border + '; color:' + prevPal.text + '; padding:1px 6px; border-radius:4px; font-weight:700;">' +
            r.idg_anterior +
          '</span>'
        ) : (r.idg_anterior || '-');

        const currVerInfo = versionMap.get(currVKey);
        const currPal = currVerInfo ? currVerInfo.palette : null;
        const currVerBadge = currVerInfo ? ('<span style="background:' + currPal.badgeBg + '; color:#fff; font-size:0.62rem; padding:1px 5px; border-radius:3px; font-weight:800; font-family:monospace; margin-right:4px;">' + currVerInfo.label + '</span>') : '';
        const currDgHgHtml = currPal ? (
          '<span style="display:inline-flex; align-items:center; background:' + currPal.bg + '; border:1px solid ' + currPal.border + '; color:' + currPal.text + '; padding:2px 8px; border-radius:4px; font-weight:700;">' +
            currVerBadge + (r.dg_recebido || '-') + ' ' + (r.hg_recebido || '-') +
          '</span>'
        ) : ('<strong style="color:#ef4444;">' + (r.dg_recebido || '-') + ' ' + (r.hg_recebido || '-') + '</strong>');

        const currIdgHtml = (r.idg_recebido && currPal) ? (
          '<span style="background:' + currPal.bg + '; border:1px solid ' + currPal.border + '; color:' + currPal.text + '; padding:1px 6px; border-radius:4px; font-weight:700;">' +
            r.idg_recebido +
          '</span>'
        ) : (r.idg_recebido || '-');

        html += '<div style="display:grid; grid-template-columns:repeat(auto-fit, minmax(240px, 1fr)); gap:12px; background:#1e293b; border:1px solid #334155; border-radius:8px; padding:10px 14px; margin-bottom:8px; font-size:0.80rem;">' +
            '<div>' +
              '<div style="font-size:0.72rem; text-transform:uppercase; color:#10b981; font-weight:700; margin-bottom:6px; display:flex; align-items:center; gap:4px;">' +
                '<span>✓</span> Versão Anterior Mais Recente:' +
              '</div>' +
              '<div style="display:grid; grid-template-columns:120px 1fr; gap:4px 8px; font-family:monospace; align-items:center;">' +
                '<span style="color:#94a3b8;">Geração (DG/HG):</span>' +
                '<div>' + prevDgHgHtml + '</div>' +
                '<span style="color:#94a3b8;">Totalização:</span>' +
                '<span style="color:#f8fafc;">' + prevTotStr + '</span>' +
                '<span style="color:#94a3b8;">Seções Apuradas:</span>' +
                '<span style="color:#f8fafc;">' + prevStStr + '</span>' +
                '<span style="color:#94a3b8;">IDG (Sequencial):</span>' +
                '<div>' + prevIdgHtml + '</div>' +
              '</div>' +
            '</div>' +

            '<div>' +
              '<div style="font-size:0.72rem; text-transform:uppercase; color:#ef4444; font-weight:700; margin-bottom:6px; display:flex; align-items:center; gap:4px;">' +
                '<span>🚨</span> Versão Recebida (Retrocesso):' +
              '</div>' +
              '<div style="display:grid; grid-template-columns:120px 1fr; gap:4px 8px; font-family:monospace; align-items:center;">' +
                '<span style="color:#94a3b8;">Geração (DG/HG):</span>' +
                '<div>' + currDgHgHtml + '</div>' +
                '<span style="color:#94a3b8;">Totalização:</span>' +
                '<span style="' + (isTotRegression ? 'color:#ef4444; font-weight:bold;' : 'color:#f8fafc;') + '">' + currTotStr + '</span>' +
                '<span style="color:#94a3b8;">Seções Apuradas:</span>' +
                '<span style="' + (isStRegression ? 'color:#ef4444; font-weight:bold;' : 'color:#f8fafc;') + '">' + currStStr + '</span>' +
                '<span style="color:#94a3b8;">IDG (Sequencial):</span>' +
                '<div>' + currIdgHtml + '</div>' +
              '</div>' +
            '</div>' +
          '</div>' +

          '<div style="font-size:0.78rem; color:#fca5a5; background:rgba(239,68,68,0.12); border:1px solid rgba(239,68,68,0.25); border-radius:6px; padding:8px 12px;">' +
            '<strong>⚠️ Diagnóstico:</strong> ' + motivoTexto +
          '</div>' +
          '</div>' +
        '</div>';
      }
      if (filtered.length > displayItems.length) {
        const remaining = filtered.length - displayItems.length;
        html += '<div style="text-align:center; padding:16px 10px;">' +
          '<button onclick="regModalPageSize += 50; renderFilteredRegressoes(false);" class="btn-copy" style="padding:10px 24px; font-size:0.85rem; font-weight:bold; background:#2563eb; color:#fff; border:none; border-radius:6px; cursor:pointer; box-shadow:0 4px 6px rgba(0,0,0,0.3);">' +
            '➕ Carregar mais 50 ocorrências (' + remaining + ' restantes)' +
          '</button>' +
        '</div>';
      }
      container.innerHTML = html;
      updateToggleAllBtn();

      // Se houver itens na tela, auto-seleciona a ocorrência corrente ou o primeiro item
      if (displayItems.length > 0) {
        const exists = selectedRegressionId && displayItems.some(function(item) { return item.id === selectedRegressionId; });
        const targetId = exists ? selectedRegressionId : displayItems[0].id;
        selectRegressionForDetails(targetId);
      } else {
        selectedRegressionId = null;
        const panelContent = document.getElementById('techPanelContent');
        const badge = document.getElementById('techPanelSelectedBadge');
        if (badge) badge.textContent = 'Nenhum selecionado';
        if (panelContent) {
          panelContent.innerHTML = '<div style="text-align:center; padding:60px 20px; color:#64748b;">Nenhuma ocorrência para exibir metadados.</div>';
        }
      }
    }

    let selectedRegressionId = null;

    function selectRegressionForDetails(id) {
      selectedRegressionId = id;
      const r = allRegressoesData.find(function(item) { return item.id === id; });
      if (!r) return;

      // Destaca o card selecionado na coluna esquerda
      const allCards = document.querySelectorAll('.regression-card');
      allCards.forEach(function(card) {
        card.style.borderColor = 'rgba(239,68,68,0.35)';
        card.style.background = '#0f172a';
        card.style.boxShadow = '0 4px 12px rgba(0,0,0,0.25)';
      });

      const selectedCard = document.getElementById('regCard_' + id);
      if (selectedCard) {
        selectedCard.style.borderColor = '#38bdf8';
        selectedCard.style.background = '#132338';
        selectedCard.style.boxShadow = '0 0 16px rgba(56,189,248,0.25)';
      }

      // Atualiza botões "Inspecionar"
      const allInspectBtns = document.querySelectorAll('[id^="btnInspect_"]');
      allInspectBtns.forEach(function(btn) {
        btn.textContent = '🌐 Inspecionar Painel 👉';
        btn.style.background = '#1e293b';
        btn.style.borderColor = '#38bdf8';
        btn.style.color = '#38bdf8';
      });
      const currentInspectBtn = document.getElementById('btnInspect_' + id);
      if (currentInspectBtn) {
        currentInspectBtn.textContent = '🔍 INSPECIONANDO ATIVO';
        currentInspectBtn.style.background = '#0284c7';
        currentInspectBtn.style.borderColor = '#38bdf8';
        currentInspectBtn.style.color = '#ffffff';
      }

      // Atualiza o badge do painel direito
      const badge = document.getElementById('techPanelSelectedBadge');
      if (badge) {
        badge.innerHTML = '<span style="color:#fca5a5; font-weight:bold;">#' + r.id + '</span> | ' + escapeHtml(r.arquivo);
      }

      // Renderiza os detalhes técnicos no painel direito
      renderTechDetailsInPanel(r);
    }

    function renderTechDetailsInPanel(r) {
      const panelContent = document.getElementById('techPanelContent');
      if (!panelContent) return;

      const rawHeaders = (r.rawMeta && (r.rawMeta.response_headers || r.rawMeta.headers)) || r.response_headers || {};
      const rawReqHeaders = (r.rawMeta && r.rawMeta.request_headers) || r.request_headers || {};
      const serverIp = rawHeaders['x-server-ip'] || (r.rawMeta && r.rawMeta.serverIp) || (r.server_ip) || '-';
      const cdnCache = rawHeaders['cdn-cache-status'] || rawHeaders['x-cache'] || '-';
      const cacheControl = rawHeaders['cache-control'] || '-';
      const expires = rawHeaders['expires'] || '-';
      const age = rawHeaders['age'] !== undefined ? (rawHeaders['age'] + 's') : '-';
      const etag = rawHeaders['etag'] || '-';
      const lastModified = rawHeaders['last-modified'] || '-';
      const dateHttp = rawHeaders['date'] || '-';
      const webServer = rawHeaders['server'] || (r.servidor === 'SIM' ? 'Akamai CDN' : 'Apache Origin');
      const originUrl = (r.rawMeta && r.rawMeta.url_origem) || '-';
      const reqCacheControl = rawReqHeaders['cache-control'] || rawReqHeaders['Cache-Control'] || '-';
      const reqPragma = rawReqHeaders['pragma'] || rawReqHeaders['Pragma'] || '-';
      const caseGrn = r.akamai_grn || rawHeaders['akamai-grn'] || rawHeaders['x-akamai-grn'] || null;
      const rawPath = r.evidencia_raw_path ? r.evidencia_raw_path : '(salvo no buffer SQLite)';

      let reqHeadersRowsHtml = '';
      const reqEntries = Object.entries(rawReqHeaders);
      if (reqEntries.length > 0) {
        for (let j = 0; j < reqEntries.length; j++) {
          const k = reqEntries[j][0];
          const v = reqEntries[j][1];
          const lk = k.toLowerCase();
          const isCacheHdr = ['cache-control', 'pragma', 'if-modified-since', 'if-none-match'].includes(lk);
          reqHeadersRowsHtml += '<tr style="border-bottom:1px solid rgba(255,255,255,0.05);">' +
            '<td style="padding:5px 8px; color:' + (isCacheHdr ? '#38bdf8; font-weight:700;' : '#94a3b8;') + '; font-family:monospace; width:200px;">' +
              (isCacheHdr ? '<span style="background:rgba(56,189,248,0.15); color:#38bdf8; border:1px solid rgba(56,189,248,0.3); padding:1px 4px; border-radius:3px; font-size:0.62rem; margin-right:4px; font-weight:bold;">CACHE</span>' : '') +
              escapeHtml(k) +
            '</td>' +
            '<td style="padding:5px 8px; color:' + (isCacheHdr ? '#f8fafc; font-weight:600;' : '#cbd5e1;') + '; font-family:monospace; word-break:break-all;">' +
              escapeHtml(String(v)) +
            '</td>' +
          '</tr>';
        }
      } else {
        reqHeadersRowsHtml = '<tr><td colspan="2" style="padding:6px 8px; color:#64748b; font-style:italic;">Cabeçalhos de solicitação padrão aplicados (User-Agent, Cache-Control: no-cache, Pragma: no-cache).</td></tr>';
      }

      let respHeadersRowsHtml = '';
      const respEntries = Object.entries(rawHeaders);
      if (respEntries.length > 0) {
        for (let j = 0; j < respEntries.length; j++) {
          const k = respEntries[j][0];
          const v = respEntries[j][1];
          const lk = k.toLowerCase();
          const isCacheHdr = ['cache-control', 'pragma', 'expires', 'age', 'etag', 'last-modified', 'date', 'vary'].includes(lk);
          const isCdnHdr = ['akamai-grn', 'x-akamai-grn', 'cdn-cache-status', 'x-cache', 'x-cache-lookup', 'x-cache-hits', 'x-check-cacheable', 'x-true-cache-key', 'x-cache-key', 'server-timing'].includes(lk);
          const isIpOrServer = ['x-server-ip', 'server'].includes(lk);

          let tagBadge = '';
          let valColor = '#cbd5e1';
          let keyColor = '#94a3b8';

          if (isCacheHdr) {
            tagBadge = '<span style="background:rgba(16,185,129,0.15); color:#34d399; border:1px solid rgba(16,185,129,0.3); padding:1px 4px; border-radius:3px; font-size:0.62rem; margin-right:4px; font-weight:bold;">CACHE</span>';
            keyColor = '#34d399';
            valColor = '#f8fafc; font-weight:bold';
          } else if (isCdnHdr) {
            tagBadge = '<span style="background:rgba(168,85,247,0.15); color:#c084fc; border:1px solid rgba(168,85,247,0.3); padding:1px 4px; border-radius:3px; font-size:0.62rem; margin-right:4px; font-weight:bold;">CDN</span>';
            keyColor = '#c084fc';
            valColor = '#f8fafc; font-weight:bold';
          } else if (isIpOrServer) {
            tagBadge = '<span style="background:rgba(56,189,248,0.15); color:#38bdf8; border:1px solid rgba(56,189,248,0.3); padding:1px 4px; border-radius:3px; font-size:0.62rem; margin-right:4px; font-weight:bold;">REDE</span>';
            keyColor = '#38bdf8';
            valColor = '#f8fafc';
          }

          respHeadersRowsHtml += '<tr style="border-bottom:1px solid rgba(255,255,255,0.05);">' +
            '<td style="padding:5px 8px; color:' + keyColor + '; font-family:monospace; width:200px;">' +
              tagBadge + escapeHtml(k) +
            '</td>' +
            '<td style="padding:5px 8px; color:' + valColor + '; font-family:monospace; word-break:break-all;">' +
              escapeHtml(String(v)) +
            '</td>' +
          '</tr>';
        }
      } else {
        respHeadersRowsHtml = '<tr><td colspan="2" style="padding:6px 8px; color:#64748b; font-style:italic;">Nenhum cabeçalho de resposta registrado.</td></tr>';
      }

      panelContent.innerHTML = 
        // Banner de Identificação do Caso Selecionado
        '<div style="background:#1e293b; border:1px solid #334155; border-radius:8px; padding:10px 12px; margin-bottom:12px;">' +
          '<div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:4px;">' +
            '<span style="font-weight:700; color:#38bdf8; font-size:0.85rem;">Caso Forense #' + r.id + '</span>' +
            '<span style="font-size:0.75rem; color:#94a3b8; font-family:monospace;">' + (r.servidor || '') + ' (' + (r.papel_servidor || (r.servidor === 'HMG' ? 'Fonte Oficial' : 'Cache Akamai')) + ')</span>' +
          '</div>' +
          '<div style="font-family:monospace; color:#f8fafc; font-size:0.78rem; word-break:break-all;">' + escapeHtml(r.arquivo) + '</div>' +
        '</div>' +

        // 1. METADADOS DE REDE E CONEXÃO
        '<div style="margin-bottom:14px;">' +
          '<div style="font-size:0.72rem; text-transform:uppercase; color:#94a3b8; font-weight:700; margin-bottom:6px; display:flex; align-items:center; gap:4px;">' +
            '<span>📍</span> Metadados de Rede e Conexão:' +
          '</div>' +
          '<table style="width:100%; border-collapse:collapse; font-size:0.74rem; background:#1e293b; border-radius:6px; overflow:hidden; border:1px solid #334155;">' +
            '<tbody>' +
              '<tr style="border-bottom:1px solid #334155;">' +
                '<td style="color:#94a3b8; padding:5px 8px; width:190px;">Instância / IP Borda (x-server-ip)</td>' +
                '<td style="color:#38bdf8; font-weight:bold; font-family:monospace; padding:5px 8px;">' + serverIp + '</td>' +
              '</tr>' +
              '<tr style="border-bottom:1px solid #334155;">' +
                '<td style="color:#94a3b8; padding:5px 8px;">Akamai-GRN</td>' +
                '<td style="color:#c084fc; font-weight:bold; font-family:monospace; padding:5px 8px; word-break:break-all;">' + (caseGrn || '-') + '</td>' +
              '</tr>' +
              '<tr style="border-bottom:1px solid #334155;">' +
                '<td style="color:#94a3b8; padding:5px 8px;">Instante de Disparo (T_call)</td>' +
                '<td style="color:#e2e8f0; font-family:monospace; padding:5px 8px;">' + (r.call_time_iso || r.timestamp_iso) + '</td>' +
              '</tr>' +
              '<tr style="border-bottom:1px solid #334155;">' +
                '<td style="color:#94a3b8; padding:5px 8px;">Latência de Rede (RTT)</td>' +
                '<td style="color:#34d399; font-family:monospace; padding:5px 8px;">' + (r.latency_ms !== null && r.latency_ms !== undefined ? (r.latency_ms + ' ms') : '-') + '</td>' +
              '</tr>' +
              '<tr style="border-bottom:1px solid #334155;">' +
                '<td style="color:#94a3b8; padding:5px 8px;">Camada Web (Server)</td>' +
                '<td style="color:#94a3b8; font-family:monospace; padding:5px 8px;">' + webServer + '</td>' +
              '</tr>' +
              '<tr style="border-bottom:1px solid #334155;">' +
                '<td style="color:#94a3b8; padding:5px 8px;">URL da Requisição</td>' +
                '<td style="color:#38bdf8; font-family:monospace; padding:5px 8px; word-break:break-all;"><a href="' + originUrl + '" target="_blank" style="color:#38bdf8;">' + originUrl + '</a></td>' +
              '</tr>' +
              '<tr>' +
                '<td style="color:#94a3b8; padding:5px 8px;">Arquivo Raw em Disco</td>' +
                '<td style="color:#64748b; font-family:monospace; padding:5px 8px; word-break:break-all;">' + rawPath + '</td>' +
              '</tr>' +
            '</tbody>' +
          '</table>' +
        '</div>' +

        // 2. DIRETIVAS DE CONTROLE DE CACHE & CDN
        '<div style="margin-bottom:14px;">' +
          '<div style="font-size:0.72rem; text-transform:uppercase; color:#34d399; font-weight:700; margin-bottom:6px; display:flex; align-items:center; gap:4px;">' +
            '<span>⚡</span> Diretivas e Headers de Controle de Cache (RFC 7234 & Akamai CDN):' +
          '</div>' +
          '<table style="width:100%; border-collapse:collapse; font-size:0.74rem; background:#1e293b; border-radius:6px; overflow:hidden; border:1px solid rgba(16,185,129,0.3);">' +
            '<tbody>' +
              '<tr style="border-bottom:1px solid #334155;">' +
                '<td style="color:#94a3b8; padding:5px 8px; width:190px;">Cache-Control (Resposta)</td>' +
                '<td style="color:#f8fafc; font-weight:bold; font-family:monospace; padding:5px 8px;">' + cacheControl + '</td>' +
              '</tr>' +
              '<tr style="border-bottom:1px solid #334155;">' +
                '<td style="color:#94a3b8; padding:5px 8px;">Cache-Control (Solicitação)</td>' +
                '<td style="color:#38bdf8; font-weight:bold; font-family:monospace; padding:5px 8px;">' + reqCacheControl + '</td>' +
              '</tr>' +
              '<tr style="border-bottom:1px solid #334155;">' +
                '<td style="color:#94a3b8; padding:5px 8px;">CDN Cache Status</td>' +
                '<td style="color:#34d399; font-weight:bold; font-family:monospace; padding:5px 8px;">' + cdnCache + '</td>' +
              '</tr>' +
              '<tr style="border-bottom:1px solid #334155;">' +
                '<td style="color:#94a3b8; padding:5px 8px;">Idade em Cache (Age)</td>' +
                '<td style="color:#f8fafc; font-family:monospace; padding:5px 8px;">' + age + '</td>' +
              '</tr>' +
              '<tr style="border-bottom:1px solid #334155;">' +
                '<td style="color:#94a3b8; padding:5px 8px;">Expiração HTTP (Expires)</td>' +
                '<td style="color:#f8fafc; font-family:monospace; padding:5px 8px;">' + expires + '</td>' +
              '</tr>' +
              '<tr style="border-bottom:1px solid #334155;">' +
                '<td style="color:#94a3b8; padding:5px 8px;">Hash de Integridade (ETag)</td>' +
                '<td style="color:#94a3b8; font-family:monospace; padding:5px 8px; word-break:break-all;">' + etag + '</td>' +
              '</tr>' +
              '<tr style="border-bottom:1px solid #334155;">' +
                '<td style="color:#94a3b8; padding:5px 8px;">Última Modificação (Last-Modified)</td>' +
                '<td style="color:#94a3b8; font-family:monospace; padding:5px 8px;">' + lastModified + '</td>' +
              '</tr>' +
              '<tr style="border-bottom:1px solid #334155;">' +
                '<td style="color:#94a3b8; padding:5px 8px;">Data do Servidor HTTP (Date)</td>' +
                '<td style="color:#94a3b8; font-family:monospace; padding:5px 8px;">' + dateHttp + '</td>' +
              '</tr>' +
              '<tr style="border-bottom:1px solid #334155;">' +
                '<td style="color:#94a3b8; padding:5px 8px;">Diretiva Pragma</td>' +
                '<td style="color:#94a3b8; font-family:monospace; padding:5px 8px;">' + (rawHeaders['pragma'] || reqPragma || '-') + '</td>' +
              '</tr>' +
              '<tr>' +
                '<td style="color:#94a3b8; padding:5px 8px;">Diretiva de Variação (Vary)</td>' +
                '<td style="color:#94a3b8; font-family:monospace; padding:5px 8px;">' + (rawHeaders['vary'] || '-') + '</td>' +
              '</tr>' +
            '</tbody>' +
          '</table>' +
        '</div>' +

        // 3. CABEÇALHOS DA SOLICITAÇÃO (REQUEST HEADERS)
        '<div style="margin-bottom:14px;">' +
          '<div style="font-size:0.72rem; text-transform:uppercase; color:#38bdf8; font-weight:700; margin-bottom:6px; display:flex; align-items:center; gap:4px;">' +
            '<span>📤</span> Cabeçalhos da Solicitação Enviada (HTTP Request Headers):' +
          '</div>' +
          '<table style="width:100%; border-collapse:collapse; font-size:0.74rem; background:#1e293b; border-radius:6px; overflow:hidden; border:1px solid rgba(56,189,248,0.25);">' +
            '<thead>' +
              '<tr style="background:#0f172a; border-bottom:1px solid #334155; text-align:left;">' +
                '<th style="padding:5px 8px; color:#94a3b8; font-weight:600; width:190px;">Header</th>' +
                '<th style="padding:5px 8px; color:#94a3b8; font-weight:600;">Valor Enviado</th>' +
              '</tr>' +
            '</thead>' +
            '<tbody>' +
              reqHeadersRowsHtml +
            '</tbody>' +
          '</table>' +
        '</div>' +

        // 4. CABEÇALHOS DA RESPOSTA (RESPONSE HEADERS)
        '<div>' +
          '<div style="font-size:0.72rem; text-transform:uppercase; color:#c084fc; font-weight:700; margin-bottom:6px; display:flex; align-items:center; gap:4px;">' +
            '<span>📥</span> Cabeçalhos da Resposta Recebida (HTTP Response Headers - Todos):' +
          '</div>' +
          '<table style="width:100%; border-collapse:collapse; font-size:0.74rem; background:#1e293b; border-radius:6px; overflow:hidden; border:1px solid rgba(168,85,247,0.25);">' +
            '<thead>' +
              '<tr style="background:#0f172a; border-bottom:1px solid #334155; text-align:left;">' +
                '<th style="padding:5px 8px; color:#94a3b8; font-weight:600; width:190px;">Header</th>' +
                '<th style="padding:5px 8px; color:#94a3b8; font-weight:600;">Valor Recebido</th>' +
              '</tr>' +
            '</thead>' +
            '<tbody>' +
              respHeadersRowsHtml +
            '</tbody>' +
          '</table>' +
        '</div>';
    }
  </script>


  <!-- MODAL DE AUDITORIA DETALHADA DE CACHE & HEADERS HTTP -->
  <div id="cacheModal" style="display:none; position:fixed; top:0; left:0; width:100%; height:100%; background:rgba(0,0,0,0.75); z-index:9999; align-items:center; justify-content:center; backdrop-filter:blur(3px);">
    <div style="background:#1e293b; border:1px solid #475569; border-radius:14px; width:92%; max-width:760px; padding:24px; box-shadow:0 25px 50px -12px rgba(0,0,0,0.6); color:#f8fafc; max-height:90vh; overflow-y:auto;">
      <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:14px; border-bottom:1px solid #334155; padding-bottom:12px;">
        <h3 style="margin:0; font-size:1.15rem; display:flex; align-items:center; gap:8px;">
          🔍 Auditoria Comparativa de Cache & Headers HTTP
        </h3>
        <button onclick="closeCacheModal()" style="background:transparent; border:none; color:#94a3b8; font-size:1.4rem; cursor:pointer; line-height:1;">&times;</button>
      </div>

      <div id="cacheModalPath" class="code" style="font-size:0.82rem; color:#38bdf8; word-break:break-all; margin-bottom:16px; background:#0f172a; padding:8px 12px; border-radius:6px; border:1px solid #334155;"></div>

      <div style="display:grid; grid-template-columns:1fr 1fr; gap:16px; margin-bottom:16px;">
        <!-- Card HMG -->
        <div style="background:#0f172a; border:1px solid rgba(168, 85, 247, 0.4); border-radius:10px; padding:14px;">
          <div style="font-size:0.85rem; font-weight:700; color:#c084fc; margin-bottom:10px; display:flex; justify-content:space-between; align-items:center;">
            <span>🟣 FONTE (HMG)</span>
            <span style="font-size:0.7rem; background:rgba(168,85,247,0.2); padding:2px 6px; border-radius:4px;">Apache Origin</span>
          </div>
          <table style="font-size:0.75rem; width:100%;">
            <tr>
              <td style="color:#94a3b8; width:135px; padding:6px 0; vertical-align:top;">
                <span class="header-hint" title="Instruções de cache para navegadores e proxies intermediários (ex: max-age, no-cache, no-store).">
                  Cache-Control <span class="hint-icon">?</span>
                </span>
                <div class="hint-desc">Regra de cache</div>
              </td>
              <td id="mHmgCc" class="code" style="color:#f8fafc; font-weight:600; padding:6px 0; vertical-align:top;">-</td>
            </tr>
            <tr>
              <td style="color:#94a3b8; padding:6px 0; vertical-align:top;">
                <span class="header-hint" title="Tempo de vida máximo que um arquivo pode ser servido sem revalidação com a origem.">
                  TTL Estimado <span class="hint-icon">?</span>
                </span>
                <div class="hint-desc">Validade do cache</div>
              </td>
              <td id="mHmgTtl" class="code" style="color:#f8fafc; padding:6px 0; vertical-align:top;">Sem TTL explícito</td>
            </tr>
            <tr>
              <td style="color:#94a3b8; padding:6px 0; vertical-align:top;">
                <span class="header-hint" title="Indica se a resposta veio direto da origem ou passou por algum proxy acelerador.">
                  CDN / Status <span class="hint-icon">?</span>
                </span>
                <div class="hint-desc">Ponto de entrega</div>
              </td>
              <td id="mHmgCdn" class="code" style="color:#94a3b8; padding:6px 0; vertical-align:top;">ORIGIN (Direto)</td>
            </tr>
            <tr>
              <td style="color:#94a3b8; padding:6px 0; vertical-align:top;">
                <span class="header-hint" title="Hash do conteúdo do arquivo gerado pelo servidor web para revalidação condicional (If-None-Match).">
                  ETag <span class="hint-icon">?</span>
                </span>
                <div class="hint-desc">Assinatura hash</div>
              </td>
              <td id="mHmgEtag" class="code" style="color:#94a3b8; word-break:break-all; padding:6px 0; vertical-align:top;">-</td>
            </tr>
            <tr>
              <td style="color:#94a3b8; padding:6px 0; vertical-align:top;">
                <span class="header-hint" title="Data e hora exata da última alteração física gravada no disco ou bucket pelo processo gerador.">
                  Last-Modified <span class="hint-icon">?</span>
                </span>
                <div class="hint-desc">Timestamp no disco</div>
              </td>
              <td id="mHmgLm" class="code" style="color:#94a3b8; padding:6px 0; vertical-align:top;">-</td>
            </tr>
            <tr>
              <td style="color:#94a3b8; padding:6px 0; vertical-align:top;">
                <span class="header-hint" title="IP do servidor que respondeu ao GET. Permite identificar exatamente a máquina física/virtual da origem.">
                  Instância (IP) <span class="hint-icon">?</span>
                </span>
                <div class="hint-desc">Origem física</div>
              </td>
              <td id="mHmgIp" class="code" style="color:#c084fc; font-weight:700; padding:6px 0; vertical-align:top;">192.168.218.33</td>
            </tr>
            <tr>
              <td style="color:#94a3b8; padding:6px 0; vertical-align:top;">
                <span class="header-hint" title="Software de serviço web responsável por atender a conexão na porta 443.">
                  Server <span class="hint-icon">?</span>
                </span>
                <div class="hint-desc">Software web</div>
              </td>
              <td id="mHmgServer" class="code" style="color:#94a3b8; padding:6px 0; vertical-align:top;">Apache</td>
            </tr>
          </table>
        </div>

        <!-- Card SIM -->
        <div style="background:#0f172a; border:1px solid rgba(56, 189, 248, 0.4); border-radius:10px; padding:14px;">
          <div style="font-size:0.85rem; font-weight:700; color:#38bdf8; margin-bottom:10px; display:flex; justify-content:space-between; align-items:center;">
            <span>🔵 CACHE (SIM)</span>
            <span style="font-size:0.7rem; background:rgba(56,189,248,0.2); padding:2px 6px; border-radius:4px;">Akamai CDN Edge</span>
          </div>
          <table style="font-size:0.75rem; width:100%;">
            <tr>
              <td style="color:#94a3b8; width:135px; padding:6px 0; vertical-align:top;">
                <span class="header-hint" title="Instruções de cache emitidas pela CDN para o navegador do eleitor. Decrementa a cada segundo (TTL dinâmico).">
                  Cache-Control <span class="hint-icon">?</span>
                </span>
                <div class="hint-desc">Regra de cache</div>
              </td>
              <td id="mSimCc" class="code" style="color:#38bdf8; font-weight:700; padding:6px 0; vertical-align:top;">-</td>
            </tr>
            <tr>
              <td style="color:#94a3b8; padding:6px 0; vertical-align:top;">
                <span class="header-hint" title="Segundos restantes antes que este nó de borda da Akamai descarte a réplica e requisite à origem.">
                  TTL Remanescente <span class="hint-icon">?</span>
                </span>
                <div class="hint-desc">Expiração na borda</div>
              </td>
              <td id="mSimTtl" class="code" style="color:#10b981; font-weight:700; padding:6px 0; vertical-align:top;">-</td>
            </tr>
            <tr>
              <td style="color:#94a3b8; padding:6px 0; vertical-align:top;">
                <span class="header-hint" title="Hit from child = entregue pela borda local da CDN. Miss = objeto não estava em cache e foi buscado na origem.">
                  CDN-Cache-Status <span class="hint-icon">?</span>
                </span>
                <div class="hint-desc">Hit ou Miss</div>
              </td>
              <td id="mSimCdn" class="code" style="color:#34d399; padding:6px 0; vertical-align:top;">-</td>
            </tr>
            <tr>
              <td style="color:#94a3b8; padding:6px 0; vertical-align:top;">
                <span class="header-hint" title="Hash atribuído ao arquivo no storage S3 / CDN. Se mudar, o conteúdo é garantidamente diferente.">
                  ETag <span class="hint-icon">?</span>
                </span>
                <div class="hint-desc">Assinatura hash</div>
              </td>
              <td id="mSimEtag" class="code" style="color:#94a3b8; word-break:break-all; padding:6px 0; vertical-align:top;">-</td>
            </tr>
            <tr>
              <td style="color:#94a3b8; padding:6px 0; vertical-align:top;">
                <span class="header-hint" title="Timestamp registrado na réplica do bucket de distribuição da CDN.">
                  Last-Modified <span class="hint-icon">?</span>
                </span>
                <div class="hint-desc">Timestamp no storage</div>
              </td>
              <td id="mSimLm" class="code" style="color:#94a3b8; padding:6px 0; vertical-align:top;">-</td>
            </tr>
            <tr>
              <td style="color:#94a3b8; padding:6px 0; vertical-align:top;">
                <span class="header-hint" title="IP do servidor de borda (Edge PoP) da Akamai que atendeu a requisição. Permite identificar se requisições caíram em nós diferentes!">
                  Instância (IP) <span class="hint-icon">?</span>
                </span>
                <div class="hint-desc">PoP / Nó de borda</div>
              </td>
              <td id="mSimIp" class="code" style="color:#38bdf8; font-weight:700; padding:6px 0; vertical-align:top;">-</td>
            </tr>
            <tr>
              <td style="color:#94a3b8; padding:6px 0; vertical-align:top;">
                <span class="header-hint" title="Camada de distribuição que intercepta o tráfego antes de chegar aos servidores do TSE.">
                  Server <span class="hint-icon">?</span>
                </span>
                <div class="hint-desc">Rede de entrega</div>
              </td>
              <td id="mSimServer" class="code" style="color:#94a3b8; padding:6px 0; vertical-align:top;">Akamai CDN</td>
            </tr>
            <tr>
              <td style="color:#94a3b8; padding:6px 0; vertical-align:top;">
                <span class="header-hint" title="Global Request Number da Akamai para rastreamento forense de requisições de borda.">
                  Akamai-GRN <span class="hint-icon">?</span>
                </span>
                <div class="hint-desc">Rastreio CDN / Borda</div>
              </td>
              <td id="mSimGrn" class="code" style="color:#c084fc; font-family:monospace; padding:6px 0; vertical-align:top; word-break:break-all;">-</td>
            </tr>
          </table>
        </div>
      </div>

      <!-- Análise Diagnóstica Automática -->
      <div style="background:#0f172a; border:1px solid #334155; border-radius:8px; padding:12px 14px; font-size:0.8rem; line-height:1.5;">
        <div style="font-weight:700; color:#fbbf24; margin-bottom:4px;">💡 Análise de Impacto no Cache:</div>
        <div id="cacheAnalysisText" style="color:#cbd5e1;">-</div>
      </div>

      <div style="display:flex; justify-content:flex-end; margin-top:18px; gap:10px;">
        <button onclick="closeCacheModal()" class="btn btn-outline" style="padding:8px 18px;">Fechar</button>
      </div>
    </div>
  </div>

  <!-- MODAL DE GERENCIAMENTO DE SERVIDORES MONITORADOS -->
  <div id="servidoresModal" style="display:none; position:fixed; top:0; left:0; width:100%; height:100%; background:rgba(0,0,0,0.75); z-index:9999; align-items:center; justify-content:center; backdrop-filter:blur(3px);">
    <div style="background:#1e293b; border:1px solid #475569; border-radius:14px; width:94%; max-width:840px; padding:24px; box-shadow:0 25px 50px -12px rgba(0,0,0,0.6); color:#f8fafc; max-height:90vh; overflow-y:auto;">
      <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:14px; border-bottom:1px solid #334155; padding-bottom:12px;">
        <h3 style="margin:0; font-size:1.15rem; display:flex; align-items:center; gap:8px;">
          🖥️ Servidores & Topologia de Polling (Multi-Nós)
        </h3>
        <button onclick="closeServidoresModal()" style="background:transparent; border:none; color:#94a3b8; font-size:1.4rem; cursor:pointer; line-height:1;">&times;</button>
      </div>

      <p style="font-size:0.83rem; color:#94a3b8; margin-bottom:16px; line-height:1.4;">
        Cadastre múltiplos servidores ou nós de borda para auditoria paralela. O nó com papel <strong>ORIGEM</strong> atua como master de referência para medir atrasos de propagação e SLAs de sincronização de todas as <strong>RÉPLICAS</strong>.
      </p>

      <!-- FORMULÁRIO DE CADASTRO / EDIÇÃO -->
      <div style="background:#0f172a; border:1px solid #334155; border-radius:10px; padding:16px; margin-bottom:20px;">
        <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:10px;">
          <h4 id="srvFormTitle" style="margin:0; font-size:0.92rem; color:#38bdf8;">➕ Cadastrar Novo Nó / Servidor</h4>
          <button id="srvCancelBtn" onclick="resetServidorForm()" class="btn-copy" style="display:none; font-size:0.75rem;">Cancelar Edição</button>
        </div>

        <div style="display:grid; grid-template-columns:repeat(auto-fit, minmax(200px, 1fr)); gap:12px; margin-bottom:12px;">
          <div>
            <label style="display:block; font-size:0.75rem; font-weight:700; color:#94a3b8; margin-bottom:4px;">CHAVE (Identificador Único):</label>
            <input type="text" id="srvChaveInput" placeholder="Ex: HMG, SIM, SLAVE1, INTERNO..." style="width:100%; background:#1e293b; border:1px solid #475569; color:#f8fafc; padding:8px 12px; border-radius:6px; font-size:0.85rem; font-family:monospace; text-transform:uppercase; outline:none;" />
          </div>
          <div>
            <label style="display:block; font-size:0.75rem; font-weight:700; color:#94a3b8; margin-bottom:4px;">NOME DESCRITIVO:</label>
            <input type="text" id="srvNomeInput" placeholder="Ex: Origem Homologação, Borda Akamai..." style="width:100%; background:#1e293b; border:1px solid #475569; color:#f8fafc; padding:8px 12px; border-radius:6px; font-size:0.85rem; outline:none;" />
          </div>
          <div>
            <label style="display:block; font-size:0.75rem; font-weight:700; color:#94a3b8; margin-bottom:4px;">PAPEL ARQUITETURAL:</label>
            <select id="srvPapelSelect" style="width:100%; background:#1e293b; border:1px solid #475569; color:#f8fafc; padding:8px 12px; border-radius:6px; font-size:0.85rem; outline:none;">
              <option value="REPLICA">RÉPLICA (Cache / Distribuição / Slave)</option>
              <option value="ORIGEM">ORIGEM (Master de Referência Oficial)</option>
            </select>
          </div>
        </div>

        <div style="margin-bottom:12px;">
          <label style="display:block; font-size:0.75rem; font-weight:700; color:#94a3b8; margin-bottom:4px;">URL BASE (com /teste/ ou /simulado/teste/ no final):</label>
          <input type="text" id="srvUrlInput" placeholder="https://resultados-sim.tse.jus.br/simulado/teste/" style="width:100%; background:#1e293b; border:1px solid #475569; color:#f8fafc; padding:8px 12px; border-radius:6px; font-size:0.85rem; font-family:monospace; outline:none;" />
        </div>

        <div style="display:flex; justify-content:space-between; align-items:center; flex-wrap:wrap; gap:10px;">
          <button type="button" onclick="testFormServer()" class="btn-copy" style="padding:6px 12px; font-size:0.80rem;">📡 Testar Conexão / IP</button>
          <div id="srvTestFeedback" style="font-size:0.80rem; font-family:monospace; color:#94a3b8;"></div>
          <button type="button" onclick="submitServidorForm()" class="btn" style="background:#10b981; padding:8px 20px;">Salvar Servidor</button>
        </div>
      </div>

      <!-- LISTA DE SERVIDORES CADASTRADOS -->
      <div>
        <h4 style="margin:0 0 10px 0; font-size:0.92rem; color:#f8fafc;">Servidores Cadastrados no Banco</h4>
        <div id="servidoresListContainer" style="display:flex; flex-direction:column; gap:10px; max-height:360px; overflow-y:auto;">
          Carregando servidores...
        </div>
      </div>

      <div style="display:flex; justify-content:flex-end; margin-top:20px;">
        <button onclick="closeServidoresModal()" class="btn btn-outline" style="padding:8px 20px;">Fechar</button>
      </div>
    </div>
  </div>

  <!-- MODAL DE MATRIZ COMPARATIVA MULTI-NÓS -->
  <div id="multiNodeModal" style="display:none; position:fixed; top:0; left:0; width:100%; height:100%; background:rgba(0,0,0,0.75); z-index:9999; align-items:center; justify-content:center; backdrop-filter:blur(3px);">
    <div style="background:#1e293b; border:1px solid #475569; border-radius:14px; width:95%; max-width:1050px; padding:24px; box-shadow:0 25px 50px -12px rgba(0,0,0,0.6); color:#f8fafc; max-height:90vh; overflow-y:auto;">
      <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:12px; border-bottom:1px solid #334155; padding-bottom:12px;">
        <h3 style="margin:0; font-size:1.15rem; display:flex; align-items:center; gap:8px;">
          🔍 Matriz Comparativa de Todos os Nós
        </h3>
        <button onclick="closeMultiNodeModal()" style="background:transparent; border:none; color:#94a3b8; font-size:1.4rem; cursor:pointer; line-height:1;">&times;</button>
      </div>

      <div id="multiNodePath" class="code" style="font-size:0.82rem; color:#38bdf8; word-break:break-all; margin-bottom:16px; background:#0f172a; padding:8px 12px; border-radius:6px; border:1px solid #334155;"></div>

      <div style="overflow-x:auto;">
        <table style="width:100%; border-collapse:collapse; font-size:0.82rem;">
          <thead>
            <tr style="background:#0f172a; border-bottom:1px solid #475569;">
              <th style="padding:10px 8px; text-align:left;">Nó / Servidor</th>
              <th style="padding:10px 8px; text-align:left;">Instância (IP)</th>
              <th style="padding:10px 8px; text-align:left;">Geração (DG/HG)</th>
              <th style="padding:10px 8px; text-align:left;">Totalização (DT/HT)</th>
              <th style="padding:10px 8px; text-align:left;">Seções (ST / %)</th>
              <th style="padding:10px 8px; text-align:left;">IDG</th>
              <th style="padding:10px 8px; text-align:left;">Δ vs Origem</th>
              <th style="padding:10px 8px; text-align:left;">SLA Sync</th>
              <th style="padding:10px 8px; text-align:left;">Cache-Control / TTL</th>
              <th style="padding:10px 8px; text-align:left;">CDN / ETag</th>
              <th style="padding:10px 8px; text-align:center;">Link</th>
            </tr>
          </thead>
          <tbody id="multiNodeTableBody">
            <tr><td colspan="11" style="text-align:center; padding:18px;">Carregando dados dos servidores...</td></tr>
          </tbody>
        </table>
      </div>

      <div style="display:flex; justify-content:flex-end; margin-top:20px;">
        <button onclick="closeMultiNodeModal()" class="btn btn-outline" style="padding:8px 20px;">Fechar</button>
      </div>
    </div>
  </div>

  <!-- MODAL DE SELEÇÃO DE ELEIÇÕES MONITORADAS -->
  <div id="eleicoesModal" style="display:none; position:fixed; top:0; left:0; width:100%; height:100%; background:rgba(0,0,0,0.75); z-index:9999; align-items:center; justify-content:center; backdrop-filter:blur(3px);">
    <div style="background:#1e293b; border:1px solid #475569; border-radius:14px; width:92%; max-width:680px; padding:24px; box-shadow:0 25px 50px -12px rgba(0,0,0,0.6); color:#f8fafc; max-height:90vh; overflow-y:auto;">
      <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:14px; border-bottom:1px solid #334155; padding-bottom:12px;">
        <h3 style="margin:0; font-size:1.15rem; display:flex; align-items:center; gap:8px;">
          🗳️ Escolher Eleições para Monitoramento
        </h3>
        <button onclick="closeEleicoesModal()" style="background:transparent; border:none; color:#94a3b8; font-size:1.4rem; cursor:pointer; line-height:1;">&times;</button>
      </div>

      <p style="font-size:0.83rem; color:#94a3b8; margin-bottom:16px; line-height:1.4;">
        Selecione quais das eleições disponíveis no combo do TSE devem ser incluídas na varredura contínua e na auditoria. Você pode monitorar todas simultaneamente ou focar em eleições específicas.
      </p>

      <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:14px; gap:10px; flex-wrap:wrap;">
        <div style="display:flex; gap:8px;">
          <button onclick="toggleAllEleicoes(true)" class="btn" style="background:#0284c7; padding:6px 12px; font-size:0.78rem;">✓ Selecionar Todas</button>
          <button onclick="toggleAllEleicoes(false)" class="btn btn-outline" style="padding:6px 12px; font-size:0.78rem;">✗ Desmarcar Todas</button>
        </div>
        <span id="eleicoesModalStats" style="font-size:0.8rem; color:#38bdf8; font-weight:600;"></span>
      </div>

      <div id="eleicoesListContainer" style="display:flex; flex-direction:column; gap:8px; max-height:360px; overflow-y:auto; padding-right:4px;">
        Carregando eleições disponíveis...
      </div>

      <div style="display:flex; justify-content:flex-end; margin-top:20px; gap:10px;">
        <button onclick="closeEleicoesModal()" class="btn btn-outline" style="padding:8px 18px;">Fechar</button>
      </div>
    </div>
  </div>

  <!-- MODAL DE GERENCIAMENTO DE RODADAS -->
  <div id="rodadasModal" style="display:none; position:fixed; top:0; left:0; width:100%; height:100%; background:rgba(0,0,0,0.75); z-index:9999; align-items:center; justify-content:center; backdrop-filter:blur(3px);">
    <div style="background:#1e293b; border:1px solid #475569; border-radius:14px; width:92%; max-width:620px; padding:24px; box-shadow:0 25px 50px -12px rgba(0,0,0,0.6); color:#f8fafc; max-height:90vh; overflow-y:auto;">
      <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:14px; border-bottom:1px solid #334155; padding-bottom:12px;">
        <h3 style="margin:0; font-size:1.15rem; display:flex; align-items:center; gap:8px;">
          📍 Delimitador Lógico de Rodadas
        </h3>
        <button onclick="closeRodadasModal()" style="background:transparent; border:none; color:#94a3b8; font-size:1.4rem; cursor:pointer; line-height:1;">&times;</button>
      </div>

      <p style="font-size:0.83rem; color:#94a3b8; margin-bottom:16px; line-height:1.4;">
        As rodadas definem o <strong>marco zero</strong> para contagem de regressões e cálculo de SLAs, sem apagar nenhum dado do banco SQLite.
      </p>

      <!-- CRIAR NOVA RODADA -->
      <div style="background:#0f172a; border:1px solid #334155; border-radius:8px; padding:14px; margin-bottom:20px;">
        <h4 style="margin:0 0 10px 0; font-size:0.9rem; color:#38bdf8;">➕ Iniciar Nova Rodada</h4>
        <div style="display:flex; gap:10px; flex-wrap:wrap;">
          <input type="text" id="newRodadaInput" placeholder="Nome da rodada (Ex: Simulado Tarde, Carga 50%)..." style="flex:1; min-width:200px; background:#1e293b; border:1px solid #475569; color:#f8fafc; padding:8px 12px; border-radius:6px; font-size:0.85rem; outline:none;" />
          <button onclick="submitNewRodada()" class="btn" style="background:#10b981; padding:8px 16px;">🚀 Iniciar Rodada Agora</button>
        </div>
      </div>

      <!-- LISTA DE RODADAS HISTÓRICAS -->
      <div>
        <h4 style="margin:0 0 10px 0; font-size:0.9rem; color:#f8fafc;">Histórico de Rodadas Registradas</h4>
        <div id="rodadasListContainer" style="display:flex; flex-direction:column; gap:8px; max-height:280px; overflow-y:auto;">
          Carregando histórico...
        </div>
      </div>

      <div style="display:flex; justify-content:flex-end; margin-top:20px;">
        <button onclick="closeRodadasModal()" class="btn btn-outline" style="padding:8px 18px;">Fechar</button>
      </div>
    </div>
  </div>

  <!-- MODAL DE EDIÇÃO DE ESCOPO DA RODADA (NOME + DATA E HORA COM SEGUNDOS) -->
  <div id="editRodadaModal" style="display:none; position:fixed; top:0; left:0; width:100%; height:100%; background:rgba(0,0,0,0.8); z-index:10000; align-items:center; justify-content:center; backdrop-filter:blur(4px);">
    <div style="background:#1e293b; border:1px solid #38bdf8; border-radius:14px; width:92%; max-width:520px; padding:24px; box-shadow:0 25px 50px -12px rgba(0,0,0,0.7); color:#f8fafc;">
      <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:14px; border-bottom:1px solid #334155; padding-bottom:12px;">
        <h3 style="margin:0; font-size:1.15rem; display:flex; align-items:center; gap:8px; color:#38bdf8;">
          ✏️ Editar Escopo da Rodada <span id="editRodadaBadge" style="font-size:0.75rem; font-weight:700; padding:2px 8px; border-radius:4px; background:rgba(255,255,255,0.08);"></span>
        </h3>
        <button onclick="closeEditRodadaModal()" style="background:transparent; border:none; color:#94a3b8; font-size:1.4rem; cursor:pointer; line-height:1;">&times;</button>
      </div>

      <input type="hidden" id="editRodadaId" value="" />

      <div style="margin-bottom:16px;">
        <label style="display:block; font-size:0.80rem; color:#94a3b8; font-weight:700; margin-bottom:6px;">Nome de Identificação da Rodada:</label>
        <input type="text" id="editRodadaNome" placeholder="Ex: Simulado Tarde, Carga 50%..." style="width:100%; background:#0f172a; border:1px solid #475569; color:#f8fafc; padding:8px 12px; border-radius:6px; font-size:0.88rem; outline:none; box-sizing:border-box;" />
      </div>

      <div style="margin-bottom:16px;">
        <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:6px;">
          <label style="font-size:0.80rem; color:#94a3b8; font-weight:700;">Data e Hora de Início (Marco Zero com Segundos):</label>
          <div style="display:flex; gap:6px;">
            <button type="button" onclick="setEditRodadaToMidnight()" class="btn-copy" style="font-size:0.68rem; padding:2px 6px;">Hoje 00:00:00</button>
            <button type="button" onclick="setEditRodadaToNow()" class="btn-copy" style="font-size:0.68rem; padding:2px 6px;">Agora</button>
          </div>
        </div>
        <input type="datetime-local" step="1" id="editRodadaInicio" style="width:100%; background:#0f172a; border:1px solid #475569; color:#f8fafc; padding:8px 12px; border-radius:6px; font-size:0.88rem; outline:none; box-sizing:border-box; color-scheme:dark;" />
        <div style="font-size:0.72rem; color:#94a3b8; margin-top:5px; line-height:1.4;">
          💡 Define o instante exato com segundos (<strong style="color:#e2e8f0;">HH:mm:ss</strong>) considerado para o marco zero das regressões e cálculo do SLA de propagação. Ocorrências anteriores são preservadas no histórico geral.
        </div>
      </div>

      <div style="display:flex; justify-content:flex-end; gap:10px; margin-top:20px;">
        <button onclick="closeEditRodadaModal()" class="btn btn-outline" style="padding:8px 16px;">Cancelar</button>
        <button onclick="submitEditRodadaModal()" class="btn" style="background:#0284c7; padding:8px 18px; font-weight:700;">💾 Salvar Alterações</button>
      </div>
    </div>
  </div>

  <!-- MODAL DE EXPORTAÇÃO ZIP DE VERSÕES -->
  <div id="zipModal" style="display:none; position:fixed; top:0; left:0; width:100%; height:100%; background:rgba(0,0,0,0.75); z-index:9999; align-items:center; justify-content:center; backdrop-filter:blur(3px);">
    <div style="background:#1e293b; border:1px solid #475569; border-radius:14px; width:90%; max-width:540px; padding:24px; box-shadow:0 25px 50px -12px rgba(0,0,0,0.6); color:#f8fafc;">
      <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:16px; border-bottom:1px solid #334155; padding-bottom:12px;">
        <h3 style="margin:0; font-size:1.15rem; display:flex; align-items:center; gap:8px;">
          📦 Exportar Versões Salvas (.ZIP)
        </h3>
        <button onclick="closeZipModal()" style="background:transparent; border:none; color:#94a3b8; font-size:1.4rem; cursor:pointer; line-height:1;">&times;</button>
      </div>

      <div id="zipFormArea">
        <p style="font-size:0.85rem; color:#94a3b8; margin-bottom:14px; line-height:1.4;">
          Baixe o pacote compactado contendo todos os arquivos JSON de versões capturadas e segregadas por eleição e ambiente.
        </p>

        <div style="display:grid; grid-template-columns:1fr; gap:12px; margin-bottom:16px;">
          <div>
            <label style="display:block; font-size:0.75rem; font-weight:600; color:#94a3b8; margin-bottom:4px; text-transform:uppercase;">Filtrar Eleição:</label>
            <select id="zipElectionSelect" onchange="updateZipEst()" style="width:100%; background:#0f172a; border:1px solid #334155; color:#f8fafc; padding:8px 10px; border-radius:8px; font-size:0.88rem; outline:none;">
              <option value="ALL">📦 Todas as Eleições</option>
            </select>
          </div>
          <div style="display:flex; align-items:center; gap:8px;">
            <input type="checkbox" id="zipApenasRodadaCheck" onchange="updateZipEst()" style="cursor:pointer;" />
            <label for="zipApenasRodadaCheck" style="font-size:0.82rem; color:#f8fafc; cursor:pointer;">Compactar apenas arquivos gerados na rodada ativa</label>
          </div>
        </div>

        <div style="background:#0f172a; border:1px solid #334155; border-radius:8px; padding:12px 14px; margin-bottom:18px;">
          <div style="display:flex; justify-content:space-between; font-size:0.82rem; margin-bottom:6px;">
            <span style="color:#94a3b8;">Arquivos a compactar:</span>
            <span id="zipEstFiles" style="font-weight:700; color:#38bdf8;">Calculando...</span>
          </div>
          <div style="display:flex; justify-content:space-between; font-size:0.82rem;">
            <span style="color:#94a3b8;">Estimativa de tamanho:</span>
            <span id="zipEstSize" style="font-weight:700; color:#10b981;">Calculando...</span>
          </div>
        </div>

        <div style="display:flex; justify-content:flex-end; gap:10px;">
          <button onclick="closeZipModal()" class="btn btn-outline" style="padding:8px 16px;">Cancelar</button>
          <button id="zipStartBtn" onclick="startZipExport()" class="btn" style="background:#0284c7; padding:8px 18px; font-weight:700;">🚀 Gerar e Baixar ZIP</button>
        </div>
      </div>

      <!-- ÁREA DE PROGRESSO -->
      <div id="zipProgressArea" style="display:none; padding:10px 0;">
        <div style="display:flex; justify-content:space-between; font-size:0.85rem; font-weight:600; margin-bottom:6px;">
          <span id="zipProgressStage" style="color:#38bdf8;">Processando arquivos...</span>
          <span id="zipProgressPercent" style="color:#10b981;">0%</span>
        </div>

        <!-- BARRA DE PROGRESSO -->
        <div style="background:#0f172a; border-radius:8px; height:18px; overflow:hidden; border:1px solid #334155; margin-bottom:10px;">
          <div id="zipProgressBar" style="width:0%; height:100%; background:linear-gradient(90deg, #0284c7, #10b981); transition:width 0.15s ease; border-radius:6px;"></div>
        </div>

        <div style="display:flex; justify-content:space-between; font-size:0.75rem; color:#94a3b8; margin-bottom:12px;">
          <span id="zipProgressCounts">0 / 0 arquivos</span>
          <span id="zipProgressSpeed"></span>
        </div>

        <div id="zipProgressCurrentFile" style="font-size:0.72rem; color:#64748b; font-family:monospace; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; background:#0f172a; padding:6px 10px; border-radius:6px; border:1px solid #1e293b; margin-bottom:16px;">
          Aguardando início...
        </div>

        <div id="zipFinishedActions" style="display:none; justify-content:flex-end; gap:10px;">
          <button onclick="closeZipModal()" class="btn btn-outline" style="padding:8px 16px;">Fechar</button>
          <a id="zipDirectDownloadLink" href="#" class="btn" style="background:#10b981; padding:8px 18px; text-decoration:none;">⬇️ Baixar Novamente</a>
        </div>
      </div>
    </div>
  </div>

  <!-- MODAL DE AUDITORIA FORENSE DE REGRESSÕES TEMPORAIS -->
  <div id="regressoesModal" style="display:none; position:fixed; top:0; left:0; width:100%; height:100%; background:rgba(0,0,0,0.85); z-index:9999; align-items:center; justify-content:center; backdrop-filter:blur(5px);">
    <div style="background:#1e293b; border:1px solid #475569; border-radius:14px; width:98vw; max-width:1700px; height:94vh; max-height:94vh; padding:18px 22px; box-shadow:0 25px 50px -12px rgba(0,0,0,0.7); color:#f8fafc; display:flex; flex-direction:column; overflow:hidden;">
      
      <!-- Cabeçalho da Modal -->
      <div style="display:flex; justify-content:space-between; align-items:flex-start; margin-bottom:12px; border-bottom:1px solid #334155; padding-bottom:10px; flex-shrink:0;">
        <div>
          <h3 style="margin:0; font-size:1.25rem; display:flex; align-items:center; gap:8px; color:#f87171;">
            🚨 Dossiê Forense de Regressões Detectadas
          </h3>
          <div style="font-size:0.80rem; color:#94a3b8; margin-top:4px;">
            Casos registrados na rodada ativa: <strong id="regModalRodadaNome" style="color:#38bdf8;">-</strong> | Total de ocorrências: <strong id="regModalTotalCount" style="color:#ef4444;">0</strong>
          </div>
        </div>
        <div style="display:flex; align-items:center; gap:8px;">
          <button onclick="loadRegressoesData()" class="btn-copy" style="padding:5px 12px; font-size:0.75rem;" title="Recarregar dados">🔄 Atualizar</button>
          <a href="/download/csv-regressoes" class="btn-copy" style="padding:5px 12px; font-size:0.75rem; text-decoration:none;" title="Baixar histórico CSV">📊 Baixar CSV</a>
          <a href="/export/dossie-html" class="btn-copy" style="padding:5px 12px; font-size:0.75rem; text-decoration:none; background:#0284c7; color:#fff; font-weight:600;" title="Exportar Dossiê HTML Completo e Autônomo Offline (compartilhável)">📥 Exportar HTML Offline</a>
          <a href="/report" target="_blank" class="btn-copy" style="padding:5px 12px; font-size:0.75rem; text-decoration:none; background:#dc2626; color:#fff;" title="Dossiê HTML para impressão">📄 Dossiê HTML</a>
          <button onclick="closeRegressoesModal()" style="background:transparent; border:none; color:#94a3b8; font-size:1.6rem; cursor:pointer; line-height:1; margin-left:8px;">&times;</button>
        </div>
      </div>

      <!-- Barra de Filtros Internos da Modal -->
      <div style="background:#0f172a; border:1px solid #334155; border-radius:8px; padding:8px 14px; margin-bottom:12px; display:flex; flex-wrap:wrap; gap:10px; align-items:center; flex-shrink:0;">
        <div style="flex:1; min-width:240px; display:flex; gap:6px;">
          <input type="text" id="regSearchInput" placeholder="🔍 Buscar por ID (#26374), arquivo, UF, cargo, IP, GRN ou motivo..." oninput="renderFilteredRegressoes(true)" onkeydown="if(event.key==='Enter') buscarRegressoesRemoto();" style="flex:1; background:#1e293b; border:1px solid #475569; color:#f8fafc; padding:5px 10px; border-radius:6px; font-size:0.80rem; outline:none;" />
          <button onclick="buscarRegressoesRemoto()" class="btn-copy" style="padding:5px 10px; font-size:0.78rem; background:#2563eb; color:#fff; border-radius:6px; white-space:nowrap; cursor:pointer;" title="Buscar no histórico do SQLite">🔍 Buscar</button>
        </div>
        <div style="display:flex; align-items:center; gap:6px;">
          <label style="font-size:0.75rem; color:#94a3b8; font-weight:700;">GRN:</label>
          <input type="text" id="regFilterGrn" placeholder="Filtrar por Akamai-GRN..." oninput="renderFilteredRegressoes(true)" onkeydown="if(event.key==='Enter') buscarRegressoesRemoto();" style="background:#1e293b; border:1px solid #475569; color:#f8fafc; padding:5px 8px; border-radius:6px; font-size:0.78rem; width:150px; outline:none;" />
        </div>
        <div style="display:flex; align-items:center; gap:6px;">
          <label style="font-size:0.75rem; color:#94a3b8; font-weight:700;">Servidor:</label>
          <select id="regFilterServer" onchange="renderFilteredRegressoes(true)" style="background:#1e293b; border:1px solid #475569; color:#f8fafc; padding:5px 8px; border-radius:6px; font-size:0.78rem;">
            <option value="">Todos os Servidores</option>
            <option value="SIM">SIM (Cache Akamai)</option>
            <option value="HMG">HMG (Fonte Oficial)</option>
          </select>
        </div>
        <div style="display:flex; align-items:center; gap:6px;">
          <label style="font-size:0.75rem; color:#94a3b8; font-weight:700;">UF:</label>
          <select id="regFilterUf" onchange="renderFilteredRegressoes(true)" style="background:#1e293b; border:1px solid #475569; color:#f8fafc; padding:5px 8px; border-radius:6px; font-size:0.78rem;">
            <option value="">Todas as UFs</option>
          </select>
        </div>
        <div style="display:flex; align-items:center; gap:6px;">
          <label style="font-size:0.75rem; color:#94a3b8; font-weight:700;">Critério:</label>
          <select id="regFilterCriterion" onchange="renderFilteredRegressoes(true)" style="background:#1e293b; border:1px solid #475569; color:#f8fafc; padding:5px 8px; border-radius:6px; font-size:0.78rem;">
            <option value="">Todos os Critérios</option>
            <option value="INVERSAO_DG_DT_ST">🚨 Inversão de Dados (DG ↗, DT/ST ↘)</option>
            <option value="TEMPO">DG/HG (Tempo Geração)</option>
            <option value="TOTALIZAÇÃO">DT/HT (Totalização)</option>
            <option value="SEÇÕES">ST (Seções Apuradas)</option>
            <option value="SEQUENCIAL">IDG (Sequencial)</option>
          </select>
        </div>
        <div style="margin-left:auto; display:flex; align-items:center; gap:10px;">
          <button id="btnToggleAllRegs" type="button" onclick="toggleAllRegCards()" class="btn-copy" style="padding:4px 10px; font-size:0.75rem; background:#1e293b; border:1px solid #475569; color:#cbd5e1; display:flex; align-items:center; gap:5px; cursor:pointer;" title="Expandir ou colapsar todas as ocorrências">
            <span id="toggleAllRegsIcon">↕️</span> <span id="toggleAllRegsText">Expandir Todos</span>
          </button>
          <div id="regShowingCount" style="font-size:0.78rem; color:#64748b;">
            Exibindo 0 de 0
          </div>
        </div>
      </div>

      <!-- CORPO DA MODAL: LAYOUT SPLIT EM 2 COLUNAS INDEPENDENTES -->
      <div style="flex:1; min-height:0; display:flex; gap:16px; overflow:hidden;">
        
        <!-- Coluna Esquerda: Feed / Lista de Ocorrências com Scroll Independente -->
        <div id="regressoesListContainer" style="flex:1.05; min-width:0; overflow-y:auto; padding-right:8px; display:flex; flex-direction:column; gap:12px;">
          <!-- Inserido dinamicamente via JS -->
        </div>

        <!-- Coluna Direita: Painel Lateral Fixo com Detalhes Técnicos e Scroll Independente -->
        <div id="regressoesTechPanel" style="flex:0.95; min-width:460px; max-width:720px; background:#0f172a; border:1px solid #334155; border-radius:10px; display:flex; flex-direction:column; overflow:hidden; box-shadow:inset 0 2px 8px rgba(0,0,0,0.3);">
          <div style="background:#1e293b; padding:10px 14px; border-bottom:1px solid #334155; display:flex; justify-content:space-between; align-items:center; flex-shrink:0;">
            <div style="font-size:0.85rem; font-weight:700; color:#38bdf8; display:flex; align-items:center; gap:6px;">
              <span>🌐</span> Painel Forense & Detalhes Técnicos
            </div>
            <div id="techPanelSelectedBadge" style="font-family:monospace; font-size:0.75rem; color:#94a3b8;">
              Nenhum selecionado
            </div>
          </div>
          <div id="techPanelContent" style="flex:1; min-height:0; overflow-y:auto; padding:14px; font-size:0.78rem;">
            <div style="text-align:center; padding:60px 20px; color:#64748b;">
              <div style="font-size:2rem; margin-bottom:8px;">👈</div>
              <div>Selecione qualquer ocorrência na lista à esquerda para auditar aqui seus metadados de rede, cabeçalhos de solicitação, resposta e controle de cache.</div>
            </div>
          </div>
        </div>

      </div>

      <!-- Rodapé da Modal -->
      <div style="display:flex; justify-content:space-between; align-items:center; margin-top:10px; border-top:1px solid #334155; padding-top:8px; font-size:0.75rem; color:#64748b; flex-shrink:0;">
        <span>💡 Clique em qualquer ocorrência na coluna esquerda para fixar a inspeção técnica de cabeçalhos e rede no painel direito.</span>
        <button onclick="closeRegressoesModal()" class="btn btn-outline" style="padding:5px 16px; font-size:0.82rem;">Fechar</button>
      </div>

    </div>
  </div>
</body>
</html>`;


// =====================================================================
// MOTOR DE COMPACTAÇÃO STREAMING ZIP (Zero dependências externas)
// =====================================================================
const zipJobs = new Map();

async function buildZipStream(files, baseDir, tempZipPath, onProgress) {
  const out = fs.createWriteStream(tempZipPath);
  const centralHeaders = [];
  let offset = 0;

  for (let i = 0; i < files.length; i++) {
    const filePath = files[i];
    const rel = path.relative(baseDir, filePath).replace(/\\/g, '/');
    const nameBuf = Buffer.from(rel, 'utf8');

    let uncompressed;
    try {
      uncompressed = fs.readFileSync(filePath);
    } catch {
      continue;
    }

    const crc = zlib.crc32(uncompressed);
    const compressed = zlib.deflateRawSync(uncompressed, { level: 1 });

    const now = new Date();
    const dosTime = ((now.getHours() << 11) | (now.getMinutes() << 5) | (now.getSeconds() >> 1)) & 0xFFFF;
    const dosDate = (((now.getFullYear() - 1980) << 9) | ((now.getMonth() + 1) << 5) | now.getDate()) & 0xFFFF;

    const localHeader = Buffer.alloc(30 + nameBuf.length);
    localHeader.writeUInt32LE(0x04034b50, 0);
    localHeader.writeUInt16LE(20, 4);
    localHeader.writeUInt16LE(0x0800, 6);
    localHeader.writeUInt16LE(8, 8);
    localHeader.writeUInt16LE(dosTime, 10);
    localHeader.writeUInt16LE(dosDate, 12);
    localHeader.writeUInt32LE(crc, 14);
    localHeader.writeUInt32LE(compressed.length, 18);
    localHeader.writeUInt32LE(uncompressed.length, 22);
    localHeader.writeUInt16LE(nameBuf.length, 26);
    localHeader.writeUInt16LE(0, 28);
    nameBuf.copy(localHeader, 30);

    out.write(localHeader);
    out.write(compressed);

    const centralHeader = Buffer.alloc(46 + nameBuf.length);
    centralHeader.writeUInt32LE(0x02014b50, 0);
    centralHeader.writeUInt16LE(20, 4);
    centralHeader.writeUInt16LE(20, 6);
    centralHeader.writeUInt16LE(0x0800, 8);
    centralHeader.writeUInt16LE(8, 10);
    centralHeader.writeUInt16LE(dosTime, 12);
    centralHeader.writeUInt16LE(dosDate, 14);
    centralHeader.writeUInt32LE(crc, 16);
    centralHeader.writeUInt32LE(compressed.length, 20);
    centralHeader.writeUInt32LE(uncompressed.length, 24);
    centralHeader.writeUInt16LE(nameBuf.length, 28);
    centralHeader.writeUInt16LE(0, 30);
    centralHeader.writeUInt16LE(0, 32);
    centralHeader.writeUInt16LE(0, 34);
    centralHeader.writeUInt16LE(0, 36);
    centralHeader.writeUInt32LE(0, 38);
    centralHeader.writeUInt32LE(offset, 42);
    nameBuf.copy(centralHeader, 46);

    centralHeaders.push(centralHeader);
    offset += localHeader.length + compressed.length;

    if (onProgress && (i % 25 === 0 || i === files.length - 1)) {
      onProgress(i + 1, files.length, rel);
      await new Promise(resolve => setImmediate(resolve));
    }
  }

  const centralDirOffset = offset;
  let centralDirSize = 0;
  for (const ch of centralHeaders) {
    centralDirSize += ch.length;
    out.write(ch);
  }

  const eocd = Buffer.alloc(22);
  eocd.writeUInt32LE(0x06054b50, 0);
  eocd.writeUInt16LE(0, 4);
  eocd.writeUInt16LE(0, 6);
  eocd.writeUInt16LE(centralHeaders.length, 8);
  eocd.writeUInt16LE(centralHeaders.length, 10);
  eocd.writeUInt32LE(centralDirSize, 12);
  eocd.writeUInt32LE(centralDirOffset, 16);
  eocd.writeUInt16LE(0, 20);

  out.write(eocd);

  await new Promise((resolve, reject) => {
    out.end(() => resolve());
    out.on('error', reject);
  });
}

function scanVersoesFast(baseDir, minMtimeMs = null) {
  if (!fs.existsSync(baseDir)) return { elections: {}, totalFiles: 0 };
  const elections = {};
  let totalFiles = 0;

  function walk(currDir, eleicaoTag) {
    const entries = fs.readdirSync(currDir, { withFileTypes: true });
    for (const ent of entries) {
      if (ent.isDirectory()) {
        let curEl = eleicaoTag;
        if (!curEl && (path.basename(currDir) === 'tdtot2026' || /^ele\d{4}$/i.test(path.basename(currDir)))) {
          curEl = ent.name;
        }
        walk(path.join(currDir, ent.name), curEl);
      } else if (ent.isFile()) {
        if (minMtimeMs) {
          try {
            const stat = fs.statSync(path.join(currDir, ent.name));
            if (stat.mtimeMs < minMtimeMs) continue;
          } catch { continue; }
        }
        totalFiles++;
        const el = eleicaoTag || 'Geral';
        elections[el] = (elections[el] || 0) + 1;
      }
    }
  }

  walk(baseDir, null);
  return { elections, totalFiles };
}

function collectFilesForZip(baseDir, eleicaoFilter, minMtimeMs = null) {
  const files = [];
  function walk(currDir, eleicaoTag) {
    if (!fs.existsSync(currDir)) return;
    const entries = fs.readdirSync(currDir, { withFileTypes: true });
    for (const ent of entries) {
      const fullPath = path.join(currDir, ent.name);
      if (ent.isDirectory()) {
        let curEl = eleicaoTag;
        if (!curEl && (path.basename(currDir) === 'tdtot2026' || /^ele\d{4}$/i.test(path.basename(currDir)))) {
          curEl = ent.name;
        }
        walk(fullPath, curEl);
      } else if (ent.isFile()) {
        if (!eleicaoFilter || eleicaoFilter === 'ALL' || eleicaoTag === eleicaoFilter) {
          if (minMtimeMs) {
            try {
              const stat = fs.statSync(fullPath);
              if (stat.mtimeMs >= minMtimeMs) files.push(fullPath);
            } catch {}
          } else {
            files.push(fullPath);
          }
        }
      }
    }
  }
  walk(baseDir, null);
  return files;
}

const evidenceMetaCache = new Map();
function getRawMetadataFast(filePath) {
  if (!filePath) return null;
  if (evidenceMetaCache.has(filePath)) return evidenceMetaCache.get(filePath);
  try {
    if (fs.existsSync(filePath)) {
      const parsed = JSON.parse(fs.readFileSync(filePath, 'utf8'));
      const meta = parsed.metadata || null;
      evidenceMetaCache.set(filePath, meta);
      if (evidenceMetaCache.size > 300) {
        const firstKey = evidenceMetaCache.keys().next().value;
        evidenceMetaCache.delete(firstKey);
      }
      return meta;
    }
  } catch {}
  evidenceMetaCache.set(filePath, null);
  return null;
}

function getTodaySyncData() {
  const now = Date.now();
  if (cachedTodaySync && (now - lastTodaySyncFetch < 5000)) {
    return cachedTodaySync;
  }
  const rodada = getActiveRodada();
  const rodadaStartUnix = rodada ? rodada.inicio_unix : getTodayMidnightUnix();
  
  try {
    const syncRows = db.prepare(`
      WITH hmg_first AS (
        SELECT arquivo, dg, hg, MIN(timestamp_unix) as h_first
        FROM leituras
        WHERE (papel_servidor = 'ORIGEM' OR servidor = 'HMG') AND timestamp_unix >= ?
        GROUP BY arquivo, dg, hg
      ),
      sim_first AS (
        SELECT arquivo, dg, hg, MIN(timestamp_unix) as s_first
        FROM leituras
        WHERE (papel_servidor = 'REPLICA' OR servidor = 'SIM') AND timestamp_unix >= ?
        GROUP BY arquivo, dg, hg
      )
      SELECT 
        h.arquivo,
        CASE WHEN s.s_first < h.h_first THEN 0 ELSE ROUND((s.s_first - h.h_first) / 1000.0) END as sync_sec
      FROM hmg_first h
      JOIN sim_first s ON h.arquivo = s.arquivo AND h.dg = s.dg AND h.hg = s.hg
    `).all(rodadaStartUnix, rodadaStartUnix);

    const todaySyncByFile = {};
    for (const row of syncRows) {
      if (!todaySyncByFile[row.arquivo]) todaySyncByFile[row.arquivo] = [];
      todaySyncByFile[row.arquivo].push(row.sync_sec);
    }

    const allTimes = syncRows.map(r => r.sync_sec).sort((a, b) => a - b);
    const globalTodayStats = {
      totalEvents: allTimes.length,
      avgSec: allTimes.length ? Math.round(allTimes.reduce((a, b) => a + b, 0) / allTimes.length) : 0,
      p90Sec: allTimes.length ? allTimes[Math.floor(allTimes.length * 0.90)] : 0,
      p95Sec: allTimes.length ? allTimes[Math.floor(allTimes.length * 0.95)] : 0,
      p99Sec: allTimes.length ? allTimes[Math.floor(allTimes.length * 0.99)] : 0,
      p100Sec: allTimes.length ? allTimes[allTimes.length - 1] : 0
    };

    cachedTodaySync = { todaySyncByFile, globalTodayStats };
  } catch(e) {
    cachedTodaySync = { todaySyncByFile: {}, globalTodayStats: { totalEvents: 0, avgSec: 0, p90Sec: 0, p95Sec: 0, p99Sec: 0, p100Sec: 0 } };
  }
  lastTodaySyncFetch = now;
  return cachedTodaySync;
}

function buildHistoricalComparisonPayload(rodadaId) {
  const rFound = db.prepare('SELECT * FROM rodadas WHERE id = ?').get(Number(rodadaId));
  if (!rFound) return null;

  const rStartUnix = rFound.inicio_unix;
  const rEndUnix = rFound.fim_unix || null;
  const rStartIso = rFound.inicio_iso;
  const rEndIso = rFound.fim_iso || null;

  const sqlHydrate = `
    SELECT l.* FROM leituras l
    INNER JOIN (
      SELECT servidor, arquivo, MAX(id) as max_id
      FROM leituras
      WHERE timestamp_unix >= ? AND (? IS NULL OR timestamp_unix <= ?)
      GROUP BY servidor, arquivo
    ) latest ON l.id = latest.max_id
  `;
  const rows = db.prepare(sqlHydrate).all(rStartUnix, rEndUnix, rEndUnix);

  const histStates = {};
  const fileSet = new Set();
  for (const r of rows) {
    if (!histStates[r.servidor]) histStates[r.servidor] = new Map();
    histStates[r.servidor].set(r.arquivo, {
      serverKey: r.servidor,
      relPath: r.arquivo,
      filename: getFilename(r.arquivo),
      idg: r.idg,
      idgNum: r.idg ? Number(r.idg) : null,
      dg: r.dg,
      hg: r.hg,
      genTime: parseDgHg(r.dg, r.hg),
      st: (r.secoes !== null && r.secoes !== undefined && String(r.secoes).trim() !== '') ? Number(r.secoes) : null,
      pst: r.secoes_pct,
      vTot: r.votos,
      dt: r.dt || null,
      ht: r.ht || null,
      totTime: parseDgHg(r.dt, r.ht),
      etag: r.etag,
      maxAge: r.max_age,
      cdnCacheStatus: r.cdn_status,
      status: r.status_ordem || 'CARREGADO_DB'
    });
    fileSet.add(r.arquivo);
  }

  // SLA da rodada histórica
  const sqlSla = `
    WITH hmg_first AS (
      SELECT arquivo, dg, hg, MIN(timestamp_unix) as h_first
      FROM leituras
      WHERE (papel_servidor = 'ORIGEM' OR servidor = 'HMG') AND timestamp_unix >= ? AND (? IS NULL OR timestamp_unix <= ?)
      GROUP BY arquivo, dg, hg
    ),
    sim_first AS (
      SELECT arquivo, dg, hg, MIN(timestamp_unix) as s_first
      FROM leituras
      WHERE (papel_servidor = 'REPLICA' OR servidor = 'SIM') AND timestamp_unix >= ? AND (? IS NULL OR timestamp_unix <= ?)
      GROUP BY arquivo, dg, hg
    )
    SELECT 
      h.arquivo,
      CASE WHEN s.s_first < h.h_first THEN 0 ELSE ROUND((s.s_first - h.h_first) / 1000.0) END as sync_sec
    FROM hmg_first h
    JOIN sim_first s ON h.arquivo = s.arquivo AND h.dg = s.dg AND h.hg = s.hg
  `;
  const syncRows = db.prepare(sqlSla).all(rStartUnix, rEndUnix, rEndUnix, rStartUnix, rEndUnix, rEndUnix);
  const todaySyncByFile = {};
  for (const row of syncRows) {
    if (!todaySyncByFile[row.arquivo]) todaySyncByFile[row.arquivo] = [];
    todaySyncByFile[row.arquivo].push(row.sync_sec);
  }
  const allTimes = syncRows.map(r => r.sync_sec).sort((a, b) => a - b);
  const todaySlaStats = {
    totalEvents: allTimes.length,
    avgSec: allTimes.length ? Math.round(allTimes.reduce((a, b) => a + b, 0) / allTimes.length) : 0,
    p90Sec: allTimes.length ? allTimes[Math.floor(allTimes.length * 0.90)] : 0,
    p95Sec: allTimes.length ? allTimes[Math.floor(allTimes.length * 0.95)] : 0,
    p99Sec: allTimes.length ? allTimes[Math.floor(allTimes.length * 0.99)] : 0,
    p100Sec: allTimes.length ? allTimes[allTimes.length - 1] : 0
  };

  const comparisonList = [];
  let simTtlSum = 0, simTtlCount = 0, simTtlMin = null, simTtlMax = null;
  let cdnHits = 0, cdnTotal = 0;

  // Garante inclusão de todos os arquivos rastreados
  for (const f of Array.from(trackedFiles)) fileSet.add(f);

  const histOrigin = getOriginServer();
  const histReplica = getReplicaServers()[0];
  const histOriginKey = histOrigin ? histOrigin.chave : 'HMG';
  const histReplicaKey = histReplica ? histReplica.chave : 'SIM';

  for (const relPath of Array.from(fileSet)) {
    const hmg = (histStates[histOriginKey]?.get(relPath) || histStates['HMG']?.get(relPath)) || null;
    const sim = (histStates[histReplicaKey]?.get(relPath) || histStates['SIM']?.get(relPath)) || null;

    let delaySec = null;
    let statusTime = 'SEM_TIMESTAMP';
    let textTime = '-';
    let status = 'DESCONHECIDO';

    if (hmg && sim && hmg.genTime !== null && sim.genTime !== null) {
      delaySec = Math.round((hmg.genTime - sim.genTime) / 1000);
      if (delaySec === 0) {
        statusTime = 'SINCRONIZADO';
        status = 'SINCRONIZADO';
        textTime = '0m 00s';
      } else if (delaySec > 0) {
        statusTime = 'CACHE_ATRASADO';
        status = 'CACHE_ATRASADO';
        textTime = '-' + formatMinSec(delaySec);
      } else {
        statusTime = 'CACHE_A_FRENTE';
        status = 'CACHE_A_FRENTE';
        textTime = '+' + formatMinSec(-delaySec);
      }
    } else if (hmg && !sim) {
      status = 'CACHE_ATRASADO';
      statusTime = 'CACHE_ATRASADO';
      textTime = 'Pendente no Cache';
    }

    let diffIdg = null;
    let statusIdg = 'SEM_IDG';
    let textIdg = '-';
    if (hmg && sim && hmg.idgNum !== null && sim.idgNum !== null) {
      diffIdg = hmg.idgNum - sim.idgNum;
      if (diffIdg === 0) {
        statusIdg = 'SINCRONIZADO';
        textIdg = '0';
      } else if (diffIdg > 0) {
        statusIdg = 'CACHE_ATRASADO';
        textIdg = '-' + diffIdg;
      } else {
        statusIdg = 'CACHE_A_FRENTE';
        textIdg = '+' + (-diffIdg);
      }
    }

    let delayTotSec = null;
    let statusTot = 'SEM_TOTALIZACAO';
    let textTot = '-';
    if (hmg && sim && hmg.totTime !== null && sim.totTime !== null) {
      delayTotSec = Math.round((hmg.totTime - sim.totTime) / 1000);
      if (delayTotSec === 0) {
        statusTot = 'SINCRONIZADO';
        textTot = '0m 00s';
      } else if (delayTotSec > 0) {
        statusTot = 'ATRASADO';
        textTot = '-' + formatMinSec(delayTotSec);
      } else {
        statusTot = 'A_FRENTE';
        textTot = '+' + formatMinSec(-delayTotSec);
      }
    }

    let diffSt = null;
    let statusSt = 'SEM_SECOES';
    let textSt = '-';
    if (hmg && sim && hmg.st !== null && sim.st !== null) {
      diffSt = hmg.st - sim.st;
      if (diffSt === 0) {
        statusSt = 'SINCRONIZADO';
        textSt = '0';
      } else if (diffSt > 0) {
        statusSt = 'ATRASADO';
        textSt = '-' + diffSt;
      } else {
        statusSt = 'A_FRENTE';
        textSt = '+' + (-diffSt);
      }
    }

    const fileSlaList = todaySyncByFile[relPath] || [];
    const syncSlaSec = fileSlaList.length ? fileSlaList[0] : 0;
    const syncSlaText = fileSlaList.length ? formatMinSec(syncSlaSec) : '-';
    const syncSlaStatus = syncSlaSec > 30 ? 'ALERTA' : (fileSlaList.length ? 'OK' : 'SEM_DADOS');

    if (sim) {
      if (sim.maxAge !== null && !isNaN(sim.maxAge)) {
        simTtlSum += sim.maxAge;
        simTtlCount++;
        if (simTtlMin === null || sim.maxAge < simTtlMin) simTtlMin = sim.maxAge;
        if (simTtlMax === null || sim.maxAge > simTtlMax) simTtlMax = sim.maxAge;
      }
      if (sim.cdnCacheStatus) {
        cdnTotal++;
        if (String(sim.cdnCacheStatus).toLowerCase().includes('hit')) cdnHits++;
      }
    }

    const comp = {
      status,
      delaySec: delaySec ?? 0,
      diffIdg: diffIdg ?? 0,
      syncSlaSec,
      syncSlaText,
      syncSlaStatus,
      statusTime,
      statusIdg,
      textTime,
      textIdg,
      delayTotSec,
      statusTot,
      textTot,
      diffSt,
      statusSt,
      textSt,
      primaryReplicaKey: histReplicaKey,
      originKey: histOriginKey,
      cacheDiff: {
        hmg: {
          serverKey: histOriginKey,
          cacheControl: '(nenhum)',
          maxAge: hmg?.maxAge ?? null,
          cdnStatus: hmg?.cdnCacheStatus || 'ORIGIN',
          etag: hmg?.etag || '-',
          server: 'Apache',
          serverIp: '-',
          lastModified: '-'
        },
        sim: {
          serverKey: histReplicaKey,
          cacheControl: sim?.maxAge ? ('max-age=' + sim.maxAge) : '(nenhum)',
          maxAge: sim?.maxAge ?? null,
          cdnStatus: sim?.cdnCacheStatus || ((sim && sim.maxAge !== null) ? 'Hit (Edge)' : '-'),
          etag: sim?.etag || '-',
          server: 'Edge/Akamai',
          serverIp: '-',
          akamaiGrn: '-',
          lastModified: '-'
        },
        ttlMismatch: (hmg?.maxAge !== sim?.maxAge),
        simTtlText: (sim && sim.maxAge !== null && sim.maxAge !== undefined) ? (sim.maxAge + 's') : '-',
        cdnHit: (sim?.cdnCacheStatus && String(sim.cdnCacheStatus).toLowerCase().includes('hit')) || false
      },
      replicas: [
        {
          chave: histReplicaKey,
          nome: histReplica ? histReplica.nome : 'Simulador Borda (Cache)',
          hg: sim?.hg || '-',
          dg: sim?.dg || '-',
          idg: sim?.idg || '-',
          dt: sim?.dt || '-',
          ht: sim?.ht || '-',
          st: sim?.st ?? null,
          pst: sim?.pst ?? null,
          delaySec: delaySec ?? 0,
          statusTime,
          textTime,
          syncSlaSec,
          syncSlaText,
          syncSlaStatus,
          maxAge: sim?.maxAge ?? null,
          cdnStatus: sim?.cdnCacheStatus || '-'
        }
      ]
    };

    const originUrl = (histOrigin ? histOrigin.baseUrl : (knownServers.get('HMG')?.baseUrl || '')) + relPath;
    const simUrl = (histReplica ? histReplica.baseUrl : (knownServers.get('SIM')?.baseUrl || '')) + relPath;

    comparisonList.push({
      relPath,
      filename: getFilename(relPath),
      meta: parseFileMetadata(relPath),
      originKey: histOriginKey,
      originUrl,
      simUrl,
      hmgUrl: originUrl,
      hmg,
      sim,
      comparison: comp
    });
  }

  const cacheStats = {
    simAvgTtl: simTtlCount > 0 ? Math.round(simTtlSum / simTtlCount) : 60,
    simTtlMin: simTtlMin ?? 0,
    simTtlMax: simTtlMax ?? 60,
    cdnHitRate: cdnTotal > 0 ? Math.round((cdnHits / cdnTotal) * 100) : 100,
    originKey: histOriginKey,
    totalAudited: comparisonList.length
  };

  const regCountRow = db.prepare(`SELECT COUNT(*) as cnt FROM regressoes WHERE timestamp_iso >= ? AND (? IS NULL OR timestamp_iso <= ?)`).get(rStartIso, rEndIso, rEndIso);
  const lastRegRow = db.prepare(`SELECT timestamp_iso FROM regressoes WHERE (criterio = 'TEMPORAL (DG/HG)' OR motivo LIKE 'REGRESSÃO TEMPORAL%') AND timestamp_iso >= ? AND (? IS NULL OR timestamp_iso <= ?) ORDER BY id DESC LIMIT 1`).get(rStartIso, rEndIso, rEndIso);

  return {
    servers: getActiveServers(),
    allServers: Array.from(knownServers.values()),
    originKey: histOriginKey,
    isMultiServer: false,
    comparison: comparisonList,
    recentLogs: [],
    regressionsTimeCount: regCountRow ? regCountRow.cnt : 0,
    lastRegressionTime: lastRegRow ? lastRegRow.timestamp_iso : '-',
    todaySyncByFile,
    todaySlaStats,
    cacheStats,
    activeRodada: rFound,
    totalChecks: rows.length
  };
}

function getComparisonPayload(rodadaId = null) {
  const activeRodada = getActiveRodada();
  if (rodadaId && rodadaId !== 'all' && (!activeRodada || Number(rodadaId) !== activeRodada.id)) {
    const hist = buildHistoricalComparisonPayload(rodadaId);
    if (hist) return hist;
  }
  const originServer = getOriginServer();
  const originKey = originServer ? originServer.chave : 'HMG';
  const activeServersList = getActiveServers();
  const comparisonList = [];

  for (const relPath of Array.from(trackedFiles)) {
    const originState = serverStates[originKey]?.get(relPath) || null;
    const originUrl = (originServer ? originServer.baseUrl : '') + relPath;

    const allServerStates = {};
    for (const srv of activeServersList) {
      allServerStates[srv.chave] = {
        chave: srv.chave,
        nome: srv.nome,
        papel: srv.papel,
        url: srv.baseUrl + relPath,
        meta: serverStates[srv.chave]?.get(relPath) || null
      };
    }

    const comp = getComparison(relPath);

    comparisonList.push({
      relPath,
      filename: getFilename(relPath),
      meta: parseFileMetadata(relPath),
      originKey,
      originUrl,
      origin: originState,
      allServers: allServerStates,
      hmgUrl: (knownServers.get('HMG')?.baseUrl || originUrl) + (knownServers.get('HMG') ? relPath : ''),
      simUrl: (comp.primaryReplicaKey && knownServers.get(comp.primaryReplicaKey)?.baseUrl ? knownServers.get(comp.primaryReplicaKey).baseUrl + relPath : (knownServers.get('SIM')?.baseUrl || originUrl)),
      hmg: originState,
      sim: (comp.primaryReplicaKey ? serverStates[comp.primaryReplicaKey]?.get(relPath) : null) || null,
      comparison: comp
    });
  }

  let regressionsTimeCount = 0;
  let lastRegressionTime = '-';
  const rodadaStartIso = activeRodada ? activeRodada.inicio_iso : new Date(getTodayMidnightUnix()).toISOString();
  try {
    const timeRow = db.prepare(`SELECT COUNT(*) as cnt FROM regressoes WHERE timestamp_iso >= ?`).get(rodadaStartIso);
    regressionsTimeCount = timeRow ? timeRow.cnt : 0;
    const lastRow = db.prepare(`SELECT timestamp_iso FROM regressoes WHERE (criterio = 'TEMPORAL (DG/HG)' OR motivo LIKE 'REGRESSÃO TEMPORAL%') AND timestamp_iso >= ? ORDER BY id DESC LIMIT 1`).get(rodadaStartIso);
    if (lastRow && lastRow.timestamp_iso) {
      const d = new Date(lastRow.timestamp_iso);
      lastRegressionTime = d.toLocaleTimeString('pt-BR');
    }
  } catch (e) {}

  const { todaySyncByFile, globalTodayStats } = getTodaySyncData();

  let simTtlSum = 0, simTtlCount = 0, simTtlMin = null, simTtlMax = null;
  let cdnHits = 0, cdnTotal = 0;

  for (const item of comparisonList) {
    if (item.sim && item.sim.maxAge !== null && !isNaN(item.sim.maxAge)) {
      const ma = item.sim.maxAge;
      simTtlSum += ma;
      simTtlCount++;
      if (simTtlMin === null || ma < simTtlMin) simTtlMin = ma;
      if (simTtlMax === null || ma > simTtlMax) simTtlMax = ma;
    }
    if (item.sim && item.sim.cdnCacheStatus) {
      cdnTotal++;
      if (item.sim.cdnCacheStatus.toLowerCase().includes('hit')) cdnHits++;
    }
  }

  const cacheStats = {
    simAvgTtl: simTtlCount > 0 ? Math.round(simTtlSum / simTtlCount) : 60,
    simTtlMin: simTtlMin ?? 0,
    simTtlMax: simTtlMax ?? 60,
    cdnHitRate: cdnTotal > 0 ? Math.round((cdnHits / cdnTotal) * 100) : 100,
    originKey,
    totalAudited: comparisonList.length
  };

  return {
    servers: activeServersList,
    allServers: Array.from(knownServers.values()),
    originKey,
    isMultiServer: activeServersList.length > 2,
    comparison: comparisonList,
    recentLogs: recentLogs.slice(0, 60),
    regressionsTimeCount,
    lastRegressionTime,
    todaySyncByFile,
    todaySlaStats: globalTodayStats,
    cacheStats,
    activeRodada: activeRodada,
    totalChecks: totalChecksCount
  };
}

function getEnrichedRegressions(filters = {}) {
  const activeRodada = getActiveRodada();
  let targetRodada = activeRodada;
  if (filters.rodadaId && filters.rodadaId !== 'all') {
    const rFound = db.prepare('SELECT * FROM rodadas WHERE id = ?').get(Number(filters.rodadaId));
    if (rFound) targetRodada = rFound;
  }
  const rodadaStartIso = targetRodada ? targetRodada.inicio_iso : new Date(getTodayMidnightUnix()).toISOString();
  const rodadaEndIso = targetRodada && targetRodada.fim_iso ? targetRodada.fim_iso : null;
  
  const limit = filters.limit !== undefined ? filters.limit : 2000;
  const q = (filters.q || '').trim();
  const grnFilter = (filters.grn || '').trim();
  const serverFilter = (filters.servidor || '').trim();
  const criterionFilter = (filters.criterio || '').trim();
  const ufFilter = (filters.uf || '').trim().toLowerCase();
  const cargoFilter = (filters.cargo || '').trim().toLowerCase();
  const eleicaoFilter = (filters.eleicao || '').trim();

  let sql = 'SELECT * FROM regressoes WHERE 1=1 ';
  const params = [];

  if (q) {
    const cleanQ = q.replace(/^#/, '');
    const qNum = parseInt(cleanQ, 10);
    if (!isNaN(qNum) && String(qNum) === cleanQ) {
      sql += 'AND (id = ? OR idg_recebido = ? OR idg_anterior = ? OR arquivo LIKE ? OR motivo LIKE ? OR servidor LIKE ? OR akamai_grn LIKE ? OR headers_json LIKE ? OR request_headers_json LIKE ?) ';
      params.push(qNum, cleanQ, cleanQ, `%${cleanQ}%`, `%${cleanQ}%`, `%${cleanQ}%`, `%${cleanQ}%`, `%${cleanQ}%`, `%${cleanQ}%`);
    } else {
      sql += 'AND (arquivo LIKE ? OR motivo LIKE ? OR servidor LIKE ? OR akamai_grn LIKE ? OR headers_json LIKE ? OR request_headers_json LIKE ?) ';
      params.push(`%${cleanQ}%`, `%${cleanQ}%`, `%${cleanQ}%`, `%${cleanQ}%`, `%${cleanQ}%`, `%${cleanQ}%`);
    }
  } else if (!grnFilter && !filters.allRodada) {
    sql += 'AND timestamp_iso >= ? ';
    params.push(rodadaStartIso);
    if (rodadaEndIso) {
      sql += 'AND timestamp_iso <= ? ';
      params.push(rodadaEndIso);
    }
  }

  if (grnFilter) {
    sql += 'AND (akamai_grn LIKE ? OR headers_json LIKE ? OR request_headers_json LIKE ?) ';
    params.push(`%${grnFilter}%`, `%${grnFilter}%`, `%${grnFilter}%`);
  }

  if (serverFilter) {
    sql += 'AND servidor = ? ';
    params.push(serverFilter);
  }

  if (criterionFilter) {
    if (criterionFilter === 'INVERSAO_DG_DT_ST') {
      sql += "AND (criterio LIKE '%INVERSÃO%' OR motivo LIKE '%INVERSÃO%' OR detalhes LIKE '%INVERSÃO_DG_DT_ST%') ";
    } else {
      sql += 'AND (criterio LIKE ? OR motivo LIKE ?) ';
      params.push(`%${criterionFilter}%`, `%${criterionFilter}%`);
    }
  }

  if (ufFilter) {
    sql += 'AND (arquivo LIKE ? OR arquivo LIKE ? OR arquivo LIKE ?) ';
    params.push(`%/dados/${ufFilter}/%`, `%/config/${ufFilter}/%`, `%-${ufFilter}-%`);
  }

  if (cargoFilter) {
    sql += 'AND (arquivo LIKE ? OR motivo LIKE ?) ';
    params.push(`%${cargoFilter}%`, `%${cargoFilter}%`);
  }

  if (eleicaoFilter) {
    sql += 'AND arquivo LIKE ? ';
    params.push(`%/${eleicaoFilter}/%`);
  }

  sql += 'ORDER BY id DESC ';
  if (limit > 0) {
    sql += 'LIMIT ? ';
    params.push(limit);
  }

  const rows = db.prepare(sql).all(...params);
  const stmtTimelineRange = db.prepare(`
    SELECT 
      id, timestamp_iso, timestamp_unix, servidor, papel_servidor, arquivo,
      idg, dg, hg, dt, ht, secoes, etag, status_ordem, server_ip, cache_control, cdn_status, akamai_grn, headers_json, request_headers_json, evidencia_raw_path,
      call_time_iso, call_time_unix, latency_ms
    FROM leituras
    WHERE arquivo = ? AND timestamp_unix >= ? AND timestamp_unix <= ?
    ORDER BY timestamp_unix ASC
  `);
  const stmtTimelineFallback = db.prepare(`
    SELECT 
      id, timestamp_iso, timestamp_unix, servidor, papel_servidor, arquivo,
      idg, dg, hg, dt, ht, secoes, etag, status_ordem, server_ip, cache_control, cdn_status, akamai_grn, headers_json, request_headers_json, evidencia_raw_path,
      call_time_iso, call_time_unix, latency_ms
    FROM leituras
    WHERE arquivo = ? AND timestamp_unix >= ? AND timestamp_unix <= ?
    ORDER BY timestamp_unix DESC
    LIMIT 8
  `);

  const items = rows.map(r => {
    let rawMeta = null;
    let rawHeaders = {};
    if (r.headers_json) {
      try {
        const parsed = JSON.parse(r.headers_json);
        rawHeaders = parsed.response ? parsed.response : parsed;
      } catch(e) {}
    }
    if (Object.keys(rawHeaders).length === 0 && r.evidencia_raw_path && rows.length <= 100) {
      rawMeta = getRawMetadataFast(r.evidencia_raw_path);
      rawHeaders = (rawMeta && (rawMeta.response_headers || rawMeta.headers)) || {};
    }

    let rawReqHeaders = {};
    if (r.request_headers_json) {
      try { rawReqHeaders = JSON.parse(r.request_headers_json); } catch(e) {}
    }
    if (Object.keys(rawReqHeaders).length === 0 && rawMeta) {
      rawReqHeaders = rawMeta.request_headers || {};
    }
    if (Object.keys(rawReqHeaders).length === 0) {
      rawReqHeaders = {
        'cache-control': 'no-cache, no-store, must-revalidate',
        'pragma': 'no-cache',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) TSE-Audit/2.0',
        'accept': 'application/json, text/plain, */*'
      };
    }
    const cacheSummary = (rawMeta && rawMeta.cache_control_headers) || null;
    const itemAkamaiGrn = r.akamai_grn || rawHeaders['akamai-grn'] || rawHeaders['x-akamai-grn'] || null;

    const regUnix = new Date(r.timestamp_iso).getTime();
    const rRodada = findRodadaForTimestamp(regUnix);
    const rodadaMinUnix = rRodada ? rRodada.inicio_unix : 0;
    const rodadaMaxUnix = (rRodada && rRodada.fim_unix) ? rRodada.fim_unix : (regUnix + 120000);

    const rangeStart = Math.max(regUnix - 900000, rodadaMinUnix);
    const rangeEnd = Math.min(regUnix + 120000, rodadaMaxUnix);

    let timeline = stmtTimelineRange.all(r.arquivo, rangeStart, rangeEnd);

    if (timeline.length < 3) {
      timeline = stmtTimelineFallback.all(r.arquivo, rodadaMinUnix, rangeEnd).reverse();
    }

    const enrichedTimeline = timeline.map(t => {
      let tHeaders = {};
      let tReqHeaders = {};
      if (!filters.light) {
        if (t.headers_json) {
          try {
            const parsed = JSON.parse(t.headers_json);
            tHeaders = parsed.response ? parsed.response : parsed;
            if (parsed.request) tReqHeaders = parsed.request;
          } catch(e) {}
        }
        if (t.request_headers_json) {
          try { tReqHeaders = JSON.parse(t.request_headers_json); } catch(e) {}
        }
      }
      const tServerIp = t.server_ip || tHeaders['x-server-ip'] || (t.servidor === 'HMG' ? '192.168.218.33' : '-');
      const tCacheControl = t.cache_control || tHeaders['cache-control'] || '-';
      const tCdnStatus = t.cdn_status || tHeaders['cdn-cache-status'] || tHeaders['x-cache'] || (t.servidor === 'HMG' ? 'ORIGIN' : '-');
      const tAkamaiGrn = t.akamai_grn || tHeaders['akamai-grn'] || tHeaders['x-akamai-grn'] || (t.id === r.id ? itemAkamaiGrn : null) || '-';
      const tEtag = t.etag || tHeaders['etag'] || '-';
      const tExpires = tHeaders['expires'] || '-';
      const tAge = tHeaders['age'] !== undefined ? tHeaders['age'] + 's' : '-';
      const isRegressionPoint = Boolean(t.status_ordem === 'REGRESSAO_DETECTADA' || t.id === r.id || (Math.abs(t.timestamp_unix - regUnix) < 2000 && t.servidor === r.servidor));

      return {
        id: t.id,
        timestamp_iso: t.timestamp_iso,
        timestamp_unix: t.timestamp_unix,
        call_time_iso: t.call_time_iso || t.timestamp_iso,
        call_time_unix: t.call_time_unix || t.timestamp_unix,
        latency_ms: t.latency_ms !== undefined ? t.latency_ms : null,
        servidor: t.servidor,
        papel_servidor: t.papel_servidor,
        dg: t.dg,
        hg: t.hg,
        dt: t.dt,
        ht: t.ht,
        idg: t.idg,
        secoes: t.secoes,
        status_ordem: t.status_ordem,
        server_ip: tServerIp,
        cache_control: tCacheControl,
        cdn_status: tCdnStatus,
        akamai_grn: tAkamaiGrn,
        etag: tEtag,
        expires: tExpires,
        age: tAge,
        isRegressionPoint
      };
    });

    if (filters.light) {
      return {
        id: r.id,
        timestamp_iso: r.timestamp_iso,
        call_time_iso: r.call_time_iso || r.timestamp_iso,
        servidor: r.servidor,
        papel_servidor: r.papel_servidor,
        arquivo: r.arquivo,
        criterio: r.criterio,
        motivo: r.motivo,
        detalhes: r.detalhes,
        dg_anterior: r.dg_anterior,
        dg_recebido: r.dg_recebido,
        hg_anterior: r.hg_anterior,
        hg_recebido: r.hg_recebido,
        dt_anterior: r.dt_anterior,
        dt_recebido: r.dt_recebido,
        ht_anterior: r.ht_anterior,
        ht_recebido: r.ht_recebido,
        idg_anterior: r.idg_anterior,
        idg_recebido: r.idg_recebido,
        secoes_anterior: r.secoes_anterior,
        secoes_recebido: r.secoes_recebido,
        votos_anterior: r.votos_anterior,
        votos_recebido: r.votos_recebido,
        akamai_grn: itemAkamaiGrn,
        fileMeta: parseFileMetadata(r.arquivo),
        timeline: enrichedTimeline
      };
    }

    return {
      ...r,
      akamai_grn: itemAkamaiGrn,
      fileMeta: parseFileMetadata(r.arquivo),
      rawMeta,
      request_headers: rawReqHeaders,
      response_headers: rawHeaders,
      cache_control_headers: cacheSummary,
      timeline: enrichedTimeline
    };
  });

  let countRow;
  if (filters.allRodada) {
    countRow = db.prepare('SELECT COUNT(*) as cnt FROM regressoes').get();
  } else if (rodadaEndIso) {
    countRow = db.prepare('SELECT COUNT(*) as cnt FROM regressoes WHERE timestamp_iso >= ? AND timestamp_iso <= ?').get(rodadaStartIso, rodadaEndIso);
  } else {
    countRow = db.prepare('SELECT COUNT(*) as cnt FROM regressoes WHERE timestamp_iso >= ?').get(rodadaStartIso);
  }

  return {
    rodada: targetRodada || activeRodada,
    total: countRow ? countRow.cnt : items.length,
    regressoes: items
  };
}

function startDashboardServer() {
  const server = http.createServer((req, res) => {
    const url = new URL(req.url, `http://localhost:${DASHBOARD_PORT}`);

    if (url.pathname === '/' || url.pathname === '/index.html' || url.pathname === '/dashboard.html') {
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
      res.end(DASHBOARD_HTML);
      return;
    }

    if (url.pathname === '/report') {
      const htmlContent = generateHtmlReport();
      res.writeHead(200, {
        'Content-Type': 'text/html; charset=utf-8',
        'Cache-Control': 'no-cache, no-store, must-revalidate'
      });
      res.end(htmlContent);
      return;
    }

    if (url.pathname === '/export/dossie-html') {
      try {
        const limitParam = url.searchParams.get('limit');
        const limit = limitParam !== null ? parseInt(limitParam, 10) : 2000;
        const q = (url.searchParams.get('q') || '').trim();
        const grn = (url.searchParams.get('grn') || '').trim();
        const servidor = (url.searchParams.get('servidor') || '').trim();
        const criterio = (url.searchParams.get('criterio') || '').trim();
        const uf = (url.searchParams.get('uf') || '').trim();
        const cargo = (url.searchParams.get('cargo') || '').trim();
        const eleicao = (url.searchParams.get('eleicao') || '').trim();

        const compPayload = getComparisonPayload();
        const regsPayload = getEnrichedRegressions({
          limit,
          allRodada: true,
          q,
          grn,
          servidor,
          criterio,
          uf,
          cargo,
          eleicao
        });

        // Ensure all inversion cases are always bundled into standalone export
        if (criterio !== 'INVERSAO_DG_DT_ST' && !q && !grn) {
          const invPayload = getEnrichedRegressions({
            limit: 2000,
            allRodada: true,
            criterio: 'INVERSAO_DG_DT_ST'
          });
          const existingIds = new Set(regsPayload.regressoes.map(r => r.id));
          for (const inv of invPayload.regressoes) {
            if (!existingIds.has(inv.id)) {
              regsPayload.regressoes.push(inv);
            }
          }
        }
        const rodadasRows = db.prepare('SELECT * FROM rodadas ORDER BY id DESC').all();
        const activeRodada = getActiveRodada();
        
        const standaloneBundle = {
          exportedAt: new Date().toLocaleString('pt-BR'),
          comparison: compPayload.comparison,
          regressoes: regsPayload.regressoes,
          rodadas: rodadasRows,
          activeRodada: activeRodada,
          cacheStats: compPayload.cacheStats,
          todaySlaStats: compPayload.todaySlaStats,
          lastRegressionTime: compPayload.lastRegressionTime,
          regressionsTimeCount: regsPayload.total
        };

        const htmlContent = generateHtmlReport(standaloneBundle);
        const dateStr = new Date().toISOString().slice(0, 10);
        res.writeHead(200, {
          'Content-Type': 'text/html; charset=utf-8',
          'Content-Disposition': `attachment; filename="dossie_forense_tdtot_${dateStr}.html"`,
          'Cache-Control': 'no-cache, no-store, must-revalidate'
        });
        res.end(htmlContent);
      } catch (err) {
        res.writeHead(500, { 'Content-Type': 'text/plain; charset=utf-8' });
        res.end('Erro ao exportar dossiê HTML: ' + err.message);
      }
      return;
    }

    if (url.pathname === '/download/db') {
      if (fs.existsSync(DB_FILE)) {
        res.writeHead(200, {
          'Content-Type': 'application/x-sqlite3',
          'Content-Disposition': 'attachment; filename="tdtot_auditoria.db"'
        });
        res.end(fs.readFileSync(DB_FILE));
      } else {
        res.writeHead(404);
        res.end('Arquivo DB não encontrado');
      }
      return;
    }

    if (url.pathname === '/download/csv-regressoes') {
      if (fs.existsSync(REGRESSIONS_CSV)) {
        res.writeHead(200, {
          'Content-Type': 'text/csv; charset=utf-8',
          'Content-Disposition': 'attachment; filename="regressoes_detectadas.csv"'
        });
        res.end(fs.readFileSync(REGRESSIONS_CSV, 'utf8'));
      } else {
        res.writeHead(404);
        res.end('CSV não encontrado');
      }
      return;
    }

    
    // --- ROTAS DA EXPORTAÇÃO ZIP ---
    
    
    // --- ROTAS DE ELEIÇÕES MONITORADAS ---
    if (url.pathname === '/api/eleicoes' && req.method === 'GET') {
      const list = [];
      for (const [cd, el] of knownElections.entries()) {
        list.push({
          cd: el.cd,
          nome: el.nm,
          tipo: el.tp,
          pleito: el.pleito,
          ciclo: el.ciclo || 'ele2026',
          ufsCount: el.ufs.length,
          ativo: el.ativo
        });
      }
      res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
      res.end(JSON.stringify({ eleicoes: list, totalTracked: trackedFiles.size }));
      return;
    }

    if (url.pathname === '/api/eleicoes/toggle' && req.method === 'POST') {
      let bodyStr = '';
      req.on('data', chunk => bodyStr += chunk);
      req.on('end', () => {
        let params = {};
        try { params = JSON.parse(bodyStr); } catch {}
        const cd = String(params.cd);
        const el = knownElections.get(cd);
        if (el) {
          el.ativo = params.ativo !== undefined ? Boolean(params.ativo) : !el.ativo;
          db.prepare('INSERT OR REPLACE INTO eleicoes_monitoradas (cd, nome, tipo, pleito, ativo, ciclo) VALUES (?, ?, ?, ?, ?, ?)')
            .run(cd, el.nm, el.tp, el.pleito, el.ativo ? 1 : 0, el.ciclo || 'ele2026');
          updateTrackedCatalog();
        }
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: true, cd, ativo: el ? el.ativo : false, totalTracked: trackedFiles.size }));
      });
      return;
    }

    if (url.pathname === '/api/eleicoes/selecionar-todas' && req.method === 'POST') {
      let bodyStr = '';
      req.on('data', chunk => bodyStr += chunk);
      req.on('end', () => {
        let params = {};
        try { params = JSON.parse(bodyStr); } catch {}
        const ativar = params.ativar !== false;
        for (const [cd, el] of knownElections.entries()) {
          el.ativo = ativar;
          db.prepare('INSERT OR REPLACE INTO eleicoes_monitoradas (cd, nome, tipo, pleito, ativo, ciclo) VALUES (?, ?, ?, ?, ?, ?)')
            .run(cd, el.nm, el.tp, el.pleito, el.ativo ? 1 : 0, el.ciclo || 'ele2026');
        }
        updateTrackedCatalog();
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: true, totalTracked: trackedFiles.size }));
      });
      return;
    }

    // --- ROTAS DE GERENCIAMENTO DE RODADAS ---
    if (url.pathname === '/api/rodadas' && req.method === 'GET') {
      const rodadas = db.prepare('SELECT * FROM rodadas ORDER BY id DESC').all();
      res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
      res.end(JSON.stringify({ active: getActiveRodada(), list: rodadas }));
      return;
    }

    if (url.pathname === '/api/rodadas/nova' && req.method === 'POST') {
      let bodyStr = '';
      req.on('data', chunk => bodyStr += chunk);
      req.on('end', () => {
        let params = {};
        try { params = JSON.parse(bodyStr); } catch {}
        const r = createNewRodada(params.nome, params.inicioUnix);
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: true, rodada: r }));
      });
      return;
    }

    if (url.pathname === '/api/rodadas/editar' && req.method === 'POST') {
      let bodyStr = '';
      req.on('data', chunk => bodyStr += chunk);
      req.on('end', () => {
        let params = {};
        try { params = JSON.parse(bodyStr); } catch {}
        updateRodada(params.id, params.nome, params.inicioUnix);
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: true }));
      });
      return;
    }

    if (url.pathname === '/api/rodadas/ativar' && req.method === 'POST') {
      let bodyStr = '';
      req.on('data', chunk => bodyStr += chunk);
      req.on('end', () => {
        let params = {};
        try { params = JSON.parse(bodyStr); } catch {}
        const id = Number(params.id);
        db.prepare('UPDATE rodadas SET ativo = 0').run();
        db.prepare('UPDATE rodadas SET ativo = 1 WHERE id = ?').run(id);
        currentActiveRodada = db.prepare('SELECT * FROM rodadas WHERE id = ?').get(id);
        cachedTodaySync = null;
        clearServerStates();
        fileSyncTracker.clear();
        hydrateStateFromDb();
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: true, active: currentActiveRodada }));
      });
      return;
    }

    if (url.pathname === '/api/rodadas/excluir' && req.method === 'POST') {
      let bodyStr = '';
      req.on('data', chunk => bodyStr += chunk);
      req.on('end', () => {
        let params = {};
        try { params = JSON.parse(bodyStr); } catch {}
        deleteRodada(Number(params.id));
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: true }));
      });
      return;
    }

    // --- ROTAS DE GERENCIAMENTO DE SERVIDORES (MULTI-NÓS) ---
    if (url.pathname === '/api/servidores' && req.method === 'GET') {
      res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
      res.end(JSON.stringify({
        servers: Array.from(knownServers.values()),
        activeServers: getActiveServers(),
        originKey: getOriginServer()?.chave || null
      }));
      return;
    }

    if (url.pathname === '/api/servidores/salvar' && req.method === 'POST') {
      let bodyStr = '';
      req.on('data', chunk => bodyStr += chunk);
      req.on('end', () => {
        let params = {};
        try { params = JSON.parse(bodyStr); } catch {}
        const chave = String(params.chave || '').trim().toUpperCase().replace(/[^A-Z0-9_]/g, '');
        const nome = String(params.nome || '').trim();
        let baseUrl = String(params.baseUrl || '').trim();
        const papel = params.papel === 'ORIGEM' ? 'ORIGEM' : 'REPLICA';
        const ativo = params.ativo !== false ? 1 : 0;

        if (!chave || !nome || !baseUrl) {
          res.writeHead(400, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ ok: false, error: 'Chave, nome e URL base são obrigatórios.' }));
          return;
        }

        if (!baseUrl.endsWith('/')) baseUrl += '/';

        if (papel === 'ORIGEM') {
          db.prepare("UPDATE servidores_monitorados SET papel = 'REPLICA'").run();
        }

        const existing = db.prepare('SELECT id FROM servidores_monitorados WHERE chave = ?').get(chave);
        if (existing) {
          db.prepare(`
            UPDATE servidores_monitorados 
            SET nome = ?, papel = ?, base_url = ?, ativo = ? 
            WHERE chave = ?
          `).run(nome, papel, baseUrl, ativo, chave);
        } else {
          const count = db.prepare('SELECT COUNT(*) as cnt FROM servidores_monitorados').get().cnt;
          const nowIso = new Date().toISOString();
          db.prepare(`
            INSERT INTO servidores_monitorados (chave, nome, papel, base_url, ativo, ordem, criado_em)
            VALUES (?, ?, ?, ?, ?, ?, ?)
          `).run(chave, nome, papel, baseUrl, ativo, count + 1, nowIso);
        }

        initServidores();
        fileSyncTracker.clear();
        cachedTodaySync = null;

        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: true, chave }));
      });
      return;
    }

    if (url.pathname === '/api/servidores/toggle' && req.method === 'POST') {
      let bodyStr = '';
      req.on('data', chunk => bodyStr += chunk);
      req.on('end', () => {
        let params = {};
        try { params = JSON.parse(bodyStr); } catch {}
        const chave = String(params.chave || '').trim().toUpperCase();
        const srv = knownServers.get(chave);
        if (srv) {
          const novoAtivo = params.ativo !== undefined ? (params.ativo ? 1 : 0) : (srv.ativo ? 0 : 1);
          db.prepare('UPDATE servidores_monitorados SET ativo = ? WHERE chave = ?').run(novoAtivo, chave);
          initServidores();
        }
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: true }));
      });
      return;
    }

    if (url.pathname === '/api/servidores/definir-origem' && req.method === 'POST') {
      let bodyStr = '';
      req.on('data', chunk => bodyStr += chunk);
      req.on('end', () => {
        let params = {};
        try { params = JSON.parse(bodyStr); } catch {}
        const chave = String(params.chave || '').trim().toUpperCase();
        if (knownServers.has(chave)) {
          db.prepare("UPDATE servidores_monitorados SET papel = 'REPLICA'").run();
          db.prepare("UPDATE servidores_monitorados SET papel = 'ORIGEM', ativo = 1 WHERE chave = ?").run(chave);
          fileSyncTracker.clear();
          cachedTodaySync = null;
          initServidores();
        }
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: true, originKey: chave }));
      });
      return;
    }

    if (url.pathname === '/api/servidores/excluir' && req.method === 'POST') {
      let bodyStr = '';
      req.on('data', chunk => bodyStr += chunk);
      req.on('end', () => {
        let params = {};
        try { params = JSON.parse(bodyStr); } catch {}
        const chave = String(params.chave || '').trim().toUpperCase();
        const srv = knownServers.get(chave);
        if (!srv) {
          res.writeHead(404, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ ok: false, error: 'Servidor não encontrado.' }));
          return;
        }
        if (srv.papel === 'ORIGEM') {
          res.writeHead(400, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ ok: false, error: 'Não é possível excluir o servidor definido como ORIGEM. Defina outro nó como Origem primeiro.' }));
          return;
        }
        if (knownServers.size <= 1) {
          res.writeHead(400, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ ok: false, error: 'É necessário manter pelo menos um servidor cadastrado.' }));
          return;
        }
        db.prepare('DELETE FROM servidores_monitorados WHERE chave = ?').run(chave);
        initServidores();
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: true }));
      });
      return;
    }

    if (url.pathname === '/api/servidores/testar' && req.method === 'POST') {
      let bodyStr = '';
      req.on('data', chunk => bodyStr += chunk);
      req.on('end', async () => {
        let params = {};
        try { params = JSON.parse(bodyStr); } catch {}
        let targetUrl = String(params.baseUrl || '').trim();
        if (!targetUrl) {
          res.writeHead(400, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ ok: false, error: 'URL é obrigatória' }));
          return;
        }
        if (!targetUrl.endsWith('/')) targetUrl += '/';
        const testFileUrl = targetUrl + 'comum/config/ele-c.json?t=' + Date.now();
        const startTime = Date.now();
        try {
          const resp = await httpRequestWithIp(testFileUrl);
          const timeMs = Date.now() - startTime;
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({
            ok: resp.ok,
            statusCode: resp.statusCode,
            serverIp: resp.serverIp || null,
            server: resp.headers?.['server'] || null,
            cacheControl: resp.headers?.['cache-control'] || null,
            timeMs,
            error: resp.error || null
          }));
        } catch (err) {
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({
            ok: false,
            statusCode: 0,
            serverIp: null,
            timeMs: Date.now() - startTime,
            error: err.message
          }));
        }
      });
      return;
    }

    if (url.pathname === '/api/zip/options') {
      const minMtime = url.searchParams.get('apenasRodada') === 'true' && getActiveRodada() ? getActiveRodada().inicio_unix : null;
      const info = scanVersoesFast(VERSOES_DIR, minMtime);
      res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
      res.end(JSON.stringify(info));
      return;
    }

    if (url.pathname === '/api/zip/start' && req.method === 'POST') {
      let bodyStr = '';
      req.on('data', chunk => { bodyStr += chunk; });
      req.on('end', async () => {
        let params = {};
        try { params = JSON.parse(bodyStr); } catch {}
        const eleicao = params.eleicao || 'ALL';
        const jobId = 'job_' + Date.now() + '_' + Math.random().toString(36).substring(2, 7);

        const tempZipFile = path.join(TEMP_ZIPS_DIR, `versoes_${eleicao}_${jobId}.zip`);
        const job = {
          jobId,
          eleicao,
          tempZipFile,
          status: 'SCANNING',
          done: 0,
          total: 0,
          percent: 0,
          currentFile: '',
          fileSizeMb: 0,
          createdAt: Date.now(),
          error: null
        };
        zipJobs.set(jobId, job);

        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ jobId }));

        // Inicia processo em background assíncrono
        (async () => {
          try {
            const minMtime = params.apenasRodada && getActiveRodada() ? getActiveRodada().inicio_unix : null;
            const files = collectFilesForZip(VERSOES_DIR, eleicao, minMtime);
            job.total = files.length;
            job.status = 'PROCESSING';

            if (files.length === 0) {
              job.status = 'READY';
              job.percent = 100;
              return;
            }

            await buildZipStream(files, VERSOES_DIR, tempZipFile, (done, total, curRel) => {
              job.done = done;
              job.percent = Math.round((done / total) * 100);
              job.currentFile = path.basename(curRel);
            });

            if (fs.existsSync(tempZipFile)) {
              job.fileSizeMb = (fs.statSync(tempZipFile).size / (1024 * 1024)).toFixed(2);
            }
            job.status = 'READY';
            job.percent = 100;
          } catch (err) {
            console.error('Erro na geração de ZIP:', err);
            job.status = 'ERROR';
            job.error = err.message;
          }
        })();
      });
      return;
    }

    if (url.pathname === '/api/zip/status') {
      const jobId = url.searchParams.get('jobId');
      const job = zipJobs.get(jobId);
      if (!job) {
        res.writeHead(404, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'Job não encontrado' }));
        return;
      }
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({
        status: job.status,
        done: job.done,
        total: job.total,
        percent: job.percent,
        currentFile: job.currentFile,
        fileSizeMb: job.fileSizeMb,
        error: job.error
      }));
      return;
    }

    if (url.pathname === '/api/zip/download') {
      const jobId = url.searchParams.get('jobId');
      const job = zipJobs.get(jobId);
      if (!job || !fs.existsSync(job.tempZipFile)) {
        res.writeHead(404);
        res.end('Arquivo ZIP não disponível ou expirado');
        return;
      }

      const dateStr = new Date().toISOString().slice(0, 10);
      const downloadName = `versoes_tdtot_${job.eleicao}_${dateStr}.zip`;
      const stat = fs.statSync(job.tempZipFile);

      res.writeHead(200, {
        'Content-Type': 'application/zip',
        'Content-Disposition': `attachment; filename="${downloadName}"`,
        'Content-Length': stat.size
      });

      const readStream = fs.createReadStream(job.tempZipFile);
      readStream.pipe(res);
      return;
    }

    if (url.pathname === '/download/csv-comparativo') {
      if (fs.existsSync(LOG_CSV)) {
        res.writeHead(200, {
          'Content-Type': 'text/csv; charset=utf-8',
          'Content-Disposition': 'attachment; filename="historico_comparativo.csv"'
        });
        res.end(fs.readFileSync(LOG_CSV, 'utf8'));
      } else {
        res.writeHead(404);
        res.end('CSV não encontrado');
      }
      return;
    }


    if (url.pathname === '/api/comparison') {
      const rodadaId = url.searchParams.get('rodadaId');
      const payload = getComparisonPayload(rodadaId);
      res.writeHead(200, {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*'
      });
      res.end(JSON.stringify(payload));
      return;
    }

    // --- ROTA DE AUDITORIA FORENSE DE REGRESSÕES ---
    if (url.pathname === '/api/regressoes' && req.method === 'GET') {
      try {
        const limitParam = url.searchParams.get('limit');
        const limit = limitParam !== null ? parseInt(limitParam, 10) : 2000;
        const q = (url.searchParams.get('q') || '').trim();
        const grn = (url.searchParams.get('grn') || '').trim();
        const servidor = (url.searchParams.get('servidor') || '').trim();
        const criterio = (url.searchParams.get('criterio') || '').trim();
        const uf = (url.searchParams.get('uf') || '').trim();
        const cargo = (url.searchParams.get('cargo') || '').trim();
        const eleicao = (url.searchParams.get('eleicao') || '').trim();

        const rodadaIdParam = url.searchParams.get('rodadaId');
        const rodadaId = (rodadaIdParam && rodadaIdParam !== 'all') ? rodadaIdParam : null;
        const allRodada = rodadaIdParam === 'all' || url.searchParams.get('all') === '1' || url.searchParams.get('allRodada') === '1';
        const light = url.searchParams.get('light') === '1' || limit > 100;
        const payload = getEnrichedRegressions({
          limit,
          allRodada,
          rodadaId,
          q,
          grn,
          servidor,
          criterio,
          uf,
          cargo,
          eleicao,
          light
        });

        res.writeHead(200, {
          'Content-Type': 'application/json; charset=utf-8',
          'Access-Control-Allow-Origin': '*'
        });
        res.end(JSON.stringify(payload));
      } catch (err) {
        res.writeHead(500, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: err.message }));
      }
      return;
    }

    // --- ROTA DE DOWNLOAD / INSPEÇÃO DE EVIDÊNCIA RAW ---
    if (url.pathname === '/api/evidencia' && req.method === 'GET') {
      const regId = url.searchParams.get('id');
      const isDownload = url.searchParams.get('download') === '1';
      if (!regId) {
        res.writeHead(400); res.end('ID de regressão não fornecido'); return;
      }
      try {
        const row = db.prepare('SELECT * FROM regressoes WHERE id = ?').get(regId);
        if (!row) {
          res.writeHead(404); res.end('Registro de regressão não encontrado'); return;
        }

        if (row.evidencia_raw_path && fs.existsSync(row.evidencia_raw_path)) {
          const filename = path.basename(row.evidencia_raw_path);
          const stat = fs.statSync(row.evidencia_raw_path);
          res.writeHead(200, {
            'Content-Type': 'application/json; charset=utf-8',
            'Content-Disposition': isDownload ? 'attachment; filename="' + filename + '"' : 'inline; filename="' + filename + '"',
            'Content-Length': stat.size,
            'Access-Control-Allow-Origin': '*'
          });
          fs.createReadStream(row.evidencia_raw_path).pipe(res);
        } else {
          // Arquivo raw ausente (ex: expurgo ou buffer inicial) -> Monta JSON detalhado com dados do banco
          const fallbackData = {
            aviso: 'Arquivo físico individual em disco não encontrado (pode ter sido expurgado ou gerado antes da ativação do repositório físico). Os metadados completos registrados no banco SQLite seguem abaixo.',
            registro_auditoria: {
              ...row,
              fileMeta: parseFileMetadata(row.arquivo)
            }
          };
          const jsonStr = JSON.stringify(fallbackData, null, 2);
          res.writeHead(200, {
            'Content-Type': 'application/json; charset=utf-8',
            'Content-Disposition': isDownload ? 'attachment; filename="regressao_' + regId + '_banco.json"' : 'inline; filename="regressao_' + regId + '_banco.json"',
            'Access-Control-Allow-Origin': '*'
          });
          res.end(jsonStr);
        }
      } catch (err) {
        res.writeHead(500, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: err.message }));
      }
      return;
    }

    if (url.pathname === '/api/events') {
      res.writeHead(200, {
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
        'Access-Control-Allow-Origin': '*'
      });
      res.write('retry: 5000\n\n');
      sseClients.add(res);
      req.on('close', () => sseClients.delete(res));
      return;
    }

    res.writeHead(404);
    res.end('Not found');
  });

  server.listen(DASHBOARD_PORT, () => {
    console.log(`${GREEN}🌐 Dashboard de Auditoria disponível em:${RESET} ${BOLD}http://127.0.0.1:${DASHBOARD_PORT}${RESET}`);
  });
}

async function start() {
  initLogs();
  initServidores();
  await discoverAvailableElections();
  updateTrackedCatalog();
  initRodadas();
  hydrateStateFromDb();
  startDashboardServer();

  console.log(`${BOLD}${CYAN}======================================================================${RESET}`);
  console.log(`${BOLD}  AUDITOR DUPLO TDTOT TSE - POR DATA/HORA (DG/HG) E POR IDG${RESET}`);
  console.log(`${BOLD}${CYAN}======================================================================${RESET}`);
  const originSrv = getOriginServer();
  const replicaSrv = getReplicaServers()[0];
  console.log(`FONTE (Origem):       ${originSrv ? originSrv.baseUrl + ' (' + originSrv.chave + ')' : SERVERS.HMG.baseUrl}`);
  console.log(`CACHE (Distrib.):     ${replicaSrv ? replicaSrv.baseUrl + ' (' + replicaSrv.chave + ')' : SERVERS.SIM.baseUrl}`);
  console.log(`Arquivos no Catálogo: ${trackedFiles.size}`);
  console.log(`Banco SQLite:         ${DB_FILE}`);
  console.log(`Pasta Evidências:     ${EVIDENCIAS_DIR}`);
  console.log(`Dossiê HTML:          ${REPORT_HTML}`);
  console.log(`Dashboard Web:        http://127.0.0.1:${DASHBOARD_PORT}`);

  await syncTabs();

  console.log(`\n${GREEN}Iniciando varredura contínua em pool concorrente com DUPLO CRITÉRIO...${RESET}\n`);

  // Varredura inicial de todos os arquivos
  const allList = Array.from(trackedFiles);
  await runWorkerPool(allList, 15);

  // Loop de varredura cíclica contínua protegido contra sobreposição de ciclos
  let isPollingCycleRunning = false;
  setInterval(async () => {
    if (isPollingCycleRunning) return;
    isPollingCycleRunning = true;
    try {
      const list = Array.from(trackedFiles);
      await runWorkerPool(list, 15);
    } catch (err) {
      console.error('[POLL] Erro no ciclo de polling:', err);
    } finally {
      isPollingCycleRunning = false;
    }
  }, 6000);

  setInterval(syncTabs, 10000);
  setInterval(checkDayRollover, 30000);
  setInterval(async () => {
    await discoverAvailableElections();
    updateTrackedCatalog();
  }, 60000);
}

start().catch(err => {
  console.error(`${RED}Erro fatal no monitor:${RESET}`, err);
  process.exit(1);
});
