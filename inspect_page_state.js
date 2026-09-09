const CDP_PORT = 9222;

async function checkTab(tabId) {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(`ws://127.0.0.1:${CDP_PORT}/devtools/page/${tabId}`);
    ws.onopen = () => {
      ws.send(JSON.stringify({
        id: 1,
        method: 'Runtime.evaluate',
        params: {
          expression: `JSON.stringify({
            title: document.title,
            hash: window.location.hash,
            subtitles: Array.from(document.querySelectorAll('h1, h2, h3, h4, .badge, .status, strong')).map(e => e.innerText.trim()).filter(Boolean).slice(0, 10),
            updateInfo: Array.from(document.querySelectorAll('*')).filter(e => e.children.length === 0 && (e.innerText.includes('Atualizado') || e.innerText.includes('Totaliz') || e.innerText.includes('Última'))).map(e => e.innerText.trim()).slice(0, 5)
          })`,
          returnByValue: true
        }
      }));
    };
    ws.onmessage = (msg) => {
      const d = JSON.parse(msg.data);
      if (d.id === 1) {
        ws.close();
        if (d.result && d.result.result && d.result.result.value) {
          try {
            resolve(JSON.parse(d.result.result.value));
          } catch (e) {
            resolve({ raw: d.result.result.value });
          }
        } else {
          resolve({ error: d.result });
        }
      }
    };
    ws.onerror = reject;
  });
}

async function main() {
  const tabs = await fetch(`http://127.0.0.1:${CDP_PORT}/json/list`).then(r => r.json());
  const simuladoTabs = tabs.filter(t => t.type === 'page' && t.url.includes('simulado'));
  for (const t of simuladoTabs) {
    console.log(`\n--- Tab: ${t.id} ---`);
    console.log(`URL: ${t.url}`);
    const state = await checkTab(t.id);
    console.log(`Title: ${state.title}`);
    console.log(`Hash: ${state.hash}`);
    console.log(`Headings/Badges:`, state.subtitles);
    console.log(`Update info found in DOM:`, state.updateInfo);
  }
}

main().catch(console.error);
