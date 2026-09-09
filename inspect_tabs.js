const CDP_PORT = 9222;

async function run() {
  const tabs = await fetch(`http://127.0.0.1:${CDP_PORT}/json/list`).then(r => r.json());
  const pageTabs = tabs.filter(t => t.type === 'page' && t.url.includes('simulado'));
  console.log(`Found ${pageTabs.length} simulado tabs:`);
  for (const t of pageTabs) {
    console.log(`- [${t.id}] ${t.title || '(no title)'}\n  URL: ${t.url}`);
  }
}

run().catch(console.error);
