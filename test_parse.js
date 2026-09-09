function parseDgHg(dg, hg) {
  if (!dg || !hg) return null;
  const [d, m, y] = dg.trim().split('/').map(Number);
  const [hh, mm, ss] = hg.trim().split(':').map(Number);
  return new Date(y, m - 1, d, hh, mm, ss).getTime();
}

console.log('Parsed timestamp:', new Date(parseDgHg('08/09/2026', '14:21:15')).toISOString());
