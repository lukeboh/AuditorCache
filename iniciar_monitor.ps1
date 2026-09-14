# Inicia o monitor de atualizacao TDTot em uma janela dedicada
$host.UI.RawUI.WindowTitle = "Monitor TDTot - Validacao de Ordem e Regressao TSE"
Write-Host "======================================================================" -ForegroundColor Cyan
Write-Host "  MONITOR TDTOT TSE - VALIDACAO DE ORDEM (dg / hg MONOTONICO)" -ForegroundColor Yellow
Write-Host "======================================================================" -ForegroundColor Cyan

# Verifica se o Chrome de depuracao esta ativo
$cdpTest = try { (Invoke-WebRequest -Uri "http://127.0.0.1:9222/json/version" -UseBasicParsing -TimeoutSec 2).StatusCode } catch { 0 }
if ($cdpTest -ne 200) {
    Write-Host "Iniciando Chrome em modo de depuracao na porta 9222..." -ForegroundColor Yellow
    Start-Process "C:\Program Files\Google\Chrome\Application\chrome.exe" -ArgumentList @(
        "--remote-debugging-port=9222",
        "--user-data-dir=$env:LOCALAPPDATA\Google\Chrome\DebugProfile",
        "--remote-allow-origins=*",
        "https://resultados-sim.tse.jus.br/simulado/simulado/app/index.html#/eleicao/21270/uf/br/cargo/1/vis/nominal/resultados",
        "https://resultados-sim.tse.jus.br/simulado/simulado/app/index.html#/eleicao/21270/uf/sp/mu/71072/zn/0001/cargo/1/vis/nominal/resumo-geral"
    )
    Start-Sleep -Seconds 3
}

node "$PSScriptRoot\monitor_tdtot.js"
