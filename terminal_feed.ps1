# Janela de Acompanhamento ao Vivo: FONTE (HMG) vs CACHE (SIM)
$host.UI.RawUI.WindowTitle = "TDTot - FONTE (HMG) vs CACHE (SIM) - Propagacao e Atraso"
Clear-Host

Write-Host "======================================================================" -ForegroundColor Cyan
Write-Host "  TDTOT TSE - PROPAGACAO: FONTE (HMG) -> CACHE (SIM)" -ForegroundColor Yellow
Write-Host "  Dashboard Web aberto no Chrome: http://127.0.0.1:3333" -ForegroundColor White
Write-Host "======================================================================" -ForegroundColor Cyan
Write-Host ""

$logPath = "$PSScriptRoot\historico_comparativo.csv"

# Mostra ultimos registros
if (Test-Path $logPath) {
    Import-Csv $logPath | Select-Object -Last 10 | ForEach-Object {
        $color = if ($_.status_propagacao -eq 'SINCRONIZADO') { 'Green' } else { 'Yellow' }
        Write-Host "[$($_.status_propagacao)] $($_.arquivo) | FONTE(HMG): $($_.hmg_fonte_hg) -> CACHE(SIM): $($_.sim_cache_hg) | $($_.detalhes)" -ForegroundColor $color
    }
}

Write-Host "`nAguardando novas comparacoes (tempo real a cada 5s)...`n" -ForegroundColor DarkGray

$lastCount = (Get-Content $logPath -ErrorAction SilentlyContinue).Count
while ($true) {
    Start-Sleep -Seconds 2
    $lines = Get-Content $logPath -ErrorAction SilentlyContinue
    $currentCount = $lines.Count
    if ($currentCount -gt $lastCount) {
        $newLines = $lines[$lastCount..($currentCount - 1)]
        foreach ($line in $newLines) {
            if ($line -like "*REGRESSAO*") {
                Write-Host "🚨 $line" -ForegroundColor Red
            } elseif ($line -like "*SINCRONIZADO*") {
                Write-Host "✅ $line" -ForegroundColor Green
            } else {
                Write-Host "📡 $line" -ForegroundColor Yellow
            }
        }
        $lastCount = $currentCount
    }
}
