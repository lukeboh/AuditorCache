# Inicia o monitor de atualizacao TDTot em uma janela dedicada
$host.UI.RawUI.WindowTitle = "Monitor TDTot - Validacao de Ordem e Regressao TSE"
Write-Host "======================================================================" -ForegroundColor Cyan
Write-Host "  MONITOR TDTOT TSE - VALIDACAO DE ORDEM (dg / hg MONOTONICO)" -ForegroundColor Yellow
Write-Host "======================================================================" -ForegroundColor Cyan

# --- Funcoes de Verificacao e Instalacao do Node.js ---
function Refresh-PathEnvironment {
    $machinePath = [System.Environment]::GetEnvironmentVariable("Path", "Machine")
    $userPath = [System.Environment]::GetEnvironmentVariable("Path", "User")
    $combined = @($machinePath, $userPath, $env:Path) -join ';'
    $paths = $combined -split ';' | Where-Object { $_ -and (Test-Path $_) } | Select-Object -Unique
    $env:Path = $paths -join ';'
}

function Get-NodeInfo {
    $nodeCmd = Get-Command node -ErrorAction SilentlyContinue
    if (-not $nodeCmd) {
        Refresh-PathEnvironment
        $nodeCmd = Get-Command node -ErrorAction SilentlyContinue
    }
    
    # Se ainda nao encontrou no PATH, verifica diretorios padroes de instalacao
    if (-not $nodeCmd) {
        $commonLocations = @(
            "$env:ProgramFiles\nodejs",
            "C:\Program Files\nodejs",
            "D:\Program Files\nodejs",
            "${env:ProgramFiles(x86)}\nodejs",
            "$env:LOCALAPPDATA\Programs\node"
        )
        foreach ($loc in $commonLocations) {
            if (Test-Path "$loc\node.exe") {
                $env:Path = "$loc;$env:Path"
                $nodeCmd = Get-Command node -ErrorAction SilentlyContinue
                break
            }
        }
    }

    if ($nodeCmd) {
        try {
            $ver = (& node -v 2>$null).Trim()
            if ($ver -match '^v(\d+)\.') {
                return [PSCustomObject]@{
                    Installed    = $true
                    Version      = $ver
                    MajorVersion = [int]$matches[1]
                    Path         = $nodeCmd.Source
                }
            }
        } catch {}
    }

    return [PSCustomObject]@{
        Installed    = $false
        Version      = $null
        MajorVersion = 0
        Path         = $null
    }
}

function Install-NodeJs {
    Write-Host "`n[!] Providenciando a instalacao do Node.js LTS (>= v22)..." -ForegroundColor Yellow

    # Metodo 1: via winget (Windows Package Manager)
    if (Get-Command winget -ErrorAction SilentlyContinue) {
        Write-Host "Tentando instalar Node.js LTS via winget..." -ForegroundColor Cyan
        try {
            winget install OpenJS.NodeJS.LTS --accept-package-agreements --accept-source-agreements
        } catch {
            Write-Host "Aviso: Nao foi possivel concluir via winget: $_" -ForegroundColor Yellow
        }
        
        Refresh-PathEnvironment
        $check = Get-NodeInfo
        if ($check.Installed -and $check.MajorVersion -ge 22) {
            return $check
        }
    }

    # Metodo 2: Download direto do instalador oficial MSI
    Write-Host "Baixando instalador oficial (.msi) do Node.js..." -ForegroundColor Cyan
    $msiUrl = $null
    try {
        $shasumTxt = (Invoke-WebRequest -Uri "https://nodejs.org/dist/latest-v22.x/SHASUMS256.txt" -UseBasicParsing -TimeoutSec 8).Content
        if ($shasumTxt -match '(node-v22\.\d+\.\d+-x64\.msi)') {
            $msiUrl = "https://nodejs.org/dist/latest-v22.x/$($matches[1])"
        }
    } catch {
        Write-Host "Aviso: Consulta direta ao site do Node.js lenta ou com falha. Tentando URL padrao..." -ForegroundColor Yellow
    }

    if (-not $msiUrl) {
        $msiUrl = "https://nodejs.org/dist/latest-v22.x/node-v22.23.3-x64.msi"
    }

    $tempMsi = Join-Path $env:TEMP "nodejs-installer-$([Guid]::NewGuid().ToString('N').Substring(0,8)).msi"
    try {
        Write-Host "Baixando $msiUrl..." -ForegroundColor Cyan
        Invoke-WebRequest -Uri $msiUrl -OutFile $tempMsi -UseBasicParsing
        Write-Host "Executando o assistente de instalacao do Node.js..." -ForegroundColor Yellow
        $proc = Start-Process msiexec.exe -ArgumentList "/i `"$tempMsi`"" -Wait -PassThru
        if ($proc.ExitCode -ne 0) {
            Write-Host "Aviso: Instalador finalizou com codigo de saida $($proc.ExitCode)." -ForegroundColor Yellow
        }
    } catch {
        Write-Host "Erro durante a instalacao automatica: $_" -ForegroundColor Red
    } finally {
        if (Test-Path -LiteralPath $tempMsi) {
            Remove-Item -LiteralPath $tempMsi -ErrorAction SilentlyContinue
        }
    }

    Refresh-PathEnvironment
    return (Get-NodeInfo)
}

# --- Validacao do Node.js ---
$nodeInfo = Get-NodeInfo

if (-not $nodeInfo.Installed) {
    Write-Host "`n[!] Node.js nao foi detectado no sistema." -ForegroundColor Yellow
    $nodeInfo = Install-NodeJs

    if (-not $nodeInfo.Installed) {
        Write-Host "`n======================================================================" -ForegroundColor Red
        Write-Host "  ERRO: Nao foi possivel detectar o Node.js apos a tentativa de instalacao." -ForegroundColor Red
        Write-Host "  Por favor, baixe e instale manualmente a versao 22 ou superior em:" -ForegroundColor Yellow
        Write-Host "  https://nodejs.org/" -ForegroundColor Cyan
        Write-Host "======================================================================" -ForegroundColor Red
        Read-Host "Pressione ENTER para encerrar..."
        exit 1
    }
} elseif ($nodeInfo.MajorVersion -lt 22) {
    Write-Host "`n[AVISO] Versao do Node.js detectada: $($nodeInfo.Version)" -ForegroundColor Yellow
    Write-Host "O Monitor TDTot requer Node.js >= 22.5.0 para compatibilidade com 'node:sqlite'." -ForegroundColor Yellow
    $ans = Read-Host "Deseja providenciar a atualizacao para a versao LTS agora? (S/N) [Padrao: N]"
    if ($ans -match '^(s|sim|y|yes)$') {
        $nodeInfo = Install-NodeJs
    }
}

Write-Host "Node.js pronto para execucao: $($nodeInfo.Version) ($($nodeInfo.Path))`n" -ForegroundColor Green

# --- Verifica se o Chrome de depuracao esta ativo ---
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

# --- Inicializa a aplicacao Node.js ---
node "$PSScriptRoot\monitor_tdtot.js"
