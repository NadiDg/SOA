$ErrorActionPreference = "Continue"
$Root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
Set-Location $Root

function Wait-Url([string]$Url, [int]$TimeoutSec = 180) {
  $deadline = (Get-Date).AddSeconds($TimeoutSec)
  while ((Get-Date) -lt $deadline) {
    try {
      $r = Invoke-WebRequest -Uri $Url -UseBasicParsing -TimeoutSec 5
      if ($r.StatusCode -ge 200 -and $r.StatusCode -lt 500) { return $true }
    } catch {}
    Start-Sleep -Seconds 3
  }
  return $false
}

Write-Host "=== SOA Lab6 demo: observability + AIOps ===" -ForegroundColor Cyan

Write-Host ""
Write-Host "[1] Waiting for gateway /health ..."
if (-not (Wait-Url "http://localhost:8080/health" 240)) { throw "Gateway not ready" }
Write-Host "OK gateway"

Write-Host ""
Write-Host "[2] Waiting for Prometheus / Grafana / Alertmanager / incident-sink ..."
$null = Wait-Url "http://localhost:9090/-/ready" 120
$null = Wait-Url "http://localhost:3000/api/health" 120
$null = Wait-Url "http://localhost:9093/-/ready" 60
$null = Wait-Url "http://localhost:8090/health" 60
Write-Host "OK observability stack"

Write-Host ""
Write-Host "[3] Metrics scrape check (gateway /metrics)"
$metrics = (Invoke-WebRequest -Uri "http://localhost:8080/metrics" -UseBasicParsing).Content
if ($metrics -match "soa_http_requests_total") {
  Write-Host "OK soa_http_requests_total present"
} else {
  Write-Host "WARN metrics missing"
}

Write-Host ""
Write-Host "[4] Generate load + structured logs with X-Trace-Id"
$trace = [guid]::NewGuid().ToString("N")
$headers = @{ "X-Trace-Id" = $trace }
1..15 | ForEach-Object {
  try { Invoke-WebRequest -Uri "http://localhost:8080/health" -Headers $headers -UseBasicParsing | Out-Null } catch {}
}
Write-Host "Trace-Id used: $trace"

Write-Host ""
Write-Host "[5] Trigger high latency (p99) via /api/demo/slow"
1..8 | ForEach-Object {
  try {
    Invoke-WebRequest -Uri "http://localhost:8080/api/demo/slow?delay_ms=1500" -UseBasicParsing -TimeoutSec 20 | Out-Null
    Write-Host "  slow request $_ done"
  } catch {
    Write-Host "  slow request $_ error: $($_.Exception.Message)"
  }
}

Write-Host ""
Write-Host "[6] Trigger replication lag gauge > 10s"
try {
  $lag = Invoke-RestMethod -Method Post -Uri "http://localhost:8080/api/demo/replication-lag?seconds=15"
  Write-Host ($lag | ConvertTo-Json -Compress)
} catch {
  Write-Host "replication-lag error: $($_.Exception.Message)"
}

Write-Host ""
Write-Host "[7] Import / activate n8n workflow (best-effort)"
try {
  docker exec notebook-n8n-lab6 n8n import:workflow --input=/data/soa-alerts-workflow.json 2>&1 | Write-Host
  $listed = docker exec notebook-n8n-lab6 n8n list:workflow 2>&1 | Out-String
  Write-Host $listed
  if ($listed -match "([A-Za-z0-9]+)\|SOA Lab6") {
    $wfId = $Matches[1]
    docker exec notebook-n8n-lab6 n8n update:workflow --id=$wfId --active=true 2>&1 | Write-Host
    Write-Host "Restarting n8n so activation takes effect ..."
    docker restart notebook-n8n-lab6 | Out-Null
    $null = Wait-Url "http://localhost:5678/healthz" 90
    Start-Sleep -Seconds 3
  }
} catch {
  Write-Host "n8n CLI import skipped; open http://localhost:5678 (admin@soa.local / SoaLab6Admin!) and Import n8n/soa-alerts-workflow.json"
}

Write-Host ""
Write-Host "[8] Simulate Alertmanager payload -> incident-sink (+ optional n8n webhook)"
$alertObj = @{
  status = "firing"
  alerts = @(
    @{
      status = "firing"
      labels = @{
        alertname = "HighLatencyP99"
        severity = "warning"
        service = "gateway-service"
      }
      annotations = @{
        summary = "High p99 latency on gateway-service"
        description = "Demo alert from scripts/demo.ps1"
      }
    }
  )
}
$alertBody = $alertObj | ConvertTo-Json -Depth 6

try {
  $t1 = Invoke-RestMethod -Method Post -Uri "http://localhost:8090/alerts/alertmanager" -ContentType "application/json" -Body $alertBody
  Write-Host "incident-sink webhook OK"
  Write-Host ($t1 | ConvertTo-Json -Compress)
} catch {
  Write-Host "incident-sink webhook failed: $($_.Exception.Message)"
}

try {
  $t2 = Invoke-RestMethod -Method Post -Uri "http://localhost:5678/webhook/soa-alerts" -ContentType "application/json" -Body $alertBody
  Write-Host "n8n webhook OK"
  Write-Host ($t2 | ConvertTo-Json -Compress)
} catch {
  Write-Host "n8n webhook not active yet; import workflow in UI, then re-run this step"
}

Write-Host ""
Write-Host "[9] Wait for Prometheus rules evaluation ..."
Start-Sleep -Seconds 35

Write-Host ""
Write-Host "[10] Prometheus targets / alerts / tickets"
try {
  $targets = Invoke-RestMethod "http://localhost:9090/api/v1/targets"
  $up = @($targets.data.activeTargets | Where-Object { $_.health -eq "up" }).Count
  $total = @($targets.data.activeTargets).Count
  Write-Host "Prometheus targets UP: $up / $total"
} catch {
  Write-Host "targets query failed"
}

try {
  $alerts = Invoke-RestMethod "http://localhost:9090/api/v1/alerts"
  Write-Host "Firing/pending alerts:"
  foreach ($a in $alerts.data.alerts) {
    $name = $a.labels.alertname
    $state = $a.state
    $sev = $a.labels.severity
    Write-Host "  - $name state=$state sev=$sev"
  }
} catch {
  Write-Host "alerts query failed"
}

try {
  $tickets = Invoke-RestMethod "http://localhost:8090/tickets"
  Write-Host "Incident tickets count: $($tickets.count)"
  foreach ($t in ($tickets.items | Select-Object -Last 5)) {
    $shortId = $t.id.Substring(0, 8)
    Write-Host "  ticket $shortId / $($t.severity) / $($t.title)"
  }
} catch {
  Write-Host "tickets query failed"
}

Write-Host ""
Write-Host "[11] Useful URLs"
Write-Host "  Gateway:      http://localhost:8080/health"
Write-Host "  Metrics:      http://localhost:8080/metrics"
Write-Host "  Prometheus:   http://localhost:9090"
Write-Host "  Alertmanager: http://localhost:9093"
Write-Host "  Grafana:      http://localhost:3000  (admin/admin) dashboard SOA Lab6 Observability"
Write-Host "  Loki:         http://localhost:3100"
Write-Host "  n8n:          http://localhost:5678"
Write-Host "  Incidents:    http://localhost:8090/tickets"
Write-Host "  Trace-Id:     $trace  (search in Loki / container logs)"
Write-Host ""
Write-Host "=== demo done ===" -ForegroundColor Green
