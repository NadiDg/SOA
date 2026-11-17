$ErrorActionPreference = "Stop"
$base = "http://localhost:8080"

Write-Host "=== 1. Health of all services ===" -ForegroundColor Cyan
Invoke-RestMethod "$base/api/health/all" | ConvertTo-Json -Depth 6

Write-Host "`n=== 2. Synchronous pipeline (auth -> notebook -> tag) ===" -ForegroundColor Cyan
$pipe = Invoke-RestMethod -Method Post -Uri "$base/api/pipeline" -ContentType "application/json" -Body '{"text":"HELLO"}'
$pipe | ConvertTo-Json -Depth 6
Write-Host "OUTPUT:" $pipe.output

Write-Host "`n=== 3. Register + create note (sync auth call + async RabbitMQ event) ===" -ForegroundColor Cyan
try {
    $reg = Invoke-RestMethod -Method Post -Uri "$base/api/auth/register" -ContentType "application/json" -Body '{"email":"student@kpi.ua","password":"pass123","display_name":"Student"}'
} catch {
    $reg = Invoke-RestMethod -Method Post -Uri "$base/api/auth/login" -ContentType "application/json" -Body '{"email":"student@kpi.ua","password":"pass123"}'
}
$token = $reg.token
Write-Host "Token acquired from auth-service instance:" $reg.instance

$headers = @{ Authorization = "Bearer $token" }
$noteBody = '{"title":"Lab2 note","content":"async demo","tags":["soa","ddd"]}'
$note = Invoke-RestMethod -Method Post -Uri "$base/api/notes" -ContentType "application/json" -Headers $headers -Body $noteBody
$note | ConvertTo-Json -Depth 6

Start-Sleep -Seconds 2
Write-Host "`n=== 4. Async consumer result in tag-service ===" -ForegroundColor Cyan
Invoke-RestMethod "$base/api/tags/events" | ConvertTo-Json -Depth 6

Write-Host "`n=== 5. Service discovery registry ===" -ForegroundColor Cyan
Invoke-RestMethod "$base/api/discovery" | ConvertTo-Json -Depth 6

Write-Host "`n=== 6. Horizontal scaling probe (repeat whoami-notebook) ===" -ForegroundColor Cyan
1..8 | ForEach-Object {
    $h = Invoke-RestMethod "$base/api/whoami-notebook"
    Write-Host ("call {0}: instance={1}" -f $_, $h.instance)
}
