$ErrorActionPreference = "Stop"
$base = "http://localhost:8080"

Write-Host "=== 1. Health ===" -ForegroundColor Cyan
Invoke-RestMethod "$base/api/health/all" | ConvertTo-Json -Depth 6

Write-Host "`n=== 2. Auth register/login ===" -ForegroundColor Cyan
try {
    $reg = Invoke-RestMethod -Method Post -Uri "$base/api/auth/register" -ContentType "application/json" -Body '{"email":"student@kpi.ua","password":"pass123","display_name":"Student"}'
} catch {
    $reg = Invoke-RestMethod -Method Post -Uri "$base/api/auth/login" -ContentType "application/json" -Body '{"email":"student@kpi.ua","password":"pass123"}'
}
$token = $reg.token
$headers = @{ Authorization = "Bearer $token" }
Write-Host "token ok, auth instance:" $reg.instance

Write-Host "`n=== 3. Create note (Transactional Outbox) ===" -ForegroundColor Cyan
$note = Invoke-RestMethod -Method Post -Uri "$base/api/notes" -ContentType "application/json" -Headers $headers -Body '{"title":"SAGA fail demo","content":"should be compensated","tags":["fail","saga"]}'
$note | ConvertTo-Json -Depth 6
$noteId = $note.note.id
Write-Host "note id:" $noteId "status:" $note.note.status "outbox:" $note.outbox_id

Write-Host "`n=== 4. Wait for outbox publish + consumer fail + DLQ compensation ===" -ForegroundColor Cyan
Start-Sleep -Seconds 5

Write-Host "`n=== 5. Outbox table ===" -ForegroundColor Cyan
Invoke-RestMethod "$base/api/outbox" | ConvertTo-Json -Depth 6

Write-Host "`n=== 6. Tag inbox/events (consumer failure) ===" -ForegroundColor Cyan
Invoke-RestMethod "$base/api/tags/inbox" | ConvertTo-Json -Depth 6
Invoke-RestMethod "$base/api/tags/events" | ConvertTo-Json -Depth 6

Write-Host "`n=== 7. Saga log + note after compensation ===" -ForegroundColor Cyan
Invoke-RestMethod "$base/api/sagas" | ConvertTo-Json -Depth 6
Invoke-RestMethod -Uri "$base/api/notes/$noteId" -Headers $headers | ConvertTo-Json -Depth 6

Write-Host "`n=== 8. Optional success path (title OK:) with FORCE still true skips via ok tag ===" -ForegroundColor Cyan
$ok = Invoke-RestMethod -Method Post -Uri "$base/api/notes" -ContentType "application/json" -Headers $headers -Body '{"title":"OK: indexed note","content":"success path","tags":["ok","search"]}'
$okId = $ok.note.id
Start-Sleep -Seconds 4
Invoke-RestMethod -Uri "$base/api/notes/$okId" -Headers $headers | ConvertTo-Json -Depth 6
Invoke-RestMethod "$base/api/sagas" | ConvertTo-Json -Depth 6
