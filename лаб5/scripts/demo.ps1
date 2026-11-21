$ErrorActionPreference = "Stop"
$gw = "http://localhost:8080"
$kc = "http://localhost:8081"

Write-Host "=== 1. Health / security info / CORS ===" -ForegroundColor Cyan
Invoke-RestMethod "$gw/api/health/all" | ConvertTo-Json -Depth 5
Invoke-RestMethod "$gw/api/security/info" | ConvertTo-Json -Depth 6

Write-Host "`nCORS preflight/response for Origin https://cad.kpi.ua" -ForegroundColor Cyan
$cors = Invoke-WebRequest -Uri "$gw/api/cors-check" -Headers @{ Origin = "https://cad.kpi.ua" } -UseBasicParsing
"Status=$($cors.StatusCode)"
"Access-Control-Allow-Origin=$($cors.Headers['Access-Control-Allow-Origin'])"
$cors.Content

function Get-Token($username, $password) {
  $body = @{
    grant_type = "password"
    client_id  = "notebook-gateway"
    username   = $username
    password   = $password
    scope      = "openid"
  }
  $resp = Invoke-RestMethod -Method Post -Uri "$kc/realms/notebook/protocol/openid-connect/token" -Body $body -ContentType "application/x-www-form-urlencoded"
  return $resp.access_token
}

Write-Host "`n=== 2. OAuth2 token (password grant for automation; browser uses Auth Code) ===" -ForegroundColor Cyan
$token = Get-Token "student" "student"
"token_len=$($token.Length)"
$headers = @{ Authorization = "Bearer $token" }

Write-Host "`n=== 3. JWT /api/security/me ===" -ForegroundColor Cyan
Invoke-RestMethod "$gw/api/security/me" -Headers $headers | ConvertTo-Json -Depth 5

Write-Host "`n=== 4. Scoped access notebook.write + tags.read ===" -ForegroundColor Cyan
Invoke-RestMethod -Method Post -Uri "$gw/api/notes" -Headers $headers -ContentType "application/json" -Body '{"title":"Lab5 secure note","content":"jwt+scopes","tags":["ok","sec"]}' | ConvertTo-Json -Depth 5
Invoke-RestMethod "$gw/api/notes" -Headers $headers | ConvertTo-Json -Depth 5
Invoke-RestMethod "$gw/api/tags/events" -Headers $headers | ConvertTo-Json -Depth 4

Write-Host "`n=== 5. Insufficient scope (reader cannot write) ===" -ForegroundColor Cyan
$reader = Get-Token "reader" "reader"
$rh = @{ Authorization = "Bearer $reader" }
try {
  Invoke-RestMethod -Method Post -Uri "$gw/api/notes" -Headers $rh -ContentType "application/json" -Body '{"title":"should fail","content":"x","tags":["ok"]}'
  Write-Host "UNEXPECTED success"
} catch {
  Write-Host "EXPECTED 403:" $_.ErrorDetails.Message
}

Write-Host "`n=== 6. Unauthorized without token ===" -ForegroundColor Cyan
try {
  Invoke-RestMethod "$gw/api/notes"
} catch {
  Write-Host "EXPECTED 401:" $_.Exception.Message
}

Write-Host "`nBrowser Auth Code login: $gw/oauth/login" -ForegroundColor Green
Write-Host "Keycloak Admin: $kc (admin/admin). Add Identity Provider for Login with Atlassian/Google." -ForegroundColor Green
