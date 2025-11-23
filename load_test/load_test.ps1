$teamBody = @'
{
  "team_name": "load-team",
  "members": [
    { "user_id": "u1", "username": "User1", "is_active": true },
    { "user_id": "u2", "username": "User2", "is_active": true },
    { "user_id": "u3", "username": "User3", "is_active": true }
  ]
}
'@

Invoke-RestMethod -Uri "http://localhost:8080/team/add" `
    -Method Post `
    -ContentType "application/json" `
    -Body $teamBody

# 20 PRs
for ($i = 1; $i -le 20; $i++) {
    $body = @{
        pull_request_id   = "pr-load-$i"
        pull_request_name = "Load test PR $i"
        author_id         = "u1"
    } | ConvertTo-Json

    Invoke-RestMethod -Uri "http://localhost:8080/pullRequest/create" `
        -Method Post `
        -ContentType "application/json" `
        -Body $body | Out-Null
}

# stats
hey -z 30s -q 5 -c 5 "http://localhost:8080/stats"

# getReview
hey -z 30s -q 5 -c 5 "http://localhost:8080/users/getReview?user_id=u2"

hey -z 30s -q 20 -c 20 "http://localhost:8080/users/getReview?user_id=u2"

# create
$prBody = @'
{
  "pull_request_id": "pr-load-single",
  "pull_request_name": "Load single PR",
  "author_id": "u1"
}
'@

hey -z 10s -q 2 -c 2 `
    -m POST `
    -H "Content-Type: application/json" `
    -d "$prBody" `
    "http://localhost:8080/pullRequest/create"

Invoke-RestMethod -Uri "http://localhost:8080/stats" -Method Get
Invoke-RestMethod -Uri "http://localhost:8080/users/getReview?user_id=u2" -Method Get

Read-Host "Нажми Enter, чтобы закрыть окно"
