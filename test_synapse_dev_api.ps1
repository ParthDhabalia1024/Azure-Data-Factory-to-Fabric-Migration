Connect-AzAccount
Select-AzSubscription -SubscriptionId "f4b50ff4-9f41-4407-bd29-9663327d220a"

$token = (Get-AzAccessToken -ResourceUrl "https://dev.azuresynapse.net").Token

Invoke-RestMethod `
  -Uri "https://synapse-fabricmigration.dev.azuresynapse.net/pipelines?api-version=2020-12-01" `
  -Headers @{ Authorization = "Bearer $token" }
