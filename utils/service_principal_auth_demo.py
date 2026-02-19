"""
Minimal service principal authentication demo.
Prereqs:
  pip install azure-identity requests
Env vars to set before running:
  AZURE_TENANT_ID       = <your tenant (directory) ID>
  AZURE_CLIENT_ID       = <your app (client) ID>
  AZURE_CLIENT_SECRET   = <your client secret value>
Optional overrides:
  ARM_SCOPE             = https://management.azure.com/.default
  GRAPH_SCOPE           = https://graph.microsoft.com/.default

Run:
  python utils/service_principal_auth_demo.py
"""
from __future__ import annotations

import os
from typing import Optional

import requests
from azure.core.exceptions import ClientAuthenticationError
from azure.identity import ClientSecretCredential


def get_env_var(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise EnvironmentError(f"Missing required environment variable: {name}")
    return value


def build_credential() -> ClientSecretCredential:
    tenant_id = get_env_var("AZURE_TENANT_ID")
    client_id = get_env_var("AZURE_CLIENT_ID")
    client_secret = get_env_var("AZURE_CLIENT_SECRET")
    return ClientSecretCredential(tenant_id=tenant_id, client_id=client_id, client_secret=client_secret)


def get_token(scope: str, credential: Optional[ClientSecretCredential] = None) -> str:
    cred = credential or build_credential()
    token = cred.get_token(scope)
    return token.token


def call_azure_resource_manager(credential: Optional[ClientSecretCredential] = None) -> requests.Response:
    """Example ARM call: list subscriptions for this service principal."""
    scope = os.getenv("ARM_SCOPE", "https://management.azure.com/.default")
    token = get_token(scope, credential)
    headers = {"Authorization": f"Bearer {token}"}
    url = "https://management.azure.com/subscriptions?api-version=2020-01-01"
    return requests.get(url, headers=headers, timeout=30)


def call_microsoft_graph_me(credential: Optional[ClientSecretCredential] = None) -> requests.Response:
    """Example Graph call; works only for app-permissioned endpoints."""
    scope = os.getenv("GRAPH_SCOPE", "https://graph.microsoft.com/.default")
    token = get_token(scope, credential)
    headers = {"Authorization": f"Bearer {token}"}
    url = "https://graph.microsoft.com/v1.0/users?$top=5&$select=id,displayName,mail,userPrincipalName"
    return requests.get(url, headers=headers, timeout=30)


def main() -> None:
    cred = build_credential()
    try:
        arm_resp = call_azure_resource_manager(cred)
        print(f"ARM status: {arm_resp.status_code}")
        print(arm_resp.text[:800], "..." if len(arm_resp.text) > 800 else "")
    except ClientAuthenticationError as exc:
        print(f"Auth failed: {exc}")
        return

    # Graph call needs appropriate app permissions (e.g., User.Read.All) with admin consent
    graph_resp = call_microsoft_graph_me(cred)
    print(f"Graph status: {graph_resp.status_code}")
    print(graph_resp.text[:800], "..." if len(graph_resp.text) > 800 else "")


if __name__ == "__main__":
    main()
