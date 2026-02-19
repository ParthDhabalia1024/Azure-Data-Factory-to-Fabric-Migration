import base64
import json
import os
import subprocess
import sys
from pathlib import Path

from azure.identity import ClientSecretCredential
import requests

# Ensure repo root on sys.path for local execution
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from Synapse_Data.fabric_copyjob_warehouse import _get_with_lro, get_fabric_token

ws = os.getenv("FABRIC_WORKSPACE_ID", "70aaeb4a-b6fe-47de-b76a-b5726c78f156")
default_cj = os.getenv("COPYJOB_ID", "")

def _try_get_user_token() -> str:
    try:
        proc = subprocess.run(
            ["az", "account", "get-access-token", "--resource", "https://api.fabric.microsoft.com", "-o", "tsv", "--query", "accessToken"],
            capture_output=True,
            text=True,
            check=True,
        )
        tok = (proc.stdout or "").strip()
        if not tok:
            print("az get-access-token returned empty token; stderr=", (proc.stderr or "").strip())
        return tok
    except Exception as e:
        print(f"az get-access-token failed: {e}")
        return ""

def _get_token() -> str:
    env_tok = os.getenv("FABRIC_BEARER_TOKEN", "").strip()
    if env_tok:
        print("Using FABRIC_BEARER_TOKEN from environment")
        return env_tok
    user_tok = _try_get_user_token()
    if user_tok:
        print("Using user token from az account get-access-token")
        return user_tok
    print("Using service principal token")
    cred = ClientSecretCredential(
        os.environ["AZURE_TENANT_ID"],
        os.environ["AZURE_CLIENT_ID"],
        os.environ["AZURE_CLIENT_SECRET"],
    )
    return get_fabric_token(cred)

token = _get_token()
headers = {"Authorization": f"Bearer {token}"}

def list_copy_jobs():
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{ws}/copyJobs"
    resp = requests.get(url, headers=headers).json()
    jobs = resp.get("value", []) if isinstance(resp, dict) else []
    print("=== Copy Jobs ===")
    print(json.dumps(jobs, indent=2))
    return jobs

def list_runs(copyjob_id: str):
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{ws}/copyJobs/{copyjob_id}/runs"
    resp = requests.get(url, headers=headers).json()
    runs = resp.get("value", []) if isinstance(resp, dict) else []
    print(f"=== Runs for {copyjob_id} ===")
    print(json.dumps(runs, indent=2))
    return runs

def show_run_details(copyjob_id: str, run_id: str):
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{ws}/copyJobs/{copyjob_id}/runs/{run_id}"
    resp = requests.get(url, headers=headers).json()
    print(f"=== Run details for {run_id} ===")
    print(json.dumps(resp, indent=2))

jobs = list_copy_jobs()

target_cj = default_cj or (jobs[0]["id"] if jobs else None)
if target_cj:
    runs = list_runs(target_cj)
    if runs:
        first_run = runs[0]["id"]
        show_run_details(target_cj, first_run)

# Existing definition dump (uses target_cj if available)
cj = target_cj or ""
if cj:
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{ws}/copyJobs/{cj}/getDefinition"
    defn = None
    err1 = None
    try:
        defn = _get_with_lro(token, url, timeout_seconds=300)
    except Exception as e:
        err1 = e

    if defn is None:
        url_items = f"https://api.fabric.microsoft.com/v1/workspaces/{ws}/items/{cj}/getDefinition"
        try:
            defn = _get_with_lro(token, url_items, timeout_seconds=300)
            print(f"copyJobs getDefinition failed, items getDefinition succeeded (url={url_items})")
        except Exception as e:
            print(f"copyJobs getDefinition failed: {err1}")
            print(f"items getDefinition failed: {e}")
            raise

    parts = (((defn or {}).get("definition") or {}).get("parts"))
    print("=== Definition parts ===")
    if isinstance(parts, list):
        for p in parts:
            if not isinstance(p, dict):
                continue
            payload = p.get("payload")
            payload_len = len(payload) if isinstance(payload, str) else None
            print({"path": p.get("path"), "payloadType": p.get("payloadType"), "payloadLen": payload_len})
    else:
        print("No parts found. Raw:")
        print(defn)

    content_part = None
    if isinstance(parts, list):
        for p in parts:
            if isinstance(p, dict) and p.get("path") == "copyjob-content.json":
                content_part = p
                break

    print("\n=== copyjob-content.json summary ===")
    if not content_part:
        print("Missing copyjob-content.json part")
    else:
        try:
            if content_part.get("payloadType") != "InlineBase64":
                raise ValueError(f"Unexpected payloadType: {content_part.get('payloadType')}")
            raw = base64.b64decode(content_part.get("payload") or "")
            obj = json.loads(raw.decode("utf-8"))
            print({"topLevelKeys": list(obj.keys()) if isinstance(obj, dict) else type(obj)})
            if isinstance(obj, dict):
                props = obj.get("properties")
                acts = obj.get("activities")
                print({
                    "propertiesType": type(props).__name__,
                    "activitiesType": type(acts).__name__,
                    "activitiesCount": len(acts) if isinstance(acts, list) else None,
                    "jobMode": props.get("jobMode") if isinstance(props, dict) else None,
                    "sourceType": ((props.get("source") or {}).get("type")) if isinstance(props, dict) else None,
                    "destinationType": ((props.get("destination") or {}).get("type")) if isinstance(props, dict) else None,
                })
        except Exception as e:
            print(f"Failed to decode content: {e}")