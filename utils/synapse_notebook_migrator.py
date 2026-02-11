import subprocess
import json
import os
from pathlib import Path
import shutil
import requests
import base64
import time

AZ_PATH: str | None = None

def _run(cmd: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=True)

def _find_az_path() -> str | None:
    # Prefer PATH resolution first
    az = shutil.which("az")
    if az:
        return az
    # Common Windows install locations for Azure CLI
    candidates = [
        r"C:\\Program Files\\Microsoft SDKs\\Azure\\CLI2\\wbin\\az.cmd",
        r"C:\\Program Files (x86)\\Microsoft SDKs\\Azure\\CLI2\\wbin\\az.cmd",
        os.path.expandvars(r"%LocalAppData%\\Programs\\AzureCLI\\wbin\\az.cmd"),
        os.path.expandvars(r"%ProgramFiles%\\AzureCLI\\wbin\\az.cmd"),
    ]
    for p in candidates:
        if p and os.path.isfile(p):
            return p
    return None

def _ensure_az_available() -> str:
    global AZ_PATH
    if AZ_PATH and os.path.isfile(AZ_PATH):
        return AZ_PATH
    az = _find_az_path()
    if not az:
        raise FileNotFoundError(
            "Azure CLI ('az') not found. Install from https://aka.ms/azure-cli and restart, or add it to PATH."
        )
    AZ_PATH = az
    return AZ_PATH

def get_cli_token(resource: str) -> str:
    az = _ensure_az_available()
    out = _run([az, "account", "get-access-token", "--resource", resource, "-o", "json"]).stdout
    return json.loads(out)["accessToken"]


def _ensure_valid_ipynb(notebook_path: Path) -> None:
    """Ensure the notebook file is a valid .ipynb with nbformat, cells, metadata.

    If the file is missing nbformat (common when using Synapse Dev API raw JSON),
    attempt to lift nbformat/cells from a 'properties' object; otherwise wrap the
    content as a markdown cell and create a minimal notebook shell.
    """
    try:
        with open(notebook_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        raise RuntimeError(f"Invalid notebook JSON: {e}") from e

    # If already valid, nothing to do
    if isinstance(data, dict) and "nbformat" in data and "cells" in data:
        return

    # Try to lift from Synapse-like 'properties' structure
    if isinstance(data, dict) and "properties" in data and isinstance(data["properties"], dict):
        props = data["properties"]
        nb = {
            "nbformat": props.get("nbformat", 4),
            "nbformat_minor": props.get("nbformat_minor", props.get("nbformatMinor", 2)),
            "metadata": props.get("metadata", {}),
            "cells": props.get("cells", []),
        }
        with open(notebook_path, "w", encoding="utf-8") as f:
            json.dump(nb, f, ensure_ascii=False, indent=2)
        return

    # Fallback: wrap raw content into a minimal notebook as markdown
    md_text = json.dumps(data, ensure_ascii=False, indent=2)
    nb = {
        "nbformat": 4,
        "nbformat_minor": 2,
        "metadata": {},
        "cells": [
            {
                "cell_type": "markdown",
                "metadata": {},
                "source": [
                    "Original content could not be auto-converted.\n",
                    "Embedded raw JSON below:\n\n",
                    f"````json\n{md_text}\n````"
                ],
            }
        ],
    }
    with open(notebook_path, "w", encoding="utf-8") as f:
        json.dump(nb, f, ensure_ascii=False, indent=2)

def list_synapse_notebooks(workspace_name: str) -> list[dict]:
    # Try Azure CLI first. If not available, fall back to Synapse Dev API.
    try:
        az = _ensure_az_available()
        try:
            out = _run([
                az, "synapse", "notebook", "list",
                "--workspace-name", workspace_name,
                "-o", "json",
                "--only-show-errors",
            ]).stdout
            return json.loads(out)
        except subprocess.CalledProcessError:
            pass
    except FileNotFoundError:
        pass

    # Dev API fallback
    token = get_cli_token("https://dev.azuresynapse.net")
    url = f"https://{workspace_name}.dev.azuresynapse.net/notebooks?api-version=2020-12-01"
    resp = requests.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    return data if isinstance(data, list) else data.get("value", [])

def export_synapse_notebook(workspace_name: str, notebook_name: str, output_dir: str) -> Path:
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    # Try using Azure CLI export first; if extension missing, fall back to Dev API export
    az = None
    try:
        az = _ensure_az_available()
        try:
            _run([az, "extension", "add", "--name", "synapse", "--only-show-errors"])  # idempotent
        except subprocess.CalledProcessError:
            az = None  # force Dev API fallback below
    except FileNotFoundError:
        az = None
    # Determine the exact notebook name as stored in Synapse (case-sensitive)
    canonical_name = notebook_name
    try:
        notebooks = list_synapse_notebooks(workspace_name)
        names = [n.get("name") for n in notebooks if isinstance(n, dict)]
        if names and notebook_name not in names:
            # Try case-insensitive match
            for nn in names:
                if isinstance(nn, str) and nn.lower() == notebook_name.lower():
                    canonical_name = nn
                    break
    except Exception:
        # Non-fatal; continue with provided name
        pass

    if az:
        try:
            _run([
                az, "synapse", "notebook", "export",
                "--workspace-name", workspace_name,
                "--name", canonical_name,
                "--output-folder", str(out_dir),
                "--only-show-errors",
            ])
        except subprocess.CalledProcessError:
            az = None  # fall through to Dev API

    if not az:
        # Dev API export: GET the notebook JSON and save as .ipynb
        token = get_cli_token("https://dev.azuresynapse.net")
        enc = requests.utils.quote(canonical_name, safe="")
        url = f"https://{workspace_name}.dev.azuresynapse.net/notebooks/{enc}?api-version=2020-12-01"
        r = requests.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=60)
        r.raise_for_status()
        nb_json = r.json()
        # Save pretty to ipynb
        p = out_dir / f"{canonical_name}.ipynb"
        with open(p, "w", encoding="utf-8") as f:
            json.dump(nb_json, f, ensure_ascii=False, indent=2)
        return p
    p = out_dir / f"{canonical_name}.ipynb"
    if not p.exists():
        # CLI may export into a subfolder named after notebook
        alt = out_dir / canonical_name / f"{canonical_name}.ipynb"
        if alt.exists():
            return alt
        # As a final attempt, search for any .ipynb created under output_dir
        matches = list(out_dir.rglob("*.ipynb"))
        if matches:
            return matches[0]
        # If we reach here, export likely did not produce a file. Provide discovered names to help selection.
        try:
            notebooks = list_synapse_notebooks(workspace_name)
            names = ", ".join(sorted([n.get("name", "") for n in notebooks if isinstance(n, dict)]))
        except Exception:
            names = "(could not retrieve names)"
        raise FileNotFoundError(f"Notebook export not found. Verify the exact name. Known notebooks: {names}")
    return p

def upload_notebook_to_fabric(workspace_id: str, notebook_path: str, display_name: str | None = None) -> dict:
    token = get_cli_token("https://api.fabric.microsoft.com")
    auth_headers = {"Authorization": f"Bearer {token}"}
    # Validate workspace exists and you have access
    ws_url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}"
    ws_resp = requests.get(ws_url, headers=auth_headers, timeout=60)
    if ws_resp.status_code == 404:
        raise FileNotFoundError(
            f"Fabric workspace not found or inaccessible: {workspace_id}. Ensure the ID is correct and you have at least Member/Contributor access."
        )
    ws_resp.raise_for_status()

    name = display_name or Path(notebook_path).stem

    def _list_notebooks() -> list[dict]:
        try:
            r = requests.get(
                f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items?type=Notebook",
                headers=auth_headers,
                timeout=60,
            )
            r.raise_for_status()
            data = r.json()
            return data if isinstance(data, list) else data.get("value", [])
        except Exception:
            return []

    def _poll_operation_if_needed(resp: requests.Response) -> None:
        # Handle LRO 202 with Location header
        if resp.status_code != 202:
            return
        op_url = resp.headers.get("Location") or resp.headers.get("location")
        if not op_url:
            return
        for _ in range(30):  # up to ~15 minutes with backoff
            r = requests.get(op_url, headers=auth_headers, timeout=60)
            try:
                data = r.json()
            except ValueError:
                data = {}
            status = (data or {}).get("status") or (data or {}).get("state")
            if status and status.lower() in ("succeeded", "failed", "cancelled"):
                if status.lower() != "succeeded":
                    raise RuntimeError(f"Fabric operation did not succeed: {data}")
                return
            ra = r.headers.get("Retry-After")
            delay = int(ra) if ra and ra.isdigit() else 5
            time.sleep(delay)

    # Path A: Generic Items Create with JSON definition (user-proven)
    with open(notebook_path, "rb") as f:
        b64 = base64.b64encode(f.read()).decode("utf-8")
    items_url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items"
    items_payload = {
        "displayName": name,
        "type": "Notebook",
        "definition": {
            "format": "ipynb",
            "parts": [
                {
                    "path": "notebook-content.ipynb",
                    "payload": b64,
                    "payloadType": "InlineBase64",
                }
            ],
        },
    }
    r = requests.post(items_url, headers={**auth_headers, "Content-Type": "application/json"}, json=items_payload, timeout=180)
    if r.status_code == 201 or r.status_code == 200:
        return r.json()
    if r.status_code == 400 and "src property" in (r.text or "").lower():
        # Retry adding a minimal .platform part to satisfy stricter schemas
        items_payload["definition"]["parts"].append({
            "path": ".platform",
            "payload": base64.b64encode(b"{}").decode("ascii"),
            "payloadType": "InlineBase64",
        })
        r = requests.post(items_url, headers={**auth_headers, "Content-Type": "application/json"}, json=items_payload, timeout=180)
        if r.status_code in (200, 201, 202):
            _poll_operation_if_needed(r)
            try:
                return r.json()
            except ValueError:
                pass
            match = next((it for it in _list_notebooks() if isinstance(it, dict) and it.get("displayName") == name), None)
            if match:
                return match
    if r.status_code == 404:
        # Try Path B: Create Notebook API
        create_url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/notebooks"
        create_payload = {
            "displayName": name,
            "definition": {
                "format": "ipynb",
                "parts": [
                    {
                        "path": "notebook-content.ipynb",
                        "payload": b64,
                        "payloadType": "InlineBase64",
                    }
                ],
            },
        }
        r2 = requests.post(create_url, headers={**auth_headers, "Content-Type": "application/json"}, json=create_payload, timeout=180)
        if r2.status_code in (200, 201, 202):
            _poll_operation_if_needed(r2)
            try:
                return r2.json()
            except ValueError:
                pass
            match = next((it for it in _list_notebooks() if isinstance(it, dict) and it.get("displayName") == name), None)
            if match:
                return match
        r2.raise_for_status()
        return r2.json()
    # Other errors from Path A
    r.raise_for_status()
    return r.json()

    # Then Path C: legacy items/import (multipart)
    import_url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/import"
    data = {"type": "Notebook", "displayName": name}
    with open(notebook_path, "rb") as f:
        files = {"file": (Path(notebook_path).name, f, "application/octet-stream")}
        r3 = requests.post(import_url, headers=auth_headers, data=data, files=files, timeout=180)
    if r3.status_code in (200, 201, 202):
        _poll_operation_if_needed(r3)
        try:
            return r3.json()
        except ValueError:
            pass
        match = next((it for it in _list_notebooks() if isinstance(it, dict) and it.get("displayName") == name), None)
        if match:
            return match
    if r3.status_code == 404:
        raise FileNotFoundError(
            f"Fabric endpoints for Notebook creation/import returned 404. Verify capacity assignment and API availability. Response: {r3.text}"
        )
    r3.raise_for_status()
    return r3.json()

def migrate_synapse_notebook_to_fabric(synapse_workspace_name: str, notebook_name: str, fabric_workspace_id: str, output_dir: str = "./utils/exported_notebooks") -> dict:
    ipynb_path = export_synapse_notebook(synapse_workspace_name, notebook_name, output_dir)
    # Double-check the file exists before attempting upload
    if not Path(ipynb_path).exists():
        # Provide context for troubleshooting
        try:
            notebooks = list_synapse_notebooks(synapse_workspace_name)
            names = ", ".join(sorted([n.get("name", "") for n in notebooks if isinstance(n, dict)]))
        except Exception:
            names = "(could not retrieve names)"
        raise FileNotFoundError(f"Exported notebook file not found at: {ipynb_path}. Known notebooks: {names}")
    _ensure_valid_ipynb(Path(ipynb_path))
    return upload_notebook_to_fabric(fabric_workspace_id, str(ipynb_path), display_name=notebook_name)
