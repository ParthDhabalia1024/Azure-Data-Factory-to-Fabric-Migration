from __future__ import annotations

import base64
import json
import os
import time
import uuid
from typing import Any, Callable, Optional

import requests
from azure.identity import ClientSecretCredential


def _get_env(name: str, default: Optional[str] = None) -> str:
    v = os.getenv(name, default)
    if v is None or v == "":
        raise EnvironmentError(f"Missing required environment variable: {name}")
    return v


def build_service_principal_credential(
    tenant_id: Optional[str] = None,
    client_id: Optional[str] = None,
    client_secret: Optional[str] = None,
) -> ClientSecretCredential:
    tid = tenant_id or _get_env("AZURE_TENANT_ID")
    cid = client_id or _get_env("AZURE_CLIENT_ID")
    csec = client_secret or _get_env("AZURE_CLIENT_SECRET")
    return ClientSecretCredential(tenant_id=tid, client_id=cid, client_secret=csec)


def get_fabric_token(credential: Optional[ClientSecretCredential] = None) -> str:
    cred = credential or build_service_principal_credential()
    return cred.get_token("https://api.fabric.microsoft.com/.default").token


def _auth_headers(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def _get_json_or_text(resp: requests.Response) -> Any:
    if not resp.text:
        return {}
    try:
        return resp.json()
    except ValueError:
        return resp.text


def _get(token: str, url: str, timeout: int = 60) -> Any:
    r = requests.get(url, headers=_auth_headers(token), timeout=timeout)
    if r.status_code in (200, 201):
        data = _get_json_or_text(r)
        return data or {}
    body = (r.text or "").strip()
    raise RuntimeError(
        "Fabric API request failed. "
        f"status={r.status_code} url={url} "
        f"response={body[:2000]}"
    )


def _get_with_lro(token: str, url: str, timeout_seconds: int = 1800) -> dict[str, Any]:
    r = requests.get(url, headers=_auth_headers(token), timeout=180)
    if r.status_code in (200, 201):
        if not r.text:
            return {}
        data = r.json()
        return data or {}
    if r.status_code == 202:
        location = r.headers.get("Location") or r.headers.get("location")
        if not location:
            raise RuntimeError(f"Fabric returned 202 without Location header. Response: {r.text}")
        _poll_fabric_operation(token, location, timeout_seconds=timeout_seconds)
        # After completion, retry the GET once to retrieve the result.
        r2 = requests.get(url, headers=_auth_headers(token), timeout=180)
        if r2.status_code in (200, 201):
            if not r2.text:
                return {}
            data = r2.json()
            return data or {}
        body = (r2.text or "").strip()
        raise RuntimeError(
            "Fabric API request failed. "
            f"status={r2.status_code} url={url} "
            f"response={body[:2000]}"
        )

    body = (r.text or "").strip()
    raise RuntimeError(
        "Fabric API request failed. "
        f"status={r.status_code} url={url} "
        f"response={body[:2000]}"
    )


def list_connections(
    credential: Optional[ClientSecretCredential] = None,
    max_pages: int = 10,
) -> list[dict[str, Any]]:
    token = get_fabric_token(credential)
    url = "https://api.fabric.microsoft.com/v1/connections"
    items: list[dict[str, Any]] = []

    for _ in range(max_pages):
        data = _get(token, url)
        if isinstance(data, dict) and isinstance(data.get("value"), list):
            items.extend([x for x in data["value"] if isinstance(x, dict)])
            next_url = data.get("continuationUri")
            if not next_url:
                break
            url = next_url
            continue
        break

    return items


def find_connection_by_display_name(
    display_name: str,
    credential: Optional[ClientSecretCredential] = None,
) -> Optional[dict[str, Any]]:
    for c in list_connections(credential=credential):
        if c.get("displayName") == display_name:
            return c
    return None


def get_copy_job_definition(
    workspace_id: str,
    copyjob_id: str,
    token: str,
) -> dict[str, Any]:
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/copyJobs/{copyjob_id}/getDefinition"
    return _get_with_lro(token, url, timeout_seconds=300)


def _extract_platform_part(defn: dict[str, Any]) -> Optional[dict[str, Any]]:
    parts = (((defn or {}).get("definition") or {}).get("parts"))
    if isinstance(parts, list):
        for p in parts:
            if isinstance(p, dict) and p.get("path") == ".platform":
                return p
    return None


def _extract_content_part(defn: dict[str, Any]) -> Optional[dict[str, Any]]:
    parts = (((defn or {}).get("definition") or {}).get("parts"))
    if isinstance(parts, list):
        for p in parts:
            if isinstance(p, dict) and p.get("path") == "copyjob-content.json":
                return p
    return None


def _b64_decode_json(payload_b64: str) -> dict[str, Any]:
    raw = base64.b64decode(payload_b64)
    obj = json.loads(raw.decode("utf-8"))
    return obj if isinstance(obj, dict) else {}


def _try_get_existing_copyjob_content(token: str, workspace_id: str, copyjob_id: str) -> Optional[dict[str, Any]]:
    try:
        defn = get_copy_job_definition(workspace_id, copyjob_id, token)
        part = _extract_content_part(defn)
        if not part:
            return None
        if part.get("payloadType") != "InlineBase64":
            return None
        payload = part.get("payload")
        if not isinstance(payload, str) or not payload:
            return None
        return _b64_decode_json(payload)
    except Exception:
        return None


def _build_copyjob_content_from_template(
    template: Optional[dict[str, Any]],
    activities: list[dict[str, Any]],
    fallback_properties: dict[str, Any],
) -> dict[str, Any]:
    if isinstance(template, dict) and template:
        content = json.loads(json.dumps(template))
    else:
        content = {}
    props = content.get("properties")
    if not isinstance(props, dict):
        props = {}
        content["properties"] = props

    for k, v in fallback_properties.items():
        props.setdefault(k, v)

    content["activities"] = activities
    return content


def list_copy_jobs(
    workspace_id: str,
    credential: Optional[ClientSecretCredential] = None,
) -> list[dict[str, Any]]:
    token = get_fabric_token(credential)
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/copyJobs"
    data = _get(token, url)
    if isinstance(data, dict) and isinstance(data.get("value"), list):
        return data["value"]
    if isinstance(data, list):
        return data
    return []


def find_copy_job_by_display_name(
    workspace_id: str,
    display_name: str,
    credential: Optional[ClientSecretCredential] = None,
) -> Optional[dict[str, Any]]:
    for it in list_copy_jobs(workspace_id, credential=credential):
        if isinstance(it, dict) and it.get("displayName") == display_name:
            return it
    return None


def _update_copyjob_definition_with_retry(
    token: str,
    workspace_id: str,
    copyjob_id: str,
    content: dict[str, Any],
    max_attempts: int = 2,
    per_attempt_lro_timeout_seconds: int = 120,
    max_total_seconds: int = 420,
    progress_callback: Optional[Callable[[str], None]] = None,
) -> None:
    base_url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/copyJobs"
    update_url_copyjobs = f"{base_url}/{copyjob_id}/updateDefinition"
    update_url_items = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{copyjob_id}/updateDefinition"

    def emit(message: str) -> None:
        # Always print to terminal for visibility; also send to UI if callback is provided
        try:
            print(message, flush=True)
        except Exception:
            pass
        if progress_callback:
            try:
                progress_callback(message)
            except Exception:
                pass

    # Build incremental candidates: minimal -> add source/destination -> full (with activities)
    def _make_payload(obj: dict[str, Any]) -> dict[str, Any]:
        return {
            "definition": {
                "parts": [
                    {
                        "path": "copyjob-content.json",
                        "payload": _b64_json(obj),
                        "payloadType": "InlineBase64",
                    }
                ]
            }
        }

    # Try to get existing template content to preserve required schema fields
    template: Optional[dict[str, Any]] = None
    try:
        defn = get_copy_job_definition(workspace_id, copyjob_id, token)
        part = _extract_content_part(defn)
        if part and part.get("payloadType") == "InlineBase64" and isinstance(part.get("payload"), str):
            template = _b64_decode_json(part.get("payload"))
    except Exception:
        template = None

    def _copy(d: Optional[dict[str, Any]]) -> dict[str, Any]:
        return json.loads(json.dumps(d)) if isinstance(d, dict) else {}

    props_in = (content or {}).get("properties") or {}

    # Stage 1: minimal (jobMode only, keep other template scaffolding if any)
    minimal = _copy(template)
    minimal_props = minimal.get("properties") if isinstance(minimal.get("properties"), dict) else {}
    if not isinstance(minimal_props, dict):
        minimal_props = {}
    minimal["properties"] = minimal_props
    minimal_props["jobMode"] = props_in.get("jobMode", "Batch")
    minimal["activities"] = []

    # Stage 3a: minimal activities only (strip optional translator/typeConversion/writeBehavior)
    def _strip_activity_options(obj: dict[str, Any]) -> dict[str, Any]:
        o = _copy(obj)
        acts = o.get("activities")
        if isinstance(acts, list):
            new_acts = []
            for a in acts:
                if not isinstance(a, dict):
                    continue
                na = _copy(a)
                props = na.get("properties") if isinstance(na.get("properties"), dict) else {}
                if not isinstance(props, dict):
                    props = {}
                # Keep type + datasetSettings (and translator/typeConversion) for source/destination
                src = props.get("source") if isinstance(props.get("source"), dict) else {}
                dst = props.get("destination") if isinstance(props.get("destination"), dict) else {}
                src_ds = (src.get("datasetSettings") if isinstance(src.get("datasetSettings"), dict) else {})
                dst_ds = (dst.get("datasetSettings") if isinstance(dst.get("datasetSettings"), dict) else {})
                # Normalize destination table to unqualified name (Warehouse tables typically don't include schema here)
                dst_table = dst_ds.get("table")
                if isinstance(dst_table, str) and "." in dst_table:
                    parts_tbl = dst_table.split(".")
                    if parts_tbl:
                        dst_table_norm = parts_tbl[-1]
                        try:
                            print(f"   normalizing destination table '{dst_table}' -> '{dst_table_norm}'", flush=True)
                        except Exception:
                            pass
                        dst_table = dst_table_norm
                props = {
                    "source": {
                        "type": src.get("type", "SqlTable"),
                        "datasetSettings": {"schema": src_ds.get("schema"), "table": src_ds.get("table")},
                    },
                    "destination": {
                        "type": dst.get("type", "WarehouseTable"),
                        "datasetSettings": {"schema": dst_ds.get("schema"), "table": dst_table},
                    },
                }
                if "writeBehavior" in dst:
                    props["destination"]["writeBehavior"] = dst.get("writeBehavior")
                if "tableOption" in dst:
                    props["destination"]["tableOption"] = dst.get("tableOption")
                if "translator" in props or "translator" in na.get("properties", {}):
                    props["translator"] = props.get("translator") or na.get("properties", {}).get("translator")
                if "typeConversionSettings" in props or "typeConversionSettings" in na.get("properties", {}):
                    props["typeConversionSettings"] = props.get("typeConversionSettings") or na.get("properties", {}).get("typeConversionSettings")
                na["properties"] = props
                new_acts.append(na)
            o["activities"] = new_acts
        return o

    full_min = _strip_activity_options(content or {})

    # Stage 3b: full content (as requested)
    full = content or {}

    # Try minimal definition -> typed minimal activities (no translator/typeConversion) -> full activities
    candidates = [minimal, full_min, full]

    def _describe_activities(obj: dict[str, Any]) -> None:
        acts = obj.get("activities") if isinstance(obj.get("activities"), list) else []
        try:
            tables = []
            for a in acts:
                if not isinstance(a, dict):
                    continue
                props = a.get("properties", {}) if isinstance(a.get("properties"), dict) else {}
                src = props.get("source", {}) if isinstance(props.get("source"), dict) else {}
                dst = props.get("destination", {}) if isinstance(props.get("destination"), dict) else {}
                src_tbl = (src.get("datasetSettings", {}) or {}).get("table") if isinstance(src.get("datasetSettings"), dict) else None
                dst_tbl = (dst.get("datasetSettings", {}) or {}).get("table") if isinstance(dst.get("datasetSettings"), dict) else None
                tables.append(f"src={src_tbl}, dst={dst_tbl}")
            emit(f"   activities count={len(acts)} tables={tables}")
            if acts:
                first_act = json.dumps(acts[0], ensure_ascii=False)
                if len(first_act) > 800:
                    first_act = first_act[:800] + "...<truncated>"
                emit(f"   first activity payload: {first_act}")
        except Exception:
            pass

    deadline = time.time() + max_total_seconds

    delay = 3
    last_err: Optional[Exception] = None
    for attempt in range(1, max_attempts + 1):
        if time.time() > deadline:
            break
        try:
            emit(f"Updating CopyJob definition (attempt {attempt}/{max_attempts})...")

            # Apply all stages sequentially; stop and retry later if any stage fails
            for idx, obj in enumerate(candidates, start=1):
                if idx == 2:
                    props_dbg = obj.get("properties", {}) if isinstance(obj.get("properties"), dict) else {}
                    src_dbg = props_dbg.get("source", {}) if isinstance(props_dbg.get("source"), dict) else {}
                    dst_dbg = props_dbg.get("destination", {}) if isinstance(props_dbg.get("destination"), dict) else {}
                    src_conn = ""
                    try:
                        src_conn_settings = src_dbg.get("connectionSettings", {}) if isinstance(src_dbg.get("connectionSettings"), dict) else {}
                        ext_refs = src_conn_settings.get("externalReferences", {}) if isinstance(src_conn_settings.get("externalReferences"), dict) else {}
                        src_conn = ext_refs.get("connection") or ext_refs.get("connectionId") or ""
                    except Exception:
                        src_conn = ""
                    dst_conn = ""
                    try:
                        dst_conn_settings = dst_dbg.get("connectionSettings", {}) if isinstance(dst_dbg.get("connectionSettings"), dict) else {}
                        dst_props = dst_conn_settings.get("typeProperties", {}) if isinstance(dst_conn_settings.get("typeProperties"), dict) else {}
                        dst_ext = dst_conn_settings.get("externalReferences", {}) if isinstance(dst_conn_settings.get("externalReferences"), dict) else {}
                        dst_conn = (
                            f"workspaceId={dst_props.get('workspaceId')}, artifactId={dst_props.get('artifactId')}, rootFolder={dst_props.get('rootFolder')}; "
                            f"extRefs.warehouse={dst_ext.get('warehouse') or dst_ext.get('warehouseId')}"
                        )
                    except Exception:
                        dst_conn = ""
                    emit(
                        "   stage 2 props: "
                        f"jobMode={props_dbg.get('jobMode')}; "
                        f"source.type={src_dbg.get('type')}, connSettings.type={(src_dbg.get('connectionSettings') or {}).get('type') if isinstance(src_dbg.get('connectionSettings'), dict) else None}, connection(len)={len(src_conn)}; "
                        f"dest.type={dst_dbg.get('type')}, connSettings.type={(dst_dbg.get('connectionSettings') or {}).get('type') if isinstance(dst_dbg.get('connectionSettings'), dict) else None}, {dst_conn}"
                    )
                    _describe_activities(obj)
                elif idx > 2:
                    _describe_activities(obj)
                try:
                    preview = json.dumps(obj, ensure_ascii=False)
                    if len(preview) > 1200:
                        preview = preview[:1200] + "...<truncated>"
                    emit(f"   payload preview: {preview}")
                except Exception:
                    pass
                payload = _make_payload(obj)
                last_err_inner: Optional[Exception] = None
                # Try copyJobs endpoint first
                try:
                    emit(f" - trying payload stage {idx}/{len(candidates)} (activities={len(obj.get('activities', []))}) via copyJobs endpoint")
                    _post_with_lro(
                        token,
                        update_url_copyjobs,
                        payload,
                        timeout_seconds=per_attempt_lro_timeout_seconds,
                    )
                    emit(f"   stage {idx} succeeded via copyJobs endpoint")
                    continue  # proceed to next stage
                except Exception as e1:
                    last_err_inner = e1
                    emit(f"   copyJobs updateDefinition failed at stage {idx}: {e1}")
                # Fallback to items endpoint
                try:
                    emit(f"   trying items endpoint at stage {idx}")
                    _post_with_lro(
                        token,
                        update_url_items,
                        payload,
                        timeout_seconds=per_attempt_lro_timeout_seconds,
                    )
                    emit(f"   stage {idx} succeeded via items endpoint")
                    continue
                except Exception as e2:
                    last_err_inner = e2
                    emit(f"   items updateDefinition failed at stage {idx}: {e2}")
                # If stage is the first with activities, try incremental per-activity to isolate offending table
                acts = (obj or {}).get("activities")
                if isinstance(acts, list) and len(acts) > 0:
                    emit(f"   attempting per-activity incremental apply at stage {idx}...")
                    for n in range(1, len(acts) + 1):
                        partial_obj = _copy(obj)
                        partial_obj["activities"] = acts[:n]
                        partial_payload = _make_payload(partial_obj)
                        try:
                            emit(f"     - trying first {n}/{len(acts)} activities via copyJobs endpoint")
                            _post_with_lro(
                                token,
                                update_url_copyjobs,
                                partial_payload,
                                timeout_seconds=per_attempt_lro_timeout_seconds,
                            )
                            emit(f"       succeeded for {n}/{len(acts)} via copyJobs")
                            continue
                        except Exception as e3:
                            emit(f"       copyJobs failed at activity index {n}: {e3}")
                            try:
                                emit(f"       trying items endpoint for first {n} activities")
                                _post_with_lro(
                                    token,
                                    update_url_items,
                                    partial_payload,
                                    timeout_seconds=per_attempt_lro_timeout_seconds,
                                )
                                emit(f"       succeeded for {n}/{len(acts)} via items")
                                continue
                            except Exception as e4:
                                emit(f"       items failed at activity index {n}: {e4}")
                                # Found the offending activity; abort stage so outer retry can act
                                raise e4
                # If both endpoints failed for this stage, raise to outer retry loop
                raise last_err_inner if last_err_inner else RuntimeError("Unknown updateDefinition failure")

            # All stages succeeded
            return
        except Exception as exc:
            last_err = exc
            emit(f"UpdateDefinition failed: {exc}")
            if time.time() + delay > deadline:
                break
            emit(f"Waiting {delay}s before retry...")
            time.sleep(delay)
            delay = min(delay * 2, 20)
    raise RuntimeError(
        "Failed to update CopyJob definition after retries. "
        f"copyJobId={copyjob_id} lastError={last_err}"
    )

def _poll_fabric_operation(token: str, location_url: str, timeout_seconds: int = 1800) -> dict[str, Any]:
    deadline = time.time() + timeout_seconds
    while True:
        if time.time() > deadline:
            raise TimeoutError(f"Timed out waiting for Fabric operation: {location_url}")
        r = requests.get(location_url, headers=_auth_headers(token), timeout=60)
        r.raise_for_status()
        data = r.json() if r.text else {}
        status = (data or {}).get("status") or (data or {}).get("state")
        if isinstance(status, str) and status.lower() in ("succeeded", "failed", "cancelled"):
            if status.lower() != "succeeded":
                raise RuntimeError(f"Fabric operation did not succeed: {data}")
            return data
        ra = r.headers.get("Retry-After")
        delay = int(ra) if ra and ra.isdigit() else 5
        time.sleep(delay)


def _post_with_lro(token: str, url: str, payload: dict[str, Any], timeout_seconds: int = 1800) -> dict[str, Any]:
    r = requests.post(url, headers=_auth_headers(token), json=payload, timeout=180)
    if r.status_code in (200, 201):
        if not r.text:
            return {}
        data = r.json()
        return data or {}
    if r.status_code == 202:
        location = r.headers.get("Location") or r.headers.get("location")
        if not location:
            raise RuntimeError(f"Fabric returned 202 without Location header. Response: {r.text}")
        _poll_fabric_operation(token, location, timeout_seconds=timeout_seconds)
        if not r.text:
            return {}
        data = r.json()
        return data or {}

    # Provide more context than requests' default HTTPError (helps with 401/403 troubleshooting)
    body = (r.text or "").strip()
    raise RuntimeError(
        "Fabric API request failed. "
        f"status={r.status_code} url={url} "
        f"response={body[:2000]}"
    )


def create_warehouse(
    workspace_id: str,
    display_name: str,
    description: str = "",
    collation_type: Optional[str] = None,
    credential: Optional[ClientSecretCredential] = None,
) -> dict[str, Any]:
    token = get_fabric_token(credential)
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/warehouses"
    payload: dict[str, Any] = {"displayName": display_name}
    if description:
        payload["description"] = description
    if collation_type:
        payload["creationPayload"] = {"collationType": collation_type}
    return _post_with_lro(token, url, payload)


def list_warehouses(
    workspace_id: str,
    credential: Optional[ClientSecretCredential] = None,
) -> list[dict[str, Any]]:
    token = get_fabric_token(credential)
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/warehouses"
    data = _get(token, url)
    if isinstance(data, dict) and isinstance(data.get("value"), list):
        return data["value"]
    if isinstance(data, list):
        return data
    return []


def find_warehouse_by_display_name(
    workspace_id: str,
    display_name: str,
    credential: Optional[ClientSecretCredential] = None,
) -> Optional[dict[str, Any]]:
    items = list_warehouses(workspace_id, credential=credential)
    for it in items:
        if isinstance(it, dict) and it.get("displayName") == display_name:
            return it
    return None


def create_or_get_warehouse(
    workspace_id: str,
    display_name: str,
    description: str = "",
    collation_type: Optional[str] = None,
    credential: Optional[ClientSecretCredential] = None,
    max_attempts: int = 8,
) -> dict[str, Any]:
    existing = find_warehouse_by_display_name(workspace_id, display_name, credential=credential)
    if existing:
        return {**existing, "_reused": True}

    delay = 15
    last_err: Optional[Exception] = None
    for _ in range(max_attempts):
        try:
            created = create_warehouse(
                workspace_id=workspace_id,
                display_name=display_name,
                description=description,
                collation_type=collation_type,
                credential=credential,
            )
            created = created or {}
            if not isinstance(created, dict):
                raise RuntimeError(f"Warehouse create returned unexpected type: {type(created)}")

            created["_reused"] = False

            # Fabric may return 201/202 with an empty body; resolve the actual created warehouse by name.
            if created.get("id") or created.get("warehouseId"):
                return created

            for _ in range(20):  # up to ~2 minutes
                found = find_warehouse_by_display_name(workspace_id, display_name, credential=credential)
                if found and (found.get("id") or found.get("warehouseId")):
                    return {**found, "_reused": False}
                time.sleep(6)

            raise RuntimeError(f"Warehouse create response missing id: {created}")
        except Exception as exc:
            last_err = exc
            msg = str(exc)
            if "ItemDisplayNameNotAvailableYet" in msg:
                time.sleep(delay)
                delay = min(delay * 2, 120)
                continue
            if "ItemDisplayNameAlreadyInUse" in msg:
                ex2 = find_warehouse_by_display_name(workspace_id, display_name, credential=credential)
                if ex2:
                    return {**ex2, "_reused": True}
            raise
    raise RuntimeError(f"Failed to create warehouse after retries: {last_err}")


def create_synapse_connection_service_principal(
    display_name: str,
    server: str,
    database: str,
    tenant_id: str,
    client_id: str,
    client_secret: str,
    credential: Optional[ClientSecretCredential] = None,
    skip_test_connection: bool = False,
) -> dict[str, Any]:
    token = get_fabric_token(credential)
    url = "https://api.fabric.microsoft.com/v1/connections"
    payload: dict[str, Any] = {
        "connectivityType": "ShareableCloud",
        "displayName": display_name,
        "connectionDetails": {
            "type": "SQL",
            "creationMethod": "SQL",
            "parameters": [
                {"dataType": "Text", "name": "server", "value": server},
                {"dataType": "Text", "name": "database", "value": database},
            ],
        },
        "privacyLevel": "Organizational",
        "credentialDetails": {
            "singleSignOnType": "None",
            "connectionEncryption": "NotEncrypted",
            "credentials": {
                "credentialType": "ServicePrincipal",
                "tenantId": tenant_id,
                "clientId": client_id,
                "servicePrincipalTenantId": tenant_id,
                "servicePrincipalClientId": client_id,
                "servicePrincipalSecret": client_secret,
            },
        },
    }
    return _post_with_lro(token, url, payload)


def create_or_get_synapse_connection_service_principal(
    display_name: str,
    server: str,
    database: str,
    tenant_id: str,
    client_id: str,
    client_secret: str,
    credential: Optional[ClientSecretCredential] = None,
    existing_connection_id: Optional[str] = None,
) -> dict[str, Any]:
    if existing_connection_id:
        return {"id": existing_connection_id, "displayName": display_name, "_reused": True}
    existing = find_connection_by_display_name(display_name, credential=credential)
    if existing and existing.get("id"):
        return {**existing, "_reused": True}
    try:
        created = create_synapse_connection_service_principal(
            display_name=display_name,
            server=server,
            database=database,
            tenant_id=tenant_id,
            client_id=client_id,
            client_secret=client_secret,
            credential=credential,
        )
        created = created or {}
        if isinstance(created, dict):
            created["_reused"] = False
        return created
    except Exception as exc:
        msg = str(exc)
        if "DuplicateConnectionName" in msg or "status=409" in msg:
            ex2 = find_connection_by_display_name(display_name, credential=credential)
            if ex2 and ex2.get("id"):
                return {**ex2, "_reused": True}
        raise


def _b64_json(obj: Any) -> str:
    return base64.b64encode(json.dumps(obj, ensure_ascii=False).encode("utf-8")).decode("ascii")


def list_synapse_tables_service_principal(
    server: str,
    database: str,
    tenant_id: str,
    client_id: str,
    client_secret: str,
    schema: Optional[str] = None,
) -> list[str]:
    import pyodbc

    parts = [
        "DRIVER={ODBC Driver 18 for SQL Server};",
        f"SERVER={server};",
        f"DATABASE={database};",
        "Encrypt=yes;",
        "TrustServerCertificate=no;",
        "Authentication=ActiveDirectoryServicePrincipal;",
        f"UID={client_id};",
        f"PWD={client_secret};",
    ]
    conn_str = "".join(parts)

    where = "WHERE TABLE_TYPE = 'BASE TABLE'"
    params: list[Any] = []
    if schema:
        where += " AND TABLE_SCHEMA = ?"
        params.append(schema)

    sql = (
        "SELECT TABLE_SCHEMA, TABLE_NAME "
        "FROM INFORMATION_SCHEMA.TABLES "
        f"{where} "
        "ORDER BY TABLE_SCHEMA, TABLE_NAME"
    )

    rows: list[str] = []
    with pyodbc.connect(conn_str, timeout=30) as conn:
        cur = conn.cursor()
        cur.execute(sql, params)
        for s, t in cur.fetchall():
            rows.append(f"{s}.{t}")
    return rows


def create_copy_job_synapse_tables_to_warehouse(
    workspace_id: str,
    display_name: str,
    source_connection_id: str,
    source_tables: list[str],
    destination_warehouse_id: str,
    destination_endpoint: Optional[str] = None,
    *,
    source_database: Optional[str] = None,
    use_existing_template: bool = False,
    credential: Optional[ClientSecretCredential] = None,
    progress_callback: Optional[Callable[[str], None]] = None,
) -> dict[str, Any]:
    token = get_fabric_token(credential)
    base_url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/copyJobs"

    try:
        print(
            "[debug] create_copy_job inputs",
            {
                "workspace_id": workspace_id,
                "display_name": display_name,
                "source_connection_id": source_connection_id,
                "source_tables": source_tables,
                "destination_warehouse_id": destination_warehouse_id,
                "destination_endpoint": destination_endpoint,
                "source_database": source_database,
                "use_existing_template": use_existing_template,
            },
            flush=True,
        )
    except Exception:
        pass

    activities: list[dict[str, Any]] = []
    for tbl in source_tables:
        act_id = str(uuid.uuid4())
        src_schema = None
        src_table = tbl
        if isinstance(tbl, str) and "." in tbl:
            parts = tbl.split(".")
            if len(parts) >= 2:
                src_schema = parts[0]
                src_table = parts[-1]
        if not src_schema:
            src_schema = "dbo"
        dst_schema = src_schema
        dst_table = src_table
        activities.append(
            {
                "id": act_id,
                "properties": {
                    "source": {
                        "type": "AzureSqlDWTable",
                        "datasetSettings": {"schema": src_schema, "table": src_table},
                        "partitionOption": "None",
                    },
                    "destination": {
                        "type": "DataWarehouseTable",
                        "datasetSettings": {"schema": dst_schema, "table": dst_table},
                        "tableOption": "autoCreate",
                    },
                    "enableStaging": True,
                    "translator": {"type": "TabularTranslator"},
                    "typeConversionSettings": {
                        "typeConversion": {
                            "allowDataTruncation": True,
                            "treatBooleanAsNumber": False,
                        }
                    },
                },
            }
        )

    fallback_properties: dict[str, Any] = {
        "jobMode": "Batch",
        "source": {
            "type": "AzureSqlDWTable",
            "connectionSettings": {
                "type": "AzureSqlDW",
                "typeProperties": {"database": source_database},
                "externalReferences": {"connection": source_connection_id},
            },
        },
        "destination": {
            "type": "DataWarehouseTable",
            "connectionSettings": {
                "type": "DataWarehouse",
                "typeProperties": {
                    "workspaceId": workspace_id,
                    "artifactId": destination_warehouse_id,
                    **({"endpoint": destination_endpoint} if destination_endpoint else {}),
                },
            },
        },
        "policy": {"timeout": "0.12:00:00"},
    }

    # Step 1: create the CopyJob item WITHOUT definition (avoids .platform validation issues)
    created: dict[str, Any]
    copyjob_id: Optional[str] = None
    try:
        created = _post_with_lro(token, base_url, {"displayName": display_name})
        created = created or {}
        if not isinstance(created, dict):
            raise RuntimeError(f"CopyJob create returned unexpected type: {type(created)}")
        copyjob_id = created.get("id")
        if not copyjob_id:
            # Fabric may create successfully but return an empty body; resolve by display name.
            for _ in range(20):
                found = find_copy_job_by_display_name(workspace_id, display_name, credential=credential)
                if found and found.get("id"):
                    created = found
                    copyjob_id = found.get("id")
                    break
                time.sleep(3)
    except Exception as exc:
        msg = str(exc)
        if "ItemDisplayNameAlreadyInUse" in msg:
            found = find_copy_job_by_display_name(workspace_id, display_name, credential=credential)
            if found and found.get("id"):
                created = {**found, "_reused": True}
                copyjob_id = found.get("id")
            else:
                raise
        else:
            raise

    if not copyjob_id:
        raise RuntimeError(f"CopyJob create response missing id: {created}")

    template_content = _try_get_existing_copyjob_content(token, workspace_id, copyjob_id) if use_existing_template else None
    content = _build_copyjob_content_from_template(template_content, activities, fallback_properties)

    _update_copyjob_definition_with_retry(
        token,
        workspace_id,
        copyjob_id,
        content,
        progress_callback=progress_callback,
    )

    if "_reused" not in created:
        created["_reused"] = False
    return {**created, "definitionUpdated": True}


def create_copy_job_synapse_to_warehouse(
    workspace_id: str,
    display_name: str,
    source_connection_id: str,
    source_table_or_query: str,
    destination_warehouse_id: str,
    destination_table: str,
    credential: Optional[ClientSecretCredential] = None,
    progress_callback: Optional[Callable[[str], None]] = None,
) -> dict[str, Any]:
    token = get_fabric_token(credential)
    base_url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/copyJobs"

    content = {
        "properties": {
            "jobMode": "Batch",
            "source": {
                "type": "SQL",
                "connectionSettings": {
                    "type": "SQL",
                    "externalReferences": {"connection": source_connection_id},
                },
            },
            "destination": {
                "type": "WarehouseTable",
                "connectionSettings": {
                    "type": "Warehouse",
                    "typeProperties": {
                        "workspaceId": workspace_id,
                        "artifactId": destination_warehouse_id,
                        "rootFolder": "Tables",
                    },
                },
            },
            "policy": {"timeout": "0.12:00:00"},
        },
        "activities": [
            {
                "id": "00000000-0000-0000-0000-000000000001",
                "properties": {
                    "source": {"datasetSettings": {"table": source_table_or_query}},
                    "destination": {
                        "writeBehavior": "Append",
                        "datasetSettings": {"table": destination_table},
                    },
                    "translator": {"type": "TabularTranslator"},
                    "typeConversionSettings": {
                        "typeConversion": {"allowDataTruncation": True, "treatBooleanAsNumber": False}
                    },
                },
            }
        ],
    }

    created = _post_with_lro(token, base_url, {"displayName": display_name})
    if not isinstance(created, dict):
        raise RuntimeError(f"CopyJob create returned unexpected type: {type(created)}")
    copyjob_id = created.get("id")
    if not copyjob_id:
        raise RuntimeError(f"CopyJob create response missing id: {created}")

    _update_copyjob_definition_with_retry(
        token,
        workspace_id,
        copyjob_id,
        content,
        progress_callback=progress_callback,
    )

    return {**created, "definitionUpdated": True}


def create_warehouse_and_copy_job_from_env() -> dict[str, Any]:
    workspace_id = _get_env("FABRIC_WORKSPACE_ID")

    warehouse_name = _get_env("FABRIC_WAREHOUSE_NAME", "SynapseWarehouse")
    warehouse_description = os.getenv("FABRIC_WAREHOUSE_DESCRIPTION", "")

    copyjob_name = _get_env("FABRIC_COPYJOB_NAME", "SynapseToWarehouseCopyJob")

    syn_server = _get_env("SYNAPSE_SERVER")
    syn_database = _get_env("SYNAPSE_DATABASE")
    syn_source_table = _get_env("SYNAPSE_SOURCE_TABLE")

    tenant_id = _get_env("AZURE_TENANT_ID")
    client_id = _get_env("AZURE_CLIENT_ID")
    client_secret = _get_env("AZURE_CLIENT_SECRET")

    cred = build_service_principal_credential(tenant_id, client_id, client_secret)

    wh = create_warehouse(workspace_id, warehouse_name, description=warehouse_description, credential=cred)
    warehouse_id = wh.get("id") or wh.get("warehouseId")
    if not warehouse_id:
        raise RuntimeError(f"Warehouse create response missing id: {wh}")

    conn = create_synapse_connection_service_principal(
        display_name=f"{syn_server};{syn_database}",
        server=syn_server,
        database=syn_database,
        tenant_id=tenant_id,
        client_id=client_id,
        client_secret=client_secret,
        credential=cred,
    )
    conn_id = conn.get("id")
    if not conn_id:
        raise RuntimeError(f"Connection create response missing id: {conn}")

    cj = create_copy_job_synapse_to_warehouse(
        workspace_id=workspace_id,
        display_name=copyjob_name,
        source_connection_id=conn_id,
        source_table_or_query=syn_source_table,
        destination_warehouse_id=warehouse_id,
        destination_table=syn_source_table,
        credential=cred,
    )

    return {
        "warehouse": wh,
        "connection": conn,
        "copyJob": cj,
    }
