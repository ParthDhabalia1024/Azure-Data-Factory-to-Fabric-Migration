import json
import re
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple, Union

import streamlit as st
from azure.identity import InteractiveBrowserCredential
from azure.mgmt.resource import SubscriptionClient
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.storage import StorageManagementClient
from azure.mgmt.sql import SqlManagementClient
from azure.storage.blob import BlobServiceClient
from azure.storage.filedatalake import DataLakeServiceClient


def _to_dict(obj: Any) -> Dict[str, Any]:
    if obj is None:
        return {}
    if isinstance(obj, dict):
        return obj
    if hasattr(obj, "as_dict"):
        try:
            return obj.as_dict()  # type: ignore[attr-defined]
        except Exception:
            pass
    # Fallback: best-effort JSON round-trip
    try:
        return json.loads(json.dumps(obj))
    except Exception:
        return {}


# Scoring helpers
CONTROL_ACTIVITY_TYPES = {
    "foreach",
    "until",
    "ifcondition",
    "switch",
    "executepipeline",
}

CONNECTIVITY_COMPLEX_KEYWORDS = {
    "onprem",
    "sqlserver",
    "oracle",
    "db2",
    "informix",
    "odbc",
    "sftp",
    "ftp",
    "sap",
    "private",
    "vnet",
}

def _score_component_parity(total_acts: int, non_migratable: int) -> int:
    if total_acts <= 0 or non_migratable <= 0:
        return 0
    ratio = non_migratable / max(total_acts, 1)
    if non_migratable <= 2 and ratio <= 0.1:
        return 1
    if non_migratable <= 5 and ratio <= 0.3:
        return 2
    return 3

def _score_non_migratable(non_migratable: int) -> int:
    if non_migratable <= 0:
        return 0
    if non_migratable <= 2:
        return 1
    if non_migratable <= 5:
        return 2
    return 3

def _score_connectivity(ls_types: Iterable[str]) -> int:
    types = [t.lower() for t in ls_types if t]
    if not types:
        return 0
    flagged = sum(1 for t in types if any(k in t for k in CONNECTIVITY_COMPLEX_KEYWORDS))
    if flagged == 0:
        return 0
    if flagged <= 1:
        return 1
    if flagged <= 3:
        return 2
    return 3

def _score_orchestration(total_acts: int, control_acts: int) -> int:
    if total_acts <= 5 and control_acts == 0:
        return 0
    if total_acts <= 10 and control_acts <= 1:
        return 1
    if total_acts <= 20 or control_acts <= 3:
        return 2
    return 3

def _extract_dataset_references(activity: Dict[str, Any]) -> Set[str]:
    refs: Set[str] = set()
    for key in ("inputs",):
        items = activity.get(key)
        if isinstance(items, list):
            for item in items:
                if isinstance(item, dict):
                    ref = item.get("referenceName") or item.get("name")
                    if isinstance(ref, str) and ref:
                        refs.add(ref)
    return refs

def _extract_linked_service_reference(dataset: Dict[str, Any]) -> str:
    if not isinstance(dataset, dict):
        return ""

    def _norm(k: Any) -> str:
        try:
            s = str(k)
        except Exception:
            return ""
        s = s.lower()
        import re as _re
        return _re.sub(r"[^a-z]", "", s)

    def _deep_find_lsn(obj: Any) -> Any:
        if isinstance(obj, dict):
            for k, v in obj.items():
                if _norm(k) == "linkedservicename":
                    return v
            for v in obj.values():
                found = _deep_find_lsn(v)
                if found is not None:
                    return found
        elif isinstance(obj, list):
            for it in obj:
                found = _deep_find_lsn(it)
                if found is not None:
                    return found
        return None

    # Prefer direct hit under properties, then fall back to deep search
    props = dataset.get("properties") if isinstance(dataset, dict) else None
    lsn: Any = None
    if isinstance(props, dict):
        for k, v in props.items():
            if _norm(k) == "linkedservicename":
                lsn = v
                break
    if lsn is None:
        lsn = _deep_find_lsn(dataset)

    if isinstance(lsn, dict):
        rn = (
            lsn.get("referenceName")
            or lsn.get("reference_name")
            or lsn.get("name")
        )
        if isinstance(rn, str) and rn.strip():
            return rn.strip()
    elif isinstance(lsn, str) and lsn.strip():
        return lsn.strip()
    return ""
    if isinstance(obj, dict):
        return obj
    if hasattr(obj, "as_dict"):
        return obj.as_dict()
    return json.loads(json.dumps(obj))


def _collect_activity_types(activities: Optional[List[Any]], types: Set[str]) -> None:
    if not activities:
        return
    for act in activities:
        a = _to_dict(act)
        t = a.get("type")
        if t:
            types.add(t)
        inner_lists: List[List[Any]] = []
        for key in (
            "activities",
            "ifTrueActivities",
            "ifFalseActivities",
            "defaultActivities",
            "innerActivities",
            "caseActivities",
        ):
            v = a.get(key)
            if isinstance(v, list):
                inner_lists.append(v)
        cases = a.get("cases")
        if isinstance(cases, list):
            for c in cases:
                cd = _to_dict(c)
                if isinstance(cd.get("activities"), list):
                    inner_lists.append(cd.get("activities"))
        for lst in inner_lists:
            _collect_activity_types(lst, types)


@st.cache_data(show_spinner=False)
def list_subscriptions(_credential: InteractiveBrowserCredential) -> List[Tuple[str, str]]:
    client = SubscriptionClient(_credential)
    subs = list(client.subscriptions.list())
    return [(s.display_name or s.subscription_id, s.subscription_id) for s in subs]


@st.cache_data(show_spinner=False)
def list_resource_groups(_credential: InteractiveBrowserCredential, subscription_id: str) -> List[str]:
    rg_client = ResourceManagementClient(_credential, subscription_id)
    return [rg.name for rg in rg_client.resource_groups.list()]


@st.cache_data(show_spinner=False)
def list_data_factories(_credential: InteractiveBrowserCredential, subscription_id: str, resource_group: str) -> List[str]:
    adf_client = DataFactoryManagementClient(_credential, subscription_id)
    return [f.name for f in adf_client.factories.list_by_resource_group(resource_group)]


@st.cache_data(show_spinner=False)
def list_rg_resources(_credential: InteractiveBrowserCredential, subscription_id: str, resource_group: str) -> List[Dict[str, str]]:
    rg_client = ResourceManagementClient(_credential, subscription_id)
    rows: List[Dict[str, str]] = []
    for res in rg_client.resources.list_by_resource_group(resource_group):
        d = _to_dict(res)
        full_type = d.get("type") or getattr(res, "type", "")
        rows.append({
            "Type": _friendly_resource_type(full_type),
            "Name": d.get("name") or getattr(res, "name", ""),
        })
    return rows


@st.cache_data(show_spinner=False)
def list_storage_accounts(
    _credential: InteractiveBrowserCredential,
    subscription_id: str,
    resource_group: str,
) -> List[str]:
    smc = StorageManagementClient(_credential, subscription_id)
    return [sa.name for sa in smc.storage_accounts.list_by_resource_group(resource_group)]


@st.cache_data(show_spinner=False)
def list_sql_servers(
    _credential: InteractiveBrowserCredential,
    subscription_id: str,
    resource_group: str,
) -> List[str]:
    """List Azure SQL logical servers in a resource group."""
    client = SqlManagementClient(_credential, subscription_id)
    return [srv.name for srv in client.servers.list_by_resource_group(resource_group)]


@st.cache_data(show_spinner=False)
def list_sql_databases_for_server(
    _credential: InteractiveBrowserCredential,
    subscription_id: str,
    resource_group: str,
    server_name: str,
) -> List[Dict[str, Any]]:
    """Return basic metadata for databases on a given Azure SQL server."""
    client = SqlManagementClient(_credential, subscription_id)
    rows: List[Dict[str, Any]] = []
    for db in client.databases.list_by_server(resource_group_name=resource_group, server_name=server_name):
        d = _to_dict(db)
        sku = d.get("sku") or {}
        max_bytes = d.get("max_size_bytes") or d.get("maxSizeBytes")
        rows.append(
            {
                "Database": d.get("name") or getattr(db, "name", ""),
                "Status": d.get("status", ""),
                "Tier": (sku.get("tier") or sku.get("name") or ""),
                "MaxSizeGB": round((max_bytes or 0) / (1024 ** 3), 2) if max_bytes else None,
                "Collation": d.get("collation", ""),
                "ReadScale": d.get("read_scale") or d.get("readScale"),
            }
        )
    return rows


@st.cache_data(show_spinner=False)
def list_blob_containers(
    _credential: InteractiveBrowserCredential,
    subscription_id: str,
    resource_group: str,
    account_name: str,
) -> List[str]:
    # Prefer data plane (RBAC) if possible
    try:
        svc = BlobServiceClient(account_url=f"https://{account_name}.blob.core.windows.net", credential=_credential)
        return [c.name for c in svc.list_containers()]
    except Exception:
        pass
    # Fallback to management plane
    try:
        smc = StorageManagementClient(_credential, subscription_id)
        return [c.name for c in smc.blob_containers.list(resource_group, account_name)]
    except Exception as exc:
        raise exc


def _blob_service(
    _credential: InteractiveBrowserCredential,
    account_name: str,
) -> BlobServiceClient:
    return BlobServiceClient(account_url=f"https://{account_name}.blob.core.windows.net", credential=_credential)


def is_hns_enabled(
    _credential: InteractiveBrowserCredential,
    subscription_id: str,
    resource_group: str,
    account_name: str,
) -> bool:
    try:
        smc = StorageManagementClient(_credential, subscription_id)
        props = smc.storage_accounts.get_properties(resource_group, account_name)
        d = _to_dict(props)
        # Common property names across SDKs
        return bool(
            d.get("is_hns_enabled")
            or d.get("is_hns")
            or getattr(props, "is_hns_enabled", False)
            or getattr(props, "is_hns", False)
        )
    except Exception:
        return False


def _dfs_service(
    _credential: InteractiveBrowserCredential,
    account_name: str,
) -> DataLakeServiceClient:
    return DataLakeServiceClient(account_url=f"https://{account_name}.dfs.core.windows.net", credential=_credential)


def _path_info(path: Any) -> Dict[str, Any]:
    info: Dict[str, Any] = {}
    name = getattr(path, "name", None)
    if callable(name):
        name = name()
    info["name"] = name

    is_dir = getattr(path, "is_directory", None)
    if callable(is_dir):
        is_dir = is_dir()
    if isinstance(is_dir, str):
        is_dir = is_dir.lower() == "true"
    info["is_directory"] = bool(is_dir)

    content_length = getattr(path, "content_length", None)
    if callable(content_length):
        content_length = content_length()
    info["content_length"] = content_length

    last_modified = getattr(path, "last_modified", None)
    if callable(last_modified):
        last_modified = last_modified()
    if last_modified is not None:
        info["last_modified"] = str(last_modified)
    else:
        info["last_modified"] = None

    return info


def list_adls_top_level_directories(
    _credential: InteractiveBrowserCredential,
    account_name: str,
    filesystem: str,
) -> List[Dict[str, str]]:
    try:
        svc = _dfs_service(_credential, account_name)
        fs = svc.get_file_system_client(filesystem)
        top_levels: Dict[str, Optional[str]] = {}
        # Use recursive=True to discover first segments even when only nested dirs exist
        for p in fs.get_paths(path="", recursive=True):
            info = _path_info(p)
            name = info.get("name") or ""
            if not name:
                continue
            is_dir = info.get("is_directory", False)
            # For directories, take their first segment; for files, skip
            if is_dir and "/" in name:
                folder = name.split("/", 1)[0]
                top_levels.setdefault(folder, info.get("last_modified"))
            elif is_dir and "/" not in name:
                top_levels.setdefault(name, info.get("last_modified"))
        return [
            {
                "Folder": folder,
                "LastModified": top_levels[folder] or "",
            }
            for folder in sorted(top_levels)
        ]
    except Exception as exc:
        raise exc


def list_adls_files_in_directory(
    _credential: InteractiveBrowserCredential,
    account_name: str,
    filesystem: str,
    directory: str,
    max_items: int = 500,
) -> List[Dict[str, str]]:
    try:
        svc = _dfs_service(_credential, account_name)
        fs = svc.get_file_system_client(filesystem)
        files: List[Dict[str, str]] = []
        for p in fs.get_paths(path=directory, recursive=False):
            info = _path_info(p)
            if info.get("is_directory", False):
                continue
            name = info.get("name") or ""
            if not name:
                continue
            rel = name
            prefix = f"{directory.rstrip('/')}/"
            if directory and name.startswith(prefix):
                rel = name[len(prefix):]
            file_name = rel.rsplit("/", 1)[-1] if "/" in rel else rel
            files.append({
                "File": file_name or rel,
                "LastModified": info.get("last_modified") or "",
            })
            if len(files) >= max_items:
                break
        return files
    except Exception as exc:
        raise exc


def list_top_level_folders(
    _credential: InteractiveBrowserCredential,
    account_name: str,
    container_name: str,
) -> List[str]:
    try:
        svc = _blob_service(_credential, account_name)
        cc = svc.get_container_client(container_name)
        folders: Set[str] = set()
        for blob in cc.list_blobs():
            name = getattr(blob, "name", "") or _to_dict(blob).get("name", "")
            if "/" in name:
                folders.add(name.split("/", 1)[0])
        return sorted(folders)
    except Exception as exc:
        raise exc


def list_files_in_folder(
    _credential: InteractiveBrowserCredential,
    account_name: str,
    container_name: str,
    folder: str,
    max_items: int = 200,
) -> List[Dict[str, str]]:
    try:
        svc = _blob_service(_credential, account_name)
        cc = svc.get_container_client(container_name)
        prefix = folder.rstrip("/") + "/"
        files: List[Dict[str, str]] = []
        for blob in cc.list_blobs(name_starts_with=prefix):
            name = getattr(blob, "name", "") or _to_dict(blob).get("name", "")
            if not name or name.endswith("/"):
                continue
            display = name[len(prefix):] if name.startswith(prefix) else name
            file_name = display.rsplit("/", 1)[-1] if "/" in display else display
            last_modified = getattr(blob, "last_modified", None)
            if callable(last_modified):
                last_modified = last_modified()
            files.append({
                "File": file_name or display,
                "LastModified": str(last_modified) if last_modified is not None else "",
            })
            if len(files) >= max_items:
                break
        return files
    except Exception as exc:
        raise exc


def sample_adls_paths(
    _credential: InteractiveBrowserCredential,
    account_name: str,
    filesystem: str,
    limit: int = 20,
) -> List[Dict[str, Any]]:
    try:
        svc = _dfs_service(_credential, account_name)
        fs = svc.get_file_system_client(filesystem)
        samples: List[Dict[str, Any]] = []
        for idx, p in enumerate(fs.get_paths(path="", recursive=True)):
            info = _path_info(p)
            samples.append(info)
            if idx + 1 >= limit:
                break
        return samples
    except Exception as exc:
        raise exc


def sample_blob_paths(
    _credential: InteractiveBrowserCredential,
    account_name: str,
    container_name: str,
    limit: int = 20,
) -> List[Dict[str, Any]]:
    try:
        svc = _blob_service(_credential, account_name)
        cc = svc.get_container_client(container_name)
        samples: List[Dict[str, Any]] = []
        for idx, blob in enumerate(cc.list_blobs()):
            bd = _to_dict(blob)
            samples.append({
                "name": bd.get("name") or getattr(blob, "name", ""),
                "size": bd.get("size") or getattr(blob, "size", None),
                "content_type": bd.get("content_settings", {}).get("content_type") if isinstance(bd.get("content_settings"), dict) else None,
                "last_modified": str(bd.get("last_modified") or getattr(blob, "last_modified", "")),
            })
            if idx + 1 >= limit:
                break
        return samples
    except Exception as exc:
        raise exc


def _friendly_resource_type(full_type: Optional[str]) -> str:
    if not full_type:
        return "Unknown"
    normalized = full_type.strip()
    provider, _, resource_path = normalized.partition("/")
    provider_tokens = _clean_provider(provider)
    if provider_tokens and provider_tokens[0].lower() == "microsoft":
        provider_tokens = provider_tokens[1:]
    resource_token = _clean_resource_segment(resource_path)

    provider_name = " ".join(provider_tokens).strip()

    if resource_token and resource_token.lower() in provider_name.lower():
        return provider_name or resource_token
    if provider_name and resource_token:
        combined = f"{provider_name} {resource_token}".strip()
        return _dedupe_words(combined)
    return provider_name or resource_token or normalized


def _split_camel(value: str) -> str:
    return re.sub(r"(?<=[a-z0-9])(?=[A-Z])", " ", value)


def _clean_provider(provider: str) -> List[str]:
    if not provider:
        return []
    parts = []
    for token in provider.split('.'):
        token = token.strip()
        if not token:
            continue
        cleaned = _split_camel(token.replace('_', ' ').replace('-', ' '))
        parts.extend(t for t in cleaned.split() if t)
    return parts


def _clean_resource_segment(resource_path: str) -> str:
    if not resource_path:
        return ""
    last_segment = resource_path.split('/')[-1]
    cleaned = _split_camel(last_segment.replace('_', ' ').replace('-', ' ')).strip()
    if cleaned.lower().endswith('ies'):
        cleaned = cleaned[:-3] + 'y'
    elif cleaned.lower().endswith('s') and len(cleaned) > 3:
        cleaned = cleaned[:-1]
    return cleaned.title()


def _dedupe_words(text: str) -> str:
    seen = []
    lower_seen = set()
    for word in text.split():
        lw = word.lower()
        if lw not in lower_seen:
            seen.append(word)
            lower_seen.add(lw)
    return " ".join(seen)


def fetch_components_for_factory(
    
    credential: InteractiveBrowserCredential,
    subscription_id: str,
    resource_group: str,
    factory_name: str,
) -> List[str]:
    adf_client = DataFactoryManagementClient(credential, subscription_id)
    types: Set[str] = set()
    for p in adf_client.pipelines.list_by_factory(resource_group, factory_name):
        name = getattr(p, "name", None) or _to_dict(p).get("name")
        if not name:
            continue
        full = adf_client.pipelines.get(resource_group, factory_name, name)
        fd = _to_dict(full)
        acts = fd.get("activities") or fd.get("properties", {}).get("activities")
        if isinstance(acts, list):
            _collect_activity_types(acts, types)
    return sorted(types)


def _normalize_type(t: Optional[str]) -> str:
    if not t:
        return ""
    s = t.strip().lower()
    if s.endswith("activity"):
        s = s[:-8]
    return s


SUPPORTED_MIGRATABLE = {
    "copy",
    "executepipeline",
    "ifcondition",
    "wait",
    "web",
    "setvariable",
    "azurefunction",
    "foreach",
    "lookup",
    "switch",
    "sqlserverstoredprocedure",
}


def is_migratable(activity_type: Optional[str]) -> bool:
    return _normalize_type(activity_type) in SUPPORTED_MIGRATABLE


def get_activity_category(activity_type: Optional[str]) -> str:
    norm = _normalize_type(activity_type)
    if not norm:
        return "Other"

    direct_map = {
        "copy": "Move & Transform",
        "executepipeline": "Orchestration",
        "ifcondition": "Orchestration",
        "wait": "Orchestration",
        "web": "External Service",
        "setvariable": "Orchestration",
        "azurefunction": "Compute",
        "foreach": "Orchestration",
        "lookup": "Data Lookup",
        "switch": "Orchestration",
        "sqlserverstoredprocedure": "Database",
        "notebook": "Synapse Notebook",
        "executedataflow":"General"
    }

    if norm in direct_map:
        return direct_map[norm]

    if "databricks" in norm and "notebook" in norm:
        return "Databricks Notebook"
    if "synapse" in norm and "notebook" in norm:
        return "Synapse Notebook"
    if "notebook" in norm:
        return "Notebook"
    if "copy" in norm:
        return "Move & Transform"

    return "Other"


def _activity_activation_status(activity: Dict[str, Any]) -> str:
    if not activity:
        return "Unknown"
    disabled = activity.get("isDisabled")
    if isinstance(disabled, bool):
        return "No" if disabled else "Yes"
    state = activity.get("state") or activity.get("status")
    if isinstance(state, str):
        state_lower = state.lower()
        if state_lower in {"disabled", "inactive", "off"}:
            return "No"
        if state_lower in {"enabled", "active", "on"}:
            return "Yes"
    return "Yes"


def _unwrap_expr(val: Any) -> Optional[str]:
    if isinstance(val, str) and val.strip():
        return val.strip()
    if isinstance(val, dict):
        for k in ("value", "expression"):
            iv = val.get(k)
            if isinstance(iv, str) and iv.strip():
                return iv.strip()
    return None


def _norm_key(key: Any) -> str:
    try:
        s = str(key)
    except Exception:
        return ""
    s = s.lower()
    # remove non-letters to match variants like sql_reader_query, sqlReaderQuery, sql-reader-query
    import re as _re
    return _re.sub(r"[^a-z]", "", s)


def _extract_sql_query_from_dataset(ds_dict: Dict[str, Any]) -> str:
    if not isinstance(ds_dict, dict):
        return ""
    props = (ds_dict.get("properties") or {})
    tprops = (props.get("typeProperties") or {})
    for k in ("query", "sqlReaderQuery", "commandText"):
        got = _unwrap_expr(tprops.get(k))
        if got:
            return got
    # Case-insensitive / snake-case variants
    lower = {k.lower(): v for k, v in tprops.items()}
    for k in ("query", "sqlreaderquery", "commandtext"):
        got = _unwrap_expr(lower.get(k))
        if got:
            return got

    # Deep search fallback
    def deep_find(o: Any) -> Optional[str]:
        if isinstance(o, dict):
            # direct hits
            for key, val in o.items():
                lk = _norm_key(key)
                if lk in ("query", "sqlreaderquery", "commandtext"):
                    un = _unwrap_expr(val)
                    if un:
                        return un
            # recurse
            for val in o.values():
                res = deep_find(val)
                if res:
                    return res
        elif isinstance(o, list):
            for it in o:
                res = deep_find(it)
                if res:
                    return res
        return None
    found = deep_find(tprops)
    if found:
        return found
    return ""


def _extract_sql_query_from_activity(activity: Dict[str, Any], ds_map: Optional[Dict[str, Dict[str, Any]]] = None) -> str:
    if not isinstance(activity, dict):
        return ""
    # Some SDK shapes put properties under activity["typeProperties"], others under activity["properties"]["typeProperties"]
    # Try direct camelCase
    tp = activity.get("typeProperties")
    # Try nested under properties
    if not isinstance(tp, dict):
        props = activity.get("properties")
        if isinstance(props, dict):
            tp = props.get("typeProperties")
    # Try normalized keys (snake/case-insensitive)
    if not isinstance(tp, dict):
        norm_map = { _norm_key(k): v for k, v in activity.items() }
        tp = norm_map.get("typeproperties")
    if not isinstance(tp, dict):
        props = activity.get("properties")
        if isinstance(props, dict):
            norm_props = { _norm_key(k): v for k, v in props.items() }
            tp = norm_props.get("typeproperties")
    if not isinstance(tp, dict):
        tp = {}
    src = tp.get("source") or {}
    if isinstance(src, dict):
        # Prefer explicit sqlReaderQuery.value for SQL sources
        if "sqlReaderQuery" in src:
            val = _unwrap_expr(src.get("sqlReaderQuery"))
            if val:
                return val
        # Other common fields across sources
        for k in ("query", "commandText"):
            got = _unwrap_expr(src.get(k))
            if got:
                return got

        # Case-insensitive keys
        # Try normalized-key lookup as well
        by_norm: Dict[str, Any] = {}
        for k, v in src.items():
            by_norm[_norm_key(k)] = v
        for k in ("sqlreaderquery", "query", "commandtext"):
            got = _unwrap_expr(by_norm.get(k))
            if got:
                return got

        # Deep search within source
        def deep_find(o: Any) -> Optional[str]:
            if isinstance(o, dict):
                for key, val in o.items():
                    lk = _norm_key(key)
                    if lk in ("query", "sqlreaderquery", "commandtext"):
                        un = _unwrap_expr(val)
                        if un:
                            return un
                for val in o.values():
                    res = deep_find(val)
                    if res:
                        return res
            elif isinstance(o, list):
                for it in o:
                    res = deep_find(it)
                    if res:
                        return res
            return None
        found = deep_find(src)
        if found:
            return found

    # Fall back to dataset-level query if inputs reference a dataset with a query
    if isinstance(ds_map, dict):
        inputs = activity.get("inputs")
        if isinstance(inputs, list):
            for ref in inputs:
                if not isinstance(ref, dict):
                    continue
                rn = ref.get("referenceName") or ref.get("name")
                if isinstance(rn, str) and rn:
                    ds_def = ds_map.get(rn)
                    if ds_def:
                        q = _extract_sql_query_from_dataset(ds_def)
                        if q:
                            return q

    # Ultimate fallback: deep search entire activity for sqlReaderQuery/query/commandText
    def deep_find(o: Any) -> Optional[str]:
        if isinstance(o, dict):
            for key, val in o.items():
                lk = _norm_key(key)
                if lk in ("sqlreaderquery", "query", "commandtext"):
                    un = _unwrap_expr(val)
                    if un:
                        return un
            for val in o.values():
                res = deep_find(val)
                if res:
                    return res
        elif isinstance(o, list):
            for it in o:
                res = deep_find(it)
                if res:
                    return res
        return None
    found_any = deep_find(activity)
    if found_any:
        return found_any

    return ""


def _get_io(a: Dict[str, Any], key: str) -> List[Dict[str, Any]]:
    """
    Extract inputs/outputs from an activity whether at:
    - root level
    - inside properties
    """
    # Case 1: root-level inputs/outputs
    if key in a and isinstance(a[key], list):
        return a[key]

    # Case 2: nested inside properties
    props = a.get("properties", {})
    if isinstance(props, dict) and key in props and isinstance(props[key], list):
        return props[key]

    return []


def _collect_activity_rows(
    activities: Optional[List[Any]],
    rows: List[Dict[str, str]],
    factory_name: str,
    pipeline_name: str,
    ds_map: Optional[Dict[str, Dict[str, Any]]] = None,
) -> None:

    if not activities:
        return

    for act in activities:
        a = _to_dict(act)

        a_type = a.get("type", "")
        name = a.get("name", "")
        desc = a.get("description", "")
        activated = _activity_activation_status(a)
        query = _extract_sql_query_from_activity(a, ds_map)

        # Output fields
        source_ds = ""
        sink_ds = ""
        source_ls = ""
        sink_ls = ""

        # ---------------------------------------
        # Extract Source/Sink dataset for Copy
        # ---------------------------------------
        if a_type.lower() == "copy":

            inputs = _get_io(a, "inputs")
            outputs = _get_io(a, "outputs")

            # Dataset names
            source_ds_list = [
                (inp.get("reference_name") or inp.get("name") or "").strip()
                for inp in inputs if isinstance(inp, dict)
            ]
            source_ds_list = [s for s in source_ds_list if s]
            source_ds = ", ".join(source_ds_list)

            sink_ds_list = [
                (out.get("reference_name") or out.get("name") or "").strip()
                for out in outputs if isinstance(out, dict)
            ]
            sink_ds_list = [s for s in sink_ds_list if s]
            sink_ds = ", ".join(sink_ds_list)

            # Resolve linked services
            for ds in source_ds_list:
                ds_def = ds_map.get(ds, {})
                ls = _extract_linked_service_reference(ds_def)
                if ls:
                    source_ls = ls

            for ds in sink_ds_list:
                ds_def = ds_map.get(ds, {})
                ls = _extract_linked_service_reference(ds_def)
                if ls:
                    sink_ls = ls

        # ---------------------------------------
        # Append row
        # ---------------------------------------
        rows.append({
            "Factory": factory_name,
            "PipelineName": pipeline_name,
            "ActivityName": name,
            "ActivityType": a_type,
            "Migratable": "Yes" if is_migratable(a_type) else "No",
            "Category": get_activity_category(a_type),
            "Activated": activated,
            "Description": desc,
            "SourceQuery": query,
            "SourceDataset": source_ds,
            "SinkDataset": sink_ds,
            "SourceLinkedService": source_ls,
            "SinkLinkedService": sink_ls,
        })

        # ---------------------------------------
        # Recurse nested activities
        # ---------------------------------------
        nested_keys = [
            "activities",
            "ifTrueActivities",
            "ifFalseActivities",
            "defaultActivities",
            "innerActivities",
            "caseActivities",
        ]

        for key in nested_keys:
            inner = a.get(key)
            if isinstance(inner, list):
                _collect_activity_rows(inner, rows, factory_name, pipeline_name, ds_map)

        # Case blocks
        cases = a.get("cases")
        if isinstance(cases, list):
            for c in cases:
                cd = _to_dict(c)
                acts = cd.get("activities")
                if isinstance(acts, list):
                    _collect_activity_rows(acts, rows, factory_name, pipeline_name, ds_map)






def fetch_activity_rows_for_factory(
    credential: InteractiveBrowserCredential,
    subscription_id: str,
    resource_group: str,
    factory_name: str,
) -> List[Dict[str, str]]:
    adf_client = DataFactoryManagementClient(credential, subscription_id)
    # Build dataset map for dataset-level query resolution
    ds_map: Dict[str, Dict[str, Any]] = {}
    try:
        for ds in adf_client.datasets.list_by_factory(resource_group, factory_name):
            name = getattr(ds, "name", None) or _to_dict(ds).get("name")
            if not name:
                continue
            try:
                full = adf_client.datasets.get(resource_group, factory_name, name)
                ds_map[name] = _to_dict(full)
            except Exception:
                ds_map[name] = _to_dict(ds)
    except Exception:
        pass
    rows: List[Dict[str, str]] = []
    for p in adf_client.pipelines.list_by_factory(resource_group, factory_name):
        name = getattr(p, "name", None) or _to_dict(p).get("name")
        if not name:
            continue
        full = adf_client.pipelines.get(resource_group, factory_name, name)
        fd = _to_dict(full)
        acts = fd.get("activities") or fd.get("properties", {}).get("activities")
        if isinstance(acts, list):
            _collect_activity_rows(acts, rows, factory_name, name, ds_map)
    return rows


def list_linked_services_for_factory(
    credential: InteractiveBrowserCredential,
    subscription_id: str,
    resource_group: str,
    factory_name: str,
) -> List[Dict[str, str]]:
    adf_client = DataFactoryManagementClient(credential, subscription_id)
    items: List[Dict[str, str]] = []
    for ls in adf_client.linked_services.list_by_factory(resource_group, factory_name):
        d = _to_dict(ls)
        ls_type = (
            (d.get("properties") or {}).get("type")
            or d.get("type")
            or getattr(ls, "type", "")
        )
        ls_name = d.get("name") or getattr(ls, "name", "")
        items.append({
            "Factory": factory_name,
            "LinkedService": ls_name,
            "LinkedServiceType": ls_type or "",
        })
    return items


def _classify_datasets_from_pipelines(adf_client, resource_group: str, factory_name: str) -> Dict[str, Dict[str, Any]]:
    """
    Analyze pipelines to classify datasets as source or sink based on Copy activities.
    Returns a dictionary with dataset names as keys and their details including pipeline names.
    """
    dataset_info: Dict[str, Dict[str, Any]] = {}
    
    try:
        pipelines = adf_client.pipelines.list_by_factory(resource_group, factory_name)
        for pipeline in pipelines:
            pipeline_dict = _to_dict(pipeline)
            pipeline_name = pipeline_dict.get('name', '')
            activities = pipeline_dict.get('properties', {}).get('activities', [])
            
            for activity in activities:
                activity_dict = _to_dict(activity)
                activity_type = activity_dict.get('type', '').lower()
                
                # Focus only on Copy activity
                if activity_type == 'copy':
                    # Get all dataset references from inputs and outputs
                    for io_type, role in [('inputs', 'Source'), ('outputs', 'Sink')]:
                        for io_ref in activity_dict.get(io_type, []):
                            if isinstance(io_ref, dict):
                                ds_name = io_ref.get('referenceName')
                                if ds_name:
                                    if ds_name not in dataset_info:
                                        dataset_info[ds_name] = {
                                            'roles': set(),
                                            'pipelines': set()
                                        }
                                    dataset_info[ds_name]['roles'].add(role)
                                    dataset_info[ds_name]['pipelines'].add(pipeline_name)
                    
                    # Also check typeProperties for additional dataset references
                    type_props = activity_dict.get('typeProperties', {})
                    if isinstance(type_props, dict):
                        # Check source
                        source = type_props.get('source')
                        if isinstance(source, dict):
                            for io_type, role in [('inputs', 'Source'), ('dataset', 'Source')]:
                                ds_ref = source.get(io_type, {}) if isinstance(source, dict) else {}
                                if isinstance(ds_ref, dict):
                                    ds_name = ds_ref.get('referenceName')
                                    if ds_name:
                                        if ds_name not in dataset_info:
                                            dataset_info[ds_name] = {
                                                'roles': set(),
                                                'pipelines': set()
                                            }
                                        dataset_info[ds_name]['roles'].add('Source')
                                        dataset_info[ds_name]['pipelines'].add(pipeline_name)
                        
                        # Check sink
                        sink = type_props.get('sink')
                        if isinstance(sink, dict):
                            for io_type, role in [('outputs', 'Sink'), ('dataset', 'Sink')]:
                                ds_ref = sink.get(io_type, {}) if isinstance(sink, dict) else {}
                                if isinstance(ds_ref, dict):
                                    ds_name = ds_ref.get('referenceName')
                                    if ds_name:
                                        if ds_name not in dataset_info:
                                            dataset_info[ds_name] = {
                                                'roles': set(),
                                                'pipelines': set()
                                            }
                                        dataset_info[ds_name]['roles'].add('Sink')
                                        dataset_info[ds_name]['pipelines'].add(pipeline_name)
    
    except Exception as e:
        print(f"Error analyzing pipelines for dataset classification: {e}")
    
    return dataset_info

def list_datasets_for_factory(
    credential: InteractiveBrowserCredential,
    subscription_id: str,
    resource_group: str,
    factory_name: str,
) -> List[Dict[str, Any]]:
    """Fetch all datasets, their linked services, and the pipelines they're used in."""
    adf_client = DataFactoryManagementClient(credential, subscription_id)
    
    # First, get all pipelines and their dataset references
    pipeline_datasets = {}
    try:
        for pipeline in adf_client.pipelines.list_by_factory(resource_group, factory_name):
            pipeline_dict = _to_dict(pipeline)
            pipeline_name = pipeline_dict.get('name', '')
            activities = pipeline_dict.get('properties', {}).get('activities', [])
            
            for activity in activities:
                activity_dict = _to_dict(activity)
                # Check both inputs and outputs for dataset references
                for io_type in ['inputs', 'outputs']:
                    for io_ref in activity_dict.get(io_type, []):
                        if isinstance(io_ref, dict):
                            ds_name = io_ref.get('referenceName')
                            if ds_name:
                                if ds_name not in pipeline_datasets:
                                    pipeline_datasets[ds_name] = set()
                                pipeline_datasets[ds_name].add(pipeline_name)
    except Exception as e:
        print(f"Error analyzing pipelines: {e}")
    
    # Then get all datasets with their details
    items: List[Dict[str, Any]] = []
    for ds in adf_client.datasets.list_by_factory(resource_group, factory_name):
        dd = _to_dict(ds)
        ds_name = dd.get("name") or getattr(ds, "name", None)
        if not ds_name:
            continue

        # Get full definition to inspect linkedServiceName
        try:
            full = adf_client.datasets.get(resource_group, factory_name, ds_name)
            dd = _to_dict(full)
        except Exception:
            pass

        ls_name = _extract_linked_service_reference(dd)
        
        items.append({
            "Dataset": ds_name,
            "LinkedService": ls_name,
            "DataFactory": factory_name
        })
    
    return items


def _dataset_table_name_from_def(ds_def: Dict[str, Any]) -> str:
    """Best-effort extraction of table name from an ADF dataset definition."""
    if not isinstance(ds_def, dict):
        return ""
    props = ds_def.get("properties") or {}
    if not isinstance(props, dict):
        return ""
    # Handle both REST shape (typeProperties) and Python SDK shape (type_properties)
    tprops = props.get("typeProperties") or props.get("type_properties") or {}
    if isinstance(tprops, dict):
        # Direct keys as seen in many ADF JSON exports
        for key in ("tableName", "table", "tableNameExpression"):
            val = tprops.get(key)
            if isinstance(val, str) and val.strip():
                return val.strip()

        # Normalized-key lookup (covers table_name / TableName, etc.)
        norm_map = { _norm_key(k): v for k, v in tprops.items() }
        for k in ("tablename", "table"):
            val = norm_map.get(k)
            if isinstance(val, str) and val.strip():
                return val.strip()

        # Combine schema + table when both are present
        schema_val = (
            tprops.get("schema")
            or tprops.get("schemaName")
            or tprops.get("schema_name")
        )
        table_val = (
            tprops.get("table")
            or tprops.get("tableName")
            or tprops.get("table_name")
        )
        if isinstance(table_val, str) and table_val.strip():
            t = table_val.strip()
            if isinstance(schema_val, str) and schema_val.strip():
                return f"{schema_val.strip()}.{t}"
            return t
    # Deep search using normalized keys (e.g., table_name nested somewhere)
    def deep_find(o: Any) -> str:
        if isinstance(o, dict):
            for k, v in o.items():
                if _norm_key(k) in ("tablename", "table") and isinstance(v, str) and v.strip():
                    return v.strip()
            for v in o.values():
                got = deep_find(v)
                if got:
                    return got
        elif isinstance(o, list):
            for it in o:
                got = deep_find(it)
                if got:
                    return got
        return ""

    return deep_find(props)


def _dataset_schema_from_def(ds_def: Dict[str, Any]) -> List[Dict[str, Any]]:
    cols: List[Dict[str, Any]] = []
    if not isinstance(ds_def, dict):
        return cols
    props = ds_def.get("properties") or {}
    if not isinstance(props, dict):
        return cols
    schema = props.get("schema")
    if not isinstance(schema, list):
        return cols
    for col in schema:
        if not isinstance(col, dict):
            continue
        cols.append(
            {
                "Column": col.get("name", ""),
                "Type": col.get("type", ""),
                "Precision": col.get("precision"),
                "Scale": col.get("scale"),
                "Nullable": col.get("nullable"),
            }
        )
    return cols


def _collect_dataset_io_rows(
    activities: Optional[List[Any]],
    rows: List[Dict[str, str]],
    factory_name: str,
    pipeline_name: str,
    ds_map: Dict[str, Dict[str, Any]],
) -> None:
    if not activities:
        return
    for act in activities:
        a = _to_dict(act)
        name = a.get("name") or ""
        a_type = a.get("type") or ""

        def _find_dataset_refs(obj: Any, target_key: str) -> List[str]:
            found: List[str] = []
            if isinstance(obj, dict):
                for k, v in obj.items():
                    if _norm_key(k) == target_key:
                        if isinstance(v, list):
                            for ref in v:
                                if not isinstance(ref, dict):
                                    continue
                                # SDK objects may expose referenceName (REST) or reference_name (Python model)
                                rn = (
                                    ref.get("referenceName")
                                    or ref.get("reference_name")
                                    or ref.get("name")
                                )
                                if isinstance(rn, str) and rn:
                                    found.append(rn)
                    else:
                        found.extend(_find_dataset_refs(v, target_key))
            elif isinstance(obj, list):
                for it in obj:
                    found.extend(_find_dataset_refs(it, target_key))
            return found

        raw_src_ds = _find_dataset_refs(a, "inputs")
        raw_sink_ds = _find_dataset_refs(a, "outputs")

        src_ds: List[str] = []
        src_ls: List[str] = []
        for rn in raw_src_ds:
            src_ds.append(rn)
            ds_def = ds_map.get(rn) or {}
            ls_name = _extract_linked_service_reference(ds_def)
            if ls_name:
                src_ls.append(ls_name)

        sink_ds: List[str] = []
        sink_ls: List[str] = []
        for rn in raw_sink_ds:
            sink_ds.append(rn)
            ds_def = ds_map.get(rn) or {}
            ls_name = _extract_linked_service_reference(ds_def)
            if ls_name:
                sink_ls.append(ls_name)

        if src_ds or sink_ds:
            rows.append({
                "Factory": factory_name,
                "Pipeline": pipeline_name,
                "Activity": name,
                "ActivityType": a_type,
                "SourceDatasets": ", ".join(sorted(set(src_ds))),
                "SourceLinkedServices": ", ".join(sorted(set(src_ls))),
                "SinkDatasets": ", ".join(sorted(set(sink_ds))),
                "SinkLinkedServices": ", ".join(sorted(set(sink_ls))),
            })

        inner_lists: List[List[Any]] = []
        for key in (
            "activities",
            "ifTrueActivities",
            "ifFalseActivities",
            "defaultActivities",
            "innerActivities",
            "caseActivities",
        ):
            v = a.get(key)
            if isinstance(v, list):
                inner_lists.append(v)
        cases = a.get("cases")
        if isinstance(cases, list):
            for c in cases:
                cd = _to_dict(c)
                if isinstance(cd.get("activities"), list):
                    inner_lists.append(cd.get("activities"))
        for lst in inner_lists:
            _collect_dataset_io_rows(lst, rows, factory_name, pipeline_name, ds_map)


@st.cache_data(show_spinner=False)
def list_dataset_io_for_factory(
    _credential: InteractiveBrowserCredential,
    subscription_id: str,
    resource_group: str,
    factory_name: str,
) -> List[Dict[str, str]]:
    adf_client = DataFactoryManagementClient(_credential, subscription_id)
    ds_map: Dict[str, Dict[str, Any]] = {}
    try:
        for ds in adf_client.datasets.list_by_factory(resource_group, factory_name):
            name = getattr(ds, "name", None) or _to_dict(ds).get("name")
            if not name:
                continue
            try:
                full = adf_client.datasets.get(resource_group, factory_name, name)
                ds_map[name] = _to_dict(full)
            except Exception:
                ds_map[name] = _to_dict(ds)
    except Exception:
        pass
    rows: List[Dict[str, str]] = []
    for p in adf_client.pipelines.list_by_factory(resource_group, factory_name):
        p_name = getattr(p, "name", None) or _to_dict(p).get("name")
        if not p_name:
            continue
        try:
            full = adf_client.pipelines.get(resource_group, factory_name, p_name)
            fd = _to_dict(full)
        except Exception:
            continue
        acts = fd.get("activities") or fd.get("properties", {}).get("activities")
        if isinstance(acts, list):
            _collect_dataset_io_rows(acts, rows, factory_name, p_name, ds_map)
    return rows


def list_sql_usage_for_database_from_adf(
    credential: InteractiveBrowserCredential,
    subscription_id: str,
    resource_group: str,
    factory_name: str,
    sql_server_name: str,
    sql_database_name: str,
) -> List[Dict[str, str]]:
    """Return linked services ADF uses for a given Azure SQL database.

    This inspects linked services only (no direct SQL connection).
    """
    adf_client = DataFactoryManagementClient(credential, subscription_id)

    server_lower = (sql_server_name or "").lower()
    db_lower = (sql_database_name or "").lower()
    if not server_lower or not db_lower:
        return []

    # 1) Find linked services that point at this server+database
    target_ls: Set[str] = set()
    ls_rows: Dict[str, Dict[str, str]] = {}
    for ls in adf_client.linked_services.list_by_factory(resource_group, factory_name):
        name = getattr(ls, "name", None) or _to_dict(ls).get("name")
        if not name:
            continue
        d = _to_dict(ls)
        props = d.get("properties") or {}
        ls_type = (
            props.get("type")
            or d.get("type")
            or getattr(ls, "type", "")
        )
        try:
            text = json.dumps(props).lower()
        except Exception:
            continue
        if not text:
            continue
        # Match either bare server name or fully qualified host
        server_match = server_lower in text or f"{server_lower}.database.windows.net" in text
        db_match = db_lower in text
        if server_match and db_match:
            target_ls.add(name)
            ls_rows[name] = {
                "LinkedService": name,
                "LinkedServiceType": ls_type or "",
            }

    return list(ls_rows.values())


def list_sql_tables_for_database_from_adf(
    credential: InteractiveBrowserCredential,
    subscription_id: str,
    resource_group: str,
    factory_name: str,
    sql_server_name: str,
    sql_database_name: str,
) -> List[Dict[str, str]]:
    """Backward-compatible wrapper: only distinct (LinkedService, Table, Dataset).

    NOTE: with linked-service-only mode, this will always return an empty list.
    """
    ds_rows: List[Dict[str, str]] = []
    _ = list_sql_usage_for_database_from_adf(
        credential=credential,
        subscription_id=subscription_id,
        resource_group=resource_group,
        factory_name=factory_name,
        sql_server_name=sql_server_name,
        sql_database_name=sql_database_name,
    )
    seen: Set[Tuple[str, str, str]] = set()
    # No dataset inspection in linked-service-only mode
    return []


def _parse_table_identifier(raw: str) -> Tuple[str, str]:
    """Parse a table identifier into (schema, table).

    Accepts forms like:
    - Table
    - schema.Table
    - [schema].[Table]
    Falls back to schema "dbo" when none is provided.
    """
    if not raw:
        return "dbo", ""
    text = str(raw).strip()
    # Remove surrounding brackets
    text = text.strip("[]")
    if "." in text:
        schema, table = text.split(".", 1)
    else:
        schema, table = "dbo", text
    schema = schema.strip().strip("[]") or "dbo"
    table = table.strip().strip("[]")
    return schema, table


def _inspect_sql_table_via_pyodbc(conn_str: str, table_identifier: str) -> Dict[str, Any]:
    """Connect to SQL Server using pyodbc and inspect a single table.

    Returns a dictionary with keys:
    - schema
    - table
    - row_count
    - columns: list of column metadata dicts
    - error: optional error message
    """
    try:
        import pyodbc  # type: ignore[import]
    except ImportError:
        return {"schema": "", "table": table_identifier, "row_count": None, "columns": [], "error": "pyodbc is not installed in this environment."}

    schema, table = _parse_table_identifier(table_identifier)
    result: Dict[str, Any] = {
        "schema": schema,
        "table": table,
        "row_count": None,
        "columns": [],
        "error": "",
    }

    if not table:
        result["error"] = "No table name was provided."
        return result

    try:
        with pyodbc.connect(conn_str) as conn:  # type: ignore[arg-type]
            with conn.cursor() as cur:
                # Row count
                try:
                    cur.execute(f"SELECT COUNT(*) AS RowCount FROM [{schema}].[{table}]")
                    rc = cur.fetchone()
                    result["row_count"] = rc[0] if rc else None
                except Exception as exc:
                    result["error"] = f"Failed to query row count: {exc}"

                # Column metadata
                try:
                    cur.execute(
                        """
                        SELECT
                            COLUMN_NAME,
                            DATA_TYPE,
                            IS_NULLABLE,
                            CHARACTER_MAXIMUM_LENGTH,
                            NUMERIC_PRECISION,
                            NUMERIC_SCALE
                        FROM INFORMATION_SCHEMA.COLUMNS
                        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
                        ORDER BY ORDINAL_POSITION
                        """,
                        (schema, table),
                    )
                    cols: List[Dict[str, Any]] = []
                    for row in cur.fetchall():
                        cols.append(
                            {
                                "Column": row.COLUMN_NAME,
                                "DataType": row.DATA_TYPE,
                                "IsNullable": row.IS_NULLABLE,
                                "MaxLength": row.CHARACTER_MAXIMUM_LENGTH,
                                "NumericPrecision": row.NUMERIC_PRECISION,
                                "NumericScale": row.NUMERIC_SCALE,
                            }
                        )
                    result["columns"] = cols
                except Exception as exc:
                    msg = f"Failed to query column metadata: {exc}"
                    result["error"] = f"{result['error']} | {msg}" if result["error"] else msg
    except Exception as exc:
        msg = f"Failed to connect or run inspection: {exc}"
        result["error"] = f"{result['error']} | {msg}" if result["error"] else msg

    return result


def _list_sql_tables_via_pyodbc(conn_str: str) -> Dict[str, Any]:
    result: Dict[str, Any] = {"tables": [], "error": ""}
    try:
        import pyodbc  # type: ignore[import]
    except ImportError:
        result["error"] = "pyodbc is not installed in this environment."
        return result

    try:
        with pyodbc.connect(conn_str) as conn:  # type: ignore[arg-type]
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT TABLE_SCHEMA, TABLE_NAME
                    FROM INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_TYPE = 'BASE TABLE'
                    ORDER BY TABLE_SCHEMA, TABLE_NAME
                    """
                )
                rows: List[Dict[str, Any]] = []
                for row in cur.fetchall():
                    rows.append(
                        {
                            "Schema": getattr(row, "TABLE_SCHEMA", None) or row[0],
                            "Table": getattr(row, "TABLE_NAME", None) or row[1],
                        }
                    )
                result["tables"] = rows
    except Exception as exc:
        result["error"] = f"Failed to list tables: {exc}"
    return result


def _get_db_properties_via_pyodbc(conn_str: str) -> Dict[str, Any]:
    result: Dict[str, Any] = {"properties": {}, "error": ""}
    try:
        import pyodbc  # type: ignore[import]
    except ImportError:
        result["error"] = "pyodbc is not installed in this environment."
        return result

    try:
        with pyodbc.connect(conn_str) as conn:  # type: ignore[arg-type]
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT TOP (1)
                        DB_NAME() AS DatabaseName,
                        collation_name AS Collation,
                        compatibility_level AS CompatibilityLevel,
                        create_date AS CreatedOn
                    FROM sys.databases
                    WHERE name = DB_NAME();
                    """
                )
                row = cur.fetchone()
                if row:
                    result["properties"] = {
                        "DatabaseName": getattr(row, "DatabaseName", None) or row[0],
                        "Collation": getattr(row, "Collation", None) or row[1],
                        "CompatibilityLevel": getattr(row, "CompatibilityLevel", None) or row[2],
                        "CreatedOn": str(getattr(row, "CreatedOn", None) or row[3]),
                    }
    except Exception as exc:
        result["error"] = f"Failed to fetch database properties: {exc}"
    return result


def _list_sql_views_via_pyodbc(conn_str: str) -> Dict[str, Any]:
    result: Dict[str, Any] = {"views": [], "error": ""}
    try:
        import pyodbc  # type: ignore[import]
    except ImportError:
        result["error"] = "pyodbc is not installed in this environment."
        return result

    try:
        with pyodbc.connect(conn_str) as conn:  # type: ignore[arg-type]
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT TABLE_SCHEMA, TABLE_NAME
                    FROM INFORMATION_SCHEMA.VIEWS
                    ORDER BY TABLE_SCHEMA, TABLE_NAME
                    """
                )
                rows: List[Dict[str, Any]] = []
                for row in cur.fetchall():
                    rows.append(
                        {
                            "Schema": getattr(row, "TABLE_SCHEMA", None) or row[0],
                            "View": getattr(row, "TABLE_NAME", None) or row[1],
                        }
                    )
                result["views"] = rows
    except Exception as exc:
        result["error"] = f"Failed to list views: {exc}"
    return result


def _list_sql_table_overview_via_pyodbc(conn_str: str) -> Dict[str, Any]:
    """Return per-table metadata: row count, PK/unique constraints, foreign keys.

    Output shape:
    {"tables": [
        {
            "Schema": str,
            "Table": str,
            "RowCount": int or None,
            "PrimaryKeys": str,   # e.g. "PK_Table(Id)"
            "UniqueConstraints": str,
            "ForeignKeys": str,   # e.g. "FK_Name: Col -> dbo.Other(Id)"
        }, ...
    ], "error": str}
    """
    result: Dict[str, Any] = {"tables": [], "error": ""}
    try:
        import pyodbc  # type: ignore[import]
    except ImportError:
        result["error"] = "pyodbc is not installed in this environment."
        return result

    try:
        with pyodbc.connect(conn_str) as conn:  # type: ignore[arg-type]
            with conn.cursor() as cur:
                # Approximate row counts per table
                cur.execute(
                    """
                    SELECT
                        s.name AS SchemaName,
                        t.name AS TableName,
                        SUM(p.rows) AS RowCnt
                    FROM sys.tables AS t
                    JOIN sys.schemas AS s ON t.schema_id = s.schema_id
                    JOIN sys.partitions AS p
                        ON t.object_id = p.object_id
                        AND p.index_id IN (0, 1)
                    GROUP BY s.name, t.name
                    ORDER BY s.name, t.name;
                    """
                )
                table_map: Dict[Tuple[str, str], Dict[str, Any]] = {}
                for row in cur.fetchall():
                    schema_name = getattr(row, "SchemaName", None) or row[0]
                    table_name = getattr(row, "TableName", None) or row[1]
                    row_count = getattr(row, "RowCnt", None) or row[2]
                    key = (schema_name, table_name)
                    table_map[key] = {
                        "Schema": schema_name,
                        "Table": table_name,
                        "No.ofRecords": int(row_count) if row_count is not None else None,
                        "PrimaryKeys": "",
                        "UniqueConstraints": "",
                        "ForeignKeys": "",
                    }

                # Primary keys and unique constraints
                cur.execute(
                    """
                    SELECT
                        s.name AS SchemaName,
                        t.name AS TableName,
                        kc.name AS ConstraintName,
                        kc.type_desc AS ConstraintType,
                        STRING_AGG(c.name, ',') WITHIN GROUP (ORDER BY ic.key_ordinal) AS Columns
                    FROM sys.key_constraints AS kc
                    JOIN sys.tables AS t ON kc.parent_object_id = t.object_id
                    JOIN sys.schemas AS s ON t.schema_id = s.schema_id
                    JOIN sys.index_columns AS ic
                        ON kc.parent_object_id = ic.object_id
                        AND kc.unique_index_id = ic.index_id
                    JOIN sys.columns AS c
                        ON ic.object_id = c.object_id
                        AND ic.column_id = c.column_id
                    GROUP BY s.name, t.name, kc.name, kc.type_desc
                    """
                )
                for row in cur.fetchall():
                    schema_name = getattr(row, "SchemaName", None) or row[0]
                    table_name = getattr(row, "TableName", None) or row[1]
                    constraint_name = getattr(row, "ConstraintName", None) or row[2]
                    constraint_type = getattr(row, "ConstraintType", None) or row[3]
                    columns = getattr(row, "Columns", None) or row[4]
                    key = (schema_name, table_name)
                    info = table_map.setdefault(
                        key,
                        {
                            "Schema": schema_name,
                            "Table": table_name,
                            "RowCount": None,
                            "PrimaryKeys": "",
                            "UniqueConstraints": "",
                            "ForeignKeys": "",
                        },
                    )
                    entry = f"{constraint_name}({columns})"
                    if "PRIMARY_KEY" in str(constraint_type).upper():
                        info["PrimaryKeys"] = (
                            f"{info['PrimaryKeys']}; {entry}".strip("; ")
                            if info["PrimaryKeys"]
                            else entry
                        )
                    else:
                        info["UniqueConstraints"] = (
                            f"{info['UniqueConstraints']}; {entry}".strip("; ")
                            if info["UniqueConstraints"]
                            else entry
                        )

                # Foreign keys
                cur.execute(
                    """
                    SELECT
                        s_from.name AS SchemaName,
                        t_from.name AS TableName,
                        fk.name AS ForeignKeyName,
                        s_to.name AS RefSchema,
                        t_to.name AS RefTable,
                        STRING_AGG(c_from.name, ',') AS Columns,
                        STRING_AGG(c_to.name, ',') AS RefColumns
                    FROM sys.foreign_keys AS fk
                    JOIN sys.foreign_key_columns AS fkc
                        ON fk.object_id = fkc.constraint_object_id
                    JOIN sys.tables AS t_from
                        ON fkc.parent_object_id = t_from.object_id
                    JOIN sys.schemas AS s_from
                        ON t_from.schema_id = s_from.schema_id
                    JOIN sys.tables AS t_to
                        ON fkc.referenced_object_id = t_to.object_id
                    JOIN sys.schemas AS s_to
                        ON t_to.schema_id = s_to.schema_id
                    JOIN sys.columns AS c_from
                        ON fkc.parent_object_id = c_from.object_id
                        AND fkc.parent_column_id = c_from.column_id
                    JOIN sys.columns AS c_to
                        ON fkc.referenced_object_id = c_to.object_id
                        AND fkc.referenced_column_id = c_to.column_id
                    GROUP BY
                        s_from.name,
                        t_from.name,
                        fk.name,
                        s_to.name,
                        t_to.name
                    """
                )
                for row in cur.fetchall():
                    schema_name = getattr(row, "SchemaName", None) or row[0]
                    table_name = getattr(row, "TableName", None) or row[1]
                    fk_name = getattr(row, "ForeignKeyName", None) or row[2]
                    ref_schema = getattr(row, "RefSchema", None) or row[3]
                    ref_table = getattr(row, "RefTable", None) or row[4]
                    cols = getattr(row, "Columns", None) or row[5]
                    ref_cols = getattr(row, "RefColumns", None) or row[6]
                    key = (schema_name, table_name)
                    info = table_map.setdefault(
                        key,
                        {
                            "Schema": schema_name,
                            "Table": table_name,
                            "RowCount": None,
                            "PrimaryKeys": "",
                            "UniqueConstraints": "",
                            "ForeignKeys": "",
                        },
                    )
                    entry = f"{fk_name}: {cols} -> {ref_schema}.{ref_table}({ref_cols})"
                    info["ForeignKeys"] = (
                        f"{info['ForeignKeys']}; {entry}".strip("; ")
                        if info["ForeignKeys"]
                        else entry
                    )

        # Convert to sorted list
        result["tables"] = [
            table_map[key]
            for key in sorted(table_map.keys(), key=lambda k: (k[0], k[1]))
        ]
    except Exception as exc:
        result["error"] = f"Failed to fetch table metadata: {exc}"
    return result


def _dot_id(prefix: str, name: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9_]+", "_", name or "")
    return f"{prefix}_{safe}"


def _dot_label(text: str) -> str:
    return (text or "").replace("\"", "'")


# def get_adf_lineage(credential: InteractiveBrowserCredential, subscription_id: str, resource_group: str, factory_name: str) -> Dict[str, Any]:
#     """
#     Generate a hierarchical view of ADF components and their relationships.
#     Returns a dictionary with the structure:
#     {
#         "factory_name": str,
#         "pipelines": [
#             {
#                 "name": str,
#                 "activities": [
#                     {
#                         "name": str,
#                         "type": str,
#                         "inputs": [{"name": str, "type": str, "linked_service": str}],
#                         "outputs": [{"name": str, "type": str, "linked_service": str}]
#                     }
#                 ]
#             }
#         ]
#     }
#     """
#     adf_client = DataFactoryManagementClient(credential, subscription_id)
#     result = {
#         "factory_name": factory_name,
#         "pipelines": []
#     }
    
#     try:
#         # Get all pipelines
#         pipelines = adf_client.pipelines.list_by_factory(resource_group, factory_name)
        
#         for pipeline in pipelines:
#             pipeline_data = {
#                 "name": pipeline.name,
#                 "activities": []
#             }
            
#             # Get pipeline details
#             pipeline_details = adf_client.pipelines.get(resource_group, factory_name, pipeline.name)
#             activities = _to_dict(pipeline_details).get("properties", {}).get("activities", [])
            
#             for activity in activities:
#                 activity_data = {
#                     "name": activity.get("name", ""),
#                     "type": activity.get("type", ""),
#                     "inputs": [],
#                     "outputs": []
#                 }
                
#                 # Process inputs
#                 for input_ref in activity.get("inputs", []):
#                     if not isinstance(input_ref, dict):
#                         continue
#                     input_name = input_ref.get("referenceName", "")
#                     if not input_name:
#                         continue
                    
#                     # Get dataset details
#                     try:
#                         dataset = adf_client.datasets.get(resource_group, factory_name, input_name)
#                         dataset_dict = _to_dict(dataset)
#                         ls_ref = (dataset_dict.get("properties") or {}).get("linkedServiceName", {})
#                         ls_name = ls_ref.get("referenceName", "") if isinstance(ls_ref, dict) else str(ls_ref)
                        
#                         activity_data["inputs"].append({
#                             "name": input_name,
#                             "type": (dataset_dict.get("properties") or {}).get("type", "Unknown"),
#                             "linked_service": ls_name
#                         })
#                     except Exception:
#                         activity_data["inputs"].append({
#                             "name": input_name,
#                             "type": "Unknown",
#                             "linked_service": "Unknown"
#                         })
                
#                 # Process outputs
#                 for output_ref in activity.get("outputs", []):
#                     if not isinstance(output_ref, dict):
#                         continue
#                     output_name = output_ref.get("referenceName", "")
#                     if not output_name:
#                         continue
                    
#                     # Get dataset details
#                     try:
#                         dataset = adf_client.datasets.get(resource_group, factory_name, output_name)
#                         dataset_dict = _to_dict(dataset)
#                         ls_ref = (dataset_dict.get("properties") or {}).get("linkedServiceName", {})
#                         ls_name = ls_ref.get("referenceName", "") if isinstance(ls_ref, dict) else str(ls_ref)
                        
#                         activity_data["outputs"].append({
#                             "name": output_name,
#                             "type": (dataset_dict.get("properties") or {}).get("type", "Unknown"),
#                             "linked_service": ls_name
#                         })
#                     except Exception:
#                         activity_data["outputs"].append({
#                             "name": output_name,
#                             "type": "Unknown",
#                             "linked_service": "Unknown"
#                         })
                
#                 pipeline_data["activities"].append(activity_data)
            
#             result["pipelines"].append(pipeline_data)
    
#     except Exception as e:
#         st.error(f"Error generating lineage: {str(e)}")
    
#     return result

# def display_lineage(lineage_data: Dict[str, Any]) -> None:
#     """Display the lineage in a hierarchical view"""
#     st.subheader("ADF Lineage View")
    
#     # Create expandable sections for each pipeline
#     for pipeline in lineage_data.get("pipelines", []):
#         with st.expander(f"📋 {pipeline['name']}", expanded=False):
#             for activity in pipeline.get("activities", []):
#                 st.markdown(f"#### 🏗️ {activity['name']} ({activity['type']})")
                
#                 # Display inputs
#                 if activity['inputs']:
#                     st.markdown("**Inputs:**")
#                     for input_ds in activity['inputs']:
#                         st.markdown(f"- 📥 **{input_ds['name']}** ({input_ds['type']})")
#                         st.markdown(f"  - 🔗 Linked Service: {input_ds['linked_service']}")
                
#                 # Display outputs
#                 if activity['outputs']:
#                     st.markdown("**Outputs:**")
#                     for output_ds in activity['outputs']:
#                         st.markdown(f"- 📤 **{output_ds['name']}** ({output_ds['type']})")
#                         st.markdown(f"  - 🔗 Linked Service: {output_ds['linked_service']}")
                
#                 st.markdown("---")


def main() -> None:
    st.set_page_config(page_title="ADF Components Browser", page_icon="🧩", layout="centered")
    st.title("Azure Data Factory Components Browser")
    st.caption("Sign in, pick subscription → resource group → factories, then view components per factory.")

    if "credential" not in st.session_state:
        st.session_state.credential = None

    # Sign-in section
    with st.container(border=True):
        st.subheader("Sign in to Azure")
        col1, col2 = st.columns([1, 3])
        with col1:
            login_clicked = st.button("Sign in", type="primary", use_container_width=True)
        with col2:
            st.info("Interactive browser login. You may be prompted in a separate window.")
        if login_clicked or st.session_state.credential is None:
            try:
                cred = InteractiveBrowserCredential()
                # Touch the graph by listing subscriptions to complete device login
                subs = list_subscriptions(_credential=cred)
                st.session_state.credential = cred
                st.success("Signed in successfully.")
            except Exception as e:
                st.error(f"Sign-in failed: {e}")
                return

    credential: Optional[InteractiveBrowserCredential] = st.session_state.credential
    if credential is None:
        st.stop()

    # Subscription selection
    with st.container(border=True):
        st.subheader("Please Select subscription")
        try:
            subs = list_subscriptions(_credential=credential)
        except Exception as e:
            st.error(f"Failed to list subscriptions: {e}")
            st.stop()
        sub_labels = [f"{name} ({sid})" for name, sid in subs]
        sub_idx = st.selectbox("Subscription", options=list(range(len(subs))), format_func=lambda i: sub_labels[i] if subs else "", index=0 if subs else None)
        if subs:
            subscription_id = subs[sub_idx][1]
        else:
            st.warning("No subscriptions available.")
            st.stop()

    # Resource group selection
    with st.container(border=True):
        st.subheader("Please Select resource group")
        try:
            rgs = list_resource_groups(_credential=credential, subscription_id=subscription_id)
        except Exception as e:
            st.error(f"Failed to list resource groups: {e}")
            st.stop()
        if not rgs:
            st.warning("No resource groups in this subscription.")
            st.stop()
        rg_name = st.selectbox("Resource group", options=rgs, index=0)
        try:
            res_rows = list_rg_resources(
                _credential=credential,
                subscription_id=subscription_id,
                resource_group=rg_name,
            )
            if res_rows:
                st.caption(f"Resources in '{rg_name}' ({len(res_rows)} found)")
                st.dataframe(res_rows, hide_index=True, width="stretch")
            else:
                st.info("No resources found in this resource group.")
        except Exception as e:
            st.warning(f"Could not list resources in '{rg_name}': {e}")

        # Render per-factory buttons to open details inline (same session)
        if "selected_df" not in st.session_state:
            st.session_state.selected_df = None
        selected_df: Optional[str] = st.session_state.selected_df
        clicked_df: Optional[str] = None
        try:
            factories = list_data_factories(
                _credential=credential,
                subscription_id=subscription_id,
                resource_group=rg_name,
            )
        except Exception as e:
            factories = []
            st.warning(f"Could not list data factories: {e}")
        if factories:
            st.caption("Open a Data Factory:")
            cols = st.columns(min(4, max(1, len(factories))))
            for i, fac in enumerate(factories):
                if cols[i % len(cols)].button(fac, key=f"open_df_{fac}"):
                    clicked_df = fac
        if clicked_df:
            st.session_state.selected_df = clicked_df
            selected_df = clicked_df
        if selected_df and selected_df not in factories:
            st.session_state.selected_df = None
            selected_df = None
        if selected_df:
            # 1) Components (pipelines + activities)
            try:
                act_rows = fetch_activity_rows_for_factory(credential, subscription_id, rg_name, selected_df)
            except Exception as e:
                st.error(f"Failed to fetch components: {e}")
                st.stop()
            st.subheader("Pipelines and components")
            if act_rows:
                st.dataframe(act_rows, width="stretch", hide_index=True)
            else:
                st.info("No components found.")

            # Load linked services for connectivity scoring and third table
            try:
                ls_rows = list_linked_services_for_factory(credential, subscription_id, rg_name, selected_df)
                ls_types = [row.get("LinkedServiceType", "") for row in ls_rows]
            except Exception:
                ls_rows = []
                ls_types = []

            # 2) Migration scoring
            st.subheader("Migration scoring (Fabric readiness)")
            from collections import defaultdict
            grouped: Dict[Tuple[str, str], List[Dict[str, str]]] = defaultdict(list)
            for r in act_rows:
                grouped[(r.get("Factory", ""), r.get("PipelineName", ""))].append(r)
            score_rows: List[Dict[str, Any]] = []
            for (fac, pipe), items in grouped.items():
                total_acts = len(items)
                non_migratable = sum(1 for it in items if (it.get("Migratable") or "").lower() == "no")
                control_acts = 0
                for it in items:
                    nt = _normalize_type(it.get("ActivityType"))
                    if nt in CONTROL_ACTIVITY_TYPES:
                        control_acts += 1
                parity_score = _score_component_parity(total_acts, non_migratable)
                non_mig_score = _score_non_migratable(non_migratable)
                connectivity_score = _score_connectivity(ls_types)
                orchestration_score = _score_orchestration(total_acts, control_acts)
                total = parity_score + non_mig_score + connectivity_score + orchestration_score
                if 3 in (parity_score, non_mig_score, connectivity_score, orchestration_score):
                    band = "Hard"
                elif total <= 4:
                    band = "Easy"
                elif total <= 8:
                    band = "Medium"
                else:
                    band = "Hard"
                score_rows.append({
                    "Factory": fac,
                    "Pipeline": pipe,
                    "Component parity(score)": parity_score,
                    "Non-migratable count(score)": non_mig_score,
                    "Connectivity(score)": connectivity_score,
                    "Orchestration(score)": orchestration_score,
                    "Total(score)": total,
                    "Band": band,
                    "Activities": total_acts,
                    "Non-migratable": non_migratable,
                })
            if score_rows:
                st.dataframe(score_rows, width="stretch", hide_index=True)
            else:
                st.info("No pipelines to score.")

            # # 3) Linked services
            # st.subheader("Linked services")
            # if ls_rows:
            #     st.dataframe(ls_rows, width="stretch", hide_index=True)
            # else:
            #     st.info("No linked services found.")

        #     # 4) Datasets and linked services (per dataset)
        #     with st.container(border=True):
        #         st.subheader("Datasets and linked services (per dataset)")
        #         try:
        #             ds_ls_rows = list_datasets_for_factory(
        #                 credential,
        #                 subscription_id,
        #                 rg_name,
        #                 selected_df,
        #             )
        #         except Exception as e:
        #             ds_ls_rows = []
        #             st.error(f"Failed to list datasets and linked services: {e}")
        #         if ds_ls_rows:
        #             st.dataframe(ds_ls_rows, width="stretch", hide_index=True)
        #         else:
        #             st.info("No datasets found in this Data Factory.")

        # SQL Servers: buttons and drilldown (server-level view)
        if "selected_sql_server" not in st.session_state:
            st.session_state.selected_sql_server = None
        selected_sql_server: Optional[str] = st.session_state.selected_sql_server
        clicked_sql_server: Optional[str] = None
        try:
            sql_servers = list_sql_servers(
                _credential=credential,
                subscription_id=subscription_id,
                resource_group=rg_name,
            )
        except Exception as e:
            sql_servers = []
            st.warning(f"Could not list SQL servers: {e}")
        if sql_servers:
            st.caption("Open an Azure SQL server:")
            cols_sql = st.columns(min(4, max(1, len(sql_servers))))
            for i, srv in enumerate(sql_servers):
                if cols_sql[i % len(cols_sql)].button(srv, key=f"open_sql_{srv}"):
                    clicked_sql_server = srv
        if clicked_sql_server:
            st.session_state.selected_sql_server = clicked_sql_server
            selected_sql_server = clicked_sql_server
        if selected_sql_server and selected_sql_server not in sql_servers:
            st.session_state.selected_sql_server = None
            selected_sql_server = None
        if selected_sql_server:
            st.subheader(f"Azure SQL server: {selected_sql_server}")
            # For now, management plane exposes databases; server 'metadata' is minimal here
            try:
                db_rows = list_sql_databases_for_server(
                    _credential=credential,
                    subscription_id=subscription_id,
                    resource_group=rg_name,
                    server_name=selected_sql_server,
                )
            except Exception as e:
                db_rows = []
                st.error(f"Failed to list databases for server '{selected_sql_server}': {e}")
            if db_rows:
                st.caption("Databases on this server")
                st.dataframe(db_rows, hide_index=True, width="stretch")

                # Per-database buttons to move further (database-level drilldown)
                if "selected_sql_database" not in st.session_state:
                    st.session_state.selected_sql_database = None
                selected_sql_database: Optional[str] = st.session_state.selected_sql_database
                clicked_db: Optional[str] = None
                db_names = [row.get("Database", "") for row in db_rows if row.get("Database")]  # type: ignore[assignment]
                if db_names:
                    st.caption("Open a database:")
                    db_cols = st.columns(min(4, max(1, len(db_names))))
                    for i, db_name in enumerate(db_names):
                        if db_cols[i % len(db_cols)].button(db_name, key=f"open_db_{selected_sql_server}_{db_name}"):
                            clicked_db = db_name
                if clicked_db:
                    st.session_state.selected_sql_database = clicked_db
                    selected_sql_database = clicked_db
                if selected_sql_database and selected_sql_database not in db_names:
                    st.session_state.selected_sql_database = None
                    selected_sql_database = None
                if selected_sql_database:
                    st.subheader(f"Database: {selected_sql_database}")
                    # Database metadata via direct SQL connection (pyodbc)
                    default_db_conn_hint = (
                        "DRIVER={ODBC Driver 18 for SQL Server};"
                        f"SERVER={selected_sql_server};"
                        f"DATABASE={selected_sql_database};"
                        "UID=your-user;PWD=your-password;Encrypt=yes;TrustServerCertificate=no;"
                    )
                    db_conn_str = st.text_input(
                        "SQL connection string for listing tables",
                        value="",
                        placeholder=default_db_conn_hint,
                        key=f"db_conn_{selected_sql_server}_{selected_sql_database}",
                    )
                    if db_conn_str:
                        if st.button(
                            "Database Components",
                            key=f"btn_list_tables_{selected_sql_server}_{selected_sql_database}",
                        ):
                            # Database properties
                            db_info = _get_db_properties_via_pyodbc(db_conn_str)
                            if db_info.get("error"):
                                st.error(db_info["error"])
                            else:
                                props = db_info.get("properties") or {}
                                if props:
                                    st.markdown("### Database properties")
                                    st.dataframe([props], hide_index=True, width="stretch")

                            # Views
                            view_info = _list_sql_views_via_pyodbc(db_conn_str)
                            if view_info.get("error"):
                                st.error(view_info["error"])
                            else:
                                views = view_info.get("views") or []
                                if views:
                                    st.markdown("### Views")
                                    st.dataframe(views, hide_index=True, width="stretch")
                                else:
                                    st.info("No views were found in this database.")

                            # Tables with row counts and key metadata
                            tbl_info = _list_sql_table_overview_via_pyodbc(db_conn_str)
                            if tbl_info.get("error"):
                                st.error(tbl_info["error"])
                            else:
                                tbl_rows = tbl_info.get("tables") or []
                                if tbl_rows:
                                    st.markdown("### Tables (row counts, keys, foreign keys)")
                                    st.dataframe(tbl_rows, hide_index=True, width="stretch")
                                else:
                                    st.info("No tables were found in this database.")
                    else:
                        st.info("Enter a SQL connection string above to list tables in this database.")
            else:
                st.info("No Azure SQL databases found on this server.")





       


        
        # Storage Accounts: buttons and drilldown
        if "selected_sa" not in st.session_state:
            st.session_state.selected_sa = None
        selected_sa: Optional[str] = st.session_state.selected_sa
        previous_sa: Optional[str] = selected_sa
        clicked_sa: Optional[str] = None
        try:
            storage_accounts = list_storage_accounts(
                _credential=credential,
                subscription_id=subscription_id,
                resource_group=rg_name,
            )
        except Exception as e:
            storage_accounts = []
            st.warning(f"Could not list storage accounts: {e}")
        if storage_accounts:
            st.caption("Open a Storage Account:")
            cols_sa = st.columns(min(4, max(1, len(storage_accounts))))
            for i, sa in enumerate(storage_accounts):
                if cols_sa[i % len(cols_sa)].button(sa, key=f"open_sa_{sa}"):
                    clicked_sa = sa
        if clicked_sa:
            st.session_state.selected_sa = clicked_sa
            selected_sa = clicked_sa
        if selected_sa and selected_sa not in storage_accounts:
            st.session_state.selected_sa = None
            selected_sa = None
        if selected_sa != previous_sa:
            st.session_state.storage_selection = {}
        if selected_sa:
            st.subheader(f"Storage account: {selected_sa}")
            # Account-level summary
            try:
                hns = is_hns_enabled(credential, subscription_id, rg_name, selected_sa)
            except Exception as e:
                hns = False
                st.warning(f"Could not determine HNS setting: {e}")
            try:
                containers = list_blob_containers(credential, subscription_id, rg_name, selected_sa)
            except Exception as e:
                containers = []
                st.error(f"Failed to list containers/filesystems: {e}")
            summary_rows = [{
                "Account": selected_sa,
                "HNSEnabled": "Yes" if hns else "No",
                "ContainerCount": len(containers),
            }]
            st.dataframe(summary_rows, hide_index=True, width="stretch")

            st.markdown("**Browse folders:**")
            if "storage_selection" not in st.session_state:
                st.session_state.storage_selection = {}
            selection_state: Dict[str, Dict[str, Optional[str]]] = st.session_state.storage_selection

            for c in containers:
                key = f"storage_{selected_sa}_{c}"
                sel = selection_state.setdefault(key, {"folder": None})
                try:
                    if hns:
                        top_dirs = list_adls_top_level_directories(credential, selected_sa, c)
                        folders = [row.get("Folder") for row in top_dirs if row.get("Folder")]
                        with st.expander(f"Folders in Container: {c}", expanded=bool(sel.get("folder"))):
                            if folders:
                                folder_cols = st.columns(min(4, max(1, len(folders))))
                                for idx, folder in enumerate(folders):
                                    if folder_cols[idx % len(folder_cols)].button(folder, key=f"{key}_folder_{folder}"):
                                        sel["folder"] = folder
                            else:
                                st.info("No top-level folders detected.")
                            if sel.get("folder"):
                                st.caption(f"Selected folder: {sel['folder']}")
                                try:
                                    files = list_adls_files_in_directory(credential, selected_sa, c, sel["folder"], max_items=200)
                                    if files:
                                        st.write(f"Files in {sel['folder']} (up to 200)")
                                        st.dataframe(files, hide_index=True, width="stretch")
                                    else:
                                        st.info("No files found in this folder.")
                                except Exception as e:
                                    st.warning(f"Failed to list files: {e}")
                    else:
                        top_folders = list_top_level_folders(credential, selected_sa, c)
                        with st.expander(f"Folders in {c}", expanded=bool(sel.get("folder"))):
                            if top_folders:
                                folder_cols = st.columns(min(4, max(1, len(top_folders))))
                                for idx, folder in enumerate(top_folders):
                                    if folder_cols[idx % len(folder_cols)].button(folder, key=f"{key}_folder_{folder}"):
                                        sel["folder"] = folder
                            else:
                                st.info("No top-level folders detected.")
                            if sel.get("folder"):
                                st.caption(f"Selected folder: {sel['folder']}")
                                try:
                                    files = list_files_in_folder(credential, selected_sa, c, sel["folder"], max_items=200)
                                    if files:
                                        st.write(f"Files in {sel['folder']} (up to 200)")
                                        st.dataframe(files, hide_index=True, width="stretch")
                                    else:
                                        st.info("No files found in this folder.")
                                except Exception as e:
                                    st.warning(f"Failed to list files: {e}")
                except Exception as e:
                    st.warning(f"Failed to browse container '{c}': {e}")
        

def get_factory_relationships(
    credential: InteractiveBrowserCredential,
    subscription_id: str,
    resource_groups: List[str] = None,
) -> List[Dict[str, str]]:
    """
    Create a comprehensive table showing relationships between:
    - Factory
    - Datasets
    - Linked Services
    - Linked Service Types
    
    Args:
        credential: Azure authentication credential
        subscription_id: Azure subscription ID
        resource_groups: Optional list of resource groups to filter by
        
    Returns:
        List of dictionaries, where each dictionary represents a relationship row
    """
    adf_client = DataFactoryManagementClient(credential, subscription_id)
    resource_client = ResourceManagementClient(credential, subscription_id)
    result = []
    
    # Get all resource groups if none specified
    if not resource_groups:
        resource_groups = [
            rg.name 
            for rg in resource_client.resource_groups.list()
            if rg.name  # Ensure name is not None
        ]
    
    # Cache for linked service types
    linked_service_types = {}
    
    for rg_name in resource_groups:
        try:
            # Get all factories in the resource group
            factories = adf_client.factories.list_by_resource_group(rg_name)
            
            for factory in factories:
                factory_name = factory.name
                
                # Get all linked services and their types for this factory
                if factory_name not in linked_service_types:
                    linked_service_types[factory_name] = {}
                    try:
                        linked_services = adf_client.linked_services.list_by_factory(rg_name, factory_name)
                        for ls in linked_services:
                            ls_dict = _to_dict(ls)
                            ls_name = ls_dict.get('name', '')
                            ls_type = (ls_dict.get('properties') or {}).get('type') or ls_dict.get('type', '')
                            if ls_name and ls_type:
                                linked_service_types[factory_name][ls_name] = ls_type
                    except Exception as e:
                        print(f"Error fetching linked services for factory {factory_name}: {e}")
                        
                # Get all pipelines in the factory
                try:
                    pipelines = adf_client.pipelines.list_by_factory(rg_name, factory_name)
                    
                    for pipeline in pipelines:
                        pipeline_name = pipeline.name
                        pipeline_dict = _to_dict(pipeline)
                        activities = pipeline_dict.get('properties', {}).get('activities', [])
                        
                        # Track datasets used in this pipeline
                        used_datasets = set()
                        
                        # Find all datasets used in pipeline activities
                        for activity in activities:
                            activity_dict = _to_dict(activity)
                            
                            # Check both inputs and outputs
                            for io_type in ['inputs', 'outputs']:
                                io_refs = activity_dict.get(io_type, [])
                                if not isinstance(io_refs, list):
                                    continue
                                    
                                for io_ref in io_refs:
                                    if isinstance(io_ref, dict):
                                        ds_name = io_ref.get('referenceName')
                                        if ds_name:
                                            used_datasets.add(ds_name)
                        
                        # Get details for each used dataset
                        for ds_name in used_datasets:
                            try:
                                dataset = adf_client.datasets.get(rg_name, factory_name, ds_name)
                                dataset_dict = _to_dict(dataset)
                                ls_name = _extract_linked_service_reference(dataset_dict)
                                
                                # Get linked service type from cache
                                ls_type = "N/A"
                                if ls_name and factory_name in linked_service_types:
                                    ls_type = linked_service_types[factory_name].get(ls_name, "N/A")
                                
                                result.append({
                                    "Data Factory": factory_name,
                                    "Dataset": ds_name,
                                    "Linked Service": ls_name or "N/A",
                                    "Linked Service Type": ls_type
                                })
                            except Exception as e:
                                print(f"Error getting dataset {ds_name}: {e}")
                                result.append({
                                    "Data Factory": factory_name,
                                    "Dataset": ds_name,
                                    "Linked Service": f"Error: {str(e)[:100]}",
                                    "Linked Service Type": "N/A"
                                })
                                    
                except Exception as e:
                    print(f"Error processing pipelines in factory {factory_name}: {e}")
                    
        except Exception as e:
            print(f"Error processing resource group {rg_name}: {e}")
    
    return result


if __name__ == "__main__":
    main()
