"""
Utility functions for ADF to Fabric Migration Tool
"""

import json
import re
from typing import Any, Dict, List, Optional, Set, Tuple

_re = re


def _to_dict(obj: Any) -> Dict[str, Any]:
    """Convert any object to dictionary representation."""
    if obj is None:
        return {}
    if isinstance(obj, dict):
        return obj
    if hasattr(obj, "as_dict"):
        try:
            return obj.as_dict()
        except Exception:
            pass
    # Fallback: best-effort JSON round-trip
    try:
        return json.loads(json.dumps(obj))
    except Exception:
        return {}


def _unwrap_expr(val: Any) -> Optional[str]:
    """Extract string value from expression wrapper objects."""
    if isinstance(val, str) and val.strip():
        return val.strip()
    if isinstance(val, dict):
        for k in ("value", "expression"):
            iv = val.get(k)
            if isinstance(iv, str) and iv.strip():
                return iv.strip()
    return None


def _norm_key(key: Any) -> str:
    """Normalize key by converting to lowercase and removing non-letters."""
    try:
        s = str(key)
    except Exception:
        return ""
    s = s.lower()
    return _re.sub(r"[^a-z]", "", s)


def _split_camel(value: str) -> str:
    """Split camelCase text into words."""
    return re.sub(r"(?<=[a-z0-9])(?=[A-Z])", " ", value)


def _clean_provider(provider: str) -> List[str]:
    """Clean and split provider name."""
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
    """Clean and singularize resource segment."""
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
    """Remove duplicate words from text."""
    seen = []
    lower_seen = set()
    for word in text.split():
        lw = word.lower()
        if lw not in lower_seen:
            seen.append(word)
            lower_seen.add(lw)
    return " ".join(seen)


def _friendly_resource_type(full_type: Optional[str]) -> str:
    """Convert Azure resource type to friendly name."""
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


def _extract_dataset_references(activity: Dict[str, Any]) -> Set[str]:
    """Extract dataset references from activity (inputs key)."""
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
    """Extract linked service name from dataset definition."""
    if not isinstance(dataset, dict):
        return ""

    def _norm(k: Any) -> str:
        try:
            s = str(k)
        except Exception:
            return ""
        s = s.lower()
        return _re.sub(r"[^a-z]", "", s)

    def _deep_find_lsn(obj: Any) -> Any:
        if isinstance(obj, dict):
            for k, v in obj.items():
                if _norm(k) == "linkedservicename":
                    return v
            for v in obj.values():
                result = _deep_find_lsn(v)
                if result:
                    return result
        elif isinstance(obj, list):
            for it in obj:
                result = _deep_find_lsn(it)
                if result:
                    return result
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


def _extract_sql_query_from_dataset(ds_dict: Dict[str, Any]) -> str:
    """Extract SQL query from dataset definition."""
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
            for key, val in o.items():
                if _norm_key(key) in ("query", "sqlreaderquery", "commandtext"):
                    return _unwrap_expr(val)
            for val in o.values():
                result = deep_find(val)
                if result:
                    return result
        elif isinstance(o, list):
            for it in o:
                result = deep_find(it)
                if result:
                    return result
        return None
    found = deep_find(tprops)
    if found:
        return found
    return ""


def _path_info(path: Any) -> Dict[str, Any]:
    """Extract metadata from file system path object."""
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


def _activity_activation_status(activity: Dict[str, Any]) -> str:
    """Get activity activation status."""
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


def _normalize_type(t: Optional[str]) -> str:
    """Normalize activity type name."""
    if not t:
        return ""
    s = t.strip().lower()
    if s.endswith("activity"):
        s = s[:-8]
    return s


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
            val = _unwrap_expr(tprops.get(key))
            if val:
                return val

        # Normalized-key lookup (covers table_name / TableName, etc.)
        norm_map = { _norm_key(k): v for k, v in tprops.items() }
        for k in ("tablename", "table"):
            val = _unwrap_expr(norm_map.get(k))
            if val:
                return val

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
            return table_val.strip()
    # Deep search using normalized keys (e.g., table_name nested somewhere)
    def deep_find(o: Any) -> str:
        if isinstance(o, dict):
            for key, val in o.items():
                if _norm_key(key) in ("tablename", "table"):
                    result = _unwrap_expr(val)
                    if result:
                        return result
            for val in o.values():
                result = deep_find(val)
                if result:
                    return result
        elif isinstance(o, list):
            for it in o:
                result = deep_find(it)
                if result:
                    return result
        return ""

    return deep_find(props)


def _dataset_schema_from_def(ds_def: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Extract schema (columns) from dataset definition."""
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


def _dot_id(prefix: str, name: str) -> str:
    """Create sanitized DOT graph identifier."""
    safe = re.sub(r"[^A-Za-z0-9_]+", "_", name or "")
    return f"{prefix}_{safe}"


def _dot_label(text: str) -> str:
    """Sanitize text for DOT graph labels."""
    return (text or "").replace("\"", "'")


def _parse_table_identifier(raw: str) -> Tuple[str, str]:
    """Parse a table identifier into (schema, table).

    Accepts forms like:
    - Table
    - schema.Table
    - [schema].[Table]
    Falls back to schema "dbo" when none is provided.
    """
    if not raw:
        return "", ""
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


def _collect_activity_types(activities: Optional[List[Any]], types: Set[str]) -> None:
    """Recursively collect activity types from nested activities."""
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
                if isinstance(c, dict):
                    c_acts = c.get("activities")
                    if isinstance(c_acts, list):
                        inner_lists.append(c_acts)
        for lst in inner_lists:
            _collect_activity_types(lst, types)


def _get_io(a: Dict[str, Any], key: str) -> List[Dict[str, Any]]:
    """Extract inputs/outputs from an activity whether at root level or inside properties."""
    # Case 1: root-level inputs/outputs
    if key in a and isinstance(a[key], list):
        return a[key]

    # Case 2: nested inside properties
    props = a.get("properties", {})
    if isinstance(props, dict) and key in props and isinstance(props[key], list):
        return props[key]

    return []


def _extract_sql_query_from_activity(activity: Dict[str, Any], ds_map: Optional[Dict[str, Dict[str, Any]]] = None) -> str:
    """Extract SQL query from an activity definition."""
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
                    if _norm_key(key) in ("query", "sqlreaderquery", "commandtext"):
                        return _unwrap_expr(val)
                for val in o.values():
                    result = deep_find(val)
                    if result:
                        return result
            elif isinstance(o, list):
                for it in o:
                    result = deep_find(it)
                    if result:
                        return result
            return None
        found = deep_find(src)
        if found:
            return found

    # Fall back to dataset-level query if inputs reference a dataset with a query
    if isinstance(ds_map, dict):
        inputs = activity.get("inputs")
        if isinstance(inputs, list):
            for ref in inputs:
                if isinstance(ref, dict):
                    ref_name = ref.get("referenceName") or ref.get("name")
                    if ref_name and ref_name in ds_map:
                        query = _extract_sql_query_from_dataset(ds_map[ref_name])
                        if query:
                            return query

    # Ultimate fallback: deep search entire activity for sqlReaderQuery/query/commandText
    def deep_find(o: Any) -> Optional[str]:
        if isinstance(o, dict):
            for key, val in o.items():
                if _norm_key(key) in ("query", "sqlreaderquery", "commandtext"):
                    return _unwrap_expr(val)
            for val in o.values():
                result = deep_find(val)
                if result:
                    return result
        elif isinstance(o, list):
            for it in o:
                result = deep_find(it)
                if result:
                    return result
        return None
    found_any = deep_find(activity)
    if found_any:
        return found_any

    return ""
