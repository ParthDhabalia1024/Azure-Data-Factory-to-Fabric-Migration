"""
ADF components discovery and analysis for ADF to Fabric Migration Tool
"""

from typing import List, Dict, Set, Any, Optional

from azure.identity import InteractiveBrowserCredential
from azure.mgmt.datafactory import DataFactoryManagementClient

from utilities import (
    _to_dict,
    _collect_activity_types,
    _normalize_type,
    _extract_linked_service_reference,
    _unwrap_expr,
    _norm_key,
)
from migration_score import is_migratable, get_activity_category
from constants import CONTROL_ACTIVITY_TYPES





def _activity_activation_status(activity: Dict[str, Any]) -> str:
    """Determine if an activity is activated/enabled."""
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
    """Extract SQL query from activity definition."""
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
    """Extract inputs/outputs from an activity whether at root level or inside properties."""
    # Case 1: root-level inputs/outputs
    if key in a and isinstance(a[key], list):
        return a[key]

    # Case 2: nested inside properties
    props = a.get("properties", {})
    if isinstance(props, dict) and key in props and isinstance(props[key], list):
        return props[key]

    return []


def fetch_components_for_factory(
    credential: InteractiveBrowserCredential,
    subscription_id: str,
    resource_group: str,
    factory_name: str,
) -> List[str]:
    """Fetch all activity types from a data factory."""
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


def _activity_rows_helper(
    activity: Dict[str, Any],
    factory_name: str,
    pipeline_name: str,
    ds_map: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Dict[str, str]:
    """Create activity row identical to legacy single-file output."""

    a_type = activity.get("type", "")
    name = activity.get("name", "")
    desc = activity.get("description", "")
    activated = _activity_activation_status(activity)

    # Always return a full, stable schema
    row: Dict[str, str] = {
        "Factory": factory_name,
        "PipelineName": pipeline_name,
        "ActivityName": name,
        "ActivityType": a_type,
        "Migratable": "Yes" if is_migratable(a_type) else "No",
        "Category": get_activity_category(a_type),
        "Activated": activated,
        "Description": desc or "",
        "SourceQuery": "",
        "SourceDataset": "",
        "SinkDataset": "",
        "SourceLinkedService": "",
        "SinkLinkedService": "",
    }

    # Populate only for Copy activity
    if a_type.lower() == "copy":
        inputs = _get_io(activity, "inputs")
        outputs = _get_io(activity, "outputs")

        # Support SDK snake_case and ADF JSON camelCase
        source_ds = [
            (i.get("referenceName") or i.get("reference_name") or i.get("name") or "").strip()
            for i in inputs if isinstance(i, dict)
        ]
        sink_ds = [
            (o.get("referenceName") or o.get("reference_name") or o.get("name") or "").strip()
            for o in outputs if isinstance(o, dict)
        ]

        source_ds = [s for s in source_ds if s]
        sink_ds = [s for s in sink_ds if s]

        row["SourceDataset"] = ", ".join(source_ds)
        row["SinkDataset"] = ", ".join(sink_ds)

        # Resolve linked services from dataset definitions
        if ds_map:
            for ds in source_ds:
                ls = _extract_linked_service_reference(ds_map.get(ds, {}))
                if ls:
                    row["SourceLinkedService"] = ls
                    break

            for ds in sink_ds:
                ls = _extract_linked_service_reference(ds_map.get(ds, {}))
                if ls:
                    row["SinkLinkedService"] = ls
                    break

        # Extract SQL query if present
        query = _extract_sql_query_from_activity(activity, ds_map)
        if query:
            row["SourceQuery"] = query

    return row



def _collect_activity_rows(
    activities: Optional[List[Any]],
    rows: List[Dict[str, str]],
    factory_name: str,
    pipeline_name: str,
    ds_map: Optional[Dict[str, Dict[str, Any]]] = None,
) -> None:
    """Recursively collect activity rows from pipeline activities."""
    if not activities:
        return

    for act in activities:
        a = _to_dict(act)
        row = _activity_rows_helper(a, factory_name, pipeline_name, ds_map)
        rows.append(row)

        # Recurse nested activities
        nested_keys = [
            "activities",
            "ifTrueActivities",
            "ifFalseActivities",
            "defaultActivities",
            "innerActivities",
            "caseActivities",
        ]

        for key in nested_keys:
            nested = a.get(key)
            if isinstance(nested, list):
                _collect_activity_rows(nested, rows, factory_name, pipeline_name, ds_map)

        # Case blocks
        cases = a.get("cases")
        if isinstance(cases, list):
            for case in cases:
                if isinstance(case, dict):
                    case_acts = case.get("activities")
                    if isinstance(case_acts, list):
                        _collect_activity_rows(case_acts, rows, factory_name, pipeline_name, ds_map)


def fetch_activity_rows_for_factory(
    credential: InteractiveBrowserCredential,
    subscription_id: str,
    resource_group: str,
    factory_name: str,
) -> List[Dict[str, str]]:
    """Fetch all activities from all pipelines in a factory."""
    adf_client = DataFactoryManagementClient(credential, subscription_id)
    # Build dataset map for dataset-level query resolution
    ds_map: Dict[str, Dict[str, Any]] = {}
    try:
        for ds in adf_client.datasets.list_by_factory(resource_group, factory_name):
            ds_name = getattr(ds, "name", None) or _to_dict(ds).get("name")
            if not ds_name:
                continue
            try:
                full = adf_client.datasets.get(resource_group, factory_name, ds_name)
                ds_map[ds_name] = _to_dict(full)
            except Exception:
                ds_map[ds_name] = _to_dict(ds)
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
    """List all linked services in a factory."""
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


def list_datasets_for_factory(
    credential: InteractiveBrowserCredential,
    subscription_id: str,
    resource_group: str,
    factory_name: str,
) -> List[Dict[str, Any]]:
    """Fetch all datasets, their linked services, and the pipelines they're used in."""
    adf_client = DataFactoryManagementClient(credential, subscription_id)
    
    # Then get all datasets with their details
    items: List[Dict[str, Any]] = []
    for ds in adf_client.datasets.list_by_factory(resource_group, factory_name):
        dd = _to_dict(ds)
        ds_name = dd.get("name") or getattr(ds, "name", None)
        if not ds_name:
            continue

        ls_name = _extract_linked_service_reference(dd)
        
        items.append({
            "Dataset": ds_name,
            "LinkedService": ls_name,
            "DataFactory": factory_name
        })
    
    return items


def list_dataset_io_for_factory(
    credential: InteractiveBrowserCredential,
    subscription_id: str,
    resource_group: str,
    factory_name: str,
) -> List[Dict[str, str]]:
    """List dataset input/output relationships in factory."""
    adf_client = DataFactoryManagementClient(credential, subscription_id)
    ds_map: Dict[str, Dict[str, Any]] = {}
    try:
        for ds in adf_client.datasets.list_by_factory(resource_group, factory_name):
            ds_name = getattr(ds, "name", None) or _to_dict(ds).get("name")
            if ds_name:
                ds_map[ds_name] = _to_dict(ds)
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
            _collect_dataset_io_rows(acts, rows, factory_name, name, ds_map)
    return rows


def _collect_dataset_io_rows(
    activities: Optional[List[Any]],
    rows: List[Dict[str, str]],
    factory_name: str,
    pipeline_name: str,
    ds_map: Dict[str, Dict[str, Any]],
) -> None:
    """Recursively collect dataset I/O relationships from activities."""
    if not activities:
        return
    for act in activities:
        a = _to_dict(act)
        name = a.get("name") or ""
        a_type = a.get("type") or ""

        def _find_dataset_refs(obj: Any, target_key: str) -> List[str]:
            refs = []
            if isinstance(obj, dict):
                io_list = obj.get(target_key)
                if isinstance(io_list, list):
                    for io_item in io_list:
                        if isinstance(io_item, dict):
                            ref_name = io_item.get("referenceName") or io_item.get("reference_name") or io_item.get("name")
                            if ref_name:
                                refs.append(ref_name)
                # Also check nested under properties
                props = obj.get("properties")
                if isinstance(props, dict):
                    io_list = props.get(target_key)
                    if isinstance(io_list, list):
                        for io_item in io_list:
                            if isinstance(io_item, dict):
                                ref_name = io_item.get("referenceName") or io_item.get("name")
                                if ref_name and ref_name not in refs:
                                    refs.append(ref_name)
            return refs

        raw_src_ds = _find_dataset_refs(a, "inputs")
        raw_sink_ds = _find_dataset_refs(a, "outputs")

        src_ds: List[str] = []
        src_ls: List[str] = []
        for rn in raw_src_ds:
            src_ds.append(rn)
            if rn in ds_map:
                ls_ref = _extract_linked_service_reference(ds_map[rn])
                if ls_ref:
                    src_ls.append(ls_ref)

        sink_ds: List[str] = []
        sink_ls: List[str] = []
        for rn in raw_sink_ds:
            sink_ds.append(rn)
            if rn in ds_map:
                ls_ref = _extract_linked_service_reference(ds_map[rn])
                if ls_ref:
                    sink_ls.append(ls_ref)

        if src_ds or sink_ds:
            rows.append({
                "Factory": factory_name,
                "Pipeline": pipeline_name,
                "Activity": name,
                "ActivityType": a_type,
                "SourceDatasets": ", ".join(src_ds),
                "SinkDatasets": ", ".join(sink_ds),
                "SourceLinkedServices": ", ".join(src_ls),
                "SinkLinkedServices": ", ".join(sink_ls),
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
                if isinstance(c, dict):
                    c_acts = c.get("activities")
                    if isinstance(c_acts, list):
                        inner_lists.append(c_acts)
        for lst in inner_lists:
            _collect_dataset_io_rows(lst, rows, factory_name, pipeline_name, ds_map)

def get_factory_relationships(
    credential: InteractiveBrowserCredential,
    subscription_id: str,
    resource_groups: List[str] = None,
) -> List[Dict[str, str]]:
    """
    Create a comprehensive table showing relationships between:
    - Data Factory
    - Datasets
    - Linked Services
    - Linked Service Types
    
    Args:
        credential: Azure authentication credential
        subscription_id: Azure subscription ID
        resource_groups: Optional list of resource groups to filter by
        
    Returns:
        List of dictionaries representing factory-dataset-linkedservice relationships
    """
    from azure.mgmt.datafactory import DataFactoryManagementClient
    from azure.mgmt.resource import ResourceManagementClient
    from utilities import _to_dict, _extract_linked_service_reference
    
    adf_client = DataFactoryManagementClient(credential, subscription_id)
    resource_client = ResourceManagementClient(credential, subscription_id)
    result = []
    
    # Get all resource groups if none specified
    if not resource_groups:
        resource_groups = [
            rg.name 
            for rg in resource_client.resource_groups.list()
            if rg.name
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