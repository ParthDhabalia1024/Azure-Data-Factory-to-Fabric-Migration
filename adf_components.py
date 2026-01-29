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
    _extract_sql_query_from_activity,
    _get_io,
)
from migration_score import is_migratable, get_activity_category
from constants import CONTROL_ACTIVITY_TYPES


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


def _activity_rows_helper(activity: Dict[str, Any], factory_name: str, pipeline_name: str, ds_map: Optional[Dict[str, Dict[str, Any]]] = None) -> Dict[str, str]:
    """Create activity row for table display."""
    a_type = activity.get("type", "")
    name = activity.get("name", "")
    desc = activity.get("description", "")
    activated = "Yes" if not activity.get("isDisabled") else "No"
    query = _extract_sql_query_from_activity(activity, ds_map)

    # Output fields
    source_ds = ""
    sink_ds = ""
    source_ls = ""
    sink_ls = ""

    # Extract Source/Sink dataset for Copy
    if a_type.lower() == "copy":
        inputs = _get_io(activity, "inputs")
        outputs = _get_io(activity, "outputs")

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

        # Linked services from dataset map
        if isinstance(ds_map, dict):
            for src_name in source_ds_list:
                if src_name in ds_map:
                    src_ls = ds_map[src_name].get("linkedServiceName", "")
                    if src_ls:
                        source_ls = src_ls
                        break
            for sink_name in sink_ds_list:
                if sink_name in ds_map:
                    sink_ls = ds_map[sink_name].get("linkedServiceName", "")
                    if sink_ls:
                        sink_ls = sink_ls
                        break

    return {
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
    }


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
                            ref_name = io_item.get("referenceName") or io_item.get("name")
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
