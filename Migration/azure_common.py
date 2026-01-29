"""
Common Azure operations for ADF to Fabric Migration Tool
"""

from typing import List, Tuple, Dict, Any

import streamlit as st
from azure.identity import InteractiveBrowserCredential
from azure.mgmt.resource import SubscriptionClient, ResourceManagementClient
from azure.mgmt.datafactory import DataFactoryManagementClient

from utilities import _to_dict, _friendly_resource_type


@st.cache_data(show_spinner=False)
def list_subscriptions(_credential: InteractiveBrowserCredential) -> List[Tuple[str, str]]:
    """List all Azure subscriptions."""
    client = SubscriptionClient(_credential)
    subs = list(client.subscriptions.list())
    return [(s.display_name or s.subscription_id, s.subscription_id) for s in subs]


@st.cache_data(show_spinner=False)
def list_resource_groups(_credential: InteractiveBrowserCredential, subscription_id: str) -> List[str]:
    """List resource groups in a subscription."""
    rg_client = ResourceManagementClient(_credential, subscription_id)
    return [rg.name for rg in rg_client.resource_groups.list()]


@st.cache_data(show_spinner=False)
def list_data_factories(_credential: InteractiveBrowserCredential, subscription_id: str, resource_group: str) -> List[str]:
    """List data factories in a resource group."""
    adf_client = DataFactoryManagementClient(_credential, subscription_id)
    return [f.name for f in adf_client.factories.list_by_resource_group(resource_group)]


@st.cache_data(show_spinner=False)
def list_rg_resources(_credential: InteractiveBrowserCredential, subscription_id: str, resource_group: str) -> List[Dict[str, str]]:
    """List all resources in a resource group."""
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
