from typing import List, Dict, Any, Optional
import requests

from azure.identity import InteractiveBrowserCredential

from Migration.adf_components import _activity_rows_helper


# ---------------------------------------------------------
# List Synapse Workspaces (ARM – still correct)
# ---------------------------------------------------------
def list_synapse_workspaces(
    credential: InteractiveBrowserCredential,
    subscription_id: str,
    resource_group: str,
) -> List[str]:
    from azure.mgmt.synapse import SynapseManagementClient

    client = SynapseManagementClient(credential, subscription_id)
    return [ws.name for ws in client.workspaces.list_by_resource_group(resource_group)]


# ---------------------------------------------------------
# Fetch Synapse Pipelines & Activities (DEV API – REQUIRED)
# ---------------------------------------------------------
def fetch_activity_rows_for_synapse(
    credential: InteractiveBrowserCredential,
    subscription_id: str,
    resource_group: str,
    workspace_name: str,
    ds_map: Optional[Dict[str, Dict[str, Any]]] = None,
) -> List[Dict[str, str]]:
    """
    Synapse pipelines are NOT ARM resources.
    They must be fetched via the Synapse Dev REST API.
    """

    # Acquire token for Synapse Dev API
    token = credential.get_token(
        "https://dev.azuresynapse.net/.default"
    ).token

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    base_url = f"https://{workspace_name}.dev.azuresynapse.net"

    # List pipelines
    pipelines_url = f"{base_url}/pipelines?api-version=2020-12-01"

    resp = requests.get(pipelines_url, headers=headers)
    resp.raise_for_status()

    pipelines = resp.json().get("value", [])

    rows: List[Dict[str, str]] = []

    for pipe in pipelines:
        pipeline_name = pipe.get("name", "")
        activities = pipe.get("properties", {}).get("activities", [])

        if not isinstance(activities, list):
            continue

        for activity in activities:
            row = _activity_rows_helper(
                activity=activity,
                factory_name=workspace_name,   # Synapse workspace
                pipeline_name=pipeline_name,
                ds_map=ds_map,
            )
            rows.append(row)

    return rows


import requests
from azure.identity import InteractiveBrowserCredential


def _get_synapse_dev_token(credential):
    token = credential.get_token("https://dev.azuresynapse.net/.default")
    return token.token


def list_synapse_linked_services(
    credential,
    synapse_workspace_name: str,
) -> list[dict]:

    token = _get_synapse_dev_token(credential)
    url = f"https://{synapse_workspace_name}.dev.azuresynapse.net/linkedservices?api-version=2020-12-01"

    headers = {
        "Authorization": f"Bearer {token}"
    }

    resp = requests.get(url, headers=headers)
    resp.raise_for_status()

    items = resp.json().get("value", [])

    return [{
        "LinkedServiceName": i["name"],
        "Type": i["properties"].get("type")
    } for i in items]


def list_synapse_datasets(
    credential,
    synapse_workspace_name: str,
) -> list[dict]:

    token = _get_synapse_dev_token(credential)
    url = f"https://{synapse_workspace_name}.dev.azuresynapse.net/datasets?api-version=2020-12-01"

    headers = {
        "Authorization": f"Bearer {token}"
    }

    resp = requests.get(url, headers=headers)
    resp.raise_for_status()

    items = resp.json().get("value", [])

    return [{
        "DatasetName": i["name"],
        "Type": i["properties"].get("type")
    } for i in items]
