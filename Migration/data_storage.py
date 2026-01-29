"""
Data storage operations (Blob Storage, ADLS) for ADF to Fabric Migration Tool
"""

from typing import List, Dict, Any

import streamlit as st
from azure.identity import InteractiveBrowserCredential
from azure.mgmt.storage import StorageManagementClient
from azure.storage.blob import BlobServiceClient
from azure.storage.filedatalake import DataLakeServiceClient

from utilities import _to_dict, _path_info


@st.cache_data(show_spinner=False)
def list_storage_accounts(
    _credential: InteractiveBrowserCredential,
    subscription_id: str,
    resource_group: str,
) -> List[str]:
    """List storage accounts in a resource group."""
    smc = StorageManagementClient(_credential, subscription_id)
    return [sa.name for sa in smc.storage_accounts.list_by_resource_group(resource_group)]


@st.cache_data(show_spinner=False)
def list_blob_containers(
    _credential: InteractiveBrowserCredential,
    subscription_id: str,
    resource_group: str,
    account_name: str,
) -> List[str]:
    """List blob containers in a storage account."""
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


@st.cache_data(show_spinner=False)
def list_adls_filesystems(
    _credential: InteractiveBrowserCredential,
    account_name: str,
) -> List[str]:
    """List filesystems (containers) in an ADLS Gen2 storage account."""
    try:
        svc = _dfs_service(_credential, account_name)
        return [fs.name for fs in svc.list_file_systems()]
    except Exception as exc:
        raise exc


def _blob_service(
    _credential: InteractiveBrowserCredential,
    account_name: str,
) -> BlobServiceClient:
    """Create a Blob Service client."""
    return BlobServiceClient(account_url=f"https://{account_name}.blob.core.windows.net", credential=_credential)


def is_hns_enabled(
    _credential: InteractiveBrowserCredential,
    subscription_id: str,
    resource_group: str,
    account_name: str,
) -> bool:
    """Check if Hierarchical Namespace (HNS) is enabled on storage account."""
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
    """Create a Data Lake Service client."""
    return DataLakeServiceClient(account_url=f"https://{account_name}.dfs.core.windows.net", credential=_credential)


def list_adls_top_level_directories(
    _credential: InteractiveBrowserCredential,
    account_name: str,
    filesystem: str,
) -> List[Dict[str, str]]:
    """List top-level directories in ADLS filesystem."""
    try:
        svc = _dfs_service(_credential, account_name)
        fs = svc.get_file_system_client(filesystem)
        top_levels: Dict[str, Any] = {}
        # Use recursive=True to discover first segments even when only nested dirs exist
        for p in fs.get_paths(path="", recursive=True):
            info = _path_info(p)
            name = info.get("name") or ""
            if not name:
                continue
            is_dir = info.get("is_directory", False)
            # For directories, take their first segment; for files, skip
            if is_dir and "/" in name:
                first_segment = name.split("/")[0]
                if first_segment not in top_levels:
                    top_levels[first_segment] = info.get("last_modified")
            elif is_dir and "/" not in name:
                top_levels[name] = info.get("last_modified")
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
    """List files in ADLS directory."""
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
    """List top-level folders in blob container."""
    try:
        svc = _blob_service(_credential, account_name)
        cc = svc.get_container_client(container_name)
        folders = set()
        for blob in cc.list_blobs():
            name = getattr(blob, "name", "") or _to_dict(blob).get("name", "")
            if "/" in name:
                folder = name.split("/")[0]
                folders.add(folder)
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
    """List files in blob container folder."""
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
                pass
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
    """Sample file paths from ADLS filesystem."""
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


def list_top_level_folders(
    _credential: InteractiveBrowserCredential,
    account_name: str,
    container_name: str,
) -> List[str]:
    """List top-level folders in a blob container."""
    try:
        svc = _blob_service(_credential, account_name)
        cc = svc.get_container_client(container_name)
        folders = set()
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
    """List files in a specific folder within a blob container."""
    try:
        svc = _blob_service(_credential, account_name)
        cc = svc.get_container_client(container_name)
        prefix = folder.rstrip("/") + "/"
        files = []
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


def sample_blob_paths(
    _credential: InteractiveBrowserCredential,
    account_name: str,
    container_name: str,
    limit: int = 20,
) -> List[Dict[str, Any]]:
    """Sample blob paths from container."""
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
