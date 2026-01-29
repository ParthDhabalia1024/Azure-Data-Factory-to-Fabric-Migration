"""
SQL Server operations for ADF to Fabric Migration Tool
"""

from typing import Dict, List, Any, Set, Tuple

import streamlit as st
from azure.identity import InteractiveBrowserCredential
from azure.mgmt.sql import SqlManagementClient
from azure.mgmt.datafactory import DataFactoryManagementClient

from utilities import _to_dict, _parse_table_identifier


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
            text = str(props)
        except Exception:
            text = ""
        if not text:
            continue
        # Match either bare server name or fully qualified host
        server_match = server_lower in text or f"{server_lower}.database.windows.net" in text
        db_match = db_lower in text
        if server_match and db_match:
            target_ls.add(name)
            ls_rows[name] = {
                "Factory": factory_name,
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
    _ = list_sql_usage_for_database_from_adf(
        credential=credential,
        subscription_id=subscription_id,
        resource_group=resource_group,
        factory_name=factory_name,
        sql_server_name=sql_server_name,
        sql_database_name=sql_database_name,
    )
    # No dataset inspection in linked-service-only mode
    return []


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
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            # Get row count
            cursor.execute(f"SELECT COUNT(*) FROM [{schema}].[{table}]")
            result["row_count"] = cursor.fetchone()[0]
            # Get column info
            cursor.execute(f"SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?", schema, table)
            for row in cursor.fetchall():
                result["columns"].append({"Column": row[0], "Type": row[1]})
    except Exception as exc:
        msg = f"Failed to connect or run inspection: {exc}"
        result["error"] = f"{result['error']} | {msg}" if result["error"] else msg

    return result


def _list_sql_tables_via_pyodbc(conn_str: str) -> Dict[str, Any]:
    """List all tables in a SQL database."""
    result: Dict[str, Any] = {"tables": [], "error": ""}
    try:
        import pyodbc  # type: ignore[import]
    except ImportError:
        result["error"] = "pyodbc is not installed in this environment."
        return result

    try:
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'")
            result["tables"] = [{"Schema": row[0], "Table": row[1]} for row in cursor.fetchall()]
    except Exception as exc:
        result["error"] = f"Failed to list tables: {exc}"
    return result


def _get_db_properties_via_pyodbc(conn_str: str) -> Dict[str, Any]:
    """Get database properties."""
    result: Dict[str, Any] = {"properties": {}, "error": ""}
    try:
        import pyodbc  # type: ignore[import]
    except ImportError:
        result["error"] = "pyodbc is not installed in this environment."
        return result

    try:
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT name, collation_name, compatibility_level FROM sys.databases WHERE database_id = DB_ID()")
            row = cursor.fetchone()
            if row:
                result["properties"] = {"name": row[0], "collation": row[1], "compatibility_level": row[2]}
    except Exception as exc:
        result["error"] = f"Failed to fetch database properties: {exc}"
    return result


def _list_sql_views_via_pyodbc(conn_str: str) -> Dict[str, Any]:
    """List all views in a SQL database."""
    result: Dict[str, Any] = {"views": [], "error": ""}
    try:
        import pyodbc  # type: ignore[import]
    except ImportError:
        result["error"] = "pyodbc is not installed in this environment."
        return result

    try:
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'VIEW'")
            result["views"] = [{"Schema": row[0], "View": row[1]} for row in cursor.fetchall()]
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
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            # Build table metadata
            cursor.execute("""
                SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_SCHEMA, TABLE_NAME
            """)
            table_map: Dict[Tuple[str, str], Dict[str, Any]] = {}
            for row in cursor.fetchall():
                schema, table = row
                table_map[(schema, table)] = {
                    "Schema": schema,
                    "Table": table,
                    "RowCount": None,
                    "PrimaryKeys": "",
                    "UniqueConstraints": "",
                    "ForeignKeys": "",
                }
            
            # Get row counts
            for (schema, table) in table_map.keys():
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM [{schema}].[{table}]")
                    table_map[(schema, table)]["RowCount"] = cursor.fetchone()[0]
                except Exception:
                    pass

        # Convert to sorted list
        result["tables"] = [
            table_map[key]
            for key in sorted(table_map.keys(), key=lambda k: (k[0], k[1]))
        ]
    except Exception as exc:
        result["error"] = f"Failed to fetch table metadata: {exc}"
    return result
