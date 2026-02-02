# import os
# import subprocess
# from collections import defaultdict
# from typing import Optional, Dict, List, Tuple, Any

# import streamlit as st
# from azure.identity import InteractiveBrowserCredential

# # Import from modular components
# from Migration.azure_common import (
#     list_subscriptions,
#     list_resource_groups,
#     list_data_factories,
#     list_rg_resources,
# )

# from Migration.adf_components import (
#     fetch_components_for_factory,
#     fetch_activity_rows_for_factory,
#     list_linked_services_for_factory,
#     list_datasets_for_factory,
# )

# from Migration.sql_server import (
#     list_sql_servers,
#     list_sql_databases_for_server,
#     list_sql_usage_for_database_from_adf,
#     _list_sql_tables_via_pyodbc,
#     _get_db_properties_via_pyodbc,
#     _list_sql_views_via_pyodbc,
#     _list_sql_table_overview_via_pyodbc,
# )

# from Migration.data_storage import (
#     list_storage_accounts,
#     list_blob_containers,
#     list_adls_filesystems,
#     is_hns_enabled,
#     list_adls_top_level_directories,
#     list_adls_files_in_directory,
#     list_top_level_folders,
#     list_files_in_folder,
#     sample_blob_paths,
#     sample_adls_paths,
# )

# from Migration.migration_score import (
#     score_component_parity,
#     score_non_migratable,
#     score_connectivity,
#     score_orchestration,
# )

# from Migration.utilities import _normalize_type
# from Migration.constants import CONTROL_ACTIVITY_TYPES
# from Migration.ui_config import apply_custom_theme, render_header_with_logo




# def main() -> None:
#     st.set_page_config(page_title="ADF to Fabric Migration Tool | OnPoint Insights", page_icon="🔷", layout="wide")
    
#     # Apply custom OnPoint Insights theme
#     apply_custom_theme()
    
#     # Get logo path
#     BASE_DIR = os.path.dirname(os.path.abspath(__file__))
#     UTILS_DIR = os.path.join(BASE_DIR, "utils")

#     logo_path = os.path.join(UTILS_DIR, "logo.png")

    
#     # Custom header with branding and logo
#     render_header_with_logo(
#         "Azure Data Factory to Fabric Migration",
#         "Powered by OnPoint Insights",
#         logo_path=logo_path
#     )
#     st.markdown("---")
#     st.info("Sign in to Azure, select your subscription, resource group, and Data Factory, then migrate pipelines to Microsoft Fabric.")

#     if "credential" not in st.session_state:
#         st.session_state.credential = None

#     # Sign-in section
#     with st.container(border=True):
#         st.subheader("🔐 Sign in to Azure")
#         col1, col2 = st.columns([1, 3])
#         with col1:
#             login_clicked = st.button("🔑 Sign In with Azure", type="primary", use_container_width=True)
#         with col2:
#             st.markdown("**Status:** " + ("✅ Signed in" if st.session_state.credential else "❌ Not signed in"))
#         if login_clicked or st.session_state.credential is None:
#             try:
#                 cred = InteractiveBrowserCredential()
#                 # Test by listing subscriptions to complete device login
#                 subs = list_subscriptions(_credential=cred)
#                 st.session_state.credential = cred
#                 st.success("✅ Signed in successfully.")
#             except Exception as e:
#                 st.error(f"❌ Sign-in failed: {e}")
#                 return

#     credential: Optional[InteractiveBrowserCredential] = st.session_state.credential
#     if credential is None:
#         st.stop()

#     # Subscription selection
#     with st.container(border=True):
#         st.subheader("📋 Select Subscription")
#         try:
#             subs = list_subscriptions(_credential=credential)
#         except Exception as e:
#             st.error(f"Failed to list subscriptions: {e}")
#             st.stop()
#         sub_labels = [f"{name} ({sid})" for name, sid in subs]
#         sub_idx = st.selectbox("Subscription", options=list(range(len(subs))), format_func=lambda i: sub_labels[i] if subs else "", index=0 if subs else None)
#         if subs:
#             subscription_id = subs[sub_idx][1]
#         else:
#             st.warning("No subscriptions available.")
#             st.stop()

#     # Resource group selection
#     with st.container(border=True):
#         st.subheader("📁 Select Resource Group")
#         try:
#             rgs = list_resource_groups(_credential=credential, subscription_id=subscription_id)
#         except Exception as e:
#             st.error(f"Failed to list resource groups: {e}")
#             st.stop()
#         if not rgs:
#             st.warning("No resource groups in this subscription.")
#             st.stop()
#         rg_name = st.selectbox("Resource group", options=rgs, index=0)
#         try:
#             res_rows = list_rg_resources(
#                 _credential=credential,
#                 subscription_id=subscription_id,
#                 resource_group=rg_name,
#             )
#             if res_rows:
#                 st.caption(f"Resources in '{rg_name}' ({len(res_rows)} found)")
#                 st.dataframe(res_rows, hide_index=True, width="stretch")
#                                 # -------------------------------
                

#             else:
#                 st.info("No resources found in this resource group.")
#         except Exception as e:
#             st.warning(f"Could not list resources in '{rg_name}': {e}")

#         # ========== DATA FACTORIES SECTION ==========
#         if "selected_df" not in st.session_state:
#             st.session_state.selected_df = None
#         selected_df: Optional[str] = st.session_state.selected_df
#         clicked_df: Optional[str] = None
#         try:
#             factories = list_data_factories(
#                 _credential=credential,
#                 subscription_id=subscription_id,
#                 resource_group=rg_name,
#             )
#         except Exception as e:
#             factories = []
#             st.warning(f"Could not list data factories: {e}")
        
#         if factories:
#             st.caption("📊 Open a Data Factory:")
#             cols = st.columns(min(4, max(1, len(factories))))
#             for i, fac in enumerate(factories):
#                 if cols[i % len(cols)].button(fac, key=f"open_df_{fac}"):
#                     clicked_df = fac
        
#         if clicked_df:
#             st.session_state.selected_df = clicked_df
#             selected_df = clicked_df
        
#         if selected_df and selected_df not in factories:
#             st.session_state.selected_df = None
#             selected_df = None
        
#         if selected_df:
#             st.markdown("---")
#             st.subheader(f"🔍 Data Factory: {selected_df}")

#             # Fetch components and linked services
#             try:
#                 act_rows = fetch_activity_rows_for_factory(credential, subscription_id, rg_name, selected_df)
#             except Exception as e:
#                 st.error(f"Failed to fetch components: {e}")
#                 act_rows = []
            
#             try:
#                 ls_rows = list_linked_services_for_factory(credential, subscription_id, rg_name, selected_df)
#                 ls_types = [row.get("LinkedServiceType", "") for row in ls_rows]
#             except Exception as e:
#                 ls_rows = []
#                 ls_types = []

#             # 1) Pipelines and Activities
#             with st.container(border=True):
#                 st.subheader("📋 Pipelines and Activities")
#                 if act_rows:
#                     st.dataframe(act_rows, width="stretch", hide_index=True)
#                 else:
#                     st.info("No components found.")

#             # 2) Migration Scoring
#             with st.container(border=True):
#                 st.subheader("📈 Migration Scoring (Fabric Readiness Assessment)")
#                 grouped: Dict[Tuple[str, str], List[Dict[str, str]]] = defaultdict(list)
#                 for r in act_rows:
#                     grouped[(r.get("Factory", ""), r.get("PipelineName", ""))].append(r)
#                 score_rows: List[Dict[str, Any]] = []
#                 for (fac, pipe), items in grouped.items():
#                     total_acts = len(items)
#                     non_migratable = sum(1 for it in items if (it.get("Migratable") or "").lower() == "no")
#                     control_acts = 0
#                     for it in items:
#                         nt = _normalize_type(it.get("ActivityType"))
#                         if nt in CONTROL_ACTIVITY_TYPES:
#                             control_acts += 1
#                     parity_score = score_component_parity(total_acts, non_migratable)
#                     non_mig_score = score_non_migratable(non_migratable)
#                     connectivity_score = score_connectivity(ls_types)
#                     orchestration_score = score_orchestration(total_acts, control_acts)
#                     total = parity_score + non_mig_score + connectivity_score + orchestration_score
#                     if 3 in (parity_score, non_mig_score, connectivity_score, orchestration_score):
#                         band = "🔴 Hard"
#                     elif total <= 4:
#                         band = "🟢 Easy"
#                     elif total <= 8:
#                         band = "🟡 Medium"
#                     else:
#                         band = "🔴 Hard"
#                     score_rows.append({
#                         "Factory": fac,
#                         "Pipeline": pipe,
#                         "Component Parity": parity_score,
#                         "Non-Migratable": non_mig_score,
#                         "Connectivity": connectivity_score,
#                         "Orchestration": orchestration_score,
#                         "Total Score": total,
#                         "Difficulty": band,
#                         "Activities": total_acts,
#                         "Non-Migratable Count": non_migratable,
#                     })
#                 if score_rows:
#                     st.dataframe(score_rows, width="stretch", hide_index=True)
#                 else:
#                     st.info("No pipelines to score.")

#             # 3) Linked Services
#             with st.container(border=True):
#                 st.subheader("🔗 Linked Services")
#                 if ls_rows:
#                     st.dataframe(ls_rows, width="stretch", hide_index=True)
#                 else:
#                     st.info("No linked services found.")

#             # 4) Datasets
#             with st.container(border=True):
#                 st.subheader("📦 Datasets")
#                 try:
#                     ds_rows = list_datasets_for_factory(credential, subscription_id, rg_name, selected_df)
#                 except Exception as e:
#                     ds_rows = []
#                     st.error(f"Failed to list datasets: {e}")
#                 if ds_rows:
#                     st.dataframe(ds_rows, width="stretch", hide_index=True)
#                 else:
#                     st.info("No datasets found.")

#             # 5) Migration to Fabric
#             with st.container(border=True):
#                 st.subheader("🚀 Migrate to Microsoft Fabric")
#                 pipeline_names = sorted({row.get("Pipeline") for row in score_rows if row.get("Pipeline")})
#                 if not pipeline_names:
#                     st.info("No pipelines available to migrate for this Data Factory.")
#                 else:
#                     migrate_all = st.checkbox(
#                         "Migrate all pipelines",
#                         value=True,
#                         key=f"migrate_all_{selected_df}",
#                     )
#                     if migrate_all:
#                         pipelines_to_migrate = pipeline_names
#                     else:
#                         pipelines_to_migrate = st.multiselect(
#                             "Select pipelines to migrate",
#                             options=pipeline_names,
#                             default=pipeline_names,
#                             key=f"pipelines_to_migrate_{selected_df}",
#                         )

#                     # Workspace ID input
#                     workspace_id = st.text_input(
#                         "Fabric Workspace ID",
#                         placeholder="Enter your Fabric Workspace ID (UUID format)",
#                         key=f"workspace_id_{selected_df}",
#                     )

#                     run_migration = st.button(
#                         "🔄 Migrate Selected Pipelines to Fabric",
#                         type="primary",
#                         key=f"run_migration_{selected_df}",
#                     )
#                     if run_migration:
#                         if not pipelines_to_migrate:
#                             st.warning("Please select at least one pipeline to migrate.")
#                         elif not workspace_id:
#                             st.warning("Please enter a Fabric Workspace ID.")
#                         else:
#                             script_path = os.path.join(UTILS_DIR, "adf_to_fabric_migration.ps1")
#                             resolutions_file = os.path.join(UTILS_DIR, "resolutions.json")
#                             region = "prod"
#                             cmd = [
#                                 "pwsh",
#                                 "-File",
#                                 script_path,
#                                 "-FabricWorkspaceId",
#                                 workspace_id,
#                                 "-ResolutionsFile",
#                                 resolutions_file,
#                                 "-Region",
#                                 region,
#                                 "-SubscriptionId",
#                                 subscription_id,
#                                 "-ResourceGroupName",
#                                 rg_name,
#                                 "-DataFactoryName",
#                                 selected_df,
#                                 "-PipelineNames",
#                                 ",".join(pipelines_to_migrate),
#                             ]
#                             try:
#                                 with st.spinner("Running migration in PowerShell (this may take a few minutes)..."):
#                                     result = subprocess.run(
#                                         cmd,
#                                         capture_output=True,
#                                         text=True,
#                                     )
#                             except FileNotFoundError:
#                                 st.error("Failed to start PowerShell 7 (pwsh). Ensure PowerShell 7 is installed and available in PATH.")
#                             except Exception as exc:
#                                 st.error(f"Failed to launch migration script: {exc}")
#                             else:
#                                 if result.returncode == 0:
#                                     st.success("✅ Migration script completed. Check Microsoft Fabric and the Logs folder for details.")
#                                 else:
#                                     st.error(f"❌ Migration script exited with code {result.returncode}.")
#                                 if result.stdout:
#                                     st.caption("PowerShell output:")
#                                     st.code(result.stdout, language="powershell")
#                                 if result.stderr:
#                                     st.caption("PowerShell errors:")
#                                     st.code(result.stderr, language="powershell")

#         # ========== SQL SERVERS SECTION ==========
#         if "selected_sql_server" not in st.session_state:
#             st.session_state.selected_sql_server = None
#         selected_sql_server: Optional[str] = st.session_state.selected_sql_server
#         clicked_sql_server: Optional[str] = None
#         try:
#             sql_servers = list_sql_servers(credential, subscription_id, rg_name)
#         except Exception as e:
#             sql_servers = []
#             st.warning(f"Could not list SQL servers: {e}")
        
#         if sql_servers:
#             st.caption("🗄️ Open an Azure SQL Server:")
#             cols_sql = st.columns(min(4, max(1, len(sql_servers))))
#             for i, srv in enumerate(sql_servers):
#                 if cols_sql[i % len(cols_sql)].button(srv, key=f"open_sql_{srv}"):
#                     clicked_sql_server = srv
        
#         if clicked_sql_server:
#             st.session_state.selected_sql_server = clicked_sql_server
#             selected_sql_server = clicked_sql_server
        
#         if selected_sql_server and selected_sql_server not in sql_servers:
#             st.session_state.selected_sql_server = None
#             selected_sql_server = None
        
#         if selected_sql_server:
#             st.markdown("---")
#             st.subheader(f"🔍 SQL Server: {selected_sql_server}")
            
#             try:
#                 db_rows = list_sql_databases_for_server(
#                     _credential=credential,
#                     subscription_id=subscription_id,
#                     resource_group=rg_name,
#                     server_name=selected_sql_server,
#                 )
#             except Exception as e:
#                 db_rows = []
#                 st.error(f"Failed to list databases: {e}")
            
#             if db_rows:
#                 st.caption("Databases on this server")
#                 st.dataframe(db_rows, hide_index=True, width="stretch")

#                 # Per-database buttons
#                 if "selected_sql_database" not in st.session_state:
#                     st.session_state.selected_sql_database = None
#                 selected_sql_database: Optional[str] = st.session_state.selected_sql_database
#                 clicked_db: Optional[str] = None
#                 db_names = [row.get("Database", "") for row in db_rows if row.get("Database")]
                
#                 if db_names:
#                     st.caption("📂 Open a Database:")
#                     db_cols = st.columns(min(4, max(1, len(db_names))))
#                     for i, db_name in enumerate(db_names):
#                         if db_cols[i % len(db_cols)].button(db_name, key=f"open_db_{selected_sql_server}_{db_name}"):
#                             clicked_db = db_name
                
#                 if clicked_db:
#                     st.session_state.selected_sql_database = clicked_db
#                     selected_sql_database = clicked_db
                
#                 if selected_sql_database and selected_sql_database not in db_names:
#                     st.session_state.selected_sql_database = None
#                     selected_sql_database = None
                
#                 if selected_sql_database:
#                     st.markdown("---")
#                     st.subheader(f"📊 Database: {selected_sql_database}")
                    
#                     default_db_conn_hint = (
#                         "DRIVER={ODBC Driver 18 for SQL Server};"
#                         f"SERVER={selected_sql_server};"
#                         f"DATABASE={selected_sql_database};"
#                         "UID=your-user;PWD=your-password;Encrypt=yes;TrustServerCertificate=no;"
#                     )
#                     db_conn_str = st.text_input(
#                         "SQL Connection String (for listing tables, views, etc.)",
#                         value="",
#                         placeholder=default_db_conn_hint,
#                         key=f"db_conn_{selected_sql_server}_{selected_sql_database}",
#                     )
                    
#                     if db_conn_str:
#                         if st.button("📋 Load Database Components", key=f"btn_list_tables_{selected_sql_server}_{selected_sql_database}"):
#                             # Database properties
#                             db_info = _get_db_properties_via_pyodbc(db_conn_str)
#                             if db_info.get("error"):
#                                 st.error(f"❌ {db_info['error']}")
#                             else:
#                                 props = db_info.get("properties") or {}
#                                 if props:
#                                     st.markdown("#### 🗂️ Database Properties")
#                                     st.dataframe([props], hide_index=True, width="stretch")
#                                     st.divider()

#                             # Views
#                             view_info = _list_sql_views_via_pyodbc(db_conn_str)
#                             if view_info.get("error"):
#                                 st.error(f"❌ {view_info['error']}")
#                             else:
#                                 views = view_info.get("views") or []
#                                 if views:
#                                     st.markdown("#### 👁️ Views")
#                                     st.dataframe(views, hide_index=True, width="stretch")
#                                     st.divider()
#                                 else:
#                                     st.info("No views found in this database.")

#                             # Tables with metadata
#                             st.markdown("#### 📑 Tables with Row Counts & Constraints")
#                             tbl_info = _list_sql_table_overview_via_pyodbc(db_conn_str)
#                             if tbl_info.get("error"):
#                                 st.error(f"❌ Error loading tables: {tbl_info['error']}")
#                             else:
#                                 tbl_rows = tbl_info.get("tables") or []
#                                 if tbl_rows:
#                                     st.dataframe(tbl_rows, hide_index=True, width="stretch")
#                                 else:
#                                     st.info("ℹ️ No tables found in this database.")
#                     else:
#                         st.info("💡 Enter a SQL connection string above to load database components.")
#             else:
#                 st.info("No databases found on this server.")

#         # ========== STORAGE ACCOUNTS SECTION ==========
#         if "selected_sa" not in st.session_state:
#             st.session_state.selected_sa = None
#         selected_sa: Optional[str] = st.session_state.selected_sa
#         previous_sa: Optional[str] = selected_sa
#         clicked_sa: Optional[str] = None
#         try:
#             storage_accounts = list_storage_accounts(
#                 _credential=credential,
#                 subscription_id=subscription_id,
#                 resource_group=rg_name,
#             )
#         except Exception as e:
#             storage_accounts = []
#             st.warning(f"Could not list storage accounts: {e}")
#         if storage_accounts:
#             st.caption("Open a Storage Account:")
#             cols_sa = st.columns(min(4, max(1, len(storage_accounts))))
#             for i, sa in enumerate(storage_accounts):
#                 if cols_sa[i % len(cols_sa)].button(sa, key=f"open_sa_{sa}"):
#                     clicked_sa = sa
#         if clicked_sa:
#             st.session_state.selected_sa = clicked_sa
#             selected_sa = clicked_sa
#         if selected_sa and selected_sa not in storage_accounts:
#             st.session_state.selected_sa = None
#             selected_sa = None
#         if selected_sa != previous_sa:
#             st.session_state.storage_selection = {}
#         if selected_sa:
#             st.subheader(f"Storage account: {selected_sa}")
#             # Account-level summary
#             try:
#                 hns = is_hns_enabled(credential, subscription_id, rg_name, selected_sa)
#             except Exception as e:
#                 hns = False
#                 st.warning(f"Could not determine HNS setting: {e}")
#             try:
#                 containers = list_blob_containers(credential, subscription_id, rg_name, selected_sa)
#             except Exception as e:
#                 containers = []
#                 st.error(f"Failed to list containers/filesystems: {e}")
#             summary_rows = [{
#                 "Account": selected_sa,
#                 "HNSEnabled": "Yes" if hns else "No",
#                 "ContainerCount": len(containers),
#             }]
#             st.dataframe(summary_rows, hide_index=True, width="stretch")

#             st.markdown("**Browse folders:**")
#             if "storage_selection" not in st.session_state:
#                 st.session_state.storage_selection = {}
#             selection_state: Dict[str, Dict[str, Optional[str]]] = st.session_state.storage_selection

#             for c in containers:
#                 key = f"storage_{selected_sa}_{c}"
#                 sel = selection_state.setdefault(key, {"folder": None})
#                 try:
#                     if hns:
#                         top_dirs = list_adls_top_level_directories(credential, selected_sa, c)
#                         folders = [row.get("Folder") for row in top_dirs if row.get("Folder")]
#                         with st.expander(f"Folders in Container: {c}", expanded=bool(sel.get("folder"))):
#                             if folders:
#                                 folder_cols = st.columns(min(4, max(1, len(folders))))
#                                 for idx, folder in enumerate(folders):
#                                     if folder_cols[idx % len(folder_cols)].button(folder, key=f"{key}_folder_{folder}"):
#                                         sel["folder"] = folder
#                             else:
#                                 st.info("No top-level folders detected.")
#                             if sel.get("folder"):
#                                 st.caption(f"Selected folder: {sel['folder']}")
#                                 try:
#                                     files = list_adls_files_in_directory(credential, selected_sa, c, sel["folder"], max_items=200)
#                                     if files:
#                                         st.write(f"Files in {sel['folder']} (up to 200)")
#                                         st.dataframe(files, hide_index=True, width="stretch")
#                                     else:
#                                         st.info("No files found in this folder.")
#                                 except Exception as e:
#                                     st.warning(f"Failed to list files: {e}")
#                     else:
#                         top_folders = list_top_level_folders(credential, selected_sa, c)
#                         with st.expander(f"Folders in {c}", expanded=bool(sel.get("folder"))):
#                             if top_folders:
#                                 folder_cols = st.columns(min(4, max(1, len(top_folders))))
#                                 for idx, folder in enumerate(top_folders):
#                                     if folder_cols[idx % len(folder_cols)].button(folder, key=f"{key}_folder_{folder}"):
#                                         sel["folder"] = folder
#                             else:
#                                 st.info("No top-level folders detected.")
#                             if sel.get("folder"):
#                                 st.caption(f"Selected folder: {sel['folder']}")
#                                 try:
#                                     files = list_files_in_folder(credential, selected_sa, c, sel["folder"], max_items=200)
#                                     if files:
#                                         st.write(f"Files in {sel['folder']} (up to 200)")
#                                         st.dataframe(files, hide_index=True, width="stretch")
#                                     else:
#                                         st.info("No files found in this folder.")
#                                 except Exception as e:
#                                     st.warning(f"Failed to list files: {e}")
#                 except Exception as e:
#                     st.warning(f"Failed to browse container '{c}': {e}")


# if __name__ == "__main__":
#     main()


# import os
# import subprocess
# from collections import defaultdict
# from typing import Optional, Dict, List, Tuple, Any

# import streamlit as st
# from azure.identity import InteractiveBrowserCredential

# # Import from modular components
# from Migration.azure_common import (
#     list_subscriptions,
#     list_resource_groups,
#     list_data_factories,
#     list_rg_resources,
# )

# from Migration.adf_components import (
#     fetch_components_for_factory,
#     fetch_activity_rows_for_factory,
#     list_linked_services_for_factory,
#     list_datasets_for_factory,
# )

# from Migration.synapse_components import (
#     list_synapse_workspaces,
#     fetch_activity_rows_for_synapse,
# )

# from Migration.sql_server import (
#     list_sql_servers,
#     list_sql_databases_for_server,
#     list_sql_usage_for_database_from_adf,
#     _list_sql_tables_via_pyodbc,
#     _get_db_properties_via_pyodbc,
#     _list_sql_views_via_pyodbc,
#     _list_sql_table_overview_via_pyodbc,
# )

# from Migration.data_storage import (
#     list_storage_accounts,
#     list_blob_containers,
#     list_adls_filesystems,
#     is_hns_enabled,
#     list_adls_top_level_directories,
#     list_adls_files_in_directory,
#     list_top_level_folders,
#     list_files_in_folder,
#     sample_blob_paths,
#     sample_adls_paths,
# )

# from Migration.migration_score import (
#     score_component_parity,
#     score_non_migratable,
#     score_connectivity,
#     score_orchestration,
# )

# from Migration.utilities import _normalize_type
# from Migration.constants import CONTROL_ACTIVITY_TYPES
# from Migration.ui_config import apply_custom_theme, render_header_with_logo


# def main() -> None:
#     st.set_page_config(
#         page_title="ADF to Fabric Migration Tool | OnPoint Insights",
#         page_icon="🔷",
#         layout="wide"
#     )

#     # Apply custom OnPoint Insights theme
#     apply_custom_theme()

#     # Paths
#     BASE_DIR = os.path.dirname(os.path.abspath(__file__))
#     UTILS_DIR = os.path.join(BASE_DIR, "utils")
#     logo_path = os.path.join(UTILS_DIR, "logo.png")

#     # Custom header with branding and logo
#     render_header_with_logo(
#         "Azure Data Factory to Fabric Migration",
#         "Powered by OnPoint Insights",
#         logo_path=logo_path
#     )
#     st.markdown("---")
#     st.info(
#         "Sign in to Azure, select your subscription, resource group, and Data Factory / Synapse Workspace, then migrate pipelines to Microsoft Fabric."
#     )

#     if "credential" not in st.session_state:
#         st.session_state.credential = None

#     # Sign-in section
#     with st.container(border=True):
#         st.subheader("🔐 Sign in to Azure")
#         col1, col2 = st.columns([1, 3])
#         with col1:
#             login_clicked = st.button("🔑 Sign In with Azure", type="primary", use_container_width=True)
#         with col2:
#             st.markdown("**Status:** " + ("✅ Signed in" if st.session_state.credential else "❌ Not signed in"))
#         if login_clicked or st.session_state.credential is None:
#             try:
#                 cred = InteractiveBrowserCredential()
#                 subs = list_subscriptions(_credential=cred)
#                 st.session_state.credential = cred
#                 st.success("✅ Signed in successfully.")
#             except Exception as e:
#                 st.error(f"❌ Sign-in failed: {e}")
#                 return

#     credential: Optional[InteractiveBrowserCredential] = st.session_state.credential
#     if credential is None:
#         st.stop()

#     # Subscription selection
#     with st.container(border=True):
#         st.subheader("📋 Select Subscription")
#         try:
#             subs = list_subscriptions(_credential=credential)
#         except Exception as e:
#             st.error(f"Failed to list subscriptions: {e}")
#             st.stop()

#         sub_labels = [f"{name} ({sid})" for name, sid in subs]
#         sub_idx = st.selectbox(
#             "Subscription",
#             options=list(range(len(subs))),
#             format_func=lambda i: sub_labels[i] if subs else "",
#             index=0 if subs else None
#         )
#         if subs:
#             subscription_id = subs[sub_idx][1]
#         else:
#             st.warning("No subscriptions available.")
#             st.stop()

#     # Resource group selection
#     with st.container(border=True):
#         st.subheader("📁 Select Resource Group")
#         try:
#             rgs = list_resource_groups(_credential=credential, subscription_id=subscription_id)
#         except Exception as e:
#             st.error(f"Failed to list resource groups: {e}")
#             st.stop()

#         if not rgs:
#             st.warning("No resource groups in this subscription.")
#             st.stop()

#         rg_name = st.selectbox("Resource group", options=rgs, index=0)

#         # RG resources table
#         try:
#             res_rows = list_rg_resources(
#                 _credential=credential,
#                 subscription_id=subscription_id,
#                 resource_group=rg_name,
#             )
#             if res_rows:
#                 st.caption(f"Resources in '{rg_name}' ({len(res_rows)} found)")
#                 st.dataframe(res_rows, hide_index=True, width="stretch")
#             else:
#                 st.info("No resources found in this resource group.")
#         except Exception as e:
#             st.warning(f"Could not list resources in '{rg_name}': {e}")

#         # ==========================================================
#         # ========== DATA FACTORIES SECTION (UNCHANGED UI) ==========
#         # ==========================================================
#         if "selected_df" not in st.session_state:
#             st.session_state.selected_df = None

#         selected_df: Optional[str] = st.session_state.selected_df
#         clicked_df: Optional[str] = None

#         try:
#             factories = list_data_factories(
#                 _credential=credential,
#                 subscription_id=subscription_id,
#                 resource_group=rg_name,
#             )
#         except Exception as e:
#             factories = []
#             st.warning(f"Could not list data factories: {e}")

#         if factories:
#             st.caption("📊 Open a Data Factory:")
#             cols = st.columns(min(4, max(1, len(factories))))
#             for i, fac in enumerate(factories):
#                 if cols[i % len(cols)].button(fac, key=f"open_df_{fac}"):
#                     clicked_df = fac

#         if clicked_df:
#             st.session_state.selected_df = clicked_df
#             selected_df = clicked_df

#         if selected_df and selected_df not in factories:
#             st.session_state.selected_df = None
#             selected_df = None

#         if selected_df:
#             st.markdown("---")
#             st.subheader(f"🔍 Data Factory: {selected_df}")

#             # Fetch activities
#             try:
#                 act_rows = fetch_activity_rows_for_factory(credential, subscription_id, rg_name, selected_df)
#             except Exception as e:
#                 st.error(f"Failed to fetch components: {e}")
#                 act_rows = []

#             # Linked Services
#             try:
#                 ls_rows = list_linked_services_for_factory(credential, subscription_id, rg_name, selected_df)
#                 ls_types = [row.get("LinkedServiceType", "") for row in ls_rows]
#             except Exception:
#                 ls_rows = []
#                 ls_types = []

#             # 1) Pipelines and Activities
#             with st.container(border=True):
#                 st.subheader("📋 Pipelines and Activities")
#                 if act_rows:
#                     st.dataframe(act_rows, width="stretch", hide_index=True)
#                 else:
#                     st.info("No components found.")

#             # 2) Migration Scoring
#             with st.container(border=True):
#                 st.subheader("📈 Migration Scoring (Fabric Readiness Assessment)")
#                 grouped: Dict[Tuple[str, str], List[Dict[str, str]]] = defaultdict(list)
#                 for r in act_rows:
#                     grouped[(r.get("Factory", ""), r.get("PipelineName", ""))].append(r)

#                 score_rows: List[Dict[str, Any]] = []
#                 for (fac, pipe), items in grouped.items():
#                     total_acts = len(items)
#                     non_migratable = sum(1 for it in items if (it.get("Migratable") or "").lower() == "no")
#                     control_acts = 0
#                     for it in items:
#                         nt = _normalize_type(it.get("ActivityType"))
#                         if nt in CONTROL_ACTIVITY_TYPES:
#                             control_acts += 1

#                     parity_score = score_component_parity(total_acts, non_migratable)
#                     non_mig_score = score_non_migratable(non_migratable)
#                     connectivity_score = score_connectivity(ls_types)
#                     orchestration_score = score_orchestration(total_acts, control_acts)
#                     total = parity_score + non_mig_score + connectivity_score + orchestration_score

#                     if 3 in (parity_score, non_mig_score, connectivity_score, orchestration_score):
#                         band = "🔴 Hard"
#                     elif total <= 4:
#                         band = "🟢 Easy"
#                     elif total <= 8:
#                         band = "🟡 Medium"
#                     else:
#                         band = "🔴 Hard"

#                     score_rows.append({
#                         "Factory": fac,
#                         "Pipeline": pipe,
#                         "Component Parity": parity_score,
#                         "Non-Migratable": non_mig_score,
#                         "Connectivity": connectivity_score,
#                         "Orchestration": orchestration_score,
#                         "Total Score": total,
#                         "Difficulty": band,
#                         "Activities": total_acts,
#                         "Non-Migratable Count": non_migratable,
#                     })

#                 if score_rows:
#                     st.dataframe(score_rows, width="stretch", hide_index=True)
#                 else:
#                     st.info("No pipelines to score.")

#             # 3) Linked Services
#             with st.container(border=True):
#                 st.subheader("🔗 Linked Services")
#                 if ls_rows:
#                     st.dataframe(ls_rows, width="stretch", hide_index=True)
#                 else:
#                     st.info("No linked services found.")

#             # 4) Datasets
#             with st.container(border=True):
#                 st.subheader("📦 Datasets")
#                 try:
#                     ds_rows = list_datasets_for_factory(credential, subscription_id, rg_name, selected_df)
#                 except Exception as e:
#                     ds_rows = []
#                     st.error(f"Failed to list datasets: {e}")

#                 if ds_rows:
#                     st.dataframe(ds_rows, width="stretch", hide_index=True)
#                 else:
#                     st.info("No datasets found.")

#             # 5) Migration to Fabric (ADF) - uses existing script
#             with st.container(border=True):
#                 st.subheader("🚀 Migrate ADF Pipelines to Microsoft Fabric")

#                 pipeline_names = sorted({row.get("Pipeline") for row in score_rows if row.get("Pipeline")})
#                 if not pipeline_names:
#                     st.info("No pipelines available to migrate for this Data Factory.")
#                 else:
#                     migrate_all = st.checkbox(
#                         "Migrate all pipelines",
#                         value=True,
#                         key=f"migrate_all_{selected_df}",
#                     )
#                     if migrate_all:
#                         pipelines_to_migrate = pipeline_names
#                     else:
#                         pipelines_to_migrate = st.multiselect(
#                             "Select pipelines to migrate",
#                             options=pipeline_names,
#                             default=pipeline_names,
#                             key=f"pipelines_to_migrate_{selected_df}",
#                         )

#                     workspace_id = st.text_input(
#                         "Fabric Workspace ID",
#                         placeholder="Enter your Fabric Workspace ID (UUID format)",
#                         key=f"workspace_id_adf_{selected_df}",
#                     )

#                     run_migration = st.button(
#                         "🔄 Migrate Selected ADF Pipelines to Fabric",
#                         type="primary",
#                         key=f"run_migration_adf_{selected_df}",
#                     )

#                     if run_migration:
#                         if not pipelines_to_migrate:
#                             st.warning("Please select at least one pipeline to migrate.")
#                         elif not workspace_id:
#                             st.warning("Please enter a Fabric Workspace ID.")
#                         else:
#                             script_path = os.path.join(UTILS_DIR, "adf_to_fabric_migration.ps1")
#                             resolutions_file = os.path.join(UTILS_DIR, "resolutions.json")
#                             region = "prod"

#                             cmd = [
#                                 "pwsh",
#                                 "-File",
#                                 script_path,
#                                 "-FabricWorkspaceId",
#                                 workspace_id,
#                                 "-ResolutionsFile",
#                                 resolutions_file,
#                                 "-Region",
#                                 region,
#                                 "-SubscriptionId",
#                                 subscription_id,
#                                 "-ResourceGroupName",
#                                 rg_name,
#                                 "-DataFactoryName",
#                                 selected_df,
#                                 "-PipelineNames",
#                                 ",".join(pipelines_to_migrate),
#                             ]

#                             try:
#                                 with st.spinner("Running migration in PowerShell..."):
#                                     result = subprocess.run(cmd, capture_output=True, text=True)
#                             except FileNotFoundError:
#                                 st.error("Failed to start pwsh. Ensure PowerShell 7 is installed and in PATH.")
#                             except Exception as exc:
#                                 st.error(f"Failed to launch migration script: {exc}")
#                             else:
#                                 if result.returncode == 0:
#                                     st.success("✅ Migration script completed. Check Microsoft Fabric + Logs.")
#                                 else:
#                                     st.error(f"❌ Migration script exited with code {result.returncode}.")

#                                 if result.stdout:
#                                     st.caption("PowerShell output:")
#                                     st.code(result.stdout, language="powershell")

#                                 if result.stderr:
#                                     st.caption("PowerShell errors:")
#                                     st.code(result.stderr, language="powershell")

#         # ==========================================================
#         # ========== SQL SERVERS SECTION (UNCHANGED) ===============
#         # ==========================================================
#         if "selected_sql_server" not in st.session_state:
#             st.session_state.selected_sql_server = None
#         selected_sql_server: Optional[str] = st.session_state.selected_sql_server
#         clicked_sql_server: Optional[str] = None
#         try:
#             sql_servers = list_sql_servers(credential, subscription_id, rg_name)
#         except Exception as e:
#             sql_servers = []
#             st.warning(f"Could not list SQL servers: {e}")

#         if sql_servers:
#             st.caption("🗄️ Open an Azure SQL Server:")
#             cols_sql = st.columns(min(4, max(1, len(sql_servers))))
#             for i, srv in enumerate(sql_servers):
#                 if cols_sql[i % len(cols_sql)].button(srv, key=f"open_sql_{srv}"):
#                     clicked_sql_server = srv

#         if clicked_sql_server:
#             st.session_state.selected_sql_server = clicked_sql_server
#             selected_sql_server = clicked_sql_server

#         if selected_sql_server and selected_sql_server not in sql_servers:
#             st.session_state.selected_sql_server = None
#             selected_sql_server = None

#         if selected_sql_server:
#             st.markdown("---")
#             st.subheader(f"🔍 SQL Server: {selected_sql_server}")

#             try:
#                 db_rows = list_sql_databases_for_server(
#                     _credential=credential,
#                     subscription_id=subscription_id,
#                     resource_group=rg_name,
#                     server_name=selected_sql_server,
#                 )
#             except Exception as e:
#                 db_rows = []
#                 st.error(f"Failed to list databases: {e}")

#             if db_rows:
#                 st.caption("Databases on this server")
#                 st.dataframe(db_rows, hide_index=True, width="stretch")

#                 if "selected_sql_database" not in st.session_state:
#                     st.session_state.selected_sql_database = None
#                 selected_sql_database: Optional[str] = st.session_state.selected_sql_database
#                 clicked_db: Optional[str] = None
#                 db_names = [row.get("Database", "") for row in db_rows if row.get("Database")]

#                 if db_names:
#                     st.caption("📂 Open a Database:")
#                     db_cols = st.columns(min(4, max(1, len(db_names))))
#                     for i, db_name in enumerate(db_names):
#                         if db_cols[i % len(db_cols)].button(db_name, key=f"open_db_{selected_sql_server}_{db_name}"):
#                             clicked_db = db_name

#                 if clicked_db:
#                     st.session_state.selected_sql_database = clicked_db
#                     selected_sql_database = clicked_db

#                 if selected_sql_database and selected_sql_database not in db_names:
#                     st.session_state.selected_sql_database = None
#                     selected_sql_database = None

#                 if selected_sql_database:
#                     st.markdown("---")
#                     st.subheader(f"📊 Database: {selected_sql_database}")
#                     st.info("💡 Database browsing kept unchanged (your existing logic continues here).")

#             else:
#                 st.info("No databases found on this server.")

#         # ==========================================================
#         # ========== STORAGE ACCOUNTS SECTION (UNCHANGED) ===========
#         # ==========================================================
#         if "selected_sa" not in st.session_state:
#             st.session_state.selected_sa = None
#         selected_sa: Optional[str] = st.session_state.selected_sa
#         previous_sa: Optional[str] = selected_sa
#         clicked_sa: Optional[str] = None
#         try:
#             storage_accounts = list_storage_accounts(
#                 _credential=credential,
#                 subscription_id=subscription_id,
#                 resource_group=rg_name,
#             )
#         except Exception as e:
#             storage_accounts = []
#             st.warning(f"Could not list storage accounts: {e}")

#         if storage_accounts:
#             st.caption("Open a Storage Account:")
#             cols_sa = st.columns(min(4, max(1, len(storage_accounts))))
#             for i, sa in enumerate(storage_accounts):
#                 if cols_sa[i % len(cols_sa)].button(sa, key=f"open_sa_{sa}"):
#                     clicked_sa = sa

#         if clicked_sa:
#             st.session_state.selected_sa = clicked_sa
#             selected_sa = clicked_sa

#         if selected_sa and selected_sa not in storage_accounts:
#             st.session_state.selected_sa = None
#             selected_sa = None

#         if selected_sa != previous_sa:
#             st.session_state.storage_selection = {}

#         if selected_sa:
#             st.subheader(f"Storage account: {selected_sa}")
#             st.info("💡 Storage browsing kept unchanged (your existing logic continues here).")

#         # ==========================================================
#         # ========== SYNAPSE ANALYTICS SECTION (NEW, SAME UI STYLE) ==
#         # ==========================================================
#         st.markdown("---")
#         st.subheader("🧠 Synapse Analytics")

#         if "selected_synapse_ws" not in st.session_state:
#             st.session_state.selected_synapse_ws = None
#         selected_synapse_ws: Optional[str] = st.session_state.selected_synapse_ws
#         clicked_syn_ws: Optional[str] = None

#         try:
#             syn_workspaces = list_synapse_workspaces(
#                 credential,
#                 subscription_id,
#                 rg_name
#             )
#         except Exception as e:
#             syn_workspaces = []
#             st.warning(f"Could not list Synapse workspaces: {e}")

#         if syn_workspaces:
#             st.caption("Open a Synapse Workspace:")
#             cols_syn = st.columns(min(4, max(1, len(syn_workspaces))))
#             for i, ws in enumerate(syn_workspaces):
#                 if cols_syn[i % len(cols_syn)].button(ws, key=f"open_syn_{ws}"):
#                     clicked_syn_ws = ws

#         if clicked_syn_ws:
#             st.session_state.selected_synapse_ws = clicked_syn_ws
#             selected_synapse_ws = clicked_syn_ws

#         if selected_synapse_ws and selected_synapse_ws not in syn_workspaces:
#             st.session_state.selected_synapse_ws = None
#             selected_synapse_ws = None

#         if selected_synapse_ws:
#             st.markdown("---")
#             st.subheader(f"🔍 Synapse Workspace: {selected_synapse_ws}")

#             try:
#                 syn_rows = fetch_activity_rows_for_synapse(
#                     credential,
#                     subscription_id,
#                     rg_name,
#                     selected_synapse_ws
#                 )
#             except Exception as e:
#                 st.error(f"Failed to fetch Synapse pipelines: {e}")
#                 syn_rows = []

#             with st.container(border=True):
#                 st.subheader("📋 Synapse Pipelines and Activities")
#                 if syn_rows:
#                     st.dataframe(syn_rows, hide_index=True, width="stretch")
#                 else:
#                     st.info("No pipelines/activities found in this Synapse workspace.")

#             # Pipeline selection (same UX as ADF migrate)
#             syn_pipeline_names = sorted({r.get("PipelineName") for r in syn_rows if r.get("PipelineName")})

#             with st.container(border=True):
#                 st.subheader("🚀 Migrate Synapse Pipeline to Microsoft Fabric")

#                 if not syn_pipeline_names:
#                     st.info("No Synapse pipelines available to migrate.")
#                 else:
#                     selected_synapse_pipeline = st.selectbox(
#                         "Select a Synapse pipeline to migrate",
#                         options=syn_pipeline_names,
#                         index=0
#                     )

#                     syn_workspace_id = st.text_input(
#                         "Fabric Workspace ID",
#                         placeholder="Enter your Fabric Workspace ID (UUID format)",
#                         key=f"workspace_id_syn_{selected_synapse_ws}",
#                     )

#                     run_synapse_migration = st.button(
#                         "🔄 Migrate Selected Synapse Pipeline to Fabric",
#                         type="primary",
#                         key=f"run_migration_syn_{selected_synapse_ws}",
#                     )

#                     if run_synapse_migration:
#                         if not selected_synapse_pipeline:
#                             st.warning("Please select a Synapse pipeline to migrate.")
#                         elif not syn_workspace_id:
#                             st.warning("Please enter a Fabric Workspace ID.")
#                         else:
#                             # Wrapper script that rehydrates Synapse -> temp ADF -> uses existing ADF migration script
#                             script_path = os.path.join(UTILS_DIR, "synapse_to_adf_then_fabric.ps1")
#                             resolutions_file = os.path.join(UTILS_DIR, "resolutions.json")
#                             region = "prod"

#                             cmd = [
#                                 "pwsh",
#                                 "-File",
#                                 script_path,
#                                 "-SubscriptionId",
#                                 subscription_id,
#                                 "-ResourceGroupName",
#                                 rg_name,
#                                 "-SynapseWorkspaceName",
#                                 selected_synapse_ws,
#                                 "-PipelineName",
#                                 selected_synapse_pipeline,
#                                 "-FabricWorkspaceId",
#                                 syn_workspace_id,
#                                 "-ResolutionsFile",
#                                 resolutions_file,
#                                 "-Region",
#                                 region,
#                             ]

#                             try:
#                                 with st.spinner("Running Synapse → ADF → Fabric migration in PowerShell..."):
#                                     result = subprocess.run(cmd, capture_output=True, text=True)
#                             except FileNotFoundError:
#                                 st.error("Failed to start pwsh. Ensure PowerShell 7 is installed and in PATH.")
#                             except Exception as exc:
#                                 st.error(f"Failed to launch migration script: {exc}")
#                             else:
#                                 if result.returncode == 0:
#                                     st.success("✅ Migration script completed. Check Microsoft Fabric + Logs.")
#                                 else:
#                                     st.error(f"❌ Migration script exited with code {result.returncode}.")

#                                 if result.stdout:
#                                     st.caption("PowerShell output:")
#                                     st.code(result.stdout, language="powershell")

#                                 if result.stderr:
#                                     st.caption("PowerShell errors:")
#                                     st.code(result.stderr, language="powershell")


# if __name__ == "__main__":
#     main()





import os
import subprocess
from collections import defaultdict
from typing import Optional, Dict, List, Tuple, Any, Set

import streamlit as st
from azure.identity import InteractiveBrowserCredential

# Import from modular components
from Migration.azure_common import (
    list_subscriptions,
    list_resource_groups,
    list_data_factories,
    list_rg_resources,
)

from Migration.adf_components import (
    fetch_components_for_factory,
    fetch_activity_rows_for_factory,
    list_linked_services_for_factory,
    list_datasets_for_factory,
)

from Migration.synapse_components import (
    list_synapse_workspaces,
    fetch_activity_rows_for_synapse,
    list_synapse_linked_services,
    list_synapse_datasets,
)

from Migration.sql_server import (
    list_sql_servers,
    list_sql_databases_for_server,
    list_sql_usage_for_database_from_adf,
    _list_sql_tables_via_pyodbc,
    _get_db_properties_via_pyodbc,
    _list_sql_views_via_pyodbc,
    _list_sql_table_overview_via_pyodbc,
)

from Migration.data_storage import (
    list_storage_accounts,
    list_blob_containers,
    list_adls_filesystems,
    is_hns_enabled,
    list_adls_top_level_directories,
    list_adls_files_in_directory,
    list_top_level_folders,
    list_files_in_folder,
    sample_blob_paths,
    sample_adls_paths,
)

from Migration.migration_score import (
    score_component_parity,
    score_non_migratable,
    score_connectivity,
    score_orchestration,
)

from Migration.utilities import _normalize_type
from Migration.constants import CONTROL_ACTIVITY_TYPES
from Migration.ui_config import apply_custom_theme, render_header_with_logo


def _extract_synapse_datasets_and_linked_services(
    syn_rows: List[Dict[str, Any]]
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Legacy / fallback extractor (NOT used anymore for the primary Synapse UI).
    Kept for safety in case Synapse Dev API calls fail.
    """
    dataset_keys = {"Dataset", "DatasetName", "InputDataset", "OutputDataset", "DatasetReference"}
    linked_service_keys = {"LinkedService", "LinkedServiceName", "LinkedServiceType", "LSName"}

    datasets: Set[str] = set()
    linked_services: Set[str] = set()

    for r in syn_rows:
        for k, v in r.items():
            if v is None:
                continue
            if k in dataset_keys and str(v).strip():
                datasets.add(str(v).strip())
            if k in linked_service_keys and str(v).strip():
                linked_services.add(str(v).strip())

        for possible in ("Inputs", "Outputs", "Input", "Output"):
            if possible in r and r[possible]:
                parts = [p.strip() for p in str(r[possible]).split(",") if p.strip()]
                for p in parts:
                    datasets.add(p)

    ds_rows = [{"Dataset": d} for d in sorted(datasets)] if datasets else []
    ls_rows = [{"LinkedService": l} for l in sorted(linked_services)] if linked_services else []
    return ds_rows, ls_rows


def main() -> None:
    st.set_page_config(
        page_title="ADF to Fabric Migration Tool | OnPoint Insights",
        page_icon="🔷",
        layout="wide"
    )

    # Apply custom OnPoint Insights theme
    apply_custom_theme()

    # Paths
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    UTILS_DIR = os.path.join(BASE_DIR, "utils")
    logo_path = os.path.join(UTILS_DIR, "logo.png")

    # Custom header with branding and logo
    render_header_with_logo(
        "Azure Data Factory to Fabric Migration",
        "Powered by OnPoint Insights",
        logo_path=logo_path
    )
    st.markdown("---")
    st.info(
        "Sign in to Azure, select your subscription, resource group, and Data Factory / Synapse Workspace, then migrate pipelines to Microsoft Fabric."
    )

    if "credential" not in st.session_state:
        st.session_state.credential = None

    # Sign-in section
    with st.container(border=True):
        st.subheader("🔐 Sign in to Azure")
        col1, col2 = st.columns([1, 3])
        with col1:
            login_clicked = st.button("🔑 Sign In with Azure", type="primary", use_container_width=True)
        with col2:
            st.markdown("**Status:** " + ("✅ Signed in" if st.session_state.credential else "❌ Not signed in"))
        if login_clicked or st.session_state.credential is None:
            try:
                cred = InteractiveBrowserCredential()
                _ = list_subscriptions(_credential=cred)
                st.session_state.credential = cred
                st.success("✅ Signed in successfully.")
            except Exception as e:
                st.error(f"❌ Sign-in failed: {e}")
                return

    credential: Optional[InteractiveBrowserCredential] = st.session_state.credential
    if credential is None:
        st.stop()

    # Subscription selection
    with st.container(border=True):
        st.subheader("📋 Select Subscription")
        try:
            subs = list_subscriptions(_credential=credential)
        except Exception as e:
            st.error(f"Failed to list subscriptions: {e}")
            st.stop()

        sub_labels = [f"{name} ({sid})" for name, sid in subs]
        sub_idx = st.selectbox(
            "Subscription",
            options=list(range(len(subs))),
            format_func=lambda i: sub_labels[i] if subs else "",
            index=0 if subs else None
        )
        if subs:
            subscription_id = subs[sub_idx][1]
        else:
            st.warning("No subscriptions available.")
            st.stop()

    # Resource group selection
    with st.container(border=True):
        st.subheader("📁 Select Resource Group")
        try:
            rgs = list_resource_groups(_credential=credential, subscription_id=subscription_id)
        except Exception as e:
            st.error(f"Failed to list resource groups: {e}")
            st.stop()

        if not rgs:
            st.warning("No resource groups in this subscription.")
            st.stop()

        rg_name = st.selectbox("Resource group", options=rgs, index=0)

        # RG resources table
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

        # ==========================================================
        # ========== DATA FACTORIES SECTION (UNCHANGED UI) ==========
        # ==========================================================
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
            st.caption("📊 Open a Data Factory:")
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
            st.markdown("---")
            st.subheader(f"🔍 Data Factory: {selected_df}")

            # Fetch activities
            try:
                act_rows = fetch_activity_rows_for_factory(credential, subscription_id, rg_name, selected_df)
            except Exception as e:
                st.error(f"Failed to fetch components: {e}")
                act_rows = []

            # Linked Services
            try:
                ls_rows = list_linked_services_for_factory(credential, subscription_id, rg_name, selected_df)
                ls_types = [row.get("LinkedServiceType", "") for row in ls_rows]
            except Exception:
                ls_rows = []
                ls_types = []

            # 1) Pipelines and Activities
            with st.container(border=True):
                st.subheader("📋 Pipelines and Activities")
                if act_rows:
                    st.dataframe(act_rows, width="stretch", hide_index=True)
                else:
                    st.info("No components found.")

            # 2) Migration Scoring
            with st.container(border=True):
                st.subheader("📈 Migration Scoring (Fabric Readiness Assessment)")
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

                    parity_score = score_component_parity(total_acts, non_migratable)
                    non_mig_score = score_non_migratable(non_migratable)
                    connectivity_score = score_connectivity(ls_types)
                    orchestration_score = score_orchestration(total_acts, control_acts)
                    total = parity_score + non_mig_score + connectivity_score + orchestration_score

                    if 3 in (parity_score, non_mig_score, connectivity_score, orchestration_score):
                        band = "🔴 Hard"
                    elif total <= 4:
                        band = "🟢 Easy"
                    elif total <= 8:
                        band = "🟡 Medium"
                    else:
                        band = "🔴 Hard"

                    score_rows.append({
                        "Factory": fac,
                        "Pipeline": pipe,
                        "Component Parity": parity_score,
                        "Non-Migratable": non_mig_score,
                        "Connectivity": connectivity_score,
                        "Orchestration": orchestration_score,
                        "Total Score": total,
                        "Difficulty": band,
                        "Activities": total_acts,
                        "Non-Migratable Count": non_migratable,
                    })

                if score_rows:
                    st.dataframe(score_rows, width="stretch", hide_index=True)
                else:
                    st.info("No pipelines to score.")

            # 3) Linked Services
            with st.container(border=True):
                st.subheader("🔗 Linked Services")
                if ls_rows:
                    st.dataframe(ls_rows, width="stretch", hide_index=True)
                else:
                    st.info("No linked services found.")

            # 4) Datasets
            with st.container(border=True):
                st.subheader("📦 Datasets")
                try:
                    ds_rows = list_datasets_for_factory(credential, subscription_id, rg_name, selected_df)
                except Exception as e:
                    ds_rows = []
                    st.error(f"Failed to list datasets: {e}")

                if ds_rows:
                    st.dataframe(ds_rows, width="stretch", hide_index=True)
                else:
                    st.info("No datasets found.")

            # 5) Migration to Fabric (ADF)
            with st.container(border=True):
                st.subheader("🚀 Migrate ADF Pipelines to Microsoft Fabric")

                pipeline_names = sorted({row.get("Pipeline") for row in score_rows if row.get("Pipeline")})
                if not pipeline_names:
                    st.info("No pipelines available to migrate for this Data Factory.")
                else:
                    migrate_all = st.checkbox(
                        "Migrate all pipelines",
                        value=True,
                        key=f"migrate_all_{selected_df}",
                    )
                    if migrate_all:
                        pipelines_to_migrate = pipeline_names
                    else:
                        pipelines_to_migrate = st.multiselect(
                            "Select pipelines to migrate",
                            options=pipeline_names,
                            default=pipeline_names,
                            key=f"pipelines_to_migrate_{selected_df}",
                        )

                    workspace_id = st.text_input(
                        "Fabric Workspace ID",
                        placeholder="Enter your Fabric Workspace ID (UUID format)",
                        key=f"workspace_id_adf_{selected_df}",
                    )

                    run_migration = st.button(
                        "🔄 Migrate Selected ADF Pipelines to Fabric",
                        type="primary",
                        key=f"run_migration_adf_{selected_df}",
                    )

                    if run_migration:
                        if not pipelines_to_migrate:
                            st.warning("Please select at least one pipeline to migrate.")
                        elif not workspace_id:
                            st.warning("Please enter a Fabric Workspace ID.")
                        else:
                            script_path = os.path.join(UTILS_DIR, "adf_to_fabric_migration.ps1")
                            resolutions_file = os.path.join(UTILS_DIR, "resolutions.json")
                            region = "prod"

                            cmd = [
                                "pwsh", "-File", script_path,
                                "-FabricWorkspaceId", workspace_id,
                                "-ResolutionsFile", resolutions_file,
                                "-Region", region,
                                "-SubscriptionId", subscription_id,
                                "-ResourceGroupName", rg_name,
                                "-DataFactoryName", selected_df,
                                "-PipelineNames", ",".join(pipelines_to_migrate),
                            ]

                            try:
                                with st.spinner("Running migration in PowerShell..."):
                                    result = subprocess.run(cmd, capture_output=True, text=True)
                            except FileNotFoundError:
                                st.error("Failed to start pwsh. Ensure PowerShell 7 is installed and in PATH.")
                            except Exception as exc:
                                st.error(f"Failed to launch migration script: {exc}")
                            else:
                                if result.returncode == 0:
                                    st.success("✅ Migration script completed. Check Microsoft Fabric + Logs.")
                                else:
                                    st.error(f"❌ Migration script exited with code {result.returncode}.")

                                if result.stdout:
                                    st.caption("PowerShell output:")
                                    st.code(result.stdout, language="powershell")

                                if result.stderr:
                                    st.caption("PowerShell errors:")
                                    st.code(result.stderr, language="powershell")

# ========== SQL SERVERS SECTION ==========
        if "selected_sql_server" not in st.session_state:
            st.session_state.selected_sql_server = None
        selected_sql_server: Optional[str] = st.session_state.selected_sql_server
        clicked_sql_server: Optional[str] = None
        try:
            sql_servers = list_sql_servers(credential, subscription_id, rg_name)
        except Exception as e:
            sql_servers = []
            st.warning(f"Could not list SQL servers: {e}")
        
        if sql_servers:
            st.caption("🗄️ Open an Azure SQL Server:")
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
            st.markdown("---")
            st.subheader(f"🔍 SQL Server: {selected_sql_server}")
            
            try:
                db_rows = list_sql_databases_for_server(
                    _credential=credential,
                    subscription_id=subscription_id,
                    resource_group=rg_name,
                    server_name=selected_sql_server,
                )
            except Exception as e:
                db_rows = []
                st.error(f"Failed to list databases: {e}")
            
            if db_rows:
                st.caption("Databases on this server")
                st.dataframe(db_rows, hide_index=True, width="stretch")

                # Per-database buttons
                if "selected_sql_database" not in st.session_state:
                    st.session_state.selected_sql_database = None
                selected_sql_database: Optional[str] = st.session_state.selected_sql_database
                clicked_db: Optional[str] = None
                db_names = [row.get("Database", "") for row in db_rows if row.get("Database")]
                
                if db_names:
                    st.caption("📂 Open a Database:")
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
                    st.markdown("---")
                    st.subheader(f"📊 Database: {selected_sql_database}")
                    
                    default_db_conn_hint = (
                        "DRIVER={ODBC Driver 18 for SQL Server};"
                        f"SERVER={selected_sql_server};"
                        f"DATABASE={selected_sql_database};"
                        "UID=your-user;PWD=your-password;Encrypt=yes;TrustServerCertificate=no;"
                    )
                    db_conn_str = st.text_input(
                        "SQL Connection String (for listing tables, views, etc.)",
                        value="",
                        placeholder=default_db_conn_hint,
                        key=f"db_conn_{selected_sql_server}_{selected_sql_database}",
                    )
                    
                    if db_conn_str:
                        if st.button("📋 Load Database Components", key=f"btn_list_tables_{selected_sql_server}_{selected_sql_database}"):
                            # Database properties
                            db_info = _get_db_properties_via_pyodbc(db_conn_str)
                            if db_info.get("error"):
                                st.error(f"❌ {db_info['error']}")
                            else:
                                props = db_info.get("properties") or {}
                                if props:
                                    st.markdown("#### 🗂️ Database Properties")
                                    st.dataframe([props], hide_index=True, width="stretch")
                                    st.divider()

                            # Views
                            view_info = _list_sql_views_via_pyodbc(db_conn_str)
                            if view_info.get("error"):
                                st.error(f"❌ {view_info['error']}")
                            else:
                                views = view_info.get("views") or []
                                if views:
                                    st.markdown("#### 👁️ Views")
                                    st.dataframe(views, hide_index=True, width="stretch")
                                    st.divider()
                                else:
                                    st.info("No views found in this database.")

                            # Tables with metadata
                            st.markdown("#### 📑 Tables with Row Counts & Constraints")
                            tbl_info = _list_sql_table_overview_via_pyodbc(db_conn_str)
                            if tbl_info.get("error"):
                                st.error(f"❌ Error loading tables: {tbl_info['error']}")
                            else:
                                tbl_rows = tbl_info.get("tables") or []
                                if tbl_rows:
                                    st.dataframe(tbl_rows, hide_index=True, width="stretch")
                                else:
                                    st.info("ℹ️ No tables found in this database.")
                    else:
                        st.info("💡 Enter a SQL connection string above to load database components.")
            else:
                st.info("No databases found on this server.")

        # ========== STORAGE ACCOUNTS SECTION ==========
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

        # ==========================================================
        # ========== SYNAPSE ANALYTICS SECTION (ENHANCED) ===========
        # ==========================================================
        st.markdown("---")
        st.subheader("🧠 Synapse Analytics")

        if "selected_synapse_ws" not in st.session_state:
            st.session_state.selected_synapse_ws = None
        selected_synapse_ws: Optional[str] = st.session_state.selected_synapse_ws
        clicked_syn_ws: Optional[str] = None

        try:
            syn_workspaces = list_synapse_workspaces(
                credential,
                subscription_id,
                rg_name
            )
        except Exception as e:
            syn_workspaces = []
            st.warning(f"Could not list Synapse workspaces: {e}")

        if syn_workspaces:
            st.caption("Open a Synapse Workspace:")
            cols_syn = st.columns(min(4, max(1, len(syn_workspaces))))
            for i, ws in enumerate(syn_workspaces):
                if cols_syn[i % len(cols_syn)].button(ws, key=f"open_syn_{ws}"):
                    clicked_syn_ws = ws

        if clicked_syn_ws:
            st.session_state.selected_synapse_ws = clicked_syn_ws
            selected_synapse_ws = clicked_syn_ws

        if selected_synapse_ws and selected_synapse_ws not in syn_workspaces:
            st.session_state.selected_synapse_ws = None
            selected_synapse_ws = None

        if selected_synapse_ws:
            st.markdown("---")
            st.subheader(f"🔍 Synapse Workspace: {selected_synapse_ws}")

            try:
                syn_rows = fetch_activity_rows_for_synapse(
                    credential,
                    subscription_id,
                    rg_name,
                    selected_synapse_ws
                )
            except Exception as e:
                st.error(f"Failed to fetch Synapse pipelines: {e}")
                syn_rows = []

            # 1) Pipelines and Activities
            with st.container(border=True):
                st.subheader("📋 Synapse Pipelines and Activities")
                if syn_rows:
                    st.dataframe(syn_rows, hide_index=True, width="stretch")
                else:
                    st.info("No pipelines/activities found in this Synapse workspace.")

            # 2) Migration Scoring (same style as ADF)
            with st.container(border=True):
                st.subheader("📈 Migration Scoring (Fabric Readiness Assessment)")

                grouped_syn: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
                for r in syn_rows:
                    grouped_syn[r.get("PipelineName", "")].append(r)

                syn_score_rows: List[Dict[str, Any]] = []
                for pipe, items in grouped_syn.items():
                    total_acts = len(items)

                    # Integrate pipelines = not ARM based → mark as non-migratable
                    non_migratable = total_acts

                    control_acts = 0
                    for it in items:
                        nt = _normalize_type(it.get("ActivityType"))
                        if nt in CONTROL_ACTIVITY_TYPES:
                            control_acts += 1

                    parity_score = score_component_parity(total_acts, non_migratable)
                    non_mig_score = score_non_migratable(non_migratable)
                    orchestration_score = score_orchestration(total_acts, control_acts)
                    total = parity_score + non_mig_score + orchestration_score

                    syn_score_rows.append({
                        "Pipeline": pipe,
                        "Component Parity": parity_score,
                        "Non-Migratable": non_mig_score,
                        "Orchestration": orchestration_score,
                        "Total Score": total,
                        "Difficulty": "🔴 Not Migratable",
                        "Activities": total_acts,
                        "Non-Migratable Count": non_migratable,
                        "Reason": "Synapse Integrate pipelines are not ARM-based",
                    })

                if syn_score_rows:
                    st.dataframe(syn_score_rows, hide_index=True, width="stretch")
                else:
                    st.info("No pipelines to score.")

            # 3) Linked Services (Synapse) — REAL Dev API (with fallback)
            with st.container(border=True):
                st.subheader("🔗 Linked Services (Synapse)")
                try:
                    syn_ls_rows = list_synapse_linked_services(
                        credential,
                        selected_synapse_ws
                    )
                    if syn_ls_rows:
                        st.dataframe(syn_ls_rows, hide_index=True, width="stretch")
                    else:
                        st.info("No linked services found in this Synapse workspace.")
                except Exception as e:
                    # fallback to old extractor if API fails
                    _, ls_rows_syn = _extract_synapse_datasets_and_linked_services(syn_rows)
                    if ls_rows_syn:
                        st.warning(f"Could not load linked services via Dev API ({e}). Showing extracted values instead.")
                        st.dataframe(ls_rows_syn, hide_index=True, width="stretch")
                    else:
                        st.error(f"Failed to load Synapse linked services: {e}")

            # 4) Datasets (Synapse) — REAL Dev API (with fallback)
            with st.container(border=True):
                st.subheader("📦 Datasets (Synapse)")
                try:
                    syn_ds_rows = list_synapse_datasets(
                        credential,
                        selected_synapse_ws
                    )
                    if syn_ds_rows:
                        st.dataframe(syn_ds_rows, hide_index=True, width="stretch")
                    else:
                        st.info("No datasets found in this Synapse workspace.")
                except Exception as e:
                    # fallback to old extractor if API fails
                    ds_rows_syn, _ = _extract_synapse_datasets_and_linked_services(syn_rows)
                    if ds_rows_syn:
                        st.warning(f"Could not load datasets via Dev API ({e}). Showing extracted values instead.")
                        st.dataframe(ds_rows_syn, hide_index=True, width="stretch")
                    else:
                        st.error(f"Failed to load Synapse datasets: {e}")

            # 5) Migration (unchanged)
            syn_pipeline_names = sorted({r.get("PipelineName") for r in syn_rows if r.get("PipelineName")})

            with st.container(border=True):
                st.subheader("🚀 Migrate Synapse Pipeline to Microsoft Fabric")

                if not syn_pipeline_names:
                    st.info("No Synapse pipelines available to migrate.")
                else:
                    selected_synapse_pipeline = st.selectbox(
                        "Select a Synapse pipeline to migrate",
                        options=syn_pipeline_names,
                        index=0
                    )

                    syn_workspace_id = st.text_input(
                        "Fabric Workspace ID",
                        placeholder="Enter your Fabric Workspace ID (UUID format)",
                        key=f"workspace_id_syn_{selected_synapse_ws}",
                    )

                    run_synapse_migration = st.button(
                        "🔄 Migrate Selected Synapse Pipeline to Fabric",
                        type="primary",
                        key=f"run_migration_syn_{selected_synapse_ws}",
                    )

                    if run_synapse_migration:
                        if not selected_synapse_pipeline:
                            st.warning("Please select a Synapse pipeline to migrate.")
                        elif not syn_workspace_id:
                            st.warning("Please enter a Fabric Workspace ID.")
                        else:
                            script_path = os.path.join(UTILS_DIR, "synapse_to_adf_then_fabric.ps1")
                            resolutions_file = os.path.join(UTILS_DIR, "resolutions.json")
                            region = "prod"

                            cmd = [
                                "pwsh",
                                "-File",
                                script_path,
                                "-SubscriptionId",
                                subscription_id,
                                "-ResourceGroupName",
                                rg_name,
                                "-SynapseWorkspaceName",
                                selected_synapse_ws,
                                "-PipelineName",
                                selected_synapse_pipeline,
                                "-FabricWorkspaceId",
                                syn_workspace_id,
                                "-ResolutionsFile",
                                resolutions_file,
                                "-Region",
                                region,
                            ]

                            try:
                                with st.spinner("Running Synapse → ADF → Fabric migration in PowerShell..."):
                                    result = subprocess.run(cmd, capture_output=True, text=True)
                            except FileNotFoundError:
                                st.error("Failed to start pwsh. Ensure PowerShell 7 is installed and in PATH.")
                            except Exception as exc:
                                st.error(f"Failed to launch migration script: {exc}")
                            else:
                                if result.returncode == 0:
                                    st.success("✅ Migration script completed. Check Microsoft Fabric + Logs.")
                                else:
                                    st.error(f"❌ Migration script exited with code {result.returncode}.")

                                if result.stdout:
                                    st.caption("PowerShell output:")
                                    st.code(result.stdout, language="powershell")

                                if result.stderr:
                                    st.caption("PowerShell errors:")
                                    st.code(result.stderr, language="powershell")


if __name__ == "__main__":
    main()
