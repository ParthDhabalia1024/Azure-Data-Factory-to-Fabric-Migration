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
from azure.identity import ClientSecretCredential, InteractiveBrowserCredential

from Synapse_Data.fabric_copyjob_warehouse import (
    create_copy_job_synapse_to_warehouse,
    create_copy_job_synapse_tables_to_warehouse,
    create_or_get_synapse_connection_service_principal,
    create_or_get_warehouse,
    list_synapse_tables_service_principal,
 )

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
from utils.synapse_notebook_migrator import migrate_synapse_notebook_to_fabric, list_synapse_notebooks


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
        for key in dataset_keys:
            if key in r and r[key]:
                datasets.add(str(r[key]))
            if key in linked_service_keys and r[key]:
                linked_services.add(str(r[key]))

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


def build_service_principal_credential() -> ClientSecretCredential:
    """Builds a ClientSecretCredential from env vars; raises if missing."""
    tenant_id = os.getenv("AZURE_TENANT_ID")
    client_id = os.getenv("AZURE_CLIENT_ID")
    client_secret = os.getenv("AZURE_CLIENT_SECRET")
    missing = [name for name, val in [
        ("AZURE_TENANT_ID", tenant_id),
        ("AZURE_CLIENT_ID", client_id),
        ("AZURE_CLIENT_SECRET", client_secret),
    ] if not val]
    if missing:
        raise EnvironmentError(f"Missing environment variables: {', '.join(missing)}")
    return ClientSecretCredential(tenant_id=tenant_id, client_id=client_id, client_secret=client_secret)


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

    # Sign-in section using service principal (env vars)
    with st.container(border=True):
        st.subheader("🔐 Service Principal Authentication")
        st.markdown(
            "Provide AZURE_TENANT_ID, AZURE_CLIENT_ID, AZURE_CLIENT_SECRET in the environment before launching Streamlit."
        )

        try:
            cred = st.session_state.credential or build_service_principal_credential()
            # Optionally test ARM access; will still proceed even if no subscriptions
            if st.session_state.credential is None:
                try:
                    _ = list_subscriptions(_credential=cred)
                except Exception:
                    # ignore; may lack ARM role, but credentials are valid
                    pass
                st.session_state.credential = cred
            st.success("✅ Service principal credential loaded from environment.")
        except Exception as e:
            st.session_state.credential = None
            st.error(f"❌ Failed to load service principal credential: {e}")
            st.stop()

    credential: Optional[ClientSecretCredential] = st.session_state.credential

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

            ls_type_by_name: Dict[str, str] = {}
            try:
                for r in ls_rows:
                    n = (r.get("LinkedService") or "").strip()
                    t = (r.get("LinkedServiceType") or "").strip()
                    if n:
                        ls_type_by_name[n] = t
            except Exception:
                ls_type_by_name = {}

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

                    # Connectivity should be scored per-pipeline based on referenced linked services,
                    # not all linked services in the factory.
                    used_ls_names: Set[str] = set()
                    for it in items:
                        sls = (it.get("SourceLinkedService") or "").strip()
                        tls = (it.get("SinkLinkedService") or "").strip()
                        if sls:
                            used_ls_names.add(sls)
                        if tls:
                            used_ls_names.add(tls)
                    used_ls_types = [ls_type_by_name.get(n, "") for n in sorted(used_ls_names)]

                    control_acts = 0
                    for it in items:
                        nt = _normalize_type(it.get("ActivityType"))
                        if nt in CONTROL_ACTIVITY_TYPES:
                            control_acts += 1

                    parity_score = score_component_parity(total_acts, non_migratable)
                    non_mig_score = score_non_migratable(non_migratable)
                    connectivity_score = score_connectivity(used_ls_types)
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

            with st.container(border=True):
                st.subheader("🏭 Fabric Warehouse + Copy Job (REST API)")
                st.caption(
                    "Creates a Fabric Warehouse, creates a Fabric Connection to Synapse using service principal, then creates a Copy Job."
                )

                fabric_workspace_id = st.text_input(
                    "Fabric Workspace ID",
                    value="",
                    placeholder="xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
                    key="fabric_ws_id_copyjob",
                )

                col1, col2 = st.columns(2)
                with col1:
                    warehouse_name = st.text_input(
                        "Warehouse name",
                        value=os.getenv("FABRIC_WAREHOUSE_NAME", "SynapseWarehouse"),
                        key="fabric_wh_name",
                    )
                with col2:
                    copyjob_name = st.text_input(
                        "Copy job name",
                        value=os.getenv("FABRIC_COPYJOB_NAME", "SynapseToWarehouseCopyJob"),
                        key="fabric_copyjob_name",
                    )

                syn_server = st.text_input(
                    "Synapse server",
                    value=os.getenv("SYNAPSE_SERVER", "synapse-fabricmigration.database.windows.net"),
                    key="syn_server",
                )

                syn_connection_id = st.text_input(
                    "Existing Fabric Synapse connection ID (leave blank to create new)",
                    # value=os.getenv("SYNAPSE_CONNECTION_ID", "0e51e237-02c3-4217-a032-0ee4dc7c0059"),
                    value=os.getenv("SYNAPSE_CONNECTION_ID", "3b073c99-84de-48fc-8efa-a46972072f41"),
                    key="syn_connection_id",
                )

                db_options_env = os.getenv("SYNAPSE_DATABASES", "").strip()
                db_options = [d.strip() for d in db_options_env.split(",") if d.strip()]
                default_db = os.getenv("SYNAPSE_DATABASE", "kartik_dedicated_pool")
                if default_db and default_db not in db_options:
                    db_options = [default_db, *db_options]
                db_options = db_options or [default_db]
                db_choice = st.selectbox(
                    "Synapse database",
                    options=[*db_options, "(enter manually)"],
                    index=0,
                    key="syn_database_choice",
                )
                if db_choice == "(enter manually)":
                    syn_database = st.text_input(
                        "Synapse database name",
                        value=default_db,
                        key="syn_database_manual",
                    )
                else:
                    syn_database = db_choice

                if "synapse_tables" not in st.session_state:
                    st.session_state.synapse_tables = []
                if "synapse_tables_selected" not in st.session_state:
                    st.session_state.synapse_tables_selected = []

                load_tables = st.button(
                    "Load Synapse tables",
                    key="btn_load_syn_tables",
                )
                if load_tables:
                    try:
                        tid = os.getenv("AZURE_TENANT_ID") or ""
                        cid = os.getenv("AZURE_CLIENT_ID") or ""
                        csec = os.getenv("AZURE_CLIENT_SECRET") or ""
                        if not (tid and cid and csec):
                            raise EnvironmentError("Missing AZURE_TENANT_ID / AZURE_CLIENT_ID / AZURE_CLIENT_SECRET")
                        tables = list_synapse_tables_service_principal(
                            server=syn_server,
                            database=syn_database,
                            tenant_id=tid,
                            client_id=cid,
                            client_secret=csec,
                        )
                        st.session_state.synapse_tables = tables
                        st.session_state.synapse_tables_selected = tables
                    except Exception as e:
                        st.error(f"Failed to load tables from Synapse: {e}")

                tables = st.session_state.synapse_tables
                if tables:
                    selected_tables = st.multiselect(
                        "Synapse tables to copy",
                        options=tables,
                        default=st.session_state.synapse_tables_selected,
                        key="syn_tables_multiselect",
                    )
                else:
                    selected_tables = []

                skip_test = st.checkbox(
                    "Skip test connection",
                    value=os.getenv("FABRIC_SKIP_TEST_CONNECTION", "").lower() in ("1", "true", "yes"),
                    key="fabric_skip_test",
                )

                run_create = st.button(
                    "Create Warehouse + Connection + Copy Job",
                    type="primary",
                    key="btn_create_wh_conn_copyjob",
                )

                if run_create:
                    if not fabric_workspace_id:
                        st.error("Fabric Workspace ID is required.")
                    elif not selected_tables:
                        st.error("Please load tables and select at least one table.")
                    else:
                        try:
                            st.write("Creating Warehouse...")
                            wh = create_or_get_warehouse(
                                workspace_id=fabric_workspace_id,
                                display_name=warehouse_name,
                                description=os.getenv("FABRIC_WAREHOUSE_DESCRIPTION", ""),
                                credential=credential,
                            )
                            if not isinstance(wh, dict):
                                raise RuntimeError(f"Warehouse API returned unexpected response type: {type(wh)}")
                            if wh.get("_reused") is True:
                                st.write("Warehouse already exists; reusing it.")
                            warehouse_id = wh.get("id") or wh.get("warehouseId")
                            warehouse_endpoint = None
                            try:
                                warehouse_endpoint = (
                                    (wh.get("properties") or {}).get("endpoint")
                                    if isinstance(wh, dict)
                                    else None
                                ) or wh.get("endpoint")
                            except Exception:
                                warehouse_endpoint = None
                            if not warehouse_id:
                                raise RuntimeError(f"Warehouse create response missing id: {wh}")

                            st.write("Creating Synapse Connection...")
                            conn = create_or_get_synapse_connection_service_principal(
                                display_name=f"SynapseConn-{syn_server}-{syn_database}",
                                server=syn_server,
                                database=syn_database,
                                tenant_id=os.getenv("AZURE_TENANT_ID") or "",
                                client_id=os.getenv("AZURE_CLIENT_ID") or "",
                                client_secret=os.getenv("AZURE_CLIENT_SECRET") or "",
                                credential=credential,
                                existing_connection_id=syn_connection_id.strip() or None,
                            )
                            if not isinstance(conn, dict):
                                raise RuntimeError(f"Connection API returned unexpected response type: {type(conn)}")
                            if conn.get("_reused") is True:
                                st.write("Connection already exists; reusing it.")
                            conn_id = conn.get("id")
                            if not conn_id:
                                raise RuntimeError(f"Connection create response missing id: {conn}")

                            st.write("Creating Copy Job...")
                            try:
                                print(
                                    "[debug] copyjob inputs",
                                    {
                                        "copyjob_name": copyjob_name,
                                        "warehouse_id": warehouse_id,
                                        "warehouse_endpoint": warehouse_endpoint,
                                        "connection_id": conn_id,
                                        "tables": selected_tables,
                                        "source_database": syn_database,
                                    },
                                    flush=True,
                                )
                            except Exception:
                                pass
                            cj_status = st.empty()
                            cj = create_copy_job_synapse_tables_to_warehouse(
                                workspace_id=fabric_workspace_id,
                                display_name=copyjob_name,
                                source_connection_id=conn_id,
                                source_tables=selected_tables,
                                destination_warehouse_id=warehouse_id,
                                destination_endpoint=warehouse_endpoint,
                                source_database=syn_database,
                                credential=credential,
                                progress_callback=lambda m: cj_status.write(m),
                            )
                            if not isinstance(cj, dict):
                                raise RuntimeError(f"CopyJob API returned unexpected response type: {type(cj)}")
                            if cj.get("_reused") is True:
                                st.write("Copy Job already exists; reusing it.")

                            st.success("Created Warehouse, Connection, and Copy Job.")
                            st.json({"warehouse": wh, "connection": conn, "copyJob": cj})
                        except Exception as e:
                            st.error(f"Failed to create Warehouse/Connection/Copy Job: {e}")

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
                st.caption(
                    "Scores are effort/risk points (0 = best). "
                    "If all activities are supported and orchestration is simple, category scores and total score can be 0 (Easy)."
                )

                grouped_syn: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
                for r in syn_rows:
                    grouped_syn[r.get("PipelineName", "")].append(r)

                syn_score_rows: List[Dict[str, Any]] = []
                for pipe, items in grouped_syn.items():
                    total_acts = len(items)

                    # Same scoring logic as ADF: rely on per-activity Migratable column
                    non_migratable = sum(
                        1 for it in items
                        if (it.get("Migratable") or "").lower() == "no"
                    )

                    # Connectivity scoring for Synapse: if linked services are not resolved in rows,
                    # this will correctly fall back to 0.
                    used_ls_names: Set[str] = set()
                    for it in items:
                        sls = (it.get("SourceLinkedService") or "").strip()
                        tls = (it.get("SinkLinkedService") or "").strip()
                        if sls:
                            used_ls_names.add(sls)
                        if tls:
                            used_ls_names.add(tls)
                    used_ls_types: List[str] = []
                    try:
                        # If Synapse rows include LS types, use them; otherwise score_connectivity([]) -> 0
                        used_ls_types = [it.get("SourceLinkedServiceType", "") for it in items if it.get("SourceLinkedServiceType")] + \
                                        [it.get("SinkLinkedServiceType", "") for it in items if it.get("SinkLinkedServiceType")]
                    except Exception:
                        used_ls_types = []

                    control_acts = 0
                    for it in items:
                        nt = _normalize_type(it.get("ActivityType"))
                        if nt in CONTROL_ACTIVITY_TYPES:
                            control_acts += 1

                    parity_score = score_component_parity(total_acts, non_migratable)
                    non_mig_score = score_non_migratable(non_migratable)
                    connectivity_score = score_connectivity(used_ls_types)
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

                    reason = ""
                    if non_migratable > 0:
                        reason = "One or more activities are not currently supported for migration"

                    syn_score_rows.append({
                        "Pipeline": pipe,
                        "Component Parity": parity_score,
                        "Non-Migratable": non_mig_score,
                        "Connectivity": connectivity_score,
                        "Orchestration": orchestration_score,
                        "Total Score": total,
                        "Difficulty": band,
                        "Activities": total_acts,
                        "Non-Migratable Count": non_migratable,
                        "Reason": reason,
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

            # 4b) Notebooks (Synapse → Fabric)
            with st.container(border=True):
                st.subheader("📓 Migrate Synapse Notebook to Microsoft Fabric")
                st.caption("Export a Synapse notebook (.ipynb) from your workspace and import it into a Fabric workspace.")

                # Try to load notebooks to drive a dropdown for accuracy
                nb_options: List[str] = []
                nb_error: Optional[str] = None
                try:
                    discovered = list_synapse_notebooks(selected_synapse_ws)
                    nb_options = sorted([n.get("name") for n in discovered if isinstance(n, dict) and n.get("name")])
                except Exception as e:
                    nb_error = str(e)

                nb_col1, nb_col2 = st.columns(2)
                with nb_col1:
                    if nb_options:
                        nb_name = st.selectbox(
                            "Select Synapse notebook",
                            options=nb_options,
                            index=0,
                            key=f"nb_select_{selected_synapse_ws}",
                        )
                    else:
                        if nb_error:
                            st.warning(f"Could not auto-load notebooks: {nb_error}")
                        nb_name = st.text_input(
                            "Synapse notebook name",
                            value="",
                            placeholder="Enter notebook name as shown in Synapse Studio",
                            key=f"nb_name_{selected_synapse_ws}",
                        )
                with nb_col2:
                    nb_workspace_id = st.text_input(
                        "Fabric Workspace ID (for notebooks)",
                        value="",
                        placeholder="Enter Fabric Workspace ID (UUID)",
                        key=f"nb_ws_{selected_synapse_ws}",
                    )
                nb_run = st.button(
                    "📥 Migrate Notebook to Fabric",
                    type="secondary",
                    key=f"nb_migrate_{selected_synapse_ws}",
                )
                if nb_run:
                    if not nb_name:
                        st.warning("Please enter a Synapse notebook name.")
                    elif not nb_workspace_id:
                        st.warning("Please enter a Fabric Workspace ID for the notebook import.")
                    else:
                        try:
                            with st.spinner("Exporting notebook from Synapse and importing into Fabric..."):
                                result = migrate_synapse_notebook_to_fabric(
                                    synapse_workspace_name=selected_synapse_ws,
                                    notebook_name=nb_name,
                                    fabric_workspace_id=nb_workspace_id,
                                    output_dir=os.path.join(UTILS_DIR, "exported_notebooks"),
                                )
                            st.success("✅ Notebook migrated to Fabric.")
                            # Only show details if useful; suppress noisy error fields
                            if isinstance(result, dict):
                                cleaned = {k: v for k, v in result.items() if k.upper() != "ERROR"}
                                if cleaned:
                                    st.json(cleaned)
                        except FileNotFoundError as fnf:
                            st.error(f"Notebook not found: {fnf}")
                        except Exception as exc:
                            st.error(f"Notebook migration failed: {exc}")

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
                    with st.expander("Optional: Override dataset paths for this migration"):
                        st.caption("If Synapse dataset APIs are blocked (401) or datasets are parameterized, provide explicit path values to avoid placeholders in Fabric.")
                        st.markdown("**Source dataset override**")
                        src_ds_name = st.text_input("Source dataset name", value="", key=f"src_ds_name_{selected_synapse_ws}")
                        c1, c2, c3 = st.columns(3)
                        with c1:
                            src_container = st.text_input("Source container/filesystem", value="", key=f"src_cnt_{selected_synapse_ws}")
                        with c2:
                            src_folder = st.text_input("Source folderPath (optional)", value="", key=f"src_fol_{selected_synapse_ws}")
                        with c3:
                            src_file = st.text_input("Source fileName (optional)", value="", key=f"src_file_{selected_synapse_ws}")

                        st.markdown("**Sink dataset override**")
                        sink_ds_name = st.text_input("Sink dataset name", value="", key=f"sink_ds_name_{selected_synapse_ws}")
                        d1, d2, d3 = st.columns(3)
                        with d1:
                            sink_container = st.text_input("Sink container/filesystem", value="", key=f"sink_cnt_{selected_synapse_ws}")
                        with d2:
                            sink_folder = st.text_input("Sink folderPath (optional)", value="", key=f"sink_fol_{selected_synapse_ws}")
                        with d3:
                            sink_file = st.text_input("Sink fileName (optional)", value="", key=f"sink_file_{selected_synapse_ws}")
                    # Option to delete the temporary ADF after successful migration
                    cleanup_temp_adf = st.checkbox(
                        "Delete temporary ADF after migration",
                        value=True,
                        key=f"cleanup_temp_adf_{selected_synapse_ws}",
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

                            # Pass optional explicit overrides to avoid placeholders when Synapse dataset fetch fails
                            if src_ds_name and (src_container or src_folder or src_file):
                                cmd.extend(["-SourceDatasetName", src_ds_name])
                                if src_container:
                                    cmd.extend(["-SourceContainer", src_container])
                                if src_folder:
                                    cmd.extend(["-SourceFolderPath", src_folder])
                                if src_file:
                                    cmd.extend(["-SourceFileName", src_file])

                            if sink_ds_name and (sink_container or sink_folder or sink_file):
                                cmd.extend(["-SinkDatasetName", sink_ds_name])
                                if sink_container:
                                    cmd.extend(["-SinkContainer", sink_container])
                                if sink_folder:
                                    cmd.extend(["-SinkFolderPath", sink_folder])
                                if sink_file:
                                    cmd.extend(["-SinkFileName", sink_file])

                            if cleanup_temp_adf:
                                cmd.append("-CleanupTempAdf")

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
