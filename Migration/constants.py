"""
Constants for ADF to Fabric Migration Tool
"""

# Activity type constants
CONTROL_ACTIVITY_TYPES = {
    "foreach",
    "until",
    "ifcondition",
    "switch",
    "executepipeline",
}

# Connectivity complexity keywords
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

# Supported migratable activity types
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
