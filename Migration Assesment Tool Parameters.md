1. ###### **Component parity**



Score 0 – Simple copy pipeline moving data from Azure SQL to Fabric Lakehouse copy activity (native equivalent).

Score 1 – Pipeline uses Copy + Wait activities; Copy maps 1:1, Wait needs minor tweak (e.g., replace with Fabric delay).

Score 2 – Pipeline mixes Copy and Mapping Data Flow; Copy ports cleanly, Data Flow must be redesigned as Dataflow Gen2 or Spark.

Score 3 – Pipeline relies on SSIS(SQL Server Integration Services) package execution a with on-prem DLL(Dynamic Link Library); both require full re-engineering.



###### **2. Non‑migratable/unsupported components**



Score 0 – No unsupported activities or datasets.

Score 1 – One Azure Batch custom activity storing logs locally; easy to swap for Fabric equivalent.

Score 2 – Four custom Python notebooks with embedded third-party libs absent in Fabric; moderate effort to refactor.

Score 3 – Eight critical activities using legacy SAP connector that Fabric lacks; each needs bespoke workaround.

###### 

###### **3.  Connectivity and network**



Score 0 – Sources are Azure SQL Database and Fabric Lakehouse using managed identities.

Score 1 – One on-prem SQL Server accessed via standard data gateway already in place.

Score 2 – Mix of on-prem SQL, private-link Cosmos DB, multiple firewall/VNET steps.

Score 3 – Custom ODBC drivers unsupported in Fabric.



###### **4.  Orchestration complexity**



Score 0 – Pipeline has three activities run sequentially (Copy → Stored Proc → Copy).

Score 1 – Pipeline fan-out to two branches with retry policies and timeout handling, then join.

Score 2 – Pipeline contains 18 activities across nested ForEach loops, Execute Pipeline calls, and custom error handling.

Score 3 – Event-triggered parent pipeline dynamically invoking dozens of child pipelines based on metadata, with parameterized ForEach, until loops, and cross-factory dependencies.









**Total score	Band	    Typical profile**

0 – 4	        Easy	    All or most scores ≤1. Example: 0+1+1+1 = 3.

5 – 8	       Medium	    At least one “2”, rest ≤2. Example: 2+2+2+1 = 7.

9 – 12	        Hard	    Any “3” or several “2”s driving the sum high. Example: 3+3+2+2 = 10.

