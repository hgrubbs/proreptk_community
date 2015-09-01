# ProrepTK
Data replication toolkit for OpenEdge Progress DB to MySQL/Postgres/other RDBMS.

### Replication highlights
1. Real-time replication from Progress to target DB
2. Parallel processing of replicated changes with linear performance scaling
3. Modular architecture making it trivial to add new target DB back-ends
4. Very fast: an initial "full load" of replicated of 300GB data set completed in 45m
5. No downtime! The Progress DB never has to be stopped or taken out of service to start replicating
6. Open source - all code is licensed as GPLv3.

### What is Progress? Why do I want to replicate it?
Progress is a proprietary database system from 1981 that is still in use today. Queries are built using "ABL"(or "4gl") syntax, a feature-poor and unsuitable replacement for SQL. Replication allows you to use traditional SQL and reporting tools that work on SQL databases to access your data.

### What's in the "toolkit"?
1. **Schema parser**: parses and creates a portable schema representation of your Progress DB.
2. **RDBMS customization for MySQL and other back-ends**: rename and map illegal SQL column names that Progress may use(such as "%" and "key") to legal SQL values.
3. **ABL/4gl "data dump" generation**: creates JSON dumps of entire Progress databases.
4. **Persistence engine**: replication daemon that changes target DB data in response to replication events.

### Known issue with some Progress schemas
Software vendors that bundle Progress with their client software will *rarely* use restricted portions of the Progress schema for their own business logic. Specifically, these are the "REPLICATION-WRITE" and "REPLICATION-DELETE" triggers, which are *reserved* by Progress for replication-related functionality. However, sometimes vendors will decide to use these anyway(a bad idea), and you will need to integrate the replication triggers with their own business logic triggers. This is a rare scenario, but it has been encountered at least once.

### Software maturity
ProrepTK is currently **beta** software, and the possibility of bugs exists. If you should encounter any undefined behavior, please report it via GitHub with as much detail surrounding your use case as possible.

### Getting started
Explore `docs/` for help getting started.
