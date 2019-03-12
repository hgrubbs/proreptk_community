## Example usage for ProrepTK to replicate to MySQL

1. Dump your Progress database "Data Definitions" using the Data Dictionary tool.
2. Create ProrepTK intermediate DDL from your data definition dump using `tools/parse_df/parse_df.py`.
3. Create MySQL-specific intermediate DDL from the generic DDL generated in step #2 using `tools/generate_rdbms_ddl/customize_ddl_mysql.py`.
4. Create MySQL db/table/index CREATE statements using `tools/generate_rdbms_ddl/generate_mysql_db.py`, and load the resulting tables output into MySQL. Do not yet load indexes to reduce your initial database load times.
5. Create ABL triggers and insertion code with `tools/generate_rdbms_ddl/generate_abl_triggers.py`, then load them into Progress.
6. Create ABL data dump code with `tools/generate_rdbms_ddl/generate_abl_dump.py`, then run them with Progress.
7. Load your dumped Progress data with `tools/persistence_engine/persistence_engine.py`. You most likely want to use the `rows` concurrency strategy option.
8. Load your MySQL index creation statements.
9. Start the persistence engine to monitor your live changes to Progress.

NOTE: These steps can not be run in parallel.
