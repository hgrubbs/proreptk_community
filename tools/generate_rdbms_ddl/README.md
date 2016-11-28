## generate_rdbms_ddl

### customize_ddl_mysql

Read an intermediate DDL file produced by `parse_df.py`, and writes a modified version with MySQL specific configuration and field changes.

### generate_mysql_db

Reads an DDL file customized by `customize_ddl_mysql` and produces MySQL CREATE statements for the database, tables, and indexes.
