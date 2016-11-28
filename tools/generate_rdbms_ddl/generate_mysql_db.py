#!/usr/bin/env python
import sys
import re
import argparse
import json
import pdb


def read_ddl(filename):
    try:
        f = open(filename, 'r')
        df = f.read()
        f.close()
        return df
    except IOError as e:
        print("Could not read file '%s'!" % filename)
        sys.exit(1)


def write_sql(filename, sql_ddl):
    try:
        f = open(filename, 'w')
        f.write(sql_ddl)
        f.close()
        return True
    except IOError as e:
        print("Could not write file '%s'!" % filename)
        sys.exit(1)


def gen_table_create(tables):
    type_map = {
        "character": "VARCHAR",
        "logical": "BOOL",
        "decimal": "DECIMAL",
        "float": "FLOAT",
        "raw": "TEXT",
        "int64": "BIGINT",
        "integer": "INTEGER",
        "recid": "BIGINT",
        "datetime": "DATETIME",
        "date": "DATE",
        "clob": "BLOB",
        "blob": "BLOB",
    }

    buf = ""
    for table in tables:
        table_buf = ""
        table_buf += ("CREATE TABLE %s" % table['name'][1])
        columns = []
        for col in table['columns']:
            # Handle progress "array" data type as a varchar that will store as ", ".join(array)
            if ('extent' in col):
                col['type'] = 'raw'
                #col['type'] = 'character'
                #col['max-width'] = 500

            b = col['name'][1] + ' ' + type_map[col['type']]
            
            # Handle types we must specify a size for
            if col['type'] == "decimal":
                if 'decimals' in col:
                    b += "(20,%s)" % col['decimals']
                else:
                    b += "(20,4)"
            elif col['type'] == "character":
                if 'max-width' in col:
                    b += "(" + str(int(col['max-width']) + 2)  + ")"
                else:
                    b += "(255)"
            columns.append(b)
        columns.insert(0, 'repl_epoch BIGINT NOT NULL')
        columns.insert(0, 'repl_recid BIGINT PRIMARY KEY NOT NULL')
        buf += table_buf + '(' + ', '.join(columns) + ");\n"
    return buf


def gen_index_create(indexes):
    
    buf = ""
    for index in indexes:
        table_buf = ""
        table_buf += "CREATE INDEX "
        columns = [c[1] for c in index['index_details']['columns']]
        table_buf += index['index_name'][1] + " ON " + index['table_name'][1] + "(" + ", ".join(columns) +")"
        buf += table_buf + ";\n"
    return buf


def main():
    parser = argparse.ArgumentParser(description="Convert intermediate DDL to MySQL-specific DDL")
    parser_required = parser.add_argument_group("required arguments")
    parser_required.add_argument("--input", type=str, required=True, help="input file containing intermediate DDL")
    parser_required.add_argument("--tableoutput", type=str, required=True, help="output file for SQL table DDL")
    parser_required.add_argument("--indexoutput", type=str, required=True, help="output file for SQL index DDL")
    parsed_args = vars(parser.parse_args())

    ddl = read_ddl(parsed_args['input'])

    try:
        print("Parsing %s" % parsed_args['input'])
        ddl = json.loads(read_ddl(parsed_args['input']), encoding="latin_1")
    except ValueError as e:
        print("Failed to load %s, error: %s" % (parsed_args['input'], e))
        sys.exit(1)

    print("Building table creation SQL.")
    create_table_sql = gen_table_create(ddl['tables'])
    print("Building index creation SQL.")
    create_index_sql = gen_index_create(ddl['indexes'])

    if write_sql(parsed_args['tableoutput'], create_table_sql) is True:
        print("Wrote SQL DDL to '%s'." % parsed_args['tableoutput'])

    if write_sql(parsed_args['indexoutput'], create_index_sql) is True:
        print("Wrote SQL DDL to '%s'." % parsed_args['indexoutput'])


if __name__ == "__main__":
    import sys 
    if sys.version_info[0] < 3:
        print("Python 3 or greater is required.")
        sys.exit(1)

    main()
