#!/usr/bin/env python
import sys
import re
import argparse
import json


def table_dump_query(table_name, path, rows_per_dump):
    """
    Function that accepts table name input string and returns ABL query to create
    temp-table then output json file named t_<table_name>_e_<epoch>.json
    """
    return"""
    DEFINE TEMP-TABLE tt NO-UNDO LIKE %(table_name)s
    FIELD rec_id AS RECID
    FIELD epoch_time AS INT64.

    DEFINE VARIABLE epoch AS DATETIME NO-UNDO.
    DEFINE VARIABLE unixTime AS INT64 NO-UNDO.
    DEFINE VARIABLE htt AS HANDLE NO-UNDO.
    DEFINE VARIABLE cFileName AS CHARACTER NO-UNDO FORMAT "x(60)".
    DEFINE VARIABLE rowCount as INT64 NO-UNDO.

    epoch = DATETIME(1,1,1970,0,0,0,0).
    rowCount = 0.

    htt = TEMP-TABLE tt:HANDLE.

    FOR EACH platte.%(table_name)s NO-LOCK:
      IF rowCount = %(rows_per_dump)s THEN DO: 
        unixTime = interval(NOW, epoch, "milliseconds").
        cFileName = "%(path)s/t__%(table_name)s__e__" + STRING(unixTime) + "__insert.json".
        htt:WRITE-JSON("FILE", cFileName + "_partial", TRUE).
        OS-RENAME VALUE(cFileName + "_partial") VALUE(cFileName).
        rowCount = 0.
        EMPTY TEMP-TABLE tt.
      END.
      rowCount = rowCount + 1.
      CREATE tt.
      BUFFER-COPY %(table_name)s TO tt.
      tt.rec_id = RECID(%(table_name)s).
      unixTime = interval(NOW, epoch, "milliseconds").
      tt.epoch_time = unixTime.
    END.
    unixTime = interval(NOW, epoch, "milliseconds").
    cFileName = "%(path)s/t__%(table_name)s__e__" + STRING(unixTime) + "__insert.json".
    htt:WRITE-JSON("FILE", cFileName + "_partial", TRUE).
    OS-RENAME VALUE(cFileName + "_partial") VALUE(cFileName)
    
""" % {'path': path, 'table_name': table_name, 'rows_per_dump': rows_per_dump}


def read_ddl(filename):
    try:
        f = open(filename, 'r')
        df = f.read()
        f.close()
        return df
    except IOError as e:
        print("Could not read file '%s'!" % filename)
        sys.exit(1)


def write_abl(filename, abl):
    try:
        f = open(filename, 'w')
        f.write(abl)
        f.close()
        print("Wrote '%s'." % filename)
        return True
    except IOError as e:
        print("Could not write file '%s'!" % filename)
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description="Output ABL files to dump contents per table.")
    parser_required = parser.add_argument_group("required arguments")
    parser_required.add_argument("--input", type=str, required=True, help="input file containing RDBMS-customized intermediate DDL")
    parser_required.add_argument("--outputdir", type=str, required=True, help="output directory for ABL files")
    parser.add_argument("--jsondir", type=str, default="/tmp", help="directory to output JSON table dumps (default: /tmp)")
    parser.add_argument("--rows", type=int, default=250000, help="rows per JSON file (default: 250000)")
    parsed_args = vars(parser.parse_args())

    ddl = read_ddl(parsed_args['input'])

    try:
        print("Parsing %s" % parsed_args['input'])
        ddl = json.loads(read_ddl(parsed_args['input']), encoding="latin_1")
    except ValueError as e:
        print("Failed to load %s, error: %s" % (parsed_args['input'], e))
        sys.exit(1)

    for table in ddl['tables']:
        filename = "%s/abl_dump_%s.p" % (parsed_args['outputdir'], table['name'][0])
        write_abl(filename, table_dump_query(table['name'][0], parsed_args['jsondir'], parsed_args['rows']))


if __name__ == "__main__":
    import sys 
    if sys.version_info[0] < 3:
        print("Python 3 or greater is required.")
        sys.exit(1)

    main()
