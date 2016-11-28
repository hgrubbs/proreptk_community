import pdb
import sys
import re
import argparse
from multiprocessing import Pool
import json


def read_df(filename):
    try:
        f = open(filename, 'r', encoding="latin_1")
        df = f.read()
        f.close()
        return df
    except IOError as e:
        print("Could not read file '%s'!" % filename)
        sys.exit(1)


def write_ddl(filename, ddl):
    try:
        f = open(filename, 'w', encoding="latin_1")
        f.write(ddl)
        f.close()
        return True
    except IOError as e:
        print("Could not write file '%s'!" % filename)
        sys.exit(1)


def clean_tables(ddl):
    True


def main():

    parser = argparse.ArgumentParser(description="Removes tables from MySQL intermediate DDL not in whitelist")
    parser_required = parser.add_argument_group("required arguments")
    parser_required.add_argument("--input", type=str, required=True, help="input file containing proreptk MySQL DDL")
    parser_required.add_argument("--output", type=str, required=True, help="output file for intermediate DDL")
    parser_required.add_argument("--input-whitelist", type=str, required=True, help="input file containing tables to include(1 per line)")
    parsed_args = vars(parser.parse_args())

    ddl = json.loads(read_df(parsed_args['input']))
    whitelist = read_df(parsed_args['input_whitelist']).lower().splitlines()

    newtables = []
    for table in ddl['tables']:
        if table['name'][0].lower() in whitelist:
            newtables.append(table)

    ddl['tables'] = newtables
    ddl = json.dumps(ddl, indent=4)

    if write_ddl(parsed_args['output'], ddl) is True:
        print("Wrote intermediate DDL to '%s'." % parsed_args['output'])
        sys.exit(0)


if __name__ == "__main__":
    import sys
    if sys.version_info[0] < 3:
        print("Python 3 or greater is required.")
        sys.exit(1)
    main()
