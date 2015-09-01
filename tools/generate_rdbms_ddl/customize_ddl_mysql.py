#!/usr/bin/env python

#    This file is part of ProrepTK.
#
#    ProrepTK is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    ProrepTK is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with ProrepTK.  If not, see <http://www.gnu.org/licenses/>.

import sys
import re
import argparse
import json


def fix_reserved_name(name):
    """Returns a tuple of (<name>, <replacement_name>) if name is a reserved word, otherwise returns (<name>, <name>).
    """
    reserved_names = [
        ('^key$', '_key_rr'),
        ('^interval$', '_interval_rr'),
        ('^load$', '_load_rr'),
        ('^primary$', '_primary_rr'),
        ('^sensitive$', '_sensitive_rr'),
        ('^limit$', 'limit_rr'),
        ('^order$', 'order_rr'),
        ('^usage$', 'usage_rr'),
        ('^precision$', 'precision_rr'),
        ('^separator$', 'separator_rr'),
        ('%', '_percent_rr_'),
        ('#', '_hash_rr_'),
        ('-', '_dash_rr_'),
    ]
    for (pat, replacement) in reserved_names:
        m = re.search(pat, name)
        if m is not None:
            print("Rewrote '%s' as '%s'." % (name, re.sub(pat, replacement, name)))
            return (name, re.sub(pat, replacement, name))
    return (name, name)
   

def read_ddl(filename):
    try:
        f = open(filename, 'r', encoding="latin_1")
        df = f.read()
        f.close()
        return df
    except IOError as e:
        print("Could not read file '%s'!" % filename)
        sys.exit(1)


def write_ddl(filename, sql_ddl):
    try:
        f = open(filename, 'w', encoding="latin_1")
        f.write(sql_ddl)
        f.close()
        return True
    except IOError as e:
        print("Could not write file '%s'!" % filename)
        sys.exit(1)


def rewrite_tables(tables):
    for table in tables:
        table['name'] = fix_reserved_name(table['name'])
        for column in table['columns']:
            column['name'] = fix_reserved_name(column['name'])
            if 'extent' in column:
                column['extent'] = int(column['extent'])
    return tables


def rewrite_indexes(indexes):
    for index in indexes:
        index['index_name'] = fix_reserved_name(index['index_name'])
        index['table_name'] = fix_reserved_name(index['table_name'])
        columns = []
        for column in index['index_details']['columns']:
            columns.append(fix_reserved_name(column))
        index['index_details']['columns'] = columns
    return indexes


def main():
    parser = argparse.ArgumentParser(description="Convert intermediate DDL to MySQL-specific DDL")
    parser_required = parser.add_argument_group("required arguments")
    parser_required.add_argument("--input", type=str, required=True, help="input file containing intermediate DDL")
    parser_required.add_argument("--output", type=str, required=True, help="output file for SQL DDL")
    parser.add_argument("--dbname", type=str, default="progress", help="MySQL database name (default: progress)")
    parser.add_argument("--dbhost", type=str, default="127.0.0.1", help="MySQL database hostname/ip (default: 127.0.0.1)")
    parser.add_argument("--dbport", type=int, default=3306, help="MySQL database port (default: 3306)")
    parser.add_argument("--dbuser", type=str, default="root", help="MySQL database username (default: root)")
    parser.add_argument("--dbpass", type=str, default="", help="MySQL database password (default: null)")
#    parser.add_argument("--concurrency", metavar='N', type=int, default=1, help="concurrency factor for multiple cpus (default: 1)")
    parsed_args = vars(parser.parse_args())

    ddl = read_ddl(parsed_args['input'])

    try:
        print("Parsing %s" % parsed_args['input'])
        ddl = json.loads(read_ddl(parsed_args['input']))
    except ValueError as e:
        print("Failed to load %s, error: %s" % (parsed_args['input'], e))
        sys.exit(1)

    if "config" in ddl:
        print("Input DDL already has been modified for '%s' and is not suitable for customizing. You must use un-customized intermediate DDL generated by parse_df with this tool.")
        sys.exit(1)
    
    ddl['config'] = {
        "rdbms": "mysql",
        "dbname": parsed_args['dbname'],
        "dbhost": parsed_args['dbhost'],
        "dbport": parsed_args['dbport'],
        "dbuser": parsed_args['dbuser'],
        "dbpass": parsed_args['dbpass'],
    }

    ddl['tables'] = rewrite_tables(ddl['tables'])
    ddl['indexes'] = rewrite_indexes(ddl['indexes'])


    if write_ddl(parsed_args['output'], json.dumps(ddl)) is True:
        print("Wrote MySQL customized DDL to '%s'." % parsed_args['output'])
        sys.exit(0)


if __name__ == "__main__":
    import sys 
    if sys.version_info[0] < 3:
        print("Python 3 or greater is required.")
        sys.exit(1)

    main()