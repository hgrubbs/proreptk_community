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


def parse_index_names(df):
    df = re.sub('\n', '_EOL_', df)
    indexes = re.findall(r'ADD INDEX "(.+?)" ON "(.*?)"', df)
    return indexes


def parse_index_fields(iteration, total_indexes, index_name, table_name, df):
    print("(%s/%s) Parsing index '%s' on table '%s'" % (iteration, total_indexes, index_name, table_name))
    lines = df.split('\n')
    pat = r'^ADD INDEX "%s" ON "%s" $' % (index_name, table_name)
    index_details = None
    for i in range(0, len(lines)):
        m = re.search(pat, lines[i])
        if m is None:
            continue
        else:
            offset = i + 250  # maximum limit of descriptive lines for an index
            if offset > len(lines)-1:
                offset = len(lines) - 1
            index_details = parse_index_metadata(index_name, lines[i+1:offset])
    return {"index_name": index_name, "table_name": table_name, "index_details": index_details}


def parse_table_names(df):
    df = re.sub('\n', '_EOL_', df)
    tables = re.findall(r'ADD TABLE "(.+?)".*?_EOL_(.*?)_EOL__EOL_', df)
    return tables


def parse_index_metadata(index_name, lines):
    tests = [
        {
          'pattern': r'AREA "(.*?)"',
          'name': 'area'
        },
        {
          'pattern': r'INDEX-FIELD "(.*?)"',
          'name': 'columns',
          'append': True,
        },
        {
          'pattern': r'^(UNIQUE)$',
          'name': 'unique'
        },
        {
          'pattern': r'^(PRIMARY)$',
          'name': 'primary'
        },
    ]
    valid_lines = []
    for line in lines:
        if line.startswith('  '):
            valid_lines.append(line)
        elif re.search(r'^$', line) is not None:
            break
    meta_data = {"columns": []}
    for line in valid_lines:
        line = re.sub('^  ', '', line)

        for test in tests:
            m = re.match(test['pattern'], line)
            if m is not None:
                if "append" not in test:
                    meta_data[test['name']] = m.groups()[0]
                elif ("append" in test) and (test['append'] is True):
                    meta_data[test['name']].append(m.groups()[0])
                continue
    return meta_data


def parse_table_fields(iteration, total_tables, table_name, df):
    print("(%s/%s) Parsing table '%s'" % (iteration, total_tables, table_name))
    lines = df.split('\n')
    pat = r'^ADD FIELD "(.+?)" OF "%s" AS (.+?) $' % table_name
    columns = []
    for i in range(0, len(lines)):
        m = re.search(pat, lines[i])
        if m is None:
            continue
        else:
            columns.append(parse_table_col_metadata(m.groups()[0], m.groups()[1], lines[i+1:i+20]))
    return {"name": table_name, "columns": columns}


def parse_table_col_metadata(col_name, col_type, lines):
    tests = [
        {
          'pattern': r'DESCRIPTION "(.*?)"',
          'name': 'description'
        },
        {
          'pattern': r'FORMAT "(.*?)"',
          'name': 'format'
        },
        {
          'pattern': r'INITIAL "(.*?)"',
          'name': 'initial'
        },
        {
          'pattern': r'LABEL "(.*?)"',
          'name': 'label'
        },
        {
          'pattern': r'POSITION (.*?)$',
          'name': 'position'
        },
        {
          'pattern': r'MAX-WIDTH (.*?)$',
          'name': 'max-width'
        },
        {
          'pattern': r'DECIMALS (.*?)$',
          'name': 'decimals'
        },
        {
          'pattern': r'COLUMN-LABEL "(.*?)"',
          'name': 'column-label'
        },
        {
          'pattern': r'ORDER (.*?)$',
          'name': 'order'
        },
        {
          'pattern': r'HELP "(.*?)"',
          'name': 'help'
        },
        {
          'pattern': r'VIEW-AS "(.*?)"',
          'name': 'view-as'
        },
        {
          'pattern': r'EXTENT (\d+)',
          'name': 'extent'
        },
    ]
    valid_lines = []
    for line in lines:
        if len(line) != 0:
            valid_lines.append(line)
        else:
            break
    meta_data = {
                  'type': col_type,
                  'name': col_name
                }
    for line in valid_lines:
        line = re.sub('^  ', '', line)

        for test in tests:
            m = re.match(test['pattern'], line)
            if m is not None:
                meta_data[test['name']] = m.groups()[0]
                continue
    return meta_data


def main():
    def do_tables(df, processes, excluded_tables):
        tables = parse_table_names(df)

        # Remove tables which are excluded
        to_remove = []
        for i in range(0, len(tables)):
            table_name, table_metadata = tables[i]
            for pat in excluded_tables:
                if re.match(pat, table_name, re.I) is not None:
                    print("Skipping table '%s'. Table name matches exclusion '%s'." % (table_name, pat))
                    to_remove.append(i)
                    break
        to_remove.reverse()
        for index in to_remove:
            tables.pop(index)

        print("Identified %s tables tables to parse." % len(tables))
        pool = Pool(processes=processes)
        pool_results = []
        for i in range(0, len(tables)):
            table_name, table_metadata = tables[i]
            pool_results.append(pool.apply_async(parse_table_fields, [i+1, len(tables), table_name, df]))
        pool.close()
        pool.join()
        return [r.get() for r in pool_results]

    def do_indexes(df, processes, excluded_tables, excluded_indexes):
        indexes = parse_index_names(df)

        # Remove indexes on tables which are excluded
        to_remove = []
        for i in range(0, len(indexes)):
            index_name, table_name = indexes[i]
            for pat in excluded_tables:
                if re.match(pat, table_name) is not None:
                    print("Skipping index '%s' on table '%s'. Table name matches exclusion '%s'." % (index_name, table_name, pat))
                    to_remove.append(i)
                    break
        to_remove.reverse()
        for index in to_remove:
            indexes.pop(index)

        # Remove indexes which are excluded
        to_remove = []
        for i in range(0, len(indexes)):
            index_name, table_name = indexes[i]
            for pat in excluded_indexes:
                if re.match(pat, index_name) is not None:
                    print("Skipping index '%s' on table '%s'. Index name matches exclusion '%s'." % (index_name, table_name, pat))
                    to_remove.append(i)
                    break
        to_remove.reverse()
        for index in to_remove:
            indexes.pop(index)

        print("Identified %s indexes to parse." % len(indexes))
        pool = Pool(processes=processes)
        pool_results = []
        for i in range(0, len(indexes)):
            index_name, table_name = indexes[i]
            pool_results.append(pool.apply_async(parse_index_fields, [i+1, len(indexes), index_name, table_name, df]))
        pool.close()
        pool.join()
        return [r.get() for r in pool_results]

    parser = argparse.ArgumentParser(description="Convert DF format to intermediate DDL")
    parser_required = parser.add_argument_group("required arguments")
    parser_required.add_argument("--input", type=str, required=True, help="input file containing progress DDL (eg 'dbname.df')")
    parser_required.add_argument("--output", type=str, required=True, help="output file for intermediate DDL")
    parser.add_argument("--concurrency", metavar='N', type=int, default=1, help="concurrency factor for multiple cpus (default: 1)")
    parser.add_argument("--skip-indexes", action="store_const", const=True, default=False, help="skip indexes (default: false)")
    parser.add_argument("--skip-tables", action="store_const", const=True, default=False,  help="skip tables (default: false)")
    parser.add_argument("--exclude-table", metavar="<pattern>", action="append", type=str, default=[], help="exclude tables matching regex pattern (can be repeated to exclude multiple patterns)")
    parser.add_argument("--exclude-index", metavar="<pattern>", action="append", type=str, default=[], help="exclude indexes matching regex pattern (can be repeated to exclude multiple indexes)")
    parsed_args = vars(parser.parse_args())

    df = read_df(parsed_args['input'])

    if parsed_args['skip_indexes'] is True:
        indexes = []
    else:
        indexes = do_indexes(df, parsed_args['concurrency'], parsed_args['exclude_table'], parsed_args['exclude_index'])

    if parsed_args['skip_tables'] is True:
        tables = []
    else:
        tables = do_tables(df, parsed_args['concurrency'], parsed_args['exclude_table'])

    ddl = json.dumps({"indexes": indexes, "tables": tables})

    if write_ddl(parsed_args['output'], ddl) is True:
        print("Wrote intermediate DDL to '%s'." % parsed_args['output'])
        sys.exit(0)


if __name__ == "__main__":
    import sys
    if sys.version_info[0] < 3:
        print("Python 3 or greater is required.")
        sys.exit(1)
    main()
