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

import pdb
import sys
import os
import re
import argparse
from multiprocessing import Pool
import json
import glob
import time
import datetime
import base64
import traceback
import socket


def read_ddl(filename):
    try:
        f = open(filename, 'r', encoding="latin_1")
        df = f.read()
        f.close()
        return df
    except IOError as e:
        print("Could not read file '%s'!" % filename)
        sys.exit(1)


def persist_row_mysql(ddl, op, table, epoch, rows, fileseq, process_num=0):
    """Persists data from filename into MySQL. Returns dictionary containing summary of persist operation.
    """
    def mysql_connect(ddl):
        mysql_dbkeywords = {
                            "host": ddl['config']['dbhost'],
                            "port": ddl['config']['dbport'],
                            "user": ddl['config']['dbuser'],
                            "passwd": ddl['config']['dbpass'],
                            "db": ddl['config']['dbname'],
                            "charset": "utf8",
                            "cursorclass": pymysql.cursors.DictCursor
                           }
        try:
            conn = pymysql.connect(**mysql_dbkeywords)
            if ddl['args']['fastinsert'] is False:
                conn.autocommit(1)
            cur = conn.cursor()
            return conn, cur
        except Exception as e:
            print("spid=%s !!! MySQL connection error: %s" % (spid, e))
            raise e

    spid = base64.b16encode(str(datetime.datetime.now().microsecond).encode() + str(base64.b16encode(os.urandom(4))).encode())
    spid = spid.decode()
    import pymysql
    import pymysql.cursors

    conn, cur = mysql_connect(ddl) 
    inserts = 0  # To report on rows inserted
    updates = 0  # To report on rows updated
    skips = 0  # To report on rows skipped
    deletes = 0
    if (op == "insert") or (op == "update") or (op =="write"):
        table_local = ddl['loaded'][table]  # Get name_local for table name
        for row in rows:
            if ddl['args']['fastinsert'] is False:  # Check for existance of row matching the one we are updating/inserting
                update = False
                rowcount = cur.execute("SELECT repl_recid, repl_epoch FROM " + table + " WHERE repl_recid = %s LIMIT 1", (row['rec_id'],))
                if rowcount != 0:
                    local_row = cur.fetchone()
                    if local_row['repl_epoch'] >= row['epoch_time']:  # Is replicated row newer than row received?
                        skips += 1
                        continue
                    else:  # Delete existing row before inserting new one
                        try:
                            cur.execute("DELETE FROM " + table + " WHERE repl_recid =%s LIMIT 1", row['rec_id'])
                        except pymysql.err.OperationalError as e:  # did MySQL disconnect us for some reason(eg timeout)?
                            print("spid=%s !!! MySQL connection error, reconnecting and trying again 1 time" % spid)
                            if e[0] == 2013:
                                conn, cur = mysql_connect(ddl)
                                cur.execute("DELETE FROM " + table + " WHERE repl_recid =%s LIMIT 1", row['rec_id'])
                        update = True
                        updates += 1

                if update is False:
                    inserts += 1
            else:
                inserts +=1

            for colname in [c for c in row.keys()]:  # Rename columns to name_local
                if (colname != "rec_id") and (colname != "epoch_time"):
                    if 'extent' in table_local['columns'][colname]:  # Join columns that contain arrays into strings such as "elem1, elem2, elem3"
                        row[colname] = ", ".join([str(v) for v in row[colname]])
                    row[table_local['columns'][colname]['name_local']] = row.pop(colname)  # Rename column to the (possibly different) new local name
            row['repl_epoch'] = row.pop('epoch_time')
            row['repl_recid'] = row.pop('rec_id')
            if (ddl['args']['fastinsert'] is True) or (ddl['args']['concurrency'] == 'files'):
                sql = "INSERT IGNORE INTO %s " % table_local['name_local']
            else:
                sql = "INSERT INTO %s " % table_local['name_local']
            colnames = [c for c in row.keys()]  # Convert to list() for .sort method
            colnames.sort()
            sql += "(%s) " % ", ".join(colnames)
            placeholders = ["%s" for col in row]
            placeholders = ",".join(placeholders)
            sql += "VALUES (%s)" % placeholders
            try:
                cur.execute(sql, [row[c] for c in colnames])
            except pymysql.err.OperationalError as e:  # did MySQL disconnect us for some reason(eg timeout)?
                if e[0] == 2013:
                    conn, cur = mysql_connect(ddl)
                    cur.execute(sql, [row[c] for c in colnames])  # Try once more, and crash if it doesn't work
            except Exception as e:  # Get some useful output that will get lost in the multiprocessing slew out output otherwise
                exc_text = """
spid=%(spid)s !!! Unhandled exception during MySQL insert !!!
table: %(table)s
row: %(row)s
sql: %(sql)s
""" % {"spid": spid, "table": table, "row": row, "sql": sql}
                print(exc_text)
                exc_type, exc_value, exc_traceback = sys.exc_info()
                traceback.print_tb(exc_traceback, limit=100, file=sys.stdout)
                traceback.print_exception(exc_type, exc_value, exc_traceback, limit=10, file=sys.stdout)
                raise e

    elif op == "delete":
        for row in rows:
            rowcount = cur.execute("SELECT repl_recid, repl_epoch FROM " + table + " WHERE repl_recid = %s LIMIT 1", (row['rec_id'],))
            if rowcount != 0:
                local_row = cur.fetchone()
                if local_row['repl_epoch'] >= row['epoch_time']:
                    skips += 1
                    continue
                else:
                    try:
                        cur.execute("DELETE FROM " + table + " WHERE repl_recid =%s LIMIT 1", row['rec_id'])
                    except pymysql.err.OperationalError as e:  # did MySQL disconnect us for some reason(eg timeout)?
                        print("spid=%s !!! MySQL connection error, reconnecting and trying again 1 time" % spid)
                        if e[0] == 2013:
                            conn, cur = mysql_connect(ddl)
                            cur.execute("DELETE FROM " + table + " WHERE repl_recid =%s LIMIT 1", row['rec_id'])
                    deletes += 1
            else:  # Row to delete is not present in replication target
                skips += 1

    if ddl['args']['fastinsert'] is True:
        conn.commit()
    cur.close()
    conn.close()
    result = {"rows": len(rows), "deletes": deletes, "inserts": inserts, "updates": updates, "skips": skips, "table": table, "op": op, "fileseq": fileseq, "spid": spid, "process_num": process_num}
    print("spid=%s finished persist_row_mysql, result: %s" % (spid, result))
    return result


def process_json_file(ddl, filename, fileseq):
    """Wraps the persist_row_mysql function, returning the same `result` types.
    """
    pid = base64.b16encode(str(datetime.datetime.now().microsecond).encode() + str(base64.b16encode(os.urandom(4))).encode())
    pid = pid.decode()
    print("pid=%s Entered process_json_file for '%s' (file #%s)" % (pid, filename, fileseq))
    if os.path.isfile(filename + ".lock"):
        print("pid=%s Skipped '%s', lock exists(file #%s)" % (pid, filename, fileseq))
        return  [{"rows": 0, "deletes": 0, "inserts": 0, "updates": 0, "skips": 0, "table": table, "op": op, "fileseq": fileseq, "spid": None, "process_num": None}]

    f = open(filename + ".lock", 'w')
    f.write("timestamp:" + str(datetime.datetime.now()) + "\nhost:" + socket.gethostname() + "\npid:" + pid + "\n")
    f.close()
    f = open(filename, 'r')
    try:
        size = os.path.getsize(filename)
        print("pid=%s Reading %s bytes from '%s'(file #%s)" % (pid, size, filename, fileseq))
        rows = json.load(f)['tt']
        f.close()
        print("pid=%s Read %s rows from '%s'(file #%s)" % (pid, len(rows), filename, fileseq))
    except Exception as e:
        print("pid=%s !!! Could not open and/or parse file '%s'. Removing lock but leaving file. Error: %s" % (pid, filename, e))
        os.remove(filename + ".lock")
        return  [{"rows": 0, "deletes": 0, "inserts": 0, "updates": 0, "skips": 0, "table": table, "op": op, "fileseq": fileseq, "spid": None, "process_num": None}]

    m = re.search(r'^.*/(t|1)__(.+?)__e__(.+?)__(insert|update|write|delete)\.json$', filename)
    if m is None:
        print("pid=%s !!! Could not determine metadata from filename '%s'. Removing lock but leaving file" % (pid, filename))
        os.remove(filename + ".lock")
        return  [{"rows": 0, "deletes": 0, "inserts": 0, "updates": 0, "skips": 0, "table": None, "op": None, "fileseq": fileseq, "spid": None, "process_num": None}]
    else:
        json_type = m.groups()[0]
        table = m.groups()[1]
        epoch = m.groups()[2]
        op = m.groups()[3]

    print("pid=%s Persisting %s rows in table '%s'" % (pid, len(rows), table))

    if ddl['args']['debug'] is True:
        ddl['args']['concurrency'] = "files"

    if ddl['args']['concurrency'] == "rows":
        if len(rows) == 0:
            os.remove(filename + ".lock")
            if ddl['args']['keepjson'] is False:
                os.remove(filename)
            return  [{"rows": 0, "deletes": 0, "inserts": 0, "updates": 0, "skips": 0, "table": table, "op": op, "fileseq": fileseq, "spid": None, "process_num": None}]
        print("pid=%s Spawning sub-processes to process %s rows in '%s'(file #%s)" % (pid, len(rows), table, fileseq))
        pool_results = []
        processrows = ddl['args']['processrows']
        if processrows > len(rows):
            pool = Pool(processes=1, maxtasksperchild=5)
        else:
            pool = Pool(processes=ddl['args']['processes'], maxtasksperchild=5)
        process_num = 0
        for i in range(0, len(rows), processrows):
            process_num += 1
            these_rows = rows[i:i+processrows]
            print("pid=%s Assigning %s rows from '%s'(file #%s) to worker process #%s" % (pid, len(these_rows), table, fileseq, process_num))
            pool_results.append(pool.apply_async(persist_row_mysql, [ddl, op, table, epoch, these_rows, fileseq, process_num]))
        pool.close()
        pool.join()
        results = [r.get() for r in pool_results]
    elif ddl['args']['concurrency'] == "files":
        results = persist_row_mysql(ddl, op, table, epoch, rows, fileseq)

    os.remove(filename + ".lock")
    if ddl['args']['keepjson'] is False:
        os.remove(filename)
        print("pid=%s Removed '%s' and lock (file #%s)" % (pid, filename, fileseq))
    else:
        print("pid=%s Removed lock for '%s' (file #%s)" % (pid, filename, fileseq))
    return results


def simplify_ddl_tables(ddl_tables):
    """Return a simplified dictionary containing original and local table names for quicker navigation during persistence.
    """
    tables = {}
    for ddl_table in ddl_tables:
        this_table = {
            "name_orig": ddl_table['name'][0],
            "name_local": ddl_table['name'][1],
            "columns": {}
        }
        for col in ddl_table['columns']:
            this_table['columns'][col['name'][0]] = {
                "name_orig": col['name'][0],
                "name_local": col['name'][1],
                "type_orig": col['type'],
            }
            if 'extent' in col:
                this_table['columns'][col['name'][0]]['extent'] = col['extent']
        tables[this_table['name_orig']] = this_table
    return tables


def main():
    parser = argparse.ArgumentParser(description="ProrepTK persistence engine")
    parser_required = parser.add_argument_group("required arguments")
    parser_required.add_argument("--input", type=str, required=True, help="input file containing RDBMS specific DDL")
    parser_required.add_argument("--jsondir", type=str, required=True, help="directory containing incoming JSON files")
    parser.add_argument("--concurrency", type=str, choices=('files', 'rows'), default="rows", help="concurrency strategy (default: rows) NOTE: 'files' is unsafe in some circumstances")
    parser.add_argument("--processes", metavar='N', type=int, default=1, help="concurrency factor for multiple cpus (default: 1)")
    parser.add_argument("--processrows", metavar='N', type=int, default=5000, help="rows assigned to each worker process (default: 5000)")
    parser.add_argument("--delay", metavar='N', type=int, default=5, help="mainloop delay (default: 5)")
    parser.add_argument("--onepass", action="store_const", const=True, default=False, help="only iterate through jsondir 1 time (default: false)")
    parser.add_argument("--fastinsert", action="store_const", const=True, default=False, help="insert new data with no checks (default: false)")
    parser.add_argument("--keepjson", action="store_const", const=True, default=False, help="keep JSON files after processing (default: false)")
    parser.add_argument("--debug", action="store_const", const=True, default=False, help="disable all concurrency to allow debugging (default: false)")
    parser.add_argument("--globpattern", type=str, default="*.json", help="globbing pattern (default: '*.json')")
    parsed_args = vars(parser.parse_args())

    print("Loading configuration '%s'" % parsed_args['input'])
    ddl = read_ddl(parsed_args['input'])
    ddl = json.loads(ddl)

    # Load the simplified table map, and reduce the size of ddl, since it will be copied between processes and referenced between threads.
    print("Adding optimized DDL and run-time configuration")
    ddl['loaded'] = simplify_ddl_tables(ddl['tables'])
    ddl['args'] = parsed_args
    del ddl['tables']
    del ddl['indexes']

    print("Loaded configuration for database '%s', backend '%s'" % (ddl['config']['dbname'], ddl['config']['rdbms']))

    if ddl['args']['debug'] is True:
        ddl['args']['concurrency'] = "rows"

    while True:  # mainloop
        try:
            results = []
            print("Mainloop delay for %ss" % ddl['args']['delay'])
            if ddl['args']['debug'] is True:
                print("DEBUG MODE ON")
            time.sleep(parsed_args['delay'])
            print("Globbing '%s/%s'" % (ddl['args']['jsondir'], ddl['args']['globpattern']))
            files = glob.glob(ddl['args']['jsondir'] + "/" + ddl['args']['globpattern'])
            files.sort()
            filecount = len(files)
            fileseq = 0
            print("%s JSON files present." % len(files))

            if ddl['args']['concurrency'] == "files":
                pool = Pool(processes=ddl['args']['processes'], maxtasksperchild=500)
                for f in files:
                    fileseq += 1
                    if os.path.isfile(f + ".lock"):
                        print("Skipping locked file '%s'" % f)
                        continue  # skip files already locked for processing
                    print("(%s/%s) Processing '%s'." % (fileseq, filecount, f))
                    results.append(pool.apply_async(process_json_file, [ddl, f, fileseq]))
                pool.close()
                pool.join()
                for r in results:
                    print("Persister returned: %s" % r.get())

            elif ddl['args']['concurrency'] == "rows":
                for f in files:
                    fileseq += 1
                    if os.path.isfile(f + ".lock"):
                        print("Skipping locked file '%s'" % f)
                        continue  # skip files already locked for processing
                    print("(%s/%s) Processing '%s'." % (fileseq, filecount, f))
                    results = process_json_file(ddl, f, fileseq)
                    print("(%s/%s) Persister returned: %s" % (fileseq, filecount, results))
                    if ddl['args']['debug'] is True:
                        results = [results]  # Summary must be a list() to work, and debug only returns single dicts
                    summary = {
                        'rows': sum([r['rows'] for r in results]),
                        'inserts': sum([r['inserts'] for r in results]),
                        'updates': sum([r['updates'] for r in results]),
                        'deletes': sum([r['deletes'] for r in results]),
                        'skips': sum([r['skips'] for r in results]),
                    }
                    print("(%s/%s) Persister summary: %s" % (fileseq, filecount, summary))

            if ddl['args']['onepass'] is True:
                print("Single pass completed! %s files processed." % (fileseq))
                sys.exit(0)
        except Exception as e:
            print("!!! Unhandled exception !!!")
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback.print_tb(exc_traceback, limit=100, file=sys.stdout)
            traceback.print_exception(exc_type, exc_value, exc_traceback, limit=10, file=sys.stdout)
            sys.exit(1)

if __name__ == "__main__":
    import sys
    if sys.version_info[0] < 3:
        print("Python 3 or greater is required.")
        sys.exit(1)
    main()
