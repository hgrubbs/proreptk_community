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


def add_trigger_query(table, triggerdir):
    return """
DEFINE VARIABLE tableName AS CHARACTER NO-UNDO.
DEFINE VARIABLE triggerFileW AS CHARACTER NO-UNDO.
DEFINE VARIABLE triggerFileD AS CHARACTER NO-UNDO.

tableName = '%(table)s'.
triggerFileW = '%(triggerdir)s/abl_trigger_%(table)s_rw.t'.
triggerFileD = '%(triggerdir)s/abl_trigger_%(table)s_rd.t'.

MESSAGE "Seeking table: " + tableName.
FIND FIRST _file WHERE _file._file-name = tableName.
IF AVAILABLE _file THEN DO:
    MESSAGE "found _file for table: " + tableName.

    /* Delete create trigger */
    FIND _file-trig OF _file WHERE _file-trig._event EQ "CREATE" NO-ERROR.
    IF AVAILABLE _file-trig THEN DO:
        DELETE _file-trig.
    END.

    /* Delete write trigger */
    FIND _file-trig OF _file WHERE _file-trig._event EQ "WRITE" NO-ERROR.
    IF AVAILABLE _file-trig THEN DO:
        DELETE _file-trig.
    END.

    /* Delete delete trigger */
    FIND _file-trig OF _file WHERE _file-trig._event EQ "DELETE" NO-ERROR.
    IF AVAILABLE _file-trig THEN DO:
        DELETE _file-trig.
    END.

    /* Add write trigger */
    FIND _file-trig OF _file WHERE _file-trig._event EQ "REPLICATION-WRITE" NO-ERROR.
    IF AVAILABLE _file-trig THEN DO:
        DELETE _file-trig.
    END.
    MESSAGE "creating _file-trig for REPLICATION-WRITE.".
    CREATE _file-trig.
    ASSIGN _file-trig._proc-name = triggerFileW.
    ASSIGN _file-trig._file-recid = RECID(_file).
    ASSIGN _file-trig._event = "REPLICATION-WRITE".

    /* Add delete trigger */
    FIND _file-trig OF _file WHERE _file-trig._event EQ "REPLICATION-DELETE" NO-ERROR.
    IF AVAILABLE _file-trig THEN DO:
        DELETE _file-trig.
    END.
    MESSAGE "creating _file-trig for REPLICATION-DELETE.".
    CREATE _file-trig.
    ASSIGN _file-trig._proc-name = triggerFileD.
    ASSIGN _file-trig._file-recid = RECID(_file).
    ASSIGN _file-trig._event = "REPLICATION-DELETE".
END.
ELSE DO:
    MESSAGE "!!! ERROR: could not find _file for '" + tableName + "' !!!".
END.
""" % {"table": table, "triggerdir": triggerdir}


def trigger_rw_query(table, jsondir):
    return """
TRIGGER PROCEDURE FOR WRITE OF %(table)s.

DEFINE TEMP-TABLE tt NO-UNDO LIKE %(table)s
    FIELD rec_id AS RECID
    FIELD epoch_time AS INT64.

DEFINE VARIABLE epoch AS DATETIME NO-UNDO.
DEFINE VARIABLE unixTime AS INT64 NO-UNDO.
DEFINE VARIABLE htt AS HANDLE NO-UNDO.
DEFINE VARIABLE cFileName AS CHARACTER NO-UNDO FORMAT "x(60)".

epoch = DATETIME(1,1,1970,0,0,0,0).
unixTime = interval(NOW, epoch, "milliseconds").
cFileName = "%(jsondir)s/t__%(table)s__e__" + STRING(unixTime) + "__write.json".

htt = TEMP-TABLE tt:HANDLE.

CREATE tt.
BUFFER-COPY %(table)s TO tt.
tt.rec_id = RECID(%(table)s).
tt.epoch_time = unixTime.

htt:WRITE-JSON("FILE", cFileName + "_partial", TRUE).
OS-RENAME VALUE(cFileName + "_partial") VALUE(cFileName).
""" % {"table": table, "jsondir": jsondir}


def trigger_rd_query(table, jsondir):
    return """
TRIGGER PROCEDURE FOR DELETE OF %(table)s.

DEFINE TEMP-TABLE tt NO-UNDO
    FIELD rec_id AS RECID
    FIELD epoch_time AS INT64.

DEFINE VARIABLE epoch AS DATETIME NO-UNDO.
DEFINE VARIABLE unixTime AS INT64 NO-UNDO.
DEFINE VARIABLE htt AS HANDLE NO-UNDO.
DEFINE VARIABLE cFileName AS CHARACTER NO-UNDO FORMAT "x(60)".

epoch = DATETIME(1,1,1970,0,0,0,0).
unixTime = interval(NOW, epoch, "milliseconds").
cFileName = "%(jsondir)s/t__%(table)s__e__" + STRING(unixTime) + "__delete.json".

htt = TEMP-TABLE tt:HANDLE.

CREATE tt.
ASSIGN tt.rec_id = RECID(%(table)s).
ASSIGN tt.epoch_time = unixTime.

htt:WRITE-JSON("FILE", cFileName + "_partial", TRUE).
OS-RENAME VALUE(cFileName + "_partial") VALUE(cFileName).
""" % {"table": table, "jsondir": jsondir}


def read_ddl(filename):
    try:
        f = open(filename, 'r', encoding="latin_1")
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
    parser = argparse.ArgumentParser(description="Output ABL files to create replication triggers per table.")
    parser_required = parser.add_argument_group("required arguments")
    parser_required.add_argument("--input", type=str, required=True, help="input file containing RDBMS-customized intermediate DDL")
    parser_required.add_argument("--outputdir", type=str, required=True, help="output directory for ABL files - YOU MUST USE a fully qualified path (GOOD: /home/user1/dir1, BAD: ~/home/user1/dir1)")
    parser.add_argument("--jsondir", type=str, default="/tmp", help="directory to output JSON table changes (default: /tmp)")
    parsed_args = vars(parser.parse_args())

    ddl = read_ddl(parsed_args['input'])

    try:
        print("Parsing %s" % parsed_args['input'])
        ddl = json.loads(read_ddl(parsed_args['input']))
    except ValueError as e:
        print("Failed to load %s, error: %s" % (parsed_args['input'], e))
        sys.exit(1)

    for table in ddl['tables']:
        filename = "%s/abl_add_trigger_%s.p" % (parsed_args['outputdir'], table['name'][0])
        write_abl(filename, add_trigger_query(table['name'][0], parsed_args['outputdir']))

        filename = "%s/abl_trigger_%s_rw.t" % (parsed_args['outputdir'], table['name'][0])
        write_abl(filename, trigger_rw_query(table['name'][0], parsed_args['jsondir']))

        filename = "%s/abl_trigger_%s_rd.t" % (parsed_args['outputdir'], table['name'][0])
        write_abl(filename, trigger_rd_query(table['name'][0], parsed_args['jsondir']))

if __name__ == "__main__":
    import sys 
    if sys.version_info[0] < 3:
        print("Python 3 or greater is required.")
        sys.exit(1)

    main()
