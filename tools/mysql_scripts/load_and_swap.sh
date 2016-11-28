#!/bin/bash
#hunter.grubbs@gmail.com

if [ $# -ne 1 ]; then
    echo "This program dumps a progress table and loads result set into <table_name>_cur"
    echo "Usage: $0 <table_name>"
    exit 1
fi

TABLE=$1
TABLECUR=${TABLE}_cur
LOCKFILE="/tmp/replication_lock_table__${TABLE}.lock"

###########################
##progress related settings
DLC=#TODO
PATH=#TODO
TERM=xterm
export DLC
export PATH
export TERM


##########################
##PROREPTK related settings
DBHOST="" # TODO
DB="" # TODO
DBUSER="" # TODO
MYSQL="mysql -h ${DBHOST} -u ${DBUSER} ${DB} "
ABLPATH="/mnt/jsondump/4gl"
JSONPATH="/mnt/jsondump/json"
ABLRUNNER=".../proreptk/tools/progress_scripts/run_4gl_silent" # TODO
DDL="" # TODO
PERUNNER="/usr/local/bin/python3.4 -u /root/src/proreptk/tools/persistence_engine/persistence_engine.py --input ${DDL} --concurrency files --processes 6 --onepass --globpattern t__${TABLE}__e*.json --jsondir ${JSONPATH} --delay 1 --fastinsert" # TODO

##################
## SQL definitions
SQL_MAKE_NEW_TABLE="CREATE TABLE ${TABLE} LIKE ${TABLECUR};" # create empty table from in-use table's data model
SQL_RENAME_TO_CUR="ALTER TABLE ${TABLE} RENAME TO ${TABLECUR};" # rename table to table_cur
SQL_DROP_CUR="DROP TABLE ${TABLECUR};" # drop table_cur

function update_now {
    NOW=$(date +"%Y.%m.%d-%H:%M:%S")
}

function check_lock {
    if [ -e $LOCKFILE ]; then
        update_now
        echo "${NOW}: lock exists: ${LOCKFILE}"
        exit 1
    fi
}

function place_lock {
    update_now
    echo "${NOW}: placing lock: ${LOCKFILE}"
    date > $LOCKFILE
}

function dump_jsons {
    update_now
    echo "${NOW}: dumping JSONs for ${TABLE}"
    ${ABLRUNNER} ${ABLPATH}/abl_dump_${TABLE}.p
}

function persist_rows {
    update_now
    echo "${NOW}: persisting dumped rows"
    ${PERUNNER}
}

function drop_cur_table {
    update_now
    echo "${NOW}: dropping ${TABLECUR}"
    echo ${SQL_DROP_CUR}
    echo ${SQL_DROP_CUR} | ${MYSQL}
}

function rename_table_to_cur {
    update_now
    echo "${NOW}: renaming ${TABLE} to ${TABLECUR}"
    echo ${SQL_RENAME_TO_CUR}
    echo ${SQL_RENAME_TO_CUR} | ${MYSQL}
}

function make_new_table {
    update_now
    echo "${NOW}: re-creating ${TABLE}"
    echo "${SQL_MAKE_NEW_TABLE}"
    echo "${SQL_MAKE_NEW_TABLE}" | ${MYSQL}
}

function remove_lock {
    update_now
    echo "${NOW}: removing lock: ${LOCKFILE}, STARTED at ${STARTED} and FINISHED at ${NOW}"
    rm -f $LOCKFILE
}

check_lock
place_lock

#####################
### START LOCKED MODE
STARTED=$(date +"%Y.%m.%d-%H:%M:%S")
dump_jsons
persist_rows
drop_cur_table
rename_table_to_cur
make_new_table

remove_lock
###################
### END LOCKED MODE
