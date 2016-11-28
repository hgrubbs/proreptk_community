#!/bin/bash

if [ $# -gt 0 ] && [ $1 = "-h" ]; then
    echo 'This program normally prints summaries of json dumps, partials, locks, and dumping processes.'
    exit 1
fi

JSONS=$(find /mnt/jsondump/json/ -name "*.json" | sed 's/\/.*\///' | sort -g)
JSONS_COUNT=$(find /mnt/jsondump/json/ -name "*.json" | wc -l)

PARTIALS=$(find /mnt/jsondump/json/ -name "*.json_partial" | sed 's/\/.*\///' | sort -g)
PARTIALS_COUNT=$(find /mnt/jsondump/json/ -name "*.json_partial" | wc -l)

JSON_LOCKS=$(find /mnt/jsondump/json/ -name "*.lock" | sed 's/\/.*\///' | sort -g)
JSON_LOCKS_COUNT=$(find /mnt/jsondump/json/ -name "*.lock" | wc -l)

DUMPS=$(ps ax | grep -e "run_4gl_silent.*[a]bl_dump" | sed 's/.*4gl\///')
DUMPS_COUNT=$(ps ax | grep -e "run_4gl_silent.*[a]bl_dump" | wc -l)

PE_MASTER_COUNT=$(ps ax | grep -e '[p]ersistence_engine' | sed 's/.*proreptk//' | sort -g | uniq | wc -l)
PE_COUNT=$(ps ax | grep -e "[p]ersistence_engine" | wc -l)

LS_LOCKS=$(ls -1 /tmp/replication_lock_table__*.lock | sed 's/.*replication_lock_table__//' | sed 's/\.lock//')

LS_LOCKS_COUNT=$(ls -1 /tmp/replication_lock_table__*.lock | sed 's/.*replication_lock_table__//' | sed 's/\.lock//' | wc -l)

echo ""
echo ">>>> JSON dumps running: ${DUMPS_COUNT}"
if [ ${DUMPS_COUNT} -gt 0 ]; then
    echo "${DUMPS}"
fi

echo ""
echo ">>>> JSON partial dumps: ${PARTIALS_COUNT}"
if [ ${PARTIALS_COUNT} -gt 0 ]; then
    echo "${PARTIALS}"
fi

echo ""
echo ">>>> JSON complete dumps: ${JSONS_COUNT}"
if [ ${JSONS_COUNT} -gt 0 ]; then
    echo "${JSONS}"
fi

echo ""
echo ">>>> persistence_engine locks: ${JSON_LOCKS_COUNT}"
if [ ${JSON_LOCKS_COUNT} -gt 0 ]; then
    echo "${JSON_LOCKS}"
fi

echo ""
echo ">>>> load_and_swap locks: ${LS_LOCKS_COUNT}"
if [ ${LS_LOCKS_COUNT} -gt 0 ]; then
    echo "${LS_LOCKS}"
fi


echo ""
echo ">>>> SUMMARY"
echo ">>>> JSON dumps running          : ${DUMPS_COUNT}"
echo ">>>> JSON partial dumps          : ${PARTIALS_COUNT}"
echo ">>>> JSON complete dumps         : ${JSONS_COUNT}"
echo ">>>> persistence_engine locks    : ${JSON_LOCKS_COUNT}"
echo ">>>> persistence_engine masters  : ${PE_MASTER_COUNT}"
echo ">>>> persistence_engine processes: ${PE_COUNT}"
echo ">>>> tables in progress(ls locks): ${LS_LOCKS_COUNT}"
