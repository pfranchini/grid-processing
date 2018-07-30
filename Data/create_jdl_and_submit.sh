#!/bin/bash

#=====================================
# Part of scripts for grid job submittions for MICE MC Simulation chain using input G4BeamLine json files.
# Script for submitions of grid jobs.
# ===== maletic@ipb.ac.rs =============
# =====================================


# ======================

MCSERIAL=50002  # MCSerialNumber a number in preprodcdb

sqlitedb="../../MICEprodDB/MICEprodDB_dirac_sqlite.db"  # path to local DB

# =======================

prodID=${MCSERIAL}  # local DB entry for new production (for each MCSerialNumber)

if [ $# -eq 0 ];
  then
    echo "No arguments supplied"
    echo "To submit chunks:"
    echo "./create_jdl_and_submit.sh 10501"
    exit
fi

touch ../submitting

chunks=$@

date=`date +"%Y-%m-%d"`
time=`date +"%H:%M:%S"`


for i in $chunks

do

    filenum=`printf "%05d" $i`
    LFC_NAME=${LFC_PREFIX}"/"${PROD_DIR}"/"${filenum}"_mc.tar"

    mkdir job_$filenum
    cd job_$filenum
    cat ../testMICE | sed s/"AAA"/$i/g  > testMICE_$filenum.jdl
    cp ../execute_data-v2.sh . 

    echo submiting testMICE_$filenum.jdl

    dirac-wms-job-submit testMICE_$filenum.jdl | awk '{print $0,"testMICE_'$filenum'.jdl job_'$filenum' "}' > submition_log.txt


    jobid=`cat submition_log.txt | awk '{print $3}'`
    jdlname=`cat submition_log.txt | awk '{print $4}'`
    jobdir=`cat submition_log.txt | awk '{print $5}'`

    sqlite3 ${sqlitedb} "insert into job_info2 (datetime, prodID, dir_name, jdl_name, ID, lfc_name, retry_count, status) values ('"$date" "$time"','"${prodID}"','"${jobdir}"', '"${jdlname}"', '"${jobid}"', '"${LFC_NAME}"','1', 'Submitted') "

    cat submition_log.txt >> ../submition_logs.txt
    cd ..

echo sleeping...
    sleep 2s
echo end sleeping

done

rm -f ../submitting
