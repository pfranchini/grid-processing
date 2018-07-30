#!/bin/bash

#=====================================
# Part of scripts for grid job submittions for MICE MC Simulation chain using input G4BeamLine json files.
# Script for submitions of grid jobs.
# ===== maletic@ipb.ac.rs =============
# =====================================


# ======================

MCSERIAL=99  # MCSerialNumber a number in preprodcdb

PROD_DIR=MCproduction/MCserial_${MCSERIAL}  # part of LFC names

#/grid/mice/users/dmaletic/test1/testprodtest/01563_mc.tar

LFC_PREFIX="/grid/mice/users/dmaletic"  # LFC prefix

#WMS_ENDPOINT=https://svr022.gla.scotgrid.ac.uk:7443/glite_wms_wmproxy_server # server where proxy is delegated

#WMS_ENDPOINT=https://wms01.grid.hep.ph.ic.ac.uk:7443/glite_wms_wmproxy_server

#WMS_ENDPOINT=https://lcgwms04.gridpp.rl.ac.uk:7443/glite_wms_wmproxy_server

#WMS_ENDPOINT=https://lcgwms05.gridpp.rl.ac.uk:7443/glite_wms_wmproxy_server                                                                  

#delegatename=dmaletic 

#delegatename=dm

#CE_USED=svr009.gla.scotgrid.ac.uk:2811/nordugrid-Condor-condor_q2d # CE for running grid jobs

#CE_USED=ceprod06.grid.hep.ph.ic.ac.uk:8443/cream-sge-grid.q

#CE_USED=ce05.esc.qmul.ac.uk:8443/cream-slurm-sl6_lcg_1G_long

#CE_USED=lcgce1.shef.ac.uk:8443/cream-pbs-mice

#CE_USED=ce3.ppgrid1.rhul.ac.uk:8443/cream-pbs-mice

#CE_USED=hepgrid2.ph.liv.ac.uk:2811/nordugrid-Condor-grid

sqlitedb="../../MICEprodDB/MICEprodDB_dirac_sqlite.db"  # path to local DB

# =======================

prodID=${MCSERIAL}  # local DB entry for new production (for each MCSerialNumber)

if [ $# -eq 0 ];
  then
    echo "No arguments supplied"
    echo "To submit all chunks:"
    echo "./create_jdl_and_submit.sh file http://path_to_file.txt"
    echo "To resubmit chunks 1 20 300 1400:"
    echo "./create_jdl_and_submit.sh 1 20 300 1400"
    exit
fi

touch ../submitting

if [ $1 == "file" ];
   then
     g4bllist=$2

     wget $g4bllist

     infile=`echo  $g4bllist | sed s/"\/"/"\n"/g | tail -1`

     if [ -f $infile ];
       then
         echo "File $infile exists."
       else
         echo "File $infile does not exist."
         exit
     fi

    number=`cat $infile | wc -l | awk '{print $1-1}'`
     echo file: $infile number of chunks: $(($number+1))
     chunks=$(eval echo "{11..$number}")
#      chunks=$(eval echo "{11..250}")
#     chunks=$(eval echo "{0..10}")
   else
 chunks=$@
fi

date=`date +"%Y-%m-%d"`
time=`date +"%H:%M:%S"`


for i in $chunks

do

    filenum=`printf "%05d" $i`
    LFC_NAME=${LFC_PREFIX}"/"${PROD_DIR}"/"${filenum}"_mc.tar"

    mkdir job_$filenum
    cd job_$filenum
    cat ../testMICE | sed s/"AAA"/$i/g  > testMICE_$filenum.jdl
    cp ../execute_against_MC.sh ../execute_MC.py  .
    echo submiting testMICE_$filenum.jdl
#    glite-wms-job-submit -d ${delegatename} --endpoint ${WMS_ENDPOINT} -r ${CE_USED} testMICE_$filenum.jdl | awk '{print $0,"testMICE_'$filenum'.jdl job_'$filenum' "}' > submition_log.txt
    dirac-wms-job-submit testMICE_$filenum.jdl | awk '{print $0,"testMICE_'$filenum'.jdl job_'$filenum' "}' > submition_log.txt

# JobID = 4511856 testMICE_00000.jdl job_00000 

    jobid=`cat submition_log.txt | awk '{print $3}'`
    jdlname=`cat submition_log.txt | awk '{print $4}'`
    jobdir=`cat submition_log.txt | awk '{print $5}'`

    sqlite3 ${sqlitedb} "insert into job_info2 (datetime, prodID, dir_name, jdl_name, ID, lfc_name, retry_count, status) values ('"$date" "$time"','"${prodID}"','"${jobdir}"', '"${jdlname}"', '"${jobid}"', '"${LFC_NAME}"','1', 'Submitted') "

    cat submition_log.txt >> ../submition_logs.txt
    cd ..

echo sleeping...
    sleep 1s
echo end sleeping

done

rm -f ../submitting
