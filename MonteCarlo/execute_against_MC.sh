#!/bin/bash

#=====================================
# Part of scripts for grid job submittions for MICE MC Simulation chain using input G4BeamLine json files.
# Script for executing of MAUS simulation script: execute_MC.py on the grid
# ===== maletic@ipb.ac.rs =============
# =====================================

#=================================

MCSERIAL=99

G4BLINPUT="https://micewww.pp.rl.ac.uk/attachments/download/8755/3_140_M3v2.txt"

MAUS_VERSION=MAUS-v3.0.1

#export USED_SE=se01.esc.qmul.ac.uk

#USED_SE=svr018.gla.scotgrid.ac.uk

#USED_SE=dc2-grid-64.brunel.ac.uk

USED_SE=gfe02.grid.hep.ph.ic.ac.uk

export LFC_HOST=lfc.gridpp.rl.ac.uk

#=================================
PROD_DIR=MCserial_${MCSERIAL} # LFC production directory

VO_MICE_SW_DIR=/cvmfs/mice.egi.eu
MAUS_DIR=$VO_MICE_SW_DIR/sl6/$MAUS_VERSION
MAUS_EXT=$MAUS_DIR/external
MAUS_BIN=$MAUS_DIR/bin

cp $MAUS_DIR/configure .
./configure -r $MAUS_DIR

source ./env.sh

export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$MAUS_EXT

echo $LD_LIBRARY_PATH

chmod a+x execute_MC.py

#time ./execute_MC.py --test --mcserialnumber ${MCSERIAL} --input-file ${G4BLINPUT} --run-number $1

time ./execute_MC.py --no-test --mcserialnumber ${MCSERIAL} --input-file ${G4BLINPUT} --run-number $1

echo "Done with simulation"


SE_PATH=`ldapsearch -x -LLL -H ldap://lcg-bdii.cern.ch:2170/ -b mds-vo-name=local,o=grid "(&(GlueChunkKey=GlueSEUniqueID=$USED_SE)(GlueVOInfoAccessControlBaseRule=VO:mice))" GlueVOInfoPath | sed -n '/GlueVOInfoPath:/p' | grep -v UNDEFINEDPATH | head -1 | awk '{print $2}'`

#CLOSE_SE="srm://$VO_MICE_DEFAULT_SE$SE_PATH"

USE_THIS_SE="srm://$USED_SE/$SE_PATH"
#echo " Close SE $CLOSE_SE "


PADDED_filenum=`printf "%05d" $1` # file is zero-padded to 5 digits

PADDED_mcserial=`printf "%06d" $MCSERIAL`

let "unpadded_century=$MCSERIAL/100*100"
century=`printf "%06d" $unpadded_century`  # century is zero-padded to 6 digits !
echo " Century (of model number) is $century"
# ten thousand path element
let "unpadded_10k=$unpadded_century/10000*10000"
PADDED_10k=`printf "%06d" $unpadded_10k`  # 10k is zero-padded to 6 digits


# file path is:

#echo "copy to" ${CLOSE_SE}/Simulation/MCproduction/$PADDED_10k/$century/${PADDED_mcserial}/${PADDED_filenum}_mc.tar
echo "copy to" ${USE_THIS_SE}/Simulation/MCproduction/$PADDED_10k/$century/${PADDED_mcserial}/${PADDED_filenum}_mc.tar 

lcg-cr --checksum -l /grid/mice/Simulation/MCproduction/$PADDED_10k/$century/${PADDED_mcserial}/${PADDED_filenum}_mc.tar -d ${USE_THIS_SE}/Simulation/MCproduction/$PADDED_10k/$century/${PADDED_mcserial}/${PADDED_filenum}_mc.tar ${PADDED_filenum}_mc.tar


#lcg-cr --checksum -l /grid/mice/users/dmaletic/MCproduction/${PROD_DIR}/${PADDED_filenum}_mc.tar -d ${CLOSE_SE}/dmaletic/MCproduction/${PROD_DIR}/${PADDED_filenum}_mc.tar ${PADDED_filenum}_mc.tar

#  lcg-cr -l /grid/mice/users/dmaletic/test1/testprodtest/01563_mc.tar -d srm://gfe02.grid.hep.ph.ic.ac.uk/pnfs/hep.ph.ic.ac.uk/data/mice/test/testprodtest/01563_mc.tar 01563_mc.tar
#  lcg-cr -l /grid/mice/users/dmaletic/test1/testprodtest2/01563_mc.tar -d srm://gfe02.grid.hep.ph.ic.ac.uk/pnfs/hep.ph.ic.ac.uk/data/mice/dmaletic/MCproduction0/01563_mc.tar 01563_mc.tar

echo "execute_against_MC.sh done..."
