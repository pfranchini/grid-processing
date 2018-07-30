#!/bin/bash


#=================================

MAUS_VERSION="MAUS-v3.1.2"

export LFC_HOST=lfc.gridpp.rl.ac.uk

#=================================

VO_MICE_SW_DIR=/cvmfs/mice.egi.eu
MAUS_DIR=$VO_MICE_SW_DIR/sl6/$MAUS_VERSION
MAUS_EXT=$MAUS_DIR/external
MAUS_BIN=$MAUS_DIR/bin

ifconfig

uname -a

df -h

cp $MAUS_DIR/configure .
./configure -r $MAUS_DIR

source ./env.sh

export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$MAUS_EXT

echo $LD_LIBRARY_PATH


runs=""
run_list=""
batch_iteration=2

# name of recon executable
EXE="execute_data_recon.py"
# location of this executable in MAUS cvmfs
echo ${MAUS_ROOT_DIR}

OFFLINE_EXE="${MAUS_ROOT_DIR}/bin/utilities/$EXE"

# base url for raw input
# we take the input from RAL PPD dcache
DATA_URL="root://dcap.pp.rl.ac.uk:///pnfs/pp.rl.ac.uk/data/mice/MICE/Step4/"

# maus version
#MAUS_VERSION="MAUS-v3.1.2"

STEP="Step4"

DAQ_DIR="./raw"

PARENT_DIR=$PWD

if [ -z $MAUS_ROOT_DIR ]; then
    echo "MAUS_ROOT_DIR is not set"
    echo "Please source env.sh from your install to set the MAUS environment"
    exit 1
fi

# cd $MAUS_ROOT_DIR

while getopts ":r:b:c:f:h:" opt; do
  case $opt in
    r)
      runs=${OPTARG}
      echo $runs
      ;;
    f)
      run_list=${OPTARG}
      #echo $run_list
      ;;
    b)
      batch_iteration=${OPTARG}
      #echo ${batch_iteration}
      ;;
    c)
      config_file=${OPTARG}
      #echo $config_file
      ;;
    h)
      echo "Usage: $0 -r <run-number> -b batch-iteration-number -c configuration-file -f runlist-file"
      echo "       At least one of -r or -f is necessary"
      echo "       If not specified, the default configuration file is config.py"
      echo "       If not specified, the default batch-iteration-number is 2"
      ;;
    \?)
      echo "FATAL: Invalid option: -$OPTARG" >&2
      exit 1
      ;;
  esac
done
shift $((OPTIND-1))

# must supply either a single run with -r argument
# or a run list file with a -f argument
# e.g. -r 9970 OR -f runlist.txt

# store the run(s) in the $run_list variable

if [ -z $runs ] && [ -z $run_list ]; then
    echo "A run number must be specified with -r. Or a file with a list of runs must be specified with -f"
    echo "Run $0 -h to get usage"
    exit 1
fi

if [ "$run_list" != "" ] && [ "$runs" != "" ]; then
    echo "Cannot specify both -r and -f"
    echo "Specify a single run with the -r option, or list of runs contained a in a file with the -f option"
    exit 1
fi

if [ "$run_list" != "" ]; then
    if [ ! -e $run_list ]; then
        echo "$run_list does not exist"
        echo "Provide a valid filename which contains a list of runs to process."
        echo "Or specify a single run with the -r option"
        exit 1
    else
        runs=( $(<$run_list) )
    fi
fi

# now get the number of runs in the run_list variable
n_runs=${#runs[@]}
echo "Will process $n_runs runs"

# loop over the list of runs
n=0
while [ $n -lt $n_runs ]; do
    cur_run=$((10#${runs[$n]}))
    printf -v runStr "%05d" $cur_run

    # create run directory
    echo "Setting up $runStr ..."
    if [ -d $runStr ]; then
        echo "Hmm...there is a directory named $runStr. Will not overwrrite. Exiting"
        exit 1
    else
        mkdir $runStr
    fi

    # copy the executable script and the config file to the run directory
    echo "config is $config_file"
    cp $OFFLINE_EXE $runStr
    if [ x"$config_file" != "x" ] && [ -e $config_file ]; then
        cp $config_file $runStr
    fi

    # change dir to the run directory
    cd $runStr
    curDir=$PWD

    # setup variables for input and output 
    infile="${runStr}.tar"
    tarFile="${runStr}_offline.tar"
    mdFile="${tarFile}.md5"
    SEMFILE="${runStr}_offline.processed"

    # get the century subdir this run belongs to
    let sd=$cur_run/100*100
    printf -v subdir "%05d" $sd

    # setup the raw input tar filename
    raw_tar="${runStr}.tar"


echo lcg-ls srm://heplnx204.pp.rl.ac.uk/pnfs/pp.rl.ac.uk/data/mice/RECO/${MAUS_VERSION}/1/Step4/${subdir}/${runStr}_offline.tar

lcg-ls srm://heplnx204.pp.rl.ac.uk/pnfs/pp.rl.ac.uk/data/mice/RECO/${MAUS_VERSION}/1/Step4/${subdir}/${runStr}_offline.tar > file_exists.txt

ls

fiex=`cat file_exists.txt | wc -l`

echo $fiex

if [ $fiex == 0 ]; then
        echo "No output file on dcache."

    # download raw data tarball
    echo "Getting $raw_tar from ${DATA_URL}/${subdir}"

    # create a raw directory --inside the run directory--
    mkdir raw
    xrdcp-old ${DATA_URL}/${subdir}/${raw_tar} - | tar -x -C raw >& /dev/null
    #xrdcp-old ${DATA_URL}/${subdir}/${raw_tar} . >& /dev/null

    if [ $? -ne 0 ]; then
        echo "Error getting raw input for $raw_tar"
        exit 1
    fi

   ls -al

   ls -al raw

    # now execute the reco
    echo "Processing ....."

    ####################### for debugging, etc
    ####python ./$EXE --input-file $raw_tar --batch-iteration 1
    ####python ./$EXE --run-number $cur_run --batch-iteration 1 --no-globals
    ########################

    \time -v python $OFFLINE_EXE --run-number $cur_run --batch-iteration 2
    status=$?

    echo $status > reco.status

    if [ $status != 0 ]; then
        echo "!!! ERROR: Reconstruction failed with status = $status"
    fi

#### copy the file and semafor
# 4) once the python process has returned *you will have to*
# — 4a) check that the output does not exist on dcache/castor
# — 4b) verify the output tarball checksum
# — 4b) copy the output tarball *and* semaphore to dcache
##

#-rw-r--r-- 1 pltmce20 pltmice       0 Feb 26 15:09 10526_offline.processed
#-rw-r--r-- 1 pltmce20 pltmice 5488820 Feb 26 15:09 10526_offline.tar


#lcg-cp 10531_offline.tar srm://heplnx204.pp.rl.ac.uk/pnfs/pp.rl.ac.uk/data/mice/RECO/MAUS-v3.1.2/1/Step4/10500/10531_offline.tar 

#lcg-cp 10531_offline.processed srm://heplnx204.pp.rl.ac.uk/pnfs/pp.rl.ac.uk/data/mice/RECO/MAUS-v3.1.2/1/Step4/10500/10531_offline.processed


    #srm://srm-mice.gridpp.rl.ac.uk/castor/ads.rl.ac.uk/prod/mice/reco/RECO/MAUS-v3.1.2
    # lcg-ls -l srm://srm-mice.gridpp.rl.ac.uk/castor/ads.rl.ac.uk/prod/mice/reco/RECO/MAUS-v3.1.2/1/Step4/10500/10573_offline.tar


##SE_PATH=`ldapsearch -x -LLL -H ldap://lcg-bdii.cern.ch:2170/ -b mds-vo-name=local,o=grid "(&(GlueChunkKey=GlueSEUniqueID=UKI-SOUTHGRID-RALPP)(GlueVOInfoAccessControlBaseRule=VO:mice))" GlueVOInfoPath | sed -n '/GlueVOInfoPath:/p' | grep -v UNDEFINEDPATH | head -1 | awk '{print $2}'`

###CLOSE_SE="srm://$VO_MICE_DEFAULT_SE$SE_PATH"

##USE_THIS_SE="srm://$USED_SE/$SE_PATH"
###echo " Close SE $CLOSE_SE "


#PADDED_filenum=`printf "%05d" $1` # file is zero-padded to 5 digits

##PADDED_mcserial=`printf "%06d" $MCSERIAL`

#let "unpadded_century=${runStr}/100*100"
#century=`printf "%05d" $unpadded_century`  # century is zero-padded to 6 digits !
##echo " Century (of model number) is $century"
### ten thousand path element
##let "unpadded_10k=$unpadded_century/10000*10000"
##PADDED_10k=`printf "%06d" $unpadded_10k`  # 10k is zero-padded to 6 digits


### file path is:

###echo "copy to" ${CLOSE_SE}/Simulation/MCproduction/$PADDED_10k/$century/${PADDED_mcserial}/${PADDED_filenum}_mc.tar
##echo "copy to" ${USE_THIS_SE}/Simulation/MCproduction/$PADDED_10k/$century/${PADDED_mcserial}/${PADDED_filenum}_mc.tar 

lcg-ls srm://heplnx204.pp.rl.ac.uk/pnfs/pp.rl.ac.uk/data/mice/RECO/${MAUS_VERSION}/1/Step4/${subdir}/${runStr}_offline.tar

lcg-cp --checksum ${runStr}_offline.tar srm://heplnx204.pp.rl.ac.uk/pnfs/pp.rl.ac.uk/data/mice/RECO/${MAUS_VERSION}/1/Step4/${subdir}/${runStr}_offline.tar

lcg-cp ${runStr}_offline.processed srm://heplnx204.pp.rl.ac.uk/pnfs/pp.rl.ac.uk/data/mice/RECO/${MAUS_VERSION}/1/Step4/${subdir}/${runStr}_offline.processed

##lcg-cr --checksum -l /grid/mice/Simulation/MCproduction/$PADDED_10k/$century/${PADDED_mcserial}/${PADDED_filenum}_mc.tar -d ${USE_THIS_SE}/Simulation/MCproduction/$PADDED_10k/$century/${PADDED_mcserial}/${PADDED_filenum}_mc.tar ${PADDED_filenum}_mc.tar

#########


    # go back to the parent directory and up the counter to process the next run
    ls -al

else
    echo "Error: Already existing output file on dcache!"
fi

    cd $PARENT_DIR
	ls -al
    let n=n+1
done
