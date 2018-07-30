#!/usr/bin/env python

#  This file is part of MAUS: http://micewww.pp.rl.ac.uk:8080/projects/maus
#
#  MAUS is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  MAUS is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with MAUS.  If not, see <http://www.gnu.org/licenses/>.

"""
excecute offline reconstruction
"""

DESCRIPTION = """
This is the script used for running reconstruction jobs 
once the data is pulled to the offline data store.

It runs a reconstruction job using analyze_data_offline_globals.py by default
The intput is a run number or an input tarball name.

The user needs to source env.sh in the usual way before running.

Creates a tarball called ######_offline.tar where ##### is the run number right
aligned and padded by 0s.

Also creates a semaphore file so that the reconstruction-mover-to-castor 
can pick up outputs as and when they are created

Return codes are:
    0 - Everything ran okay.
    1 - There was a transient error. Try again later. Transient errors are
        errors like the configuration database could not be contacted, or there
        was no suitable run geometry uploaded yet.
    2 - there was some internal problem with the reconstruction. It needs to be
        checked by the software expert.
    3 - there was some problem with this script. It needs to be checked by the
        software expert.
"""

#pylint: disable = W0622, C0103
__doc__ = DESCRIPTION+"""

Three classes are defined
  - RunManager: handles overall run execution;
  - FileManager: handles logging and output tarball;
  - RunSettings: handles run setup #pylint: disable = W0622
"""

import argparse
import tarfile
import sys
import glob
import os
import subprocess
import shutil
import cdb
import time
import hashlib
from fnmatch import fnmatch

def arg_parser():
    """
    Parse command line arguments.

    Use -h switch at the command line for information on command line args used.
    """
    parser = argparse.ArgumentParser(description=DESCRIPTION, \
                           formatter_class=argparse.RawDescriptionHelpFormatter)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--input-file', dest='input_file', \
                        default=None, \
                        help='Read in raw data tarball file with this name')
    group.add_argument('--run-number', dest='run_number', \
                        default=None, \
                        help='Run number to process')
    parser.add_argument('--test', dest='test_mode', \
                        help='Run the batch job using test cdb output',
                        action='store_true', default=False)
    parser.add_argument('--no-test', dest='test_mode', \
                        help="Don't run the batch job using test cdb output",
                        action='store_false', default=False)
    parser.add_argument('--batch-iteration', dest='batch_iteration', type=int, \
                        help='Batch iteration number for configuration DB', \
                        default=0)
    parser.add_argument('--config-file', dest='config_file', \
                        help='Configuration file with additional cards', \
                        default=None)
    parser.add_argument('--no-globals', dest='basic_reco', \
                        action='store_true', default=False, \
                        help='Basic reconstruction without globals')
    return parser

###############################################################################
class DownloadError(Exception):
    """
    DownloadError indicates a failure to download some necessary data for the
    run - these are considered transient errors (i.e. the calibration wasn't
    uploaded, the database wasn't available, etc). Try again later.
    """
    def __init__(self, error_message):
        """Initialise the exception with some error message"""
        super(DownloadError, self).__init__(error_message)
        self.error_message = error_message

    def __str__(self):
        """Return a string containing the the error message"""
        return repr(self.error_message)

###############################################################################
class MausError(Exception):
    """
    MausError indicates the batch job failed due to some internal MAUS
    error. This would need human intervention to understand what happened. It
    may be the result of corrupt data, in which case we want to know about it.
    """
    def __init__(self, error_message):
        """Initialise the exception with some error message"""
        super(MausError, self).__init__(error_message)
        self.error_message = error_message

    def __str__(self):
        """Return a string containing the the error message"""
        return repr(self.error_message)

###############################################################################
class RunManager:
    """
    Run manager manages the overall run - calls reconstruction and monte carlo

    Main function is run()
    """
    def __init__(self, args_in_):
        """
        Setup log files and run setup

        @param args_in_ arg_parser object (called from arg_parser function)
        """
        print 'Setting up Run manager'
        self.run_setup = RunSettings(args_in_)

        self.cleanup()

        self.logs = FileManager()

        dl_logname = self.run_setup.run_number_as_string + "_download.log"
        reco_logname = self.run_setup.run_number_as_string + "_reco.log"
        batch_logname = self.run_setup.run_number_as_string + "_batch.log"

        self.logs.open_log(dl_logname, reco_logname, batch_logname)
        self.logs.tar_file_name = self.run_setup.tar_file_name
        self.logs.sem_file_name = self.run_setup.sem_file_name

        self.reco_status = -1

    def run(self):
        """
        Does the main execution loop against a run file

        - Checks that the run number can be executed
        - Performs any setup on the working directory
        - downloads geometry files from cdb
        - executes the reconstruction code
        """
        if not self.check_valid():
            print 'Error - run not valid'
            return 1
        if not self.setup():
            print 'Error - could not setup input'
            return 1
        self.download_cards()
        self.download_geometry()
        self.download_scifi_calibration()
        retcode = self.execute_reconstruction()
        return retcode

    def check_valid(self): # pylint: disable = R0201
        """
        Checks that the run number can be executed

        Not implemented yet, but some day would like to check that the run has
        valid calibrations, cabling and geometry

        @returns True if run is valid
        """
        print 'Checking run validity'
        return True

    def check_raw_dir(self, rawdir):
        '''
        check that the directory exists
        check that it contains (some) raw data
        '''
        if os.path.isdir(rawdir):
            for f in os.listdir(rawdir):
                if fnmatch(f, self.run_setup.run_number_as_string+'.0*'):
                    return True
        return False

    def setup(self):
        """
        Set up the current working directory

        Cleans the current working directory; Makes a directory for downloads;
        extracts the raw data tarball 
        """
        print 'Setup files'
        print '   ', self.run_setup.download_target
        print '   ', self.run_setup.input_file_name
        print '   ', os.getcwd()
        os.mkdir(self.run_setup.download_target)
        tar_dir = self.run_setup.raw_dir
        if self.run_setup.input_file_name is not None:
            tar_in = tarfile.open(self.run_setup.input_file_name)
            tar_in.extractall(tar_dir)
            return True
        else:
            return self.check_raw_dir(tar_dir)

    def cleanup_postrun(self): # pylint: disable = R0201
        '''
        Cleans up working directory
        At this point tarball, semaphore, checksum have been created
        Delete everything else
        '''
        print 'Done. Cleaning working directory'
        clean_target = glob.glob('*')
        for item in clean_target:
            if not 'offline' in item:
                if os.path.isdir(item):
                    shutil.rmtree(item)
                else:
                    os.remove(item)

    def cleanup(self):
        """
        Cleans up the current working directory

        Removes the download target directory, and removes all files belonging
        to this run (denoted by <run_number>*) unless they are a tar ball
        """
        print 'Cleaning working directory'
        download_dir = self.run_setup.download_target
        clean_dirs = ['raw', 'calib', download_dir]
        for d in clean_dirs:
            if os.path.isdir(download_dir):
                if d == 'raw' and self.run_setup.input_file_name is None:
                    continue
                shutil.rmtree(d)
        clean_target = glob.glob('*'+str(self.run_setup.run_number)+'*')
        for item in clean_target:
            if item[-3:] != 'tar':
                os.remove(item)

    def download_cards(self):
        """
        Downloads the datacards from the configuration database
        
        If running in test mode, uses legacy Stage4 geometry instead.

        @raises DownloadError on failure
        """
        print 'Getting cards'
        bi_number = self.run_setup.batch_iteration
        for i in range(5):
            try:
                print "    Contacting CDB"
                if self.run_setup.test_mode:
                    bi_service = cdb.BatchIteration(
                                              "http://preprodcdb.mice.rl.ac.uk")
                else:
                    bi_service = cdb.BatchIteration()
                print "    Found, accessing cards"
                reco_cards = bi_service.get_reco_datacards(bi_number)['reco']
                if reco_cards == 'null':
                    raise DownloadError(
                       "No MC cards for batch iteration number "+str(bi_number))
                reco_out = open(self.run_setup.reco_cards, 'w')
                if self.run_setup.config_file is not None:
                    # if self.run_setup.batch_iteration == 1:
                    #   raise ValueError("Error: Batch iteration = 1 but extra cards supplied.")
                    with open(self.run_setup.config_file, 'r') as infile:
                        reco_out.write(infile.read())
                    reco_out.write(reco_cards)
                else:
                    reco_out.write(reco_cards)
                reco_out.close()
                self.logs.tar_queue.append(self.run_setup.reco_cards)
                return
            except cdb.CdbTemporaryError:
                print "CDB lookup failed on attempt", i+1
                time.sleep(1)
            except cdb.CdbPermanentError:
                raise DownloadError("Failed to download cards - CDB not found")
        raise DownloadError("Failed to download cards after 5 attempts")

    def download_scifi_calibration(self):
        """
        Downloads SciFi calibration, bad channels and mapping file
        Files go to the files/
        """
        download = [os.path.join(self.run_setup.maus_root_dir, 'src', 
                                 'common_py', 'calibration',
                                 'get_scifi_calib.py')]

        download += self.run_setup.get_calibration_download_parameters()
        print download

        proc = subprocess.Popen(["python"] + download, \
                                stdout=self.logs.download_log, \
                                stderr=subprocess.STDOUT)
        proc.wait()
        if proc.returncode != 0:
            raise DownloadError("Failed to download SciFi calibration/mapping")
        mapfile = str(self.run_setup.run_number) + "/scifi_mapping.txt"
        bcfile = str(self.run_setup.run_number) + "/scifi_bad_channels.txt"
        calfile = str(self.run_setup.run_number) + "/scifi_calibration.txt"
        with open(self.run_setup.reco_cards, 'a+') as incards:
            incards.write('\nSciFiConfigDir = \"%s\"' % self.run_setup.calib_path)
            incards.write('\nSciFiMappingFileName = \"%s\"' % mapfile)
            incards.write('\nSciFiCalibrationFileName = \"%s\"' % calfile)
            incards.write('\nSciFiBadChannelsFileName = \"%s\"' % bcfile)
        self.logs.tar_queue.append(self.run_setup.calib_path)

    def download_geometry(self):
        """
        Downloads the geometry from the configuration database
        
        Calls the download_geometry.py utility script to download geometries by
        run number

        If running in test mode, uses legacy Stage4 geometry instead.

        @raises DownloadError on failure
        """
        print 'Getting geometry'
        if os.environ['MAUS_UNPACKER_VERSION'] == "StepI":
            test_path_in = os.path.join(self.run_setup.maus_root_dir, 'src',
                      'legacy', 'FILES', 'Models', 'Configurations', 'Test.dat')
            test_path_out = os.path.join(self.run_setup.download_target, \
                                                         'ParentGeometryFile.dat')
            shutil.copy(test_path_in, test_path_out)
        else:
            download = [os.path.join(self.run_setup.maus_root_dir, 'bin', 
                                           'utilities', 'download_geometry.py')]
            download += self.run_setup.get_download_parameters()
            proc = subprocess.Popen(download, stdout=self.logs.download_log, \
                                              stderr=subprocess.STDOUT)
            proc.wait()
            if proc.returncode != 0:
                raise DownloadError("Failed to download geometry successfully")
        self.logs.tar_queue.append(self.run_setup.download_target)

    def execute_reconstruction(self):
        """
        Execute the reconstruction

        Executes the reconstruction; puts the output file into the tar queue
        """
        print 'Running reconstruction'

        reco_name = 'analyze_data_offline_globals.py'
        if self.run_setup.basic_reco:
            reco_name = 'analyze_data_offline.py'
        reconstruction = [os.path.join(self.run_setup.maus_root_dir, 'bin', 
                                                                reco_name)]
        reconstruction += self.run_setup.get_reconstruction_parameters()
        print reconstruction

        proc = subprocess.Popen(reconstruction, stdout=self.logs.rec_log, \
                                                       stderr=subprocess.STDOUT)
        proc.wait()
        if proc.returncode != 0:
            raise MausError("MAUS reconstruction returned "+str(proc.returncode))
        self.reco_status = proc.returncode

        self.logs.tar_queue.append(self.run_setup.recon_file_name)

        return proc.returncode

    def __del__(self):
        """
        If not in test mode, calls cleanup to clean current working directory
        and closes log files.
        """
        print 'Deleting run'
        if not self.run_setup == None:
            self.logs.close_log()
            if not self.run_setup.test_mode:
                if self.reco_status == 0:
                    self.logs.create_archive()
                self.cleanup_postrun()

###############################################################################
class RunSettings: #pylint: disable = R0902
    """
    RunSettings holds settings for the run - run numbers, file names, etc

    Can write this stuff out as a list of command line parameters for each of
    the main execution blocks
    """

    def __init__(self, args_in):
        """
        Initialise the run parameters

        @param argv list of command line arguments (strings)

        Run number is taken from the name of the tarball passed as an argument.
        All other file names etc are then built off that
        """
        print 'Setting up run'
        self.input_file_name = args_in.input_file
        self.run_number = args_in.run_number
        self.test_mode = args_in.test_mode
        self.batch_iteration = args_in.batch_iteration
        self.config_file = args_in.config_file
        self.basic_reco = args_in.basic_reco

        if self.run_number is None and self.input_file_name is not None:
            self.run_number = self.get_run_number_from_file_name \
                                                          (self.input_file_name)
        self.run_number_as_string = str(self.run_number).rjust(5, '0')

        self.tar_file_name = self.run_number_as_string+"_offline.tar"
        self.sem_file_name = self.run_number_as_string+"_offline.processed"
        self.recon_file_name = self.run_number_as_string+"_recon.root"

        self.maus_root_dir = os.environ["MAUS_ROOT_DIR"]
        self.download_target = "geo-"+self.run_number_as_string
        self.reco_cards = self.run_number_as_string+'_reco.cards'
        self.raw_dir = 'raw'
        self.calib_path = "calib"

    def get_run_number_from_file_name(self, file_name): #pylint: disable = R0201
        """
        Get the run number based on the file name

        @param file_name Input file name
        
        Assumes a file of format 000####.* where #### is some integer run
        number.

        @returns run number as an int
        """
        file_name = os.path.basename(file_name)
        file_name.lstrip('0')
        file_name = file_name.split('.')[0]
        run_number = int(file_name)
        return run_number

    def get_reconstruction_parameters(self):
        """
        Get the parameters for the reconstruction exe

        Sets output filename, geometry filename, verbose_level, daq file and 
        path, verbose_level

        @return list of command line arguments for reconstruction
        """
        if os.environ['MAUS_UNPACKER_VERSION'] == "StepI":
            return [
                '-simulation_geometry_filename', \
                       os.path.join(self.download_target, 'ParentGeometryFile.dat'),
                '-reconstruction_geometry_filename', os.path.join \
                               (self.download_target, 'ParentGeometryFile.dat'),
                '-output_root_file_name', str(self.recon_file_name),
                '-daq_data_file', str(self.run_number),
                '-daq_data_path', self.raw_dir,
                '-verbose_level', '1',
                '-will_do_stack_trace', 'False',
                '-configuration_file', self.reco_cards,
                '-DAQ_cabling_by', "date",
                '-TOF_calib_by', "date",
                '-TOF_cabling_by', "date",
            ]
       
        return [
            '-simulation_geometry_filename', \
                   os.path.join(self.download_target, 'ParentGeometryFile.dat'),
            '-reconstruction_geometry_filename', os.path.join \
                               (self.download_target, 'ParentGeometryFile.dat'),
            '-output_root_file_name', str(self.recon_file_name),
            '-daq_data_file', str(self.run_number),
            '-daq_data_path', self.raw_dir,
            '-verbose_level', '1',
            '-will_do_stack_trace', 'False',
            '-configuration_file', self.reco_cards,
        ]


    def get_calibration_download_parameters(self):
        """
        Get the parameters for downloading the SciFi calib, map files

        Specifies run number and download directory

        @return list of command line arguments for download
        """
        return [
            '-SciFiCalibMethod', "Run",
            '-SciFiCalibSrc', str(self.run_number),
            '-SciFiConfigDir', self.calib_path,
        ]

    def get_download_parameters(self):
        """
        Get the parameters for the reconstruction exe

        Sets the run number, download directory, verbose level

        @return list of command line arguments for download
        """
        return [
            '-geometry_download_by', 'run_number',
            '-geometry_download_run_number', str(self.run_number),
            '-geometry_download_directory', str(self.download_target),
            '-verbose_level', '0',
        ]

###############################################################################
class FileManager: # pylint: disable = R0902
    """
    File manager handles log files and the tar archive

    File manager has two components; logs of this batch script and each of the
    downloaded components and a queue (actually a list) of items that should be
    added to the tarball before exiting.
    """

    def __init__(self):
        """
        Initialises to None
        """
        print 'Setting file manager'
        self._is_open = False
        self.tar_ball = None
        self.download_log = None
        self.batch_log = None
        self.rec_log = None
        self.tar_file_name = None
        self.tar_queue = []

    def is_open(self):
        """
        @return true if files are open; else false
        """
        return self._is_open

    def open_log(self, download_name, reco_name, batch_name):
        """
        Open the log files

        Open the log files if they are not open; add them to the queue to be
        tarred on exit. Redirect stdout and stderr to log files.
        """
        print 'Opening logs'
        if self._is_open:
            raise IOError('Logs are already open')
        try:
            self.download_log = open(download_name, 'w')
            self.rec_log = open(reco_name, 'w')
            self.batch_log = open(batch_name, 'w')
            sys.stderr = self.batch_log
            sys.stdout = self.batch_log
        except:
            raise
        self.tar_queue += [download_name, reco_name, batch_name]
        self._is_open = True

    def close_log(self):
        """
        Close the logs; add items in the tar_queue to the tarball

        If the logs aren't open, does nothing
        """
        print 'Closing logs'
        if not self._is_open:
            print 'Logs are not open'
            return
        self.batch_log.close()
        self.rec_log.close()
        self.download_log.close()
        sys.stderr = sys.__stdout__
        sys.stdout = sys.__stderr__
        self._is_open = False
       
    def create_archive(self):
        ''''
        1) Creates output tarball
        Includes logs, geometry, calibration, cards, output root file
        Content names are held in list tar_queue
        2) Creates the semaphore file needed by the reco mover
        3) creates a checksum file
        '''
        print 'Creating output tarball'
        if self.tar_file_name != None:
            if os.path.isfile(self.tar_file_name):
                os.remove(self.tar_file_name)
            try:
                tar_file = tarfile.open(self.tar_file_name, 'w:gz')
                for item in self.tar_queue:
                    if item == 'raw':
                        continue
                    tar_file.add(item)
                tar_file.close()

                sem_file = open(self.sem_file_name, 'w')
                sem_file.close()

                md5hash = hashlib.md5()
                with open(self.tar_file_name, 'rb') as fin:
                    for chunk in iter(lambda: fin.read(4096), b""):
                        md5hash.update(chunk)
                md5name = self.tar_file_name + '.md5'
                with open(md5name, 'w') as fout:
                    fout.write(md5hash.hexdigest() + "  " + self.tar_file_name)
            except Exception as e:
                print 'Failed to create output tarball or semaphore', e.message

    def __del__(self):
        """
        Close the logs
        """
        # self.close_log()

###############################################################################
def main(argv):
    """
    Calls run manager to run the execution

    return values are:
        0 - everything ran okay
        1 - transient error, try again later
        2 - there was some internal problem with the reconstruction - needs to be
            checked by software expert
        3 - there was some problem with this script - needs to be checked by
            checked by software expert
    """
    my_return_value = 3
    my_run = None
    args = arg_parser()
    args_in_ = args.parse_args(argv) # call the arg_parser before logging
                                     # starts so we get -h output okay
    try:
        my_run = RunManager(args_in_)
        my_return_value = my_run.run()
        print 'Reconstruction returned with status', my_return_value
    # download errors are considered transient - i.e. try again later
    except DownloadError:
        my_return_value = 1
        sys.excepthook(*sys.exc_info())       
    # some failure in the reconstruction algorithms - needs investigation
    except MausError:
        my_return_value = 2
        sys.excepthook(*sys.exc_info())
    # some other exception - probably a failure in this script - needs
    # investigation
    except:
        my_return_value = 3
        sys.excepthook(*sys.exc_info())
    finally:
        del my_run
    return my_return_value
        

if __name__ == "__main__":
    RETURN_VALUE = main(sys.argv[1:])
    sys.exit(RETURN_VALUE)

