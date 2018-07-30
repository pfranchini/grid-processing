#!/bin/env python

"""
Simulate the MICE experiment

This will simulate MICE spills through the entirety of MICE using Geant4, then
digitize and reconstruct TOF and tracker hits to space points.
"""

import io   # generic python library for I/O
import MAUS # MAUS libraries
import os
import json
import time
import sys
import getopt
import subprocess
import shutil
import fileinput
import json

def download_geometry(self):
    """
    Downloads the geometry from the configuration database
        
    Calls the download_geometry.py utility script to download geometries by
    run number... But the table does not link step IV geometries to MC data
    cards yet. 

    If running in test mode, uses legacy Stage4 geometry instead.

    @raises DownloadError on failure
    """
    print 'Getting geometry'
    download = [os.path.join(self.run_setup.maus_root_dir, 'bin', \
                            'utilities', 'download_geometry.py')]
    # check that there is a selection for the geometry in the datacards
    download += self.run_setup.get_download_parameters()
    proc = subprocess.Popen(download, stdout=self.logs.download_log, \
                                                       stderr=subprocess.STDOUT)
    proc.wait()
    if self.run_setup.test_mode:
        test_path_in = os.path.join(self.run_setup.maus_root_dir, 'src',
                    'legacy', 'FILES', 'Models', 'Configurations', 'Test.dat')
        test_path_out = os.path.join(self.run_setup.download_target, \
                                                       'ParentGeometryFile.dat')
        shutil.copy(test_path_in, test_path_out)
    if proc.returncode != 0:
        raise DownloadError("Failed to download geometry successfully")

def get_setup_dict(deck_name, chunk_number, sim_run, seed, run_number):

    """
    # Settings for a classic 3mm-200MeV/c muon beam
    g4bl = {"run_number":run_number,"q_1":1.018,"q_2":-1.271,"q_3":0.884,"d_1":-1.242,"d_2":d2current,\
            "d_s":3.666,"particles_per_spill":0,"rotation_angle":0,"translation_z":1000.0,\
            "protonabsorberin":1,"proton_absorber_thickness":83,"proton_number":1E12,"proton_weight":1,\
            "particle_charge":'all',"file_path":'MAUS_ROOT_DIR/src/map/MapPyBeamlineSimulation/G4bl',\
            "get_magnet_currents_pa_cdb":"False","random_seed":seed}
    """
    deck_file = deck_name + '.json'
    with open(deck_file) as data_file:
        fields = json.load(data_file)

    #G4BL_PATH='MAUS_ROOT_DIR/src/map/MapPyBeamlineSimulation/G4bl'
    maus_root_dir = os.environ["MAUS_ROOT_DIR"]
    G4BL_PATH=os.path.join(maus_root_dir, 'src/map/MapPyBeamlineSimulation/G4bl')
    GEO_DIR="geo-"+str(sim_run)
    GEO_FILE = GEO_DIR + "/ParentGeometryFile.dat"
    if not "proton_absorber" in fields:
        fields["proton_absorber"] = 29

    g4bl = {"run_number":run_number,"q_1":fields["Q1"],"q_2":fields["Q2"],"q_3":fields["Q3"],\
            "d_1":fields["D1"],"d_2":fields["D2"],\
            "d_s":fields["DS"],"particles_per_spill":0,"rotation_angle":0,"translation_z":1000.0,\
            "protonabsorberin":1,"proton_absorber_thickness":fields["proton_absorber"],\
            "proton_number":1E12,"proton_weight":1,\
            "particle_charge":'all',"file_path":G4BL_PATH,\
            "get_magnet_currents_pa_cdb":"False","random_seed":seed}
            
    my_dict = {
        "simulation_geometry_filename":GEO_FILE,
        "output_root_file_name":deck_name+"_"+chunk_number+".root",
	"spill_generator_number_of_spills":1,
	"g4bl":g4bl
    }
    return my_dict

def setup_from_dict(key_dict):
    my_keys = ""
    for key, value in key_dict.iteritems():
        my_keys += str(key)+" = "+json.dumps(value)+"\n"
    return io.StringIO(unicode(my_keys))

def run():
    """ Run the macro
    """
    G4BL_EXE='run_g4bl.py'
    run_args = ["python", G4BL_EXE, "--configuration_file", "datacard.json"]
    proc = subprocess.Popen(run_args)
    proc.wait()

if __name__ == '__main__':
    
    # Get the name from the command line
    deck_name = ""
    chunk_number = -1 
    sim_run = -1 

    (opts, args) = getopt.getopt(sys.argv[1:], "", ["deck_name=", "chunk_number=", "sim_run="])
    for o, a in opts:
	    if o == "--deck_name":
	        if a:
		    deck_name = "%s" % a
	    if o == "--chunk_number":
	        if a:
		    chunk_number = a
	    if o == "--sim_run":
	        if a:
		    sim_run = a


    if not deck_name or chunk_number < 0 or sim_run < 0:
        print "Some of the parameter(s) is/are missing"
        sys.exit(1)
        
    # Get the simulation seed and run_numbeer from the current time
    seed = int(time.time())
    run_number = seed
    
    # Fetch the data cards and load them
    setup_dict = get_setup_dict(deck_name, chunk_number, sim_run, seed, run_number)
    datacards = setup_from_dict(setup_dict)
    with open('datacard.json', 'w') as fd:
        datacards.seek (0)
        shutil.copyfileobj (datacards, fd)
        
    # Run the simulation (e12 protons on target)
    run()

