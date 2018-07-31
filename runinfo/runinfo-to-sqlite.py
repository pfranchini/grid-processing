"""
for a range of runs:
    extract the cooling channel tag
        if tag not in CDB, find the tag based on cooling channel currents
    extract the cooling channel mode based on FC polarity in CDB
    extract the absorber tag
        if tag not in CDB, find the tag based on absorber shape and material
    extract the beamline optics
    dump all this info to a sqlite database
"""
        

import cdb
import sys
import sqlite3
from datetime import datetime, date

CDBURL = "http://cdb.mice.rl.ac.uk"

CHANNEL_TAGS_FILE = "channel_tags.txt"

_SQLITE_OUTFILE = "runinfo.sqlite"

##################################################################

class RunInfo():
    #########################
    def __init__(self):
        """
        initialize
        """
        self._cc = cdb.CoolingChannel(CDBURL)
        self._bl = cdb.Beamline(CDBURL)
        self.absdict = {}
        self.rundict = {}
        self.build_absorber_tags()
        self.abs_tag = None
        self.cc_tag = None
        self.bl_tag = None
        self._cctagdict = {}
        self._NCOILS = 11

    #########################
    def check_run_exists(self, run_num):
        """
        boolean -- check if the run has a CDB entry
        """
        self.blinfo = None
        try:
            self.blinfo = self._bl.get_beamline_for_run(run_num)[run_num]
            return True
        except:
            return False

    #########################
    def get_beamline_info(self, run_num):
        """
        get the beamline optics, #triggers, start and end times for a given run
        """
        self.bl_tag = None
        self.bl_start = None
        self.bl_end = None
        self.bl_ntrigs = -1
        self.bl_daqtrigger = None
        try:
            # get beamline info from CDB
            blinfo = self._bl.get_beamline_for_run(run_num)[run_num]
            self.bl_tag = blinfo['optics']
            self.bl_start = blinfo['start_time'] 
            self.bl_end = blinfo['end_time'] 
            self.bl_ntrigs = blinfo['scalars']['Particle Triggers']
            self.bl_daqtrigger = blinfo['daq_trigger']
        except:
            pass

    #########################
    def build_cctaginfo(self):
        """
        from a list of tags, build a dictionary of dictionaries for the tag: coil-currents
        """
        # option 1: just get all tags from cdb using list_tags() and get currents for those
        #           this has a problem. some tags have wrong coil names
        #self._cc_tag_list = self._cc.list_tags()
        #
        # option 2: specify the list of "good" or "known" tags (from run control logs, run plans, etc)
        #
        with open(CHANNEL_TAGS_FILE) as infile:
            self._cc_tag_list = infile.read().splitlines()
        # build a dictionary of coilnames:coilcurrents for tag
        self.build_ccdict_from_tag()

    #########################
    def build_ccdict_from_tag(self):
        """
        given a list of tags
        build a dictionary of dictionaries which is
        {tag:{coilname1:coilcurrent1, coilname2:coilcurrent2...}}
        for ssu-c ssu-e1 ssu-e2 ssu-m2 ssu-m1 fc ssd-m1 ssd-m2 ssd-c ssd-e1 ssd-e2
        """
        for tag in self._cc_tag_list:
            self._cctagdict[tag] = {}
            maginfo = self._cc.get_coolingchannel_for_tag(tag)
            for mag in maginfo:
                for coil in mag['coils']:
                    # round current to int so as to make comparisons sensible
                    self._cctagdict[tag][coil['name']] = int(coil['iset']+0.5)
            # issue 1: some tags have iset=5 even though the trims were off
            #            in this case, set it to 0
            if int(self._cctagdict[tag]['SSU-T1']) == 5:
                self._cctagdict[tag]['SSU-T1'] = 0
            if int(self._cctagdict[tag]['SSU-T2']) == 5:
                self._cctagdict[tag]['SSU-T2'] = 0
            if int(self._cctagdict[tag]['SSD-T1']) == 5:
                self._cctagdict[tag]['SSD-T2'] = 0
            if int(self._cctagdict[tag]['SSD-T2']) == 5:
                self._cctagdict[tag]['SSD-T2'] = 0

            # issue 2: tags have T1, T2 -- trim currents
            #          cooling channel for run has E1, E2 - end coil currents
            # fix t1, t2 e1,e2 correspondence
            self._cctagdict[tag]['SSU-E1'] = self._cctagdict[tag]['SSU-C'] - self._cctagdict[tag]['SSU-T1']
            self._cctagdict[tag]['SSU-E2'] = self._cctagdict[tag]['SSU-C'] - self._cctagdict[tag]['SSU-T2']
            self._cctagdict[tag]['SSD-E1'] = self._cctagdict[tag]['SSD-C'] - self._cctagdict[tag]['SSU-T1']
            self._cctagdict[tag]['SSD-E2'] = self._cctagdict[tag]['SSD-C'] - self._cctagdict[tag]['SSU-T2']
            del self._cctagdict[tag]['SSU-T1']
            del self._cctagdict[tag]['SSU-T2']
            del self._cctagdict[tag]['SSD-T1']
            del self._cctagdict[tag]['SSD-T2']

            # issue 3: SSU-E1CE2-NoTrims140A has FC=0, 
            #            but *SOME* runs with those tags(according to rc log) have FC = 50
            #--> this is for 8161 <= run <= 8196
            # --> need to double check with archiver, run plan, etc
#           if tag == "SSU-E1CE2-NoTrims140A" and int(self._cctagdict[tag]['FCU-C']) == 0:
#               self._cctagdict[tag]['FCU-C'] = 50

            # issue 4: CHAN-E1CE2-NoTrims140A-FCD44A has FC=0, 
            #            but runs with those tags(according to rc log) have FC = 44.7 (tag name has 44)
            if tag == "CHAN-E1CE2-NoTrims140A-FCD44A" and int(self._cctagdict[tag]['FCU-C']) == 0:
                self._cctagdict[tag]['FCU-C'] = 45

            # issue 5: CHAN-E1CE2-NoTrims140A-FCD47.5A has FC=0, 
            #            but runs with those tags(according to rc log) have FC = 47.7 (tag name has 47.5)
            if tag == "CHAN-E1CE2-NoTrims140A-FCD47.5A" and int(self._cctagdict[tag]['FCU-C']) == 0:
                self._cctagdict[tag]['FCU-C'] = 48
            if tag == "CHAN-E1CE2-NoTrims140A-FCD50.0A" and int(self._cctagdict[tag]['FCU-C']) == 0:
                self._cctagdict[tag]['FCU-C'] = 50
            if tag == "CHAN-E1CE2-NoTrims140A-FCD40.0A" and int(self._cctagdict[tag]['FCU-C']) == 0:
                self._cctagdict[tag]['FCU-C'] = 40

            #print
            #print tag
            #print self._cctagdict[tag]
            #print

    #########################
    def get_cc_for_run(self, run_num):
        """
        input: run number
        lookup cooling channel info for run
        if tag exists in cooling channel info, then return with the tag
        else, try to find the tag based on coil currents
        """

        self.cc_tag = "unknown"
        self.cc_mode = "unknown"
        try:
            ccinfo = self._cc.get_coolingchannel_for_run(run_num)
        except:
            print run_num, ' has no coolingchannel info'
            return
        # make sure coolingchannel info is not an empty list
        if not ccinfo:
            return
        self.get_cctag_from_runinfo(ccinfo)


    #########################
    def get_cctag_from_runinfo(self, ccinfo):
        # try to lookup tag -- should be there for runs from Feb 2017
        self.cc_tag = ccinfo['tag']

        self.cc_mode = "solenoid"
        ccrundict = {}
        # go through cooling channel info and build a dict of coil names and currents
        # 1) check the FC polarity and set the mode
        # ---> potential problem, before 2017/01, the mode was based on hall probe readout
        #      and may not be fully reliable, may need to check run plans etc
        # 2) round up currents as an integer, so that comparison with tag currents is valid
        magnet_data = ccinfo['magnets'] if isinstance(ccinfo, dict) else ccinfo
        for mag in magnet_data:
            if mag['name'] == "FCU" and mag['polarity'] == -1:
                self.cc_mode = "flip"
                print '>>>>>>> ',self.cc_mode

            for coil in mag['coils']:
                ccrundict[coil['name']] = int(coil['iset']+0.5)
        #print
        #print set(ccrundict.items())
        #print

        # if tag not in cc info, then do a lookup
        if self.cc_tag != 'null':
            return

        # go through the list of tags, compare coil-current dict with that from the tag
        # if they match, then we have found the tag corresponding to these currents
        for tag in self._cc_tag_list:
            #print '... ',tag,set(self._cctagdict[tag].items())
            if len(set(self._cctagdict[tag].items()) & set(ccrundict.items())) == self._NCOILS:
                self.cc_tag = tag
                return
        #return tag

    #########################
    def get_absorber_for_run(self, run_num):
        """
        try to find absorber tag from coolingchannel table
        if not, try to do a lookup
        """
        try:
            absrun = self._cc.get_absorber_for_run(run_num)
        except:
            print run_num, 'has no absorber info'
            self.abs_tag = "unknown"
            return
        for k, v in absrun.items():
            # first check if the absorber info has a non-null tag
            # should be valid for runs from Feb 2017
            if k[1] != 'null':
                self.abs_tag = k[1]
                return
            else:
                # if no tag in info, build a tuple: (material, shape)
                for abs in v:
                    if abs['name'].rstrip() == 'primary':
                        absinfo = (abs['material'].rstrip(), abs['shape'].rstrip())
                        try:
                            # lookup the tag corresponding to (material, shape)
                            self.abs_tag = self.absdict[absinfo]
                        except:
                            # lookup will fail if (material, shape) is not in any tag
                            self.abs_tag = "unknown"

    #########################
    def build_absorber_tags(self):
        """
        (material, shape) <-> tag correspondence
        """
        self.absdict[('LiH','disk')] = 'ABS-SOLID-LiH'
        self.absdict[('LiH','empty')] = 'ABS-SOLID-Empty'
        self.absdict[('solid','empty')] = 'ABS-SOLID-Empty'
        self.absdict[('LH2','full')] = 'ABS-LH2'
        self.absdict[('LH2','empty')] = 'ABS-LH2-EMPTY'


##################################################################

class RunDB():
    #########################
    def __init__(self):
        # create a sqlite database
        self.rundb = sqlite3.connect(_SQLITE_OUTFILE)
        self.create_table()

    #########################
    def create_table(self):
        dbconn = self.rundb.cursor()
        # if table exists, drop it and create a new one
#       dbconn.execute("DROP TABLE IF EXISTS runinfo")
        dbconn.execute("CREATE TABLE IF NOT EXISTS runinfo(\
                run INTEGER PRIMARY KEY,\
                start DATE,\
                daqtrigger STRING,\
                optics STRING,\
                channel STRING,\
                mode STRING,\
                absorber STRING,\
                triggers INTEGER,\
                end DATE,\
                comment STRING)\
                ")

    #########################
    def update_index(self):
        dbconn = self.rundb.cursor()
        dbconn.execute("CREATE TABLE IF NOT EXISTS runindex(\
                lastrun INTEGER PRIMARY KEY)\
                ")
        dbconn.execute("INSERT INTO runindex(lastrun) VALUES (?)", (self.lastrun,))
        self.rundb.commit()

    #########################
    def update_table(self, run, start, daqtrigger, optics, channel, mode, absorber, triggers, end, comment):
        """
        Update the database
        If a run already exists in the DB, skip and keep going
        """
        dbconn = self.rundb.cursor()
        dbconn.execute("INSERT INTO runinfo(run, start, daqtrigger, optics, channel, mode, absorber, triggers, end,  comment) VALUES (?,?,?,?,?,?,?,?,?,?)", (run,start,daqtrigger,optics,channel,mode,absorber,triggers,end,comment))
        self.rundb.commit()
        self.lastrun = run

    def run_exists(self, run):
        """
        input is run number
        check if run exists in the table, if yes return true
        """
        dbconn = self.rundb.cursor()
        dbconn.execute("SELECT run FROM runinfo WHERE run = ?", (run,))
        check = dbconn.fetchall()
        if len(check) != 0:
            print 'DB: ',run, ' exists'
            return True
        return False
##################################################################

if __name__ == "__main__":

    try:
        run_num_min = int(sys.argv[1])
        run_num_max = int(sys.argv[2])
    except:
        print "Usage: ", sys.argv[0], "start-run-number end-run-number"
        sys.exit(1)

    # initialize runinfo, db classes
    _ri = RunInfo()
    _db = RunDB()

    # generate the cooling channel dictionary with currents from tags
    # this will be needed to find the tag based on currents for a given run
    _ri.build_cctaginfo()

    commentstring = ""
    for run_num in range(run_num_min, run_num_max):
        
        # check if this run is already in our SQLITE DB
        if _db.run_exists(run_num):
            continue

        # get out if no run in cdb
        if not _ri.check_run_exists(run_num):
            continue
        # get beamline optics, start/end, #triggers
        _ri.get_beamline_info(run_num)
      
        # get the coolingchannel tag for the run
        _ri.get_cc_for_run(run_num)

        # get the absorber tag for the run
        _ri.get_absorber_for_run(run_num)

        if run_num >= 8161 and run_num < 8196:
            _ri.cc_tag = "SSU-E1CE2-NoTrims140A-FC50A" # this is a new tag that has to be created
        # update db table with info
        print '+++ ',run_num, _ri.bl_start, _ri.bl_daqtrigger, _ri.bl_tag, _ri.cc_tag, _ri.cc_mode, _ri.abs_tag, _ri.bl_ntrigs, _ri.bl_end, commentstring
        _db.update_table(run_num, _ri.bl_start, _ri.bl_daqtrigger, _ri.bl_tag, _ri.cc_tag, _ri.cc_mode, _ri.abs_tag, _ri.bl_ntrigs, _ri.bl_end, commentstring)
    # update indexdb with the last run number processed
    _db.update_index()

