#! /usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import glob
import json
import argparse
import re

import CRABClient

# import CMSSW stuff
CMSSW_BASE = os.environ['CMSSW_BASE']

from cp3_llbb.SAMADhi.SAMADhi import Dataset, Sample, File, DbStore

from cp3_llbb.GridIn import utils

def get_sample(sample):
    dbstore = DbStore()
    resultset = dbstore.find(Sample, Sample.name==sample)
    return list(resultset.values(Sample.sample_id))

def get_options():
    """
    Parse and return the arguments provided by the user.
    """
    parser = argparse.ArgumentParser(description='Babysit-helper for CRAB3 jobs')
    parser.add_argument('--new', action='store_true', help='Start monitoring a new production', dest='new')
    parser.add_argument('-j', '--json', type=str, action='store', dest='outjson', default='prod_default.json',
                        help='json file storing the status of your on-going production') 
    parser.add_argument("--recheckcompleted", action="store_true", help="Also check the status of jobs marked as 'completed' again", dest="recheckcompleted")
    options = parser.parse_args()
    return options

# helper method
def jobstatusPerStage(jobList):
    stages = dict()
    for jli in jobList:
        status, ji = tuple(jli)
        if "-" in ji:
            stage, sji = ji.split("-")
        else:
            stage, sji = "0", ji
        if stage not in stages:
            stages[stage] = dict()
        stages[stage][sji] = status
    return stages

def main():
    #####
    # Initialization
    #####
    options = get_options()
    alltasks = [t for t in os.listdir('tasks') if os.path.isdir(os.path.join('tasks', t))]
    assert len(alltasks) > 0, "No task to monitor in the tasks/ directory"
        
    tasks = {}
    # CRAB3 status
    tasks['COMPLETED'] = []
    tasks['SUBMITFAILED'] = []
    tasks['RESUBMITFAILED'] = []
    tasks['NEW'] = []
    tasks['SUBMITTED'] = []
    tasks['TORESUBMIT'] = []
    tasks['UNKNOWN'] = []
    tasks['QUEUED'] = []
    tasks['FAILED'] = []
    tasks['KILLED'] = []
    tasks['HOLDING'] = []
    # GRIDIN status
    tasks['GRIDIN-INDB'] = []
    
    FWHash = ""
    AnaRepo = ""
    AnaHash = ""

    #####
    # Figure out what is the name of the file things should be written into
    #####
    outjson = options.outjson
    if options.new:
        # NB: assumes all the on-going tasks are for the same analyzer
        module = utils.load_request('tasks/' + alltasks[0])
        psetName = module['OriginalConfig'].JobType.psetName
        print "##### Figure out the code(s) version"
        # first the version of the framework
        FWHash, FWRepo, FWUrl = utils.getGitTagRepoUrl( os.path.join(CMSSW_BASE, 'src/cp3_llbb/Framework') )
        # then the version of the analyzer
        AnaHash, AnaRepo, AnaUrl = utils.getGitTagRepoUrl( os.path.dirname( psetName ) )
        outjson = 'prod_' + FWHash + '_' + AnaRepo + '_' + AnaHash + '.json'
        print "The output json will be:", outjson
    else:
        newestjson = max(glob.iglob('prod_*.json'), key=os.path.getctime)
        if outjson == 'prod_default.json' and newestjson != 'prod_default.json':
            outjson = newestjson
            FWHash, AnaRepo, AnaHash = outjson.strip('prod_').strip('.json').split('_')

    #####
    # Read the json if it exists, then check if COMPLETED samples have been entered in SAMADhi since the script was last run
    #####
    data = {}
    if os.path.isfile(outjson):
        with open(outjson) as f:
            data = json.load(f)
    
        for t in data[u'COMPLETED']:
            if t in data[u'GRIDIN-INDB']:
                continue
            s = re.sub('crab_', '', str(t)) + '_' + FWHash + '_' + AnaRepo + '_' + AnaHash
            s_id = get_sample(unicode(s))
            if len(s_id) > 0:
                data['GRIDIN-INDB'].append(t)
    
    #####
    # Loop over the tasks and perform a crab status
    #####
    for task in alltasks:
        if len(data) > 0:
            if unicode(task) in data[u'GRIDIN-INDB']:
                tasks['GRIDIN-INDB'].append(task)
                continue
            elif ( not options.recheckcompleted ) and unicode(task) in data[u'COMPLETED']:
                tasks['COMPLETED'].append(task)
                continue
        taskdir = os.path.join('tasks/', task)
        print ""
        print "#####", task, "#####"
        try:
            status = utils.send_crab_command('status', dir = taskdir)
            if status["dbStatus"] != "SUBMITTED": ## still being processed by the CRAB server
                status_code = status["dbStatus"]
            else: ## submitted to the grid, get the DAG status (SUBMITTED, COMPLETED or FAILED)
                status_code = status["dagStatus"]
                if status_code == "SUBMITTED": ## can already try to resubmit if any of the followup stages have failed jobs
                    jobsPerStage = jobstatusPerStage(status["jobList"])
                    lSt = "0" if len(jobsPerStage) == 1 else max(jobsPerStage.iterkeys(), key=lambda st : int(st))
                    if any(jst == "failed" for sji,jst in jobsPerStage[lSt].iteritems()):
                        status_code = "TORESUBMIT"
            tasks[status_code].append(task)
        except CRABClient.ClientExceptions.CachefileNotFoundException:
            print("Something went wrong: directory {} was not properly created. Will count it as 'SUBMITFAILED'...\n".format(taskdir))
            tasks['SUBMITFAILED'].append(task)
            continue

    
    #####
    # Dump the crab status into the output json file
    #####
    with open(outjson, 'w') as f:
        json.dump(tasks, f)
    
    #####
    # Print summary
    #####
    print "##### ##### Status summary (" + str(len(alltasks)), " tasks) ##### #####"
    for key in tasks:
        if len(tasks[key]) == 0:
            continue
        line = key + ": " + str(len(tasks[key]))
        print line

    def format_blacklist():
        if os.path.isfile("blacklist.txt"):
            with open("blacklist.txt") as f:
                sites = f.readlines()
                if len(sites) > 0:
                    return " --siteblacklist={}".format(",".join(sites))

        return ""
    
    #####
    # Suggest some actions depending on the crab status
    #    * COMPLETED -> suggest the runPostCrab.py command
    #    * SUBMITFAILED -> suggest to rm -r the task and submit again
    #####
    print "##### ##### Suggested actions ##### #####"
    if len(tasks['COMPLETED']) > 0:
        print "##### COMPLETED tasks #####"
        for task in tasks['COMPLETED']:
            print "runPostCrab.py tasks/" + task
    if len(tasks['SUBMITFAILED']) > 0:
        print "##### SUBMITFAILED tasks #####"
        for task in tasks['SUBMITFAILED']:
            print "rm -r tasks/" + task + "; crab submit " + task + ".py" + format_blacklist()
    if len(tasks['FAILED']) > 0:
        print "##### FAILED tasks #####"
        for task in tasks['FAILED']:
            print "crab resubmit tasks/" + task + format_blacklist()
    if len(tasks['TORESUBMIT']) > 0:
        print "##### TORESUBMIT tasks #####"
        for task in tasks['TORESUBMIT']:
            print "crab resubmit tasks/" + task + format_blacklist()

if __name__ == '__main__':
    main()
