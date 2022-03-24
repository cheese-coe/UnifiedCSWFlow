#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Unified CyberShake Workflow (UnifiedCSWFlow)
#
# Author:  Juan Esteban Rodríguez, Josep de la Puente
# Contact: juan.rodriguez@bsc.es, josep.delapuente@bsc.es
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the 
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

################################################################################
# Module imports
import sys
import os
import glob
import traceback
import argparse
import json
import shutil
import concurrent.futures
import threading
import time
from tqdm import tqdm
from itertools import repeat

from unifiedCSWFlow import workflow, DAL, ruptures

################################################################################
# Methods and classes

# Arguments parser
def parser():

    # Parse the arguments
    parser = argparse.ArgumentParser(
        prog='unifiedCSWFlow',
        description='This program automate the CyberShake flow and scripts calls')
    parser.add_argument('config', help='JSON configuration file')
    args = parser.parse_args()

    # Check the arguments
    if not os.path.isfile(args.config):
      raise Exception("The configuration file '"+args.config+ "' doesn't exist")

    # Return them
    return args
    
# Generate CyberShake CFG file
def generateCyberShakeCFG(config):

    # Just for simplify
    path = config["output"]["cRunpath"]        
        
    # Generate the CS CFG file
    with open(path + "/cybershake.cfg", 'w') as f:
        f.write("CS_PATH = %s\n" % config["input"]["cyberShake"]["path"])
        f.write("SCRATCH_PATH = %s\n" % (path + "/scratch"))
        f.write("TMP_PATH = %s\n" % (path + "/tmp"))
        f.write("RUPTURE_ROOT = %s\n" % config["input"]["ERF"]["ruptures"])
        f.write("MPI_CMD = %s\n" % "srun")
        f.write("LOG_PATH = %s\n" % (path + "/logs"))

    # Copy the config.py (this avoids to modify the original config.py file)
    shutil.copyfile(config["input"]["cyberShake"]["path"] + "/config.py",
                    path + "/config.py")

# Run site
def runSite(config, runID, site):
    # Obtain Thread ID
    #id = int(threading.current_thread().name.split("_")[-1])+1
    id = (runID%2+1)*2-1

    # Show current site
    #print("#"*40 + "    " + site + "    " + "#"*40)

    dal = DAL.SQLiteHandler(config["input"]["database"]["path"])
    
    # Disable restart
    restartSite = False   
    
    # Create workflow    
    wflow = workflow.Workflow()
    
    # Create the directory for the current run
    cRunPath = config["output"]["path"] + "/" + site + "_" + str(runID)
    # Check if current site has started
    config["compute"]["restartFile"] = "stage.txt"
    rstage = None
    if not (config["compute"]["restart"] and os.path.isdir(cRunPath)):
        os.makedirs(cRunPath, exist_ok=True)
    else:
        stageFile = cRunPath+ "/" + config["compute"]["restartFile"]
        if (os.path.exists(stageFile)):
            with open(stageFile, 'r') as f:
                rstage = f.readlines()[0]
                
    if rstage and rstage == list(wflow)[-1]:
        print("Skipping site " + site + "': Already done ")            
        return
                   
    os.chdir(cRunPath)
    
    # Set the current path
    config["output"]["cRunpath"] = cRunPath
                
    # Obtain the Site ID for a given site name
    siteId = dal.getSiteID(site)    
                
    # Check if the site do exist
    if not siteId:
        print("[ERROR] Skipping site '" + site + ": It was not found in the database", file=sys.stderr)
        return
    
    # Just a shortcut
    csetup = config["compute"]["setup"]
        
    # Insert the current run information onto the Database
    dal.addRunInfo(runID, siteId, config["input"]["ERF"]["id"], 
                   config["input"]["model"]["id"],
                   csetup["sourceFrequency"], csetup["frequency"])
    
    # Store the ERF ID
    if not 'ruptures' in config["input"]["ERF"].keys():
        config["input"]["ERF"]["ruptures"] = cRunPath + "/ruptures/"
   
        # Generate rupture file
        rupt = ruptures.Ruptures(config["input"]["ERF"]["ruptures"] + "Ruptures_erf" 
                        + str(config["input"]["ERF"]["id"]) + "/",
                        config["input"]["database"]["path"])

        fm = config["compute"]["setup"]["focalMechanism"]
        rupt.setFocalMechanism(fm["dip"], fm["strike"], fm["rake"])
        rupt.generateRuptures(id, site)

    # Write the CyberShake CFG file
    generateCyberShakeCFG(config)  
                    
    pbar = tqdm(wflow, desc='[' + site + '] Running CyberShake task', 
            position=id, total=len(list(wflow)), leave=False)
    for stage in pbar:
        
        # Progress bar
        pbar.set_description("[%s] Running stage '%s'" % (site, stage))
        
        # Check the start point
        # NOTICE!!! Be careful using this feature, no control is taken about previous
        #           assumed "done" steps
        
        if rstage:
            if stage == rstage:
                #print("Starting after stage " + stage)
                rstage = None
            #else:
                #print("Skipping stage " + stage)
            continue
        
        # Obtain the current stage's controller
        siteSN = dal.getSiteShortName(site)
        stage = eval("workflow." + stage)(config, id+1, runID, siteSN)
        
        # Built the curent stage script
        stage.build()
        
        # Run the stage
        stage.run()
        
        # Postprocess steps
        stage.postprocess()
        
    pbar.set_description("SITE: %s computed!" % site)
    pbar.close()                        
    
                    
# Main Method
def main():
    try:
        # Call the parser
        args = parser()
        
        print("#"*40 + "   Preprocess (setup from: " + args.config + ")  " + "#"*40)
        
        # Read the configuration file
        with open(args.config, 'r') as f:
            config = json.load(f)    
                    
        # Create the base directory for the run and enter in it
        workSpace = os.path.abspath(config["output"]["path"])
        config["output"]["path"] = workSpace
        
        os.makedirs(workSpace, exist_ok=True)
        os.chdir(workSpace)
        
        # DataBase handling
        db = config["input"]["database"]
        
        # Create connection        
        dal = DAL.SQLiteHandler(db["path"])
        
        # Check if the DB must be populated
        if db["populate"]:
            dal.importData(db["importFrom"])
        
        # Obtain a valid starting point RunID from DataBase
        runID = dal.getValidRunId()
        if config["compute"]["restart"]:
            tmpids = [i.split("_")[-1] for i in glob.glob(config["output"]["path"]+"/*[0-9]")]
            if tmpids:
              runID = int(min([i.split("_")[-1] for i in glob.glob(config["output"]["path"]+"/*[0-9]")]))
        
        # Obtain both model and ERF IDs 
        modelID = dal.getModelID(config["input"]["model"]["name"])
        erfID = dal.getERFID(config["input"]["ERF"]["name"])
         
        # Check if both erfID and modelID were found
        foo =  "Please, check input JSON file '" + args.config + "' "
        if erfID == None:
            raise Exception("ERF not found." + foo)
            
        if modelID == None:
            raise Exception("Model not found." + foo)
        
        # Store the ERF ID and Model ID
        config["input"]["ERF"]["id"] = erfID
        config["input"]["model"]["id"] = modelID

        # For each Site defined as input
        runIDs = []
        i = 0;
        for site in config["input"]["sites"]:
            runIDs.append(runID+i)
            i = i + 1

#        with concurrent.futures.ThreadPoolExecutor(max_workers=config["compute"]['workers']) as executor:
        with concurrent.futures.ProcessPoolExecutor(max_workers=config["compute"]['workers']) as executor:
            results = list(tqdm(executor.map(runSite, repeat(config),
                                runIDs, config["input"]["sites"]), 
                                total=len(config["input"]["sites"]), position=0,
                                desc='[Sites simulation progress]'))                     
                            
    except Exception as error:
        print("Exception in code:")
        print('-'*80)
        traceback.print_exc(file=sys.stdout)
        print('-'*80)

################################################################################
# Run the program
if __name__ == "__main__":
    main()
