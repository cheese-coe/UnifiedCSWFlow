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
import os
import time
import subprocess
import threading
import math
from tqdm import tqdm
from abc import ABC, abstractmethod

from unifiedCSWFlow import DAL

################################################################################
# Methods and classes
        
# CyberShake main Workflow definition for one site
class Workflow():
    
    # Define common names for outputs
    OUTPUTS = {"fdloc": "fdloc", 
               "faultlist": "faultlist",
               "radiusfile": "radiusfile",
               "coordfile": "coordfile"}
                   
    # Initialization method
    def __init__(self):
# Complete (Original Cybershake workflow)
#       self.wf = [
#            'preSGT',
#            'preAWP',
#            'AWPX',
#            'AWPY',
#            'checkX',
#            'checkY',
#            'postX',
#            'postY',
#            'rupVar',                                                                                                                                                                                             
#            'runDS',
#            'insertDB',
#            'checkDB',
#            'curveCalc',
#            'cleanUp'
#       ]

        self.wf = [ 
            'preSGT',
            'preAWP',
            'AWPX',
            'AWPY',
            'checkX',
            'checkY',
            'postX',
            'postY',
            'rupVar',
            'runDS',
            'cleanUp'
        ]   
                
    # Define the iterator    
    def __iter__(self):
        return iter(self.wf)
        
# Abstract class for defining a common interface between all workflow stages
class scriptABC(ABC):

    @property
    @classmethod
    @abstractmethod
    # Default script type
    def type(cls):
        return NotImplementedError
            
    # Initialization method
    def __init__(self, config, id, runID, site):
        self.lines = []
        self.config = config
        self.runID = runID
        self.site = site
        self.time = 1
        self.id = id
                
    # Method for defining the "buildScript" required method
    def build(self):
        raise NotImplementedError("Error: 'build' method should be implemented")
        
    # Method for defining the "run" required method# Method for defining the "postprocess" required method
    def postprocess(self):
        raise NotImplementedError("Error: 'postprocess' method should be implemented")        
            
    # Just a class method for obtaining the class name
    @classmethod
    def _className(cls):
        return(cls.__name__)

    # Obtain common script header 
    def _getHeader(self):
        self.lines.append("#!/bin/bash")
        self.lines.append("")
        
    # Build specific slurm rules
    def _getSlurmRules(self, tlimit, nodes, tasks, cpus, qos):

        #Obtain class name
        cname = self._className()
        
        # Set time estimation
        self.time = tlimit
                
        # Add rules to the slurm script
        self.lines.append("#SBATCH --time=" + time.strftime('%H:%M:%S', time.gmtime(tlimit)))
        self.lines.append("#SBATCH --nodes=" + str(nodes))
        self.lines.append("#SBATCH --tasks-per-node=" + str(tasks))
        self.lines.append("#SBATCH --cpus-per-task=" + str(cpus))
        self.lines.append("#SBATCH --ntasks=" + str(int(nodes) * int(tasks)))
        self.lines.append("#SBATCH --error=" + cname + ".e")
        self.lines.append("#SBATCH --output=" + cname + ".o")
        self.lines.append("#SBATCH --qos=" + qos)
        self.lines.append("")
        self.lines.append("cd $SLURM_SUBMIT_DIR")
        
    # Generate script
    def _saveScript(self):
        
        # Generate the specific script
        file = self._className() + "." + self.type
        with open(file, 'w') as f:
            for item in self.lines:
                f.write("%s\n" % item)
    
        # Assign execution permisions to the script
        os.chmod(file, 0o777)
                
    # Method in charge of run and wait the script
    def run(self):

        # Build the script filename 
        script = os.path.abspath(self._className() + "." + self.type)
        
        # Decide how to execute the script
        if self.type == "slurm":
            Runner(self._className(), self.time).enqueueSlurm(script, self.id)
        else:
            Runner(self._className(), self.time).runBashScript(script)
        
        pass

# Class in charge of running a process either Slurm or bash 
class Runner():
    
    # Some attributes
    message = ""
    time = 0
    task = ""
    resultAvailable = None
    
    # Initialization method
    def __init__(self, task, tlimit):
        # Task name
        self.task = task
        
        # Initialize the synchronizerpos
        self.resultAvailable = threading.Event()
        
        # Time limit
        self.time = tlimit
        
    # Run a bash script and return 
    def runBashScript(self, cmd):
        #print("Runnning bash script " + cmd)
        
        # Run the command and wait
        process = subprocess.run(cmd, stdout=subprocess.PIPE,
        stdin=subprocess.PIPE,
        stderr=subprocess.PIPE,
        encoding='utf8')
        
        # Check for and error
        if process.returncode != 0:
            raise Exception("Command '" + cmd + "' failed. Error: " + process.stderr)
            pass
    
    # Enqueue a process and wait
    def enqueueSlurm(self, cmd, id):

        # Run the command and wait
        cmdm = 'sbatch ' +  cmd
    
        process = subprocess.run(cmdm, shell=True, 
                                       stdout=subprocess.PIPE,
                                       stdin=subprocess.PIPE,
                                       stderr=subprocess.PIPE,
                                       encoding='utf8')
        
        # Check for and error
        if process.returncode != 0:
            raise Exception("Command '" + cmdm + "' failed. Error: " + process.stderr)
            
        # Obtain the job ID
        jobId = [x.strip() for x in process.stdout.split(' ')][3]
        
        thread = threading.Thread(target=self._waitForSlurmJob, args=(jobId,))
        thread.start()
        
        # Wait for the process to finish
        pbar = tqdm(total = self.time, position=id, leave=False)
        timeout = 1
        while not self.resultAvailable.wait(timeout=timeout):
            #print('\r{}'.format(self.time), end='', flush=True)
            pbar.set_description("     STATUS: %s" % (self.message))
            pbar.update(timeout)

        pbar.set_description("STATUS: %s" % self.task + " completed!")
        pbar.close()            
        
    # Wait for a Slurm job to finish
    def _waitForSlurmJob(self, jobId):

        # Wait for the job to finish
        while True:
            # Query for the job state
            cmd = 'squeue -h -o "Slurm job %T (%M of %l)" --job ' + jobId
            process = subprocess.run(cmd, shell=True,
                                          stdout=subprocess.PIPE,
                                          stdin=subprocess.PIPE,
                                          stderr=subprocess.PIPE,
                                          encoding='utf8')
                                                                                       
            # Check for and error
            if process.returncode != 0:
                raise Exception("Command '" + cmd + "' failed. Error: " + process.stderr)                                                      
        
            # Set a message
            self.message = process.stdout.rstrip()
            
            # Stop condition
            if process.stdout == "":
                break

        # Results are available
        self.resultAvailable.set()
            
# Stage "PreSGT" definition
class preSGT(scriptABC):
    # Attributes     
    type = "slurm"       # Script type
    
    def build(self):
        # Obtain Header 
        self._getHeader()
        
        # Obtain Slurm rules
        r = self.config["compute"]["resources"]
        self._getSlurmRules(r["time"]/6, 1, 1, r["task-per-node"], r["qos"])
        
        # Additional instructions
        self.lines.append("export PYTHONPATH=" + self.config["output"]["cRunpath"] + ":$PYTHONPATH")
        self.lines.append("module load python/2.7.16")
        self.lines.append("")           
        
        # Build command
        cmd = []
        cmd.append(self.config["input"]["cyberShake"]["path"] + "/PreSgt/presgt.py")
        cmd.append(self.site)
        cmd.append(str(self.config["input"]["ERF"]["id"]))
        cmd.append(self.config["input"]["model"]["box"])
        cmd.append(self.config["input"]["model"]["gridOut"])
        cmd.append(self.config["input"]["model"]["coords"])

        outs = Workflow.OUTPUTS
        cmd.append(self.site + "." + Workflow.OUTPUTS["fdloc"])
        cmd.append(self.site + "." + Workflow.OUTPUTS["faultlist"])
        cmd.append(self.site + "." + Workflow.OUTPUTS["radiusfile"])
        cmd.append(self.site + "." + Workflow.OUTPUTS["coordfile"])
        
        cmd.append(self.config["input"]["database"]["path"])
        
        cmd.append(str(self.config["compute"]["setup"]["spacing"]))
        cmd.append(str(self.config["compute"]["setup"]["frequency"]))
        
        self.lines.append(" ".join(cmd))

        # Save the script to disk
        self._saveScript()
        
        pass
            
    def postprocess(self):
        # Write to stage.txt
        with open(self.config["compute"]["restartFile"], 'w') as f:
            f.write('preSGT')

# Stage "PreAWP" definition
class preAWP(scriptABC):
    # Script type
    type = "slurm"
        
    def build(self):
        # Obtain Header 
        self._getHeader()
        
        # Obtain Slurm rules
        r = self.config["compute"]["resources"]
        self._getSlurmRules(r["time"]/6, 1, 1, r["task-per-node"], r["qos"])
        
        # Additional instructions
        self.lines.append("module load python/2.7.16")
        
        self.lines.append("ln -s " + self.config["input"]["model"]["path"] + " awp." + self.site + ".media")
        self.lines.append("")
                
        # Build command
        cmd = []
        cmd.append(self.config["input"]["cyberShake"]["path"] + "/AWP-ODC-SGT/utils/build_awp_inputs.py")        
        cmd.append("--site " + self.site)
        cmd.append("--gridout " + self.config["input"]["model"]["gridOut"])
        cmd.append("--fdloc " + self.site + "." + Workflow.OUTPUTS["fdloc"])
        cmd.append("--cordfile " + self.site + "." + Workflow.OUTPUTS["coordfile"])
        cmd.append("--frequency " + str(self.config["compute"]["setup"]["frequency"]))
        cmd.append("--px " + str(self.config["compute"]["decomposition"]["x"]))
        cmd.append("--py " + str(self.config["compute"]["decomposition"]["y"]))
        cmd.append("--pz " + str(self.config["compute"]["decomposition"]["z"]))
        cmd.append("--source-frequency " + str(self.config["compute"]["setup"]["sourceFrequency"]))        
        cmd.append("--run_id " + str(self.runID))
        cmd.append("--velocity-mesh " + self.config["input"]["model"]["path"])
        
        self.lines.append(" ".join(cmd))

        # Save the script to disk
        self._saveScript()        
        
        pass
    
    def postprocess(self):
        # Write to stage.txt
        with open(self.config["compute"]["restartFile"], 'w') as f:
            f.write('preAWP')

# Stage "AWPX" definition
class AWPX(scriptABC):
    # Script type
    type = "slurm"
        
    def build(self):
        # Obtain Header 
        self._getHeader()
        
        # Obtain Slurm rules
        r = self.config["compute"]["resources"]
        dd = self.config["compute"]["decomposition"]
        ntasks = int(dd["x"]) * int(dd["y"]) * int(dd["z"])
        nodes = int(r["nodes"])
        tnode = math.ceil(float(ntasks)/nodes)
        
        self._getSlurmRules(r["time"], nodes, tnode, r["cpus-per-task"], r["qos"])
        
        # Additional instructions
        self.lines.append("module swap intel gcc")
        self.lines.append("ulimit -c unlimited")
        self.lines.append("")            

        # Additional instructions        
        self.lines.append("export CYBERSHAKE_HOME="\
                         + self.config["input"]["cyberShake"]["path"])
        self.lines.append("")                         
        
        cmd = []
        cmd.append(self.config["input"]["cyberShake"]["path"] + "/AWP-ODC-SGT/awp_odc_wrapper.sh")        
        cmd.append(str(ntasks))
        cmd.append("IN3D." + self.site + ".x")
        
        self.lines.append(" ".join(cmd))

        # Save the script to disk
        self._saveScript()    
        
        pass
    
    def postprocess(self):
        # Write to stage.txt
        with open(self.config["compute"]["restartFile"], 'w') as f:
            f.write('AWPX')

# Stage "AWPY" definition
class AWPY(scriptABC):
    # Script type
    type = "slurm"
        
    def build(self):
        # Obtain Header 
        self._getHeader()
        
        # Obtain Slurm rules
        r = self.config["compute"]["resources"]
        dd = self.config["compute"]["decomposition"]
        ntasks = int(dd["x"]) * int(dd["y"]) * int(dd["z"])
        nodes = int(r["nodes"])
        tnode = math.ceil(float(ntasks)/nodes)
        
        self._getSlurmRules(r["time"], nodes, tnode, r["cpus-per-task"], r["qos"])
        
        # Additional instructions
        self.lines.append("module swap intel gcc")
        self.lines.append("ulimit -c unlimited")
        self.lines.append("")          

        # Additional instructions        
        self.lines.append("export CYBERSHAKE_HOME="\
                         + self.config["input"]["cyberShake"]["path"])
        self.lines.append("")
        
        cmd = []
        cmd.append(self.config["input"]["cyberShake"]["path"] + "/AWP-ODC-SGT/awp_odc_wrapper.sh")        
        cmd.append(str(ntasks))
        cmd.append("IN3D." + self.site + ".y")
        
        self.lines.append(" ".join(cmd))

        # Save the script to disk
        self._saveScript()      
              
        pass
    
    def postprocess(self):
        # Write to stage.txt
        with open(self.config["compute"]["restartFile"], 'w') as f:
            f.write('AWPY')

# Stage "checkX" definition
class checkX(scriptABC):
    # Script type
    type = "slurm"
        
    def build(self):
        # Obtain Header 
        self._getHeader()
        
        # Obtain Slurm rules
        r = self.config["compute"]["resources"]
        nodes = int(self.config["compute"]["decomposition"]["x"])
        ntasks = nodes*r["task-per-node"]
        
        self._getSlurmRules(r["time"]*0.5, 1, 1, r["task-per-node"], r["qos"])
        
        # Additional instructions
        self.lines.append("module load python/2.7.16")
        self.lines.append("")
        
        cmd = []
        cmd.append(self.config["input"]["cyberShake"]["path"] + "/SgtTest/perform_checks.py")        
        cmd.append("comp_x/output_sgt/awp-strain-" + self.site + "-fx")
        cmd.append(self.site + "." + Workflow.OUTPUTS["coordfile"])        
        cmd.append("IN3D." + self.site + ".x")
        
        self.lines.append(" ".join(cmd))

        # Save the script to disk
        self._saveScript()            
        pass
    
    def postprocess(self):
        # Write to stage.txt
        with open(self.config["compute"]["restartFile"], 'w') as f:
            f.write('checkX')

# Stage "checkY" definition
class checkY(scriptABC):
    # Script type
    type = "slurm"
        
    def build(self):
        # Obtain Header 
        self._getHeader()
        
        # Obtain Slurm rules
        r = self.config["compute"]["resources"]
        nodes = int(self.config["compute"]["decomposition"]["x"])
                
        self._getSlurmRules(r["time"]*0.5, 1, 1, r["task-per-node"], r["qos"])
        
        # Additional instructions
        self.lines.append("module load python/2.7.16")        
        self.lines.append("")
        
        cmd = []
        cmd.append(self.config["input"]["cyberShake"]["path"] + "/SgtTest/perform_checks.py")        
        cmd.append("comp_y/output_sgt/awp-strain-" + self.site + "-fy")
        cmd.append(self.site + "." + Workflow.OUTPUTS["coordfile"])        
        cmd.append("IN3D." + self.site + ".y")
        
        self.lines.append(" ".join(cmd))

        # Save the script to disk
        self._saveScript()            
        pass
        
    def postprocess(self):
        # Write to stage.txt
        with open(self.config["compute"]["restartFile"], 'w') as f:
            f.write('checkY')
        
# Stage "postX" definition
class postX(scriptABC):
    # Script type
    type = "slurm"
        
    def build(self):
        # Obtain Header 
        self._getHeader()
        
        # Obtain Slurm rules
        r = self.config["compute"]["resources"]
        self._getSlurmRules(r["time"]/6, 1, 1, r["task-per-node"], r["qos"])
        
        # Additional instructions
        self.lines.append("module load python/2.7.16")
        self.lines.append("")
        
        # Build command
        cmd = []
        cmd.append(self.config["input"]["cyberShake"]["path"] + "/AWP-GPU-SGT/utils/prepare_for_pp.py")
        cmd.append(self.site)
        cmd.append("comp_x/output_sgt/awp-strain-" + self.site + "-fx")
        cmd.append(self.site + "_fy_" + str(self.runID) + ".sgt")
        cmd.append(self.config["input"]["model"]["box"])
        cmd.append(self.site + "." + Workflow.OUTPUTS["coordfile"])
        cmd.append(self.site + "." + Workflow.OUTPUTS["fdloc"])
        cmd.append(self.config["input"]["model"]["gridOut"])
        cmd.append("IN3D." + self.site + ".x")
        cmd.append("awp." + self.site+ ".media")
        cmd.append("x")
        cmd.append(str(self.runID))
        cmd.append(self.site + "_fy_" + str(self.runID) + ".sgthead")
        cmd.append(str(self.config["compute"]["setup"]["frequency"]))
        cmd.append("-s " + str(self.config["compute"]["setup"]["sourceFrequency"]))

        self.lines.append(" ".join(cmd))

        # Save the script to disk
        self._saveScript()                 
        pass
    
    def postprocess(self):
        # Write to stage.txt
        with open(self.config["compute"]["restartFile"], 'w') as f:
            f.write('postX')
        
# Stage "postY" definition
class postY(scriptABC):
    # Script type
    type = "slurm"
        
    def build(self):
        # Obtain Header 
        self._getHeader()
        
        # Obtain Slurm rules
        r = self.config["compute"]["resources"]
        self._getSlurmRules(r["time"]/6, 1, 1, r["task-per-node"], r["qos"])
        
        # Additional instructions
        self.lines.append("module load python/2.7.16")
        self.lines.append("")        
        
        # Build command
        cmd = []
        cmd.append(self.config["input"]["cyberShake"]["path"] + "/AWP-GPU-SGT/utils/prepare_for_pp.py")
        cmd.append(self.site)
        cmd.append("comp_y/output_sgt/awp-strain-" + self.site + "-fy")
        cmd.append(self.site + "_fx_" + str(self.runID) + ".sgt")
        cmd.append(self.config["input"]["model"]["box"])
        cmd.append(self.site + "." + Workflow.OUTPUTS["coordfile"])
        cmd.append(self.site + "." + Workflow.OUTPUTS["fdloc"])
        cmd.append(self.config["input"]["model"]["gridOut"])
        cmd.append("IN3D." + self.site + ".y")
        cmd.append("awp." + self.site+ ".media")
        cmd.append("y")
        cmd.append(str(self.runID))
        cmd.append(self.site + "_fx_" + str(self.runID) + ".sgthead")
        cmd.append(str(self.config["compute"]["setup"]["frequency"]))
        cmd.append("-s " + str(self.config["compute"]["setup"]["sourceFrequency"]))                      

        self.lines.append(" ".join(cmd))

        # Save the script to disk
        self._saveScript()        
        pass
    
    def postprocess(self):
        # Write to stage.txt
        with open(self.config["compute"]["restartFile"], 'w') as f:
            f.write('postY')

# Stage "rupVar" definition
class rupVar(scriptABC):
    # Script type
    type = "sh"
        
    def build(self):
        # Obtain Header 
        self._getHeader()
        
        # Build command
        cmd = []
        self.lines.append("module load python/2.7.16")
        self.lines.append("")
        
        cmd.append(self.config["input"]["cyberShake"]["path"] + "/populate_rvs.py")
        cmd.append(str(self.config["input"]["cyberShake"]["gravesPitarka"]))
        cmd.append(str(self.config["input"]["ERF"]["id"]))
        cmd.append("1")
        cmd.append(self.config["input"]["database"]["path"])

        self.lines.append(" ".join(cmd))
                
        self.lines.append("mkdir post-processing")
        self.lines.append("cd post-processing")
        
        # Save the script to disk
        self._saveScript()
                    
        pass
    
    def postprocess(self):
        # Generate the rupture file list
        dal = DAL.SQLiteHandler(self.config["input"]["database"]["path"], True)
        dal.generateRuptureFile("post-processing/rupture_file_list_" + self.config["input"]["region"])
        
        # Write to stage.txt
        with open(self.config["compute"]["restartFile"], 'w') as f:
            f.write('rupVar')        
        
# Stage "runDS" definition
class runDS(scriptABC):
    # Script type
    type = "slurm"
                
    def build(self):
        # Obtain Header 
        self._getHeader()
        
        # Obtain Slurm rules
        r = self.config["compute"]["resources"]
        #self._getSlurmRules(r["time"], r["nodes"],
        #                    r["task-per-node"], r["cpus-per-task"], r["qos"])
        self._getSlurmRules(r["time"], r["nodes"],
                            int(r["task-per-node"]/2), r["cpus-per-task"]*2, r["qos"])
        
        # Additional instructions
        #self.lines.append("module load python/2.7.16")
            
        self.lines.append("module purge")
        self.lines.append("module load impi/2017.4")
        self.lines.append("module load gcc")
        self.lines.append("module load fftw")
        
        self.lines.append("ulimit -c unlimited")
        self.lines.append("")

        self.lines.append("cd post-processing/")
        self.lines.append("ln -s ../" + self.site + "_fx_" + str(self.runID) + ".sgt")
        self.lines.append("ln -s ../" + self.site + "_fy_" + str(self.runID) + ".sgt")
        self.lines.append("ln -s ../" + self.site + "_fx_" + str(self.runID) + ".sgthead")
        self.lines.append("ln -s ../" + self.site + "_fy_" + str(self.runID) + ".sgthead")

        cmd = []        
        cmd.append(self.config["input"]["cyberShake"]["path"] + "/make_lns.py")
        cmd.append("rupture_file_list_" + self.config["input"]["region"])
        cmd.append(self.config["input"]["ERF"]["ruptures"] + "Ruptures_erf" 
                   + str(self.config["input"]["ERF"]["id"]) + "/")
        self.lines.append(" ".join(cmd))
        self.lines.append("")

        # Obtain the site's location
        dal = DAL.SQLiteHandler(self.config["input"]["database"]["path"], True)
        lat, lon = dal.getSiteLocation(self.site)
                
        # Build command
        cmd = []
        cyberShake = self.config["input"]["cyberShake"]
        cmd.append(cyberShake["path"] + "/DirectSynth/direct_synth.py")
        cmd.append(cyberShake["gravesPitarka"])
        cmd.append("stat=" + self.site)
        cmd.append("slat=" + str(lat))
        cmd.append("slon=" + str(lon))
        cmd.append("sgt_handlers=84")
        cmd.append("run_id=" + str(self.runID))
        cmd.append("debug=1")
        cmd.append("max_buf_mb=512")
        cmd.append("rupture_spacing=uniform")
        cmd.append("ntout=3000")
        cmd.append("rup_list_file=rupture_file_list_" + self.config["input"]["region"])
        cmd.append("sgt_xfile=" + self.site + "_fx_" + str(self.runID) + ".sgt")
        cmd.append("sgt_yfile=" + self.site + "_fy_" + str(self.runID) + ".sgt")
        cmd.append("x_header=" + self.site + "_fx_" + str(self.runID) + ".sgthead")
        cmd.append("y_header=" + self.site + "_fy_" + str(self.runID) + ".sgthead")
        cmd.append("det_max_freq=" + str(self.config["compute"]["setup"]["frequency"]))
        cmd.append("stoch_max_freq=-1.0")
        cmd.append("run_psa=1")
        cmd.append("run_rotd=1")
        cmd.append("run_durations=1")
        cmd.append("dtout=0.1")
        cmd.append("simulation_out_pointsX=2")
        cmd.append("simulation_out_pointsY=1")
        cmd.append("simulation_out_timesamples=3000")
        cmd.append("simulation_out_timeskip=0.1")
        cmd.append("surfseis_rspectra_seismogram_units=cmpersec")
        cmd.append("surfseis_rspectra_output_units=cmpersec2")
        cmd.append("surfseis_rspectra_output_type=aa")
        cmd.append("surfseis_rspectra_period=all")
        cmd.append("surfseis_rspectra_apply_filter_highHZ=5.0")
        cmd.append("surfseis_rspectra_apply_byteswap=no")
        
        self.lines.append(" ".join(cmd))
        
        self.lines.append("cd ..")
        
        # Save the script to disk
        self._saveScript()
        
    def postprocess(self):
        # Write to stage.txt
        with open(self.config["compute"]["restartFile"], 'w') as f:
            f.write('runDS')
        
# Stage "db_insert" definition
class insertDB(scriptABC):
    # Script type
    type = "sh"
        
    def build(self):
        # Obtain Header 
        self._getHeader()
        
        # Additional instructions        
        self.lines.append("export CYBERCOMMANDS_JARS='"\
                         + self.config["input"]["cyberShake"]["path"]\
                         + "/CyberCommands/lib'")
        self.lines.append("")                         

        # Build command
        self.lines.append("n=0")
        self.lines.append('until [ "$n" -ge 10 ]')
        self.lines.append("do")

        cmd = []
        cmd.append(self.config["input"]["cyberShake"]["path"] + "/CyberCommands/bin/CyberLoadAmps_SC")
        cmd.append("-c -r -p post-processing")
        cmd.append("-run " + str(self.runID))
        cmd.append("-server sqlite:" + self.config["input"]["database"]["path"])
        cmd.append("-periods " + ",".join(str(x) for x in self.config["compute"]["setup"]["periods"]))

        self.lines.append(" ".join(cmd) + " && break")

        self.lines.append("   n=$((n+1))")
        self.lines.append("   sleep 15")
        self.lines.append("done")
        
        # Save the script to disk
        self._saveScript()
                
        pass
    
    def postprocess(self):
        # Write to stage.txt
        with open(self.config["compute"]["restartFile"], 'w') as f:
            f.write('insertDB')
        
# Stage "check_db" definition
class checkDB(scriptABC):
    # Script type
    type = "sh"
        
    def build(self):
        # Main path
        path = self.config["input"]["cyberShake"]["path"]
        # Obtain Header 
        self._getHeader()
        
        # Additional instructions        
        self.lines.append("set -o errexit")
        
        self.lines.append("cd " + path + "/db")
        self.lines.append("")
        
        # Build command
        cmd = []
        cmd.append("java -classpath .:sqlite-jdbc-3.27.2.1.jar:mysql-connector-java-5.0.5-bin.jar:commons-cli-1.0.jar CheckDBDataForSite")
        cmd.append("-s sqlite:" + self.config["input"]["database"]["path"])
        cmd.append("-r " + str(self.runID))
        cmd.append("-o check.out")
        cmd.append("-c rotd")
        cmd.append("-p " + ",".join(str(x) for x in self.config["compute"]["setup"]["periods"]))

        self.lines.append(" ".join(cmd))
        
        self.lines.append("")
        self.lines.append("cd -")
        
        # Save the script to disk
        self._saveScript()
                        
        pass
    
    def postprocess(self):
        # Inser Hazard dataset
        dal = DAL.SQLiteHandler(self.config["input"]["database"]["path"], True)
        
        # Obtain information for a given RunID
        runInfo = dal.getRunIDInfo(self.runID)
    
        # Insert a Hazard dataset
        erfID = runInfo['ERF_ID']
        rupVarScenarioID = runInfo['Rup_Var_Scenario_ID']
        SGTID = runInfo['SGT_Variation_ID']
        velModelID = runInfo['Velocity_Model_ID']
        dal.addHazardDataset(erfID, rupVarScenarioID, SGTID, velModelID, 
            self.config['compute']['setup']['frequency'])
        
        # Write to stage.txt
        with open(self.config["compute"]["restartFile"], 'w') as f:
            f.write('checkDB')
            
# Stage "curve_calc" definition
class curveCalc(scriptABC):
    # Script type
    type = "sh"
        
    def build(self):
        # Obtain Header 
        self._getHeader()
            
        # Build command
        cmd = []
        cmd.append("CYBERSHAKE_HOME=" + self.config["input"]["cyberShake"]["path"])
        cmd.append(self.config["input"]["cyberShake"]["path"] + "/OpenSHA/scripts/curve_plot_wrapper.sh")
        cmd.append(self.config["input"]["database"]["path"])
        cmd.append("--site " + self.site)
        cmd.append("--run-id " + str(self.runID))
        xmlPath = "/OpenSHA//opensha-cybershake/src/org/opensha/sha/cybershake/conf/MeanICERF.xml"
        cmd.append("--erf-file " + self.config["input"]["cyberShake"]["path"] + xmlPath)
        cmd.append("--period " + ",".join(str(x) for x in self.config["compute"]["setup"]["periods"]))        
        cmd.append("--output-dir " + os.path.abspath(".") + "/")
        cmd.append("--type pdf,png")
        cmd.append("--force-add")
        cmd.append("--cmp RotD50")
        
        self.lines.append(" ".join(cmd))
        
        # Save the script to disk
        self._saveScript()        
        
        pass
    
    def postprocess(self):
        # Write to stage.txt
        with open(self.config["compute"]["restartFile"], 'w') as f:
            f.write('curveCalc')
     
        
# Stage "cleanUp" definition
class cleanUp(scriptABC):
    # Script type
    type = "sh"
        
    def build(self):
        # Obtain Header 
        self._getHeader()
        
        # Additional instructions        
        self.lines.append("rm -fr *sgt")
        self.lines.append("rm -fr comp_*/output_sgt/*")          

        # Save the script to disk
        self._saveScript()
                
        pass
    
    def postprocess(self):
        # Write to stage.txt
        with open(self.config["compute"]["restartFile"], 'w') as f:
            f.write('cleanUp')
