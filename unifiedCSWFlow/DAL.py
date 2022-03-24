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
import sqlite3
from datetime import datetime

################################################################################
# Methods and classes

def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

# SQLite handler definition
class SQLiteHandler():
    
    # Initialization method
    def __init__(self, path, dict_factory = False):
        self.connection = sqlite3.connect(path, check_same_thread=False)
        
        if dict_factory:
            self.connection.row_factory = self._dict_factory
        
        self.cursor = self.connection.cursor()
        
    # Obtain a valid Run Identifier
    def getValidRunId(self):
        # Define the query
        query = "select max(Run_ID) from CyberShake_Runs;"
        self.cursor.execute(query);
        
        # Obtain the RunId
        row = self.cursor.fetchone()

        # Just check if the DB is empty
        runid = 1
        if row[0]:
            runid = runid +row[0]
            
        return runid

    # Insert a row onto the Runs table
    def addRunInfo(self, runID, siteID, erfID, modelID, sfreq, freq):
        
        # Define the query
        query = "insert into CyberShake_Runs(Run_Id, Site_ID, ERF_ID, SGT_Variation_ID, Velocity_Model_ID,\
 Rup_Var_Scenario_ID, Status, Status_Time, Last_User, Max_Frequency, Low_Frequency_Cutoff,\
 SGT_Source_Filter_Frequency) values ("
 
        # Date
        date = datetime.now().strftime("%Y-0%m-%d %H:%M:%S")
        
        # Build the rest of the query
        query = query + str(runID) + ', ' + str(siteID) + ', ' + str(erfID) + ', 1, ' + str(modelID)
        query = query + ', 1, "SGT Started", "' + date + '", "BSC", ' 
        query = query + str(freq) + ', ' + str(freq) + ', ' + str(sfreq) + ');'
        
        # Execute query
        try:
            self.cursor.execute(query);
        except sqlite3.IntegrityError:
            pass        
        
        # Commit the transaction
        self.connection.commit()

    # Obtain a site ID given its name (or shortname)
    def getSiteID(self, site):
        
        # Define the query
        query = 'select CS_Site_ID from CyberShake_Sites where CS_Site_Name=="' + site \
                + '" or CS_Short_Name=="' + site + '";'

        # Execute query
        return self._runQuery(query)
    
    # Obtain geographical position for a given site    
    def getSiteLocation(self, site):
        
        # Define the query
        query = 'select CS_Site_Lat, CS_Site_Lon from CyberShake_Sites where CS_Site_Name=="' + site \
                + '" or CS_Short_Name=="' + site + '";'

        # Execute query
        self.cursor.execute(query);
        
        # Obtain the RunId
        row = self.cursor.fetchone()

        return row['CS_Site_Lat'], row['CS_Site_Lon']
    
    # Obtain a site ID given its name (or shortname)
    def getSiteShortName(self, site):
        
        # Define the query
        query = 'select CS_Short_Name from CyberShake_Sites where CS_Site_Name=="' + site \
                + '" or CS_Short_Name=="' + site + '";'

        # Execute query
        return self._runQuery(query)
        
    # Obtain a site ID given its name (or shortname)
    def getModelID(self, model):
        
        # Define the query
        query = 'select Velocity_Model_ID from Velocity_Models where Velocity_Model_Name=="' + model \
                + '";'

        # Execute query
        return self._runQuery(query)

    # Obtain a ERF ID given its name (or shortname)
    def getERFID(self, erf):
        # Define the query
        query = 'select ERF_ID from ERF_IDs where ERF_Name like "%' + erf + '%";'
        
        # Execute query
        return self._runQuery(query)

    # Obtain the ruptures from the DB
    def getRupturesDict(self):
        # Define the query
        query = 'select * from Ruptures;'

        # Execute query
        self.cursor.execute(query);
        
        # Obtain the RunId
        row = self.cursor.fetchall()

        return row


    # Obtain information for a given RunID
    def getRunIDInfo(self, runID):        
        # Define the query        
        query = "select ERF_ID, Rup_Var_Scenario_ID, SGT_Variation_ID, Velocity_Model_ID\
         from CyberShake_Runs where Run_ID=" + str(runID)
         
        # Execute query
        self.cursor.execute(query);        
        
        # Obtain the RunId
        row = self.cursor.fetchone()

        return row
        
    # Insert a hazard dataset
    def addHazardDataset(self, erfID, rupVarScenarioID, SGTID, velModelID, freq):
        
        # Define the query
        query = "insert into Hazard_Datasets (ERF_ID, Rup_Var_Scenario_ID, SGT_Variation_ID,\
 Velocity_Model_ID, Prob_Model_ID, Time_Span_ID, Max_Frequency, Low_Frequency_Cutoff) values ("
         
        # Build the rest of the query
        query = query + str(erfID) + ', ' + str(rupVarScenarioID) + ', ' + str(SGTID) + ', ' \
                + str(velModelID) + ', 1, 1, ' + str(freq) + ', ' + str(freq) + ');'

        # Execute query
        try:
            self.cursor.execute(query);
        except sqlite3.IntegrityError:
            pass        
        
        # Commit the transaction
        self.connection.commit()
    
    # Generate a rupture file
    def generateRuptureFile(self, file):
        # Define the query
        query = 'select r.ERF_ID, rv.Rup_Var_Scenario_ID, r.Source_ID, r.Rupture_ID,\
 count(*) as Count, r.Num_Points, r.Mag from Ruptures as r inner join Rupture_Variations as rv on\
 r.Source_ID=rv.Source_ID and r.Rupture_ID = rv.Rupture_ID group by r.Source_ID, r.Rupture_ID;'
 
        # Execute query
        self.cursor.execute(query);
         
        # Obtain the RunId
        rows = self.cursor.fetchall()
    
        # Obtain the number of rows
        count = len(list(rows))
        
        # Generate the rupture file list
        with open(file, 'w') as rup:
            # Dumping header information
            rup.write(str(count) + "\n")
            
            # For each row
            for r in rows:
                rup.write("e" + str(r["ERF_ID"]) + "_rv" + str(r["Rup_Var_Scenario_ID"]) + "_" \
                + str(r["Source_ID"]) + "_" + str(r["Rupture_ID"]) + ".txt " + str(r["Count"])\
                + " 1 " + str(r["Num_Points"]) + " " + str(r["Mag"]) + "\n")
        pass
        
    # Import data for a directory contaning a .csv per table
    def importData(self, path):
        # Define the query
        queryHeader = "insert into "
        
        # For each file in the provided path
        for file in os.listdir(path):
            queryWhere = os.path.splitext(os.path.basename(file))[0]
                        
            # Read the file
            with open(path + "/" + file, 'r') as f:
                lines = f.read().splitlines()
                
            queryWhat = " (" + lines[0] + ")"
            
            # Buidl and insert the current tuple
            for values in lines[1:]:
                # Build query
                values = values.replace(",", "','")
                query = queryHeader + queryWhere + queryWhat + " values ('" + values + "');"
                                                
                # Execute query
                try:
                    self.cursor.execute(query);
                except sqlite3.IntegrityError:
                    pass
                    
            # Commit the transaction
            self.connection.commit()
                            
        
    # Query the DB
    def _runQuery(self, query):
        # Execute query
        self.cursor.execute(query);
        
        # Obtain the RunId
        row = self.cursor.fetchone()                
        
        # Just check if the DB is empty
        return row[0] if row else None

    # Convert results to 'dict' type
    def _dict_factory(self, cursor, row):
        d = {}
        for idx, col in enumerate(cursor.description):
            d[col[0]] = row[idx]
        return d        
        
   # Deleting
    def __del__(self):
        # DB Conenction is closed when the object is removed
        self.connection.close()
