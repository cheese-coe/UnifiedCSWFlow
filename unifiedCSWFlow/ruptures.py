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
import numpy as np
from tqdm import tqdm

from unifiedCSWFlow import DAL

################################################################################
# Methods and classes

# Rupture generator
class Ruptures():
    # Initialization method
    def __init__(self, opath, database):
        # Set the output path
        self.opath = opath
        
        # Initialze focal mechanism
        self.fm = {"dip": 0.0, "strike": 0.0, "rake": 0.0}
        
        # Create DB  sconnection        
        self.dal = DAL.SQLiteHandler(database, True)
        
    # Set the focal mechanism
    def setFocalMechanism(self, dip, strike, rake):
        self.fm = {"dip": dip, "strike": strike, "rake": rake}
        
    # Generate ruptures
    def generateRuptures(self, id, site):

        # Obtain all ruptures in the DB
        rows = self.dal.getRupturesDict()
        count = len(list(rows))
                
        # For each Rupture found
        pbar = tqdm(rows, desc='[' + site +'] Generating rupture files', total=count, position=id, leave=False)
        for row in pbar:
            # Creating output directory
            path = "%s/%s/%s" % (self.opath, row['Source_ID'], row['Rupture_ID']);
            if os.path.isdir(path):
               continue
            os.makedirs(path, exist_ok=True)

            # Building the filename
            file = "%s_%s.txt"% (row['Source_ID'], row['Rupture_ID'])

            #pbar.set_description("Generating rupture files --> %s/%s" % (path, file))
            #print("Generating file: " + path + "/" + file)

            # Generate the rupture
            with open(path + "/" + file, 'w') as rup:
                # Dumping header information
                rup.write("Probability = " + str(row['Prob']) + "\n")
                rup.write("Magnitude = " + str(row['Mag']) + "\n")
                rup.write("GridSpacing = " + str(row['Grid_Spacing']) + "\n")
                rup.write("NumRows = " + str(row['Num_Rows']) + "\n")
                rup.write("NumCols = " + str(row['Num_Columns']) + "\n")
                rup.write("#   Lat         Lon         Depth      Rake    Dip     Strike\n")

                ncols = int(row['Num_Columns'])-1
                nrows = int(row['Num_Rows'])-1

                #sphereDist = haversine((x2, y1), (x1, y2)) * 1000
                #print(str(round((sphereDist/200) + 0.5)) + " " + str(row['Num_Columns']))

                slat = float(row['Start_Lat'])
                elat = float(row['End_Lat'])
                slon = float(row['Start_Lon'])
                elon = float(row['End_Lon'])
                sdepth = float(row['Start_Depth'])
                edepth = float(row['End_Depth'])

                # Generate the 3 axis points
                x = self._generateAxis(slat, elat, 0, ncols, 1e-10)
                y = self._generateAxis(slon, elon, 0, ncols, 1e-10)
                z = self._generateAxis(sdepth, edepth, 0, nrows, 1e-2)

                # Write to file the ruptures
                s =  "    "
                for j in z:
                    for i in np.arange(0, ncols):
                        rup.write(str(x[i]) + s + str(y[i]) + s + str(j) + s + str(self.fm['rake'])\
                         + s + str(self.fm['dip']) + s + str(self.fm['strike']) + "\n")

            
    # Generate axis
    def _generateAxis(self, ini, end, offset, num, prec):
        # Calculate the step
        step = abs(end-ini) / num

        prec = 1/prec
        step = int(step *prec)/prec

        # Generate the axis
        if ini == end:
            axis = np.repeat(ini, num)
        elif ini < end:
            axis = np.arange(ini+offset, end, step)
        else:
            axis = np.arange(end+offset, ini, step)

        return axis        
            
    
    
        
    
        
        
    
          
    
