#   ConcurrentGeocode.py
# -*- coding: utf-8 -*-
#
#   Very simple example of using concurrent processing to geocode
#   a table with Pro & Next Generation StreetMap Premium
#
#
import arcpy
import os
import platform
import sys
import subprocess
import tempfile
import time
import traceback

#   Housekeeping
arcpy.env.overwriteOutput = True

#   Get the input parameters
inTable = r"C:\Users\b35143921880\Downloads\ConcurrentGeocode\Geocode_RAIS\RAIS.gdb\rais_2019"
inLocator = r"C:\StreetMap\NewLocators\BRA\BRA.loc"
procCount =  8
scratchDir = r"C:\Users\b35143921880\Downloads\scratch"
workerScript = r"C:\Users\b35143921880\Downloads\ConcurrentGeocode\Geocode_RAIS\Worker.py"
outFC = r"C:\Users\b35143921880\Documents\ArcGIS\Projects\MyProject4\MyProject4.gdb\rais_2019_output_geocode_streetmap"
           
#   Variables - Edit FieldInfo and field list to suit your data and locator
#   You can harvest fInfo from a manual run of Geocode Addresses GP tool
procList = list(range(procCount))
rowCount = int(arcpy.GetCount_management(inTable).getOutput(0))
inDesc = arcpy.Describe(inTable)
oidName = inDesc.OIDFieldName
fInfo = "'Address or Place' logradouro VISIBLE NONE;Address2 <None> VISIBLE NONE;Address3 <None> VISIBLE NONE;Neighborhood bairro VISIBLE NONE;City name_muni VISIBLE NONE;County <None> VISIBLE NONE;State uf VISIBLE NONE;ZIP cep VISIBLE NONE;ZIP4 <None> VISIBLE NONE;Country <None> VISIBLE NONE"
fList = ['OID@','logradouro','bairro','name_muni','uf','cep','id_estab','qt_vinc_ativos','code_muni','type_input_galileo'] # include any other fields desired
exprList = ['MOD({},{}) = {}'.format(oidName,procCount,i) for i in range(procCount)]


#   Make sure subprocesses inherit the right Python environment
pythonExe = os.path.join(sys.exec_prefix,'pythonw.exe')
os.environ['PYTHONPATH'] = r'C:\Program Files\ArcGIS\Pro\bin\Python\envs\arcgispro-py3\lib\site-packages'


#   Announcement!
arcpy.AddMessage("\nGeocoding " + str(rowCount) + " records")
startTime = time.time()


#   Create scratch workspaces (sequentially)
arcpy.AddMessage("\nCreating {} scratch workspaces...".format(procCount))
gdbList = []
for i in procList:
    gdbName = tempfile.mkdtemp(suffix='.gdb',prefix='part',dir=scratchDir)
    gdb = arcpy.CreateFileGDB_management(scratchDir,os.path.basename(gdbName)).getOutput(0)
    gdbList.append(gdb)


#   Create part tables (sequentially)
def MakePartTable(table,i):
    part = arcpy.CreateTable_management(gdbList[i],"PartTable",table).getOutput(0)
    return part
partList = [MakePartTable(inTable,i) for i in procList]


#   Populate part tables (concurrently)
icList = [arcpy.da.InsertCursor(partList[i],fList[1:]) for i in procList]
with arcpy.da.SearchCursor(inTable,fList) as cursor:
    for row in cursor:
        idx = row[0] % procCount
        icList[idx].insertRow(row[1:])
del icList


#   Geocode all part tables (concurrently) to make PartResult in each GDB
commands = []
for part in partList:  # double quotes preserve spaces in paths
    command = ' '.join(['"'+pythonExe+'"',
                        '"'+workerScript+'"',
                        '"'+part+'"',
                        '"'+inLocator+'"',
                        '"'+fInfo+'"'])
    commands.append(command)
processList = [subprocess.Popen(command,cwd=scratchDir) for command in commands]
duration, complete = 0, None not in [p.poll() for p in processList]
while not complete:
    time.sleep(5)
    duration += 5
    minutes, seconds = divmod(duration,60)
    if minutes == 0:
        arcpy.AddMessage('Waiting for process completion at {} seconds...'.format(seconds))
    elif minutes == 1:
        arcpy.AddMessage('Waiting for process completion at {} minute {} seconds...'.format(minutes,seconds))
    else:
        arcpy.AddMessage('Waiting for process completion at {} minutes {} seconds...'.format(minutes,seconds))
    complete = None not in [p.poll() for p in processList]


#   Merge results
arcpy.AddMessage('\nMerging results...')
results = [os.path.join(gdb,'PartResult') for gdb in gdbList
           if arcpy.Exists(os.path.join(gdb,'PartResult'))]
if results:
    arcpy.Merge_management(results,outFC)
    finalCount = int(arcpy.GetCount_management(outFC).getOutput(0))
else:
    finalCount = 0

    
#   Report and clean up
endTime = time.time()
rate = int(finalCount / (endTime - startTime) * 3600.0)
arcpy.AddMessage("\nGeocoded {} records at a net {} records per hour".format(finalCount,rate))
#
for gdb in gdbList:
    arcpy.Delete_management(gdb)

