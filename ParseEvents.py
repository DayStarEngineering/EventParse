##! /usr/bin/python
__author__ = 'Zach Dischner'
__copyright__ = "NA"
__credits__ = ["NA"]
__license__ = "NA"
__version__ = "1.0.0"
__maintainer__ = "Zach Dischner"
__email__ = "zach.dischner@gmail.com"
__status__ = "Dev"

"""
File name: ParseEvents.py
Authors: Zach Dischner
Created: 5/21/2014
Modified: 

This module facilitates the parsing of different event type files into Flexplan-compliant XML input products

Run this file on any of the input file types ["ECLIPSE","PHOTO","MANEUVER"...] and the resulting translation
into Flexplan-compliant XML will be generated. 

Have fun! 

@todos Get it to work...
"""

# -------------------------
# --- IMPORT AND GLOBAL ---
# -------------------------
import pandas as pd
import sys as sys
import numpy as np
import os, string, pickle, fnmatch
from datetime import datetime
import xml.etree.cElementTree as ET  # Great XLM library.
# Easy tutorial http://stackoverflow.com/questions/3605680/creating-a-simple-xml-file-using-python
from ZD_Utils import SpreadsheetUtils as zsheet
from ZD_Utils import XMLUtils as zxml

"""
Global Variables
@param pname:  Pickle filename storing unique ID incrementor 
@param in_date_format:  Input date format of string in CSV
@param xml_date_format:  Output date format of string for XML
"""
pName = "UniqueID.pickle"
## For datetime package. Use http://strftime.org/ for reference
in_date_format = '%Y/%m/%d_%H:%M:%S.%f'
xml_date_format = '%d-%b-%Y %H:%M:%S'

typical_in_format = '%d %b %Y %H:%M:%S.%f'

# Cool way to do it all functionally! 
# Can call with     eval(parsers[PLATFORM])(arg)
# Just not as pretty I don't think... but more dynamic! 
# parsers = dict((platform,func) for (platform,func) in [(platform,"parse"+platform) for platform in platforms])

def getNextUniqueID():
    """Fetch the next unique ID to use in event creation. Opens a pickle, gets value,
    increments, and saves the pickle. 
    
    Returns:
        uid:   New unique ID

    Example:
        id = ParseEvents.getNextUniqueID()
    """
    ###### Get last used Unique ID and increment
    global pName
    f = open(pName, 'r')
    uid = pickle.load(f) + 1
    f.close()
    f = open(pName, 'w')
    pickle.dump(uid,f)
    f.close()
    return uid


def convertTimeFormat(timestamp, input_format=typical_in_format, output_format=xml_date_format, to_string=True, from_string=False):
    """Converts time format to be Flexplan-compliant

    Args:
        timestamp:  String input time

    Kwargs:
        input_format:   Input format string. See http://strftime.org
        output_format:  Output format string. See http://strftime.org
        to_string:      Boolean, return string or datetime object
        from_string:    Boolean, input is already a datetime object. Returns formatted string
    
    Returns:
        timestamp:  Reformatted time string

    Example:
        datestr = ParseEvents.convertTimeFormat(dataframe.StartTime.iloc[0])
        datestr_local = ParseEvents.convertTimeFormat(dataframe.StartTime.iloc[0],output_format='%c')
        dateval = ParseEvents.convertTimeFormat(dataframe.StartTime.iloc[0],to_string=False)
    """
    if from_string is True:
        print timestamp
        return datetime.strftime(timestamp,output_format)

    if to_string is True:
        return datetime.strftime(datetime.strptime(timestamp,input_format),output_format)
    else:
        return datetime.strptime(timestamp,input_format)

#http://norwied.wordpress.com/2013/08/27/307/
# DONT REALLY UNDERSTAND THIS. IT MUST BE CALLED IN YOUR MODULE TO WORK...
def indent(elem, level=0):
    """ Makes ElementTree XML object pretty for printing

    Args:
        elem:  object to prettify

    Kwargs:
        level: Internally used

    Example:
        root = ET.ElementTree("SomeFile.xml")
        XML_Utils.indent(root)
    """
    i = "\n" + level*"    "
    if len(elem):
        if not elem.text or not elem.text.strip():
            elem.text = i + "    "
        if not elem.tail or not elem.tail.strip():
            elem.tail = i
        for elem in elem:
            indent(elem, level+1)
        if not elem.tail or not elem.tail.strip():
            elem.tail = i
    else:
        if level and (not elem.tail or not elem.tail.strip()):
            elem.tail = i

def generateXMLHeader(startTime,stopTime,filename):
    """Generates an XML handle with header information filled in

    Args:
        startTime:  UTC starttime marking the start of events
        stopTime:   UTC stoptime marking the end of events
        filename:   Filename of the input event file
    
    Returns:
        root:   Etree XML root object. Manipulate later. 

    Example:
        df = pd.read_csv("filename.csv")
        root = ParseEvents.generateXMLHeader(df, "foobar.csv")
    """
    ###### Create the XML root object
    root = ET.Element("FDF_to_FP")

    ###### Create all of the header elements
    fname_elem = ET.SubElement(root,"FILENAME")
    fname_elem.text = string.replace(filename, "csv", "xml").split('/')[-1]
    date_elem = ET.SubElement(root,"CREATION_DATE")
    date_elem.text = datetime.strftime(datetime.utcnow(),xml_date_format)
    ## Start time is the first start time in the file
    start_elem = ET.SubElement(root,"START")
    start_elem.text = startTime
    ## End time is the last stop time in the file
    end_elem = ET.SubElement(root,"END")
    end_elem.text = stopTime
    ## Some unused/Empty elements 
    sve_element = ET.SubElement(root,"STATE_VECTOR_EPOCH")
    sv_element = ET.SubElement(root,"STATE_VECTOR")
    return root

def getStartStopTimes(dataframe, as_string=True, input_format=typical_in_format):
    """Finds the erliest start time and latest stop time in a dataframe

    Args:
        dataframe:  dataframe containing event info. Assumes 'Start' and 'Stop' column

    Kwargs:
        as_string:  Return the times in a formatted string (xml format for now)
    
    Returns:
        startTime:   Earliest event start time
        stopTime:    Latest event stop time

    Example:
        df = pd.read_csv("filename.csv", names = ['Start','Stop',...])
        t1,t2 = ParseEvents.getStartStopTimes(df)
    """
    startTime = min([convertTimeFormat(xx,to_string=False,input_format=input_format)
                        for xx in dataframe.Start.loc[~dataframe.Start.isnull()].tolist()])    
    stopTime = max([convertTimeFormat(xx,to_string=False,input_format=input_format) 
                        for xx in dataframe.Stop.loc[~dataframe.Stop.isnull()].tolist()])    
    if as_string is True:
        ## Format datetime objects as strings
        startTime = convertTimeFormat(startTime,from_string=True)
        stopTime = convertTimeFormat(stopTime,from_string=True)
    return startTime,stopTime


def getPairedEventFiles(filename, cols=None):
    """Loads event files that come in pairs (for each satellite, etc) into a single dataframe

    Args:
        filename:  First filename. Assumes 'SAT1' and 'SAT2' are interchangeable in file naming convention
        Will work on either filename

    Kwargs:
        cols:  override column names as list
    
    Returns:
        df:         Combined dataframe
        combo_fn:   New filename with 'SAT*' removed. 

    Example:
        df = ParseEvents.getPairedEventFiles('SAT1_Event.csv')
    """
    df1 = zsheet.import_csv(filename, header=0, names=cols)

    ######  Files come in SAT1 SAT2 Pairs
    sats = ["SAT1","SAT2"]
    if fnmatch.fnmatch(filename,'*SAT1*'):
        ## Swap filename
        filename2 = string.replace(filename,'SAT1','SAT2')
    else:
        ## Swap filename
        filename2 = string.replace(filename,'SAT2','SAT1')

    df2 = zsheet.import_csv(filename2, header=0, names=cols)

    ###### Append the two dataframes, with a satellite column added
    sat1 = filename.split("/")[-1].split("_")[1]
    df1['Sat'] = sat1
    ## Switch sats
    df2['Sat'] = sats[(sat1 == sats[0])-2]

    df = df1.append(df2).dropna()

    combo_fn=string.replace(string.replace(filename,'SAT1_',''),'SAT2_','').split('/')[-1]

    return df,combo_fn


def parseCSV(filename):
    """Parses a CSV Event File

    Args:
        filename:   Filename of the input event file
    
    Returns:
        root:   Etree XML root object. Manipulate later. 
        df:     Pandas Dataframe of the Data
    Example:
        root,df = ParseEvents.parseCSV("filename.csv")
    """
    ###### Define Dynamic Parsers Function Caller
    platforms = ["COMM","ECLIPSE","MANEUVER","MEMORY","PHOTO"]
    parseFuncs = [parseCOMM, parseECLIPSE, parseMANEUVER, parseMEMORY, parsePHOTO]
    # reformatFuncs = [formatCOMM, formatECLIPSE, formatMANEUVER, formatMEMORY, formatPHOTO]
    parsers = dict(zip(platforms,parseFuncs))
    # formatters = dict(zip(platforms,reformatFuncs))

    platform = filename.split('/')[-1].split('_')[0]

    ## Dynamically call function from dictionary
    root,csv_filename,df = parsers[platform](filename)

    ###### Write the output XML
    indent(root)
    xml_tree = ET.ElementTree(root)
    out_filename = 'Output/' + string.replace(csv_filename,'csv','xml')
    xml_tree.write(out_filename, xml_declaration=True, method="xml")
    return root,df

def createEventElement(xmlroot,entities,subname='Event', override_keys=None):

    subEl = ET.SubElement(xmlroot,subname)

    ######Can loop by keys since I made the input dictionary keys match XML fields!
    if override_keys is None:
        keys = entities.keys()
    else:
        keys = override_keys
    for key in keys: 
        val = entities[key]
        if val is not None:
            if type(val) is dict:
                subEl = createEventElement(subEl,val,subname=key)
            elif type(val) is str:
                subelement = ET.SubElement(subEl,key)
                subelement.text = entities[key]
            elif type(val) is list:
                for v in val:
                    if type(v) is dict:
                        subEl = createEventElement(subEl,v,subname=key)
                    elif type(v) is str:
                        subelement = ET.SubElement(subEl,key)
                        subelement.text = v
            else:
                raise error('Uh Oh, Need to write a STRING type to XML!')
        else:
            #Just create empty field
            subelement=ET.SubElement(subEl,key)

    """
    Basically does this whole process, just recursively and awesomely

    ###### Form Start Time Element
    utc_start_elem = ET.SubElement(event,'UTC_Start_Time')
    if entities['UTC_Start_Time']: utc_start_elem.text = entities['UTC_Start_Time']

    ###### Form Duration Element (in miliseconds)
    utc_start_elem = ET.SubElement(event,'Duration')
    utc_start_elem.text = entities['Duration']

    ###### Form Unique ID Element
    utc_start_elem = ET.SubElement(event,'Unique_Id')
    utc_start_elem.text = entities['Unique_Id']

    ###### Form Event Description Element 
    utc_start_elem = ET.SubElement(event,'Event_Description')
    utc_start_elem.text = entities['Event_Description']

    ###### Form Sat Element
    utc_start_elem = ET.SubElement(event,'Sat')
    utc_start_elem.text = entities['Sat']

    ###### Form Empty Entity element
    entity_elem = ET.SubElement(event,'Entity')

    ###### Form Event Parameters Element
    params_elem = ET.SubElement(event,'List_of_Event_Parameters')

    ###### Form Parameter Subelements
    if entities['List_of_Event_Parameters']:
        param_ul_elem = ET.SubElement(params_elem,'Event_Parameter')
        dl_name_elem = ET.SubElement(param_ul_elem,'Event_Par_Name')
        dl_name_elem.text=entities['List_of_Event_Parameters'][0][0]
        dl_val_elem = ET.SubElement(param_ul_elem,'Event_Par_Value')
        dl_val_elem.text = entities['List_of_Event_Parameters'][1][0]

        param_dl_elem = ET.SubElement(params_elem,'Event_Parameter')
        ul_name_elem = ET.SubElement(param_dl_elem,'Event_Par_Name')
        ul_name_elem.text=entities['List_of_Event_Parameters'][0][1]
        ul_val_elem = ET.SubElement(param_dl_elem,'Event_Par_Value')
        ul_val_elem.text = entities['List_of_Event_Parameters'][1][1]

    ###### Yay Finished! 
    return xmlroot
    """
    return xmlroot


def parseCOMM(filename):
    comm_date = '%Y/%m/%d_%H:%M:%S.%f'
    ###### Load The dataframe
    df = zsheet.import_csv(filename, header=0).dropna()


    ###### Find Start and Stop Times
    startTime,stopTime = getStartStopTimes(df,input_format=comm_date)

    ###### Generate Header 
    root = generateXMLHeader(startTime,stopTime,filename)

    ###### Parse dataframe to add individual events
    csv_filename = filename.split('/')[-1]

    ###### Iterate over COMM dataframe (each row of events)
    for idx,row in df.iterrows():
        utcStart =convertTimeFormat(row.Start,input_format=comm_date)
        duration = str((convertTimeFormat(row.Stop,to_string=False,input_format=comm_date) - 
                    convertTimeFormat(row.Start,to_string=False,input_format=comm_date)).total_seconds()*1e3)
        uid = str(getNextUniqueID())
        descr = "COMM"
        sat = row.Groups
        param_names = ['COMM_SET_DL_RATE','COMM_SET_UL_RATE']
        param_values = ['DL','UL']

        entity_names = ['UTC_Start_Time','Duration','Unique_Id','Event_Description','Sat',
                            'Entity','List_of_Event_Parameters']

        # param_dict = dict(zip(['Event_Par_Name','Event_Par_Value'],[param_names,param_values]))
        # event_params = {'Event_Parameter':param_dict}
        event_params = {'Event_Parameter':[{'Event_Par_Name':param_names[i],'Event_Par_Value':param_values[i]} for i in np.arange(len(param_names))]}

        entity_values = [utcStart,duration,uid,descr,sat,None,None]

        entities = dict(zip(entity_names,entity_values))
        root = createEventElement(root,entities,override_keys=entity_names) # Override to preserve order in xml
        print "This is wrong? There is no specifier for NULL/UL/DL parameters"

    csv_filename = filename.split('/')[-1]

    return root, csv_filename, df
        
        
def parseECLIPSE(filename):

    ###### Load The dataframe
    df,combo_filename = getPairedEventFiles(filename,cols=["Start","Stop","Duration"])

    ###### Find Start and Stop Times
    startTime,stopTime = getStartStopTimes(df)

    root = generateXMLHeader(startTime,stopTime,combo_filename)

    ###### Iterate over COMM dataframe (each row of events)
    for idx,row in df.iterrows():
        utcStart =convertTimeFormat(row.Start)
        duration = str((convertTimeFormat(row.Stop,to_string=False) - 
                    convertTimeFormat(row.Start,to_string=False)).total_seconds()*1e3)
        duration = str(row.Duration*1e3)
        uid = str(getNextUniqueID())
        descr = "ECLIPSE"
        sat = row.Sat
        param_names = None
        param_values = None

        entity_names = ['UTC_Start_Time','Duration','Unique_Id','Event_Description','Sat',
                            'Entity','List_of_Event_Parameters']
        event_params = None #{'Event_Parameter':[{'Event_Par_Name':param_names[i],'Event_Par_Value':param_values[i]} for i in np.arange(len(param_names))]}

        entity_values = [utcStart,duration,uid,descr,sat,None,event_params]

        entities = dict(zip(entity_names,entity_values))
        root = createEventElement(root,entities,override_keys=entity_names) # Override to preserve order in xml
        
    # csv_filename = string.replace(string.replace(filename.split('/')[-1],'SAT1_',''),'SAT2_','')
    return root,combo_filename,df

def parseMANEUVER(dataframe, xmlroot, filename):
    return 0

def parseMEMORY(dataframe, xmlroot, filename):
    return 0

def parsePHOTO(dataframe, xmlroot, filename):
    return 0

def formatCOMM(dataframe, filename=None):
    return 0

def formatECLIPSE(dataframe, filename=None):
    return 0

def formatMEMORY(dataframe, filename=None):
    return 0

def formatMANEUVER(dataframe, filename=None):
    return 0

def formatPHOTO(dataframe, filename=None):
    return 0





# ------------
# --- cmd_to_xml---
# ------------
def event_to_xml(df_h, df_d, root, platform = "SAT2", mode='w', pretty=True):
    """This function turns a command dataframe (split by header/data rows) into a printable XML format
    Run: 'help(cmd_to_xml)' in interactive mode to print this statement out
    Input:  *df_h   - Header row dataframe taken from command spreadsheet
            *df_d   - Data rows dataframe taken from command spreadsheet. Contains Argument and format specification
            *(filename) - if provided, write the XML output to a file given in this variable.
            *(mode)     - The mode of the file to write xml output to. Default is 'w', change to 'a' to append
                --NOTE I ADDED APPENDING TO THE ETREE SOURCE CODE. WON'T EXIST NORMALLY...
    Returns:*cmd_xml    - xml Etree Object
    """

    # Root XML Element
    #root = ET.Element("CMD_TO_FP")

    command = ET.SubElement(root, "Command")

    ###### Spacecraft Element
    sc = ET.SubElement(command, "SpacecraftID")
    sc.text = str(platform)

    ###### Command Element
    command_name = ET.SubElement(command, "CommandName")
    command_name.text = df_h["COMMAND_MNEMONIC"]
    #print df_h["COMMAND_MNEMONIC"]

    ###### Filter Element (empty for now)
    command_filter = ET.SubElement(command, "Filter")
    # Eventually [for r in resources...]
    resource = ET.SubElement(command_filter, "Resource")
    resource.set("name", "SolarBattery")

    intervals = ET.SubElement(resource, "ConsumeInterval")
    # Eventually [for interval in intervals...]
    consumeStart = ET.SubElement(intervals, "ConsumeStart")
    consumeStart.text = str(0)
    consumeStop = ET.SubElement(intervals, "ConsumeStop")
    consumeStop.text = str(0)

    ###### Must have a command argument
    commandArgument = ET.SubElement(command, "CommandArgument")






    ## Split into arguments
    #Get row indices of unique arguments
    arg_rows = np.array(df_d.groupby("ARG_DESCRIPTION").groups.values()).squeeze()
    if arg_rows.size > 0:

        ###### Command Argument Elements
        
        arg_rows.sort(axis=0)                       # Sort to be ascending  [30,20,10] ==> [10,20,30]
        # Need to do this in a dumb way because single element numpy arrays can't be accessed with arg_rows[0]
        # http://stackoverflow.com/questions/9814226/error-extracting-element-from-an-array-python
        arg_rows = np.append(arg_rows, -1)  # Append -1 to make indexing easier (have to re-set it in a minute)
        arg_rows = np.subtract(arg_rows, arg_rows[0])  # Start index at 0, relative indexing of dataframe ==>[0,10,20]
        arg_rows[-1] = df_d.shape[0]


        idx_start = arg_rows[0]
        while idx_start != len(arg_rows)-1:
            d = df_d.iloc[arg_rows[idx_start]:arg_rows[idx_start+1]]

            # Indiviual Agrument
            # arg = ET.SubElement(args, "Argument")
            # Description Max and Min Range Value
            # desc = ET.SubElement(arg, "DESCRIPTION")
            # desc.text = str(d["ARG_DESCRIPTION"].iget_value(0))

            # # CHECK FOR NAN!!!
            # if pd.isnull(d["ARG_DATA_RANGE_LOW"].iget_value(0)) == False:
            #     min_val = ET.SubElement(arg, "MIN_VALUE")
            #     min_val.text = str(d["ARG_DATA_RANGE_LOW"].iget_value(0))

            # if pd.isnull(d["ARG_DATA_RANGE_HIGH"].iget_value(0)) == False:
            #     max_val = ET.SubElement(arg, "MAX_VALUE")
            #     max_val.text = str(d["ARG_DATA_RANGE_HIGH"].iget_value(0))

            # Mnemonics (Arguments)
            # Get dataframe of non-null memonic rows
            mnemonics = d.ARG_MNEMONIC[pd.notnull(d.ARG_MNEMONIC)]
            values = d.ARG_MNEMONIC_VALUE
            values = values[pd.notnull(values)]

            if len(values) > 0:
                for row_idx in range(len(values)):
                    # arg = ET.SubElement(command, "CommandArgument")
                    arg = ET.SubElement(commandArgument, "Argument")
                    # name, type, value are not subelements, but attributes
                    arg.set("name", str(mnemonics.iloc[row_idx]))
                    arg.set("type", "DOUBLE")
                    arg.set("value", str(values.iloc[row_idx]))
                    # mem_d = ET.SubElement(mem, "name")
                    # mem_d.text = str(mnemonics.iloc[row_idx])

                    # mem_t = ET.SubElement(mem, "type")
                    # mem_t.text = "DOUBLE"

                    # mem_v = ET.SubElement(mem, "value")
                    # mem_v.text = str(values.iloc[row_idx])
            idx_start += 1

    ###### Write xml or return
    if pretty:
        indent(root)

    return root




if __name__ == "__main__":
    # If no arguments
    if len( sys.argv ) < 2:
        fname = "Input/COMM_20120717000000_20120719000000_20140604114400_V1.csv"
    else:
        fname = sys.argv[1]

    data = zsheet.import_csv(fname, header=0)

    












