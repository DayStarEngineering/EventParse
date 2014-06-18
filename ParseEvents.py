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

@todos 
    *Rework the getPairedEventFiles() method, kinda clugy now.
    *Integrate simpler row parser, lots of repeated code within parse`EVENT` methods. Should be quick 
"""

# -------------------------
# --- IMPORT AND GLOBAL ---
# -------------------------
import pandas as pd
import sys as sys
import numpy as np
import os, string, pickle, fnmatch, operator
from datetime import datetime
import xml.etree.cElementTree as ET  # Great XLM library.
# Easy tutorial http://stackoverflow.com/questions/3605680/creating-a-simple-xml-file-using-python
from ZD_Utils import SpreadsheetUtils as zsheet
from ZD_Utils import XMLUtils as zxml

"""
Global Variables
@param pname:  Pickle filename storing unique ID incrementor 
@param typical_in_format:  Input date format of string in CSV
@param xml_date_format:  Output date format of string for XML
"""
pName = "UniqueID.pickle"
## For datetime package. Use http://strftime.org/ for reference
xml_date_format = '%d-%b-%Y %H:%M:%S'
typical_in_format = '%d %b %Y %H:%M:%S.%f'


def getNextUniqueID():
    """Fetch the next unique ID to use in event creation. Opens a pickle, gets value,
    increments, and saves the pickle. 
    
    Returns:
        uid:   New unique ID

    Examples:
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

    Examples:
        datestr = ParseEvents.convertTimeFormat(dataframe.StartTime.iloc[0])
        datestr_local = ParseEvents.convertTimeFormat(dataframe.StartTime.iloc[0],output_format='%c')
        dateval = ParseEvents.convertTimeFormat(dataframe.StartTime.iloc[0],to_string=False)
    """
    if from_string is True:
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

    Examples:
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

    Examples:
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
        as_string:      Return the times in a formatted string (xml format for now)
        input_format:   Specify input datestring format instead of using default
    
    Returns:
        startTime:   Earliest event start time
        stopTime:    Latest event stop time

    Examples:
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
    """Loads event files that come in pairs (for each satellite, etc) into a single dataframe. 
    Assumes the Start Times and Stop times will match up. I.E.
        PHOTO_SAT1_STARTTIME_STOPTIME_**
        is paired with:
        PHOTO_SAT2_STARTTIME_STOPTIME_**

    Args:
        filename:  First filename. Assumes 'SAT1' and 'SAT2' are interchangeable in file naming convention
        Will work on either filename

    Kwargs:
        cols:  override column names as list
    
    Returns:
        df:         Combined dataframe
        combo_fn:   New filename with 'SAT*' removed. 

    Examples:
        df = ParseEvents.getPairedEventFiles('SAT1_Event.csv')

    Todo:
        Rewrite. Logic is kinda lame, not flexible. Just had to do it quickly
    """
    ###### Load this file no matter what
    df1 = zsheet.import_csv(filename, header=0, names=cols)
    in_dir = filename.split('/')[0]
    in_file = filename.split('/')[1]
    first_sat = filename.split("/")[-1].split("_")[1]
    df1['Sat'] = first_sat

    ###### Try to find a matching file for the other platform
    swapper={'SAT1':'SAT2','SAT2':'SAT1'}
    swapsat=swapper[first_sat]
    ## Look through files
    files = os.listdir(in_dir)

    ###### Extract just the [Event,Platform, Start, Stop] from filename. (More flexible for the future, can just get specific elements)
    file_keys = [list(operator.itemgetter(0,1,2,3)(f.split('_'))) for f in os.listdir(in_dir)]

    ###### See if the expected file exists
    expected_keys = list(operator.itemgetter(0,1,2,3)(in_file.split('_')))
    expected_keys[1] = swapsat

    ## Switch to numpy arrays for logical indexing, return list of matching files
    swapfiles = list(np.array(files)[np.array([keys==expected_keys for keys in file_keys])])
    swapfilenames = [in_dir + '/' + f for f in swapfiles]

    print "Orig filename: ", filename
    print "match filename: ", swapfilenames

    if len(swapfilenames) == 0:
        print 'NO MATCHES FOUND FOR FILENAME:',filename
        return df1,filename

    ## Probably want to eventually loop over all matches, not for now since that shouldn't happen
    df2 = zsheet.import_csv(swapfilenames[0], header=0, names=cols)

    ###### Append the two dataframes, with a satellite column added
    
    ## Switch sats
    df2['Sat'] = swapsat

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
    Examples:
        root,df = ParseEvents.parseCSV("filename.csv")
    """
    ###### Define Dynamic Parsers Function Caller
    # Cool way to do it all functionally! 
    # You can also call with     eval(parsers[PLATFORM])(arg)
    # Just not as pretty I don't think... but more dynamic! Then your parsers struct can contain string function calls,
    #   which is nice if you don't have the function handles defined yet.
    # parsers = dict((platform,func) for (platform,func) in [(platform,"parse"+platform) for platform in platforms])
    platforms = ["COMM","ECLIPSE","MANEUVER","MEMORY","PHOTO"]
    parseFuncs = [parseCOMM, parseECLIPSE, parseMANEUVER, parseMEMORY, parsePHOTO]
    parsers = dict(zip(platforms,parseFuncs))

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
    """Creates an ETree XML element. Home cooked solution to dynamically convert a 
    dictionary of key-value pairs into xml key-value pairs. Logic to handle lists, empty
    elements, and sub-dictionaries. File recursively calls itself, so that is cool. Won't handle really complex
    xml forms yet.

    Args:
        xmlroot:    Etree root element uder which to append xml sub-elements. 
        entities:   Dictionary of key-value entities. 

    Kwargs:
        subname:        Xml element under which to nest this subelement.
        override_keys:  List of dictionary keys to use instead of those provided by the dict.keys() function. 
        Purpose is to allow selective element usage, and order specification. the keys() method returns keys in 
        a seemingly random order. 
    
    Returns:
        No returns, modifies the Etree object in place

    Examples:
        root = ET.Element('EXAMPLE')
        xml_entity = None
        entity_keys = ['FOO','BAR','BRO']
        entity_values = [1,2,{foobar:71,purpose:'worthless'}]
        entities = dict(zip(entity_names,entity_values))
        root = createEventElement(root,entities,override_keys=entity_names)
        # now print or write the root element to a file...
    """
    subEl = ET.SubElement(xmlroot,subname)

    ######Can loop by keys since I made the input dictionary keys match XML fields!
    if override_keys is None:
        keys = entities.keys()
        keys.reverse()      # Quick hack to get event param order right. Rethink to pass recursive override keys
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
    """Converts COMM csv into an xml file for Flexplan ingestion
    Intent is for this menthod to be dynamically called from `parseCSV()`

    Args:
        filename:   Filename of the input event file
    
    Returns:
        root:           Etree XML root object. Manipulate later.
        csv_filename:   Output Filename to rename xml file as
        df:             Pandas Dataframe of the COMM Data
    Examples:
        root,df = ParseEvents.parseCOMM("COMM_filename.csv")
    """
    print "\nNow Parsing COMM file"
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

        xml_entity = None
        entity_values = [utcStart,duration,uid,descr,sat,xml_entity,event_params]

        entities = dict(zip(entity_names,entity_values))
        root = createEventElement(root,entities,override_keys=entity_names) # Override to preserve order in xml
        print "This is wrong? There is no specifier for NULL/UL/DL parameters"

    csv_filename = filename.split('/')[-1]

    return root, csv_filename, df
        
        
def parseECLIPSE(filename):
    """Converts ECLIPSE csv into an xml file for Flexplan ingestion
    Intent is for this menthod to be dynamically called from `parseCSV()`

    Args:
        filename:   Filename of the input event file
    
    Returns:
        root:           Etree XML root object. Manipulate later.
        csv_filename:   Output Filename to rename xml file as (combines the paired event files)
        df:             Pandas Dataframe of the ECLIPSE Data
    Examples:
        root,df = ParseEvents.parseCOMM("ECLIPSE_filename.csv")
    """
    print "\nNow Parsing ECLIPSE file"
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

        xml_entity = None

        entity_values = [utcStart,duration,uid,descr,sat,xml_entity,event_params]

        entities = dict(zip(entity_names,entity_values))
        root = createEventElement(root,entities,override_keys=entity_names) # Override to preserve order in xml
        
    # csv_filename = string.replace(string.replace(filename.split('/')[-1],'SAT1_',''),'SAT2_','')
    return root,combo_filename,df


def parseMANEUVER(filename):
    """Converts MANEUVER csv into an xml file for Flexplan ingestion
    Intent is for this menthod to be dynamically called from `parseCSV()`

    Args:
        filename:   Filename of the input event file
    
    Returns:
        root:           Etree XML root object. Manipulate later.
        csv_filename:   Output Filename to rename xml file as (combines the paired event files)
        df:             Pandas Dataframe of the MANEUVER Data
    Examples:
        root,df = ParseEvents.parseCOMM("MANEUVER_filename.csv")
    """
    print "\nNow Parsing MANEUVER file"
    ###### Load The dataframe
    df,combo_filename = getPairedEventFiles(filename,cols=["Target","Start","Stop","Duration"])

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
        descr = "MANEUVER"
        sat = row.Sat
        param_names = ["ACS_POINT"]
        param_values = [row.Target]

        entity_names = ['UTC_Start_Time','Duration','Unique_Id','Event_Description','Sat',
                            'Entity','List_of_Event_Parameters']
        event_params = {'Event_Parameter':[{'Event_Par_Name':param_names[i],'Event_Par_Value':param_values[i]} for i in np.arange(len(param_names))]}

        xml_entity = None

        entity_values = [utcStart,duration,uid,descr,sat,xml_entity,event_params]

        entities = dict(zip(entity_names,entity_values))
        root = createEventElement(root,entities,override_keys=entity_names) # Override to preserve order in xml
        
    # csv_filename = string.replace(string.replace(filename.split('/')[-1],'SAT1_',''),'SAT2_','')
    return root,combo_filename,df


def parseMEMORY(filename):
    """Converts MEMORY csv into an xml file for Flexplan ingestion
    Intent is for this menthod to be dynamically called from `parseCSV()`

    Args:
        filename:   Filename of the input event file
    
    Returns:
        root:           Etree XML root object. Manipulate later.
        csv_filename:   Output Filename to rename xml file as (combines the paired event files)
        df:             Pandas Dataframe of the MEMORY Data
    Examples:
        root,df = ParseEvents.parseCOMM("MEMORY_filename.csv")
    """
    print "\nNow Parsing MEMORY file"
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
        descr = "MEMORY"
        sat = row.Sat
        param_names = None
        param_values = None

        entity_names = ['UTC_Start_Time','Duration','Unique_Id','Event_Description','Sat',
                            'Entity','List_of_Event_Parameters']
        event_params = None 

        xml_entity = None

        entity_values = [utcStart,duration,uid,descr,sat,xml_entity,event_params]

        entities = dict(zip(entity_names,entity_values))
        root = createEventElement(root,entities,override_keys=entity_names) # Override to preserve order in xml
        
    # csv_filename = string.replace(string.replace(filename.split('/')[-1],'SAT1_',''),'SAT2_','')
    return root,combo_filename,df


def parsePHOTO(filename):
    """Converts PHOTO csv into an xml file for Flexplan ingestion
    Intent is for this menthod to be dynamically called from `parseCSV()`

    Args:
        filename:   Filename of the input event file
    
    Returns:
        A pluthera of things! 

        root:           Etree XML root object. Manipulate later.
        csv_filename:   Output Filename to rename xml file as (combines the paired event files)
        df:             Pandas Dataframe of the PHOTO Data

    Examples:
        root,df = ParseEvents.parseCOMM("PHOTO_filename.csv")
    """
    print "\nNow Parsing PHOTO file"
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
        descr = "PHOTO"
        sat = row.Sat
        param_names = None
        param_values = None

        entity_names = ['UTC_Start_Time','Duration','Unique_Id','Event_Description','Sat',
                            'Entity','List_of_Event_Parameters']
        event_params = None 

        xml_entity = None

        entity_values = [utcStart,duration,uid,descr,sat,xml_entity,event_params]

        entities = dict(zip(entity_names,entity_values))
        root = createEventElement(root,entities,override_keys=entity_names) # Override to preserve order in xml
        
    # csv_filename = string.replace(string.replace(filename.split('/')[-1],'SAT1_',''),'SAT2_','')
    return root,combo_filename,df


if __name__ == "__main__":
    """
    Main method. Either call with a filename argument, or without any to parse all of the event types in the Input Folder

    See help documentation automatically generated with doxygen in the doc subfolder.
    """
    # If no arguments
    if len( sys.argv ) < 2:
        fnames = ['Input/COMM_20120717000000_20120719000000_20140604114400_V1.csv',
                  'Input/ECLIPSE_SAT1_20140704000000_20140711000000_20140604123800_V1.csv',
                  'Input/MANEUVER_SAT1_20140704000000_20140711000000_20140604124700_V1.csv',
                  'Input/MEMORY_SAT2_20140704000000_20140711000000_20140604124400_V1.csv',
                  'Input/PHOTO_SAT1_20140704000000_20140711000000_20140604124500_V1.csv'
                  ]
    else:
        fnames = [sys.argv[1]]

    [parseCSV(fname) for fname in fnames] 

    












