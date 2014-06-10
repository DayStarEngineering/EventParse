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
import os, string, pickle
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


def generateXMLHeader(df,filename):
    """Generates an XML handle with header information filled in

    Args:
        df:         Dataframe with containing all event information
        filename:   Filename of the input event file
    
    Returns:
        root:   Etree XML root object. Manipulate later. 

    Example:
        df = pd.read_csv("filename.csv")
        root = ParseEvents.generateXMLHeader(df, "foobar.csv")
    """
    global in_date_format
    ###### Create the XML root object
    root = ET.Element("FDF_to_FP")

    ###### Create all of the header elements
    fname_elem = ET.SubElement(root,"FILENAME")
    fname_elem.text = string.replace(filename, "csv", "xml")
    date_elem = ET.SubElement(root,"CREATION_DATE")
    date_elem.text = datetime.strftime(datetime.utcnow(),xml_date_format)
    ## Start time is the first start time in the file
    start_elem = ET.Subelement(root,"START")
    start_elem.text = datetime.strftime(datetime.strptime(df.iloc[0]['Start'],in_date_format),xml_date_format)
    ## End time is the last stop time in the file
    end_elem = ET.Subelement(root,"END")
    end_elem.text = datetime.strftime(datetime.strptime(df.iloc[-1]['Stop'],in_date_format),xml_date_format)
    ## Some unused/Empty elements 
    sve_element = ET.SubElement(root,"STATE_VECTOR_EPOCH")
    sv_element = ET.SubElement(root,"STATE_VECTOR")

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

    u1 = getNextUniqueID()
    u2 = getNextUniqueID()
    print u1
    print u2






