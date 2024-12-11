#This module contains functions that may be used across various models
import shutil
import ctypes
import ast
from io import StringIO
import os
from os import listdir
from os.path import isfile, join
import polars as pl
import pandas as pd
from numpy import percentile as np_percentile
import pickle
import configparser
import sys
from tkinter import filedialog
from tkinter import *
import glob
from IPython.display import display
import logging
import _myLogging
import xlwings as xw
import panel as pn
import modelFunctions as fns
import _dataformClass as dfc

dtypesconvert={'Boolean':pl.Boolean,'Utf8':pl.Utf8,'Utf8Trim.0':pl.Utf8,'Date':pl.Utf8,'Float64RoundToInt':pl.Float64,'Float64':pl.Float64,'Int64':pl.Int64}
MYLOGGER = _myLogging.get_logger("Misc")       
tempCodeResult=None
def selectAnalysisFile_PanelVersion(selectedfile=None,):
    MYLOGGER.debug('Starting Select Analysis File or Folder')  
    specfiles=glob.glob(os.path.join('/app/RSA', selectedfile))

    try:
        if len(specfiles)>0:
            return specfiles[0].replace(os.sep, "/")
        else:
            return None
    except:
        return None
        
def selectAnalysisFile_LocalVersion(filetypes=[("Excel File","*.xlsx;*.xlsm"),("Spec File","*.gzip")]):
    #For selecting analysis file from python script, not via panel app
    from tkinter import Tk
    from tkinter import filedialog

    MYLOGGER.debug('Starting Select Analysis File or Folder')  

    root = Tk()
    root.wm_attributes('-topmost', 1)
    root.withdraw()
    selectedfile=filedialog.askopenfilename(parent=root,title="Select analysis file. Use filetype dropdown to specify Excel or gzip prepared Spec file.",
                                 multiple=False,filetypes=filetypes)
    
    if selectedfile:
        return selectedfile.replace(os.sep, "/")
    else:
        return ""
        
def showMessageBox(titlestring,textstring):
    WS_EX_TOPMOST = 0x40000

    # display a message box; execution will stop here until user acknowledges
    ctypes.windll.user32.MessageBoxExW(None, textstring, titlestring, WS_EX_TOPMOST)

def selectFiles_TkinterVersion():
    #Not currently being used
    #Future use: loading cat ELTs etc.
    # root = Tk()
    # root.wm_attributes('-topmost', 1)
    # root.withdraw()
    # files = filedialog.askopenfilename(parent=root,title="Select files",multiple=True)
    # return files
    pass        
        
def convertDictToTable(configdict,dictname,dict):
    mapper=pl.DataFrame([{'keys':x, 'values':y} for x,y in dict.items()])
    vals=pl.Series(dict.keys()).to_frame("keys").join(mapper, on="keys",how="left").to_series(1)
    dictkeycols=recapitalizeConfigDictKey(configdict,"dict_keyCols")
    dictvalcols=recapitalizeConfigDictKey(configdict,"dict_valCols")
    keycollist=dictkeycols[dictname].split(",")
    keycollist=[x.strip() for x in keycollist]
    valcollist=dictvalcols[dictname].split(",")
    valcollist=[x.strip() for x in valcollist]
    TblKeys=pl.Series(dict.keys()).to_frame("keys").with_columns(
        [
            pl.col("keys")
            .str.split_exact("|",len(keycollist))
            .struct.rename_fields(keycollist)
            .alias("fields"),
        ]
    ).unnest("fields").drop("keys")

    TblVals=vals.to_frame("values")
    
    if len(valcollist)>1:
        TblVals=(TblVals
                .with_columns(pl.col('values').str.strip_chars())
                .with_columns(pl.col('values').str.replace(r".$",""))
                .with_columns(pl.col('values').str.replace(r"^.",""))
                .with_columns(pl.col('values').str.replace_all("'","",literal=True))
                .with_columns(pl.col('values').str.replace_all("`","'",literal=True))   
                .with_columns(
                    [
                        pl.col("values")
                        .str.split_exact(",",len(valcollist))
                        .struct.rename_fields(valcollist)
                        .alias("fields"),
                    ]
                ).unnest("fields").drop("values")
                .with_columns(pl.all().str.strip_chars())
                .with_columns(pl.all().str.replace_all(";",",",literal=True)))
    else:
        TblVals=TblVals.rename({"values":valcollist[0]})
    result=TblKeys.hstack(TblVals)
    MYLOGGER.debug('Finished convertDictToTable')

    return result

def createSpecCleanInfo(fromdict):
    mapper=pl.DataFrame([{'keys':x, 'values':y} for x,y in fromdict.items()])

    vals=pl.Series(fromdict.keys()).to_frame("keys").join(mapper, on="keys",how="left").to_series(1)

    TblKeys=pl.Series(fromdict.keys()).to_frame("keys").with_columns(
        [
            pl.col("keys")
            .str.split_exact("|", 1)
            .struct.rename_fields(["Table", "Column"])
            .alias("fields"),
        ]
    ).unnest("fields").drop("keys")

    TblVals=(vals.to_frame("values")
    .with_columns(pl.col('values').str.strip_chars())
    .with_columns(pl.col('values').str.replace(r".$",""))
    .with_columns(pl.col('values').str.replace(r"^.",""))
    .with_columns(pl.col('values').str.replace_all("'","",literal=True))
    .with_columns(
        [
            pl.col("values")
            .str.split_exact(",", 2)
            .struct.rename_fields(["Data Type", "Default Value","Exclude if Null"])
            .alias("fields"),
        ]
    ).unnest("fields").drop("values")
    .with_columns(pl.all().str.strip_chars()))
    
    return TblKeys.hstack(TblVals)

def createPanelSpecs(fromdict):
    mapper=pl.DataFrame([{'keys':x, 'values':y} for x,y in fromdict.items()])
    vals=pl.Series(fromdict.keys()).to_frame("keys").join(mapper, on="keys",how="left").to_series(1)

    TblKeys=pl.Series(fromdict.keys()).to_frame("Spec Sheet")

    TblVals=(vals.to_frame("values")
    .with_columns(pl.col('values').str.strip_chars())
    .with_columns(pl.col('values').str.replace(r".$",""))
    .with_columns(pl.col('values').str.replace(r"^.",""))
    .with_columns(pl.col('values').str.replace_all("'","",literal=True))
    .with_columns(
        [
            pl.col("values")
            .str.split_exact(",", 5)
            .struct.rename_fields(["Display Name", "Requires Data Form","Dataform Type","Column for Select Dropdown",
                                   "Column Name for Multiple Data Columns","Override Function"])
            .alias("fields"),
        ]
    ).unnest("fields").drop("values")
    .with_columns(pl.all().str.strip_chars()))
    return TblKeys.hstack(TblVals)

def createPanelDataTypes(fromdict):
    mapper=pl.DataFrame([{'keys':x, 'values':y} for x,y in fromdict.items()])
    vals=pl.Series(fromdict.keys()).to_frame("keys").join(mapper, on="keys",how="left").to_series(1)

    TblKeys=pl.Series(fromdict.keys()).to_frame("keys").with_columns(
        [
            pl.col("keys")
            .str.split_exact("|", 1)
            .struct.rename_fields(["Spec Sheet", "Column Name"])
            .alias("fields"),
        ]
    ).unnest("fields").drop("keys")

    TblVals=(vals.to_frame("values")
    .with_columns(pl.col('values').str.strip_chars())             
    .with_columns(pl.col('values').str.replace(r".$",""))
    .with_columns(pl.col('values').str.replace(r"^.",""))
    .with_columns(pl.col('values').str.replace_all("'","",literal=True))
    .with_columns(pl.col('values').str.replace_all("||",",",literal=True))
    .with_columns(
        [
            pl.col("values")
            .str.split_exact(",", 7)
            .struct.rename_fields(["Card Number","Data Type","Source Type for Select Data Types","Source for Select Data Types","Data Format","Lower Bound","Upper Bound","Default"])
            .alias("fields"),
        ]
    ).unnest("fields").drop("values")
    .with_columns(pl.all().str.strip_chars()))

    return TblKeys.hstack(TblVals)

def flagValueIfNotNullNorInList(str1,list2):
    if str1==None:
        return str1
    elif str1 in list2:
        return str1
    else:
        return "Flag"

def list_intersection_sls(str1,list2)->str:
    #list1 as string, list2 as list, return string
    #returns sorted instersection of two lists
    if (str1==None)|(list2==[]):
        result= pl.lit(None)
    else:
        list1=str1.split(",")
        list1=[x.strip() for x in list1]
        result=sorted(list(set(list1).intersection(set(list2))))
        result=",".join(result)
    return result

def list_intersection_sll(str1,list2):
    #list1 as string, list2 as list, return string
    #returns sorted instersection of two lists
    if (str1==None)|(list2==[]):
        return pl.lit([])
    else:
        list1=str1.split(",")
        list1=[x.strip() for x in list1]
        result=sorted(list(set(list1).intersection(set(list2))))
        return result

def list_intersection_sss(str1,str2)->str:
    #list1 as string, list2 as string, return string
    #returns sorted instersection of two lists
    if (str1==None)|(str2==None):
        return pl.lit(None)
    else:
        list1=str1.split(",")
        list1=[x.strip() for x in list1]
        list2=str2.split(",")
        list2=[x.strip() for x in list2]    
        result=sorted(list(set(list1).intersection(set(list2))))
        return ",".join(result)    

def list_intersection_ssl(str1,str2):
    #list1 as string, list2 as string, return list
    #returns sorted instersection of two lists
    if (str1==None)|(str2==None):
        return pl.lit([])
    else:
        list1=str1.split(",")
        list1=[x.strip() for x in list1]
        list2=str2.split(",")
        list2=[x.strip() for x in list2]    
        result=sorted(list(set(list1).intersection(set(list2))))
        return result

def list_intersection(list1, list2):
    #returns sorted instersection of two lists
    return sorted(list(set(list1) & set(list2)))

def list_difference(list1, list2):
    #returns sorted difference between two lists
    return sorted(list(set(list1).symmetric_difference(set(list2))))

def list_dropped(list1,list2):
    #returns sorted list1 with items in list2 removed
    return sorted(list(set(list1)-set(list2)))

def list_added(list1,list2):
    #returns sorted list1 with items in list2 added
    return sorted(list(set(list2)-set(list1)))

def firstRowToDict(tbl):
    result= dict(zip(tbl.columns,tbl[0].transpose().get_column("column_0").to_list()))
    return result

def createFolderIfNot(rootfolder,newfolder):
    #Checks if the desired directory exists
    #If the directory exists: returns the path to the directory
    #If the directory does not exist: creates the directory and returns the path in string form

    retPath = os.path.join(rootfolder, newfolder)
    if not os.path.isdir(retPath):
        os.makedirs(retPath)
    return str(retPath).replace(os.sep, "/")

def deleteFolderIfExists(rootfolder,newfolder):
    #Checks if the desired directory exists
    #If the directory exists: returns the path to the directory
    #If the directory does not exist: creates the directory and returns the path in string form

    retPath = os.path.join(rootfolder, newfolder)
    if os.path.exists(retPath):
        shutil.rmtree(retPath)

def getFromParquet(parquetFullName):
    if os.path.isfile(parquetFullName) and os.access(parquetFullName, os.R_OK):
        result = pl.scan_parquet(parquetFullName)
    else:
        result = "No file found"
    return result

def fromGzipParquet(filename,key1,key2=None):
    temp=ast.literal_eval(pd.read_parquet(filename, filters=[("key","=",key1)],columns=["value"],engine='pyarrow')['value'].values[0])
    result={}
    if key2 is None:
        keylist=temp.keys()
        resulttype='dict'
    else:
        if isinstance(key2,str):
            resulttype='value'
            key2=[key2]
        else:
            resulttype='dict'
        keylist=list_intersection(temp.keys(),key2)

    for key in keylist:    
        if isinstance(temp[key],dict):
            result[key]=temp[key]
        else:
            result[key]=pl.from_pandas(pd.read_csv(StringIO(temp[key]),sep="\t"))

    if resulttype=='dict':
        return result
    else:
        return result[key2[0]]

def saveToParquet(objecttosave, parquetname):
    try:
        objecttosave.to_parquet(parquetname)
    except:
        objecttosave.write_parquet(parquetname)

def clip(_val, minval, maxval):
    #polars doesn't have a vector clip function
    #use within pl.with_columns to clip a column
    return (
        pl.when(_val < minval)
        .then(minval)
        .when(_val > maxval)
        .then(maxval)
        .otherwise(_val)
    )


def getMean(loss):
    #0 if no series passed, otherwise average
    if len(loss)==0:
        return 0
    else:
        return loss.mean()
    
def getStdDev(loss):
    #0 if no series passed, otherwise std dev
    if len(loss)==0:
        return 0
    else:
        return loss.std()

def getVaR(loss,pctile):
    if len(loss)==0:
        return 0
    else:
        return np_percentile(loss,100*pctile)
    
def getTVaR(loss,pctile,numsims =1000):
    if len(loss)==0:
        return 0
    else:
        _var=np_percentile(loss,100*pctile)
        return ((loss.filter(loss>=_var).sum())/(len(loss.filter(loss>=_var))))    

def aggdescribe(df,infocol,compl,pctile):
    #infocol: column name to describe

    if compl==True:
        x=tuple([1.0-i for i in list(pctile)])
    else:
        x=pctile
    
    result=df.describe(percentiles=[])[infocol]
    
    if infocol=='value':
        result2=pl.Series([np_percentile(df,i*100) for i in x])
    else:
        result2=pl.Series([str(i) for i in x])

    return result.append(result2)

def load_spec_table_to_df(xlbook,connectionType,shtName, tableName,hasHeader,formatSchema={},defaultValues={},excludeIfNull={}):
    #connectionType: 1=xlwings, 2=openpyxl
    #read in table from excel and convert to dataframe and format columns
    MYLOGGER.debug('Starting load_spec_table_to_df')
    if type(hasHeader)==str:
        if hasHeader=='True':
            hasHeader=True
        else:
            hasHeader=False
    
    if connectionType==2:
        tblrange = xlbook[shtName].tables[tableName].ref
        data_rows = []
        first = True
        for row in xlbook[shtName][tblrange]:
            if first and hasHeader:
                columnlist = [cell.value for cell in row]
                first = False
            else:
                data_rows.append([cell.value for cell in row])
        if hasHeader:
            result = pd.DataFrame(data_rows, columns=columnlist)
            result[columnlist] = result[columnlist].astype(str)
        else:
            result=pd.DataFrame(data_rows)
    else:
        result = (
            xlbook.sheets[shtName]
            .range(tableName + "[[#All]]")
            .options(pd.DataFrame, index=False,header=hasHeader)
            .value)


    #Try formatting dataframe
    result = result.astype(str)
    result=result.replace({'None':None,'nan':None,'NaT':None,'NaN':None,'':None,' ':None})
    result=pl.from_pandas(result)

    actualcols=result.columns
    lowercasecols=[x for x in actualcols]  #.lower()
    result.columns=lowercasecols
    if bool(excludeIfNull)==True:
        for key,val in excludeIfNull.items():
            result=result.filter(pl.col(key).is_not_null())
    if bool(defaultValues)==True:
        for key,val in defaultValues.items():
            result=result.with_columns(pl.col(key).fill_null(val))
    if bool(formatSchema)==True:
        for key,val in formatSchema.items():
            print(key,val)
            if val=='Boolean':
                result=result.with_columns(pl.when(pl.col(key)=='True')
                                           .then(pl.lit(True))
                                           .when(pl.col(key)=='False')
                                           .then(False)
                                           .cast(pl.Boolean)
                                           .alias(key))
            elif val=='Date':
                result=result.with_columns(pl.col(key).cast(dtypesconvert[val]).str.strptime(pl.Date,format=('%Y-%m-%d'), strict=False))
            elif val=='Float64RoundToInt':
                result=result.with_columns(pl.col(key).cast(dtypesconvert[val]).round(0).cast(pl.Int64))
            elif val=='Utf8Trim.0':
                try:
                    result=result.with_columns(pl.col(key).str.replace(r'\.0$',''))
                except:
                    pass
            else:
                result=result.with_columns(pl.col(key).cast(dtypesconvert[val]))

    result.columns=actualcols
    return result
    
def assertStringFormats(df,hasHeader,formatSchema):
    #Try formatting dataframe
    
    if type(hasHeader)==str:
        if hasHeader=='True':
            hasHeader=True
        else:
            hasHeader=False

    if hasHeader==False:
        return df
    else:
        result=df
        try:
            for key,val in formatSchema.items():
                if val in ['Utf8','Utf8Trim.0']:
                    try:
                        result=result.with_columns(pl.col(key).cast(pl.Utf8).alias(key))
                    except:
                        pass
        except:
            pass   
    return result

def copyTableToSht(xlbook,connectionType,df, shtName, tblName):
    if connectionType==1:
        try:
            if df.shape[0]>0:
                if isinstance(df, pl.DataFrame):
                    df=df.to_pandas()
                xlbook.sheets[shtName].tables[tblName].update(df, index=False)
            else:
                xlbook.sheets[shtName].tables[tblName].data_body_range.clear()
        except:
            pass
    elif connectionType==2:
        pass

def getReturnPeriod(pctile):
    try:
        if pctile<.5:
            suffix=" (Best)"
            rp=1/(pctile)
        elif pctile==1:
            return "max"
        elif pctile==0:
            return "min"            
        else:
            suffix=""
            rp=1/(1-pctile)

        if round(rp,0)==round(rp,4):
            return str(int(round(rp,0)))+ " yr" + suffix
        else:
            return str(round(rp,3))+ " yr" + suffix
    except:
        return ""

def uniqueList(list_in):
    unique_list=[]
    for x in list_in:
        if x not in unique_list:
            unique_list.append(x)
    return unique_list

def configparser_to_dict(configFilename,fromdict=False):
    import configparser

    MYLOGGER.debug('Starting configparser_to_dict')
    try:
        config_dict = {}
        
        config = configparser.ConfigParser()
        config.read(configFilename)

        for section in config.sections():
            config_dict[section] = {}
            if section!='dict_specDataFormats':
                for key, value in config.items(section):

                    # Now try to convert back to original types if possible
                    if value in ['True',True]:
                        value = True
                    elif value in ['False',False]:
                        value = False   
                    elif value in ['None',None]:
                        value = None
                    elif isinstance(value, str):
                        try:
                            if '.' in value:
                                value = float(value)
                            else:
                                value = int(value)
                        except:
                            pass
                    config_dict[section][key] = value
            else:
                config_dict[section]=dict(config.items(section))
                
        # Now drop root section if present
        config_dict.pop('root', None)

        #Recapitalize 
        tempdict={}
        for key in config_dict.keys():
            if key[-5:]!="_keys":
                tempdict[key]=recapitalizeConfigDictKey(config_dict,key)

        return tempdict
    except:
        return "Error Creating Config: "+configFilename

def recapitalizeConfigDictKey(_configdict,_key):
    try:
        temp=_configdict[_key]
        tempkeys=_configdict[_key+'_keys']
        result={}
        tempkeydiff=list_difference(list(temp.keys()),list(tempkeys.keys()))

        for key2,val2 in tempkeys.items():
            result[val2]=temp[key2]
        
        for key3 in tempkeydiff:                          
            result[key3]=temp[key3]
    except:
        result=_configdict[_key]
    return result

def dfReplaceNanNone(df):
    #replace NaN with None
    result= (df
              .with_columns(
                pl.when(pl.col(pl.Utf8).is_in(["nan","None","NaN"]))
                .then(None)
                .otherwise(pl.col(pl.Utf8))
                .name.keep()))
    return result

def getFileList(folder,extension):
    MYLOGGER.debug("Entered getFileList")
    if not os.path.isdir(folder):
        if not os.path.isdir(str(folder).replace("\\", "/") ):
            return []
        else:
            folder=str(folder).replace("\\", "/")
    else:
        pass
        
    temp= [f for f in listdir(folder) if isfile(join(folder, f))]
    result= [f for f in temp if f.endswith('.'+extension)]
    return result

def dictMissingDataformItems(specs,blanks,dataTypes,dfInfo=None,spec=None,selected=None):
    #Create dictionary of missing dataform items
    #Will be used by all dataform widget functions
    tempDataTypes=dataTypes.copy()
    if blanks==False:
        tempdfInfo=dfInfo.copy()

    speclist=tempDataTypes['Spec Sheet'].unique().tolist()

    #region
    if spec==None:
        spec=speclist
    else:
        tempdfInfo=tempdfInfo[tempdfInfo['Spec Sheet']==spec]
        tempDataTypes=tempDataTypes[tempDataTypes['Spec Sheet']==spec]
        spec=list_intersection([spec],speclist)
    #endregion    

    if blanks==True:
        missingDataformItems={}
        for spec in speclist:
            missingDataformItems[spec]={}
            for row in tempDataTypes[tempDataTypes['SpecSheet']==spec].itertuples():
                if row.DataFormat=='DataForm':
                    if row.ColumnName not in selected:
                        missingDataformItems[spec][row.ColumnName]=row.ColumnName
        return missingDataformItems

def updateSpecWidgetBlankCodeOptions(specs,dataTypes,blankDict,spec=None,colname=None):
    _locals = locals()
    tempDataTypes=dataTypes.copy() 
    tempDataTypes.columns = tempDataTypes.columns.str.replace(' ', '')
    speclist=tempDataTypes['SpecSheet'].unique().tolist()
    
    if spec is not None:
        if spec in speclist:
            speclist=[spec]
        else:
            return blankDict

    #region ##LOOP
    for spec in speclist:
        print(spec)
        try:
            filteredDataTypes=tempDataTypes[(tempDataTypes['SpecSheet']==spec) & (tempDataTypes['SourceTypeifDataTypeisSelect']=='Code')]
            
            if colname is not None:
                filteredDataTypes=filteredDataTypes[filteredDataTypes['ColumnName']==colname]

            print(1)
            display(filteredDataTypes)

            if filteredDataTypes.shape[0]>0:
                for row in filteredDataTypes.itertuples():
                    
                    tempStr = row.SourceifDataTypeisSelect.replace("`","'")
                        
                    try:
                        tempOptions=None
                        print("tempCodeResult="+tempStr)
                        exec("tempCodeResult="+tempStr,globals(), _locals)
                        tempOptions=_locals['tempCodeResult']
                    except:
                        tempOptions=None

                    if row.DefaultSourceType == 'Code':
                        tempStr = row.Default.replace("`","'")
                        try:
                            tempDefault=None
                            exec("tempCodeResult="+tempStr,globals(), _locals)
                            tempDefault=_locals['tempCodeResult']
                        except:
                            tempDefault=None

                    try:
                        blankDict[spec][row.ColumnName].options=tempOptions
                    except:
                        pass

                    try:
                        blankDict[spec][row.ColumnName].default=tempDefault
                    except:
                        pass
        except:
            pass
    
    return blankDict
    #endregion

def createAllSpecWidgetBlanks(specs,dataTypes,wtypeMap):
    #dataTypes should be pandas dataframe
    #create blank widgets for all specs for data entry
    #does not require analysis to be instantiated before running
    
    _locals = locals()
    tempDataTypes=dataTypes.copy() 
    tempDataTypes.columns = tempDataTypes.columns.str.replace(' ', '')
    speclist=tempDataTypes['SpecSheet'].unique().tolist()
    blankWidgetMap={}

    for spec in speclist:
        blankWidgetMap[spec] = {}
        for row in tempDataTypes[tempDataTypes['SpecSheet']==spec].itertuples():
            print('row.DataFormat')
            print(row.DataFormat)
            print(row)
            
            keyTypeFormat=row.DataType+'|'+row.DataFormat if row.DataFormat else row.DataType+'|None'  #=='Data Entry'
            widgetType=wtypeMap[keyTypeFormat]

            #region ##build blank widget
            if row.SourceTypeifDataTypeisSelect == 'Code':
                tempStr = row.SourceifDataTypeisSelect.replace("`","'")
                
                try:
                    tempOptions=None
                    exec("tempCodeResult="+tempStr,globals(), _locals)
                    tempOptions=_locals['tempCodeResult']
                except:
                    tempOptions=None
            elif row.SourceTypeifDataTypeisSelect=='List':
                try:
                    tempOptions=row.SourceifDataTypeisSelect.split(',')
                except:
                    tempOptions=None
            else:
                tempOptions = None     

            if row.DefaultSourceType == 'Code':
                tempStr = row.Default.replace("`","'")
                try:
                    tempDefault=None
                    exec("tempCodeResult="+tempStr,globals(), _locals)
                    tempDefault=_locals['tempCodeResult']
                except:
                    tempDefault=None
            elif row.DefaultSourceType=='List':
                try:
                    tempDefault=row.Default.split(',')
                except:
                    tempDefault=None
            elif row.DefaultSourceType=='Value':
                tempDefault=row.Default
            else:
                tempDefault = None           

            if widgetType=='IntInput': #Add the lower bound, upper bound, step, and default
                tempwidget = pn.widgets.IntInput(format="0,0") 
                if (row.LowerBound != None):
                    try:
                        tempwidget.start = int(round(float(row.LowerBound),0))
                    except:
                        pass
                if (row.UpperBound != None) :
                    try:
                        tempwidget.end = int(round(float(row.UpperBound),0))
                    except:
                        pass
                if (row.Step != None) :
                    try:
                        tempwidget.step = int(round(float(row.Step),0))
                    except:
                        pass
                if (tempDefault != None) :
                    try:
                        tempwidget.default = int(round(float(tempDefault),0))
                    except:
                        pass            
            elif widgetType == 'FloatInput':
                tempwidget = pn.widgets.FloatInput(format="0.0%") 
                if (row.LowerBound != None):
                    try:
                        tempwidget.start = float(row.LowerBound)
                    except:
                        pass
                if (row.UpperBound != None) :
                    try:
                        tempwidget.end = float(row.UpperBound)
                    except:
                        pass
                if (row.Step != None) :
                    try:
                        tempwidget.step = float(row.Step)
                    except:
                        pass
                if (tempDefault!= None) :
                    try:
                        tempwidget.default = float(tempDefault)
                    except:
                        pass            
            elif widgetType == 'Select':
                tempwidget = pn.widgets.Select() 
                if tempOptions != None:
                    tempwidget.options = tempOptions
                if tempDefault != None:
                    try:
                        tempwidget.default = tempDefault
                    except:
                        pass
                
            elif widgetType == 'MultiChoice':
                tempwidget = pn.widgets.MultiChoice()
                if tempOptions != None:
                    tempwidget.options = tempOptions    
                if tempDefault != None:
                    try:
                        tempwidget.default = tempDefault
                    except:
                        pass      
            elif widgetType == 'TextInput':
                tempwidget = pn.widgets.TextInput() 
                if tempDefault != None:
                    try:
                        tempwidget.default = tempDefault
                    except:
                        pass            
            elif widgetType == 'FileInput':
                tempwidget = pn.widgets.FileInput()   
                if tempDefault != None:
                    try:
                        tempwidget.default = tempDefault
                    except:
                        pass                                     
            else:
                tempwidget = pn.widgets.TextInput()
            #endregion

            blankWidgetMap[row.SpecSheet][row.ColumnName] = tempwidget
    return blankWidgetMap

def createSpecWidgets(specs,blankDict,specWidgetDict,dfInfo,dataTypes,spec=None,selected=None,colname=None,recreate=False):
    #use by assigning result to same specWidgetDict as used in input parameters.
    #If the key already exists, it will be skipped
    #if the key doesn't exist, it will be added to dictionary
    #Then, use the dictionary to create the specwidget
    MYLOGGER.debug('Enter createSpecWidgets')
    
    tempDataTypes=dataTypes.copy()
    tempdfInfo=dfInfo.copy()
    speclist=tempDataTypes['Spec Sheet'].unique().tolist()


    #region
    if spec==None:
        spec=speclist
    else:
        tempdfInfo=tempdfInfo[tempdfInfo['Spec Sheet']==spec]
        tempDataTypes=tempDataTypes[tempDataTypes['Spec Sheet']==spec]
        spec=list_intersection([spec],speclist)      
    #endregion

    if len(spec)>0:
        selectColMap=dict(zip(tempdfInfo['Spec Sheet'],tempdfInfo['Column for Select Dropdown']))
        multipleDataColMap=dict(zip(tempdfInfo['Spec Sheet'],tempdfInfo['Column Name for Multiple Data Columns']))

        result=specWidgetDict.copy()

        #region
        selectColVals={}
        for key,val in selectColMap.items():
            if val in specs[key].columns:
                selectColVals[key]=specs[key].get_column(val).unique().to_list()
            else:
                selectColVals[key]=[]
        #endregion

        #region
        for thisspec in spec:
            #See if key already exists, if not create blank
            try:
                temp=result[thisspec].keys()
            except:
                result[thisspec]={}

            tempSelCol=selectColMap[thisspec]
            tempSelColVals=selectColVals[thisspec]

            if selected==None:
                pass
            else:
                tempSelColVals=list_intersection([selected],tempSelColVals)

            for tempval in tempSelColVals:
                try:
                    testforkeys=result[thisspec][tempval].keys()
                    #if key already exists, skip
                except:
                    collist=tempDataTypes[tempDataTypes['Spec Sheet']==thisspec]['Column Name'].unique().tolist()
                    temp=specs[thisspec].filter(pl.col(tempSelCol)==tempval).to_pandas().to_dict(orient='records')

                    result[thisspec][tempval]={}

                    if multipleDataColMap[thisspec] ==None:
                        result[thisspec][tempval]['View']={}
                        result[thisspec][tempval]['Edit']={}   

                        for key in temp[0].keys():
                            result[thisspec][tempval]['View'][key]=[]
                            result[thisspec][tempval]['Edit'][key]=[]                            

                        for thistemp in temp:
                            for key,val in thistemp.items():
                                if key in list_difference(collist,tempSelCol):
                                    #For Edit Version of Widget
                                    tempwidget=blankDict[thisspec][key].clone()
                                    if ("MultiChoice" in str(type(tempwidget)))|("MultiSelect" in str(type(tempwidget))):
                                        try:
                                            tempwidget.value=val.split(',')
                                        except:
                                            pass
                                    elif "IntInput" in str(type(tempwidget)):
                                        try:
                                            tempwidget.value=int(round(val,0))
                                        except:
                                            pass
                                    else:
                                        try:
                                            tempwidget.value=val
                                        except:
                                            pass
                                    result[thisspec][tempval]['Edit'][key]=result[thisspec][tempval]['Edit'][key]+[tempwidget]     

                                    #For View Version of Widget
                                    thisformat=tempDataTypes[(tempDataTypes['Spec Sheet']==thisspec) & (tempDataTypes['Column Name']==key)]['Data Format'].values[0]

                                    if val==None or val=="" or val=="nan" or val=="NaN" or pd.isna(val):
                                        val="None"
                                    else:
                                        print(key,val)
                                        if thisformat=="Percent":
                                            val=format(val, '0.00%')
                                        elif thisformat=="Whole Number":
                                            val=format(val, ',')
                                        elif thisformat=="Currency Whole Number":
                                            val='$'+format(val, ',')
                                    
                                    tempwidget=pn.pane.Markdown('#### '+str(val),width=275,styles={'text-align':'right'},height_policy='min')
                                    result[thisspec][tempval]['View'][key]=result[thisspec][tempval]['View'][key]+[tempwidget]                                     

                    else:
                        subColumn=multipleDataColMap[thisspec]
                        collist=list_difference(collist,[subColumn])

                        for thistemp in temp:
                            subColumnValue=thistemp[subColumn]
                            result[thisspec][tempval][subColumnValue]={}
                            result[thisspec][tempval][subColumnValue]['Edit']={}
                            result[thisspec][tempval][subColumnValue]['View']={}                            

                            for key,val in thistemp.items():
                                if key in collist:
                                    #For edit version of widget
                                    tempwidget=blankDict[thisspec][key].clone()
                                    result[thisspec][tempval][subColumnValue]['Edit'][key]=tempwidget
                                    if ("MultiChoice" in str(type(result[thisspec][tempval][subColumnValue]['Edit'][key])))|("MultiSelect" in str(type(result[thisspec][tempval][subColumnValue]['Edit'][key]))):
                                        try:
                                            result[thisspec][tempval][subColumnValue]['Edit'][key].value=result[thisspec][tempval][subColumnValue]['Edit'][key].options
                                        except:
                                            pass
                                    elif "IntInput" in str(type(result[thisspec][tempval][subColumnValue]['Edit'][key])):
                                        try:
                                            result[thisspec][tempval][subColumnValue]['Edit'][key].value=int(round(val,0))
                                        except:
                                            pass
                                    else:
                                        try:
                                            result[thisspec][tempval][subColumnValue]['Edit'][key].value=val
                                        except:
                                            pass  

                                    #For view version of widget    
                                    thisformat=tempDataTypes[(tempDataTypes['Spec Sheet']==thisspec) & (tempDataTypes['Column Name']==key)]['Data Format'].values[0]

                                    if val==None or val=="" or val=="nan" or val=="NaN" or pd.isna(val):
                                        val="None"
                                    else:
                                        if thisformat=="Percent":
                                            val=format(val, '0.00%')
                                        elif thisformat=="Whole Number":
                                            val=format(val, ',')
                                        elif thisformat=="Currency Whole Number":
                                            val='$'+format(val, ',')

                                    tempwidget=pn.pane.Markdown('#### '+str(val),width=275,styles={'text-align':'right'},height_policy='min') 
                                    result[thisspec][tempval][subColumnValue]['View'][key]=tempwidget                                
        #endregion

        if (selected!=None) & (len(spec)==1):
            if multipleDataColMap[spec[0]] ==None:
                return result[spec[0]][selected]
            elif colname!=None:
                return result[spec[0]][selected][colname]
            else:
                return result[spec[0]][selected]
        else:
            return result

def createDataFormWidgetDict_accordion(specs,blankDict,specWidgetDict,dataformWidgetDict,dfInfo,dataTypes,dfCardInfo,spec=None,selected=None) -> dict:
    #use by assigning result to same dataformWidgetDict as used in input parameters.
    #If the key already exists, it will be skipped
    #if the key doesn't exist, it will be added to dictionary
    #Then, use the dictionary to create the dataform
    MYLOGGER.debug('Enter createDataFormDict')   
    tempDataTypes=dataTypes.copy()
    tempdfInfo=dfInfo.copy()
    speclist=tempDataTypes['Spec Sheet'].unique().tolist()

    #region
    if spec==None:
        spec=speclist
    else:
        tempdfInfo=tempdfInfo[tempdfInfo['Spec Sheet']==spec]
        tempDataTypes=tempDataTypes[tempDataTypes['Spec Sheet']==spec]
        spec=list_intersection([spec],speclist)
    #endregion    
    
    if len(spec)==0:
        return dataformWidgetDict
    else:
        selectColMap=dict(zip(tempdfInfo['Spec Sheet'],tempdfInfo['Column for Select Dropdown']))
        result=dataformWidgetDict.copy()
    
        #region
        selectColVals={}
        for key,val in selectColMap.items():
            if val in specs[key].columns:
                selectColVals[key]=specs[key].get_column(val).unique().to_list()
            else:
                selectColVals[key]=[]
        #endregion  

        #region
        for thisspec in spec:
            #See if key already exists, if not create blank
            try:
                temp=result[thisspec].keys()
            except:
                result[thisspec]={}        
        
            tempSelCol=selectColMap[thisspec]
            tempSelColVals=selectColVals[thisspec]  

            if selected==None:
                pass
            else:
                tempSelColVals=list_intersection([selected],tempSelColVals)

            for tempval in tempSelColVals:
                try:
                    testforkeys=result[thisspec][tempval]['View'].keys()
                    #if key already exists, skip
                except:
                    result[thisspec][tempval]={}
                    result[thisspec][tempval]['View']={}
                    result[thisspec][tempval]['Edit']={}

                default_orientation = 'Field names in columns - Single row per item'
                #title = pn.Row(pn.pane.Markdown('## ' + thisspec+': '+tempval))
                view_main = pn.Column()
                edit_main = pn.Column()
               # view_main.append(title)
               # edit_main.append(title)
                cardInfo = dfCardInfo[dfCardInfo['Spec Sheet'] == thisspec] #Get the card info for the spec sheet
                
                specWidget=createSpecWidgets(specs,blankDict,specWidgetDict,dfInfo,dataTypes,thisspec,tempval) #Create the widget if it doesn't exist
                
                dtypeInfo=dataTypes[dataTypes['Spec Sheet']==thisspec].sort_values(by=['Card Number','Column Order']).copy()
                cards=sorted(dtypeInfo['Card Number'].unique().tolist())

                if len(cards)==1:
                    hidecardheader=True
                else:
                    hidecardheader=False
                    thisaccordionView=pn.Accordion(width_policy='max',height_policy='min',
                                                   header_color='#1d5aa5')
                    thisaccordionEdit=pn.Accordion(width_policy='max',height_policy='min',
                                                   header_color='#1d5aa5')

                for card_num in cards: 
                    #Set up Card Parameters
                    collist=dtypeInfo[dtypeInfo['Card Number']==card_num].sort_values(by=['Column Order'])['Column Name'].tolist() 

                    #Check for Card title (if this needs to be changed or is set to None)
                    try:
                        cardnumInfo=cardInfo[cardInfo['Card Number'] == card_num].to_dict(orient='records')[0]
                    except:
                        cardnumInfo={}
                    
                    if len(list(cardnumInfo.keys()))==0:
                        card_title = ""
                        card_collapsible = False  
                        card_orientation=default_orientation
                        card_collapsed=False                      
                    else:
                        if ((len(cardnumInfo)==0) | (cardnumInfo['Card Name']==None)):
                            card_title = ""
                            card_collapsible = False
                        else:
                            card_title = cardnumInfo['Card Name']
                            card_collapsible = True
                    
                        if cardnumInfo['Collapsed on Start']=="True":
                            card_collapsed = True
                        else:
                            card_collapsed = False

                        #Check for dataform orientation
                        try:  
                            card_orientation = cardnumInfo['Dataform Orientation']  
                        except:
                            card_orientation = default_orientation

                    #Create the Card and add to output
                    if ((card_orientation == 'Field names in columns - Single row per item')  | (card_orientation == 'Field names in columns - Multiple rows per item')):  #| (card_orientation == 'None')
                        print('here')
                        print(specWidget)
                        view_card_main = pn.WidgetBox(styles={'background':'#f5f5f5'},margin=(0,0,0,5))
                        edit_card_main = pn.WidgetBox(styles={'background':'#f5f5f5'},margin=(0,0,0,5))
                        view_card_main.append(pn.pane.HTML(styles={'height':'2px','width_policy':'max','background-color': '#bfbfbf'}))
                        edit_card_main.append(pn.pane.HTML(styles={'height':'2px','width_policy':'max','background-color': '#bfbfbf'}))
                        view_card_title_output=pn.Row() 
                        edit_card_title_output=pn.Row()

                        for col in collist:
                            #The column is ordered by 'Column Order'
                            column_title = pn.pane.Markdown('#### '+ str(col), width=275,styles={'text-align':'right'})
                            view_card_title_output.append(column_title)
                            edit_card_title_output.append(column_title)
                        
                        view_card_main.append(view_card_title_output) 
                        edit_card_main.append(edit_card_title_output) 

                        for rownum in range(len(specWidget['View'][collist[0]])): 
                            view_card_main_output=pn.Row(height_policy='min')
                            edit_card_main_output=pn.Row()

                            for col in collist:
                                view_column_value = specWidget['View'][col][rownum-1]
                                edit_column_value = specWidget['Edit'][col][rownum-1]
                                view_card_main_output.append(view_column_value)
                                edit_card_main_output.append(edit_column_value)
                            
                            view_card_main.append(view_card_main_output)
                            view_card_main.append(pn.layout.Divider(margin=(-20, 0, 0, 0)))
                            edit_card_main.append(edit_card_main_output)
                    elif card_orientation == 'Field names in rows - Single data column per item':
                        view_card_main = pn.Column()
                        edit_card_main = pn.Column()

                        for rownum in range(len(specWidget['View'][collist[0]])): 
                            for col in collist:
                                view_card_main_output=pn.Row(height_policy='min')
                                edit_card_main_output=pn.Row(height_policy='min')
                                column_title = pn.pane.Markdown('#### '+ str(col)+':', width=275,styles={'text-align':'left'})
                                view_card_main_output.append(column_title)
                                edit_card_main_output.append(column_title)
                                view_column_value = pn.Column(specWidget['View'][col][rownum-1],width=250)
                                edit_column_value = pn.Column(specWidget['Edit'][col][rownum-1],width=250)
                                view_card_main_output.append(view_column_value)
                                edit_card_main_output.append(edit_column_value)
       
                                view_card_main.append(view_card_main_output) 
                                if col!=collist[-1]:
                                    view_card_main.append(pn.layout.Divider(margin=(-20, 0, 0, 0)))                                  
                                edit_card_main.append(edit_card_main_output)   
                    elif card_orientation == 'Field names in rows - Multiple data columns per item':
                        tempcss = """
                                    :host {
                                    --padding-vertical: 1px;
                                    }
                                    """
                        view_card_main = pn.Column(scroll=True)
                        edit_card_main = pn.Column(scroll=True)
                        view_card_title_output=pn.Row(pn.pane.Markdown('#### ', width=275,height=50,styles={'text-align':'left'},stylesheets=[tempcss]))            
                        edit_card_title_output=pn.Row(pn.pane.Markdown('#### ', width=275,height=50,styles={'text-align':'left'},stylesheets=[tempcss]))            
                        subspecs=list(specWidget.keys())
                        availcols=specWidget[subspecs[0]]['View'].keys()
                        collist=[col for col in collist if col in availcols]

                        for subspec in subspecs:
                            #The column is ordered by 'Column Order'
                            subspec_title = pn.pane.Markdown('### '+ str(subspec), width=275,styles={'text-align':'right'})
                            view_card_title_output.append(subspec_title)
                            edit_card_title_output.append(subspec_title)
                            
                        view_card_main.append(view_card_title_output)   
                        view_card_main.append(pn.pane.HTML(styles={'height':'5px','background-color': '#1d5aa5'},width_policy='max'))          
                        edit_card_main.append(edit_card_title_output)             
                        edit_card_main.append(pn.pane.HTML(styles={'height':'5px','background-color': '#1d5aa5'},width_policy='max'))

                        for col in collist:
                            view_card_main_output=pn.Row(pn.pane.Markdown('#### '+ str(col)+':', width=275))
                            edit_card_main_output=pn.Row(pn.pane.Markdown('#### '+ str(col)+':', width=275))

                            for subspec in subspecs:
                                view_column_value = specWidget[subspec]['View'][col]
                                view_card_main_output.append(pn.Column(view_column_value,width=300))
                                edit_column_value = specWidget[subspec]['Edit'][col]
                                edit_card_main_output.append(pn.Column(edit_column_value,width=300))                                    
                            
                            view_card_main.append(view_card_main_output)
                            if col!=collist[-1]:
                                view_card_main.append(pn.layout.Divider(margin=(-20, 0, 0, 0)))                             
                            edit_card_main.append(edit_card_main_output)

                    if hidecardheader==True:
                        view_main.append(pn.Column(view_card_main, width_policy='max'))
                        edit_main.append(pn.Column(edit_card_main, width_policy='max'))
                    else:
                        thisaccordionView.append((card_title,view_card_main))
                        thisaccordionEdit.append((card_title,edit_card_main))

                if hidecardheader==False:
                    thisaccordionEdit.active=[0]
                    thisaccordionView.active=[0]
                    view_main=thisaccordionView
                    edit_main=thisaccordionEdit

                result[thisspec][tempval]['View']=view_main
                result[thisspec][tempval]['Edit']=edit_main

        if (selected!=None) & (len(spec)==1):
            return result[spec[0]][selected]
        else:
            return result

def createDataFormWidgetDict(specs,blankDict,specWidgetDict,dataformWidgetDict,dfInfo,dataTypes,dfCardInfo,spec=None,selected=None) -> dict:
    #use by assigning result to same dataformWidgetDict as used in input parameters.
    #If the key already exists, it will be skipped
    #if the key doesn't exist, it will be added to dictionary
    #Then, use the dictionary to create the dataform
    MYLOGGER.debug('Enter createDataFormDict')   
    tempDataTypes=dataTypes.copy()
    tempdfInfo=dfInfo.copy()
    speclist=tempDataTypes['Spec Sheet'].unique().tolist()

    #region
    if spec==None:
        spec=speclist
    else:
        tempdfInfo=tempdfInfo[tempdfInfo['Spec Sheet']==spec]
        tempDataTypes=tempDataTypes[tempDataTypes['Spec Sheet']==spec]
        spec=list_intersection([spec],speclist)
    #endregion    
    
    if len(spec)==0:
        return dataformWidgetDict
    else:
        selectColMap=dict(zip(tempdfInfo['Spec Sheet'],tempdfInfo['Column for Select Dropdown']))
        result=dataformWidgetDict.copy()
    
        #region
        selectColVals={}
        for key,val in selectColMap.items():
            if val in specs[key].columns:
                selectColVals[key]=specs[key].get_column(val).unique().to_list()
            else:
                selectColVals[key]=[]
        #endregion  

        #region
        for thisspec in spec:
            #See if key already exists, if not create blank
            try:
                temp=result[thisspec].keys()
            except:
                result[thisspec]={}        
        
            tempSelCol=selectColMap[thisspec]
            tempSelColVals=selectColVals[thisspec]  

            if selected==None:
                pass
            else:
                tempSelColVals=list_intersection([selected],tempSelColVals)

            for tempval in tempSelColVals:
                try:
                    testforkeys=result[thisspec][tempval]['View'].keys()
                    #if key already exists, skip
                except:
                    result[thisspec][tempval]={}
                    result[thisspec][tempval]['View']={}
                    result[thisspec][tempval]['Edit']={}

                default_orientation = 'Field names in columns - Single row per item'

                view_main = pn.Column()
                edit_main = pn.Column()

                cardInfo = dfCardInfo[dfCardInfo['Spec Sheet'] == thisspec] #Get the card info for the spec sheet
                
                specWidget=createSpecWidgets(specs,blankDict,specWidgetDict,dfInfo,dataTypes,thisspec,tempval) #Create the widget if it doesn't exist
                
                dtypeInfo=dataTypes[dataTypes['Spec Sheet']==thisspec].sort_values(by=['Card Number','Column Order']).copy()
                cards=sorted(dtypeInfo['Card Number'].unique().tolist())

                if len(cards)==1:
                    hidecardheader=True
                else:
                    hidecardheader=False
                    thistabView=pn.Tabs(width_policy='max',height_policy='max')
                    thistabEdit=pn.Tabs(width_policy='max',height_policy='max')
                    # thisaccordionView=pn.Accordion(width_policy='max',height_policy='min',
                    #                                header_color='#1d5aa5')
                    # thisaccordionEdit=pn.Accordion(width_policy='max',height_policy='min',
                    #                                header_color='#1d5aa5')

                for card_num in cards: 
                    #Set up Card Parameters
                    collist=dtypeInfo[dtypeInfo['Card Number']==card_num].sort_values(by=['Column Order'])['Column Name'].tolist() 

                    #Check for Card title (if this needs to be changed or is set to None)
                    try:
                        cardnumInfo=cardInfo[cardInfo['Card Number'] == card_num].to_dict(orient='records')[0]
                    except:
                        cardnumInfo={}
                    
                    if len(list(cardnumInfo.keys()))==0:
                        card_title = "" 
                        card_orientation=default_orientation                    
                    else:
                        if ((len(cardnumInfo)==0) | (cardnumInfo['Card Name']==None)):
                            card_title = ""
                        else:
                            card_title = cardnumInfo['Card Name']

                        #Check for dataform orientation
                        try:  
                            card_orientation = cardnumInfo['Dataform Orientation']  
                        except:
                            card_orientation = default_orientation

                    #Create the Card and add to output
                    if ((card_orientation == 'Field names in columns - Single row per item')  | (card_orientation == 'Field names in columns - Multiple rows per item')):  #| (card_orientation == 'None')
                        view_card_main = pn.WidgetBox(styles={'background':'#f5f5f5'},width_policy='max') #margin=(0,0,0,5))
                        edit_card_main = pn.WidgetBox(styles={'background':'#f5f5f5'},width_policy='max') #,margin=(0,0,0,5))
                        view_card_main.append(pn.pane.HTML(styles={'height':'2px','width_policy':'max','background-color': '#bfbfbf'}))
                        edit_card_main.append(pn.pane.HTML(styles={'height':'2px','width_policy':'max','background-color': '#bfbfbf'}))
                        view_card_title_output=pn.Row() 
                        edit_card_title_output=pn.Row()

                        for col in collist:
                            #The column is ordered by 'Column Order'
                            column_title = pn.pane.Markdown('#### '+ str(col), width=275,styles={'text-align':'right'})
                            view_card_title_output.append(column_title)
                            edit_card_title_output.append(column_title)
                        
                        view_card_main.append(view_card_title_output) 
                        edit_card_main.append(edit_card_title_output) 

                        for rownum in range(len(specWidget['View'][collist[0]])): 
                            view_card_main_output=pn.Row(height_policy='min')
                            edit_card_main_output=pn.Row()

                            for col in collist:
                                view_column_value = specWidget['View'][col][rownum-1]
                                edit_column_value = specWidget['Edit'][col][rownum-1]
                                view_card_main_output.append(view_column_value)
                                edit_card_main_output.append(edit_column_value)
                            
                            view_card_main.append(view_card_main_output)
                            view_card_main.append(pn.layout.Divider(margin=(-20, 0, 0, 0)))
                            edit_card_main.append(edit_card_main_output)
                    elif card_orientation == 'Field names in rows - Single data column per item':
                        view_card_main = pn.Column()
                        edit_card_main = pn.Column()

                        for rownum in range(len(specWidget['View'][collist[0]])): 
                            for col in collist:
                                view_card_main_output=pn.Row() #height_policy='min')
                                edit_card_main_output=pn.Row() #height_policy='min')
                                column_title = pn.pane.Markdown('#### '+ str(col)+':', width=275,styles={'text-align':'left'})
                                view_card_main_output.append(column_title)
                                edit_card_main_output.append(column_title)
                                view_column_value = pn.Column(specWidget['View'][col][rownum-1],width=250)
                                edit_column_value = pn.Column(specWidget['Edit'][col][rownum-1],width=250)
                                view_card_main_output.append(view_column_value)
                                edit_card_main_output.append(edit_column_value)
       
                                view_card_main.append(view_card_main_output) 
                                if col!=collist[-1]:
                                    view_card_main.append(pn.layout.Divider(margin=(-20, 0, 0, 0)))                                  
                                edit_card_main.append(edit_card_main_output)   
                    elif card_orientation == 'Field names in rows - Multiple data columns per item':
                        tempcss = """
                                    :host {
                                    --padding-vertical: 1px;
                                    }
                                    """
                        view_card_main = pn.Column(scroll=True)
                        edit_card_main = pn.Column(scroll=True)
                        view_card_title_output=pn.Row(pn.pane.Markdown('#### ', width=275,height=50,styles={'text-align':'left'},stylesheets=[tempcss]))            
                        edit_card_title_output=pn.Row(pn.pane.Markdown('#### ', width=275,height=50,styles={'text-align':'left'},stylesheets=[tempcss]))            
                        subspecs=list(specWidget.keys())
                        availcols=specWidget[subspecs[0]]['View'].keys()
                        collist=[col for col in collist if col in availcols]

                        for subspec in subspecs:
                            #The column is ordered by 'Column Order'
                            subspec_title = pn.pane.Markdown('### '+ str(subspec), width=275,styles={'text-align':'right'})
                            view_card_title_output.append(subspec_title)
                            edit_card_title_output.append(subspec_title)
                            
                        view_card_main.append(view_card_title_output)   
                        view_card_main.append(pn.pane.HTML(styles={'height':'5px','background-color': '#1d5aa5'},width_policy='max'))          
                        edit_card_main.append(edit_card_title_output)             
                        edit_card_main.append(pn.pane.HTML(styles={'height':'5px','background-color': '#1d5aa5'},width_policy='max'))

                        for col in collist:
                            view_card_main_output=pn.Row(pn.pane.Markdown('#### '+ str(col)+':', width=275))
                            edit_card_main_output=pn.Row(pn.pane.Markdown('#### '+ str(col)+':', width=275))

                            for subspec in subspecs:
                                view_column_value = specWidget[subspec]['View'][col]
                                view_card_main_output.append(pn.Column(view_column_value,width=300))
                                edit_column_value = specWidget[subspec]['Edit'][col]
                                edit_card_main_output.append(pn.Column(edit_column_value,width=300))                                    
                            
                            view_card_main.append(view_card_main_output)
                            if col!=collist[-1]:
                                view_card_main.append(pn.layout.Divider(margin=(-20, 0, 0, 0)))                             
                            edit_card_main.append(edit_card_main_output)

                    if hidecardheader==True:
                        view_main.append(pn.Column(view_card_main, width_policy='max'))
                        edit_main.append(pn.Column(edit_card_main, width_policy='max'))
                    else:
                        thistabView.append((card_title,view_card_main))
                        thistabEdit.append((card_title,edit_card_main))
                        # thisaccordionView.append((card_title,view_card_main))
                        # thisaccordionEdit.append((card_title,edit_card_main))

                if hidecardheader==False:
                    # thisaccordionEdit.active=[0]
                    # thisaccordionView.active=[0]
                    # view_main=thisaccordionView
                    # edit_main=thisaccordionEdit
                    # thistabView.active=[0]
                    # thistabEdit.active=[0]
                    view_main=thistabView
                    edit_main=thistabEdit                    

                result[thisspec][tempval]['View']=view_main
                result[thisspec][tempval]['Edit']=edit_main

        if (selected!=None) & (len(spec)==1):
            return result[spec[0]][selected]
        else:
            return result

def createDataFormDict(dataformWidgetDict,dfInfo) -> dict:
    result={}
    for row in dfInfo.rows(named=True):
        result[row['Dataform Name']]=dfc.Dataform(dataformWidgetDict[row['Spec Sheet']])

def getDictValue(key,dictname):
    try:
        return dictname[key]
    except:
        return "key not found"

def concatenateDictListVals(checkdict):
    MYLOGGER.debug('Enter dictValsToList')
    result=[]
    for v in checkdict.values():
        if isinstance(v,list):
            for x in v:
                result.append(x)
    return result

def keysAreDicts(checkdict):
    MYLOGGER.debug('Enter keysAreDicts')
    if isinstance(checkdict,dict):
        return [isinstance(x,dict) for x in checkdict.values()]
    else:
        return [False]

def resource_path(relative_path):
    #Get absolute path to resource, works for dev and for PyInstaller
    #Used to identify path for logo
    try:
        # PyInstaller creates a temp folder and stores path in _MEIPASS
        base_path = sys._MEIPASS
    except Exception:
        base_path = os.path.abspath(".")

    return os.path.join(base_path, relative_path)