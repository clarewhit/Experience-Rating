#### SEARCH "MODEL-SPECIFIC" TO FIND MODEL-SPECIFIC CODE ####
import xlwings as xw
import ctypes
import _misc
import modelFunctions as fns
import os
from openpyxl import load_workbook
import polars as pl
import pandas as pd
import sys
import logging
import _myLogging
import module_locator
from IPython.display import display
CONFIGFILE="config.ini"
MYLOGGER = _myLogging.get_logger("Analysis")  
EXECNAME="ReinsuranceStrategyAnalysis"    ####MODEL-SPECIFIC####
DELETEPARQUETSONRERUN=False   #For connection type=1 (running from Excel)

class Analysis():
    def __init__(self, connectiontype,modeltype,book=None,specfile=''):
        #specfile will be excel file if connectiontype=1, or parquet.gzip file if connectiontype=2
        self.error=""
        self.specfile=specfile
        self.specversions={}
        self.initialspecs={}
        self.activespecs={}
        self.dict_specDataFormats={}
        self.dict_specTableName={}
        self.dict_specSheetName={}
        self.dict_specHeader={}
        self.connectiontype=connectiontype
        self.book=book
        self.modeltype=modeltype
        MYLOGGER.debug('Starting Analysis Class initialization')    
        self.analysisDictList= ["dict_specSheetName","dict_specTableName","dict_specHeader","dict_specInfoTable","dict_excelResultSheets","dict_excelResultTables",
                        "dict_excelResultFunctions","dict_userPaths","dict_specDataFormats","dict_keyCols","dict_valCols"]    

        ########################ADD SPECIFIC DICTIONARIES TO ANALYSIS OBJECT################
        self.analysisDictList =self.analysisDictList+["dict_complement"]

        #IMPORT DICTIONARIES FROM CONFIG FILE. USE EXCEL FILE TO GET PATH TO CONFIG FILE
        try:
            if self.book==None:
                CWD=module_locator.module_path()        
            elif self.connectiontype==1:
                CWD = self.book.sheets['File Paths'].range("_fulltoolpath").value
            else:
                CWD=module_locator.module_path()
        except:
            CWD=module_locator.module_path()
        
        if self.connectiontype==1:
            self.configdict= _misc.configparser_to_dict(os.path.join(CWD, CONFIGFILE))
        else:         
            self.configdict= _misc.configparser_to_dict(os.path.join(CWD, CONFIGFILE))  

        #Create dictionaries from config file
        for key in self.analysisDictList:
            exec('self.'+key+"=self.configdict[key]")

            try:
                if self.configdict[key]['addtospecs']==False:
                    exec('del self.'+key+"['addtospecs']")
            except:
                pass            

        ###################################################
        ####SECTION 1: IMPORT SPECS FROM EXCEL OR PICKLE###
        ###################################################
        if self.connectiontype==1:
            MYLOGGER.debug('Connection type is 1')
            self.book=book
            self.specfile=specfile
            self.specpath = os.path.dirname(self.specfile)
            self.specpathstring = str(self.specpath).replace("\\","/")
            import_specs=True
            try:
                MYLOGGER.debug('Trying to read ModelType sheet')
                filemodeltype = self.book.sheets["ModelType"].range("A1").value
                if filemodeltype != self.modeltype:
                    self.error = "Excel File is not a valid Analysis. Check cell A1 in ModelType Sheet. Close or add file to folder."
                    return "Invalid Analysis"              
            except:
                MYLOGGER.debug('ModelType sheet not found')
                self.error = "Excel File is not a valid Analysis.  No sheet called ModelType."
                return 
                
        elif self.connectiontype==2:
            MYLOGGER.debug('Connection type is 2')
            self.specfile=specfile
            self.specpath = os.path.dirname(self.specfile)
            self.specpathstring = str(self.specpath).replace("\\", "/")
            self.initialspecs=_misc.fromGzipParquet(self.specfile,'initialspecs')
            import_specs=True

        if import_specs:
            # Create dataframe version of dict_specDataFormats
            df_SpecCleanInfo=_misc.createSpecCleanInfo(self.dict_specDataFormats)
            try:
                for key,val in self.dict_specTableName.items():
                    thisSpecCleanInfo=df_SpecCleanInfo.filter(pl.col("Table")==key).select(pl.col(["Column","Data Type","Default Value","Exclude if Null"]))

                    if thisSpecCleanInfo.shape[0]>0:
                        self.dict_formats=dict(zip(thisSpecCleanInfo.get_column("Column").to_list(),thisSpecCleanInfo.get_column("Data Type").to_list()))
                        self.dict_defaultvalues=dict(zip(thisSpecCleanInfo.filter(pl.col('Default Value')!='None').get_column("Column").to_list(),
                                                        thisSpecCleanInfo.filter(pl.col('Default Value')!='None').get_column("Default Value").to_list()))
                        self.dict_excludeifnull=dict(zip(thisSpecCleanInfo.filter(pl.col('Exclude if Null')=="True").get_column("Column").to_list(),
                                                        thisSpecCleanInfo.filter(pl.col('Exclude if Null')=="True").get_column("Exclude if Null").to_list()))
                    else:
                        self.dict_formats={}
                        self.dict_defaultvalues={}
                        self.dict_excludeifnull={}

                    if self.connectiontype==1:
                        self.initialspecs[key] = _misc.load_spec_table_to_df(self.book,connectiontype,
                                                                        self.dict_specSheetName[key], self.dict_specTableName[key],self.dict_specHeader[key],
                                                                        self.dict_formats,self.dict_defaultvalues,self.dict_excludeifnull)
                    elif self.connectiontype==2:
                        if key in self.initialspecs.keys():
                            try:
                                self.initialspecs[key]=_misc.assertStringFormats(self.initialspecs[key],self.dict_specHeader[key],self.dict_formats)
                            except:
                                pass  
                        self.activespecs[key]=self.initialspecs[key]                          
            except:
                self.error = "Unable to create specs from excel."

        else:
            pass

        self.prepSpecs()

        #If connection type=1, delete dataparquet folder. (Will re-run entire model each time. May change this in the future to depend on changes in specs.)
        if self.connectiontype==1:
            if DELETEPARQUETSONRERUN:
                _misc.deleteFolderIfExists(self.specpath,"DataParquets")

        #Create parquet subfolders
        try:
            self.dataparquetpath = _misc.createFolderIfNot(self.specpath,"DataParquets")  # ensures that parquet folder exists and returns path as string
            self.preppedspecs.update({"dataparquetpath":self.dataparquetpath})
            self.preppedspecs.update({"specfile":self.specfile})
        except:
            self.error = "Unable to create parquet folders."
            return 

        #######################MODEL-SPECIFIC CODE#######################
        #Update/initialize any model-specific dictionaries
        dfSims=fns.CreateSimsTable(self.preppedspecs)
        self.preppedspecs.update({"dfSims":dfSims})
        self.preppedspecs.update({"dict_complement":self.dict_complement})
        #######################END MODEL-SPECIFIC CODE####################      

    def prepSpecs(self):
        #Perform initial data cleaning steps
        if self.connectiontype==1:
            self.initialspecs=fns.initialCleanSpecs(self.connectiontype,self.initialspecs.copy(),self.specpathstring)
            self.activespecs=self.initialspecs.copy()
        else:
            pass
        
        self.preppedspecs=fns.createPreppedSpecs(self.connectiontype,self.activespecs.copy())

        #Add additional dictionaries to prepped specs   
        for key in self.configdict.keys():
            try:
                addtospecs=self.configdict[key]['addtospecs']
            except:
                addtospecs=False

            if addtospecs==True:
                try:
                    exec('del self.'+key+"['addtospecs']")
                    self.preppedspecs[key]=self.configdict[key]
                except:
                    pass

    def copyResultDFstoExcel(self,xlbook,conntype,specs):
        for key,val in specs['dict_excelResultSheets'].items():
            _misc.copyTableToSht(xlbook,conntype,eval(specs['dict_excelResultFunctions'][key]),
                                specs['dict_excelResultSheets'][key],
                                specs['dict_excelResultTables'][key])   

    def showMessageBox(self,titlestring,textstring,exitbool):
        WS_EX_TOPMOST = 0x40000

        # display a message box; execution will stop here until user acknowledges
        ctypes.windll.user32.MessageBoxExW(None, textstring, titlestring, WS_EX_TOPMOST)
        
        if exitbool==True:
            sys.exit()
        else:
            pass
        
    def getResults(self,scenario,infotype):   ####MODEL-SPECIFIC. CHANGE PARAMETERS FOR THIS FUNCTION, IF NECESSARY####
        #rerun step to create dataparquet folder, in case it was deleted
        _misc.createFolderIfNot(self.specpath,"DataParquets") 

        ####MODEL-SPECIFIC. CHANGE FUNCTION, OR PARAMETERS FOR THIS FUNCTION, IF NECESSARY####
        # result=fns.getResults(scenario,infotype)   
        # return result
        ####END MODEL-SPECIFIC.###############################################################