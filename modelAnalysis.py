#### SEARCH "MODEL-SPECIFIC" TO FIND MODEL-SPECIFIC CODE ####
import xlwings as xw
import _misc
import modelFunctions as aFns
import os
from openpyxl import load_workbook
import polars as pl
import pandas as pd
import logging
import _myLogging
#import module_locator
from IPython.display import display
CONFIGFILE="config.ini"
MYLOGGER = _myLogging.get_logger("Analysis")  
EXECNAME="ReinsuranceStrategyAnalysis"    ####MODEL-SPECIFIC####
USEDATAPARQUETFOLDER=False
DELETEPARQUETSONRERUN=False   #For connection type=1 (running from Excel)

class Analysis():
    def __init__(self, connectiontype,modeltype,book=None,specfile=''):
        #specfile will be excel file if connectiontype=1, or parquet.gzip file if connectiontype=2
        self.error=""
        self.specfile=specfile
        self.connectiontype=connectiontype
        self.fileext=''
        self.specversions={}
        self.initialspecs={}
        self.activespecs={}
        self.dict_specDataFormats={}
        self.dict_specTableName={}
        self.dict_specSheetName={}
        self.dict_specHeader={}
        self.book=book
        self.modeltype=modeltype
        MYLOGGER.debug('Starting Analysis Class initialization')    
        self.analysisDictList= ["dict_specSheetName","dict_specTableName","dict_specHeader","dict_specInfoTable","dict_excelResultSheets","dict_excelResultTables",
                                "dict_specDataFormats","dict_keyCols","dict_valCols"]

        ########################ADD SPECIFIC DICTIONARIES TO ANALYSIS OBJECT################
        
        ########################END ADD SPECIFIC DICTIONARIES TO ANALYSIS OBJECT################

        
        if self.book:
            try:
                CWD=os.path.dirname(self.book.sheets["Model Path"].range("_executablepath").value)
            except:
                self.error="Unable to find model path in Excel file."
                return
        else:
            CWD=os.path.dirname(os.path.realpath(__file__))           

        print(os.path.join(CWD, CONFIGFILE))
        self.configdict= _misc.configparser_to_dict(os.path.join(CWD, CONFIGFILE))
        print(self.configdict)
        
        #Create dictionaries from config file
        for key in self.analysisDictList:
            print(key)
            print(self.configdict[key])
            exec('self.'+key+"=self.configdict[key]")

            try:
                if self.configdict[key]['addtospecs']==False:
                    exec('del self.'+key+"['addtospecs']")
            except:
                pass        

        ###################################################
        ####SECTION 1: IMPORT SPECS FROM EXCEL OR GZIP#####
        ###################################################
        self.fileext=os.path.splitext(specfile)[1]
        if self.fileext in ['.xlsx','.xlsm']:
            self.fileext='xls'
        elif self.fileext=='.gzip':
            self.fileext='gzip'
        else:
            self.fileext=''
        
        if self.fileext =='xls':
            MYLOGGER.debug('Connection type is 1')
            self.specpath = os.path.dirname(self.specfile)
            self.specpathstring = str(self.specpath).replace("\\","/")

            if self.connectiontype==3:  #link to book
                self.book=xw.Book(self.specfile)            
        elif self.fileext == 'gzip':
            MYLOGGER.debug('Connection type is 2')
            self.specfile=specfile
            self.specpath = os.path.dirname(self.specfile)
            self.specpathstring = str(self.specpath).replace("\\", "/")
            self.initialspecs=_misc.fromGzipParquet(self.specfile,'initialspecs')

        #if import_specs:
        # Create dataframe version of dict_specDataFormats
        df_SpecCleanInfo=_misc.createSpecCleanInfo(self.dict_specDataFormats)

        try:
            for key,val in self.dict_specTableName.items():
                if connectiontype in [0,1]:
                    book.sheets['Navigation'].range('H2').value='Reading '+key+' from Excel'
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

                if self.fileext=='xls':
                    try:
                        self.initialspecs[key] = _misc.load_spec_table_to_df(self.book,1,self.dict_specSheetName[key], self.dict_specTableName[key],self.dict_specHeader[key],
                                                                    self.dict_formats,self.dict_defaultvalues,self.dict_excludeifnull)
                    except:
                        collist=list(self.dict_formats.keys())
                        self.initialspecs[key] =pl.DataFrame(schema=collist).with_columns(pl.all().cast(pl.Utf8))                  
                        #self.initialspecs[key] = pl.DataFrame(columns=collist)
                elif self.fileext=='gzip':
                    if key in self.initialspecs.keys():
                        try:
                            self.initialspecs[key]=_misc.assertStringFormats(self.initialspecs[key],self.dict_specHeader[key],self.dict_formats)
                        except:
                            pass  
                    self.activespecs[key]=self.initialspecs[key]                          
        except:
            self.error = "Unable to create specs from excel."

        self.prepSpecs()

        #If connection type=1, delete dataparquet folder. (Will re-run entire model each time. May change this in the future to depend on changes in specs.)
        if USEDATAPARQUETFOLDER:
            if DELETEPARQUETSONRERUN:
                _misc.deleteFolderIfExists(self.specpath,"DataParquets")

        #Create parquet subfolders
        try:
            self.dataparquetpath = _misc.createFolderIfNot(self.specpath,"DataParquets")  # ensures that parquet folder exists and returns path as string
            self.preppedspecs.update({"dataparquetpath":self.dataparquetpath})
            
            #MODEL-SPECIFIC CODE
            self.preppedspecs.update({"lossfolder":_misc.createFolderIfNot(self.specpath,"Loss Sources/CSVs")})
            #END MODEL-SPECIFIC CODE

            self.preppedspecs.update({"specfile":self.specfile})
            self.preppedspecs.update({"fileext":self.fileext})
        except:
            self.error = "Unable to create parquet folders."
            return     

        #######################MODEL-SPECIFIC CODE#######################
        #Update/initialize any model-specific dictionaries
        #self.preppedspecs.update({"dict_complement":self.dict_complement})
        #aFns.allSummaryUWStatistics(self.preppedspecs)
        
        #######################END MODEL-SPECIFIC CODE####################
        
        if connectiontype in [0,1]:
            aFns.modelSpecificAnalysisSteps(self)

    def prepSpecs(self):
        #Perform initial data cleaning steps
        if self.fileext=='xls':
            self.initialspecs=aFns.initialCleanSpecs(self.initialspecs.copy(),self.specpathstring)
            self.activespecs=self.initialspecs.copy()
        else:
            pass
        
        self.preppedspecs=aFns.createPreppedSpecs(self.activespecs.copy())
        if self.connectiontype in [0,1]:
            self.book.sheets['Navigation'].range('H2').value='Finished Prepped Specs'        

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
                