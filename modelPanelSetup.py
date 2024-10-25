#Dependencies
import _misc
import param
import _dataformClass as dfc
import modelFunctions as fns
import modelIcons as icons
import xlwings as xw
import os
import _chartClasses as pc
import modelAnalysis as analysis
import polars as pl
import polars.selectors as cs
import pandas as pd
import numpy as np
import panel as pn
from openpyxl import Workbook, load_workbook
from openpyxl.worksheet.table import Table, TableStyleInfo
from openpyxl.utils import get_column_interval
from bokeh.plotting import figure
from bokeh.models import ColumnDataSource, Legend, LegendItem, NumeralTickFormatter
from bokeh.models.widgets.tables import NumberFormatter, BooleanFormatter,StringFormatter,DateFormatter,CheckboxEditor, NumberEditor, SelectEditor, StringEditor, DateEditor
import base64
import random
import panel as pn
from tkinter import Tk, filedialog
import time
from mplcursors import cursor
import shutil
from IPython.display import display
import hvplot.pandas
import matplotlib.pyplot as plt
import holoviews as hv
import matplotlib.patches as mpatches
import logging
import _myLogging

MYLOGGER = _myLogging.get_logger("PanelSetup")   
DEBUGGER =pn.widgets.Debugger(name='Debugger debug level', level=logging.INFO, sizing_mode='stretch_both',logger_names=['PanelSetup','Analysis','Functions','Misc','Dataforms','Charts'],collapsed=False)
RAW_CSS="""
.sidenav#sidebar {
    background-color: #f5f5f5;
}
"""


class Panel():
    def __init__(self, connectiontype,modeltype,folderguess,devmode):
        MYLOGGER.debug('Starting Panel class initialization')
        self.connectiontype=connectiontype
        self.folderguess=folderguess 
        self.devmode=devmode 
        self.modeltype=modeltype         
        self.analysis=None
        self.panelDictList= ["dict_panelMainMenu","dict_panelMainMenuActions","dict_panelWidgetGroups","dict_panelWidgets","dict_panelWidgetGroupMembers","dict_panelWidgetSpecs",
                        "dict_panelTabs","dict_panelDataformCards","dict_panelDataforms","dict_panelDataTypes","dict_panelMapWidgetTypes"]
        self.panelDictsConvertToTableList=["dict_panelMainMenu","dict_panelMainMenuActions","dict_panelWidgetGroupMembers","dict_panelWidgetSpecs","dict_panelTabs","dict_panelDataformCards","dict_panelDataforms","dict_panelDataTypes"]

        self.configdict=_misc.configparser_to_dict('config.ini')
        self.eventwatches=[]
        self.templist=[]
        self.panelDicts={}
        self.widgetDict={}
        self.widgetGroupDict={}
        self.widgetGroupLevelsDict={}
        self.dataformWidgetBlanksDict: dict = {}
        self.specWidgetDict: dict = {}
        self.dataformWidgetDict: dict = {}
        self.dataformDict: dict = {}
        self.tabsDict={}
        self.tabPagesDict={}
        self.specdfs_dfsOnly={}
        self.dict_mainmenubutton={}  #dict to store main menu buttons 
        self.mainmenuwidgets=[] 
        self.mainAreaWidget=[pn.Column(pn.pane.Markdown("Welcome to the "+self.modeltype,width=200))]
       
       #css
        self.css = '''
                .test button{
                    white-space: normal !important;
                    word-break:break-all;
                    width:150px
                    }

                .test .bk-btn.bk-btn-light{
                    text-align: left;
                    }
                    
                .test .bk-btn-group{
                    display: inline;
                    align-items: left;
                }
                '''

        self.mainmenucss = '''
                .bk-btn-group{
                    display: inline-block;
                    align-items: left;}

                .bk-btn.bk-btn-light{
                    background-color: #f5f5f5;
                }

                .bk-btn.bk-btn-light:hover{
                    background-color: #f5f5f5;
                    font-weight: bold;
                    color: #1d5aa5;
                }

                .bk-btn.bk-btn-light:focus{
                    background-color: #f5f5f5;
                    font-weight: bold;
                    color: #1d5aa5;                    
                }                
                
                .bk-menu:not(.bk-divider){
                    background-color: #f5f5f5;
                    color: #1d5aa5;
                }        

                .bk-menu:not(.bk-divider):hover{
                    background-color: #f5f5f5;
                    color: #1d5aa5;
                }

                .bk-menu:not(.bk-divider):focus{
                    background-color: #f5f5f5;
                    color: #1d5aa5;
                }
                '''
       #Model specific dictionary initialized here
       #End model specific

        MYLOGGER.debug('Starting dictionaries and widgets initialization')
        self.initializeDictionariesAndWidgets()
        MYLOGGER.debug('Resuming after dictionaries and widgets initialized')   

    def changeKeysToNames(self,dict):
        MYLOGGER.debug('Enter changeKeysToNames')
        result={}
        for key in dict.keys():
            result[self.panelDicts["dict_panelMainMenuNames"][key]]=dict[key]
        MYLOGGER.debug('Exit changeKeysToNames')
        return result

    #Function to handle all button clicks
    def onButtonClick(self,event):
        MYLOGGER.debug('Enter onButtonClick')
        selected=event.obj.name
        MYLOGGER.debug('onButtonClick Selected: '+selected)
        if selected =='Refresh Analysis':
            MYLOGGER.debug('onButtonClick Refresh Analysis')
            self.refreshAnalysis()   
            MYLOGGER.debug('Exit onButtonClick')
        elif selected =='Switch Analysis Folder':
            MYLOGGER.debug('onButtonClick Switch Analysis Folder')
            #self.resetmenubuttons("Switch Analysis")
            self.initializeAnalysis()               
            MYLOGGER.debug('Exit onButtonClick')
        elif selected =='Select Files':
            MYLOGGER.debug('onButtonClick Select Files')
            self.copyCSVsELTs()      
        elif selected=='Create Analysis':
            MYLOGGER.debug('onButtonClick Create Analysis')
            pass

    #Used to handle all events set up with watch. RadioButtonGroups, for example.
    def eventresponses(self,*events):
        ## Model-specific code here
        for event in events:
            try:
                tag=event.obj.tag
            except:
                tag=""
            try:
                name=event.obj.name
            except:
                name=""
            if tag=='Risk Portfolio Options':
                if event.obj.value=='Portfolio Inputs':
                    self.mainAreaWidget[0]=self.dataformEditDictionary['Portfolio Assumptions'].view
                else:
                    self.mainAreaWidget[0]=pn.pane.Markdown("### "+event.obj.value)
            elif tag=='Strategy Submenu Options':
                if event.obj.value=='Design Strategies':
                    self.mainAreaWidget[0]=self.dataformEditDictionary['Design Strategies'].view
                elif event.obj.value=='Theoretical Pricing':
                    df=(self.analysis.getResults('All','Theoretical Premiums')
                        .select(['Scenario','Strategy','Treaty Loss Layer','Method 1: Target Reinsurer CR','Method 2: Loss + Margin to Std Dev',
                        'Method 3: Loss + VaR x CoC','Method 4: Loss + TVaR x CoC'])
                        .with_columns([pl.col('Method 1: Target Reinsurer CR').round(0).cast(pl.Int32),
                                        pl.col('Method 2: Loss + Margin to Std Dev').round(0).cast(pl.Int32),
                                        pl.col('Method 3: Loss + VaR x CoC').round(0).cast(pl.Int32),
                                        pl.col('Method 4: Loss + TVaR x CoC').round(0).cast(pl.Int32)])
                        .to_pandas())
                    self.mainAreaWidget[0]=pn.widgets.Tabulator(df,height=750,width_policy='max',show_index=False)
                else:
                    self.mainAreaWidget[0]=pn.pane.Markdown("### "+event.obj.value)
            elif tag=='Evaluate Strategies Submenu Options':
                if event.obj.value=='Select KPIs':
                    self.mainAreaWidget[0]=self.widgetGroupKPI                              
                elif event.obj.value=='Result Summary':
                    df=self.analysis.getResults("All","KPI Results").to_pandas()
                    strategycomparison=pc.CreateChart(source_df=df,
                                                        categorycols=['Scenario','Strategy','Segmentation','Segment','KPI Metric','Percentile'],
                                                        chart_type='Bump and Radar',
                                                        dffiltercols=['Scenario','Segmentation','Segment'],
                                                        legendcols=['Strategy','KPI Metric','Percentile'],
                                                        valuecols=['Value','Rank'],
                                                        x_valueCols=['Rank'],
                                                        colCombinations={'KPI Metric':['KPI Metric','Percentile']},
                                                        groupByLegendColumn=True).view
                    self.mainAreaWidget[0]=strategycomparison
                elif event.obj.value=='Premium Allocations':
                    df=(self.analysis.getResults('All','Premium Allocations')
                                            .select(['Scenario','Segmentation','Segment','Layer','Allocation Percent'])
                                            .to_pandas())
                    premalloc=pn.Row()
                    # premalloc=pc.CreateChart(source_df=df,categorycols=['Scenario','Segmentation','Segment','Layer'],
                    #                             dffiltercols=['Scenario','Segmentation'],legendcols=['Layer','Segment'],valuecols=['Allocation Percent']).view                    
                    self.mainAreaWidget[0]=premalloc
                else:
                    self.mainAreaWidget[0]=pn.pane.Markdown("### "+event.obj.value)             
            elif tag=='Switch Analysis Submenu Options':
                if event.obj.value=='Switch Analysis':
                    self.mainAreaWidget[0]=self.widgetGroupDict['Switch Analysis Widgets']                       
                elif event.obj.value=='Create New Analysis':
                    self.mainAreaWidget[0]=self.widgetGroupDict['Create New Analysis']     
            elif tag=='Cat Modeling Submenu Options':
                if event.obj.value=='Results':
                    try:
                        df=self.analysis.getResults("All","Cat OEPs All Scenarios").drop('Return Period')
                        pcts=df.select(['Percentile']).filter(pl.col('Percentile')>=.8).get_column('Percentile').unique().to_list()
                        legend={'Percentile':pcts}
                        oepchart1=pn.Row()
                        # oepchart1=pc.CreateChart(source_df=df.to_pandas(),
                        #                          categorycols=['Scenario','Strategy','Segmentation','Segment','Return Period'],
                        #                          dffiltercols=['Scenario','Strategy','Segmentation'],
                        #                          legendcols=['Percentile','Segment'],
                        #                          legend=legend,
                        #                          valuecols=['Gross Loss OEP','Gross Loss TVaR','Ceded Loss OEP','Ceded Loss TVaR','Net Loss OEP','Net Loss TVaR']).view

                        self.mainAreaWidget[0]=oepchart1
                    except:
                        self.mainAreaWidget[0]=pn.pane.Markdown("### No Results Available")
                elif event.obj.value=='Copy ELTs':
                    self.mainAreaWidget[0]=self.widgetGroupDict['Select Files Widgets']    
                elif event.obj.value=='Copy Loss CSVs':
                    self.mainAreaWidget[0]=self.widgetGroupDict['Select Files Widgets']    
            elif tag=='Developer Tools Submenu Options':
                if event.obj.value=='Debugger':
                    DEBUGGER.collapsed=False
                    DEBUGGER.background='lightgray'
                    self.mainAreaWidget[0]=pn.Column(DEBUGGER ,height=750)                       
                elif event.obj.value=='Spec Tables':
                    self.mainAreaWidget[0]=pn.Column()
                elif event.obj.value=='Results':
                    self.mainAreaWidget[0]=pn.Column() 
            elif name=='Select Results Table':
                try:
                    df=self.analysis.getResults(self.widgetDict['Select Results Scenario'].value,event.obj.value).head(100)
                except:
                    df=pl.DataFrame()
                self.widgetDict['Results Table'].value=df.to_pandas()                                                      
            elif name=='Select Results Scenario':
                try:
                    df=self.analysis.getResults(event.obj.value,self.widgetDict['Select Results Table'].value).head(100)
                except:
                    df=pl.DataFrame()
                self.widgetDict['Results Table'].value=df.to_pandas()                                                                      

    def createMainMenuButtons(self):
        result=pn.Column(width=250)
        temp=(self.panelDicts['dict_panelMainMenu']
                .with_columns(pl.col("Submenu Items").str.split(",").cast(pl.List(pl.Utf8)).alias("Submenu Items"))
                .with_columns(pl.col("Action Keys").str.split(",").cast(pl.List(pl.Utf8)).alias("Action Keys")))

        for row in temp.rows(named=True):
            if row['Submenu Items'][0]=='None':   #Button, not dropdown
                if row['Icon']=='None':
                    self.dict_mainmenubutton[row['Main Menu Item']]=pn.widgets.Button(name=row['Name'],button_type="light", width_policy='max', height=50,stylesheets=[self.mainmenucss])
                else:
                    if row['Icon'][:6]=='icons.':
                        _icon=eval(row['Icon'])
                    else:
                        _icon=row['Icon']                    
                    self.dict_mainmenubutton[row['Main Menu Item']]=pn.widgets.Button(name=row['Name'], icon=_icon, icon_size='2em',button_type="light", width_policy='max', height=50,stylesheets=[self.mainmenucss])

                self.dict_mainmenubutton[row['Main Menu Item']].on_click(self.executeMainMenuAction)
                result.append(self.dict_mainmenubutton[row['Main Menu Item']])
                if row['End of Section']=='True':
                    result.append(pn.Spacer(height=10))
                    result.append(pn.pane.HTML(styles={'height':'2px','width':'250px','background-color': '#bfbfbf'},margin=(0,0,0,0)))
                    result.append(pn.Spacer(height=10))
            else:
                if row['Icon']=='None':
                    self.dict_mainmenubutton[row['Main Menu Item']]=pn.widgets.MenuButton(name=row['Name'], items=list(zip(row['Submenu Items'],row['Action Keys'])), button_type="light", width_policy='max', height=50,stylesheets=[self.mainmenucss])
                else:
                    if row['Icon'][:6]=='icons.':
                        _icon=eval(row['Icon'])
                    else:
                        _icon=row['Icon']
                    self.dict_mainmenubutton[row['Main Menu Item']]=pn.widgets.MenuButton(name=row['Name'], icon=_icon, icon_size='2em',items=list(zip(row['Submenu Items'],row['Action Keys'])), button_type="light", width_policy='max', height=50,stylesheets=[self.mainmenucss])
                pn.bind(self.executeMainMenuAction, self.dict_mainmenubutton[row['Main Menu Item']].param.clicked, watch=True)
                result.append(self.dict_mainmenubutton[row['Main Menu Item']])
                if row['End of Section']=='True':
                    result.append(pn.Spacer(height=10))
                    result.append(pn.pane.HTML(styles={'height':'2px','width':'250px','background-color': '#bfbfbf'},margin=(0,0,0,0)))
                    result.append(pn.Spacer(height=10))
        return result

    def executeMainMenuAction(self,event=None):
        try:
            pn.state.notifications.position = 'top-right'
            try:
                action=event.obj.name
            except:
                action=event

            pn.state.notifications.success(action,duration=2500)
            self.createMainPanel(action)
        except:
            pass

    def copyCSVsELTs(self):
        if self.analysis!=None:
            files=_misc.selectFiles()
            filetype=self.widgetDict['Cat Modeling Submenu Options'].value
            if filetype=='Copy ELTs':
                destinationfolder=self.analysis.preppedspecs['eltfolder']
            elif filetype=='Copy Loss CSVs':
                destinationfolder=self.analysis.preppedspecs['lossfolder']
            
            for file in files:
                destinationpath=os.path.join(destinationfolder,os.path.basename(file))
                if os.path.exists(destinationpath):
                    pn.state.notifications.warning(os.path.basename(file)+' already exists. Replacing Existing File',duration=0)
                    os.remove(destinationpath)
                shutil.copy2(file, destinationpath)            
    
    def initializeAnalysis(self):  
        #Initialized analysis variable
        MYLOGGER.debug('Folder Guess')
        MYLOGGER.debug(self.folderguess)
        self.filename = _misc.selectAnalysisFileOrFolder(self.connectiontype,self.modeltype,self.folderguess)
        self.widgetDict["File Input Text"].value = "Current file: " + str(self.filename)
        
        MYLOGGER.debug('Enter initializeAnalysis')        
        if self.filename!=None:
            self.analysis=None
            MYLOGGER.debug('Before Analysis Initialization')
            self.analysis= analysis.Analysis(self.connectiontype,self.modeltype,None,self.filename)
            MYLOGGER.debug('After Analysis Initialization')

            if self.analysis.error=="":
                MYLOGGER.debug("Analysis initiated successfully")
                self.widgetDict["File Input Text"].disabled=True
                self.widgetDict["Loading Analysis"].visible=True

                #Add custom steps into this function
                self.additionalAnalysisInitializationSteps()
                MYLOGGER.debug('After Additional Analysis Initialization Steps')
                self.widgetDict["Loading Analysis"].visible=False
                self.widgetDict["File Input Text"].disabled=False
               #self.enablemenubuttons()
            else:
                pass       

    def getExecStringToFormatWidgets(self,_type,_parameter,_value): 
        MYLOGGER.debug('Enter getExecStringToFormatWidgets')
        if _parameter in ['height','width','size']:
            if (_type=='Select') & (_parameter=='size'):
                MYLOGGER.debug('Exit getExecStringToFormatWidgets')
                return None
            else:   
                MYLOGGER.debug('Exit getExecStringToFormatWidgets')
                return "."+_parameter+"="+_value
        elif (_parameter in ['visible','disabled','hide_header','collapsible']) |((_type=='LoadingSpinner') &(_parameter=='value')):
            if _value=='true':
                MYLOGGER.debug('Exit getExecStringToFormatWidgets')
                return "."+_parameter+"=True"
            else:
                MYLOGGER.debug('Exit getExecStringToFormatWidgets')
                return "."+_parameter+"=False"
        elif _parameter=='options':
            MYLOGGER.debug('Exit getExecStringToFormatWidgets')
            return "."+_parameter+"=parametervalue.split(';')"
        elif _parameter=='hidden name':
            MYLOGGER.debug('Exit getExecStringToFormatWidgets')
            return ".tag='"+_value+"'"
        else:
            MYLOGGER.debug('Exit getExecStringToFormatWidgets')
            return "."+_parameter+"='"+_value+"'"    
        
    def createWidgets(self,widgetlistdict,widgetspectbl):
        def buildWidget(_widgetid,_widgettype,_dataformat="None"):
            
            parameters=widgetspectbl.filter((pl.col('Widget or Group ID')==_widgetid)&(pl.col('Parameter Value')!=None)).unique(subset=['Parameter Type'],keep='first')
            tempwidgettype=self.panelDicts["dict_panelMapWidgetTypes"][_widgettype+"|"+_dataformat]
            
            if tempwidgettype=="Button":
                result=pn.widgets.Button()
            elif tempwidgettype=="TextInput":
                result=pn.widgets.TextInput()
            elif tempwidgettype=="FileInput":
                result=pn.widgets.FileInput(accept='.gzip')
            elif tempwidgettype=="LoadingSpinner":
                result=pn.widgets.LoadingSpinner()
            elif tempwidgettype=="RadioButtonGroup":
                result=pn.widgets.RadioButtonGroup()
                self.eventwatches.append(result.param.watch(self.eventresponses,'value',onlychanged=False))
            elif tempwidgettype=="Select":
                if 'size' in parameters.get_column('Parameter Type').to_list():
                    size=parameters.filter(pl.col('Parameter Type')=='size').get_column('Parameter Value').to_list()[0]
                else:
                    size=None
                if size==None:
                    result=pn.widgets.Select()
                else:
                    result=pn.widgets.Select(size=int(size))
                self.eventwatches.append(result.param.watch(self.eventresponses,['value'],onlychanged=False))

            
            for row in parameters.rows(named=True):
                parametervalue=row['Parameter Value']
                parametertype=row['Parameter Type']
                execstring=self.getExecStringToFormatWidgets(tempwidgettype,parametertype,parametervalue)
                if execstring!=None:
                    exec("result"+execstring)

            if _widgettype=='Button':
                result.on_click(self.onButtonClick)

            return result
        
        resultdict={}
        for key,val in widgetlistdict.items():
            resultdict[key]=buildWidget(key,val)  

        return resultdict          

    def createWidgetGroups(self,widgetgrpdict,widgetgrpmembertbl):
        def addLevel(groupID):  #used to ensure that widget groups are built in the correct order (in case of nested groups)
            if groupID not in widgetgrpmembertbl.filter(pl.col('Member Type')=="Widget Group").get_column('Member ID').unique().to_list():
                return 0
            else:
                widgetgroupid=widgetgrpmembertbl.filter(pl.col('Member ID')==groupID).get_column('Widget Group ID').to_list()[0]
                return 1+addLevel(widgetgroupid)
        
        resultdict={}
        for key in widgetgrpdict.keys():
            self.widgetGroupLevelsDict[key]=addLevel(key)

        for level in range(max(self.widgetGroupLevelsDict.values()),-1,-1):
            for key in widgetgrpdict.keys():
                if self.widgetGroupLevelsDict[key]==level:
                    tempWidgetTable=widgetgrpmembertbl.filter(pl.col('Widget Group ID')==key).sort('Order',descending=False)
                    tempContainer=widgetgrpdict[key]
                    if tempContainer=='WidgetBox':
                        result= pn.WidgetBox(name=key)
                    elif tempContainer=='Card':
                        result=pn.Card(name=key)

                    parameters=self.panelDicts["dict_panelWidgetSpecs"].filter((pl.col('Widget or Group ID')==key)&(pl.col('Parameter Value')!=None)).unique(subset=['Parameter Type'],keep='first')                        
                    for row in parameters.rows(named=True):
                        parametervalue=row['Parameter Value']
                        parametertype=row['Parameter Type']
                        execstring=self.getExecStringToFormatWidgets(tempContainer,parametertype,parametervalue)
                        exec("result"+execstring)
                    
                    for row in tempWidgetTable.rows(named=True):
                        if row['Member Type']=='Widget':
                            result.append(self.widgetDict[row['Member ID']])
                        elif row['Member Type']=='Widget Group':
                            result.append(self.widgetGroupDict[row['Member ID']])
                    #self.widgetGroupDict[key]=result
                    resultdict[key]=result
        return resultdict

    def buildTabStructure(self):
        MYLOGGER.debug("Beginning buildTabStructure")
        
        def addLevel(tabID):
            _parent=tabsTable.filter(pl.col('Tab ID')==tabID).get_column('Parent')[0]
            _parenttype=tabsTable.filter(pl.col('Tab ID')==tabID).get_column('Parent Type')[0]
            if _parenttype!="Tab":
                return 0
            else:
                return 1+addLevel(_parent)  #.lower()

        #Add level to tabs table to determine how order for building tabs. Subtabs must be built before parent tabs.
        try:
            tabsTable=self.panelDicts["dict_panelTabs"]  
            MYLOGGER.debug(tabsTable.shape[0])
        except:
            tabsTable=pl.DataFrame()
            MYLOGGER.debug('No dict_panelTabs')

        if tabsTable.shape[0]>0:
            tabsTable=tabsTable.unique(subset=['Tab ID'],keep='first')
            tabsTable=(tabsTable
                    .with_columns((pl.col('Tab ID').apply(lambda x: addLevel(x)).alias('Level')))
                    .with_columns(pl.when(pl.col('Tab ID').is_in(tabsTable.get_column('Parent').to_list()))
                                    .then(pl.lit('Tabs'))
                                    .otherwise(pl.col('Content Type'))
                                    .alias('Content Type')))
            tablist=tabsTable.filter((pl.col('Content Type')=='Tabs')).sort('Level',descending=True).get_column('Tab ID').to_list()
            tablist2=tabsTable.filter(pl.col('Parent').is_in(tablist).is_not()).get_column('Parent').unique().to_list()
            tablist=tablist+tablist2
            MYLOGGER.debug('Tablist: '+str(tablist))
            for parent in tablist:
                if parent in self.panelDicts["dict_panelMainMenu"].keys():
                    temptbl=tabsTable.filter(pl.col('Parent')==parent).sort('Order',descending=False)
                else:
                    temptbl=tabsTable.filter(pl.col('Parent')==parent).sort('Order',descending=False)
                
                self.tabsDict[parent]=pn.Tabs(dynamic=True)
                for row in temptbl.rows(named=True):
                    if row['Content Type']=='Tabs':
                        self.tabsDict[parent].append((row['Tab Name'],self.tabsDict[row['Tab ID']]))
                    elif row['Content Type']=='Dataform':
                        self.tabsDict[parent].append((row['Tab Name'],self.dataformEditDictionary[row['Parameter']].view))
                    elif row['Content Type']=='Widget Group':
                        self.tabsDict[parent].append((row['Tab Name'],self.widgetGroupDict[row['Parameter']]))
                    else:
                        self.tabsDict[parent].append((row['Tab Name'],pn.pane.Markdown("### "+row['Tab ID'])))
            self.eventwatches.append(self.tabsDict[parent].param.watch(self.eventresponses,'active',onlychanged=True))
        MYLOGGER.debug('Exiting buildTabStructure')

    def buildDataformDict(self):
        MYLOGGER.debug('Entered buildDataformDict')

        for row in self.panelDicts["dict_panelDataforms"].rows(named=True):
            MYLOGGER.debug('Calling DataForm for '+row['Dataform Name'])
            dform=dfc.Dataform(parent=self,spec=row['Spec Sheet']) 
            self.dataformDict[row['Dataform Name']]=dform  

    def enablemenubuttons(self):
        for key in self.panelDicts["dict_panelMainMenuIcons"].keys():
            self.dict_mainmenubutton[key].disabled=False       
    
    def createMainPanel(self,selected):
        contenttype="None"
        contenttype=self.panelDicts["dict_panelMainMenuActions"].filter(pl.col('Action Key')==selected).get_column('Content Type').to_list()[0]
        contentparameter=self.panelDicts["dict_panelMainMenuActions"].filter(pl.col('Action Key')==selected).get_column('Parameter').to_list()[0]
        # MYLOGGER.debug('Selected: '+selected+', Content Type: '+contenttype+', Content Parameter: '+contentparameter)
        if contenttype=='Widget Group':
            self.mainAreaWidget[0]=self.widgetGroupDict[contentparameter]  #.lower()
        elif contenttype=='Tabs':
            MYLOGGER.debug('Selected: '+selected+', Content Type: '+contenttype+', Content Parameter: '+contentparameter)
            self.mainAreaWidget[0]=self.tabsDict[contentparameter]
        elif contenttype=='Dataform':
            self.mainAreaWidget[0]=self.dataformDict[contentparameter].view 
        else:  #if has submenu, pick first item on list and show that
            self.mainAreaWidget[0]=pn.pane.Markdown("### "+selected)   

    def refreshAnalysis(self,type='All'):
        try:
            shutil.rmtree(self.analysis.preppedspecs['dataparquetpath'])
        except:
            pass
        _misc.createFolderIfNot(self.analysis.preppedspecs['dataparquetpath'][:-13],"DataParquets")
        self.analysis.getResults('All','Gross Ceded Net Statistics')
        self.analysis.getResults('All','Premium Allocations')
        self.analysis.getResults('All','Theoretical Premiums')
       # self.analysis.getResults('All','Cat OEPs All Scenarios')
        
    def additionalAnalysisInitializationSteps(self):
        if not isinstance(self.analysis,str):
            MYLOGGER.debug('Analysis is instantiated')
            self.specdfs_dfsOnly={}
            for (key, value) in self.analysis.activespecs.items():
                MYLOGGER.debug('Key: '+key)
                # Check if value is a dataframe
                if isinstance(value, pl.DataFrame):
                    MYLOGGER.debug('Value is a DataFrame')
                    self.specdfs_dfsOnly[key] = value

            #Initialize dataform formats
            self.dataformWidgetBlanksDict=_misc.createAllSpecWidgetBlanks(self.analysis.activespecs,
                                                                          _misc.dfReplaceNanNone(self.panelDicts["dict_panelDataTypes"]).to_pandas(),
                                                                          self.panelDicts["dict_panelMapWidgetTypes"])
            
           # _misc.updateSpecWidgetBlankCodeOptions(self.analysis.activespecs,_misc.dfReplaceNanNone(self.panelDicts["dict_panelDataTypes"]).to_pandas(),self.dataformWidgetBlanksDict)

            self.specWidgetDict=_misc.createSpecWidgets(self.analysis.activespecs,
                                                                    self.dataformWidgetBlanksDict,
                                                                    self.specWidgetDict,
                                                                    _misc.dfReplaceNanNone(self.panelDicts["dict_panelDataforms"]).to_pandas(),
                                                                    _misc.dfReplaceNanNone(self.panelDicts["dict_panelDataTypes"]).to_pandas(),
                                                                    None,None)
            self.dataformWidgetDict=_misc.createDataFormWidgetDict(self.analysis.activespecs,
                                                                    self.dataformWidgetBlanksDict,
                                                                    self.specWidgetDict,
                                                                    {},
                                                                    _misc.dfReplaceNanNone(self.panelDicts["dict_panelDataforms"]).to_pandas(),
                                                                    _misc.dfReplaceNanNone(self.panelDicts["dict_panelDataTypes"]).to_pandas(),
                                                                    _misc.dfReplaceNanNone(self.panelDicts["dict_panelDataformCards"]).to_pandas(),
                                                                    None,None)
            MYLOGGER.debug('Entering buildDataformDict')
            self.buildDataformDict()                   
            MYLOGGER.debug('Entering buildTabStructure')
            self.buildTabStructure() 

            self.analysis.getResults('All','Gross Ceded Net Statistics')
            self.analysis.getResults('All','Premium Allocations')
            self.analysis.getResults('All','Theoretical Premiums')
            self.analysis.getResults('All','KPI Results')
         #   self.analysis.getResults('All','Cat OEPs All Scenarios')
            MYLOGGER.debug('Exiting additionalAnalysisInitializationSteps')

    def funtionCaller(self,functionname):
        #All functions will be called from here. (Can't call from string in Excel)
        if functionname=='test':
            print('test')

    def initializeDictionariesAndWidgets(self):
        #Import all Panel dictionaries, change keys to names (so not all lowercase) and create tables for certain dictionaries
        MYLOGGER.debug('Enter initializeDictionariesAndWidgets')
        for key in self.panelDictList:
                
            try:
                MYLOGGER.debug('Initializing '+key)
                self.panelDicts[key]= self.configdict[key]  
                try:
                    exec("del self.panelDicts['"+key+"']['addtospecs']")                    
                except:
                    pass

                if key in self.panelDictsConvertToTableList:
                    MYLOGGER.debug('Converting '+key+' to table')
                    MYLOGGER.debug("self.panelDicts['"+key+"']=_misc.convertDictToTable(self.configdict,'"+key+"',self.panelDicts['"+key+"'])")
                    exec("self.panelDicts['"+key+"']=_misc.convertDictToTable(self.configdict,'"+key+"',self.panelDicts['"+key+"'])")
                    MYLOGGER.debug('Converted '+key+' to table')

                MYLOGGER.debug('Dictionaries and tables initialized '+key)
            except:
                exec("self.panelDicts['"+key+"']={}")

        self.widgetDict=self.createWidgets(self.panelDicts["dict_panelWidgets"],self.panelDicts["dict_panelWidgetSpecs"])
        self.widgetGroupDict=self.createWidgetGroups(self.panelDicts["dict_panelWidgetGroups"],self.panelDicts["dict_panelWidgetGroupMembers"])

    def view(self):
        #Add BMS logo to dashboard
        pic_pathway = _misc.resource_path("BMS-Logo-modified.png")
        with open(pic_pathway, "rb") as img_file:
            bms_logo = base64.b64encode(img_file.read()).decode('utf-8')
        img_markdown = f'<img src="data:image/jpg;base64,{bms_logo}" align="right" width="100"/>'

        pn.extension(notifications=True)
        pn.state.notifications.position='top-right'
        pn.extension('tabulator') 
        pn.extension('floatpanel')
        pn.extension('terminal',console_output='disable')
       # pn.extension(raw_css=[self.css])

        #Create main menu buttons
        self.mainmenuwidgets=self.createMainMenuButtons()
        self.template = pn.template.FastListTemplate(
            title=self.modeltype,
            header_color='#1d5aa5', 
            header_background='#ffffff',
            accent_base_color='#1d5aa5',
            main_layout=None,
            sidebar=[self.mainmenuwidgets],  
            sidebar_width=250,
            busy_indicator=pn.indicators.BooleanStatus(value=True),
            favicon="./BMS-Logo-modified.png",
            raw_css=[RAW_CSS]
        )

        self.template.header.append(pn.HSpacer(width=50))
        self.template.header.append(pn.pane.Markdown(img_markdown, align="end"))
        self.mainAreaWidget=pn.Column(pn.pane.Markdown("### Welcome to the "+self.modeltype))
        
        self.template.main.append(self.mainAreaWidget)
        
        if self.devmode==True:
            self.widgetDict['Select Analysis Folder'].clicks+=1
        
        self.template.show()        
        #return self.template
