import pandas as pd
import param
import panel as pn
import _misc as _misc
import hvplot.pandas
import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import holoviews as hv
import matplotlib.patches as mpatches
from bokeh.models.widgets.tables import CheckboxEditor, NumberEditor, SelectEditor
from bokeh.models import NumeralTickFormatter
import altair as alt
import _myLogging
from altair.utils.data import to_values
pn.extension('vega')
#alt.data_transformers.enable('default')
# alt.data_transformers.disable_max_rows()
alt.data_transformers.enable("vegafusion")
MYLOGGER = _myLogging.get_logger("Charts") 

#######################################
# Each class can have up to four dimensions d1-d4
# The dimensions can represent axis (x,y,z, for example) or legends, or dataframe filters
# Each dimension can have up to 6 levels (0-5) that are used to filter the dataframe, if necessary
# 

class CreateChart(param.Parameterized):

    #region Parameters
    menubuttoncss = '''
        .bk-btn-group{
            display: inline-block;
            align-items: left;}

        .bk-btn.bk-btn-light{
            background-color: #ffffff;
        }            

        .bk-btn.bk-btn-light:hover{
            background-color: #ffffff;
            color: #1d5aa5;
        }

        .bk-btn.bk-btn-light:focus{
            background-color: #ffffff;
            color: #1d5aa5;                    
        }                
        
        .bk-menu:not(.bk-divider){
            background-color: #ffffff;
            color: #1d5aa5;
        }        

        .bk-menu:not(.bk-divider):hover{
            background-color: #ffffff;
            color: #1d5aa5;
        }

        .bk-menu:not(.bk-divider):focus{
            background-color: #ffffff;
            color: #1d5aa5;
        }
        '''   
    
    tabscss='''
        :host(.bk-above) .bk-header .bk-tab.bk-active{
            color: #1d5aa5;
            background-color: #ffffff;} 
    '''
    
    lefttabscss='''
        :host(.bk-left) .bk-header .bk-tab.bk-active{
            color: #1d5aa5;
            background-color: #ffffff;}
    '''

    #Bar chart has a single value axis
    #Categories are on the x-axis
    #Categories can either be category columns or different value columns
    #Categories can also appear in legend for stacked column charts

    #Input Parameters
    sourceDataCodeDict=param.Dict(default={})
    sourceData = param.Selector()
    initialSourceData=param.String()
    changedSourceData=param.Boolean(default=False)
    data = param.DataFrame(default=pd.DataFrame())
    filtereddata = param.DataFrame(default=pd.DataFrame())
    transformeddata = param.DataFrame(default=pd.DataFrame())
    categoricalCols = param.ListSelector(default=[])     #which cols in source dataframe are categorical
    valueTypeCols = param.ListSelector(default=[])        #subset of categoricalCols that describe type of data in Quantitative or Ordinal value cols. (ex. if a column called Metric exists). Used to convert from long to wide if needed.
    excludedCols=param.ListSelector(default=[])       #which cols in source dataframe are excluded from analysis. 
    excludedColsString=param.String()
    ordinalCols=param.ListSelector(default=[])        #which cols in source dataframe are ordinal (ex. rank, year, return period, percentile)
    quantCols=param.ListSelector(default=[])          #which cols in source dataframe are quantitative (values, but not ordinal)
    valueCols=param.List(default=[])          #which cols in source dataframe are quantitative (values, but not ordinal)
    transformedValueCols=param.List(default=[])          #which cols in source dataframe are quantitative (values, but not ordinal)
    transformedCategoricalCols=param.List(default=[])
    transformedCols=param.List(default=[])
    dataTypeDictionary=param.Dict(default={})  #dictionary of data types for each column in transformed dataframe
    dataShorthandDictionary=param.Dict(default={})
    dataStructureDictionary=param.Dict(default={})
    selectVisualization=param.Selector()
    visualizationDictionary=param.Dict(default={})
    selectVisualizationSourceData=param.Selector()
    chartDictionary=param.Dict(default={})
    
    #Dataframe filter parameters
    singleSelectSourceDataFilterCols=param.ListSelector(default=[])     #subset of category cols to filter source dataframe (ex: Segment or Segmentation). Single Select.
    multiSelectSourceDataFilterCols=param.ListSelector(default=[])      #subset of category cols to filter source dataframe (ex: Segment or Segmentation). Multi Select.
    useValueDescriptorAsCategory=param.Boolean(default=False)           #If True, then value descriptor columns are used as categories. If False, then data will be converted from long to wide based on value descriptor columns
    useValueDescriptorVisible=param.Boolean(default=False)    #If True, then the useValueDescriptorAsCategory checkbox will be visible. If False, then it will be hidden.
    valueDescriptorSuffixes=param.Dict(default={})                      #If value descriptor columns are used as categories, and there are multiple value columns (example: Value, Rank), suffix will be value column name unless otherwise specified
    editValueSuffixSelect=param.Selector()
    editValueSuffixText=param.String()
    editValueSuffixVisible=param.Boolean(default=True)

    chartType=param.Selector()
    plotPane=pn.pane.Vega()
    previewPlotPane=pn.pane.Vega()
    wait=param.Boolean(default=True)   #use to hold off recalcs until all parameters are set

    _x=param.Selector()
    _y=param.Selector()
    _x2=param.Selector()
    _y2=param.Selector()    
    _offsetx=param.Selector()
    _offsety=param.Selector()
    _color=param.Selector()
    _shape=param.Selector()
    _size=param.Selector()
    _layer=param.Selector()
    _col=param.Selector()
    _row=param.Selector()
    _facet=param.Selector()
    _tooltip=param.ListSelector()
    visibleByChartType={'Simple Bar Chart':['_x','_y'],'Simple Heatmap':['_x','_y','_color'],'Simple Histogram':['_x'],
                        'Simple Line Chart':['_x','_y'],'Simple Scatterplot with Tooltips':['_x','_y','_color','_tooltip'],
                        'Simple Stacked Area Chart':['_x','_y','_color'],'Simple Strip Plot':['_x','_y'],'Violin':['_x','_y']}


    data = param.DataFrame(default=pd.DataFrame())
    #endregion
    
    def __init__(self, parent,**params):
        self.parent = parent
        super().__init__(**params)
        self.viewParam = pn.Param(self.param)
     
        if self.sourceDataCodeDict!={}:
            self.dataFilters=pn.Card(title='Filters',width_policy='min',collapsible=True,visible=False)
            self.singleSelectSourceDataFilters=[]
            self.multiSelectSourceDataFilters=[]  

            self.param.sourceData.objects=list(self.sourceDataCodeDict.keys())
            if self.sourceData==None:
                self.changedSourceData=True
                self.sourceData=list(self.sourceDataCodeDict.keys())[0]
            else:
                self.initialSourceData=self.sourceData


            self.updateData()
            self.createOptionsDialog()
            self.setupVisualizationSelection()
            self.createView()
        else:
            self.view=pn.Column(pn.pane.Markdown("#### Must Specify sourceDataCodeDict as dictionary where values are code snippets that will return dataframes."))

        self._sleep=.05

    def createOptionsDialog(self):       
        print('createoptionsdialog')
        self.optionsTabs=pn.Tabs(('Describe Data Sources',pn.Column()),('Design Visualizations',pn.Column()),stylesheets=[self.tabscss])
                
        #region Data Source and Data Structure
        dfStructureInfo=pn.Row()           
        dfStructureInfo.append(pn.Column(pn.widgets.MultiChoice.from_param(self.param.categoricalCols,visible=True,disabled=False,options=self.param.categoricalCols.objects,name='Categorical Columns',width=250),
                                                pn.pane.Markdown("If any of the categorical columns are used to specify the type of information contained in the value cols, list them below:",width=250),
                                                pn.widgets.MultiChoice.from_param(self.param.valueTypeCols,visible=True,disabled=False,options=self.param.categoricalCols.objects,name='',width=250)))
        dfStructureInfo.append(pn.widgets.MultiChoice.from_param(self.param.quantCols,visible=True,disabled=False,options=self.param.quantCols.objects,name='Quantitative Value Columns',width=250))
        dfStructureInfo.append(pn.widgets.MultiChoice.from_param(self.param.ordinalCols,visible=True,disabled=False,options=self.param.ordinalCols.objects,name='Ordinal Value Columns',width=250))
        dfStructureInfo.append(pn.widgets.MultiChoice.from_param(self.param.excludedCols,visible=True,disabled=False,options=self.param.excludedCols.objects,name='Excluded Columns',width=250))

        #region Describe Source Data
        self.optionsTabs[0].append(pn.Row(pn.pane.Markdown("Source Data:"),pn.widgets.Select.from_param(self.param.sourceData,visible=True,options=self.param.sourceData.objects,name="")))           
        self.optionsTabs[0].append(pn.layout.Divider())

        dataTabs=pn.Tabs(('Specify Data Structure',pn.Column()),('Preview Data',pn.Column()),stylesheets=[self.tabscss])
        self.optionsTabs[0].append(dataTabs)

        dataTabs[0].append(pn.pane.Markdown("""
                                                         All columns in dataframe must be categorized as either **Excluded** (to be dropped from data), **Categorical** (all remaining columns that do not include numerical data that can be plotted), or **Value** (all remaining columns that include numerical data that can be plotted).
                                                         """,width=800))        

        dataTabs[0].append(dfStructureInfo)
        dataTabs[1].append(pn.widgets.Tabulator.from_param(self.param.data,height=300,width_policy='max',layout='fit_data_fill',show_index=False))
        #endregion

        #region Design Visualization
        visualizationMenuOptions = ["Copy Existing", "Create New"]
        visualizationMenu = pn.widgets.MenuButton(name="Add New Visualization", items=visualizationMenuOptions, button_type="primary", margin=5)
        visualizationMenu.on_click(self.onVisualizationMenuClick)
        selectVisualizationWidgets=pn.Row(pn.pane.Markdown("Select Visualization:"),pn.widgets.Select.from_param(self.param.selectVisualization,options=self.param.selectVisualization.objects,visible=True,name=''))      
        self.optionsTabs[1].append(pn.Row(selectVisualizationWidgets,visualizationMenu))  #menu=optionsTabs[1][0][1]  widgets=optionsTabs[1][0][0]

        copyVisualizationWidgets=pn.Row(pn.pane.Markdown("Copy From:"),pn.widgets.Select.from_param(self.param.selectVisualization,visible=True,options=['a','b'],name=''),pn.Spacer(width=25),pn.pane.Markdown("New Name:"),pn.widgets.TextInput(name='',value=''),pn.widgets.Button(name='Copy',button_type='primary'),pn.widgets.Button(name='Cancel Copy',button_type='primary'))
        self.optionsTabs[1].append(copyVisualizationWidgets)    #optionsTabs[1][1]
        self.optionsTabs[1][1][5].on_click(self.onButtonClick)
        self.optionsTabs[1][1][6].on_click(self.onButtonClick)
        createVisualizationWidgets=pn.Row(pn.pane.Markdown("New Name:"),pn.widgets.TextInput(name='',value=''),pn.widgets.Button(name='Create',button_type='primary'),pn.widgets.Button(name='Cancel Create',button_type='primary'))  
        self.optionsTabs[1].append(createVisualizationWidgets)      #optionsTabs[1][2]
        self.optionsTabs[1][2][2].on_click(self.onButtonClick)
        self.optionsTabs[1][2][3].on_click(self.onButtonClick)
        self.optionsTabs[1].append(pn.layout.Divider())     #optionsTabs[1][3]
        self.optionsTabs[1].append(pn.Tabs(('Filter and Transform Data',pn.Column()),('Design Visualization Components',pn.Column()),('Arrange Components',pn.Column()),stylesheets=[self.tabscss]))  #optionsTabs[1][4] 
        #endregion

        #region Data Filters
        self.chartControls=pn.WidgetBox()     
        self.dfFiltersInfo=pn.Row()
        self.valueDescriptorSection=pn.Column()   
        self.valueDescriptorRow=pn.Row()            
        self.singleSelectSourceDataFilterCols=_misc.list_intersection(self.categoricalCols,self.singleSelectSourceDataFilterCols)
        self.multiSelectSourceDataFilterCols=_misc.list_intersection(self.categoricalCols,self.multiSelectSourceDataFilterCols)                                          
        self.optionsTabs[1][4][0].append(pn.Row(pn.pane.Markdown("Select Data Source:"),pn.widgets.Select.from_param(self.param.selectVisualizationSourceData,options=self.param.selectVisualizationSourceData.objects,visible=True,name='')))     
        self.optionsTabs[1][4][0].append(pn.layout.Divider())
        self.optionsTabs[1][4][0].append(pn.Tabs(('Filters',pn.Column()),('Transformations',pn.Column()),('Preview',pn.Column()),stylesheets=[self.lefttabscss],tabs_location='left'))  #optionsTabs[1][4][0][2]
        self.optionsTabs[1][4][0][2][0].append(pn.Row(pn.widgets.MultiChoice.from_param(self.param.singleSelectSourceDataFilterCols,
                                                                                        visible=True,
                                                                                        disabled=False,
                                                                                        name='Filter Columns: Each requires single selection'),
                                                pn.widgets.MultiChoice.from_param(self.param.multiSelectSourceDataFilterCols,
                                                                                        visible=True,
                                                                                        disabled=False,
                                                                                        name='Filter Columns: Each allows multiple selections'))) 
        # self.dataTabs[2].append(pn.pane.Markdown("#### Select Data Filters")) 
        # self.dataTabs[2].append(pn.pane.Markdown("""
        #                                                  Select filters to enable filtering of **entire** dataset before charting.
        #                                                  """,width=800))        
        # self.dataTabs[2].append(self.dfFiltersInfo)
        # self.dataTabs[2].append(pn.layout.Divider())     
        # self.dataTabs[2].append(pn.pane.Markdown("#### Data Preview"))
        # self.dataTabs[2].append(pn.widgets.Tabulator.from_param(self.param.filtereddata,height=300,width_policy='max',layout='fit_data_fill',show_index=False))               

        # self.valueDescriptorRow.append(pn.widgets.Checkbox.from_param(self.param.useValueDescriptorAsCategory,name='Use Value Descriptor(s) as Category')) #,disabled=self.param.useValueDescriptorAsCategoryDisabled))
        # self.valueDescriptorSection.append(pn.pane.Markdown("#### Specify Data Transformations")) 
        # self.valueDescriptorSection.append(pn.pane.Markdown("""
        #                                                  The quantitative/ordinal columns in the data include more than one type of information. \
        #                                                     The information type contained in each row is described in another column. The different \
        #                                                     information types can be used, as is, as a category (for example, a column chart with different metrics by column). \
        #                                                     Alternatively, the data can be transformed so the data for different information types will \
        #                                                     appear in different columns. New column names will be the information type plus a specified suffix for each \
        #                                                     quantitative/ordinal column. The suffix can be specified below. A unique suffix must be provided for each \
        #                                                     quantitiative/ordinal column. One suffix can be left blank. \
        #                                                  """))   
        # self.valueDescriptorSection.append(self.valueDescriptorRow)

        # saveValueSuffixButton=pn.widgets.Button(name='Save Changes',button_type='default',align=('start','end'),visible=self.param.editValueSuffixVisible)
        # saveValueSuffixButton.on_click(lambda x: self.valueDescriptorSuffixes.update({self.editValueSuffixSelect:self.editValueSuffixText}))

        # self.valueDescriptorSection.append(pn.Row(pn.widgets.Select.from_param(self.param.editValueSuffixSelect,name='Select Value Column',options=self.param.valueCols,visible=self.param.editValueSuffixVisible),
        #                                  pn.widgets.TextInput.from_param(self.param.editValueSuffixText,name='Enter Value Column Suffix',visible=self.param.editValueSuffixVisible),
        #                                  saveValueSuffixButton))
        # self.valueDescriptorSection.append(pn.layout.Divider())
        # self.dataTabs[3].append(self.valueDescriptorSection)
        # self.dataTabs[3].append(pn.pane.Markdown("#### Data Preview"))
        # self.dataTabs[3].append(pn.widgets.Tabulator.from_param(self.param.transformeddata,height=300,width_policy='max',layout='fit_data_fill',show_index=False))       
        

                
        # self.optionsTabs[0].append(pn.layout.Divider())   
        # self.optionsTabs[0].append(self.dataTabs)

        # self.optionsTabs[1].append(pn.pane.Markdown("#### Select Chart Type"))        

        # code = """
        # window.open("https://altair-viz.github.io/gallery/index.html")
        # """
        # button = pn.widgets.Button(name="Open Altair Chart Gallery in New Tab", button_type="primary")
        # button.js_on_click(code=code)

        # self.optionsTabs[1].append(pn.Row(pn.widgets.Select.from_param(self.param.chartType,name='',
        #                                                                visible=True,
        #                                                                groups={'Simple Charts':['Simple Bar Chart','Simple Heatmap','Simple Histogram',
        #                                                                                         'Simple Line Chart','Simple Scatterplot with Tooltips',
        #                                                                                         'Simple Stacked Area Chart','Simple Strip Plot'],
        #                                                                         'Bar Charts':['Bar Chart','Stacked Bar Chart','Grouped Bar Chart'],
        #                                                                         'Line Charts':['Bump Chart'],
        #                                                                         'Distributions':['Violin'],}),
        #                                                                button))                                                        
        # self.encodingsTabs=pn.Tabs(('Position (x,y)',pn.Column()),('Mark (shape,color)',pn.Column()),('Facet (col,row)',pn.Column()),('Labels',pn.Column()),tabs_location='left') 
        # self.optionsTabs[1].append(pn.layout.Divider())  
        # self.optionsTabs[1].append(pn.pane.Markdown("#### Assign Columns to Chart Characteristics"))           
        # self.optionsTabs[1].append(pn.Row(self.encodingsTabs,pn.Column(pn.pane.Markdown("#### Preview"),self.previewPlotPane)))  
        # self._xWidget=pn.widgets.Select.from_param(self.param._x,options=self.param.transformedCols,disabled=False,name='X',width=250)
        # self._yWidget=pn.widgets.Select.from_param(self.param._y,options=self.param.transformedCols,disabled=False,name='Y',width=250)
        # self._x2Widget=pn.widgets.Select.from_param(self.param._x2,options=self.param.transformedCols,visible=True,disabled=False,name='X2',width=250)
        # self._y2Widget=pn.widgets.Select.from_param(self.param._y2,options=self.param.transformedCols,visible=True,disabled=False,name='Y2',width=250)
        # self._offsetxWidget=pn.widgets.Select.from_param(self.param._offsetx,options=self.param.transformedCols,visible=True,disabled=False,name='X Axis Offset',width=250)
        # self._offsetyWidget=pn.widgets.Select.from_param(self.param._offsety,options=self.param.transformedCols,visible=True,disabled=False,name='Y Axis Offset',width=250)
        # self._colorWidget=pn.widgets.Select.from_param(self.param._color,options=self.param.transformedCols,visible=True,disabled=False,name='Color',width=250)
        # self._shapeWidget=pn.widgets.Select.from_param(self.param._shape,options=self.param.transformedCols,visible=True,disabled=False,name='Shape',width=250)
        # self._sizeWidget=pn.widgets.Select.from_param(self.param._size,options=self.param.transformedCols,visible=True,disabled=False,name='Size',width=250)
        # self._layerWidget=pn.widgets.Select.from_param(self.param._layer,options=self.param.transformedCols,visible=True,disabled=False,name='Layer',width=250)
        # self._rowWidget=pn.widgets.Select.from_param(self.param._row,options=self.param.transformedCols,visible=True,disabled=False,name='Row',width=250)
        # self._colWidget=pn.widgets.Select.from_param(self.param._col,options=self.param.transformedCols,visible=True,disabled=False,name='Column',width=250)
        # self._facetWidget=pn.widgets.Select.from_param(self.param._facet,options=self.param.transformedCols,visible=True,disabled=False,name='Facet',width=250)
        # self._tooltipWidget=pn.widgets.MultiChoice.from_param(self.param._tooltip,options=self.param.transformedCols,visible=True,disabled=False,name='Tooltip',width=250)
        # self.encodingsTabs[0].append(self._xWidget)
        # self.encodingsTabs[0].append(self._yWidget)
        # self.encodingsTabs[0].append(self._x2Widget)
        # self.encodingsTabs[0].append(self._y2Widget)
        # self.encodingsTabs[0].append(self._offsetxWidget)
        # self.encodingsTabs[0].append(self._offsetyWidget)
        # self.encodingsTabs[1].append(self._colorWidget)
        # self.encodingsTabs[1].append(self._shapeWidget)
        # self.encodingsTabs[1].append(self._sizeWidget)
        # self.encodingsTabs[2].append(self._layerWidget)
        # self.encodingsTabs[2].append(self._rowWidget)
        # self.encodingsTabs[2].append(self._colWidget)
        # self.encodingsTabs[2].append(self._facetWidget)
        # self.encodingsTabs[1].append(self._tooltipWidget)

        # self.currChart=None

    def showModal(self,event=None):
        self.parent.template.modal[0].clear()
        self.parent.template.modal[0].append(self.optionsTabs)
        self.parent.template.open_modal()

    def createView(self):
        print('viewtriggered')
        self.mainmenuwidget=pn.widgets.Button(name='Options', button_type="default")
        self.mainmenuwidget.on_click(self.showModal)

        sideMenu=pn.Column()
        sideMenu.append(self.mainmenuwidget)

        self.view=pn.Row(self.dataFilters,pn.Column(sideMenu, 
                         pn.widgets.Tabulator.from_param(self.param.transformeddata,height=300,width_policy='max',layout='fit_data_fill',show_index=False),
                         self.plotPane))
        print('endviewtriggered')
        self.view.show()
        
    def setupVisualizationSelection(self):
        if self.visualizationDictionary=={}:
            self.optionsTabs[1][0][1].visible=False
            self.optionsTabs[1][0][0].visible=False
            self.optionsTabs[1][1].visible=False
            self.optionsTabs[1][2].visible=True
            self.optionsTabs[1][4].visible=False
        else:
            self.optionsTabs[1][0][1].visible=True
            self.param.selectVisualization.objects=list(self.visualizationDictionary.keys())
            self.optionsTabs[1][0][0].visible=True
            self.optionsTabs[1][1].visible=False
            self.optionsTabs[1][2].visible=False
            self.optionsTabs[1][4].visible=True
    
    def onVisualizationMenuClick(self,event=None):
        if event=='View Existing':
            self.optionsTabs[1][0][1].visible=True
            self.optionsTabs[1][0][0].visible=True
            self.optionsTabs[1][1].visible=False
            self.optionsTabs[1][2].visible=False
            self.optionsTabs[1][4].visible=True
        elif event.obj.clicked=='Copy Existing':
            self.optionsTabs[1][0][1].visible=False            
            self.optionsTabs[1][0][0].visible=False
            self.optionsTabs[1][1].visible=True
            self.optionsTabs[1][2].visible=False
            self.optionsTabs[1][4].visible=False
        elif event.obj.clicked=='Create New':
            self.optionsTabs[1][0][1].visible=False            
            self.optionsTabs[1][0][0].visible=False
            self.optionsTabs[1][1].visible=False
            self.optionsTabs[1][2].visible=True
            self.optionsTabs[1][4].visible=False

    def onButtonClick(self,clicked):
        currentVisualizations=list(self.visualizationDictionary.keys())
        if clicked.obj.name=='Copy':
            print('create copy')
            if self.optionsTabs[1][1][4].value=='':
                pass
            elif self.optionsTabs[1][1][4].value in currentVisualizations:
                print('visualization already exists')
            else:
                self.visualizationDictionary[self.optionsTabs[1][1][4].value]={}
                self.param.selectVisualization.objects=list(self.visualizationDictionary.keys())
                self.selectVisualization=self.optionsTabs[1][1][4].value
                self.onVisualizationMenuClick('View Existing')            
        elif clicked.obj.name=='Create':
            print('create visualization')
            if self.optionsTabs[1][2][1].value=='':
                pass
            elif self.optionsTabs[1][2][1].value in currentVisualizations:
                print('visualization already exists')
            else:
                self.visualizationDictionary[self.optionsTabs[1][2][1].value]={}
                self.param.selectVisualization.objects=list(self.visualizationDictionary.keys())
                self.selectVisualization=self.optionsTabs[1][2][1].value
                self.onVisualizationMenuClick('View Existing')
        elif clicked.obj.name=='Cancel Copy':
            self.onVisualizationMenuClick('View Existing')
        elif clicked.obj.name=='Cancel Create':
            if self.visualizationDictionary!={}:
                self.onVisualizationMenuClick('View Existing')



    @pn.depends('chartType',watch=True)
    def updateChartWidgets(self):
        #Show or hide select widgets based on chart type
        if '_x' in self.visibleByChartType[self.chartType]:
            self._xWidget.visible=True
        else:
            self._xWidget.visible=False

        if '_y' in self.visibleByChartType[self.chartType]:
            self._yWidget.visible=True
        else:
            self._yWidget.visible=False

        if '_color' in self.visibleByChartType[self.chartType]:
            self._colorWidget.visible=True
        else:
            self._colorWidget.visible=False

        if '_shape' in self.visibleByChartType[self.chartType]:
            self._shapeWidget.visible=True
        else:
            self._shapeWidget.visible=False    

        if '_size' in self.visibleByChartType[self.chartType]:
            self._sizeWidget.visible=True
        else:
            self._sizeWidget.visible=False

        if '_x2' in self.visibleByChartType[self.chartType]:
            self._x2Widget.visible=True
        else:
            self._x2Widget.visible=False

        if '_y2' in self.visibleByChartType[self.chartType]:
            self._y2Widget.visible=True
        else:
            self._y2Widget.visible=False

        if '_offsetx' in self.visibleByChartType[self.chartType]:
            self._offsetxWidget.visible=True
        else:
            self._offsetxWidget.visible=False

        if '_offsety' in self.visibleByChartType[self.chartType]:
            self._offsetyWidget.visible=True
        else:
            self._offsetyWidget.visible=False

        if '_layer' in self.visibleByChartType[self.chartType]:
            self._layerWidget.visible=True
        else:
            self._layerWidget.visible=False

        if '_row' in self.visibleByChartType[self.chartType]:
            self._rowWidget.visible=True
        else:
            self._rowWidget.visible=False

        if '_col' in self.visibleByChartType[self.chartType]:
            self._colWidget.visible=True
        else:
            self._colWidget.visible=False

        if '_facet' in self.visibleByChartType[self.chartType]:
            self._facetWidget.visible=True
        else:
            self._facetWidget.visible=False

        if '_tooltip' in self.visibleByChartType[self.chartType]:
            self._tooltipWidget.visible=True
        else:
            self._tooltipWidget.visible=False

    @pn.depends('transformeddata','chartType','_x','_y','_color','_shape','_size','_x2','_y2','_offsetx','_offsety','_layer','_row','_col','_facet','_tooltip',watch=True)
    def plot(self):
        print('updatechart')
        self.plotPane.object={}
        self.previewPlotPane.object={}

        if self.wait==False:
            try:
                if self.chartType=='Simple Bar Chart':
                    result= alt.Chart(self.transformeddata).mark_bar().encode(alt.X(field=self._x,type=self.dataTypeDictionary[self._x]),
                                                                            alt.Y(field=self._y,type=self.dataTypeDictionary[self._y])).interactive()
                elif self.chartType=='Simple Heatmap':
                    result= alt.Chart(self.transformeddata).mark_rect().encode(alt.X(field=self._x,type=self.dataTypeDictionary[self._x]),
                                                                            alt.Y(field=self._y,type=self.dataTypeDictionary[self._y]),
                                                                            alt.Color(field=self._color)).interactive()
                elif self.chartType=='Simple Histogram':
                    result= alt.Chart(self.transformeddata).mark_bar().encode(alt.X(field=self._x,type=self.dataTypeDictionary[self._x],bin=True),
                                                                            y='count()').interactive()
                elif self.chartType=='Simple Line Chart':
                    result= alt.Chart(self.transformeddata).mark_line().encode(alt.X(field=self._x,type=self.dataTypeDictionary[self._x]),
                                                                            alt.Y(field=self._y,type=self.dataTypeDictionary[self._y])).interactive()
                elif self.chartType=='Simple Scatterplot with Tooltips':
                    result= alt.Chart(self.transformeddata).mark_circle(size=60).encode(alt.X(field=self._x,type=self.dataTypeDictionary[self._x]),
                                                                                alt.Y(field=self._y,type=self.dataTypeDictionary[self._y]),
                                                                                alt.Color(field=self._color),
                                                                                tooltip=self._tooltip).interactive()
                elif self.chartType=='Simple Stacked Area Chart':
                    result= alt.Chart(self.transformeddata).mark_area().encode(alt.X(field=self._x,type=self.dataTypeDictionary[self._x]),
                                                                            alt.Y(field=self._y,type=self.dataTypeDictionary[self._y]),
                                                                            alt.Color(field=self._color)).interactive()
                elif self.chartType=='Bump Chart':
                    result=alt.Chart(self.transformeddata).mark_line(point=True).encode(
                                                                                alt.X(field=self._x,type=self.dataTypeDictionary[self._x]),
                                                                                alt.Y(field="rank").title('Rank'),
                                                                                color=alt.Color(field=self._color)
                                                                                ).transform_window(
                                                                                    rank="rank()",
                                                                                    sort=[alt.SortField(self._y, order="ascending")],
                                                                                    groupby=[self._x]
                                                                                ).interactive()
                elif self.chartType=='Violin':
                    result=alt.Chart(self.transformeddata, width=100).transform_density(
                                                                    self._y,
                                                                    as_=[self._y, 'density'],
                                                                    groupby=[self._x]
                                                                ).transform_sample(25000
                                                                ).mark_area(orient='horizontal').encode(
                                                                    alt.X('density:Q')
                                                                        .stack('center')
                                                                        .impute(None)
                                                                        .title(None)
                                                                        .axis(labels=False, values=[0], grid=False, ticks=True),
                                                                    alt.Y(self._y),
                                                                    alt.Color(self._x),
                                                                    alt.Column(self._x)
                                                                        .spacing(0)
                                                                        .header(titleOrient='bottom', labelOrient='bottom', labelPadding=0)
                                                                ).configure_view(
                                                                    stroke=None
                                                                ).interactive()       
                else:
                    result=alt.Chart().mark_bar()
            except:
                result=alt.Chart().mark_bar()
        else:
            result=alt.Chart().mark_bar()

            # try:
        self.plotPane.object=result.to_dict(format='vega')
        self.previewPlotPane.object=result.to_dict(format='vega')
        # except:
        #     pass
                
    @pn.depends('categoricalCols','quantCols','ordinalCols','valueTypeCols',watch=True,on_init=False)
    def callbackDFStructure(self) : #,target,event):
        #Adjusts available options for select widgets based on other selections
        print('callbackDFStructure')
        if self.wait==False:
            self.updateDfStructureOptions()                        

    def updateDfStructureOptions(self):
        #Adjust available options for select widgets related to dataframe structure
        self.param.categoricalCols.objects=_misc.list_difference(list(self.data.columns),self.quantCols+self.ordinalCols) 
        self.param.valueTypeCols.objects=self.categoricalCols   
        self.param.quantCols.objects=_misc.list_difference(list(self.data.columns),self.categoricalCols+self.ordinalCols)     
        self.param.ordinalCols.objects=_misc.list_difference(list(self.data.columns),self.quantCols+self.categoricalCols)             
        self.excludedCols=_misc.list_difference(list(self.data.columns),self.ordinalCols+self.quantCols+self.categoricalCols)                     
        self.excludedColsString=', '.join(self.excludedCols)

        self.param.singleSelectSourceDataFilterCols.objects=self.categoricalCols
        self.param.multiSelectSourceDataFilterCols.objects=self.categoricalCols
        self.param.singleSelectSourceDataFilterCols.objects=_misc.list_difference(self.categoricalCols,self.multiSelectSourceDataFilterCols)
        self.param.multiSelectSourceDataFilterCols.objects=_misc.list_difference(self.categoricalCols,self.singleSelectSourceDataFilterCols)

        self.valueCols=self.ordinalCols+self.quantCols
        if len(self.valueCols)>0:
            self.editValueSuffixSelect=self.valueCols[0]
            self.editValueSuffixText=self.valueDescriptorSuffixes.get(self.editValueSuffixSelect,self.valueCols[0])

        for key in self.ordinalCols+self.quantCols:
            if key not in list(self.valueDescriptorSuffixes.keys()):
                self.valueDescriptorSuffixes[key]=key

        if len(self.valueTypeCols)==0:
            self.useValueDescriptorVisible=False
            self.useValueDescriptorAsCategory=False
        else:
            self.useValueDescriptorVisible=True

        self.updateDfFilters()

    @pn.depends('editValueSuffixSelect',watch=True)
    def getValueSuffixText(self):
        self.editValueSuffixText=self.valueDescriptorSuffixes.get(self.editValueSuffixSelect,"")

    @param.depends('useValueDescriptorAsCategory','useValueDescriptorVisible',watch=True)
    def showEditValueSuffix(self):
        if (self.useValueDescriptorAsCategory==False) and (self.useValueDescriptorVisible==True):
            self.editValueSuffixVisible=True
        else:
            self.editValueSuffixVisible=False

    @param.depends('singleSelectSourceDataFilterCols','multiSelectSourceDataFilterCols',watch=True)
    def updateDfFilters(self):
        #Objective: update set of widgets for dataframe filters based on singleSelectSourceDataFilterCols and multiSelectSourceDataFilterCols
        singleSelectList=self.singleSelectSourceDataFilterCols
        multiSelectList=self.multiSelectSourceDataFilterCols
        
        tempSingleSelectWidgets=[]
        for paramname in singleSelectList:
            temp=pn.widgets.Select(name=paramname,options=self.data[paramname].unique().tolist(),width=300)
            temp.param.watch(self.filterDataframe,"value")
            tempSingleSelectWidgets.append(temp)

        tempMultiSelectWidgets=[]
        for paramname in multiSelectList:
            temp=pn.widgets.MultiChoice(name=paramname,options=self.data[paramname].unique().tolist(),width=300)
            temp.param.watch(self.filterDataframe,"value")
            tempMultiSelectWidgets.append(temp)
        
        self.singleSelectSourceDataFilters=tempSingleSelectWidgets
        self.multiSelectSourceDataFilters=tempMultiSelectWidgets

        self.dataFilters.clear()        
        if len(self.singleSelectSourceDataFilters)>0 or len(self.multiSelectSourceDataFilters)>0:
            tempFilterWidgets=self.singleSelectSourceDataFilters+self.multiSelectSourceDataFilters
            self.dataFilters[:]=tempFilterWidgets
            self.dataFilters.visible=True
        else:
            self.dataFilters.visible=False

        self.filterDataframe()

    def filterDataframe(self,event=None):
        if self.wait==False:
            filterlist=[]
            filtervalues=[]
            singlefilterlist=[]
            filtereddata=self.data.copy()
            filtereddata=filtereddata.drop(columns=self.excludedCols)

            for filter in self.singleSelectSourceDataFilters:
                if filter.value!=None:
                    filterlist.append(filter.name)
                    filtervalues.append(filter.value)
                    singlefilterlist.append(filter.name)

            for filter in self.multiSelectSourceDataFilters:
                if filter.value!=[]:
                    filterlist.append(filter.name)
                    filtervalues.append(filter.value)                

            if len(filterlist)>0 and len(filtervalues)>0:
                f=pd.DataFrame({'field':filterlist,'value':filtervalues})
                f['type']=np.where(f['value'].apply(lambda x: isinstance(x,list)),1,2)
                f.loc[f['type']==2,'value']=f.loc[f['type']==2,'value'].apply(lambda x: [x])
                f=f.drop(columns=['type'])
                f=f.groupby('field')['value'].apply('min')
                filtereddata=filtereddata[np.logical_and.reduce([filtereddata[col].isin(val) for col,val in f.items()])]  
                filtereddata=filtereddata.drop(columns=singlefilterlist)
            
            self.filtereddata=filtereddata.copy()
            self.transformData()

    @pn.depends('useValueDescriptorVisible','useValueDescriptorAsCategory','valueDescriptorSuffixes',watch=True)
    def transformData(self,event=None,on_init=False):
        print('transforming data')
        if self.wait==False:
            try:
                filtereddata=self.filtereddata
                if self.useValueDescriptorVisible==True:
                    if self.useValueDescriptorAsCategory==True:
                        self.transformeddata=filtereddata
                        self.transformedValueCols=_misc.list_difference(self.transformeddata.columns.tolist(),_misc.list_intersection(self.categoricalCols.copy(),filtereddata.columns.tolist()))
                        self.transformedCategoricalCols=_misc.list_intersection(self.categoricalCols.copy(),filtereddata.columns.tolist())
                        self.transformedCols=self.transformeddata.columns.tolist()  
                        self.dataShorthandDictionary={}
                        self.dataTypeDictionary={}
                        for col in self.transformedCols:
                            if col in self.transformedValueCols:
                                if col in self.ordinalCols:
                                    self.dataShorthandDictionary[col]=col+':O'
                                    self.dataTypeDictionary[col]='ordinal'
                                else:
                                    self.dataShorthandDictionary[col]=col+':Q'
                                    self.dataTypeDictionary[col]='quantitative'
                            else:
                                self.dataShorthandDictionary[col]=col+':N' 
                                self.dataTypeDictionary[col]='nominal'              
                    else:
                        indexcols=_misc.list_intersection(self.categoricalCols.copy(),filtereddata.columns.tolist())

                        try:
                            indexcols.remove(self.valueTypeCols[0])
                        except:
                            pass
                        transformeddata = filtereddata.pivot(index=indexcols, columns=self.valueTypeCols, values=self.valueCols)
                        suffixes=self.valueDescriptorSuffixes.copy()
                        for key,val in suffixes.items():
                            if val=="":
                                suffixes[key]=""
                            else:
                                suffixes[key]=" "+suffixes[key]

                        templist=[f'{val} {suffixes[col]}:Q' if col in self.quantCols else f'{val} {suffixes[col]}:O' for col, val in transformeddata.columns ]
                        self.dataShorthandDictionary={}
                        self.dataTypeDictionary={}
                        for i in templist:
                            self.dataShorthandDictionary[i.split(':')[0]]=i  
                            if i.split(':')[1]=='Q':
                                self.dataTypeDictionary[i.split(':')[0]]='quantitative'
                            else:
                                self.dataTypeDictionary[i.split(':')[0]]='ordinal'

                        for i in indexcols:
                            self.dataShorthandDictionary[i]='nominal'            
                    
                        transformeddata.columns = [f'{val} {suffixes[col]}' for col, val in transformeddata.columns]

                        transformeddata.reset_index(inplace=True)
                        self.transformeddata=transformeddata
                        self.transformedValueCols=_misc.list_difference(self.transformeddata.columns.tolist(),indexcols)
                        self.transformedCategoricalCols=indexcols
                        self.transformedCols=self.transformeddata.columns.tolist()
                else:
                    self.transformeddata=filtereddata
                    self.transformedValueCols=_misc.list_difference(self.transformeddata.columns.tolist(),_misc.list_intersection(self.categoricalCols.copy(),filtereddata.columns.tolist()))
                    self.transformedCategoricalCols=_misc.list_intersection(self.categoricalCols.copy(),filtereddata.columns.tolist())
                    self.transformedCols=self.transformeddata.columns.tolist()  
                    self.dataShorthandDictionary={}
                    self.dataTypeDictionary={}
                    for col in self.transformedCols:
                        if col in self.transformedValueCols:
                            if col in self.ordinalCols:
                                self.dataShorthandDictionary[col]=col+':O'
                                self.dataTypeDictionary[col]='ordinal'
                            else:
                                self.dataShorthandDictionary[col]=col+':Q'
                                self.dataTypeDictionary[col]='quantitative'
                        else:
                            self.dataShorthandDictionary[col]=col+':N' 
                            self.dataTypeDictionary[col]='nominal'    
            except:
                pass                  

    @param.depends('sourceData',watch=True)
    def updateData(self):
        print('updating data')
        data=self.sourceDataCodeDict[self.sourceData].copy()

        if self.sourceData!=self.initialSourceData:
            self.changedSourceData=True

        if self.changedSourceData==True:
            quantCols=list(data.columns)
            categoricalCols=[]
            ordinalCols=[]
            excludedCols=[]
            valueTypeCols=[]
        else:
            quantCols=_misc.list_intersection(list(data.columns),self.quantCols)
            categoricalCols=_misc.list_intersection(list(data.columns),self.categoricalCols)
            ordinalCols=_misc.list_intersection(list(data.columns),self.ordinalCols)
            excludedCols=_misc.list_difference(list(data.columns),self.excludedCols)
            valueTypeCols=_misc.list_intersection(categoricalCols,self.valueTypeCols)

        if len(_misc.list_intersection(list(data.columns),categoricalCols+quantCols+ordinalCols))==0:
            excludedCols=list(data.columns)
        else:
            quantCols=_misc.list_intersection(list(data.columns),quantCols)

        excludedCols=_misc.list_difference(list(data.columns),quantCols+categoricalCols+ordinalCols)
        excludedColsString=', '.join(excludedCols)
        print('made it here')
        self.wait=True
        self.quantCols,self.categoricalCols,self.valueTypeCols,self.ordinalCols,self.excludedCols,self.excludedColsString,self.data=quantCols,categoricalCols,valueTypeCols,ordinalCols,excludedCols,excludedColsString,data
        print('and here')
        self.wait=False
        self.updateDfStructureOptions()

    #region OLD
    # @param.depends("data", "_zdivider","incltable","_radarbumplegend","user_chart_type",watch=True)
    # def _update_plot(self, *event):
    #     #Update to match user_chart_type if needed #NEED TO UPDATE TO MAKE SURE THAT CHART TYPE FUNCTIONABILITY IS CHANGED 
    #     if (self.chartType != self.user_chart_type) & (self.user_select):
    #         self.chartType = self.user_chart_type
    #         self.numdimensions = self._get_chart_type(self.chartType)

    #     def create_radar_graph():
    #         def _prepRadarData(val,leg,met):
    #             #reshape data
    #             tempdf=self.data.copy()
    #             metriclist=_misc.uniqueList(tempdf[met].unique().tolist())

    #             #Cap at eight metrics
    #             if len(metriclist)>8:
    #                 metriclist=metriclist[:8]
    #                 tempdf=tempdf[tempdf[met].isin(metriclist)]

    #             tempdf=tempdf.pivot(index=leg,columns=met,values=val).reset_index(leg)
    #             tempdf=tempdf.rename_axis(None,axis=1).reset_index().drop(columns=['index'])
    #             tempdf=tempdf.set_index(leg)
    #             return tempdf        
            
    #         valuescol=self._x0
    #         try:
    #             radarlegendcol=self._radarbumplegend
    #         except:
    #             radarlegendcol=self._radarbumplegend.default
            
    #         metriccol=[x for x in self.data.columns if x not in [valuescol,radarlegendcol]][0]

    #         radardata=_prepRadarData(valuescol,radarlegendcol,metriccol)

    #         #PLOT
    #         N = len(radardata.columns) #Equal to number of data columns
    #         theta = np.linspace(0, 2*np.pi, N, endpoint=False)

    #         spoke_labels = radardata.columns.tolist() #metrics

    #         legend_labels = radardata.index.tolist() #strategies

    #         chart_data = radardata.values.tolist() #values


    #         fig, ax = plt.subplots(figsize=(8, 8), subplot_kw=dict(projection='polar'))
    #         ax.set_theta_zero_location('N')
    #         ax.set_rlabel_position(0)
    #         ax.set_xticks(theta)
    #         ax.set_xticklabels(spoke_labels)

    #         colors = ['b', 'r', 'g', 'c', 'm', 'y', 'k', 'orange', 'purple', 'brown', 'pink', 'olive']

    #         for d, color in zip(chart_data, colors[:len(legend_labels)]):
    #             # Plot the data points
    #             line, = ax.plot(theta, d, color=color,linewidth=1)
    #             ax.fill(theta, d, color=color, alpha=0.25)

    #             # Append the first data point to the end of the line
    #             x, y = line.get_data()
    #             x = np.append(x, x[0])
    #             y = np.append(y, y[0])
    #             line.set_data(x, y)

    #         legend = ax.legend(legend_labels, loc=(0.9, .95),
    #                                     labelspacing=0.2, fontsize='medium')

    #         plt.tight_layout()
    #         plt.close(fig)
    #         return fig
    #         # except:
    #         #     return 

    #     def create_bump_chart():
    #         valuescol=self._x0
    #         try:
    #             radarlegendcol=self._radarbumplegend
    #         except:
    #             radarlegendcol=self.param._radarbumplegend.default
            
    #         metriccol=[x for x in self.data.columns if x not in [valuescol,radarlegendcol]][0]
               
    #         bumpdata=self.data.copy()

    #         #Reset Rank by ['Metric','Statistic']
    #         def re_rank(data):
    #             for metric in data[metriccol].unique().tolist():
    #                 selected = data.loc[data[metriccol] == metric, 'Rank'].to_list()
    #                 ranked = sorted(selected)
    #                 selected = [ranked.index(x) + 1 for x in selected]
    #                 data.loc[data[metriccol] == metric, 'Rank'] = selected  
    #             return data 
             
    #         bumpdata = re_rank(bumpdata)

    #         #Cap at eight metrics
    #         metriclist=_misc.uniqueList(bumpdata[metriccol].unique().tolist())
    #         if len(metriclist)>8:
    #             metriclist=metriclist[:8]
    #             bumpdata=bumpdata[bumpdata[metriccol].isin(metriclist)]            
            
    #         print('bumpdata',bumpdata.head(5))
    #         _bump_chart = alt.Chart(bumpdata).mark_line(point=True).encode(
    #             x = str(metriccol) + ':O',
    #             y = 'Rank:O',
    #             color = alt.Color(self._legendcol+":N")
    #         ).transform_window(
    #             rank='rank()',
    #             sort=[alt.SortField("Rank", order="descending")],
    #             groupby=[metriccol]
    #         ).properties(
    #             width = 1000,
    #             height = 200,
    #         title="Bump Chart for Strategies"
    #         ).interactive() 

    #         return _bump_chart

    #     def create_histogram_chart():
    #         data = self.data.copy()
    #         value_column = data[str(self.data.columns.tolist()[1])]

    #         domain = [np.percentile(value_column, 5), np.percentile(value_column, 95)]
            
    #         #HISTOGRAM
    #         histogram_chart = alt.Chart(data).mark_bar(
    #             clip=True,
    #             opacity=0.5,
    #             binSpacing=1
    #         ).encode(
    #             alt.X(self._x0+':Q').bin(maxbins=1000).scale(domain=domain),
    #             alt.Y('count()').stack(None).axis(title='Count of Values'),
    #             alt.Color(self._legendcol+':N').scale(reverse=True)
    #         ).properties(
    #             width=1000,
    #             height=500
    #         ).interactive()
    #         return histogram_chart
        
    #     def create_box_chart():
    #         data = self.data.copy()
    #         strategy_col = self._legendcol
    #         value_col = self._x0

    #         if self.chartType=='Box-Outliers':
    #         #BOX PLOT
    #             box_plot = alt.Chart(data).mark_boxplot(
    #                 opacity=0.8
    #             ).encode(
    #                 alt.X(value_col+':Q'),
    #                 alt.Y(strategy_col+':N'),
    #                 alt.Color(strategy_col+':N').scale(reverse=True)
    #             ).properties(
    #                 width=1000,
    #                 height=300
    #             ).interactive()

    #         elif self.chartType=='Box-MinMax':
    #             #BOX PLOT
    #             box_plot = alt.Chart(data).mark_boxplot(
    #                 extent='min-max',
    #                 opacity=0.8
    #             ).encode(
    #                 alt.X(value_col+':Q'),
    #                 alt.Y(strategy_col+':N'),
    #                 alt.Color(strategy_col+':N').scale(reverse=True)
    #             ).properties(
    #                 width=1000,
    #                 height=300
    #             ).interactive()
            
    #         print('box chart updated')
    #         return box_plot
        
    #     def create_violin_chart():
    #         data = self.data.copy()
    #         violin_chart = data.hvplot.violin(y=self._x0,by=self._legendcol,height=600,width=800).opts(
    #                 yformatter=NumeralTickFormatter(format='0,0')
    #         )
    #         return violin_chart
        
    #     def create_kde():
    #         data = self.data.copy()
    #         kde_chart = data.hvplot.kde(by=self._legendcol,height=600,width=1000).opts(
    #                 xformatter=NumeralTickFormatter(format='0,0')
    #         )
    #         return kde_chart
        
    #     def create_column_chart():
    #         data = self.data.copy()
    #         if self.chartType == 'Column':
    #             strategy_col = str(data.columns[0])
    #             metric_col = str(data.columns[1])
    #             value_col = str(data.columns[2])
    #             color_col = strategy_col
    #         elif self.chartType == 'Column-Flipped':
    #             strategy_col = str(data.columns[1])
    #             metric_col = str(data.columns[0])
    #             value_col = str(data.columns[2])
    #             color_col = metric_col
    #         try:
    #             extra_col = str(data.columns[3])
    #             data[extra_col] = data[extra_col].fillna(0)
    #         except:
    #             print("Must be second value column. Ex: 'Percentile'")

    #         #Set Extra Col (Percentile) Title
    #         if self.groupByLegendColumn:
    #             extra_col_title = 'Sum of ' + extra_col
    #         else:
    #             extra_col_title = 'Average ' + extra_col

    #         graph_lst = []
    #         for strategy in sorted(data[strategy_col].unique().tolist()):
    #             _data = data[data[strategy_col] == strategy]
               
    #             base = alt.Chart(_data).encode(
    #                 alt.X(metric_col+':N')
    #             )

    #             area = base.mark_bar(opacity=0.3).encode(
    #                     x=metric_col+':N',
    #                     y=value_col+':Q',
    #                     color = color_col+':N'
    #                 )

    #             line = base.mark_line(stroke='#5276A7', interpolate='linear').encode(
    #                 alt.Y('average('+extra_col+')').title(extra_col_title, titleColor='#5276A7').axis(format='.1%')
    #             )

    #             chart_out = alt.layer(area, line).resolve_scale(
    #                 y='independent'
    #             ).properties(
    #                 width=200,
    #                 height=200,
    #                 title = strategy
    #             )
    #             graph_lst.append(chart_out)

    #         #Split the graphs into a grid that has max 3 columns
    #         def split_graph_lst(graph_lst, max_cols=3):
    #             output_lst = []
    #             updated_graph_lst = [graph_lst[i:i+max_cols] for i in range(0, len(graph_lst), max_cols)]
    #             for split_lst in updated_graph_lst:
    #                 output_lst.append(alt.hconcat(*split_lst))
    #             return alt.vconcat(*output_lst)
                    
    #         output = split_graph_lst(graph_lst)
    #         return output
        
    #     def create_heatmap():
    #         data = self.data.copy()
    #         strategy_col = self._legendcol
    #         metric_col = data.columns[1]
    #         value_col = self._x0
    #         rank_col = data.columns[3]

    #         base = alt.Chart(
    #             data,
    #             height=100,
    #             width=300,
    #             title='Bar Chart by ' + metric_col
    #         )

    #         bar = base.mark_bar().encode(
    #             alt.X(metric_col+':N', axis=alt.Axis(labelAngle=-45)).title(None),
    #             alt.Y(value_col+':Q'),
    #             alt.Color(strategy_col+':N').legend(orient='top').scale(scheme='paired')
    #         )

    #         bump = base.mark_line(point=True).encode(
    #             alt.X(metric_col+':N', axis=alt.Axis(labelAngle=-45)),
    #             alt.Y(rank_col+':O', axis=alt.Axis(grid=True)),
    #             color = alt.Color(strategy_col+":N")
    #         ).transform_window(
    #             rank='rank()',
    #             sort=[alt.SortField(rank_col, order="descending")],
    #             groupby=[metric_col+':N']
    #         ).properties(
    #             width = 300,
    #             height = 100,
    #             title="Bump Chart for " + strategy_col
    #         )

    #         return bar & bump
        
    #     def create_scatter():
    #         data = self.data.copy()
    #         scatter_chart = alt.Chart(data).mark_point().encode(
    #             x=str(self._x1),
    #             y=str(self._y1),
    #             color=self._legendcol+':N'
    #         ).properties(
    #                 width=500,
    #                 height=500
    #         ).interactive()
            
    #         return scatter_chart

    #     def create_bubble_chart():
    #         data = self.data.copy()
    #         #data[self._z1] = data[self._z1] // self._zdivider
    #         bubble_chart = alt.Chart(data).mark_point(filled=True).encode(
    #             x=str(self._x1),
    #             y=str(self._y1),
    #             color=self._legendcol+':N',
    #             size=str(self._z1)
    #         ).properties(
    #                 width=500,
    #                 height=500
    #         ).interactive()
            
    #         return bubble_chart
        
    #     def create_pie_chart():
    #         #value_col = self._x0
    #         data = self.data.copy()
    #         pie_chart = alt.Chart(data).mark_arc().encode(
    #             theta=self._x0+':Q',
    #             color=self._legendcol+':N'
    #         ).properties(
    #             width=500,
    #             height=500
    #         ).interactive()
    #         return pie_chart
        
    #     def create_radial_chart():
    #         data = self.data.copy()

    #         if self.numdimensions == 1:
    #             base = alt.Chart(data).encode(
    #                 alt.Theta(str(self._x0)+':Q').stack(True),
    #                 alt.Radius(str(self._x0)).scale(type='sqrt',zero=True),
    #                 color=str(self._legendcol)+':N'
    #             ).properties(
    #                 width=500,
    #                 height=500
    #             )

    #             c1 = base.mark_arc(innerRadius=20, stroke='#fff')
    #             c2 = base.mark_text(radiusOffset=50).encode(
    #                 text=alt.Text(str(self._x0)+':Q',format='0,.2f')
    #             )
    #             return c1+c2 
            
    #         elif self.numdimensions == 2:

    #             base = alt.Chart(data).encode(
    #                 alt.Theta(str(self._x1)+':Q').stack(True),
    #                 alt.Radius(str(self._y1)).scale(type='sqrt',zero=True),
    #                 color=str(self._legendcol)+':N'
    #             ).properties(
    #                 width=500,
    #                 height=500
    #             )

    #             c1 = base.mark_arc(innerRadius=20, stroke='#fff')
    #             return c1       
        
    #     def create_waterfall_chart():
    #         data = self.data.copy()
    #         data.rename(columns={self._legendcol:'label',self._x0:'amount'},inplace=True)
    #         new_row_end = pd.DataFrame({'label': ['End'], 'amount': [0]})
    #         source = pd.concat([data, new_row_end])

    #         # The "base_chart" defines the transform_window, transform_calculate, and X axis
    #         base_chart = alt.Chart(source).transform_window(
    #             window_sum_amount="sum(amount)",
    #             window_lead_label="lead(label)",
    #         ).transform_calculate(
    #             calc_lead="datum.window_lead_label === null ? datum.label : datum.window_lead_label",
    #             calc_prev_sum="datum.label === 'End' ? 0 : datum.window_sum_amount - datum.amount",
    #             calc_amount="datum.label === 'End' ? datum.window_sum_amount : datum.amount",
    #             calc_text_amount="(datum.label !== 'Begin' && datum.label !== 'End' && datum.calc_amount > 0 ? '+' : '') + datum.calc_amount",
    #             calc_center="(datum.window_sum_amount + datum.calc_prev_sum) / 2",
    #             calc_sum_dec="datum.window_sum_amount < datum.calc_prev_sum ? datum.window_sum_amount : ''",
    #             calc_sum_inc="datum.window_sum_amount > datum.calc_prev_sum ? datum.window_sum_amount : ''",
    #         ).encode(
    #             x=alt.X(
    #                 "label:O",
    #                 axis=alt.Axis(title=str(self._legendcol), labelAngle=-45),
    #                 sort=None,
    #             )
    #         )

    #         # alt.condition does not support multiple if else conditions which is why
    #         # we use a dictionary instead. See https://stackoverflow.com/a/66109641
    #         # for more information
    #         color_coding = {
    #             "condition": [
    #                 {"test": "datum.label === 'Begin' || datum.label === 'End'", "value": "#878d96"},
    #                 {"test": "datum.calc_amount < 0", "value": "#24a148"},
    #             ],
    #             "value": "#fa4d56",
    #         }

    #         bar = base_chart.mark_bar(size=45).encode(
    #             y=alt.Y("calc_prev_sum:Q", title="Amount"),
    #             y2=alt.Y2("window_sum_amount:Q"),
    #             color=color_coding,
    #         )

    #         # The "rule" chart is for the horizontal lines that connect the bars
    #         rule = base_chart.mark_rule(
    #             xOffset=-22.5,
    #             x2Offset=22.5,
    #         ).encode(
    #             y="window_sum_amount:Q",
    #             x2="calc_lead",
    #         )

    #         # Add values as text
    #         text_pos_values_top_of_bar = base_chart.mark_text(
    #             baseline="bottom",
    #             dy=-4
    #         ).encode(
    #             text=alt.Text("calc_sum_inc:N",format='.2s'),
    #             y="calc_sum_inc:Q"
    #         )
    #         text_neg_values_bot_of_bar = base_chart.mark_text(
    #             baseline="top",
    #             dy=4
    #         ).encode(
    #             text=alt.Text("calc_sum_dec:N",format='.2s'),
    #             y="calc_sum_dec:Q"
    #         )
    #         text_bar_values_mid_of_bar = base_chart.mark_text(baseline="middle").encode(
    #             text=alt.Text("calc_text_amount:N",format='.2s'),
    #             y="calc_center:Q",
    #             color=alt.value("white"),
    #         )

    #         return alt.layer(
    #             bar,
    #             rule,
    #             text_pos_values_top_of_bar,
    #             text_neg_values_bot_of_bar,
    #             text_bar_values_mid_of_bar
    #         ).properties(
    #             width=1000,
    #             height=600
    #         )

    #     if self.data is None:
    #         return

    #     try:
    #         self._scale=1/(self._zdivider)
    #     except:
    #         pass

    #     data = self.data.copy()

    #     #RUN CHARTS BASED ON self.chartType
    #     try:
    #         if self.chartType in ['Column','Column-Flipped']:
    #             column_chart = create_column_chart()
    #             self.plot_panel.object = {}
    #             self.plot_panel.object = column_chart.to_dict(format='vega')
            
    #         elif self.chartType=='Radar':
    #             graphResult=create_radar_graph()
    #             self.plot_panel.object = graphResult 
            
    #         elif self.chartType=='Bump':
    #             graphResult=create_bump_chart()
    #             self.plot_panel.object = graphResult 
            
    #         elif self.chartType=='Bump and Radar':
    #             graphResult=create_radar_graph()
    #             graphResult2=create_bump_chart()
    #             self.plot_panel.object = graphResult 

    #             self.plot_panel2.object= graphResult2.to_dict(format='vega')
            
    #         elif self.chartType=='Bubble':
    #             graphResult = create_bubble_chart()
    #             self.plot_panel.object = graphResult.to_dict(format='vega')
            
    #         elif self.chartType=='Bubble incl z':
    #             self.plot_panel.object = data.hvplot(x=self._x1, y=self._yname,s=self._zname,by=self.legendCols, kind='scatter',scale=self._scale)
            
    #         elif self.chartType=='Violin':
    #             self.plot_panel.object = create_violin_chart()

    #         elif self.chartType=='Line':
    #             pass

    #         elif self.chartType=='Scatter':
    #             graphResult = create_scatter()
    #             self.plot_panel.object = graphResult.to_dict(format='vega')

    #         elif self.chartType=='Heatmap':
    #             heatmap = create_heatmap()
    #             self.plot_panel.object={}
    #             self.plot_panel.object= heatmap.to_dict(format='vega')

    #         elif self.chartType in ['Table-1dim','Table-2dim','Table-3dim']:
    #             self.plot_panel.value = self.data

    #         elif self.chartType=='KDE':
    #             self.plot_panel.object = create_kde()

    #         elif self.chartType in ['Box-Outliers','Box-MinMax']:
    #             boxResult = create_box_chart()
    #             self.plot_panel.object={}
    #             self.plot_panel.object = boxResult.to_dict(format='vega')

    #         elif self.chartType=='Histogram':
    #             histogramResult = create_histogram_chart()
    #             self.plot_panel.object = histogramResult.to_dict(format='vega')

    #         elif self.chartType == 'Pie Chart':
    #             pie_chart = create_pie_chart()
    #             self.plot_panel.object = pie_chart.to_dict(format='vega')
            
    #         elif self.chartType in ['Radial Chart','Radial Chart-2dim']:
    #             self.plot_panel.object={}
    #             radial_chart = create_radial_chart()
    #             self.plot_panel.object = radial_chart.to_dict(format='vega')

    #         elif self.chartType == 'Waterfall':
    #             self.plot_panel.object={}
    #             waterfall_chart = create_waterfall_chart()
    #             self.plot_panel.object = waterfall_chart.to_dict(format='vega')
            
    #         if self.incltable==True:
    #             self.data_panel_card.visible=True
    #             self.data_panel.value=data
    #         else:
    #             self.data_panel_card.visible=False
    #     except:
    #         pass
    #endregion