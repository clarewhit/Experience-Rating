import pandas as pd
import param
import panel as pn
import polars as pl
import _misc as _misc
import modelFunctions as fns
import numpy as np
from bokeh.models.widgets.tables import CheckboxEditor, NumberEditor, SelectEditor
import _myLogging

MYLOGGER = _myLogging.get_logger("Dataforms") 

class Dataform(param.Parameterized):
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

    #Parameters - controls
    viewEditModeBoolean=param.Selector(default=True,objects=[True,False])
    viewEditModeText=param.Selector(default="Switch to Edit Mode",objects=["Switch to Edit Mode","Switch to View Mode"])
    viewEditModeVisible=param.Boolean(default=True)
    relatedSpecsVisible=param.Boolean(default=True)
    selectActionList=param.List(default=["Copy","Copy Checked Item(s)",None,"Create New","Create New Item(s)",None,"Delete","Delete Checked Item(s)"])
    selectActionVisible=param.Boolean(default=False)
    save=param.Action()
    undo=param.Action()

    #Parameters - dataform
    spec=param.Selector()
    selected=param.Selector()
    viewEdit=param.Selector(objects=['Edit','View'])
    relatedSpecs=param.List(default=['A','B','C'])
    dataformWidgetDict=param.Dict(default={})
    dataformEditVersion=param.Parameter
    dataformViewVersion=param.Parameter
    thisdataform=param.Parameter

    def __init__(self, parent,**params):
        self.parent = parent
        super().__init__(**params)
     
        self.dataTypes=_misc.dfReplaceNanNone(parent.panelDicts["dict_panelDataTypes"]).to_pandas()
        self.dataformInfo=_misc.dfReplaceNanNone(parent.panelDicts["dict_panelDataforms"]).to_pandas()    
        self.dataformCardInfo=_misc.dfReplaceNanNone(parent.panelDicts["dict_panelDataformCards"]).to_pandas()               
        self.dictMapWidgetTypes=parent.panelDicts["dict_panelMapWidgetTypes"]            
        self.specs=parent.analysis.activespecs
        self.specMap=dict(zip(self.dataformInfo['Dataform Name'],self.dataformInfo['Spec Sheet']))      #Map selected menu item to spec sheet name
        self.blankWidgetDict=parent.dataformWidgetBlanksDict
        self.specWidgetDict=parent.specWidgetDict
        self.dataformWidgetDict=parent.dataformWidgetDict
        self.dataform_panel = pn.Row()
        self._getdataform()

        self.param.selected.objects=list(self.dataformWidgetDict[self.spec].keys())   
        if self.selected==None:
            self.selected=self.param.selected.objects[0]

        if self.dataformInfo['Menu Button Name for Modals'][self.dataformInfo['Spec Sheet']==self.spec].iloc[0] is None:
            self.relatedSpecsVisible=False

        self.createView()
        self.save=self._save
        self.undo=self._undo    

    @pn.depends("viewEditModeBoolean", watch = True)
    def toggleViewEdit(self):
        if self.viewEditModeBoolean==True:
            self.viewEditModeText="Switch to Edit Mode"
            self.selectActionVisible=False
            self.viewEdit='View'
        else:
            self.viewEditModeText="Switch to View Mode"
            self.selectActionVisible=True        
            self.viewEdit='Edit'
        self._getdataform()

    def _save(self,event):
        print('save')

    def _undo(self,event):
        print('undo')

    def _edit(self,event):
        print('edit')
        print(event)

    def _related(self,event):
        print('related')
        print(event) 

    @param.depends('selected','viewEdit',watch=True)
    def _getdataform(self):   
        try:
            self.dataformEditVersion=self.dataformWidgetDict[self.spec][self.selected]['Edit']
            self.dataformViewVersion=self.dataformWidgetDict[self.spec][self.selected]['View']

            if self.viewEdit=='Edit':
                self.thisdataform=self.dataformEditVersion
            else:
                self.thisdataform=self.dataformViewVersion
        except:
            try:
                self.thisdataform=self.dataformViewVersion
            except:
                self.thisdataform=pn.Column()
        
        self.dataform_panel.clear()

        if self.viewEdit=='Edit':
            self.dataform_panel.append(self.dataformEditVersion)              
        else:
            self.dataform_panel.append(self.dataformViewVersion)              

    def createView(self):
        self.relatedMenuButton=pn.widgets.MenuButton(name="View/Edit Related Assumptions",icon='hierarchy-2',icon_size='1.5em',
                                                        items=self.param.relatedSpecs,visible=self.param.relatedSpecsVisible,button_type="primary", stylesheets=[self.menubuttoncss])
        pn.bind(self._related, self.relatedMenuButton.param.clicked, watch=True)

        self.editMenuButton=pn.widgets.MenuButton(name="Copy/Create/Delete",icon='edit',icon_size='1.5em',width=175,height=50,
                                                        items=self.param.selectActionList,visible=self.param.selectActionVisible,button_type="light",stylesheets=[self.menubuttoncss])
        pn.bind(self._edit, self.editMenuButton.param.clicked, watch=True)

        self.view= pn.Column(
                                pn.Row(pn.pane.Markdown("#### "+self.dataformInfo['Text for Select'][self.dataformInfo['Spec Sheet']==self.spec].iloc[0]+":"),
                                        pn.widgets.Select.from_param(self.param.selected,name="",options=self.param.selected.objects),
                                        pn.Spacer(width=10),
                                        pn.widgets.Toggle.from_param(self.param.viewEditModeBoolean,button_type="primary",icon='switch-2',icon_size='1.5em',name=self.param.viewEditModeText,
                                            visible=self.param.viewEditModeVisible),
                                        pn.Spacer(width=10),                                            
                                        self.relatedMenuButton,
                                        align=('start','center'),
                                        styles={'background':'#ffffff'},
                                        margin=0),
                                pn.Spacer(height=5),
                                pn.Row(self.editMenuButton,
                                    pn.Spacer(width=10),
                                    pn.widgets.Button.from_param(self.param.save,name='Save Changes',icon='device-floppy',icon_size='1.5em',button_type="light",visible=self.param.selectActionVisible,stylesheets=[self.menubuttoncss]),
                                    pn.Spacer(width=10),
                                    pn.widgets.Button.from_param(self.param.undo,name='Undo Unsaved Changes',icon='arrow-back-up',icon_size='1.5em',button_type="light",visible=self.param.selectActionVisible,stylesheets=[self.menubuttoncss]),
                                    styles={'background':'#ffffff'},
                                    margin=0
                                    ),  
                          pn.pane.HTML(width_policy='max',styles={'height':'2px','background-color': '#1d5aa5'}),                                                                    
                          pn.Spacer(height=5),
                          self.dataform_panel
                          )            

