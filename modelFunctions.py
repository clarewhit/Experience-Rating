import polars as pl
import pandas as pd
import os
import math
import _misc
from IPython.display import display
import datetime
import math
import _myLogging
import xlwings as xw

infiniteloss=999999999999
MYLOGGER = _myLogging.get_logger("Functions")  

# Clean Initial Specs for Ceded Loss Layers
def cleanCededLossLayerSpecs(df):
    MYLOGGER.debug('Start cleanCededLossLayerSpecs')  
    def cleanLimitsRetentionsALAE(df, limfield, retfield, alaefield, resultfield):
        df = df.with_columns(
            pl.when((pl.col(limfield).is_null())&(pl.col(retfield).is_null()))
            .then(pl.lit(False))
            .when((pl.col(limfield).is_null())&(pl.col(retfield)==0))
            .then(pl.lit(False))
            .otherwise(pl.lit(True))
            .alias(resultfield))

        df=df.with_columns(
            [pl.when(pl.col(resultfield)==False).then(pl.lit(0)).otherwise(pl.col(retfield)).alias(retfield),
            pl.when(pl.col(resultfield)==False).then(pl.lit(999999999)).otherwise(pl.col(limfield)).alias(limfield)])

        df = df.with_columns(
            pl.when(pl.col(resultfield)==True)
            .then(pl.when(pl.col(limfield).is_null())
                .then(pl.lit(infiniteloss))
                .otherwise(pl.col(limfield)))
            .otherwise(pl.col(limfield))
            .alias(limfield))

        df = df.with_columns(
            pl.when(pl.col(resultfield)==True)
            .then(pl.when(pl.col(retfield).is_null())
                .then(pl.lit(0))
                .otherwise(pl.col(retfield)))
            .otherwise(pl.col(retfield))
            .alias(retfield))

        df = df.with_columns(
            pl.when(pl.col(resultfield) == True)
            .then(
                pl.when(pl.col(alaefield).is_null())
                .then(pl.lit("Pro Rata"))
                .otherwise(pl.col(alaefield))
            )
            .otherwise(pl.lit(None))
            .alias(alaefield))
        return df

    result = cleanLimitsRetentionsALAE(
        df,
        "Per Claim Limit",
        "Per Claim Retention",
        "Per Claim ALAE Handling",
        "HasPerClaim",
    )

    result = cleanLimitsRetentionsALAE(
        result,
        "Per Event Limit",
        "Per Event Retention",
        "Per Event ALAE Handling",
        "HasPerEvent",
    )

    result = cleanLimitsRetentionsALAE(
        result,
        "Aggregate Limit",
        "Aggregate Retention",
        "Aggregate ALAE Handling",
        "HasAgg",
    )

    result=(result
            .with_columns(pl.when(pl.col('HasPerEvent')==True)
            .then(pl.col('Per Event Limit'))
            .when(pl.col('HasPerClaim')==True)
            .then(pl.col('Per Claim Limit'))
            .otherwise(pl.lit(0.0))
            .alias('Default Limit for Reinstatements')))
    
    return result

def initialCleanSpecs(connectiontype, specdict,xlspath):
    global LOSSFOLDER
    MYLOGGER.debug('Start initialCleanSpecs')
    #Just preliminary cleaning of specs. Shouldn't be adding or deleting columns. Info after this step will be source data for data forms.
    result=specdict
    keys=list(specdict.keys())
    MYLOGGER.debug(keys)
    LOSSFOLDER=xlspath + "/Loss Sources/CSVs"
    MYLOGGER.debug('Loss Folder: '+LOSSFOLDER)

    def CleanStepsByKey(connectiontype,key):
        global LOSSFOLDER
        if key == "Percentiles":
            pctiletables=result['Percentile Tables'].get_column('Percentile Table').unique().to_list()
            result[key]=result[key].filter(pl.col('Percentile Table').is_in(pctiletables))
        elif key == "Percentile Tables":
            result[key] = (result[key].with_columns(pl.col("Percentile Table Description").fill_null(pl.col("Percentile Table")))
                                      .unique(subset=["Percentile Table"], keep='first',maintain_order=True))
        elif key=="State Codes to Names":
            pass
        elif key=="State Names to Codes":
            pass
        elif key=="Cat Peril Maps":
            pass
        elif key == "Contract Layers":
            result[key]=cleanCededLossLayerSpecs(result[key])
            result.update({'dict_DefaultReinstmtLimit':dict(
                                    (result[key].select(["Layer", "Default Limit for Reinstatements"])).iter_rows()
                                    )})             
            
            #Delete rows with invalid entries for inuring layer or underlying layer
            collist=result[key].select(pl.col("*").exclude("Subject Perils")).columns  
            perillist=result['Cat Peril Maps'].get_column('Peril').unique().to_list()

            result[key]=(result[key]
                         .with_columns(pl.col("Subject Perils").str.split(",").cast(pl.List(pl.Utf8)).alias("Subject Perils"))
                         .explode('Subject Perils')
                         .with_columns(pl.col("Subject Perils").str.strip_chars().alias("Subject Perils"))
                         .with_columns(pl.when(pl.col('Subject Perils').is_not_null())
                                       .then(pl.col(['Subject Perils']).map_elements(lambda x: x if x in perillist else 'Delete',skip_nulls=False,return_dtype=pl.Utf8))
                                       .otherwise(pl.col('Subject Perils'))
                                       .alias('Subject Perils'))
                         .filter(pl.col('Subject Perils').ne_missing('Delete'))
                         .group_by(collist).agg(
                                                    temp=pl.col("Subject Perils").filter(pl.col('Subject Perils').is_not_null()).map_elements(
                                                        lambda group: ",".join(group.unique().sort()),return_dtype=pl.Utf8))
                         .rename({'temp':'Subject Perils'}))

            collist=result[key].select(pl.col("*").exclude("Underlying Layers")).columns  
            layerlist=result[key].get_column('Layer').to_list()

            result[key]=(result[key]
                         .with_columns(pl.col("Underlying Layers").str.split(",").cast(pl.List(pl.Utf8)).alias("Underlying Layers"))
                         .explode('Underlying Layers')
                         .with_columns(pl.col("Underlying Layers").str.strip_chars().alias("Underlying Layers"))
                         .with_columns(pl.when(pl.col('Underlying Layers').is_not_null())
                                       .then(pl.col(['Underlying Layers']).map_elements(lambda x: x if x in layerlist else 'Delete',skip_nulls=False,return_dtype=pl.Utf8))
                                       .otherwise(pl.col('Underlying Layers'))
                                       .alias('Underlying Layers'))
                         .filter(pl.col('Underlying Layers').ne_missing('Delete'))
                         .filter(pl.col('Underlying Layers').ne_missing(pl.col('Layer')))
                         .group_by(collist).agg(
                                                    temp=pl.col("Underlying Layers").filter(pl.col('Underlying Layers').is_not_null()).map_elements(
                                                        lambda group: ",".join(group.unique().sort()),return_dtype=pl.Utf8))
                         .rename({'temp':'Underlying Layers'}))
            
            collist=result[key].select(pl.col("*").exclude("Inuring Layers")).columns  
            layerlist=result[key].get_column('Layer').to_list()
            layerlistplaced=[x+"(As Placed)" for x in layerlist]
            layerlistdeemed=[x+"(Deemed)" for x in layerlist]
            layerlist=layerlistplaced+layerlistdeemed
            layerlist=[x.strip() for x in layerlist]
            result[key]=(result[key]
                         .with_columns(pl.col("Inuring Layers").str.split(",").cast(pl.List(pl.Utf8)).alias("Inuring Layers"))
                         .explode('Inuring Layers')
                         .with_columns(pl.col("Inuring Layers").str.strip_chars().alias("Inuring Layers"))
                         .with_columns(pl.when(pl.col('Inuring Layers').is_not_null())
                                       .then(pl.col(['Inuring Layers']).map_elements(lambda x: x if x in layerlist else 'Delete',skip_nulls=False,return_dtype=pl.Utf8))
                                       .otherwise(pl.col('Inuring Layers'))
                                       .alias('Inuring Layers')))
            result[key]=(result[key]
                         .filter(pl.col('Inuring Layers').ne_missing('Delete'))
                         .filter(pl.col('Inuring Layers').ne_missing(pl.col('Layer')))
                         .group_by(collist).agg(
                                                    temp=pl.col("Inuring Layers").filter(pl.col('Inuring Layers').is_not_null()).map_elements(
                                                        lambda group: ",".join(group.unique().sort()),return_dtype=pl.Utf8))
                         .rename({'temp':'Inuring Layers'}))
        elif key=="Loss Sources":
            MYLOGGER.debug(LOSSFOLDER)
            lossfilelist=_misc.getFileList(LOSSFOLDER,'csv')
            MYLOGGER.debug("Loss Source List"+str(lossfilelist))
            result[key]=(result[key]
                         .filter(pl.col('Loss CSV Filename').is_in(lossfilelist))
                         .with_columns(pl.when(pl.col('ALAE CSV Filename').is_not_null())
                                       .then(pl.col('ALAE CSV Filename').map_elements(lambda x: x if x in lossfilelist else 'Delete',skip_nulls=False,return_dtype=pl.Utf8))
                                       .otherwise(pl.lit(None))
                                       .alias('ALAE CSV Filename')))

            result[key]=(result[key] 
                         .filter(pl.col('ALAE CSV Filename').ne_missing('Delete')))
        elif key=='Scenario Loss Sources':
            result[key]=(result[key]
                         .unique(subset=['Scenario','Loss Source IDs'], keep='first',maintain_order=True)
                         .with_columns(pl.col('Loss Source IDs').map_elements(lambda x: _misc.list_intersection_sls(x,result['Loss Sources'].get_column('Loss Source ID').to_list()),skip_nulls=False,return_dtype=pl.Utf8))
                         .sort('Scenario'))
        elif key == "KPIs":
            MYLOGGER.debug(result[key])
            result[key]=(result[key]
                        .with_columns([pl.col('Profitability Measures').map_elements(lambda x: _misc.list_intersection_sls(x,result['KPI Metrics'].filter(pl.col('KPI Category')=='Profitability').get_column('Label').unique().to_list()),skip_nulls=False,return_dtype=pl.Utf8).alias('Profitability Measures'),
                                        pl.col('Volatility Measures').map_elements(lambda x: _misc.list_intersection_sls(x,result['KPI Metrics'].filter(pl.col('KPI Category')=='Volatility').get_column('Label').unique().to_list()),skip_nulls=False,return_dtype=pl.Utf8).alias('Volatility Measures'),
                                        pl.col('Surplus Protection Measures').map_elements(lambda x: _misc.list_intersection_sls(x,result['KPI Metrics'].filter(pl.col('KPI Category')=='Surplus Protection').get_column('Label').unique().to_list()),skip_nulls=False,return_dtype=pl.Utf8).alias('Surplus Protection Measures'),
                                        pl.col('Other Measures').map_elements(lambda x: _misc.list_intersection_sls(x,result['KPI Metrics'].filter(pl.col('KPI Category')=='Other').get_column('Label').unique().to_list()),skip_nulls=False,return_dtype=pl.Utf8).alias('Other Measures')]))
            
            if result[key].get_column('VaR and TVaR Percentiles')[0]!='':
                result[key]=(result[key]
                                .with_columns(pl.col('VaR and TVaR Percentiles').map_elements(lambda x: x.split(","),return_dtype=pl.List(pl.Utf8)).alias('VaR and TVaR Percentiles'))
                                .with_columns(pl.col('VaR and TVaR Percentiles').map_elements(lambda x: [float(y) for y in x],return_dtype=pl.List(pl.Float64)).alias('VaR and TVaR Percentiles'))
                                .with_columns(pl.col('VaR and TVaR Percentiles').map_elements(lambda x: [y for y in x if y in result['KPI Percentiles'].get_column('Percentile').to_list()],return_dtype=pl.List(pl.Float64)).alias('VaR and TVaR Percentiles'))
                                .with_columns(pl.col('VaR and TVaR Percentiles').map_elements(lambda x: sorted(x),return_dtype=pl.List(pl.Float64)).alias('VaR and TVaR Percentiles'))
                                .with_columns(pl.col('VaR and TVaR Percentiles').map_elements(lambda x: ([_misc.getReturnPeriod(y) for y in x]),return_dtype=pl.List(pl.Utf8)).alias('Return Periods'))                                
                                .with_columns(pl.col('Return Periods').map_elements(lambda x:",".join(x),return_dtype=pl.Utf8).alias('Return Periods'))
                                .drop('VaR and TVaR Percentiles'))            
        elif key == "KPI Percentiles":
            result[key]=result[key].with_columns(pl.col('Percentile').map_elements(lambda x:_misc.getReturnPeriod(x),return_dtype=pl.Utf8).alias('Return Period'))        
        elif key == "Scenarios":
            result[key]= result[key].unique(subset=["Scenario"], keep='first',maintain_order=True)
        elif key == "Strategies":
            result[key] = (result[key].unique(subset=["Strategy"], keep='first',maintain_order=True)) 
        elif key == "Risk Sources":
            result[key]  = (result[key].unique(subset=["Risk Source ID"], keep='first',maintain_order=True)
                            .unique(subset=['Risk Source'], keep='first',maintain_order=True)
                            .with_columns(pl.col("Risk Source ID").cast(pl.Int64))
                            .with_columns(pl.col("Parent Risk Source ID for Inurances").cast(pl.Int64)))
            result['Risk Sources']=result[key]
        elif key == "Risk Transfer Discount Factors":
            result[key] = (result[key].unique(subset=["Name"], keep='first',maintain_order=True))
        elif key == "Theoretical Premium Assumptions":
            result[key] = (result[key].unique(subset=["Name"], keep='first',maintain_order=True))
        elif key == "Premium Allocation Assumptions":
            result[key] = (result[key].unique(subset=["Name"], keep='first',maintain_order=True))                        
        elif key == "CoTVaR Thresholds":
            result[key] = (result[key].unique(subset=["Threshold"], keep='first',maintain_order=True))
        elif key == "CoTVaR Segmentations":
            segmentations=result['Segmentations'].get_column('Segmentation').unique().to_list()
            result[key] = (result[key]
                                    .unique(subset=["Segmentation"], keep='first',maintain_order=True)
                                    .filter(pl.col('Segmentation').is_in(segmentations)))
        elif key == "CoTVaR Metrics":
            result[key]= result[key].filter(pl.col("Include in Analysis") == "Include")
        elif key == "Segmentations":
            result[key] = (result[key]
                           .with_columns(pl.col('Risk Sources')
                                         .map_elements(lambda x: _misc.list_intersection_sls(x,result['Risk Sources'].get_column('Risk Source').unique().to_list()),skip_nulls=False,return_dtype=pl.Utf8)
                                         .alias('Risk Sources'))
                           .unique(subset=['Segmentation','Segment'], keep='first',maintain_order=True))
        elif key == "Treaty Terms":
            result[key] = (result[key].unique(subset=["Strategy", "Treaty Loss Layer","Treaty Loss Layer Suffix"], keep='first',maintain_order=True))
            
            try:
                result["Treaty Terms"] = result["Treaty Terms"].with_columns(pl.col('Ceding Commission Terms').cast(pl.Utf8))
            except:
                pass
            
            try:
                result["Treaty Terms"] = result["Treaty Terms"].with_columns(pl.col('Treaty Loss Layer Suffix').cast(pl.Utf8))
            except:
                pass

            result["Treaty Terms"] = (result["Treaty Terms"]
                                        .with_columns(pl.when(pl.col("Treaty Loss Layer Suffix").is_null())
                                                    .then(pl.col("Treaty Loss Layer"))
                                                    .otherwise(
                                                        pl.col("Treaty Loss Layer")
                                                        + " - "
                                                        + pl.col("Treaty Loss Layer Suffix")
                                                    )
                                                    .alias('Treaty Loss Layer Name')))           
            
            strategiesIncluded = (result["Strategies"]
                                .get_column("Strategy")
                                .to_list())
            result[key]  = result[key] .filter(pl.col("Strategy").is_in(strategiesIncluded))
    for key in keys:
        MYLOGGER.debug(key)
        CleanStepsByKey(connectiontype,key)

    return result

def createPreppedSpecs(connectiontype, specdict):
    MYLOGGER.debug('Start createPreppedSpecs')
    result=specdict
    keys=list(specdict.keys())

    def CleanStepsByKey(connectiontype,result,key):
        if key=='Contract Layers':
            try:
                result[key]=result[key].with_columns(pl.col('Subject Perils').cast(pl.Utf8))
            except:
                pass
            try:
                result[key]=result[key].with_columns(pl.col('Underlying Layers').cast(pl.Utf8))
            except:
                pass
            try:
                result[key]=result[key].with_columns(pl.col('Inuring Layers').cast(pl.Utf8))
            except:
                pass

            if result[key].shape[0]>0:
                # Convert strings to lists for underlying layers and inuring layers
                result[key]=(result[key]
                        .with_columns([pl.when(pl.col("Subject Perils")=="")
                                      .then(pl.lit(None))
                                      .otherwise(pl.col('Subject Perils'))
                                      .alias("Subject Perils"),
                                      pl.when(pl.col("Underlying Layers")=="")
                                      .then(pl.lit(None))
                                      .otherwise(pl.col('Underlying Layers'))
                                      .alias("Underlying Layers"),
                                      pl.when(pl.col("Inuring Layers")=="")
                                      .then(pl.lit(None))
                                      .otherwise(pl.col('Inuring Layers'))
                                      .alias("Inuring Layers")])                                   
                        .with_columns(
                        [
                            pl.col("Subject Perils")
                            .str.split(",")
                            .cast(pl.List(pl.Utf8))
                            .alias("Subject Perils"),                            
                            pl.col("Underlying Layers")
                            .str.split(",")
                            .cast(pl.List(pl.Utf8))
                            .alias("Underlying Layers"),
                            pl.col("Inuring Layers")
                            .str.split(",")
                            .cast(pl.List(pl.Utf8))
                            .alias("Inuring Layers"),
                        ]
                    )
                    .with_columns(
                        [
                            pl.col("Subject Perils")
                            .fill_null([])
                            .cast(pl.List(pl.Utf8))                        
                            .alias("Subject Perils"),                            
                            pl.col("Underlying Layers")
                            .fill_null([])
                            .cast(pl.List(pl.Utf8))                        
                            .alias("Underlying Layers"),
                            pl.col("Inuring Layers")
                            .fill_null([])
                            .cast(pl.List(pl.Utf8))                        
                            .alias("Inuring Layers"),
                        ]
                    )
                )

                result[key]=result[key].with_columns(
                    pl.col("Inuring Layers")
                    .map_elements(lambda x: [y[0 : y.rfind("(")] for y in x])
                    .cast(pl.List(pl.Utf8))
                    .alias("tempInuring")
                )

                # Add column for deemed or placed for inuring layers
                if result[key].filter(pl.col("Inuring Layers").list.len()>0).shape[0] > 0:
                    result[key]=result[key].with_columns(
                        pl.col("Inuring Layers")
                        .map_elements(lambda x: [y.rsplit("(", 1)[1][:-1] for y in x])
                        .cast(pl.List(pl.Utf8))
                        .alias("Deemed or Placed")
                    ).with_columns(
                        pl.lit("As Placed")
                        .is_in(pl.col("Deemed or Placed"))
                        .alias("Has Inuring As Placed")
                    )
                else:
                    result[key]=result[key].with_columns([pl.lit(None).alias("Deemed or Placed"),pl.lit(False).alias("Has Inuring As Placed")])

                if result[key].filter(pl.col("Has Inuring As Placed")).shape[0] > 0:
                    result[key]=result[key].with_columns(
                        pl.when(pl.col("Has Inuring As Placed") == True)
                        .then(
                            pl.col("Inuring Layers")
                            .map_elements(
                                lambda x: [
                                    y[0 : y.rfind("(")]
                                    for y in x
                                    if y.rsplit("(", 1)[1][:-1] == "As Placed"
                                ]
                            )
                        )
                        .otherwise(pl.lit(None))
                        .alias("Inuring As Placed"))

                    treatiesWithInuringAsPlaced = (
                        result[key].filter(pl.col("Has Inuring As Placed") == True)
                        .get_column("Layer")
                        .to_list()
                    )

                    df2 = (
                        result["Treaty Terms"]
                        .select(["Strategy", "Treaty Loss Layer"])
                        .filter(
                            pl.col("Treaty Loss Layer").is_in(treatiesWithInuringAsPlaced)
                        )
                        .rename({"Treaty Loss Layer": "Layer"})
                        .unique()
                    )

                    ###More prep on Treaty Premium and Expense Terms after initial prep on Ceded Loss Layers
                    ###If there is inuring as placed
                    result["Treaty Terms"] = result[
                        "Treaty Terms"
                    ].with_columns(
                        pl.when(
                            pl.col("Treaty Loss Layer").is_in(treatiesWithInuringAsPlaced)
                        )
                        .then(
                            (pl.col("Treaty Loss Layer") + ": " + pl.col("Strategy"))
                        )
                        .otherwise(pl.col("Treaty Loss Layer"))
                        .alias("Layer Lookup"))

                    result[key] = (
                        pl.concat(
                            [
                                result[key].filter(
                                    pl.col("Has Inuring As Placed") == False
                                ).with_columns(pl.lit("All").alias("Strategy")),
                                result[key].filter(pl.col("Has Inuring As Placed") == True).join(
                                    df2, how="left", on="Layer"
                                ),
                            ]
                        )
                        .with_columns(
                            pl.when(pl.col("Has Inuring As Placed") == False)
                            .then(pl.col("Layer"))
                            .otherwise(pl.when(pl.col("Strategy").is_null())
                                        .then(pl.col("Layer").alias("Layer"))
                                        .otherwise((pl.col("Layer") + ": " + pl.col("Strategy"))))
                            .alias("Layer"))
                        .with_columns(pl.col("Strategy").fill_null("All"))
                    )
                else:
                    result[key]=result[key].with_columns(pl.lit("All").alias("Strategy"))

                ###More prep on Treaty Premium and Expense Terms after initial prep on Ceded Loss Layers
                if 'Layer Lookup' in result['Treaty Terms'].columns:
                    pass
                else:
                    result['Treaty Terms'] = (result['Treaty Terms']
                                                .with_columns(pl.col('Treaty Loss Layer').alias('Layer Lookup')))

                result["Treaty Terms"] = (result["Treaty Terms"]
                    .with_columns(pl.col('Treaty Loss Layer Name')
                                  .alias('Treaty Loss Layer'))
                    .drop('Treaty Loss Layer Suffix'))      

                result['Treaty Terms']=result['Treaty Terms'].with_columns(
                    pl.when((pl.col('Reinstatements').is_not_null()) & (pl.col("Limit for Reinstatements").is_null()))
                    .then(pl.col("Layer Lookup").replace_strict(result["dict_DefaultReinstmtLimit"],default=999999999))
                    .otherwise(pl.col("Limit for Reinstatements"))
                    .alias("Limit for Reinstatements")
                    )                

                # add dictionary item for reinstatement limit
                result.update({'dict_ReinstmtLimit':dict(
                                    (result['Treaty Terms'].select(["Treaty Loss Layer", "Limit for Reinstatements"])).iter_rows()
                                    )})     
                ####End additional prep on Treaty Premium and Expense Terms

                def getLevel(_currlevel, _level, _ul, _inuring):
                    if _level >= 0:
                        return _level
                    else:
                        if _ul is None:
                            _ul = []
                        if _inuring is None:
                            _inuring = []
                        layersNeeded = _ul + _inuring
                        stillUnassigned = list(set(layersNeeded).difference(set(assigned)))
                        if len(stillUnassigned) == 0:
                            return _currlevel + 1
                        else:
                            return -1

                # Initialize variables
                result[key]=result[key].with_columns(pl.lit(-1).alias("Level"))
                currlevel = 0
                assigned = []

                while (
                    currlevel
                    <= result['maxlevels']
                ) and result[key].filter(pl.col("Level") == -1).shape[0] > 0:
                    result[key]=result[key].with_columns(
                        pl.struct(
                            [
                                "Level",
                                "Underlying Layers",
                                "tempInuring",
                            ]
                        )
                        .map_elements(
                            lambda x: getLevel(
                                currlevel,
                                x["Level"],
                                x["Underlying Layers"],
                                x["tempInuring"],
                            ),
                            skip_nulls=False,
                        )
                        .alias("Level")
                    )
                    assigned = result[key].filter(pl.col("Level") >= 0).get_column("Layer").to_list()
                    currlevel = currlevel + 1

                # Derive Subject Risk Source IDs
                result[key]=result[key].with_columns(
                    pl.when(pl.col("HasPerClaim") == True)
                    .then(
                        pl.col("Subject Risk Source Group")
                        .map_elements(
                            lambda x: result["Risk Source Groups"]
                            .filter(
                                (pl.col("Risk Source Group") == x)
                                & (pl.col("Risk Source Type") == "Individual")
                            )
                            .get_column("Risk Source")
                            .to_list()
                        )
                        .alias("Risk Sources")
                    )
                    .otherwise(
                        pl.col("Subject Risk Source Group")
                        .map_elements(
                            lambda x: result["Risk Source Groups"]
                            .filter((pl.col("Risk Source Group") == x))
                            .get_column("Risk Source")
                            .to_list()
                        )
                        .alias("Risk Sources")
                    )
                ).with_columns(
                    pl.when(pl.col("HasPerClaim") == True)
                    .then(
                        pl.col("Subject Risk Source Group")
                        .map_elements(
                            lambda x: result["Risk Source Groups"]
                            .filter(
                                (pl.col("Risk Source Group") == x)
                                & (pl.col("Risk Source Type") == "Individual")
                            )
                            .get_column("Risk Source ID")
                            .to_list()
                        )
                        .alias("Risk Source IDs")
                    )
                    .otherwise(
                        pl.col("Subject Risk Source Group")
                        .map_elements(
                            lambda x: result["Risk Source Groups"]
                            .filter((pl.col("Risk Source Group") == x))
                            .get_column("Risk Source ID")
                            .to_list()
                        )
                        .alias("Risk Source IDs")
                    )
                )
        elif key == "General":
            # Set MaxSims and Analysis variables
            try:
                result.update({'maxsims':int(float(result[key].filter(pl.col("Information") == "Target Number of Simulations").get_column("Value")[0]))})
            except:
                result.update({'maxsims':999999999999})

            try:
                result.update({'cateventaggthreshold':int(float(result[key].filter(pl.col("Information") == "Non-Aggregation Threshold for Cat Events").get_column("Value")[0]))})
            except:
                result.update({'cateventaggthreshold':0})

            try:
                result.update({'maxlevels':int(float(result[key].filter(pl.col("Information") == "Maximum Number of Treaty Layer Levels").get_column("Value")[0]))})
            except:
                result.update({'maxlevels':10})                         
                
            try:
                result.update({'oeppctiles':result[key].filter(pl.col("Information") == "Percentile Table for OEPs").get_column("Value")[0]})
            except:
                result.update({'oeppctiles':result['Percentile Tables'].get_column('Percentile Table')[0]})  

            try:
                result.update({'gcnpctiles':result[key].filter(pl.col("Information") == "Percentile Table for Gross Ceded Net Results").get_column("Value")[0]})
            except:
                result.update({'gcnpctiles':result['Percentile Tables'].get_column('Percentile Table')[0]})

            try:
                result.update({'forcerefresh':result[key].filter(pl.col("Information") == "Force Refresh - New Loss and ALAE Files").get_column("Value")[0]})
            except:
                result.update({'forcerefresh':'No'})   

            try:
                result.update({'savegross':result[key].filter(pl.col("Information") == "Save Gross UW Results").get_column("Value")[0]})
            except:
                result.update({'savegross':'No'})

            try:
                result.update({'savecededloss':result[key].filter(pl.col("Information") == "Save Ceded Loss Detail").get_column("Value")[0]})
            except:
                result.update({'savecededloss':'No'})

            try:
                result.update({'savelossalloc':result[key].filter(pl.col("Information") == "Save Loss Allocations").get_column("Value")[0]})
            except:
                result.update({'savelossalloc':'No'})                

            try:
                result.update({'savecededuw':result[key].filter(pl.col("Information") == "Save Ceded UW Results").get_column("Value")[0]})
            except:
                result.update({'savecededuw':'No'})

            try:
                result.update({'savepremalloc':result[key].filter(pl.col("Information") == "Save Premium Allocations").get_column("Value")[0]})
            except:
                result.update({'savepremalloc':'No'})

            try:
                result.update({'savedetail':result[key].filter(pl.col("Information") == "Save Detailed Results").get_column("Value")[0]})
            except:
                result.update({'savedetail':'No'}) 

            try:
                result.update({'savesummary':result[key].filter(pl.col("Information") == "Save Summary Results").get_column("Value")[0]})
            except:
                result.update({'savesummary':'No'})                 

            #try:
            result.update({'lossfolder':LOSSFOLDER}) #result[key].filter(pl.col("Information") == "Loss Source Folder").get_column("Value")[0]})
            # except:
            #     result.update({'lossfolder':'Loss Sources'})                
        elif key == "Scenarios":
            result[key]=result[key].filter(pl.col("Include in Analysis") == "Include").drop("Include in Analysis")
        elif key == "Optional Analyses":
            result[key]=result[key].filter(pl.col("Include in Analysis") == "Include").drop("Include in Analysis")
        elif key == "Strategies":
            result[key]=(result[key]
                         .filter(pl.col("Include in Analysis") == "Include")
                         .drop("Include in Analysis")
                         .with_columns(pl.lit(1).cast(pl.Int64).alias('Strategy ID'))
                         .with_columns(pl.cum_sum('Strategy ID').alias('Strategy ID')))
        elif key == "Risk Sources":
            #Dictionary of initial non cat risk source types (to use in CreateLALAENonCat)
            result.update({'Risk Sources':result[key]})
            result.update({"noncatrisksourcetypes": dict((result[key].filter(~pl.col("Risk Source Type").is_in(['AIR Cat','RMS Cat','RMS Attritional Cat'])).select(["Risk Source ID","Risk Source Type"])).iter_rows())})
            result.update({"catrisksourceids":dict((result[key].filter(pl.col("Risk Source Type").is_in(['AIR Cat','RMS Cat','RMS Attritional Cat'])).select(["Risk Source","Risk Source ID"])).iter_rows())})
            result.update({"risksourcetypes": dict((result[key].select(["Risk Source ID","Risk Source Type"])).iter_rows())})
        elif key == "Gross Premium and Expense":
            MYLOGGER.debug('Gross Premium and Expense')
            result[key] = (result[key].join(
                result["Risk Sources"].select(["Risk Source", "Risk Source ID"]),
                how="left",
                on="Risk Source")
                .unique(subset=["Scenario", "Risk Source"], keep='first',maintain_order=True)
                .with_columns((pl.col('Premium')*pl.col('Expense Ratio')).alias('Expense'))
                .drop('Expense Ratio'))

            temp=result[key].filter((pl.col('Scenario').is_not_null()))
            temp1=result[key].filter(pl.col('Scenario').is_null())
            scenariolist=result['Scenarios'].get_column('Scenario').to_list()
            temp1=(temp1
                .drop('Scenario')
                .join(pl.DataFrame({'Scenario':_misc.list_difference(scenariolist,temp.get_column('Scenario').to_list())})
                      .with_columns(pl.col('Scenario').cast(pl.Utf8)),how='cross')
                .select(['Scenario','Risk Source','Premium','Risk Source ID','Expense']))
            result[key]=pl.concat([temp,temp1],how='diagonal')
        elif key == "Reinstatement Terms":
            result[key]=(result[key]
                            .with_columns(pl.col("Reinstatement Number").cast(pl.Int64))
                            .with_columns([pl.when(pl.col('Reinstatement Number')==-1)
                                .then(pl.col('Cost'))
                                .otherwise(pl.struct(['Name','Reinstatement Number','Cost']).map_elements(lambda x: x['Cost'] if x['Reinstatement Number']==-1 else 
                                                                                            (result[key]
                                                                                            .filter((pl.col('Name')==x['Name'])
                                                                                            &(pl.col('Reinstatement Number')!=-1)
                                                                                            &(pl.col('Reinstatement Number')<=x['Reinstatement Number']))
                                                                                            .get_column('Cost').sum())))
                                .alias('Cumulative Cost'),
                                pl.col('Reinstatement Number').max().over('Name').alias('MaxReinstmt'),
                                pl.col('Reinstatement Number').min().over('Name').alias('MinReinstmt')])
                            .with_columns(pl.when((pl.col('Reinstatement Number')==pl.col('MaxReinstmt'))&(pl.col('MinReinstmt')!=-1))
                                .then(pl.lit(0))
                                .when((pl.col('Reinstatement Number')==pl.col('MaxReinstmt'))&(pl.col('MinReinstmt')==-1))
                                .then(pl.col('Name').map_elements(lambda x:(result[key]
                                                                        .filter((pl.col('Name')==x)
                                                                        &(pl.col('Reinstatement Number')==-1))
                                                                        .get_column('Cost').min())))
                                .otherwise(pl.struct(['Name','Reinstatement Number','Cost']).map_elements(lambda x:(result[key]
                                                                                            .filter((pl.col('Name')==x['Name'])
                                                                                            &(pl.col('Reinstatement Number')==x['Reinstatement Number']+1))
                                                                                            .get_column('Cost').max())))
                                .alias('Next Reinstatement Cost'))
                .drop(['MaxReinstmt','MinReinstmt'])
                .filter(pl.col('Reinstatement Number')>0))
            
            result.update({'dict_lastReinstmt':dict(result[key].select(["Name", "Reinstatement Number"]).group_by("Name").max().iter_rows())})
        elif key == "Sliding Scale CC Terms":
            result[key] = (result[key]
                                    .unique(subset=["Name", "Loss Ratio"], keep='first',maintain_order=True)
                                    .sort(["Name", "Loss Ratio"], descending=[False, False])
                                    .with_columns(pl.col("Ceding Commission").min().over("Name").alias("min"))
                                    .with_columns(pl.col("Ceding Commission").max().over("Name").alias("max"))
                                    .with_columns(1* (pl.col("Ceding Commission") == pl.col("min")).alias("min"))
                                    .with_columns(1 * (pl.col("Ceding Commission") == pl.col("max")).alias("max"))
                                    .with_columns(pl.when(pl.col("min") == 1)
                                                    .then(pl.lit(0))
                                                    .otherwise((pl.col("Ceding Commission").shift(-1)- pl.col("Ceding Commission"))
                                                                / (pl.col("Loss Ratio").shift(-1) - pl.col("Loss Ratio")))
                                                    .alias("Slope"))
                                    .with_columns(pl.col("Slope").fill_null(strategy="zero")))
        elif key == "CoTVaR Variations":
            result[key]= result[key].filter(pl.col("Include in Analysis") == "Include")
        elif key == "CoTVaR Segmentations":
            segmentations=result['Segmentations'].get_column('Segmentation').unique().to_list()
            result[key] = (result[key]
                                    .unique(subset=["Segmentation"], keep='first',maintain_order=True)
                                    .filter(pl.col('Segmentation').is_in(segmentations)))
            #Create CoTVaR setup table
            tempVariations=result['CoTVar Variations']
            tempMetrics=result['CoTVaR Metrics']
            tempSegments=result['Segmentations'].select(['Segmentation','Segment']).filter(pl.col('Segmentation').is_in(result['CoTVaR Segmentations'].get_column('Segmentation').to_list()))
            tempSegmentations=tempSegments.select('Segmentation').unique()
            tempThresholds=result['CoTVaR Thresholds']
            tempTreaties=result['Treaty Terms'].select(['Strategy','Treaty Loss Layer'])
            tempStrategies=result['Treaty Terms'].select('Strategy').unique()

            dfCoTVaR=pl.DataFrame()
            for row in tempVariations.rows(named=True):
                if (row['CoTVaR Type']=='Gross to Net and Ceded') & (tempTreaties.shape[0]>0):
                    temp=(tempStrategies.join(pl.DataFrame({'Base GCN':['Gross','Gross'],'Base Segmentation':['All','All'],'Base Segment':['All','All'],
                                    'Base Treaty':['All','All'],'By GCN':['Ceded','Net'],'By Segmentation':['All','All'],
                                    'By Segment':['All','All'],'By Treaty':['All','All']}),
                                    how='cross')
                            .with_columns(pl.lit(row['CoTVaR Type']).cast(pl.Utf8).alias('CoTVaR Type'))
                            .select(['CoTVaR Type','Strategy','Base GCN','Base Segmentation','Base Segment','Base Treaty','By GCN','By Segmentation','By Segment','By Treaty']))
                    dfCoTVaR=pl.concat([dfCoTVaR,temp],how='diagonal')
                elif (row['CoTVaR Type']=='Gross to Net and Ceded by Segment') & (tempSegmentations.shape[0]>0):
                    tempCededNet=pl.concat([(tempSegments
                                            .with_columns([pl.lit('Ceded').alias('By GCN'),
                                                            pl.lit('All').alias('By Treaty')])
                                            .rename({'Segment':'By Segment','Segmentation':'By Segmentation'})),
                                            (tempSegments
                                                .with_columns([pl.lit('Net').alias('By GCN'),
                                                                pl.lit('All').alias('By Treaty')])
                                                .rename({'Segment':'By Segment','Segmentation':'By Segmentation'}))])

                    temp=(tempStrategies.join(
                                    tempSegmentations.join(pl.DataFrame({'Base GCN':['Gross'],'Base Segment':['All'],
                                    'Base Treaty':['All']}),
                                    how='cross'),
                                    how='cross')
                                .rename({'Segmentation':'Base Segmentation'})
                                .join(tempCededNet,how='left',left_on='Base Segmentation',right_on='By Segmentation')
                            .with_columns([pl.col('Base Segmentation').alias('By Segmentation'),
                                        pl.lit(row['CoTVaR Type']).cast(pl.Utf8).alias('CoTVaR Type')])
                            .with_columns([pl.lit('All').alias('Base Segmentation')])
                            .select(['CoTVaR Type','Strategy','Base GCN','Base Segmentation','Base Segment','Base Treaty','By GCN','By Segmentation','By Segment','By Treaty']))        
                    dfCoTVaR=pl.concat([dfCoTVaR,temp],how='diagonal')
                elif (row['CoTVaR Type']=='Gross Total to Gross by Segment') & (tempSegmentations.shape[0]>0):
                    temp=(tempSegmentations.join(pl.DataFrame({'Strategy':['All'],'Base GCN':['Gross'],'Base Segment':['All'],
                                    'Base Treaty':['All'],'By GCN':['Gross'],'By Treaty':['All']}),
                                    how='cross')
                                .rename({'Segmentation':'Base Segmentation'})
                                .join(tempSegments,how='left',left_on='Base Segmentation',right_on='Segmentation')
                            .with_columns([pl.col('Base Segmentation').alias('By Segmentation'),
                                        pl.lit(row['CoTVaR Type']).cast(pl.Utf8).alias('CoTVaR Type')])
                            .with_columns([pl.lit('All').alias('Base Segmentation')])                               
                            .rename({'Segment':'By Segment'})
                            .select(['CoTVaR Type','Strategy','Base GCN','Base Segmentation','Base Segment','Base Treaty','By GCN','By Segmentation','By Segment','By Treaty']))   
                    dfCoTVaR=pl.concat([dfCoTVaR,temp],how='diagonal')        
                elif (row['CoTVaR Type']=='Net Total to Net by Segment') & (tempSegmentations.shape[0]>0) & (tempTreaties.shape[0]>0):
                    temp=(tempStrategies.join(
                                tempSegmentations.join(pl.DataFrame({'Base GCN':['Net'],'Base Segment':['All'],
                                    'Base Treaty':['All'],'By GCN':['Net'],'By Treaty':['All']}),
                                    how='cross'),
                                    how='cross')
                                .rename({'Segmentation':'Base Segmentation'})
                                .join(tempSegments,how='left',left_on='Base Segmentation',right_on='Segmentation')
                            .with_columns([pl.col('Base Segmentation').alias('By Segmentation'),
                                        pl.lit(row['CoTVaR Type']).cast(pl.Utf8).alias('CoTVaR Type')])
                            .with_columns([pl.lit('All').alias('Base Segmentation')])                               
                            .rename({'Segment':'By Segment'})
                            .select(['CoTVaR Type','Strategy','Base GCN','Base Segmentation','Base Segment','Base Treaty','By GCN','By Segmentation','By Segment','By Treaty']))   
                    dfCoTVaR=pl.concat([dfCoTVaR,temp],how='diagonal')        
                elif (row['CoTVaR Type']=='Ceded Total to Ceded by Segment') & (tempSegmentations.shape[0]>0) & (tempTreaties.shape[0]>0):
                    temp=(tempStrategies.join(
                                tempSegmentations.join(pl.DataFrame({'Base GCN':['Ceded'],'Base Segment':['All'],
                                    'Base Treaty':['All'],'By GCN':['Ceded'],'By Treaty':['All']}),
                                    how='cross'),
                                    how='cross')
                                .rename({'Segmentation':'Base Segmentation'})
                                .join(tempSegments,how='left',left_on='Base Segmentation',right_on='Segmentation')
                            .with_columns([pl.col('Base Segmentation').alias('By Segmentation'),
                                        pl.lit(row['CoTVaR Type']).cast(pl.Utf8).alias('CoTVaR Type')])
                            .with_columns([pl.lit('All').alias('Base Segmentation')])                               
                            .rename({'Segment':'By Segment'})
                            .select(['CoTVaR Type','Strategy','Base GCN','Base Segmentation','Base Segment','Base Treaty','By GCN','By Segmentation','By Segment','By Treaty']))   
                    dfCoTVaR=pl.concat([dfCoTVaR,temp],how='diagonal')  
                elif (row['CoTVaR Type']=='Gross to Net and Ceded by Treaty') & (tempTreaties.shape[0]>0):
                    tempCeded=(tempTreaties
                                .with_columns([pl.lit('Ceded').alias('By GCN'),
                                            pl.lit('All').alias('By Segmentation'),
                                            pl.lit('All').alias('By Segment')])
                                .rename({'Treaty Loss Layer':'By Treaty'}))
                    tempNet=(tempStrategies
                                .with_columns([pl.lit('Net').alias('By GCN'),
                                            pl.lit('All').alias('By Treaty'),
                                            pl.lit('All').alias('By Segmentation'),
                                            pl.lit('All').alias('By Segment')]))

                    temp=(tempStrategies
                                .with_columns([pl.lit('Gross').alias('Base GCN'),
                                                pl.lit('All').alias('Base Treaty'),
                                                pl.lit('All').alias('Base Segmentation'),
                                                pl.lit('All').alias('Base Segment')])
                                .join(pl.concat([tempCeded,tempNet],how='diagonal'),how='left',left_on='Strategy',right_on='Strategy')
                                .with_columns([pl.lit(row['CoTVaR Type']).cast(pl.Utf8).alias('CoTVaR Type')])
                            .select(['CoTVaR Type','Strategy','Base GCN','Base Segmentation','Base Segment','Base Treaty','By GCN','By Segmentation','By Segment','By Treaty']))        
                    dfCoTVaR=pl.concat([dfCoTVaR,temp],how='diagonal')        
                elif (row['CoTVaR Type']=='Gross by Segment to Net and Ceded by Treaty') & (tempSegmentations.shape[0]>0) & (tempTreaties.shape[0]>0):
                    tempCeded=(tempTreaties
                                .join(tempSegments,how='cross')
                                .with_columns([pl.lit('Ceded').alias('By GCN')])
                                .rename({'Treaty Loss Layer':'By Treaty','Segment':'By Segment','Segmentation':'By Segmentation'}))
                    tempNet=(tempStrategies
                                .join(tempSegments,how='cross')
                                .with_columns([pl.lit('Net').alias('By GCN'),
                                            pl.lit('All').alias('By Treaty')])
                                .rename({'Segment':'By Segment','Segmentation':'By Segmentation'}))
                    temp=(tempStrategies
                                .join(tempSegments,how='cross')
                                .with_columns([pl.lit('Gross').alias('Base GCN'),
                                                pl.lit('All').alias('Base Treaty')])
                                .rename({'Segment':'Base Segment','Segmentation':'Base Segmentation'})
                                .join(pl.concat([tempCeded,tempNet],how='diagonal'),how='left',left_on=['Strategy','Base Segmentation','Base Segment'],right_on=['Strategy','By Segmentation','By Segment'])
                                .with_columns([pl.lit(row['CoTVaR Type']).cast(pl.Utf8).alias('CoTVaR Type'),
                                            pl.col('Base Segmentation').alias('By Segmentation'),
                                            pl.col('Base Segment').alias('By Segment')])
                            .select(['CoTVaR Type','Strategy','Base GCN','Base Segmentation','Base Segment','Base Treaty','By GCN','By Segmentation','By Segment','By Treaty']))        
                    dfCoTVaR=pl.concat([dfCoTVaR,temp],how='diagonal')          
                elif (row['CoTVaR Type']=='Treaty Total to Treaty by Segment') & (tempSegmentations.shape[0]>0) & (tempTreaties.shape[0]>0):
                    temp=(tempTreaties
                                .join(tempSegments,how='cross')
                                .with_columns([pl.lit('Ceded').alias('By GCN')])
                                .rename({'Treaty Loss Layer':'By Treaty','Segment':'By Segment','Segmentation':'By Segmentation'})
                                .with_columns([pl.lit('Ceded').alias('Base GCN'),
                                            pl.col('By Treaty').alias('Base Treaty'),
                                            pl.lit('All').alias('Base Segmentation'),
                                            pl.lit('All').alias('Base Segment'),
                                            pl.lit(row['CoTVaR Type']).cast(pl.Utf8).alias('CoTVaR Type')])
                                .select(['CoTVaR Type','Strategy','Base GCN','Base Segmentation','Base Segment','Base Treaty','By GCN','By Segmentation','By Segment','By Treaty']))
                    dfCoTVaR=pl.concat([dfCoTVaR,temp],how='diagonal')   
            if dfCoTVaR.shape[0]>0:
                result.update({"cotvar setup":tempMetrics.select(['CoTVaR Metric']).join(tempThresholds,how='cross').join(dfCoTVaR,how='cross')})
            else:
                result.update({"cotvar setup":pl.DataFrame()})
        elif key == "CoTVaR Metrics":
            result[key]= result[key].filter(pl.col("Include in Analysis") == "Include").drop("Include in Analysis")
        elif key == "Segmentations":
            dfSeg = (result["Segmentations"]
                    .filter(pl.col('Segmentation').str.to_lowercase()!='none')
                    .with_columns(pl.col("Risk Sources").str.split(",").alias("Risk Source"))
                    .drop("Risk Sources")
                    .explode("Risk Source")
                    .with_columns(pl.col("Risk Source").str.strip_chars().alias("Risk Source")))
                        
            #dfSegmentation has list of all risk sources used in each segmentation
            dfSegmentation = (
                dfSeg.select(["Segmentation", "Risk Source"])
                .group_by("Segmentation")
                .all()
            )

            risksources = result["Risk Sources"]["Risk Source"].unique()
            dfSeg=dfSeg.filter(pl.col('Risk Source').is_in(risksources))

            # Fill in any missing risk sources in segmentation definitions
            if dfSegmentation.shape[0] > 0:
                dfSegmentation = (dfSegmentation
                                .with_columns(pl.col("Risk Source").map_elements(lambda x: _misc.list_intersection(x,risksources)).alias("Risk Source"))                              
                                .with_columns(pl.col("Risk Source").map_elements(lambda x: _misc.list_difference(x, risksources)).alias("Missing Risk Sources"))
                                .drop("Risk Source")
                                .with_columns(pl.col("Missing Risk Sources").list.len().alias("Missing Count"))
                                .filter(pl.col("Missing Count") > 0)
                                .drop("Missing Count")
                                .explode("Missing Risk Sources")
                                .rename({"Missing Risk Sources": "Risk Source"})
                                .with_columns(pl.lit("Missing Segment Definition").alias("Segment"))
                                .select(["Segmentation", "Segment", "Risk Source"]))

            if dfSegmentation.shape[0] > 0:
                dfSeg = pl.concat([dfSeg, dfSegmentation])

            dfSeg=pl.concat([dfSeg,pl.DataFrame({'Segmentation':'None','Segment':'NA','Risk Source':risksources})])


            # Add Risk Source ID, drop Risk Source, keep as string and list
            dfSeg = (dfSeg.join(
                result["Risk Sources"]
                .select(["Risk Source", "Risk Source ID"]).unique(subset=["Risk Source"], keep='first',maintain_order=True),
                how="left",
                on="Risk Source")
                .drop('Risk Source')
                .sort('Risk Source ID')
                .group_by(['Segmentation','Segment'])
                .all()
                .rename({'Risk Source ID':'RSID List'})
                .with_columns(pl.col('RSID List').cast(pl.List(pl.Utf8)).list.join("_").alias('RSID List String')))
            
            dfReqRSID=(dfSeg.drop(['Segmentation','Segment'])
                       .unique(subset=['RSID List'], keep='first',maintain_order=True)
                       .join(result["Scenarios"].select(["Scenario"]),how='cross')
                       .with_columns([pl.struct([pl.col('RSID List'),pl.col('Scenario')])
                                     .map_elements(lambda x: result['Gross Premium and Expense']
                                            .filter((pl.col('Risk Source ID').is_in(x['RSID List'])) & (pl.col('Scenario')==x['Scenario']))
                                            .get_column('Premium')
                                            .sum())
                                    .alias('Premium'),
                                    pl.struct([pl.col('RSID List'),pl.col('Scenario')])
                                     .map_elements(lambda x: result['Gross Premium and Expense']
                                            .filter((pl.col('Risk Source ID').is_in(x['RSID List'])) & (pl.col('Scenario')==x['Scenario']))
                                            .get_column('Expense')
                                            .sum())
                                    .alias('Expense')]))
        
            result.update({"required rsid groups":dfReqRSID})
            result[key] = dfSeg

            #Make changes to risk source groups table after segmentations table is finalized
            #Add additional rows for newly created blended risk sources
            dfRSG=result['Risk Source Groups']
        elif key == "Risk Source Groups":
            MYLOGGER.debug(result['Risk Sources'].columns)
            dfRSG = (result[key]
                                    .with_columns(pl.col("Risk Sources").str.split(",").alias("Risk Source"))
                                    .drop("Risk Sources")
                                    .explode("Risk Source")
                                    .with_columns(pl.col("Risk Source").str.strip_chars().alias("Risk Source"))
                                    .join(result['Risk Sources']
                                          .unique(subset=['Risk Source'],keep='first',maintain_order=True)
                                          .select(["Risk Source", "Risk Source ID","Risk Source Type",
                                                                         "Include in Gross"]), how="left", on="Risk Source"))
            MYLOGGER.debug(dfRSG.columns)
            result[key]=dfRSG
        elif key == "Treaty Terms":
            result[key] = (result[key].unique(subset=["Strategy", "Treaty Loss Layer"], keep='first',maintain_order=True))
            strategiesIncluded = (result["Strategies"]
                                .get_column("Strategy")
                                .to_list())
            result[key]  = (result[key].filter(pl.col("Strategy").is_in(strategiesIncluded))
                                .with_columns(pl.lit(1).cast(pl.Int64).alias('Treaty ID'))
                                .with_columns(pl.cum_sum('Treaty ID').over(['Strategy']).alias('Treaty ID'))
                                .join(result['Strategies'].select(['Strategy','Strategy ID']),how='left',on='Strategy'))
        elif key=="Default Values":
            result[key]=result[key]             
        elif key=='KPIs':
            result[key]=result[key].with_columns(pl.exclude('KPI Group').map_elements(lambda x: x.split(",")))
        return result

    for key in keys:
        CleanStepsByKey(connectiontype,result,key)

    #Add entry for connection type (for passing to other functions)
    result.update({'connectiontype':connectiontype})
    result.update({'treatyrisksources':{}})
    
    return result

def CreateSimsTable(specs):
    # Build dataframe for set of sims --- will be used to fill in blank sims so percentiles can be calculated
    # Needs maxsims. Run after step to set General Spec variables.
    dfSims = pl.DataFrame([x+1 for x in range(specs["maxsims"])], ["Simulation"])
              
    return dfSims

def removeFromDataFrame(df,scenarios=None,contracts=None,strategies=None,segmentations=None):
    #This will remove the intersection of scenarios/contracts/strategies... ie.e the listed contracts in the listed scenarios in the listed strategies in the listed segmentations
    #If you want to remove ALL data associated with the listed scenarios, first removeFromDataFrame(df,scenarios=scenarios) then removeFromDataFrame(df,contracts=contracts)
    print(segmentations)
    if "Scenario" in df.columns:
        if scenarios==None:
            pass
        else:
            df = df.filter(pl.col("Scenario").is_in(scenarios).not_())
    if "Layer" in df.columns:
        if contracts==None:
            pass
        else:
            df = df.filter(pl.col("Layer").is_in(contracts).not_())
    if "Strategy" in df.columns:
        if strategies==None:
            pass
        else:
            df = df.filter(pl.col("Strategy").is_in(strategies).not_())
    if "Segmentation" in df.columns:
        if segmentations==None:
            pass
        else:
            df = df.filter(pl.col("Segmentation").is_in(segmentations).not_())
    return df

def CreateLALAEFromCSV(specs,scenario):
    #FROM EXCEL SPECS
    #Non-cat AND non-modeled cat (i.e. cat risk sources modeled without ELTs)
    #Key steps:
    #1. Read in loss and alae files
    #2. Assign Event ID to Individual Claims that are not part of a multi-claimant event so detail isn't lost for Event LALAE (i.e. occurrence terms will be applied to individual claims, too). Event ID will be <0.
    #3. Create small event table. Event ID for small events will be 0. This will be used to make sure per claim and per event terms are not applied to aggregated small cats.
    #4. Create LALAE table, aggregating small cats to risk source level.

    MYLOGGER.debug('Start CreateLALAENonCat')
    MYLOGGER.debug(specs['connectiontype'])
    MYLOGGER.debug(specs['lossfolder'])
    connectiontype=specs['connectiontype']
    lossfolder=specs['lossfolder']

    if connectiontype==1:
        if scenario in specs['Scenario Loss Sources'].get_column('Scenario').unique().to_list():
            losssourceids=specs['Scenario Loss Sources'].filter(pl.col('Scenario')==scenario).get_column('Loss Source IDs').unique().to_list()
            dfLossSpecs=specs['Loss Sources'].filter(pl.col('Loss Source ID').is_in(losssourceids))
        elif specs['Loss Sources'].filter(pl.col('Scenario').is_null()).shape[0]>0:
            dfLossSpecs=specs['Loss Sources']  
        else:
            return pl.DataFrame(schema={"Simulation":pl.Int64, "Risk Source ID": pl.Int64, "Event ID": pl.Int64,"RS Type":pl.Utf8,
                                      "Loss":pl.Float64,"Loss and ALAE":pl.Float64,"Large Claim Count":pl.Int64,"Cat Event Count":pl.Int64,
                                      "Event LALAE":pl.Float64,"Loss #" : pl.Int64})
    else:
        dfLossSpecs=pl.DataFrame()

    def adjustNumberofSims(df,currsims):
        #Expand to desired number of simulations
        if specs['maxsims']==currsims:
            pass
        elif specs['maxsims']<currsims:
            df=df.filter(pl.col('Simulation')<=specs['maxsims'])
        else:
            df=df.filter(pl.col('Simulation')<=currsims)   #force currsims to apply
            numFullCopies=specs['maxsims']//currsims
            numPartialSims=specs['maxsims']-numFullCopies*currsims
            rowsinit=df.shape[0]
            if numPartialSims>0:
                dfPartial=df.filter(pl.col('Simulation')<=numPartialSims)
            df=pl.concat([df]*numFullCopies)
            
            if numPartialSims>0:
                df=pl.concat([df,dfPartial],how='diagonal')

            df=(df
                .with_columns(pl.lit(1).alias('row'))
                .with_columns(pl.cum_sum('row').alias('row'))
                .with_columns(((pl.col('row')-1)//rowsinit).alias('row'))
                .with_columns(((currsims*pl.col('row'))+pl.col('Simulation')).alias('Simulation'))
                .drop('row'))
        return df

    def readLossAndALAE(_lossfilename,_alaefilename,_numsims,_clasheventidfile=None):
        if pd.isnull(_alaefilename) and pd.isnull(_lossfilename):
            return "Error creating LALAE. This scenario not used"

        # try:
            #First, get loss file or loss and alae, if alae file exists
            #Then, assign Event ID to Individual Claims so detail isn't lost for Event LALAE

        if pd.isnull(_alaefilename):
            temploss = adjustNumberofSims(pl.read_csv(
                source=lossfolder+"/"+_lossfilename,
                has_header=True,
                columns=[
                    "Simulation",
                    "Risk Source ID",
                    "Event ID",
                    "Loss #",
                    "Gross Loss",
                ],
                dtypes=[
                    pl.Int64,
                    pl.Int64,
                    pl.Int64,
                    pl.Int64,
                    pl.Float64,
                ],
            ),_numsims)
        else:
            temploss = adjustNumberofSims(pl.read_csv(
                source=lossfolder+"/"+_lossfilename,
                has_header=True,
                columns=[
                    "Simulation",
                    "Risk Source ID",
                    "Event ID",
                    "Loss #",
                    "Gross Loss",
                ],
                dtypes=[
                    pl.Int64,
                    pl.Int64,
                    pl.Int64,
                    pl.Int64,
                    pl.Float64,
                ],
            ),_numsims)

            temploss = (temploss
            .join(adjustNumberofSims(pl.read_csv(
                    source=lossfolder+"/"+_alaefilename,
                    has_header=True,
                    columns=[
                        "Simulation",
                        "Risk Source ID",
                        "Event ID",
                        "Loss #",
                        "Gross ALAE",
                    ],
                    dtypes=[
                        pl.Int64,
                        pl.Int64,
                        pl.Int64,
                        pl.Int64,
                        pl.Float64,
                    ],
                ),_numsims),
                how="left",
                on=['Simulation','Risk Source ID','Event ID','Loss #']))
        
        if pd.isnull(_clasheventidfile):
            pass
        else:
            tempmap=pl.read_csv(
                source=lossfolder+"/"+_clasheventidfile,
                has_header=True,
                columns=['Simulation','Risk Source ID','Loss #','Gross Loss','Ceded - Clash'],
                dtypes=[pl.Int64,pl.Int64,pl.Int64,pl.Float64,pl.Float64]
            )
            tempmap=(tempmap
                        .filter(pl.col('Ceded - Clash')>0)
                        .with_columns((pl.col('Gross Loss')-pl.col('Ceded - Clash')).alias('Net Loss'))
                        .sort(['Simulation','Risk Source ID','Loss #'])
                        .with_columns(pl.cum_sum('Net Loss').over(['Simulation','Risk Source ID']).alias('Net Loss Total'))
                        .with_columns((((pl.col('Net Loss Total')-1)//100)+1).alias('Clash ID'))
                        .with_columns((-1*pl.col('Clash ID')).alias('Clash ID'))   
                        .select(['Simulation','Risk Source ID','Loss #','Clash ID']))
            
            temploss=(temploss
                        .join(tempmap,how='left',on=['Simulation','Risk Source ID','Loss #'])
                        .with_columns(pl.when(pl.col('Clash ID').is_null())
                                    .then(pl.col('Event ID'))
                                    .otherwise(pl.col('Clash ID'))
                                    .cast(pl.Int64)
                                    .alias('Event ID'))
                        .drop('Clash ID'))        

        return temploss

    if dfLossSpecs.shape[0]>0:
        MYLOGGER.debug('dfLossSpecs')
    
        lalae=pl.DataFrame()

        for row in dfLossSpecs.rows(named=True):
            temploss=readLossAndALAE(row['Loss CSV Filename'],row['ALAE CSV Filename'],
                                        row['Number of Simulations in Source'],row['Event ID Mapping File'])
            lalae= pl.concat([lalae,temploss],how='diagonal')

        numsets=math.ceil(lalae.shape[0]/25000000)
        simwidth=math.ceil(lalae.get_column('Simulation').max()/numsets)
        inclingrosscatrisksources=specs['Risk Sources'].filter(pl.col('Include in Gross')=='Include').filter(pl.col('Risk Source Type').is_in(['AIR Cat','RMS Cat','RMS Attritional Cat'])).get_column('Risk Source ID').unique().to_list()
        catperils=specs['Cat Peril Maps'].select(['Code','Peril']).unique()
        catperilmap=dict(zip(catperils.get_column('Code'),catperils.get_column('Peril')))

        result=pl.DataFrame()

        for i in range(numsets):
            if i==numsets-1:
                rng=range(i*simwidth+1,specs['maxsims']+1)
            else:
                rng=range(i*simwidth+1,(i+1)*simwidth+1)

            result= pl.concat([result,
                                (pl.LazyFrame(lalae
                                    .filter(pl.col('Simulation').is_in(rng))
                                    .with_columns(
                                        [
                                            pl.col("Gross Loss").fill_null(strategy="zero"),
                                            pl.col("Gross ALAE").fill_null(strategy="zero"),
                                        ],
                                    ).rename({"Gross Loss": "Loss", "Gross ALAE": "ALAE"})
                                    .with_columns(
                                        (pl.col("Loss") + pl.col("ALAE")).alias("Loss and ALAE")
                                    ).drop(["ALAE"])
                                    .filter(pl.col('Loss and ALAE')>0)
                                    .with_columns(pl.col("Risk Source ID").replace_strict(specs["risksourcetypes"],default='Missing RS Type').alias("RS Type"))
                                    .with_columns(pl.when(pl.col('RS Type').is_in(['AIR Cat','RMS Cat','RMS Attritional Cat']))
                                                    .then(pl.when(pl.col('Risk Source ID').is_in(inclingrosscatrisksources))
                                                        .then(pl.col('Loss'))
                                                        .otherwise(pl.lit(0)))
                                                        .alias('CatLoss'))                                                   
                                    #Add Peril for AIR and RMS Risk Sources, Change 
                                    .with_columns(pl.when(pl.col('RS Type')=='AIR Cat')
                                                    .then((pl.col('Event ID')//10000000).cast(pl.Utf8))
                                                    .when(pl.col('RS Type')=='RMS Cat')
                                                    .then(pl.when(pl.col('Event ID')<10000000)
                                                        .then(("7_"+((pl.col('Event ID')//100000).cast(pl.Utf8))))
                                                        .otherwise(("8_"+((pl.col('Event ID')//1000000).cast(pl.Utf8)))))
                                                    .otherwise(pl.lit('Non-Cat'))
                                                    .alias("Peril"))
                                    #Set Event ID to 0 for attritional cats (either attritional RMS Risk Source or event loss < threshold)
                                    .with_columns(pl.when(pl.col('RS Type').is_in(['AIR Cat','RMS Cat']))
                                                        .then(pl.when(pl.col('CatLoss').sum().over(['Simulation','Event ID'])<specs['cateventaggthreshold'])
                                                                .then(pl.lit(0))
                                                                .otherwise(pl.col('Event ID')))
                                                    .when(pl.col('RS Type')=='RMS Attritional Cat')
                                                    .then(pl.lit(0))
                                                    .otherwise(pl.col('Event ID').alias("Event ID"))
                                                    .alias("Event ID"))
                                    #Get Event ID count for each large cat risk source (to check for duplicate event ids in single simulation year)
                                    .with_columns(pl.when((pl.col('RS Type').is_in(['AIR Cat','RMS Cat']))&(pl.col('Event ID')>0))
                                                  .then(pl.count('Loss #').over(['Simulation','Risk Source ID','Event ID']))
                                                  .otherwise(pl.lit(1))
                                                  .alias('Event ID Count'))
                                    #ADD SUFFIX TO REPEAT OCCURRENCES --- LOSS # FROM RISK EXPLORER IS IDENTICAL FOR SAME EVENT ACROSS RISK SOURCES. IF NOT USING RISK EXPLORER, MAKE SURE LOSS # CAN BE SIMILARLY USED TO IDENTIFY SAME EVENT ACROSS RISK SOURCES
                                    #ASSUMES NO MORE THAN 10000 CAT LOSSES PER SIM
                                    .with_columns(pl.when(pl.col('Event ID Count')>1)
                                                  .then((pl.col('Event ID')*10000)+pl.col('Loss #'))
                                                  .otherwise(pl.col('Event ID'))
                                                  .alias('Event ID'))
                                    .with_columns(pl.when(pl.col('RS Type').is_in(['AIR Cat','RMS Cat','RMS Attritional Cat']))
                                                    .then(pl.lit(0))
                                                    .otherwise(pl.col('Loss #'))
                                                    .alias('Loss #'))
                                    .drop(["CatLoss",'Event ID Count'])
                                    .group_by(['Simulation','RS Type','Risk Source ID','Peril','Event ID','Loss #']).sum())
                                    .with_columns(pl.when(pl.col('RS Type').is_in(['AIR Cat','RMS Cat','RMS Attritional Cat']))
                                                  .then(pl.col('Peril').replace_strict(catperilmap,default='Unnamed Cat Peril'))
                                                  .otherwise(pl.lit('Non-Cat'))
                                                  .alias('Peril'))
                                    .with_columns([pl.when(pl.col('RS Type').is_in(['Attritional','RMS Attritional Cat']))
                                                    .then(pl.lit(0))
                                                    .otherwise(pl.col('Loss #'))
                                                    .alias('Loss #'),
                                                    pl.when(pl.col('RS Type')=='Attritional')
                                                    .then(pl.lit(0))
                                                    .otherwise(pl.col('Event ID'))
                                                    .alias('Event ID'),
                                                    pl.when((pl.col("RS Type")=="Individual")&(pl.col("Event ID")==0))
                                                    .then(pl.lit(1))
                                                    .otherwise(pl.lit(0))
                                                    .alias("ones")])
                                    .with_columns([pl.col('Event ID').min().over(['Simulation']).alias('mineventid'),
                                                pl.when((pl.col("RS Type")=="Individual")&(pl.col("Event ID")==0))
                                                .then(pl.lit(True))
                                                .otherwise(pl.lit(False))
                                                .alias("Add Event ID")])
                                    .select([
                                                    pl.all().exclude("ones"),
                                                    pl.cum_sum("ones")
                                                    .over(["Simulation", "Add Event ID"])
                                                    .alias("New Event ID"),
                                                ])
                                    .with_columns(pl.when(pl.col("Add Event ID")==True)
                                                    .then(-1*pl.col('New Event ID'))
                                                    .otherwise(pl.col('Event ID'))
                                                    .alias('Event ID'))
                                    .with_columns(pl.when(pl.col('Add Event ID')==True)
                                                    .then(pl.col('mineventid')+pl.col('Event ID'))
                                                    .otherwise(pl.col('Event ID'))
                                                    .alias('Event ID'))
                                    .drop('Add Event ID','New Event ID','mineventid')
                                    .with_columns([pl.when(pl.col('Event ID')!=0)
                                                           .then(pl.col('Loss and ALAE').sum().over(['Simulation','Event ID']))
                                                           .otherwise(pl.lit(0))
                                                            .alias('Event LALAE'),
                                                    pl.when(pl.col('RS Type')=='Individual')
                                                    .then(1 * (pl.col("Loss") > 0))
                                                    .otherwise(pl.lit(0))
                                                    .alias('Large Claim Count'),
                                                    pl.when((pl.col('RS Type').is_in(['AMS Cat','RMS Cat'])) & (pl.col('Event ID')>0))
                                                    .then(1 * (pl.col("Loss") > 0))
                                                    .otherwise(pl.lit(0))
                                                    .alias('Cat Event Count')]).collect())])     
        return result   
    else:   #No loss sources
        return pl.DataFrame()

def CreateLALAE(specs,scenario):
    MYLOGGER.debug('Start CreateLALAE')
    # try:
    if specs['connectiontype']==1:
        if specs['forcerefresh']=='Yes':
            result=CreateLALAEFromCSV(specs,scenario)
            _misc.saveToParquet(result,specs['dataparquetpath']+'/'+scenario + ' - LALAE.parquet')
        elif scenario + ' - LALAE.parquet' in _misc.getFileList(specs['dataparquetpath'],'parquet'):
            result=_misc.getFromParquet(specs['dataparquetpath']+'/'+scenario + ' - LALAE.parquet').collect()
        else:
            result=CreateLALAEFromCSV(specs,scenario)
            _misc.saveToParquet(result,specs['dataparquetpath']+'/'+scenario + ' - LALAE.parquet')
        return result
    else:
        #READ FROM GZIP
        result=pl.DataFrame()
        return result

def missingAndRequiredCombinations(specs,scenario:str,infotype:str,df=None,strategies=None,segmentations=None,treaties=None,layers=None,inclNone=True,inclNoReinsurance=True,inclAllTreaties=True):
    #get missing combinations for infotype
    #df is dataframe version of requested combinations
    #alternatively, specify list or string of requested strategies,segmentations,treaties,layers
    #if inclNone=True, include 'None' in segmentations
    #if inclNoReinsurance=True, include 'No Reinsurance' in strategies
    #if inclAllTreaties=True, include 'All Treaties' in treaties

    validNoneSegmentationInfotypes=['gross uw','ceded uw details by layer','ceded net uw details by strategy','summary uw statistics']
    validNoReinsuranceStrategyInfotypes=['ceded net uw details by strategy','summary uw statistics']
    validAllTreatiesTreatyInfotypes=['ceded net uw details by strategy','summary uw statistics']

    #First, check for valid infotype
    #region
    infotype=infotype.lower()
    if infotype not in ['gross uw','ceded loss','loss allocation','ceded uw','premium allocation',
                        'ceded uw by segmentation','ceded uw details by layer','ceded net uw details by strategy',
                        'summary uw statistics']:
        return "Invalid infotype in missingCombinations"
    #endregion
    
    #Then, check for valid scenario
    #region
    if scenario not in specs['Scenarios'].get_column('Scenario').unique().to_list():
        return "Invalid scenario in missingCombinations"
    #endregion
    
    #Get lists of valid treaties, layers, segmentations, strategies
    #region
    validTreaties=specs['Treaty Terms'].get_column('Treaty Loss Layer').unique().to_list()
    validLayers=specs['Treaty Terms'].get_column('Layer Lookup').unique().to_list()
    validSegmentations=specs['Segmentations'].get_column('Segmentation').unique().to_list()
    validStrategies=specs['Treaty Terms'].get_column('Strategy').unique().to_list()

    if inclAllTreaties and infotype in validAllTreatiesTreatyInfotypes:
        validTreaties=validTreaties+['All Treaties']

    if infotype in validNoneSegmentationInfotypes and inclNone:
        pass
    else:
        validSegmentations=[x for x in validSegmentations if x!='None']

    if inclNoReinsurance and infotype in validNoReinsuranceStrategyInfotypes:
        validStrategies=validStrategies+['No Reinsurance']
    #endregion

    #Then, check for valid filtering inputs --- df, segmentations, strategies, treaties, layers and derive df from lists, if no df
    #region
    filtercolsdict={'gross uw':['Segmentation'],'ceded loss':['Strategy','Layer'],'loss allocation':['Layer','Segmentation'],'ceded uw':['Treaty','Strategy'],'premium allocation':['Treaty','Strategy','Segmentation'],
                    'ceded uw by segmentation':['Treaty','Strategy','Segmentation'],'ceded uw details by layer':['Treaty','Strategy','Segmentation'],'ceded net uw details by strategy':['Strategy','Segmentation'],
                    'summary uw statistics':['Treaty','Strategy','Segmentation']}
    keycolsdict={'gross uw':['Segmentation'],'ceded loss':['Layer'],'loss allocation':['Layer','Segmentation'],'ceded uw':['Treaty','Strategy'],'premium allocation':['Treaty','Strategy','Segmentation'],
                'ceded uw by segmentation':['Treaty','Strategy','Segmentation'],'ceded uw details by layer':['Treaty','Strategy','Segmentation'],'ceded net uw details by strategy':['Strategy','Segmentation'],
                'summary uw statistics':['Treaty','Strategy','Segmentation']}
    
    if not isinstance(df,pl.DataFrame):
        df=pl.DataFrame()
        #use segmentations, strategies, treaties, layers parameters to create df
        if 'Treaty' in filtercolsdict[infotype]:
            if treaties:
                if isinstance(treaties,str):
                    if treaties=='All':
                        treaties=validTreaties
                    else:
                        treaties=[treaties]
                elif isinstance(treaties,list):
                    pass
                else:
                    return "Invalid treaties in missingCombinations"
            else:
                #if missing, use all valid treaties
                treaties=validTreaties

            if inclAllTreaties:
                treaties=treaties+['All Treaties']

            treaties=[x for x in list(set(treaties)) if x in validTreaties]
            
            if len(treaties)==0:
                return "No valid treaties in missingCombinations"
            
            df=pl.DataFrame({'Treaty':treaties})

        if 'Strategy' in filtercolsdict[infotype]:
            if strategies:
                if isinstance(strategies,str):
                    if strategies=='All':
                        strategies=validStrategies
                    else:
                        strategies=[strategies]
                elif isinstance(strategies,list):
                    pass
                else:
                    return "Invalid strategies in missingCombinations"
            else:
                #if missing, use all valid treaties
                strategies=validStrategies

            if inclNoReinsurance:
                strategies=strategies+['No Reinsurance']
            
            strategies=[x for x in list(set(strategies)) if x in validStrategies]

            if len(strategies)==0:
                return "No valid strategies in missingCombinations"            

            if df.shape[0]==0:
                df=pl.DataFrame({'Strategy':strategies})
            else:
                df=df.with_columns(pl.lit(strategies).alias('Strategy')).explode('Strategy')

        if 'Segmentation' in filtercolsdict[infotype]:
            if segmentations:
                if isinstance(segmentations,str):
                    if segmentations=='All':
                        segmentations=validSegmentations
                    else:
                        segmentations=[segmentations]
                elif isinstance(segmentations,list):
                    pass
                else:
                    return "Invalid segmentations in missingCombinations"
            else:
                #if missing, use all valid treaties
                segmentations=validSegmentations

            if not inclNone:
                segmentations=[x for x in segmentations if x!='None']
            
            segmentations=[x for x in list(set(segmentations)) if x in validSegmentations]

            if len(segmentations)==0:
                return "No valid segmentations in missingCombinations"            

            if df.shape[0]==0:
                df=pl.DataFrame({'Segmentation':segmentations})
            else:
                df=df.with_columns(pl.lit(segmentations).alias('Segmentation')).explode('Segmentation')                     

        if 'Layer' in filtercolsdict[infotype]:
            if layers:
                if isinstance(layers,str):
                    if layers=='All':
                        layers=validLayers
                    else:
                        layers=[layers]
                elif isinstance(layers,list):
                    pass
                else:
                    return "Invalid layers in missingCombinations"
            else:
                #if missing, use all valid treaties
                layers=validLayers
            
            layers=[x for x in list(set(layers)) if x in validLayers]
            
            if len(layers)==0:
                return "No valid layers in missingCombinations"                        

            if df.shape[0]==0:
                df=pl.DataFrame({'Layer':layers})
            else:
                df=df.with_columns(pl.lit(layers).alias('Layer')).explode('Layer')                                 
    elif not isinstance(df,pl.DataFrame):
        return "Invalid df in missingCombinations"
    elif not all([x in df.columns for x in filtercolsdict[infotype]]):
        return "Invalid df columns in missingCombinations"
    else:
        df=df.select(filtercolsdict[infotype]).unique()
        if 'Treaty' in filtercolsdict[infotype]:
            df=df.filter(pl.col('Treaty').is_in(validTreaties))

        if 'Segmentation' in filtercolsdict[infotype]:
            df=df.filter(pl.col('Segmentation').is_in(validSegmentations))

        if 'Strategy' in filtercolsdict[infotype]:
            df=df.filter(pl.col('Strategy').is_in(validStrategies))
    #endregion
    
    #Then, get required combinations
    #region
    if infotype=='gross uw':
        required=specs['Segmentations'].select(['Segmentation']).unique()
    elif infotype=='ceded loss':
        required=specs['Treaty Terms'].filter(pl.col('Strategy').is_in(df.get_column('Strategy').unique().to_list())).select(['Layer Lookup']).unique().rename({'Layer Lookup':'Layer'})
    elif infotype=='loss allocation':
        temp=specs['Treaty Terms'].select(['Layer Lookup']).unique().rename({'Layer Lookup':'Layer'})
        if temp.shape[0]==0:
            required= pl.DataFrame(schema={'Layer':pl.Utf8,'Segmentation':pl.Utf8})
        else:
            required=(temp
                    .with_columns(pl.lit(specs['Segmentations'].filter(pl.col('Segmentation')!='None').get_column('Segmentation').unique().to_list())
                                    .alias('Segmentation'))
                    .explode('Segmentation'))             
    elif infotype=='ceded uw':
        required=specs['Treaty Terms'].select(['Strategy','Treaty Loss Layer']).unique().rename({'Treaty Loss Layer':'Treaty'})
    elif infotype in ['premium allocation','ceded uw by segmentation']:
        required=(specs['Treaty Terms'].select(['Strategy','Treaty Loss Layer']).unique().rename({'Treaty Loss Layer':'Treaty'})
                .with_columns(pl.lit(specs['Segmentations'].filter(pl.col('Segmentation')!='None').get_column('Segmentation').unique().to_list())
                                .alias('Segmentation'))
                .explode('Segmentation'))                
    elif infotype=='ceded net uw details by strategy':
        required=(pl.DataFrame({'Segmentation':specs['Segmentations'].get_column('Segmentation').unique().to_list()})
                    .with_columns(pl.lit(specs['Treaty Terms'].get_column('Strategy').unique().to_list()+['No Reinsurance']).alias('Strategy'))
                    .explode('Strategy'))
    elif infotype=='ceded uw details by layer':
        temp=specs['Treaty Terms'].select(['Strategy','Treaty Loss Layer']).unique().rename({'Treaty Loss Layer':'Treaty'})
        if temp.shape[0]==0:
            required= pl.DataFrame(schema={'Treaty':pl.Utf8,'Segmentation':pl.Utf8})
        else:
            required=(temp
                    .with_columns(pl.lit(specs['Segmentations'].get_column('Segmentation').unique().to_list())
                                    .alias('Segmentation'))
                    .explode('Segmentation')) 
    elif infotype=='summary uw statistics':
        required=pl.concat([
                    #First, excluding 'No Reinsurance' and 'All Treaties'
                    (specs['Treaty Terms'].select(['Strategy','Treaty Loss Layer']).unique().rename({'Treaty Loss Layer':'Treaty'})
                    .with_columns(pl.lit(specs['Segmentations'].get_column('Segmentation').unique().to_list())
                                    .alias('Segmentation'))
                    .explode('Segmentation')),
                    #Then, add on No Reinsurance strategy for all segmentations
                    (pl.DataFrame({'Segmentation':specs['Segmentations'].get_column('Segmentation').unique().to_list()})
                    .with_columns(pl.lit('No Reinsurance').alias('Strategy'))
                    .with_columns(pl.lit('All Treaties').alias('Treaty'))),
                    #Finally, add on All Treaties strategy for all strategies and segmentations
                    (pl.DataFrame({'Strategy':specs['Treaty Terms'].get_column('Strategy').unique().to_list()})
                    .with_columns(pl.lit('All Treaties').alias('Treaty'))
                    .with_columns(pl.lit(specs['Segmentations'].get_column('Segmentation').unique().to_list())
                                    .alias('Segmentation'))
                    .explode('Segmentation'))],how='diagonal')
    #endregion

    #Then, refine df to only include required combinations and refine required to only include requested combinations
    #region
    required=df.join(required,how='inner',on=keycolsdict[infotype])
    #endregion
     
    #Then, get available combinations if saved results exist
    #region
    savedict={'gross uw':'savegross','ceded loss':'savecededloss','loss allocation':'savelossalloc','ceded uw':'savecededuw','premium allocation':'savepremalloc',
                'ceded uw by segmentation':'savecededuw','ceded uw details by layer':'savedetail','ceded net uw details by strategy':'savedetail',
                'summary uw statistics':'savesummary'}
        
    if specs[savedict[infotype]].upper()!='YES':
        available=pl.DataFrame()
    else:
        if os.path.exists(f'{specs["dataparquetpath"]}/{scenario} - {infotype}.parquet'):
            available=_misc.getFromParquet(f'{specs["dataparquetpath"]}/{scenario} - {infotype}.parquet').select(keycolsdict[infotype]).unique().collect()
        else:
            available=pl.DataFrame()

    if available.shape[0]==0:
        return (required,required)
    else:
        return (required.join(available
                         .with_columns(pl.lit('Available').alias('Available'))
                         ,how='left',on=keycolsdict[infotype])
                         .filter(pl.col('Available').is_null())
                         .drop('Available'),required) 
    #endregion

def getResults(specs,infotype,scenario,df=None):
    #First, check for valid infotype
    #region
    infotype=infotype.lower()
    if infotype not in ['gross uw','ceded loss','loss allocation','ceded uw','premium allocation',
                        'ceded uw by segmentation','ceded uw details by layer','ceded net uw details by strategy',
                        'summary uw statistics']:
        return "Invalid infotype in getResults"
    #endregion    

    if infotype in ['premium allocation','summary uw statistics']:
        if infotype in specs.keys():
            if scenario in specs[infotype].keys():
                if isinstance(df,pl.DataFrame):
                    if df.shape[0]==0:
                        return pl.DataFrame()                
                    
                    #Add column for filtering
                    cols=df.columns
                    keep=df.with_columns(pl.concat_str(cols,separator='|').alias('filter')).get_column('filter').unique().to_list()                
                    return (specs[infotype][scenario]
                                .with_columns(pl.concat_str(cols,separator='|').alias('filter'))
                                .filter(pl.col('filter').is_in(keep))
                                .drop('filter'))
                else:
                    return specs[infotype][scenario]

    if os.path.exists(f'{specs["dataparquetpath"]}/{scenario} - {infotype}.parquet'):
        if isinstance(df,pl.DataFrame):
            if df.shape[0]==0:
                return pl.DataFrame()
            
            #Add column for filtering
            cols=df.columns
            keep=df.with_columns(pl.concat_str(cols,separator='|').alias('filter')).get_column('filter').unique().to_list()
            return (_misc.getFromParquet(f'{specs["dataparquetpath"]}/{scenario} - {infotype}.parquet')
                    .with_columns(pl.concat_str(cols,separator='|').alias('filter'))
                    .filter(pl.col('filter').is_in(keep))
                    .drop('filter')
                    .collect())            
        else:
            return _misc.getFromParquet(f'{specs["dataparquetpath"]}/{scenario} - {infotype}.parquet').collect()
    else:
        return pl.DataFrame()

def grossUWResult(specs,scenario,segmentations=None,df=None):
    #region
    infotype='gross uw'
    if not isinstance(df,pl.DataFrame):
        missingAndRequired=missingAndRequiredCombinations(specs,scenario,infotype,segmentations=segmentations)
    else:
        missingAndRequired=missingAndRequiredCombinations(specs,scenario,infotype,df=df)          

    missing=missingAndRequired[0]

    if missing.shape[0] + missingAndRequired[1].shape[0]==0:
       return 'No gross uw results to calculate'
    elif missing.shape[0]==0:
        #get from file
        return getResults(specs,infotype,scenario,missingAndRequired[1])
    #endregion    

    #region
    grossRSIDs=specs['Risk Sources'].filter(pl.col('Include in Gross')=='Include').get_column('Risk Source ID').unique().to_list()

    #Check if LALAE file exists for scenario
    if isinstance(_misc.getFromParquet(f'{specs["dataparquetpath"]}/{scenario} - LALAE.parquet'),pl.LazyFrame):
        lalae=_misc.getFromParquet(f'{specs["dataparquetpath"]}/{scenario} - LALAE.parquet').filter(pl.col('Simulation')<=specs['maxsims']).collect()
    else:
        lalae=CreateLALAE(specs,scenario).filter(pl.col('Simulation')<=specs['maxsims'])
        _misc.saveToParquet(lalae,f'{specs["dataparquetpath"]}/{scenario} - LALAE.parquet')

    premdict=dict(zip(specs['Gross Premium and Expense'].filter(pl.col('Scenario')==scenario).get_column('Risk Source ID'),
                    specs['Gross Premium and Expense'].filter(pl.col('Scenario')==scenario).get_column('Premium')))
    expdict=dict(zip(specs['Gross Premium and Expense'].filter(pl.col('Scenario')==scenario).get_column('Risk Source ID'),
                    specs['Gross Premium and Expense'].filter(pl.col('Scenario')==scenario).get_column('Expense'))) 
    
    lalae=(lalae.filter(pl.col('Risk Source ID').is_in(grossRSIDs))
            .select(['Simulation','Risk Source ID','Loss and ALAE'])
            .group_by(['Simulation','Risk Source ID']).sum())
    lalae=(lalae
            .group_by("Risk Source ID", maintain_order=True)
            .agg(pl.int_range(1, specs['maxsims']+1, 1).alias('Simulation'))
            .explode("Simulation")
            .join(lalae,on=["Risk Source ID","Simulation"], how="left")                
            .with_columns([pl.col('Loss and ALAE').fill_null(0.0),
                        pl.col('Risk Source ID').replace_strict(premdict,default=0).alias('Premium'),
                        pl.col('Risk Source ID').replace_strict(expdict,default=0.0).alias('Expense')])
            .rename({'Loss and ALAE':'Gross Loss and ALAE','Premium':'Gross Premium','Expense':'Gross Expense'}))
    
    if 'None' in missing.get_column('Segmentation').unique().to_list():
        result=(lalae.select(['Simulation','Gross Loss and ALAE','Gross Premium','Gross Expense'])
                            .with_columns([pl.lit('None').alias('Segmentation'),
                                            pl.lit('NA').alias('Segment')])
                            .group_by(['Segmentation','Segment','Simulation']).sum()
                            .select(['Segmentation','Segment','Simulation','Gross Loss and ALAE','Gross Premium','Gross Expense']))
    else:
        result=pl.DataFrame(schema={'Segmentation':pl.Utf8,'Segment':pl.Utf8,'Simulation':pl.Int64,'Gross Loss and ALAE':pl.Float64,'Gross Premium':pl.Float64,'Gross Expense':pl.Float64})     
    
    _segmentations=specs['Segmentations'].filter(pl.col('Segmentation')!='None').filter(pl.col('Segmentation').is_in(missing.get_column('Segmentation').unique().to_list()))
    if _segmentations.shape[0]>0:
        result=pl.concat([result,(lalae
            .with_columns([((pl.col('Gross Loss and ALAE')/(pl.col('Gross Loss and ALAE').sum().over(['Simulation'])))).alias('Allocated Gross Loss'),
                        ((pl.col('Gross Premium')/(pl.col('Gross Premium').sum().over(['Simulation'])))).alias('Allocated Premium'),
                        ((pl.col('Gross Expense')/(pl.col('Gross Expense').sum().over(['Simulation'])))).alias('Allocated Expense')])
            .group_by(['Simulation'])
                    .agg(pl.col('Gross Loss and ALAE').sum(),
                        pl.col('Gross Premium').sum(),
                        pl.col('Gross Expense').sum(),
                        pl.col('Risk Source ID'),
                        pl.col('Allocated Gross Loss'),
                        pl.col('Allocated Premium'),
                        pl.col('Allocated Expense'))
            .join(_segmentations,how='cross')
            .explode(['Risk Source ID','Allocated Gross Loss','Allocated Premium','Allocated Expense'])
            .filter(pl.col('Risk Source ID').is_in(pl.col('RSID List')))
            .drop(['RSID List','RSID List String','Risk Source ID'])
            .group_by(['Segmentation','Segment','Simulation'])
            .agg(((pl.col('Allocated Gross Loss'))*(pl.col('Gross Loss and ALAE'))).sum().alias('Gross Loss and ALAE'),
                    ((pl.col('Allocated Premium'))*(pl.col('Gross Premium'))).sum().alias('Gross Premium'),
                    ((pl.col('Allocated Expense'))*(pl.col('Gross Expense'))).sum().alias('Gross Expense')))],how='diagonal')   
    #endregion         

    #Return results and save, if necessary
    #region  
    if specs['savegross'].upper()=='YES':
        #if file exists, append missing. Otherwise, create file.
        if os.path.exists(f'{specs["dataparquetpath"]}/{scenario} - {infotype}.parquet'):
            _misc.saveToParquet(pl.concat([result,getResults(specs,infotype,scenario)]),
                                f'{specs["dataparquetpath"]}/{scenario} - {infotype}.parquet')
            return getResults(specs,infotype,scenario,df=missingAndRequired[1])
        else:
            _misc.saveToParquet(result,f'{specs["dataparquetpath"]}/{scenario} - {infotype}.parquet')
            return result
    else:
        return result
    #endregion

def cededLayerLosses(specs,scenario,strategies=None,layers=None):
    #For a single scenario
    #If LALAE file doesn't exist for scenario, create it
    #Get the missing layers for the selected strategies
    #Get the required contracts for the missing layers

    #filterContracts function gets list of all required contracts across all levels, for list of base contracts
    def filterContracts(contracts,outputlist=[]):
        outputlist+=contracts
        tempcontracts=specs['Contract Layers'].filter(pl.col('Layer').is_in(contracts))    
        dependencies=[]
        for row in tempcontracts.rows(named=True):
            dependencies+=row['Underlying Layers']
            dependencies+=row['tempInuring']
        dependencies=list(set(dependencies))
        if len(dependencies)>0:
            filterContracts(dependencies,outputlist)
        return outputlist

    #Derive missing and required layers and return results if no missing layers
    #region
    infotype='ceded loss'
    missingAndRequired=missingAndRequiredCombinations(specs,scenario,infotype,strategies=strategies,layers=layers)
    missing=missingAndRequired[0]
    missingcontracts=missing.get_column('Layer').unique().to_list()
    if len(missingcontracts)+missingAndRequired[1].shape[0]==0:
       return 'No ceded layer losses to calculate'
    elif len(missingcontracts)==0:
        #get from file
        return getResults(specs,infotype,scenario,df=missingAndRequired[1].select(['Layer']).unique())
    #endregion
             
    #If no return has occurred, there are layers to calculate. Calculate layers.
    #region
    dfLossLayers=(specs['Contract Layers']
                    .filter(pl.col('Layer').is_in(filterContracts(missingcontracts)))
                    .drop(["Risk Sources","Deemed or Placed","Subject Risk Source Group"]))    
        
    dfResults = pl.DataFrame(
        schema={
            "Layer": pl.Utf8,
            "Simulation": pl.Int64,
            "Event ID": pl.Int64,
            "Risk Source ID": pl.Int64,
            "Peril": pl.Utf8,
            "Loss #": pl.Int64,
            "Layer Loss": pl.Float64,
            "Layer Loss and ALAE": pl.Float64,
        }
    )        
    #Check if LALAE file exists for scenario
    if isinstance(_misc.getFromParquet(f'{specs["dataparquetpath"]}/{scenario} - LALAE.parquet'),pl.LazyFrame):
        lalae=_misc.getFromParquet(f'{specs["dataparquetpath"]}/{scenario} - LALAE.parquet').filter(pl.col('Simulation')<=specs['maxsims']).collect()
    else:
        lalae=CreateLALAE(specs,scenario).filter(pl.col('Simulation')<=specs['maxsims'])
        _misc.saveToParquet(lalae,f'{specs["dataparquetpath"]}/{scenario} - LALAE.parquet')

    numlevels = dfLossLayers.get_column("Level").max()
    asplacedpcts=specs['Treaty Terms'].filter(pl.col('Layer Lookup').is_in(specs['Contract Layers'].filter(pl.col('Has Inuring As Placed')==True).get_column('Layer').to_list())).select(['Layer Lookup','Placement Pct']).group_by('Layer Lookup').sum()
    asplacedpcts=dict(zip(asplacedpcts.get_column('Layer Lookup').to_list(),asplacedpcts.get_column('Placement Pct').to_list()))        
        
    for i in range(1, numlevels + 1):
        layersThisLevel=(dfLossLayers
                        .filter(pl.col('Level')==i)
                        .with_columns(pl.when(pl.col("Underlying Layers").list.len() == 0)
                                    .then(pl.lit('Gross'))
                                    .otherwise(pl.lit('Underlying'))
                                    .alias('Data Source')))

        for row in layersThisLevel.rows(named=True):      
            temp=pl.DataFrame()
            if row['Data Source']=='Gross':
                if (row['HasPerClaim']==True)|(row['HasPerEvent']==True):
                    temp=(lalae          
                                .filter(
                                    (pl.col('Risk Source ID').is_in(row['Risk Source IDs']))
                                    &(pl.col('Event ID')!=0))
                                .with_columns([pl.col('Loss').cast(pl.Float64),
                                    pl.col('Loss and ALAE').cast(pl.Float64)])
                                .with_columns(pl.when(row['Subject Perils']==[])
                                            .then(pl.lit('Keep'))
                                            .when(pl.col('Peril').is_in(row['Subject Perils']))
                                            .then(pl.lit('Keep'))
                                            .otherwise(pl.lit('Drop'))
                                            .alias('Flag Peril'))
                                .filter(pl.col('Flag Peril')=='Keep')
                                .drop('Flag Peril')
                                .select(['Simulation','Event ID','Risk Source ID','Peril','Loss #','Loss','Loss and ALAE'])
                                .rename({"Loss": "Subject Loss",
                                    "Loss and ALAE": "Subject Loss and ALAE"}))    
                else:
                    temp=(lalae          
                                .filter(
                                pl.col('Risk Source ID').is_in(row['Risk Source IDs']))
                                .with_columns([pl.col('Loss').cast(pl.Float64),
                                    pl.col('Loss and ALAE').cast(pl.Float64)])
                                .with_columns(pl.when(row['Subject Perils']==[])
                                            .then(pl.lit('Keep'))
                                            .when(pl.col('Peril').is_in(row['Subject Perils']))
                                            .then(pl.lit('Keep'))
                                            .otherwise(pl.lit('Drop'))
                                            .alias('Flag Peril'))
                                .filter(pl.col('Flag Peril')=='Keep')
                                .drop('Flag Peril')                                        
                                .select(['Simulation','Event ID','Risk Source ID','Peril','Loss #','Loss','Loss and ALAE'])
                                .rename({"Loss": "Subject Loss",
                                    "Loss and ALAE": "Subject Loss and ALAE"}))
            elif row['Data Source']=='Underlying':
                if (row['HasPerClaim']==True)|(row['HasPerEvent']==True):
                    temp=(dfResults
                                .filter(
                                (pl.col('Risk Source ID').is_in(row['Risk Source IDs']+specs['Risk Sources'].filter(pl.col('Parent Risk Source ID for Inurances').is_in(row['Risk Source IDs'])).get_column('Risk Source ID').to_list()))
                                &(pl.col('Layer').is_in(row['Underlying Layers']))
                                &(pl.col('Event ID')!=0))
                                .with_columns(pl.when(row['Subject Perils']==[])
                                            .then(pl.lit('Keep'))
                                            .when(pl.col('Peril').is_in(row['Subject Perils']))
                                            .then(pl.lit('Keep'))
                                            .otherwise(pl.lit('Drop'))
                                            .alias('Flag Peril'))
                                .filter(pl.col('Flag Peril')=='Keep')
                                .drop('Flag Peril')                                       
                                .select(["Simulation","Event ID","Risk Source ID",'Peril',"Loss #","Layer Loss","Layer Loss and ALAE"])
                                .rename({"Layer Loss": "Subject Loss",
                                        "Layer Loss and ALAE": "Subject Loss and ALAE"}))
                else:
                    temp=(dfResults
                                .filter(
                                (pl.col('Risk Source ID').is_in(row['Risk Source IDs']+specs['Risk Sources'].filter(pl.col('Parent Risk Source ID for Inurances').is_in(row['Risk Source IDs'])).get_column('Risk Source ID').to_list()))                                         
                                &(pl.col('Layer').is_in(row['Underlying Layers'])))
                                .with_columns(pl.when(row['Subject Perils']==[])
                                            .then(pl.lit('Keep'))
                                            .when(pl.col('Peril').is_in(row['Subject Perils']))
                                            .then(pl.lit('Keep'))
                                            .otherwise(pl.lit('Drop'))
                                            .alias('Flag Peril'))
                                .filter(pl.col('Flag Peril')=='Keep')
                                .drop('Flag Peril')                                         
                                .select(["Simulation","Event ID","Risk Source ID",'Peril',"Loss #","Layer Loss","Layer Loss and ALAE"])
                                .rename({"Layer Loss": "Subject Loss",
                                        "Layer Loss and ALAE": "Subject Loss and ALAE"}))
                    
            if (len(row['Inuring Layers'])>0) & ((row['HasPerClaim']==True)|(row['HasPerEvent']==True)):
                (temp.extend(dfResults
                                .filter(
                                (pl.col('Risk Source ID').is_in(row['Risk Source IDs']+specs['Risk Sources'].filter(pl.col('Parent Risk Source ID for Inurances').is_in(row['Risk Source IDs'])).get_column('Risk Source ID').to_list()))
                                &(pl.col('Layer').is_in(row['tempInuring']))
                                &(pl.col('Event ID')!=0))
                                .rename({"Layer Loss": "Subject Loss",
                                        "Layer Loss and ALAE": "Subject Loss and ALAE"})
                                .with_columns(pl.when(row['Subject Perils']==[])
                                            .then(pl.lit('Keep'))
                                            .when(pl.col('Peril').is_in(row['Subject Perils']))
                                            .then(pl.lit('Keep'))
                                            .otherwise(pl.lit('Drop'))
                                            .alias('Flag Peril'))
                                .filter(pl.col('Flag Peril')=='Keep')
                                .drop('Flag Peril')                                              
                                .with_columns(pl.when(row['Has Inuring As Placed']==False)
                                            .then(pl.lit(1))
                                            .otherwise(pl.col('Layer').replace_strict(asplacedpcts,default=1))
                                            .alias('Inuring Pct'))
                                .with_columns([(pl.col("Subject Loss")
                                        * (-1.0)
                                        * pl.col("Inuring Pct"))
                                        .alias("Subject Loss"),
                                        (pl.col("Subject Loss and ALAE")
                                        * (-1.0)
                                        * pl.col("Inuring Pct"))
                                        .alias("Subject Loss and ALAE")])
                                .drop("Inuring Pct")
                                .select(["Simulation","Event ID","Risk Source ID","Peril","Loss #","Subject Loss","Subject Loss and ALAE"])
                                )
                        .group_by(["Simulation","Event ID","Risk Source ID","Peril","Loss #"]).sum())               
            elif (len(row['Inuring Layers'])>0):
                (temp.extend(dfResults
                                .filter(
                                (pl.col('Risk Source ID').is_in(row['Risk Source IDs']+specs['Risk Sources'].filter(pl.col('Parent Risk Source ID for Inurances').is_in(row['Risk Source IDs'])).get_column('Risk Source ID').to_list()))
                                &(pl.col('Layer').is_in(row['tempInuring'])))
                                .rename({"Layer Loss": "Subject Loss",
                                        "Layer Loss and ALAE": "Subject Loss and ALAE"})
                                .with_columns(pl.when(row['Subject Perils']==[])
                                            .then(pl.lit('Keep'))
                                            .when(pl.col('Peril').is_in(row['Subject Perils']))
                                            .then(pl.lit('Keep'))
                                            .otherwise(pl.lit('Drop'))
                                            .alias('Flag Peril'))
                                .filter(pl.col('Flag Peril')=='Keep')
                                .drop('Flag Peril')                                              
                                .with_columns(pl.when(row['Has Inuring As Placed']==False)
                                            .then(pl.lit(1))
                                            .otherwise(pl.col('Layer').replace_strict(asplacedpcts,default=1))
                                            .alias('Inuring Pct'))
                                .with_columns([(pl.col("Subject Loss")
                                        * (-1.0)
                                        * pl.col("Inuring Pct"))
                                        .alias("Subject Loss"),
                                        (pl.col("Subject Loss and ALAE")
                                        * (-1.0)
                                        * pl.col("Inuring Pct"))
                                        .alias("Subject Loss and ALAE")])
                                .drop("Inuring Pct")
                                .select(["Simulation","Event ID","Risk Source ID","Peril","Loss #","Subject Loss","Subject Loss and ALAE"])
                                ))

            if (len(row['Inuring Layers'])>0) or (len(row['Underlying Layers'])>1):
                temp=(temp
                      .group_by(["Simulation","Event ID","Risk Source ID",'Peril',"Loss #"])
                      .sum()             
                      .filter(pl.col('Subject Loss and ALAE')>0))                  
                                    
            if row['HasPerClaim']==True:
                if row['Per Claim ALAE Handling']=="Pro Rata":
                    temp=(temp
                            .filter(pl.col('Event ID')!=0)   #Exclude attritional risk sources from per claim calculations
                            .with_columns(pl.col("Subject Loss").alias("PerClmSubj"))
                            .filter(pl.col('PerClmSubj')>row['Per Claim Retention'])
                            .with_columns(pl.when((pl.col('PerClmSubj')-row['Per Claim Retention'])>row['Per Claim Limit'])
                                        .then(row['Per Claim Limit'])
                                        .otherwise(pl.col('PerClmSubj')-row['Per Claim Retention'])
                                        .alias('Layer Loss'))
                            .with_columns((pl.col('Layer Loss')*(pl.col('Subject Loss and ALAE')/pl.col('Subject Loss'))).alias('Layer Loss and ALAE'))
                            .with_columns([pl.col('Layer Loss').cast(pl.Float64),
                                pl.col('Layer Loss and ALAE').cast(pl.Float64)])
                            .drop(['Subject Loss','Subject Loss and ALAE','PerClmSubj'])
                            .rename({'Layer Loss':'Subject Loss','Layer Loss and ALAE':'Subject Loss and ALAE'}))
                elif row['Per Claim ALAE Handling']=="Excluded":
                    temp=(temp
                            .filter(pl.col('Event ID')!=0)     #Exclude attritional risk sources from per claim calculations                         
                            .with_columns((pl.col("Subject Loss")).alias("PerClmSubj"))
                            .filter(pl.col('PerClmSubj')>row['Per Claim Retention'])
                            .with_columns(pl.when((pl.col('PerClmSubj')-row['Per Claim Retention'])>row['Per Claim Limit'])
                                        .then(row['Per Claim Limit'])
                                        .otherwise(pl.col('PerClmSubj')-row['Per Claim Retention'])
                                        .alias('Layer Loss'))
                            .with_columns(pl.col('Layer Loss').alias('Layer Loss and ALAE'))
                            .with_columns([pl.col('Layer Loss').cast(pl.Float64),
                                pl.col('Layer Loss and ALAE').cast(pl.Float64)])
                            .drop(['Subject Loss','Subject Loss and ALAE','PerClmSubj'])
                            .rename({'Layer Loss':'Subject Loss','Layer Loss and ALAE':'Subject Loss and ALAE'}))
                elif row['Per Claim ALAE Handling']=='Included':
                    temp=(temp
                            .filter(pl.col('Event ID')!=0)      #Exclude attritional risk sources from per claim calculations                        
                            .with_columns((pl.col("Subject Loss and ALAE")).alias("PerClmSubj"))
                            .filter(pl.col('PerClmSubj')>row['Per Claim Retention'])
                            .with_columns(pl.when((pl.col('PerClmSubj')-row['Per Claim Retention'])>row['Per Claim Limit'])
                                        .then(row['Per Claim Limit'])
                                        .otherwise(pl.col('PerClmSubj')-row['Per Claim Retention'])
                                        .alias('Layer Loss and ALAE'))
                            .with_columns((pl.col('Layer Loss and ALAE')*(pl.col('Subject Loss')/pl.col('Subject Loss and ALAE'))).alias('Layer Loss'))
                            .with_columns([pl.col('Layer Loss').cast(pl.Float64),
                                pl.col('Layer Loss and ALAE').cast(pl.Float64)])
                            .drop(['Subject Loss','Subject Loss and ALAE','PerClmSubj'])
                            .rename({'Layer Loss':'Subject Loss','Layer Loss and ALAE':'Subject Loss and ALAE'}))                        
            
            if row['HasPerEvent']==True:
                if row['Per Event ALAE Handling']=="Pro Rata":
                    temp=(temp
                            .with_columns((pl.col("Subject Loss").sum().over(['Simulation','Event ID'])).alias("TotalEvSubj"))
                            .filter(pl.col('TotalEvSubj')>row['Per Event Retention'])
                            .with_columns(pl.when((pl.col('TotalEvSubj')-row['Per Event Retention'])>row['Per Event Limit'])
                                        .then(row['Per Event Limit'])
                                        .otherwise(pl.col('TotalEvSubj')-row['Per Event Retention'])
                                        .alias('TotalEvRecovery'))
                            .with_columns((pl.col('TotalEvRecovery')*(pl.col('Subject Loss')/pl.col('TotalEvSubj'))).alias('Layer Loss'))
                            .with_columns((pl.col('Layer Loss')*(pl.col('Subject Loss and ALAE')/pl.col('Subject Loss'))).alias('Layer Loss and ALAE'))
                            .with_columns([pl.col('Layer Loss').cast(pl.Float64),
                                pl.col('Layer Loss and ALAE').cast(pl.Float64)])
                            .drop(['Subject Loss','Subject Loss and ALAE','TotalEvSubj','TotalEvRecovery'])
                            .rename({'Layer Loss':'Subject Loss','Layer Loss and ALAE':'Subject Loss and ALAE'}))
                elif row['Per Event ALAE Handling']=="Excluded":
                    temp=(temp
                            .with_columns((pl.col("Subject Loss").sum().over(['Simulation','Event ID'])).alias("TotalEvSubj"))                              
                            .filter(pl.col('TotalEvSubj')>row['Per Event Retention'])
                            .with_columns(pl.when((pl.col('TotalEvSubj')-row['Per Event Retention'])>row['Per Event Limit'])
                                        .then(row['Per Event Limit'])
                                        .otherwise(pl.col('TotalEvSubj')-row['Per Event Retention'])
                                        .alias('TotalEvRecovery'))
                            .with_columns((pl.col('TotalEvRecovery')*(pl.col('Subject Loss')/pl.col('TotalEvSubj'))).alias('Layer Loss'))
                            .with_columns(pl.col('Layer Loss').alias('Layer Loss and ALAE'))
                            .with_columns([pl.col('Layer Loss').cast(pl.Float64),
                                pl.col('Layer Loss and ALAE').cast(pl.Float64)])
                            .drop(['Subject Loss','Subject Loss and ALAE','TotalEvSubj','TotalEvRecovery'])
                            .rename({'Layer Loss':'Subject Loss','Layer Loss and ALAE':'Subject Loss and ALAE'}))
                elif row['Per Event ALAE Handling']=='Included':
                    temp=(temp
                            .with_columns((pl.col("Subject Loss and ALAE").sum().over(['Simulation','Event ID'])).alias("TotalEvSubj"))                              
                            .filter(pl.col('TotalEvSubj')>row['Per Event Retention'])
                            .with_columns(pl.when((pl.col('TotalEvSubj')-row['Per Event Retention'])>row['Per Event Limit'])
                                        .then(row['Per Event Limit'])
                                        .otherwise(pl.col('TotalEvSubj')-row['Per Event Retention'])
                                        .alias('TotalEvRecovery'))
                            .with_columns((pl.col('TotalEvRecovery')*(pl.col('Subject Loss and ALAE')/pl.col('TotalEvSubj'))).alias('Layer Loss and ALAE'))
                            .with_columns((pl.col('Layer Loss and ALAE')*(pl.col('Subject Loss')/pl.col('Subject Loss and ALAE'))).alias('Layer Loss'))
                            .with_columns([pl.col('Layer Loss').cast(pl.Float64),
                                pl.col('Layer Loss and ALAE').cast(pl.Float64)])
                            .drop(['Subject Loss','Subject Loss and ALAE','TotalEvSubj','TotalEvRecovery'])
                            .rename({'Layer Loss':'Subject Loss','Layer Loss and ALAE':'Subject Loss and ALAE'}))

            if row['HasAgg']==True: 
                if row['Aggregate ALAE Handling']=="Pro Rata":
                    temp=(temp
                            .with_columns((pl.col("Subject Loss").sum().over(['Simulation'])).alias("TotalAggSubj"))
                            .filter(pl.col('TotalAggSubj')>row['Aggregate Retention'])
                            .with_columns(pl.when((pl.col('TotalAggSubj')-row['Aggregate Retention'])>row['Aggregate Limit'])
                                        .then(row['Aggregate Limit'])
                                        .otherwise(pl.col('TotalAggSubj')-row['Aggregate Retention'])
                                        .alias('TotalAggRecovery'))
                            .with_columns((pl.col('TotalAggRecovery')*(pl.col('Subject Loss')/pl.col('TotalAggSubj'))).alias('Layer Loss'))
                            .with_columns((pl.col('Layer Loss')*(pl.col('Subject Loss and ALAE')/pl.col('Subject Loss'))).alias('Layer Loss and ALAE'))
                            .with_columns([pl.col('Layer Loss').cast(pl.Float64),
                                pl.col('Layer Loss and ALAE').cast(pl.Float64)])
                            .drop(['Subject Loss','Subject Loss and ALAE','TotalAggSubj','TotalAggRecovery']))
                elif row['Aggregate ALAE Handling']=="Excluded":
                    temp=(temp
                            .with_columns((pl.col("Subject Loss").sum().over(['Simulation'])).alias("TotalAggSubj"))
                            .filter(pl.col('TotalAggSubj')>row['Aggregate Retention'])
                            .with_columns(pl.when((pl.col('TotalAggSubj')-row['Aggregate Retention'])>row['Aggregate Limit'])
                                        .then(row['Aggregate Limit'])
                                        .otherwise(pl.col('TotalAggSubj')-row['Aggregate Retention'])
                                        .alias('TotalAggRecovery'))
                            .with_columns((pl.col('TotalAggRecovery')*(pl.col('Subject Loss')/pl.col('TotalAggSubj'))).alias('Layer Loss'))
                            .with_columns(pl.col('Layer Loss').alias('Layer Loss and ALAE'))
                            .with_columns([pl.col('Layer Loss').cast(pl.Float64),
                                pl.col('Layer Loss and ALAE').cast(pl.Float64)])
                            .drop(['Subject Loss','Subject Loss and ALAE','TotalAggSubj','TotalAggRecovery']))
                elif row['Aggregate ALAE Handling']=='Included':
                    temp=(temp
                            .with_columns((pl.col("Subject Loss and ALAE").sum().over(['Simulation'])).alias("TotalAggSubj"))
                        .filter(pl.col('TotalAggSubj')>row['Aggregate Retention'])
                            .with_columns(pl.when((pl.col('TotalAggSubj')-row['Aggregate Retention'])>row['Aggregate Limit'])
                                        .then(row['Aggregate Limit'])
                                        .otherwise(pl.col('TotalAggSubj')-row['Aggregate Retention'])
                                        .alias('TotalAggRecovery'))
                            .with_columns((pl.col('TotalAggRecovery')*(pl.col('Subject Loss and ALAE')/pl.col('TotalAggSubj'))).alias('Layer Loss and ALAE'))
                            .with_columns((pl.col('Layer Loss and ALAE')*(pl.col('Subject Loss')/pl.col('Subject Loss and ALAE'))).alias('Layer Loss'))
                            .with_columns([pl.col('Layer Loss').cast(pl.Float64),
                                pl.col('Layer Loss and ALAE').cast(pl.Float64)])
                            .drop(['Subject Loss','Subject Loss and ALAE','TotalAggSubj','TotalAggRecovery']))
            else:
                temp=temp.rename({'Subject Loss':'Layer Loss','Subject Loss and ALAE':'Layer Loss and ALAE'})  

            dfResults=pl.concat([dfResults,temp
                            .with_columns(pl.lit(row['Layer']).alias('Layer'))
                            .select(["Layer","Simulation","Event ID","Risk Source ID","Peril","Loss #","Layer Loss","Layer Loss and ALAE"])])
    
    if dfResults.shape[0]>0:
        dfResults=(dfResults
                        .filter(pl.col('Layer').is_in(missingcontracts))
                        .drop('Loss #','Event ID')
                        .group_by(['Layer','Simulation','Risk Source ID','Peril']).sum())

        contractsWithLoss=dfResults.get_column('Layer').unique().to_list()
    else: 
        dfResults=(pl.DataFrame(schema={'Layer':pl.Utf8,
                                        'Simulation': pl.Int64,
                                        'Layer Loss':pl.Float64,
                                        'Layer Loss and ALAE':pl.Float64,
                                        'Risk Source ID':pl.Int64,
                                        'Peril':pl.Utf8}))
        
        contractsWithLoss=[]

    #Add blank rows for simulation 0 for contracts with no loss (to use in ceded uw detail calculations for sims with no loss) 
    contractsNoLoss=[x for x in missingcontracts if x not in contractsWithLoss]
    if len(contractsNoLoss)>0:
        dfResults=(pl.concat([dfResults,
                            pl.DataFrame({'Layer':contractsNoLoss})
                            .with_columns([pl.lit(-1).cast(pl.Int64).alias('Simulation'),
                                            pl.col('Layer').map_elements(lambda x:(specs['Contract Layers'].filter(pl.col('Layer')==x).get_column('Risk Source IDs')[0][0]),return_dtype=pl.Int64).alias('Risk Source ID'),
                                            pl.lit('').alias('Peril'),                                            
                                            pl.lit(0.0).alias('Layer Loss'),
                                            pl.lit(0.0).alias('Layer Loss and ALAE'),])],how='vertical')
                            .with_columns(pl.when(pl.col('Risk Source ID')==0)
                                          .then(pl.lit(None))
                                          .otherwise(pl.col('Risk Source ID'))
                                          .alias('Risk Source ID')))
    #endregion

    #Return results and save, if necessary
    #region  
    if specs['savecededloss'].upper()=='YES':
        #if file exists, append missing. Otherwise, create file.
        if os.path.exists(f'{specs["dataparquetpath"]}/{scenario} - {infotype}.parquet'):
            _misc.saveToParquet(pl.concat([dfResults,getResults(specs,infotype,scenario)]),
                                f'{specs["dataparquetpath"]}/{scenario} - {infotype}.parquet')
            return getResults(specs,infotype,scenario,df=missingAndRequired[1])
        else:
            _misc.saveToParquet(dfResults,f'{specs["dataparquetpath"]}/{scenario} - {infotype}.parquet')
            return dfResults
    else:
        return dfResults
    #endregion

def cededLayerUWResult(specs,scenario,strategies=None,treaties=None,df=None):   
    #region
    infotype='ceded uw'
    if not isinstance(df,pl.DataFrame):
        missingAndRequired=missingAndRequiredCombinations(specs,scenario,infotype,strategies=strategies,treaties=treaties)
    else:
        missingAndRequired=missingAndRequiredCombinations(specs,scenario,infotype,df=df)          

    missing=missingAndRequired[0]

    if missing.shape[0] + missingAndRequired[1].shape[0]==0:
       return 'No ceded uw results to calculate'
    elif missing.shape[0]==0:
        #get from file
        return getResults(specs,infotype,scenario,missingAndRequired[1])
    #endregion    
    
    #set up filters
    #region
    specsTreaties=(specs['Treaty Terms']
                   .join(missingAndRequired[1].with_columns(pl.lit('Required').alias('Required')),how='left',left_on=['Strategy','Treaty Loss Layer'],right_on=['Strategy','Treaty'])
                   .filter(pl.col('Required')=='Required'))
    #endregion

    #region
    dfCededLoss=cededLayerLosses(specs,scenario,layers=specsTreaties.get_column('Layer Lookup').unique().to_list())
    #check for treaties with no loss (will be handled at end)
    nolossinfo=(specs['Treaty Terms']
            .select(['Strategy','Treaty Loss Layer','Layer Lookup'])
            .filter(pl.col('Layer Lookup').is_in(dfCededLoss
                                                .filter(pl.col('Simulation')==-1)
                                                .get_column('Layer')
                                                .unique().to_list()))
            .rename({'Layer Lookup':'Layer','Treaty Loss Layer':'Treaty'}))


    #Summarize ceded losses by layer and add 0 row for each treaty (labeled with Simulation 0). Used to derive 0 loss premium and pc calcs
    dfCededLoss=(pl.concat([dfCededLoss
                            .drop(['Risk Source ID','Peril','Layer Loss'])
                            .group_by(['Layer','Simulation'])
                            .sum(),
                            (pl.DataFrame({'Layer':dfCededLoss.get_column('Layer').unique().to_list()})
                            .with_columns([pl.lit(0).cast(pl.Int64).alias('Simulation'),
                                            pl.lit(0.0).alias('Layer Loss and ALAE')]))],how='diagonal')
                            .filter(pl.col('Simulation')>=0))
    
    if dfCededLoss.shape[0]>0:
        dfResults=pl.DataFrame()
        for row in specsTreaties.rows(named=True):
        # assumes if have swing rated, don't have reinstmts  
            if row['Risk Transfer Discount Factors'] is None:
                premdisct,premadjdisct,lossdisct,expdisct,expadjdisct=1.0,1.0,1.0,1.0,1.0
            else:
                temp=specs['Risk Transfer Discount Factors'].filter(pl.col('Name')==row['Risk Transfer Discount Factors'])
                if temp.shape[0]>0:
                    temp=list(temp.drop('Name').row(0))
                    premdisct,premadjdisct,lossdisct,expdisct,expadjdisct=temp[0],temp[1],temp[2],temp[3],temp[4]
                else:
                    premdisct,premadjdisct,lossdisct,expdisct,expadjdisct=1.0,1.0,1.0,1.0,1.0
        
            _tempCededLoss=(dfCededLoss
                            .filter(pl.col('Layer')==row['Layer Lookup'])
                            .rename({"Layer Loss and ALAE": "Ceded Loss and ALAE" })
                            .with_columns([pl.lit(row["Premium"])
                                        .cast(pl.Float64)
                                        .alias("Ceded Base Premium"),
                                        (pl.col("Ceded Loss and ALAE")
                                        / specs["dict_ReinstmtLimit"][row["Treaty Loss Layer"]])
                                        .alias("NumLimits")])
                            )

            if not((row['Swing Minimum Premium'] is not None) & (row['Swing Maximum Premium'] is not None) & (row['Swing Loss Load'] is not None)) and (row['Reinstatements'] is not None):
                _tempCededLoss=(_tempCededLoss
                                .with_columns(_misc.clip(pl.col("NumLimits"),
                                                        1,
                                                        specs["dict_lastReinstmt"][row["Reinstatements"]])
                                                .cast(pl.Int64)
                                                .alias("LookupReinstmt"))
                                .with_columns(pl.lit(row['Reinstatements']).alias("Reinstatements"))
                                .join(
                                    specs["Reinstatement Terms"],
                                    how="left",
                                    left_on=["Reinstatements", "LookupReinstmt"],
                                    right_on=["Name", "Reinstatement Number"],
                                ))

                _tempCededLoss=(_tempCededLoss
                                .with_columns(
                                    pl.when(pl.col("NumLimits") < 1)
                                    .then(pl.col("Ceded Base Premium") * pl.col("NumLimits") * pl.col("Cost"))
                                    .otherwise(pl.col("Ceded Base Premium")*
                                                (pl.col("Cumulative Cost")
                                                + ((pl.col("NumLimits") - pl.col("LookupReinstmt"))
                                                * pl.col("Next Reinstatement Cost"))))
                                    .alias("Ceded Premium Adjustment"))
                                .with_columns(pl.col('Ceded Premium Adjustment').fill_null(0))
                                .drop(["Reinstatements","Cost", "Cumulative Cost","Next Reinstatement Cost",
                                        "NumLimits","LookupReinstmt"])                                
                                )
            elif not((row['Swing Minimum Premium'] is not None) & (row['Swing Maximum Premium'] is not None) & (row['Swing Loss Load'] is not None)) and (row['Reinstatements'] is None):
                _tempCededLoss=(_tempCededLoss
                                .with_columns(pl.lit(0).alias("Ceded Premium Adjustment")))
            elif (row['Swing Minimum Premium'] is not None) & (row['Swing Maximum Premium'] is not None) & (row['Swing Loss Load'] is not None):
                _tempCededLoss=(_tempCededLoss
                                .with_columns((_misc.clip((pl.col("Ceded Loss and ALAE")
                                                    * row["Swing Loss Load"])
                                            + row["Swing Minimum Premium"],
                                            0,
                                            row["Swing Maximum Premium"]) - pl.col("Ceded Base Premium"))
                                            .alias('Ceded Premium Adjustment')))
                
            _tempCededLoss=(_tempCededLoss
                                .with_columns(
                                    ((pl.col("Ceded Base Premium"))
                                    * row["Provisional or Fixed Ceding Commission"])
                                    .alias("Provisional Ceding Commission"))
                                .with_columns(
                                    ((pl.col("Ceded Premium Adjustment"))
                                    * row["Provisional or Fixed Ceding Commission"])
                                    .cast(pl.Float64)
                                    .alias("Ceding Commission Adjustment")))
            
            if row['Ceding Commission Terms'] is not None:
                temp=specs["Sliding Scale CC Terms"].filter(pl.col('Name')==row['Ceding Commission Terms'])
                if temp.shape[0]>0:
                    maxcc=temp.get_column('max')[0]
                    _tempCededLoss=(_tempCededLoss
                                .with_columns([pl.lit(row["Ceding Commission Terms"]).alias("Ceding Commission Terms"),
                                            pl.lit(maxcc).alias("maxCC"),
                                            pl.when(pl.col("Ceded Base Premium") + pl.col("Ceded Premium Adjustment") > 0)
                                                .then((pl.col("Ceded Loss and ALAE")/(pl.col("Ceded Base Premium")) + pl.col("Ceded Premium Adjustment")))
                                                .otherwise(pl.lit(0))
                                                .alias("LR")])
                                .sort("LR", nulls_last=True)
                                .join_asof((temp
                                            .select(["Name",
                                                "Ceding Commission",
                                                "Loss Ratio",
                                                "Slope"])),
                                    by="Ceding Commission Terms",
                                    left_on="LR",
                                    right_on="Loss Ratio",
                                    strategy="backward")
                                .with_columns(
                                    pl.when(pl.col("Slope").is_null())
                                    .then((pl.col("maxCC")
                                            * (pl.col("Ceded Base Premium") + pl.col("Ceded Premium Adjustment")))
                                        - pl.col("Provisional Ceding Commission"))
                                    .otherwise((((pl.col("Ceding Commission")
                                                + (pl.col("Slope")
                                                    * (pl.col("LR") - pl.col("Loss Ratio"))))
                                                * (pl.col("Ceded Base Premium") + pl.col("Ceded Premium Adjustment")))
                                                - pl.col("Provisional Ceding Commission")))
                                    .alias("Ceding Commission Adjustment"))
                                .drop(["LR", "maxCC", "Ceding Commission", "Loss Ratio", "Slope","Ceding Commission Terms","Has Sliding CC"]))
                    
            if row['PC Pct']==0:
                _tempCededLoss=(_tempCededLoss
                                .with_columns(pl.lit(0.0).alias("Profit Commission")))
            else:
                _tempCededLoss=(_tempCededLoss                    
                                .with_columns((row["PC Pct"]
                                    * _misc.clip(((pl.col("Ceded Base Premium") + pl.col("Ceded Premium Adjustment"))
                                    * (1- row["PC RHOE Pct"]))
                                    - pl.col("Ceded Loss and ALAE")
                                    - pl.col("Provisional Ceding Commission")
                                    - pl.col("Ceding Commission Adjustment"),
                                    0,
                                    infiniteloss))
                                .alias("Profit Commission")))

            #Calculate Reinsurer Deficit
            _tempCededLoss=(_tempCededLoss
                .with_columns((pl.col("Ceded Base Premium")*premdisct
                        + pl.col("Ceded Premium Adjustment")*premadjdisct
                        - pl.col("Ceded Loss and ALAE")*lossdisct
                        - pl.col("Provisional Ceding Commission")*expdisct
                        - pl.col("Ceding Commission Adjustment")*expadjdisct
                        - pl.col("Profit Commission")*expadjdisct)
                        .alias("Reinsurer Deficit"))
                .with_columns(
                    pl.when(pl.col("Reinsurer Deficit") > 0)
                    .then(pl.lit(0.0))
                    .otherwise(pl.col("Reinsurer Deficit"))
                    .alias("Reinsurer Deficit"))
                .with_columns([
                    (pl.when(row['Include in Ceded Losses']=='Exclude')
                    .then(pl.lit(0))
                    .otherwise(pl.col('Ceded Loss and ALAE')*row["Placement Pct"])
                    .alias('Ceded Loss and ALAE')),
                    (pl.col('Ceded Base Premium')*row["Placement Pct"]).alias('Ceded Base Premium'),
                    (pl.col('Ceded Premium Adjustment')*row["Placement Pct"]).alias('Ceded Premium Adjustment'),
                    (pl.col('Provisional Ceding Commission')*row["Placement Pct"]).alias('Provisional Ceding Commission'),
                    (pl.col('Ceding Commission Adjustment')*row["Placement Pct"]).alias('Ceding Commission Adjustment'),
                    (pl.col('Profit Commission')*row["Placement Pct"]).alias('Profit Commission'),
                    (pl.col('Reinsurer Deficit')*row["Placement Pct"]).alias('Reinsurer Deficit'),
                    pl.lit(row['Treaty Loss Layer']).alias('Treaty'),
                    pl.lit(row['Strategy']).alias('Strategy'),
                    pl.lit(row['Layer Lookup']).alias('Layer')]))

            dfResults=pl.concat([dfResults,_tempCededLoss],how='diagonal').select(['Strategy','Treaty','Layer','Simulation',  
                                                                            'Ceded Loss and ALAE','Ceded Base Premium','Ceded Premium Adjustment','Provisional Ceding Commission',
                                                                            'Ceding Commission Adjustment','Profit Commission','Reinsurer Deficit'])
            
        #Add rows for simulation 1 for contracts with no loss
        if nolossinfo.shape[0]>0:
            dfResults=pl.concat([dfResults,
                        (nolossinfo
                        .join(dfResults.filter(pl.col('Simulation')==0),
                        on=['Strategy','Treaty','Layer'],
                        how='left')
                        .with_columns(pl.lit(1).cast(pl.Int64).alias('Simulation')))],
                        how='diagonal')
    else:
        return 'No results to save in cededLayerUWResult'
    #endregion
    
    #Return results and save, if necessary
    #region  
    if specs['savecededuw'].upper()=='YES':
        #if file exists, append missing. Otherwise, create file.
        if os.path.exists(f'{specs["dataparquetpath"]}/{scenario} - {infotype}.parquet'):
            _misc.saveToParquet(pl.concat([dfResults,getResults(specs,infotype,scenario)]),
                                f'{specs["dataparquetpath"]}/{scenario} - {infotype}.parquet')
            return getResults(specs,infotype,scenario,df=missingAndRequired[1])
        else:
            _misc.saveToParquet(dfResults,f'{specs["dataparquetpath"]}/{scenario} - {infotype}.parquet')
            return dfResults
    else:
        return dfResults
    #endregion

def lossAllocation(specs,scenario,df=None):
    #Derive missing and required combinations and return results if no missing strategies/segmentations
    #region
    infotype='loss allocation'
    if df is None:
        missingAndRequired=missingAndRequiredCombinations(specs,scenario,infotype)
    else:
        missingAndRequired=missingAndRequiredCombinations(specs,scenario,infotype,df=df)   

    missing=missingAndRequired[0]

    if missing.shape[0] + missingAndRequired[1].shape[0]==0:
       return 'No loss allocation results to calculate'
    elif missing.shape[0]==0:
        #get from file
        return getResults(specs,infotype,scenario,missingAndRequired[1])
    #endregion  

    #return results if file exists and no missing strategies/segmentations, otherwise derive missing
    layers=missing.get_column('Layer').unique().to_list()
    segmentations=missing.get_column('Segmentation').unique().to_list()
    ceded=cededLayerLosses(specs,scenario,layers=layers).drop(['Layer Loss'])

    if 'None' in segmentations:
        result=(ceded
                    .with_columns([pl.lit('None').alias('Segmentation'),
                                pl.lit('NA').alias('Segment')]))
    else:
        result=pl.DataFrame()

    for row in specs['Segmentations'].filter(pl.col('Segmentation').is_in(segmentations)).filter(pl.col('Segmentation')!='None').rows(named=True):
        result=(pl.concat([result,(ceded
                                .filter(pl.col('Risk Source ID').is_in(row['RSID List']))
                                .with_columns([pl.lit(row['Segmentation']).alias('Segmentation'),
                                                pl.lit(row['Segment']).alias('Segment')])
                                .drop(['Risk Source ID','Peril']))]))

    #Cleanup data for layers with no simulated loss
    if result.filter(pl.col('Simulation')==-1).shape[0]>0:
        result=pl.concat([result
                          .filter(pl.col('Simulation')>=0)
                          .select(['Simulation','Segmentation','Segment','Layer','Layer Loss and ALAE']),
                          (result.filter(pl.col('Simulation')==-1)
                                .select(['Segmentation','Layer'])
                                .join(specs['Contract Layers']
                                    .select(['Layer','Risk Source IDs']),
                                how='left',on='Layer')
                                .rename({'Risk Source IDs':'Layer RSIDs'})
                                .join(specs['Segmentations']
                                    .drop('RSID List String'),
                                how='left',on='Segmentation')
                                .with_columns(pl.struct(['Layer RSIDs','RSID List']).map_elements(lambda x: list(set(x['Layer RSIDs']) & set(x['RSID List'])),return_dtype=pl.List(pl.Int64)).alias('RSID List'))
                                .filter(pl.col('RSID List')!=[])
                                .drop(['Layer RSIDs','RSID List'])
                                .with_columns([pl.lit(-1).cast(pl.Int64).alias('Simulation'),
                                            pl.lit(0.0).alias('Layer Loss and ALAE')])
                                .select(['Simulation','Segmentation','Segment','Layer','Layer Loss and ALAE']))])        

    result=(result
            .group_by(['Simulation','Segmentation','Segment','Layer'])
            .sum()
            .with_columns((pl.col('Layer Loss and ALAE')/pl.col('Layer Loss and ALAE').sum().over(['Simulation','Segmentation','Layer'])).alias('Loss Allocation'))
            .with_columns(pl.col('Loss Allocation').fill_nan(0.0).alias('Loss Allocation'))
            .with_columns(pl.when(pl.col('Simulation')==-1)
                           .then(pl.lit(1))
                           .otherwise(pl.col('Simulation'))
                           .alias('Simulation')))
    
    #Return results and save, if necessary
    #region  
    if specs['savelossalloc'].upper()=='YES':
        #if file exists, append missing. Otherwise, create file.
        if os.path.exists(f'{specs["dataparquetpath"]}/{scenario} - {infotype}.parquet'):
            _misc.saveToParquet(pl.concat([result,getResults(specs,infotype,scenario)]),
                                f'{specs["dataparquetpath"]}/{scenario} - {infotype}.parquet')
            return getResults(specs,infotype,scenario,df=missingAndRequired[1])
        else:
            _misc.saveToParquet(result,f'{specs["dataparquetpath"]}/{scenario} - {infotype}.parquet')
            return result
    else:
        return result
    #endregion

def lossAllocationByPeril(specs,scenario,df=None):
    #lossAllocationByPeril is not saved
    ceded=cededLayerLosses(specs,scenario).drop(['Layer Loss'])

    if isinstance(ceded, str):
        return "No Ceded Losses"
    else:
        result=(ceded
                    .with_columns([pl.lit('None').alias('Segmentation'),
                                pl.lit('NA').alias('Segment')]))

        for row in specs['Segmentations'].filter(pl.col('Segmentation')!='None').rows(named=True):
            result=pl.concat([result,(ceded
                                    .filter(pl.col('Risk Source ID').is_in(row['RSID List']))
                                    .with_columns([pl.lit(row['Segmentation']).alias('Segmentation'),
                                                    pl.lit(row['Segment']).alias('Segment')]))])
        result=(result
            .drop(['Risk Source ID'])
            .group_by(['Simulation','Segmentation','Segment','Layer','Peril'])
            .sum()
            .with_columns((pl.col('Layer Loss and ALAE')/pl.col('Layer Loss and ALAE').sum().over(['Simulation','Segmentation','Layer'])).alias('Loss Allocation'))
            .with_columns(pl.when(pl.col('Simulation')==-1)
                          .then(pl.lit(1))
                          .otherwise(pl.col('Simulation'))
                          .alias('Simulation')))
        
        return result

def premiumAllocation(specs,scenario,strategies=None,segmentations=None,df=None):
    #Derive missing and required combinations and return results if no missing strategies/segmentations
    #region
    infotype='premium allocation'
    if not isinstance(df,pl.DataFrame):
        missingAndRequired=missingAndRequiredCombinations(specs,scenario,infotype,strategies=strategies,segmentations=segmentations)
    else:
        missingAndRequired=missingAndRequiredCombinations(specs,scenario,infotype,df=df)   

    missing=missingAndRequired[0]

    if missing.shape[0] + missingAndRequired[1].shape[0]==0:
       return 'No premium allocation results to calculate'
    elif missing.shape[0]==0:
        #get from file
        return getResults(specs,infotype,scenario,missingAndRequired[1])
    #endregion  

    #calculate premium allocation
    #region
    specsTreaties=(specs['Treaty Terms']
                   .join(missingAndRequired[0]
                         .select(['Treaty','Strategy'])
                         .unique()
                         .with_columns(pl.lit('Required').alias('Required')),
                    how='left',left_on=['Strategy','Treaty Loss Layer'],right_on=['Strategy','Treaty'])
                   .filter(pl.col('Required')=='Required')
                   .drop('Required'))

    lossalloc=(lossAllocation(specs,scenario,df=missing.join(specsTreaties
                                                            .select(['Strategy','Treaty Loss Layer','Layer Lookup']),
                                                        how='left',left_on=['Strategy','Treaty'],right_on=['Strategy','Treaty Loss Layer'])
                                                        .select(['Segmentation','Layer Lookup'])
                                                        .unique()
                                                        .rename({'Layer Lookup':'Layer'}))
                    .drop(['Loss Allocation'])
                    .group_by(['Segmentation','Segment','Layer'])
                    .agg(pl.col('Layer Loss and ALAE'),
                        pl.col('Simulation').min().alias('Simulation Min'))
                    .with_columns(pl.col('Layer Loss and ALAE').map_elements(lambda x:(specs['maxsims']-len(x)),return_dtype=pl.Int64).alias('newsimscount'))
                    .with_columns(pl.col('newsimscount').map_elements(lambda x: x*[0.0],return_dtype=pl.List(pl.Float64)).alias('newlosses'))
                    .with_columns(pl.concat_list(pl.col('Layer Loss and ALAE'),pl.col('newlosses')).alias('Layer Loss and ALAE'))
                    .drop(['newsimscount','newlosses'])
                    .with_columns(pl.col('Layer Loss and ALAE').map_elements(lambda x: x.mean(),return_dtype=pl.Float64).alias('Expected Loss'))
                    .with_columns(pl.col('Layer Loss and ALAE').map_elements(lambda x: x.std(),return_dtype=pl.Float64).alias('Standard Deviation'))
                    .drop(['Layer Loss and ALAE']))                                                        
    
    result=(specsTreaties
        .select(['Strategy','Treaty Loss Layer','Layer Lookup','Premium Allocation Assumptions'])
        .join(specs['Contract Layers']
        .select(['Layer','Risk Source IDs'])
        .join(specs['Segmentations']
            .filter(pl.col('Segmentation')!='None')
            .drop('RSID List String'),
        how='cross')
        .with_columns(pl.struct(['Risk Source IDs','RSID List'])
                    .map_elements(lambda x: list(set(x['Risk Source IDs']) & set(x['RSID List'])),return_dtype=pl.List(pl.Int64))
                    .alias('RSID List'))
        .filter(pl.col('RSID List')!=[])
        .drop(['Risk Source IDs','RSID List']),
        how='left',left_on='Layer Lookup',right_on='Layer')
        .join(lossalloc,
            how='left',left_on=['Layer Lookup','Segmentation','Segment'],right_on=['Layer','Segmentation','Segment'])
        .join(specs['Premium Allocation Assumptions'],how='left',left_on='Premium Allocation Assumptions',right_on='Name')
        .join(specs['Fixed Premium Allocation Assumptions'],how='left',left_on=['Table Name for Fixed Allocation','Segmentation','Segment'],right_on=['Name','Segmentation','Segment'])
        .with_columns(pl.col('Std Dev Multiple').fill_null(0.0))
        .join(specs['Segmentations']
            .filter(pl.col('Segmentation')!='None')
            .drop('RSID List String')
            .explode('RSID List')
            .join(specs['Gross Premium and Expense']
                    .filter(pl.col('Scenario')==scenario)
                    .select(['Risk Source ID','Premium']),
                    how='left',left_on='RSID List',right_on='Risk Source ID')
            .with_columns(pl.col('Premium').fill_null(0.0))
            .group_by(['Segmentation','Segment'])
            .agg(pl.col('Premium').sum().alias('Gross Premium')),
            how='left',on=['Segmentation','Segment'])
            .with_columns([(pl.col('Expected Loss')+pl.col('Std Dev Multiple')*pl.col('Standard Deviation')).alias('Method 1'),
                        (pl.col('Gross Premium')/pl.col('Gross Premium').sum().over(['Strategy','Treaty Loss Layer','Segmentation'])).alias('Method 2'),
                        (pl.col('Allocation Percent')/pl.col('Allocation Percent').sum().over(['Strategy','Treaty Loss Layer','Segmentation'])).alias('Method 3')])
            .with_columns((pl.col('Method 1')/pl.col('Method 1').sum().over(['Strategy','Treaty Loss Layer','Segmentation'])).alias('Method 1'))
            #if no loss, method 1 is null, use gross premium for method 1
            .with_columns(pl.when((pl.col('Method 1').is_nan()) | (pl.col('Method 1').is_null())) 
                                .then(pl.lit('Gross Premium'))
                                .otherwise(pl.lit('Std Dev'))
                                .alias('Method 1 Type'))            
            .with_columns(pl.when((pl.col('Method 1').is_nan()) | (pl.col('Method 1').is_null())) 
                                .then(pl.col('Method 3'))
                                .otherwise(pl.col('Method 1')).alias('Method 1'))
            #if no table allocation, method 2 is null, use method 1
            .with_columns(pl.when((pl.col('Method 2').is_nan()) | (pl.col('Method 2').is_null())) 
                                .then(pl.col('Method 1 Type'))
                                .otherwise(pl.lit('Table'))
                                .alias('Method 2 Type'))            
            .with_columns(pl.when((pl.col('Method 2').is_nan()) | (pl.col('Method 2').is_null())) 
                                .then(pl.col('Method 1'))
                                .otherwise(pl.col('Method 2')).alias('Method 2'))                        
            .with_columns(pl.when(pl.col('Primary Method')=='Std Dev Multiple')
                            .then(pl.col('Method 1 Type'))
                            .otherwise(pl.col('Method 2 Type'))
                            .alias('Premium Allocation Method'))
            .with_columns(pl.when(pl.col('Primary Method')=='Std Dev Multiple')
                            .then(pl.col('Method 1'))
                            .otherwise(pl.col('Method 2'))
                            .alias('Premium Allocation Percent'))
            .rename({'Treaty Loss Layer':'Treaty'})
            .select(['Strategy','Treaty','Segmentation','Segment','Expected Loss','Standard Deviation','Table Name for Fixed Allocation','Std Dev Multiple','Premium Allocation Method','Premium Allocation Percent']))
                
    #Return results and save, if necessary
    #region  
    if infotype not in specs.keys():
        specs[infotype]={}

    if specs['savepremalloc'].upper()=='YES':
        #if file exists, append missing. Otherwise, create file.
        if os.path.exists(f'{specs["dataparquetpath"]}/{scenario} - {infotype}.parquet'):
            _misc.saveToParquet(pl.concat([result,getResults(specs,infotype,scenario)]),
                                f'{specs["dataparquetpath"]}/{scenario} - {infotype}.parquet')
            result=getResults(specs,infotype,scenario,df=missingAndRequired[1])
            specs[infotype].update({scenario:result})
            return result
        else:
            _misc.saveToParquet(result,f'{specs["dataparquetpath"]}/{scenario} - {infotype}.parquet')
            specs[infotype].update({scenario:result})
            return result
    else:
        specs[infotype].update({scenario:result})
        return result
    #endregion    

def allPremiumAllocations(specs):
    if 'premium allocation' not in specs.keys():
        specs['premium allocation']={}

    result=pl.DataFrame()
    for scenario in specs['Scenarios'].get_column('Scenario').unique().to_list():
        if scenario not in specs['premium allocation'].keys():
            premiumAllocation(specs,scenario)
        result=pl.concat([result,specs['premium allocation'][scenario].with_columns(pl.lit(scenario).alias('Scenario'))])    
    return result.select(['Scenario','Strategy','Treaty','Segmentation','Segment','Expected Loss',
                          'Standard Deviation','Table Name for Fixed Allocation','Std Dev Multiple',
                          'Premium Allocation Method','Premium Allocation Percent'])

def cededLayerUWResultBySegmentation(specs,scenario,strategies=None,segmentations='All',treaties=None,df=None):
    #Derive missing and required combinations and return results if no missing strategies/segmentations
    #region
    infotype='ceded uw by segmentation'
    if not isinstance(df,pl.DataFrame):
        missingAndRequired=missingAndRequiredCombinations(specs,scenario,infotype,strategies=strategies,segmentations=segmentations,treaties=treaties)
    else:
        missingAndRequired=missingAndRequiredCombinations(specs,scenario,infotype,df=df)   

    missing=missingAndRequired[0]

    if missing.shape[0] + missingAndRequired[1].shape[0]==0:
       return 'No ceded uw by segmentation results to calculate'
    elif missing.shape[0]==0:
        #get from file
        return getResults(specs,infotype,scenario,missingAndRequired[1])
    #endregion  

    cededuw=cededLayerUWResult(specs,scenario,df=missingAndRequired[1])
    result=pl.DataFrame()
    lossalloc=lossAllocation(specs,scenario,missing.join(specs['Treaty Terms'].select(['Strategy','Treaty Loss Layer','Layer Lookup']),how='left',left_on=['Strategy','Treaty'],right_on=['Strategy','Treaty Loss Layer']).select(['Segmentation','Layer Lookup']).unique().rename({'Layer Lookup':'Layer'}))
    premallocdict=(dict((premiumAllocation(specs,scenario,df=missingAndRequired[1])
                            .select(['Strategy','Treaty','Segmentation','Segment','Premium Allocation Percent'])
                            .with_columns((pl.col('Strategy')+'|'+pl.col('Treaty')+'|'+pl.col('Segmentation')+'|'+pl.col('Segment')).alias('Key'))
                            .drop(['Strategy','Treaty','Segmentation','Segment'])
                            .select(['Key','Premium Allocation Percent'])).iter_rows()))
        
    for row in specs['Segmentations'].filter(pl.col('Segmentation')!='None').rows(named=True):
        result=pl.concat([result,
                                cededuw
                                .filter(pl.col('Layer').is_in(lossalloc.filter(pl.col('Segment')==row['Segment']).filter(pl.col('Segmentation')==row['Segmentation']).get_column('Layer').unique().to_list()))
                                .join(lossalloc.filter(pl.col('Segment')==row['Segment']).filter(pl.col('Segmentation')==row['Segmentation']),
                                how='left',on=['Simulation','Layer'])
                                .filter(pl.col('Segmentation').is_not_null())])
        

    result=(pl.concat([(result
                            .with_columns((pl.col('Ceded Loss and ALAE')*pl.col('Loss Allocation')).alias('Ceded Loss and ALAE'))
                            .drop(['Layer Loss and ALAE','Loss Allocation','Reinsurer Deficit'])
                            .filter(pl.col('Simulation')>0)),
                            #Add ceded uw for contracts with no loss
                            (cededuw
                            .filter(pl.col('Simulation')==0)
                            .join(result.select(['Strategy','Treaty','Layer','Segmentation','Segment']).unique(),
                            how='left',on=['Strategy','Treaty','Layer']))],how='diagonal')
                            .with_columns(pl.concat_str(pl.col('Strategy'),
                                            pl.lit('|'),
                                            pl.col('Treaty'),
                                            pl.lit('|'),
                                            pl.col('Segmentation'),
                                            pl.lit('|'),
                                            pl.col('Segment')).alias('StrategyTreatySegSeg'))
                            .with_columns(pl.col('StrategyTreatySegSeg').replace_strict(premallocdict,default=1.0).alias('Premium Allocation Percent'))
                            .with_columns(pl.col('Ceded Base Premium','Ceded Premium Adjustment','Provisional Ceding Commission',
                                            'Ceding Commission Adjustment','Profit Commission')*pl.col('Premium Allocation Percent'))
                            .drop(['StrategyTreatySegSeg','Premium Allocation Percent','Reinsurer Deficit']))    

    #Return results and save, if necessary
    #region  
    if specs['savecededuw'].upper()=='YES':
        #if file exists, append missing. Otherwise, create file.
        if os.path.exists(f'{specs["dataparquetpath"]}/{scenario} - {infotype}.parquet'):
            _misc.saveToParquet(pl.concat([result,getResults(specs,infotype,scenario)]),
                                f'{specs["dataparquetpath"]}/{scenario} - {infotype}.parquet')
            return getResults(specs,infotype,scenario,df=missingAndRequired[1])
        else:
            _misc.saveToParquet(result,f'{specs["dataparquetpath"]}/{scenario} - {infotype}.parquet')
            return result
    else:
        return result
    #endregion    

def strategyCededDetails(specs,scenario,strategies=None,segmentations='All',treaties=None,df=None,forReporting=False):
    #Unnecessary to save some simple calculations. Remove columns before saving and add before reporting.
    def cleanupBeforeReporting(df):
        return (df
                .with_columns([(pl.col('Ceded Base Premium')+pl.col('Ceded Premium Adjustment')-pl.col('Provisional Ceding Commission')-pl.col('Ceding Commission Adjustment')-pl.col('Profit Commission')-pl.col('Ceded Loss and ALAE')).alias('Ceded Underwriting Profit')])
                        .with_columns([(pl.col('Provisional Ceding Commission')+pl.col('Ceding Commission Adjustment')+pl.col('Profit Commission')).alias('Ceded Expense'),
                        (pl.col('Ceded Base Premium')+pl.col('Ceded Premium Adjustment')).alias('Ceded Premium')])
                .with_columns([pl.when(pl.col('Ceded Premium')!=0)
                        .then(pl.col('Ceded Loss and ALAE')/pl.col('Ceded Premium'))
                        .otherwise(pl.lit(0))
                        .alias('Ceded Loss and ALAE Ratio')])
                .with_columns([pl.when(pl.col('Ceded Premium')!=0)
                        .then(1-(pl.col('Ceded Underwriting Profit')/pl.col('Ceded Premium')))
                        .otherwise(pl.lit(0))
                        .alias('Ceded Combined Ratio')]))   

    #Derive missing and required combinations and return results if no missing strategies/segmentations
    #region
    infotype='ceded uw details by layer'
    if not isinstance(df,pl.DataFrame):
        missingAndRequired=missingAndRequiredCombinations(specs,scenario,infotype,strategies=strategies,segmentations=segmentations,treaties=treaties)
    else:
        missingAndRequired=missingAndRequiredCombinations(specs,scenario,infotype,df=df)   

    missing=missingAndRequired[0]

    if missing.shape[0] + missingAndRequired[1].shape[0]==0:
       return 'No ceded uw details by layer results to calculate'
    elif missing.shape[0]==0:
        #get from file
        if forReporting:
            return cleanupBeforeReporting(getResults(specs,infotype,scenario,missingAndRequired[1]))
        else:
            return getResults(specs,infotype,scenario,missingAndRequired[1])
    #endregion  

    #Calculations
    #region 
    missingceded=missing.filter(pl.col('Segmentation')=='None').drop(['Segmentation']).unique()

    if missingceded.shape[0]>0:
        ceded=cededLayerUWResult(specs,scenario,df=missingceded).with_columns([pl.lit('None').alias('Segmentation'),pl.lit('NA').alias('Segment')])
    else:
        ceded=pl.DataFrame()

    missingcededbyseg=missing.filter(pl.col('Segmentation')!='None')
    if missingcededbyseg.shape[0]>0:
        cededbyseg=cededLayerUWResultBySegmentation(specs,scenario,df=missingcededbyseg).with_columns(pl.lit(0.0).alias('Reinsurer Deficit'))
    else:
        cededbyseg=pl.DataFrame()
                
    allceded=(pl.concat([ceded,cededbyseg],how='diagonal')
              .with_columns(pl.concat_str(pl.col(['Strategy','Treaty','Layer','Segmentation','Segment']),separator='|').alias('IndexString'))
              .drop(['Strategy','Treaty','Layer','Segmentation','Segment'])
              .filter(pl.col('IndexString').is_not_null()))
    
    if allceded.shape[0]>0:
        def missingsims(availablesims,targetsims):
            return list(targetsims-set(availablesims))

        #Split into 25 row chunks to avoid memory issues
        indexlist=pl.DataFrame({'IndexString':allceded.get_column('IndexString').unique().to_list()}).with_columns(pl.lit(1).alias('RowNum')).with_columns(pl.col('RowNum').cum_sum().alias('RowNum')).with_columns((pl.col('RowNum')-1)//25).partition_by('RowNum')
        result=pl.DataFrame()
        for i in indexlist:
            temp=allceded.filter(pl.col('IndexString').is_in(i.get_column('IndexString').to_list()))
            tempresult=(pl.concat([temp.filter(pl.col('Simulation')>0),
                                (temp
                                    .select(['IndexString','Simulation'])
                                    .filter(pl.col('Simulation')>0)
                                    .sort(['IndexString','Simulation'])
                                    .group_by('IndexString')
                                    .agg(pl.col('Simulation').alias('Simulation'))
                                    .with_columns(pl.col('Simulation').map_elements(lambda x: missingsims(x,set(range(1,specs['maxsims']+1))),return_dtype=pl.List(pl.Int64)).alias('MissingSims'))
                                    .drop('Simulation')
                                    .filter(pl.col('MissingSims')!=[])
                                    .explode('MissingSims')
                                    .rename({'MissingSims':'Simulation'})
                                    .join(temp
                                        .filter(pl.col('Simulation')==0)
                                        .drop('Simulation'),
                                        how='left',on='IndexString'))],how='diagonal'))
            result=pl.concat([result,tempresult])

        result=(result
                .with_columns(pl.col('IndexString').str.split_exact('|',n=4))
                .unnest('IndexString')
                .rename({'field_0':'Strategy','field_1':'Treaty','field_2':'Layer','field_3':'Segmentation','field_4':'Segment'}))
    else:
        return 'No results to save in strategyCededDetails'
    
    #Return results and save, if necessary
    #region  
    if specs['savedetail'].upper()=='YES':
        #if file exists, append missing. Otherwise, create file.
        if os.path.exists(f'{specs["dataparquetpath"]}/{scenario} - {infotype}.parquet'):
            _misc.saveToParquet(pl.concat([result,getResults(specs,infotype,scenario)]),
                                f'{specs["dataparquetpath"]}/{scenario} - {infotype}.parquet')
            if forReporting:
                return cleanupBeforeReporting(getResults(specs,infotype,scenario,df=missingAndRequired[1]))
            else:
                return getResults(specs,infotype,scenario,df=missingAndRequired[1])
        else:
            _misc.saveToParquet(result,f'{specs["dataparquetpath"]}/{scenario} - {infotype}.parquet')
            if forReporting:
                return cleanupBeforeReporting(result)
            else:
                return result
    else:
        if forReporting:
            return cleanupBeforeReporting(result)
        else:
            return result
    #endregion   

def createCededLayerCatLosses(specs,scenario,strategies=None,save=True):
    #For a single scenario
    #If LALAE file doesn't exist for scenario, create it
    #Get the required contracts for the list of strategies provided (or all strategies if no strategies provide)
    #Save layer detail if save=True

    #filterContracts function gets list of all required contracts across all levels, for list of base contracts
    def filterContracts(contracts,outputlist=[]):
        outputlist+=contracts
        tempcontracts=specs['Contract Layers'].filter(pl.col('Layer').is_in(contracts))    
        dependencies=[]
        for row in tempcontracts.rows(named=True):
            dependencies+=row['Underlying Layers']
            dependencies+=row['tempInuring']
        dependencies=list(set(dependencies))
        if len(dependencies)>0:
            filterContracts(dependencies,outputlist)
        return outputlist
    
    def requiredContracts(strategies):
        basecontracts=specs['Treaty Terms'].filter(pl.col('Strategy').is_in(strategies)).get_column('Layer Lookup').unique().to_list()
        result=filterContracts(basecontracts)
    
    if strategies==None:
        _strategies=specs['Treaty Terms'].get_column('Strategy').unique().to_list()
    elif isinstance(strategies,str):
        _strategies=_misc.list_intersection(specs['Treaty Terms'].get_column('Strategy').unique().to_list(),[strategies])
    elif isinstance(strategies,list):
        _strategies=_misc.list_intersection(specs['Treaty Terms'].get_column('Strategy').unique().to_list(),strategies)
    else:
        _strategies=[]

    if len(_strategies)>0:
        #Filter specsTreaties and specsContracts to required rows for selected strategies
        dfLossLayers=(specs['Contract Layers']
                      .filter(pl.col('Layer').is_in(requiredContracts(_strategies)))
                      .drop(["Risk Sources","Deemed or Placed","Subject Risk Source Group"]))
    
    if dfLossLayers.shape[0] > 0:
        #Check if LALAE file exists for scenario
        if isinstance(_misc.getFromParquet(f'{specs["dataparquetpath"]}/{scenario} - LALAE.parquet'),pl.LazyFrame):
            lalae=_misc.getFromParquet(f'{specs["dataparquetpath"]}/{scenario} - LALAE.parquet').filter(pl.col('Simulation')<=specs['maxsims']).filter(pl.col('Peril')!='Non-Cat').collect()
        else:
            lalae=CreateLALAE(specs,scenario).filter(pl.col('Simulation')<=specs['maxsims'])
            _misc.saveToParquet(lalae,f'{specs["dataparquetpath"]}/{scenario} - LALAE.parquet')
            lalae=lalae.filter(pl.col('Peril')!='Non-Cat')
        
        dfResults = pl.DataFrame(
            schema={
                "Layer": pl.Utf8,
                "Simulation": pl.Int64,
                "Event ID": pl.Int64,
                "Risk Source ID": pl.Int64,
                "Peril": pl.Utf8,
                "Loss #": pl.Int64,
                "Layer Loss": pl.Float64,
                "Layer Loss and ALAE": pl.Float64,
            }
        )

        numlevels = dfLossLayers.get_column("Level").max()
        asplacedpcts=specs['Treaty Terms'].filter(pl.col('Layer Lookup').is_in(specs['Contract Layers'].filter(pl.col('Has Inuring As Placed')==True).get_column('Layer').to_list())).select(['Layer Lookup','Placement Pct']).group_by('Layer Lookup').sum()
        asplacedpcts=dict(zip(asplacedpcts.get_column('Layer Lookup').to_list(),asplacedpcts.get_column('Placement Pct').to_list()))        
             
        for i in range(1, numlevels + 1):
            layersThisLevel=(dfLossLayers
                             .filter(pl.col('Level')==i)
                             .with_columns(pl.when(pl.col("Underlying Layers").list.len() == 0)
                                           .then(pl.lit('Gross'))
                                           .otherwise(pl.lit('Underlying'))
                                           .alias('Data Source')))
            
            for row in layersThisLevel.rows(named=True):
                temp=pl.DataFrame()
                
                if row['Data Source']=='Gross':
                    if (row['HasPerClaim']==True)|(row['HasPerEvent']==True):
                        temp=(lalae          
                                    .filter(
                                        (pl.col('Risk Source ID').is_in(row['Risk Source IDs']))
                                        &(pl.col('Event ID')!=0))
                                    .with_columns([pl.col('Loss').cast(pl.Float64),
                                        pl.col('Loss and ALAE').cast(pl.Float64)])
                                    .with_columns(pl.when(row['Subject Perils']==[])
                                                  .then(pl.lit('Keep'))
                                                  .when(pl.col('Peril').is_in(row['Subject Perils']))
                                                  .then(pl.lit('Keep'))
                                                  .otherwise(pl.lit('Drop'))
                                                  .alias('Flag Peril'))
                                    .filter(pl.col('Flag Peril')=='Keep')
                                    .drop('Flag Peril')
                                    .select(['Simulation','Event ID','Risk Source ID','Peril','Loss #','Loss','Loss and ALAE'])
                                    .rename({"Loss": "Subject Loss",
                                        "Loss and ALAE": "Subject Loss and ALAE"}))
                    else:
                        temp=(lalae          
                                    .filter(
                                       pl.col('Risk Source ID').is_in(row['Risk Source IDs']))
                                    .with_columns([pl.col('Loss').cast(pl.Float64),
                                        pl.col('Loss and ALAE').cast(pl.Float64)])
                                    .with_columns(pl.when(row['Subject Perils']==[])
                                                  .then(pl.lit('Keep'))
                                                  .when(pl.col('Peril').is_in(row['Subject Perils']))
                                                  .then(pl.lit('Keep'))
                                                  .otherwise(pl.lit('Drop'))
                                                  .alias('Flag Peril'))
                                    .filter(pl.col('Flag Peril')=='Keep')
                                    .drop('Flag Peril')                                        
                                    .select(['Simulation','Event ID','Risk Source ID','Peril','Loss #','Loss','Loss and ALAE'])
                                    .rename({"Loss": "Subject Loss",
                                        "Loss and ALAE": "Subject Loss and ALAE"}))
                elif row['Data Source']=='Underlying':
                    if (row['HasPerClaim']==True)|(row['HasPerEvent']==True):
                        temp=(dfResults
                                    .filter(
                                       (pl.col('Risk Source ID').is_in(row['Risk Source IDs']))                                           
                                       &(pl.col('Layer').is_in(row['Underlying Layers']))
                                       &(pl.col('Event ID')!=0))
                                    .with_columns(pl.when(row['Subject Perils']==[])
                                                  .then(pl.lit('Keep'))
                                                  .when(pl.col('Peril').is_in(row['Subject Perils']))
                                                  .then(pl.lit('Keep'))
                                                  .otherwise(pl.lit('Drop'))
                                                  .alias('Flag Peril'))
                                    .filter(pl.col('Flag Peril')=='Keep')
                                    .drop('Flag Peril')                                       
                                    .select(["Simulation","Event ID","Risk Source ID",'Peril',"Loss #","Layer Loss","Layer Loss and ALAE"])
                                    .rename({"Layer Loss": "Subject Loss",
                                            "Layer Loss and ALAE": "Subject Loss and ALAE"}))
                    else:
                        temp=(dfResults
                                    .filter(
                                       (pl.col('Risk Source ID').is_in(row['Risk Source IDs']))                                           
                                       &(pl.col('Layer').is_in(row['Underlying Layers'])))
                                    .with_columns(pl.when(row['Subject Perils']==[])
                                                  .then(pl.lit('Keep'))
                                                  .when(pl.col('Peril').is_in(row['Subject Perils']))
                                                  .then(pl.lit('Keep'))
                                                  .otherwise(pl.lit('Drop'))
                                                  .alias('Flag Peril'))
                                    .filter(pl.col('Flag Peril')=='Keep')
                                    .drop('Flag Peril')                                         
                                    .select(["Simulation","Event ID","Risk Source ID",'Peril',"Loss #","Layer Loss","Layer Loss and ALAE"])
                                    .rename({"Layer Loss": "Subject Loss",
                                            "Layer Loss and ALAE": "Subject Loss and ALAE"}))
                        
                    if (len(row['Underlying Layers'])>1) & (row['HasPerClaim']==True):
                        temp=temp.group_by(["Simulation","Event ID","Risk Source ID",'Peril',"Loss #"]).sum()
                        
                if (len(row['Inuring Layers'])>0) & ((row['HasPerClaim']==True)|(row['HasPerEvent']==True)):
                    (temp.extend(dfResults
                                    .filter(
                                       (pl.col('Risk Source ID').is_in(row['Risk Source IDs']))                                           
                                       &(pl.col('Layer').is_in(row['Inuring Layers']))
                                       &(pl.col('Event ID')!=0))
                                    .rename({"Layer Loss": "Subject Loss",
                                            "Layer Loss and ALAE": "Subject Loss and ALAE"})
                                    .with_columns(pl.when(row['Subject Perils']==[])
                                                  .then(pl.lit('Keep'))
                                                  .when(pl.col('Peril').is_in(row['Subject Perils']))
                                                  .then(pl.lit('Keep'))
                                                  .otherwise(pl.lit('Drop'))
                                                  .alias('Flag Peril'))
                                    .filter(pl.col('Flag Peril')=='Keep')
                                    .drop('Flag Peril')                                              
                                    .with_columns(pl.when(row['Has Inuring As Placed']==False)
                                                  .then(pl.lit(1))
                                                  .otherwise(pl.col('Layer').replace_strict(asplacedpcts,default=1))
                                                  .alias('Inuring Pct'))
                                    .with_columns([(pl.col("Subject Loss")
                                            * (-1.0)
                                            * pl.col("Inuring Pct"))
                                            .alias("Subject Loss"),
                                            (pl.col("Subject Loss and ALAE")
                                            * (-1.0)
                                            * pl.col("Inuring Pct"))
                                            .alias("Subject Loss and ALAE")])
                                    .drop("Inuring Pct")
                                    .select(["Simulation","Event ID","Risk Source ID","Peril","Loss #","Subject Loss","Subject Loss and ALAE"])
                                    )
                            .group_by(["Simulation","Event ID","Risk Source ID","Peril","Loss #"]).sum())
                elif (len(row['Inuring Layers'])>0):
                    (temp.extend(dfResults
                                    .filter(
                                       (pl.col('Risk Source ID').is_in(row['Risk Source IDs']))                                           
                                       &(pl.col('Layer').is_in(row['Inuring Layers'])))
                                    .rename({"Layer Loss": "Subject Loss",
                                            "Layer Loss and ALAE": "Subject Loss and ALAE"})
                                    .with_columns(pl.when(row['Subject Perils']==[])
                                                  .then(pl.lit('Keep'))
                                                  .when(pl.col('Peril').is_in(row['Subject Perils']))
                                                  .then(pl.lit('Keep'))
                                                  .otherwise(pl.lit('Drop'))
                                                  .alias('Flag Peril'))
                                    .filter(pl.col('Flag Peril')=='Keep')
                                    .drop('Flag Peril')                                              
                                    .with_columns(pl.when(row['Has Inuring As Placed']==False)
                                                  .then(pl.lit(1))
                                                  .otherwise(pl.col('Layer').replace_strict(asplacedpcts,default=1))
                                                  .alias('Inuring Pct'))
                                    .with_columns([(pl.col("Subject Loss")
                                            * (-1.0)
                                            * pl.col("Inuring Pct"))
                                            .alias("Subject Loss"),
                                            (pl.col("Subject Loss and ALAE")
                                            * (-1.0)
                                            * pl.col("Inuring Pct"))
                                            .alias("Subject Loss and ALAE")])
                                    .drop("Inuring Pct")
                                    .select(["Simulation","Event ID","Risk Source ID","Peril","Loss #","Subject Loss","Subject Loss and ALAE"])
                                    ))
                                        
                # if row['HasPerClaim']==True:
                #     if row['Per Claim ALAE Handling']=="Pro Rata":
                #         temp=(temp
                #                 .filter(pl.col('Event ID')!=0)
                #                 .with_columns(pl.col("Subject Loss").alias("PerClmSubj"))
                #                 .filter(pl.col('PerClmSubj')>row['Per Claim Retention'])
                #                 .with_columns(pl.when((pl.col('PerClmSubj')-row['Per Claim Retention'])>row['Per Claim Limit'])
                #                               .then(row['Per Claim Limit'])
                #                               .otherwise(pl.col('PerClmSubj')-row['Per Claim Retention'])
                #                               .alias('Layer Loss'))
                #                 .with_columns((pl.col('Layer Loss')*(pl.col('Subject Loss and ALAE')/pl.col('Subject Loss'))).alias('Layer Loss and ALAE'))
                #                 .with_columns([pl.col('Layer Loss').cast(pl.Float64),
                #                     pl.col('Layer Loss and ALAE').cast(pl.Float64)])
                #                 .drop(['Subject Loss','Subject Loss and ALAE','PerClmSubj'])
                #                 .rename({'Layer Loss':'Subject Loss','Layer Loss and ALAE':'Subject Loss and ALAE'}))
                #     elif row['Per Claim ALAE Handling']=="Excluded":
                #         temp=(temp
                #                 .filter(pl.col('Event ID')!=0)                              
                #                 .with_columns((pl.col("Subject Loss")).alias("PerClmSubj"))
                #                 .filter(pl.col('PerClmSubj')>row['Per Claim Retention'])
                #                  .with_columns(pl.when((pl.col('PerClmSubj')-row['Per Claim Retention'])>row['Per Claim Limit'])
                #                               .then(row['Per Claim Limit'])
                #                               .otherwise(pl.col('PerClmSubj')-row['Per Claim Retention'])
                #                               .alias('Layer Loss'))
                #                 .with_columns(pl.col('Layer Loss').alias('Layer Loss and ALAE'))
                #                 .with_columns([pl.col('Layer Loss').cast(pl.Float64),
                #                     pl.col('Layer Loss and ALAE').cast(pl.Float64)])
                #                 .drop(['Subject Loss','Subject Loss and ALAE','PerClmSubj'])
                #                 .rename({'Layer Loss':'Subject Loss','Layer Loss and ALAE':'Subject Loss and ALAE'}))
                #     elif row['Per Claim ALAE Handling']=='Included':
                #         temp=(temp
                #                 .filter(pl.col('Event ID')!=0)                              
                #                 .with_columns((pl.col("Subject Loss and ALAE")).alias("PerClmSubj"))
                #                 .filter(pl.col('PerClmSubj')>row['Per Claim Retention'])
                #                  .with_columns(pl.when((pl.col('PerClmSubj')-row['Per Claim Retention'])>row['Per Claim Limit'])
                #                               .then(row['Per Claim Limit'])
                #                               .otherwise(pl.col('PerClmSubj')-row['Per Claim Retention'])
                #                               .alias('Layer Loss and ALAE'))
                #                 .with_columns((pl.col('Layer Loss and ALAE')*(pl.col('Subject Loss')/pl.col('Subject Loss and ALAE'))).alias('Layer Loss'))
                #                 .with_columns([pl.col('Layer Loss').cast(pl.Float64),
                #                     pl.col('Layer Loss and ALAE').cast(pl.Float64)])
                #                 .drop(['Subject Loss','Subject Loss and ALAE','PerClmSubj'])
                #                 .rename({'Layer Loss':'Subject Loss','Layer Loss and ALAE':'Subject Loss and ALAE'}))                        
                
                if row['HasPerEvent']==True:
                    if row['Per Event ALAE Handling']=="Pro Rata":
                        temp=(temp
                                .with_columns((pl.col("Subject Loss").sum().over(['Simulation','Event ID'])).alias("TotalEvSubj"))
                                .filter(pl.col('TotalEvSubj')>row['Per Event Retention'])
                                .with_columns(pl.when((pl.col('TotalEvSubj')-row['Per Event Retention'])>row['Per Event Limit'])
                                              .then(row['Per Event Limit'])
                                              .otherwise(pl.col('TotalEvSubj')-row['Per Event Retention'])
                                              .alias('TotalEvRecovery'))
                                .with_columns((pl.col('TotalEvRecovery')*(pl.col('Subject Loss')/pl.col('TotalEvSubj'))).alias('Layer Loss'))
                                .with_columns((pl.col('Layer Loss')*(pl.col('Subject Loss and ALAE')/pl.col('Subject Loss'))).alias('Layer Loss and ALAE'))
                                .with_columns([pl.col('Layer Loss').cast(pl.Float64),
                                    pl.col('Layer Loss and ALAE').cast(pl.Float64)])
                                .drop(['Subject Loss','Subject Loss and ALAE','TotalEvSubj','TotalEvRecovery'])
                                .rename({'Layer Loss':'Subject Loss','Layer Loss and ALAE':'Subject Loss and ALAE'}))
                    elif row['Per Event ALAE Handling']=="Excluded":
                        temp=(temp
                                .with_columns((pl.col("Subject Loss").sum().over(['Simulation','Event ID'])).alias("TotalEvSubj"))                              
                                .filter(pl.col('TotalEvSubj')>row['Per Event Retention'])
                                .with_columns(pl.when((pl.col('TotalEvSubj')-row['Per Event Retention'])>row['Per Event Limit'])
                                              .then(row['Per Event Limit'])
                                              .otherwise(pl.col('TotalEvSubj')-row['Per Event Retention'])
                                              .alias('TotalEvRecovery'))
                                .with_columns((pl.col('TotalEvRecovery')*(pl.col('Subject Loss')/pl.col('TotalEvSubj'))).alias('Layer Loss'))
                                .with_columns(pl.col('Layer Loss').alias('Layer Loss and ALAE'))
                                .with_columns([pl.col('Layer Loss').cast(pl.Float64),
                                    pl.col('Layer Loss and ALAE').cast(pl.Float64)])
                                .drop(['Subject Loss','Subject Loss and ALAE','TotalEvSubj','TotalEvRecovery'])
                                .rename({'Layer Loss':'Subject Loss','Layer Loss and ALAE':'Subject Loss and ALAE'}))
                    elif row['Per Event ALAE Handling']=='Included':
                        temp=(temp
                                .with_columns((pl.col("Subject Loss and ALAE").sum().over(['Simulation','Event ID'])).alias("TotalEvSubj"))                              
                                .filter(pl.col('TotalEvSubj')>row['Per Event Retention'])
                                .with_columns(pl.when((pl.col('TotalEvSubj')-row['Per Event Retention'])>row['Per Event Limit'])
                                              .then(row['Per Event Limit'])
                                              .otherwise(pl.col('TotalEvSubj')-row['Per Event Retention'])
                                              .alias('TotalEvRecovery'))
                                .with_columns((pl.col('TotalEvRecovery')*(pl.col('Subject Loss and ALAE')/pl.col('TotalEvSubj'))).alias('Layer Loss and ALAE'))
                                .with_columns((pl.col('Layer Loss and ALAE')*(pl.col('Subject Loss')/pl.col('Subject Loss and ALAE'))).alias('Layer Loss'))
                                .with_columns([pl.col('Layer Loss').cast(pl.Float64),
                                    pl.col('Layer Loss and ALAE').cast(pl.Float64)])
                                .drop(['Subject Loss','Subject Loss and ALAE','TotalEvSubj','TotalEvRecovery'])
                                .rename({'Layer Loss':'Subject Loss','Layer Loss and ALAE':'Subject Loss and ALAE'}))

                if row['HasAgg']==True: 
                    if row['Aggregate ALAE Handling']=="Pro Rata":
                        temp=(temp
                                .with_columns((pl.col("Subject Loss").sum().over(['Simulation'])).alias("TotalAggSubj"))
                                .filter(pl.col('TotalAggSubj')>row['Aggregate Retention'])
                                .with_columns(pl.when((pl.col('TotalAggSubj')-row['Aggregate Retention'])>row['Aggregate Limit'])
                                              .then(row['Aggregate Limit'])
                                              .otherwise(pl.col('TotalAggSubj')-row['Aggregate Retention'])
                                              .alias('TotalAggRecovery'))
                                .with_columns((pl.col('TotalAggRecovery')*(pl.col('Subject Loss')/pl.col('TotalAggSubj'))).alias('Layer Loss'))
                                .with_columns((pl.col('Layer Loss')*(pl.col('Subject Loss and ALAE')/pl.col('Subject Loss'))).alias('Layer Loss and ALAE'))
                                .with_columns([pl.col('Layer Loss').cast(pl.Float64),
                                    pl.col('Layer Loss and ALAE').cast(pl.Float64)])
                                .drop(['Subject Loss','Subject Loss and ALAE','TotalAggSubj','TotalAggRecovery']))
                    elif row['Aggregate ALAE Handling']=="Excluded":
                        temp=(temp
                                .with_columns((pl.col("Subject Loss").sum().over(['Simulation'])).alias("TotalAggSubj"))
                                .filter(pl.col('TotalAggSubj')>row['Aggregate Retention'])
                                .with_columns(pl.when((pl.col('TotalAggSubj')-row['Aggregate Retention'])>row['Aggregate Limit'])
                                              .then(row['Aggregate Limit'])
                                              .otherwise(pl.col('TotalAggSubj')-row['Aggregate Retention'])
                                              .alias('TotalAggRecovery'))
                                .with_columns((pl.col('TotalAggRecovery')*(pl.col('Subject Loss')/pl.col('TotalAggSubj'))).alias('Layer Loss'))
                                .with_columns(pl.col('Layer Loss').alias('Layer Loss and ALAE'))
                                .with_columns([pl.col('Layer Loss').cast(pl.Float64),
                                    pl.col('Layer Loss and ALAE').cast(pl.Float64)])
                                .drop(['Subject Loss','Subject Loss and ALAE','TotalAggSubj','TotalAggRecovery']))
                    elif row['Aggregate ALAE Handling']=='Included':
                        temp=(temp
                                .with_columns((pl.col("Subject Loss and ALAE").sum().over(['Simulation'])).alias("TotalAggSubj"))
                               .filter(pl.col('TotalAggSubj')>row['Aggregate Retention'])
                                .with_columns(pl.when((pl.col('TotalAggSubj')-row['Aggregate Retention'])>row['Aggregate Limit'])
                                              .then(row['Aggregate Limit'])
                                              .otherwise(pl.col('TotalAggSubj')-row['Aggregate Retention'])
                                              .alias('TotalAggRecovery'))
                                .with_columns((pl.col('TotalAggRecovery')*(pl.col('Subject Loss and ALAE')/pl.col('TotalAggSubj'))).alias('Layer Loss and ALAE'))
                                .with_columns((pl.col('Layer Loss and ALAE')*(pl.col('Subject Loss')/pl.col('Subject Loss and ALAE'))).alias('Layer Loss'))
                                .with_columns([pl.col('Layer Loss').cast(pl.Float64),
                                    pl.col('Layer Loss and ALAE').cast(pl.Float64)])
                                .drop(['Subject Loss','Subject Loss and ALAE','TotalAggSubj','TotalAggRecovery']))
                else:
                    temp=temp.rename({'Subject Loss':'Layer Loss','Subject Loss and ALAE':'Layer Loss and ALAE'})  
                
                dfResults=pl.concat([dfResults,temp
                                 .with_columns(pl.lit(row['Layer']).alias('Layer'))
                                 .select(["Layer","Simulation","Event ID","Risk Source ID","Peril","Loss #","Layer Loss","Layer Loss and ALAE"])])
                        
        if dfResults.shape[0]>0:
            dfResults=(dfResults
                            .drop('Loss #')
                            .with_columns(((pl.col('Layer Loss and ALAE')/(pl.col('Layer Loss and ALAE').sum().over(['Layer','Simulation','Event ID'])))).alias('Allocated Loss')))         
            
            #Aggregate twice to get allocations by risk source and allocations by peril. SORT. Then concatenate horizontally.
            dfResults=(dfResults
                            .group_by(['Layer','Simulation','Event ID','Peril','Risk Source ID']).sum()
                            .group_by(['Layer','Simulation','Event ID','Peril'])
                            .agg(pl.col('Layer Loss').sum(),
                                pl.col('Layer Loss and ALAE').sum(),
                                pl.col('Risk Source ID'),
                                pl.col('Allocated Loss'))
                            .rename({'Allocated Loss':'Risk Source Allocated Loss'})
                            .sort('Layer','Simulation'))
            if save==True:
                _misc.saveToParquet(dfResults,f'{specs["dataparquetpath"]}/{scenario} - Ceded Layer Losses Cat.parquet')
        else:
            dfResults='Empty'

    return dfResults       
    
def strategyCededNetDetails(specs,scenario,strategies=None,segmentations='All',df=None,forReporting=False): 
    #Unnecessary to save some simple calculations. Remove columns before saving and add before reporting.
    def cleanupBeforeReporting(df):
        return (df
                .with_columns([(pl.col('Ceded Base Premium')+pl.col('Ceded Premium Adjustment')-pl.col('Provisional Ceding Commission')-pl.col('Ceding Commission Adjustment')-pl.col('Profit Commission')-pl.col('Ceded Loss and ALAE')).alias('Ceded Underwriting Profit')])
                        .with_columns([(pl.col('Provisional Ceding Commission')+pl.col('Ceding Commission Adjustment')+pl.col('Profit Commission')).alias('Ceded Expense'),
                        (pl.col('Ceded Base Premium')+pl.col('Ceded Premium Adjustment')).alias('Ceded Premium')])
                .with_columns([pl.when(pl.col('Ceded Premium')!=0)
                        .then(pl.col('Ceded Loss and ALAE')/pl.col('Ceded Premium'))
                        .otherwise(pl.lit(0))
                        .alias('Ceded Loss and ALAE Ratio')])
                .with_columns([pl.when(pl.col('Ceded Premium')!=0)
                        .then(1-(pl.col('Ceded Underwriting Profit')/pl.col('Ceded Premium')))
                        .otherwise(pl.lit(0))
                        .alias('Ceded Combined Ratio')])                  
                .with_columns([(pl.col('Gross Premium')-pl.col('Gross Expense')-pl.col('Gross Loss and ALAE')).alias('Gross Underwriting Profit'),
                        (pl.col('Gross Premium')-pl.col('Ceded Base Premium')).alias('Net Base Premium'),
                        (pl.col('Gross Loss and ALAE')-pl.col('Ceded Loss and ALAE')).alias('Net Loss and ALAE'),                            
                        (-1*pl.col('Ceded Premium Adjustment')).alias('Net Premium Adjustment'),
                        (pl.col('Gross Expense')-pl.col('Provisional Ceding Commission')).alias('Net Base Expense'),                    
                        (-1*pl.col('Ceding Commission Adjustment')).alias('Net Ceding Commission Adjustment'),
                        (-1*pl.col('Profit Commission')).alias('Net Profit Commission'),
                        (pl.col('Ceded Base Premium')+pl.col('Ceded Premium Adjustment')-pl.col('Provisional Ceding Commission')-pl.col('Ceding Commission Adjustment')-pl.col('Profit Commission')-pl.col('Ceded Loss and ALAE')).alias('Ceded Underwriting Profit')])
                .with_columns([(pl.col('Gross Underwriting Profit')-pl.col('Ceded Underwriting Profit')).alias('Net Underwriting Profit'),
                        (pl.col('Net Base Premium')+pl.col('Net Premium Adjustment')).alias('Net Premium'),
                        (pl.col('Net Base Expense')+pl.col('Net Profit Commission')+pl.col('Net Ceding Commission Adjustment')).alias('Net Expense')])
                .with_columns([pl.when(pl.col('Net Premium')!=0)
                        .then(pl.col('Net Loss and ALAE')/pl.col('Net Premium'))
                        .otherwise(pl.lit(0))
                        .alias('Net Loss and ALAE Ratio'),
                        pl.when(pl.col('Ceded Premium')!=0)
                        .then(pl.col('Ceded Loss and ALAE')/pl.col('Ceded Premium'))
                        .otherwise(pl.lit(0))
                        .alias('Ceded Loss and ALAE Ratio')])
                .with_columns([pl.when(pl.col('Net Premium')!=0)
                        .then(1-(pl.col('Net Underwriting Profit')/pl.col('Net Premium')))
                        .otherwise(pl.lit(0))
                        .alias('Net Combined Ratio'),
                        pl.when(pl.col('Ceded Premium')!=0)
                        .then(1-(pl.col('Ceded Underwriting Profit')/pl.col('Ceded Premium')))
                        .otherwise(pl.lit(0))
                        .alias('Ceded Combined Ratio')])
                .drop(['Gross Loss and ALAE','Gross Premium','Gross Expense','Gross Underwriting Profit']))  

    #Derive missing and required combinations and return results if no missing strategies/segmentations
    #region
    infotype='ceded net uw details by strategy'
    if not isinstance(df,pl.DataFrame):
        missingAndRequired=missingAndRequiredCombinations(specs,scenario,infotype,strategies=strategies,segmentations=segmentations)
    else:
        missingAndRequired=missingAndRequiredCombinations(specs,scenario,infotype,df=df)   

    missing=missingAndRequired[0]

    if missing.shape[0] + missingAndRequired[1].shape[0]==0:
       return 'No ceded net uw details by strategy results to calculate'
    elif missing.shape[0]==0:
        #get from file
        if forReporting:
            return cleanupBeforeReporting(getResults(specs,infotype,scenario,missingAndRequired[1]))
        else:
            return getResults(specs,infotype,scenario,missingAndRequired[1])
    #endregion  

    #Calculations
    #region 
    missingceded=missing.filter(pl.col('Strategy')!='No Reinsurance').join(specs['Treaty Terms'].select(['Strategy','Treaty Loss Layer']).rename({'Treaty Loss Layer':'Treaty'}),how='left',on='Strategy')
    if missingceded.shape[0]>0:
        ceded=(strategyCededDetails(specs,scenario,df=missingceded)
               .drop('Reinsurer Deficit')
               .with_columns([pl.lit('All Treaties').alias('Treaty'),pl.lit('All Layers').alias('Layer')])
               .group_by(['Segmentation','Segment','Strategy','Treaty','Layer','Simulation'])
               .sum()
               .sort('Strategy','Segmentation','Segment','Simulation'))
    else:
        ceded=pl.DataFrame()

    IndexStrings=ceded.select(['Segmentation','Segment','Strategy']).unique().get_columns()
    IndexStrings=IndexStrings[0]+'|'+IndexStrings[1]+'|'+IndexStrings[2]
    missinggross=missing.select('Segmentation').unique()
    missinggrosswithceded=missing.filter(pl.col('Strategy')!='No Reinsurance').select('Segmentation').unique()
    missinggrosswithnoceded=missing.filter(pl.col('Strategy')=='No Reinsurance').select('Segmentation').unique()

    if missinggross.shape[0]>0:
        gross=grossUWResult(specs,scenario,df=missinggross)
        if missinggrosswithceded.shape[0]>0:
            grosswithceded=(gross
                .filter(pl.col('Segmentation').is_in(missinggrosswithceded.get_column('Segmentation').unique().to_list()))
                .join(specs['Treaty Terms'].select(['Strategy']).unique(),
                     how='cross')
                .filter(pl.concat_str(['Segmentation','Segment','Strategy'],separator='|').is_in(IndexStrings.to_list()))
                .sort(['Strategy','Segmentation','Segment','Simulation'])
                .drop(['Simulation','Segmentation','Segment','Strategy']))
        else:
            grosswithceded=pl.DataFrame()

        if missinggrosswithnoceded.shape[0]>0:
            grosswithnoceded=pl.DataFrame()
            grosswithnoceded=(gross
                .filter(pl.col('Segmentation').is_in(missinggrosswithnoceded.get_column('Segmentation').unique().to_list()))
                .with_columns(pl.lit('No Reinsurance').alias('Strategy'))
                .with_columns([pl.lit('All Treaties').alias('Treaty'),pl.lit('All Layers').alias('Layer')])
                .with_columns([pl.lit(0.0).alias('Ceded Loss and ALAE'),
                               pl.lit(0.0).alias('Ceded Base Premium'),
                               pl.lit(0.0).alias('Ceded Premium Adjustment'),
                               pl.lit(0.0).alias('Provisional Ceding Commission'),
                               pl.lit(0.0).alias('Ceding Commission Adjustment'),
                               pl.lit(0.0).alias('Profit Commission')]))
        else:
            grosswithnoceded=pl.DataFrame()
    else:
        gross=pl.DataFrame()
                
    result=pl.concat([pl.concat([grosswithceded,ceded],how='horizontal'),grosswithnoceded],how='diagonal')

    if result.shape[0]==0:
        return 'No ceded net uw details by strategy results to calculate'
    
    #Return results and save, if necessary
    #region  
    if specs['savedetail'].upper()=='YES':
        #if file exists, append missing. Otherwise, create file.
        if os.path.exists(f'{specs["dataparquetpath"]}/{scenario} - {infotype}.parquet'):
            _misc.saveToParquet(pl.concat([result,getResults(specs,infotype,scenario)]),
                                f'{specs["dataparquetpath"]}/{scenario} - {infotype}.parquet')
            if forReporting:
                return cleanupBeforeReporting(getResults(specs,infotype,scenario,df=missingAndRequired[1]))
            else:
                return getResults(specs,infotype,scenario,df=missingAndRequired[1])
        else:
            _misc.saveToParquet(result,f'{specs["dataparquetpath"]}/{scenario} - {infotype}.parquet')
            if forReporting:
                return cleanupBeforeReporting(result)
            else:
                return result
    else:
        if forReporting:
            return cleanupBeforeReporting(result)
        else:
            return result
    #endregion   

def summaryUWStatistics(specs,scenario,strategies=None,segmentations='All',treaties=None,df=None):
    def finalCleanup(df):
        #Need to recalculate rank if new strategies are added to an existing file
        df=(df
                .with_columns(pl.when(pl.col('Treaty')=='All Treaties')
                              .then(pl.lit('All Treaties'))
                              .otherwise(pl.lit('By Treaty'))
                              .alias('Partition'))
                .partition_by('Partition',as_dict=True))
        
        if ('By Treaty',) in df.keys():
            df[('By Treaty',)]=(df[('By Treaty',)]
                                .with_columns(pl.lit(0).cast(pl.Int64).alias('Rank'))
                                .join(specs['Treaty Terms'].select(['Treaty Loss Layer','Strategy','Strategy ID','Treaty ID']),how='left',left_on=['Strategy','Treaty'],right_on=['Strategy','Treaty Loss Layer']))
        else:
            df[('By Treaty',)]=pl.DataFrame()

        if ('All Treaties',) in df.keys():
            df[('All Treaties',) ]=(df[('All Treaties',) ]
                                .with_columns(pl.col('Value').rank(method='ordinal').over(['Segmentation','Segment','Metric','Statistic','Percentile']).cast(pl.Int64).alias('Rank'))
                                .join(specs['Treaty Terms'].select(['Strategy','Strategy ID']).unique(),how='left',on=['Strategy'])
                                .with_columns(pl.lit(0).cast(pl.Int64).alias('Treaty ID')))
        else:
            df[('All Treaties',) ]=pl.DataFrame()

        return pl.concat([df[('By Treaty',)],df[('All Treaties',) ]],how='diagonal').with_columns(pl.col('Strategy ID').fill_null(0)).drop('Partition')
            
    #Derive missing and required combinations and return results if no missing strategies/segmentations
    #region
    infotype='summary uw statistics'
    if not isinstance(df,pl.DataFrame):
        missingAndRequired=missingAndRequiredCombinations(specs,scenario,infotype,strategies=strategies,segmentations=segmentations,treaties=treaties)
    else:
        missingAndRequired=missingAndRequiredCombinations(specs,scenario,infotype,df=df)   

    missing=missingAndRequired[0]

    if missing.shape[0] + missingAndRequired[1].shape[0]==0:
       return 'No summary uw statistics to calculate'
    elif missing.shape[0]==0:
        #get from file
        return finalCleanup(getResults(specs,infotype,scenario,missingAndRequired[1]))
    #endregion      

    #Calculations
    #region
    missingcedednet=missing.filter(pl.col('Treaty')=='All Treaties').drop('Treaty')
    missingceded=missing.filter(pl.col('Treaty')!='All Treaties')
    complementcols=[x for x in specs['dict_complement'].keys() if specs['dict_complement'][x]==True]
    tvarcols=specs['KPI Metrics'].filter(pl.col('Statistic')=='TVaR').get_column('Metric').to_list()
    tvarpctiles=specs['KPI Percentiles'].get_column('Percentile').to_list()
    pctilelist=(specs["Percentiles"]
                .filter(pl.col("Percentile Table") == specs["gcnpctiles"])
                .get_column('Percentile')
                .to_list())

    if missingcedednet.shape[0]>0:
        cedednet=(strategyCededNetDetails(specs,scenario,df=missingcedednet,forReporting=True)
              .drop('Layer')
              .with_columns(pl.concat_str(['Strategy','Segmentation','Segment','Treaty'],separator='|').alias('IndexString'))
              .drop('Strategy','Segmentation','Segment','Treaty'))
        
        cedednetcomplementcols=[x for x in complementcols if x in cedednet.columns]
        resultCededNet=pl.DataFrame()
        for indexstring in cedednet.get_column('IndexString').unique().to_list():
            resultCededNet=pl.concat([resultCededNet,(cedednet
                                    .filter(pl.col('IndexString')==indexstring)
                                    .with_columns((-1*pl.col(x)).alias(x) for x in cedednetcomplementcols)
                                    .drop(['Simulation','IndexString'])
                                    .describe(percentiles=pctilelist)
                                    .with_columns(pl.lit(indexstring).alias('IndexString')))])
        resultCededNet=(resultCededNet.unpivot(on=[x for x in resultCededNet.columns if x not in ['statistic','IndexString']],index=['IndexString','statistic'])
                    .filter(~pl.col('statistic').is_in(['count','null_count']))
                    .pivot(on='statistic',index=['IndexString','variable'],values='value')
                    .with_columns((pl.col('std')/pl.col('mean')).alias('Coefficient of Variation'))
                    .rename({'variable':'Metric','min':'Min','mean':'Mean','std':'Std Dev','max':'Max'}))
        resultCededNet=(resultCededNet.unpivot(on=[x for x in resultCededNet.columns if x not in ['Metric','IndexString']],index=['IndexString','Metric'])
                .rename({'variable':'Statistic','value':'Value'})
                .with_columns(pl.col('Value').cast(pl.Float64)))   
    else:
        resultCededNet=pl.DataFrame()

    if missingceded.shape[0]>0:     
        ceded=(strategyCededDetails(specs,scenario,df=missingceded,forReporting=True)
           .drop(['Layer','Reinsurer Deficit'])
           .with_columns(pl.concat_str(['Strategy','Segmentation','Segment','Treaty'],separator='|').alias('IndexString'))
           .drop('Strategy','Segmentation','Segment','Treaty'))

        cededcomplementcols=[x for x in complementcols if x in ceded.columns]
        resultCeded=pl.DataFrame()
        for indexstring in ceded.get_column('IndexString').unique().to_list():
            resultCeded=pl.concat([resultCeded,(ceded
                                    .filter(pl.col('IndexString')==indexstring)
                                    .with_columns((-1*pl.col(x)).alias(x) for x in cededcomplementcols)
                                    .drop(['Simulation','IndexString'])
                                    .describe(percentiles=pctilelist)
                                    .with_columns(pl.lit(indexstring).alias('IndexString')))])
        resultCeded=(resultCeded.unpivot(on=[x for x in resultCeded.columns if x not in ['statistic','IndexString']],index=['IndexString','statistic'])
                    .filter(~pl.col('statistic').is_in(['count','null_count']))
                    .pivot(on='statistic',index=['IndexString','variable'],values='value')
                    .with_columns((pl.col('std')/pl.col('mean')).alias('Coefficient of Variation'))
                    .rename({'variable':'Metric','min':'Min','mean':'Mean','std':'Std Dev','max':'Max'}))
        resultCeded=(resultCeded.unpivot(on=[x for x in resultCeded.columns if x not in ['Metric','IndexString']],index=['IndexString','Metric'])
                .rename({'variable':'Statistic','value':'Value'})
                .with_columns(pl.col('Value').cast(pl.Float64)))

    #Combine results
    complementcols=[x for x in complementcols if x in list(set(resultCeded.get_column('Metric').to_list()+resultCededNet.get_column('Metric').to_list()))]
    result=(pl.concat([resultCeded,resultCededNet])
        .with_columns(pl.col('IndexString').str.split_exact('|',3).alias('IndexString'))
        .unnest('IndexString')
        .rename({'field_0':'Strategy','field_1':'Segmentation','field_2':'Segment','field_3':'Treaty'})        
        .with_columns(pl.col('Statistic')
                    .map_elements(lambda x: None if x in ['Coefficient of Variation','Mean','Min','Max','Std Dev'] else float(x.strip('%'))/100,return_dtype=pl.Float64)
                    .alias('Percentile'))
        .with_columns(pl.when(pl.col('Percentile').is_not_null())
                    .then(pl.lit('Percentile'))
                    .otherwise(pl.col('Statistic'))
                    .alias('Statistic'))
        .with_columns(pl.when((pl.col('Metric').is_in(complementcols)) & (pl.col('Statistic')!='Std Dev'))
                    .then(-1*pl.col('Value'))
                    .otherwise(pl.col('Value'))
                    .alias('Value'))
        .with_columns(pl.when((pl.col('Metric').is_in(complementcols))&(pl.col('Statistic')=='Min'))
                    .then(pl.lit('Max'))
                    .when((pl.col('Metric').is_in(complementcols))&(pl.col('Statistic')=='Max'))
                    .then(pl.lit('Min'))
                    .otherwise(pl.col('Statistic'))
                    .alias('Statistic')))
    #endregion

    #Return results and save, if necessary
    #region  
    if 'summary uw statistics' not in specs.keys():
        specs['summary uw statistics']={}

    if specs['savesummary'].upper()=='YES':
        #if file exists, append missing. Otherwise, create file.
        if os.path.exists(f'{specs["dataparquetpath"]}/{scenario} - {infotype}.parquet'):
            _misc.saveToParquet(finalCleanup(pl.concat([result,getResults(specs,infotype,scenario)]),
                                f'{specs["dataparquetpath"]}/{scenario} - {infotype}.parquet'))
            result= getResults(specs,infotype,scenario,df=missingAndRequired[1])
            specs[infotype].update({scenario:result.with_columns(pl.lit(scenario).alias('Scenario'))})            
            return finalCleanup(result)
        
        else:
            _misc.saveToParquet(result,f'{specs["dataparquetpath"]}/{scenario} - {infotype}.parquet')
            specs[infotype].update({scenario:result.with_columns(pl.lit(scenario).alias('Scenario'))})            
            return finalCleanup(result)
    else:
        specs[infotype].update({scenario:result.with_columns(pl.lit(scenario).alias('Scenario'))})        
        return finalCleanup(result)
    #endregion  

def allSummaryUWStatistics(specs):
    def finalCleanup(df):
        #Need to recalculate rank if new strategies are added to an existing file
        df=(df
                .with_columns(pl.when(pl.col('Treaty')=='All Treaties')
                              .then(pl.lit('All Treaties'))
                              .otherwise(pl.lit('By Treaty'))
                              .alias('Partition'))
                .partition_by('Partition',as_dict=True))
        
        if ('By Treaty',) in df.keys():
            df[('By Treaty',)]=(df[('By Treaty',)]
                                .with_columns(pl.lit(0).cast(pl.Int64).alias('Rank'))
                                .join(specs['Treaty Terms'].select(['Treaty Loss Layer','Strategy','Strategy ID','Treaty ID']),how='left',left_on=['Strategy','Treaty'],right_on=['Strategy','Treaty Loss Layer']))
        else:
            df[('By Treaty',)]=pl.DataFrame()

        if ('All Treaties',) in df.keys():
            df[('All Treaties',) ]=(df[('All Treaties',) ]
                                .with_columns(pl.col('Value').rank(method='ordinal').over(['Scenario','Segmentation','Segment','Metric','Statistic','Percentile']).cast(pl.Int64).alias('Rank'))
                                .join(specs['Treaty Terms'].select(['Strategy','Strategy ID']).unique(),how='left',on=['Strategy'])
                                .with_columns(pl.lit(0).cast(pl.Int64).alias('Treaty ID')))
        else:
            df[('All Treaties',) ]=pl.DataFrame()

        return pl.concat([df[('By Treaty',)],df[('All Treaties',) ]],how='diagonal').with_columns(pl.col('Strategy ID').fill_null(0)).drop('Partition')
        
    if 'summary uw statistics' not in specs.keys():
        specs['summary uw statistics']={}

    result=pl.DataFrame()
    for scenario in specs['Scenarios'].get_column('Scenario').unique().to_list():
        print(1)
        if scenario not in specs['summary uw statistics'].keys():
            print(2)
            summaryUWStatistics(specs,scenario)
        result=pl.concat([result,specs['summary uw statistics'][scenario].with_columns(pl.lit(scenario).alias('Scenario'))])    
    return finalCleanup(result).select(['Scenario','Strategy','Segmentation','Segment','Treaty','Metric','Statistic','Value','Percentile','Rank','Strategy ID','Treaty ID'])

def CreateKPIResults(specs,scenarios=None,strategies=None,segmentations=None,df=None):
    MYLOGGER.debug('Start CreateKPIResults') 
    if not scenarios:
        _scenarios=specs['Scenarios'].get_column('Scenario').unique().to_list()
    elif isinstance(scenarios,str):
        _scenarios=specs['Scenarios'].filter(pl.col('Scenario')==scenarios).get_column('Scenario').unique().to_list()
    elif isinstance(scenarios,list):
        _scenarios=specs['Scenarios'].filter(pl.col('Scenario').is_in(scenarios)).get_column('Scenario').unique().to_list()

    if not segmentations:
        _segmentations=specs['Segmentations'].filter(pl.col('Segmentation')!='All')
    elif isinstance(segmentations,str):
        _segmentations=specs['Segmentations'].filter(pl.col('Segmentation')==segmentations).filter(pl.col('Segmentation')!='All')
    elif isinstance(segmentations,list):
        _segmentations=specs['Segmentations'].filter(pl.col('Segmentation').is_in(segmentations)).filter(pl.col('Segmentation')!='All')

    if not strategies:
        _strategies=specs['Treaty Terms'].get_column('Strategy').unique().to_list()
    elif isinstance(strategies,str):
        _strategies=_misc.list_intersection(specs['Treaty Terms'].get_column('Strategy').unique().to_list(),[strategies])
    elif isinstance(strategies,list):
        _strategies=_misc.list_intersection(specs['Treaty Terms'].get_column('Strategy').unique().to_list(),strategies)
    else:
        _strategies=[]

    selectedKPIs=(specs['KPIs']
        .melt(id_vars=['KPI Group'],
                value_vars=['Profitability Measures',
                'Volatility Measures',
                'Surplus Protection Measures',
                'Other Measures',
                'Return Periods'])
        .rename({'variable':'KPI Category','value':'KPI Metric'})
        .explode('KPI Metric'))

    if (selectedKPIs.filter(pl.col('KPI Category')!='Return Periods').filter(pl.col('KPI Metric').str.contains('VaR')).shape[0]>0) & (selectedKPIs.filter(pl.col('KPI Category')=='Return Periods').shape[0]>0):
        selectedKPIsWithRP=(selectedKPIs.filter(pl.col('KPI Category')!='Return Periods').filter(pl.col('KPI Metric').str.contains('VaR'))
            .join(selectedKPIs.filter(pl.col('KPI Category')=='Return Periods').select(['KPI Metric']).rename({'KPI Metric':'Return Period'}),how='cross')
            .join(specs['KPI Percentiles'].select(['Return Period','Percentile']),how='left',on='Return Period'))
    else:
        selectedKPIsWithRP=pl.DataFrame()

    result=pl.DataFrame()
    for thisscenario in _scenarios:
        GCNStats=GCNStatistics(specs,thisscenario,_strategies,_segmentations,save=True)

        result=pl.concat([result,(pl.concat([selectedKPIs
                    .filter(pl.col('KPI Category')!='Return Periods')
                    .filter(~pl.col('KPI Metric').str.contains('VaR'))
                    .with_columns(pl.lit(None).cast(pl.Utf8).alias('Return Period'),pl.lit(None).cast(pl.Float64).alias('Percentile')),selectedKPIsWithRP],how='diagonal')
        .drop('KPI Group')
        .join(specs['KPI Metrics'].select(['Metric','Label','Statistic']).unique(),how='left',left_on='KPI Metric',right_on='Label')
        .join(GCNStats
            .with_columns(
                pl.when(pl.col('Statistic')=='Percentile')
                .then(pl.lit('VaR'))
                .otherwise(pl.col('Statistic'))
                .alias('Statistic'))
            .filter(pl.col('Treaty')=='All Treaties')
            .drop(['Treaty','Treaty ID']),
            how='left',on=['Metric','Statistic','Percentile'])
        .with_columns(
            pl.when(pl.col('Return Period').is_not_null())
            .then((pl.col('Return Period')+' '+pl.col('KPI Metric')).alias('KPI Metric'))
            .otherwise(pl.col('KPI Metric'))
            .alias('KPI Metric'))
        .with_columns(pl.when(pl.col('Metric').replace_strict(specs['dict_complement'],default=False)==False)
                        .then(pl.col('Value')
                            .rank(method='ordinal',descending=False)
                            .over(['Segmentation','Segment','KPI Metric','Percentile']))
                        .otherwise(pl.col('Value')
                            .rank(method='ordinal',descending=True)
                            .over(['Segmentation','Segment','KPI Metric','Percentile']))
                        .alias('Rank'))
        .select(['Scenario','Strategy','Strategy ID','Segmentation','Segment','KPI Category','KPI Metric','Statistic','Percentile','Rank','Value'])
        .with_columns(pl.lit(thisscenario).alias('Scenario')))],how='vertical')

    result=(result
        .sort(['KPI Category','Strategy ID','Segmentation','Segment','KPI Metric','Percentile','Rank'])
        .filter(pl.col('Strategy ID').is_not_null())
        )

    return result

def createCoTVaRs(scenario,strategies=None,segmentations=None):
    pass

def modelSpecificAnalysisSteps(analysis):
    _misc.copyTableToSht(analysis.book,1,allSummaryUWStatistics(analysis.preppedspecs),'Model Results','Results_GCNStatistics')