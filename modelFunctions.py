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


def initialCleanSpecs(specdict,xlspath):
    global LOSSFOLDER
    MYLOGGER.debug('Start initialCleanSpecs')
    #Just preliminary cleaning of specs. Shouldn't be adding or deleting columns. Info after this step will be source data for data forms.
    result=specdict
    result.update({"error":""})
    result.update({"warnings": []}) #to add warning - result["warnings"] = result["warnings"] + ["this is making some kind of assumption"]
    keys=list(specdict.keys())

    def CleanStepsByKey(key):
        MYLOGGER.debug("Starting clean specs for " + key)
        if key == "General":
            result[key].columns=["Information","Value"]           
            result[key]=result[key].filter(pl.col("Information").is_in(['Loss Data Source','CSV Filename','Aggregation Threshold for Events',"Evaluation Dates for Ceded Detail","Destination for Gross and Ceded Detail"]))
            result.update({"losscsv":result[key].filter(pl.col("Information")=="CSV Filename").get_column("Value")[0] if result[key].filter(pl.col("Information")=="Loss Data Source").get_column("Value")[0]=="CSV" else None})
            result.update({"aggregationThreshold":float(result[key].filter(pl.col("Information")=="Aggregation Threshold for Events").get_column("Value")[0])})
            result.update({"detaildestination":result[key].filter(pl.col("Information")=="Destination for Gross and Ceded Detail").get_column("Value")[0]})
            if result["detaildestination"] not in ['This File','New File','Exclude']:
                result["detaildestination"]="This File"            
            result.update({"cededDetailVals":result[key].filter(pl.col("Information")=="Evaluation Dates for Ceded Detail").get_column("Value")[0]})
            if result["cededDetailVals"] not in ['Current','All']:
                result["cededDetailVals"]="Current"

            MYLOGGER.debug("Finished cleaning specs for " + key)
        elif key == "Risk Sources":
            if result[key].shape[0]==0:
                #no Risk Source info
                MYLOGGER.critical("Risk Source spec table is empty")
                result["error"]="Risk Source spec table is empty"
            else:
                result[key]=result[key].unique(subset=["Risk Source"], keep='first',maintain_order=True)
                result.update({"mapECOXPLALAEHandling":dict(zip(result[key].get_column("Risk Source").to_list(),result[key].get_column("ECOXPL ALAE Handling").to_list()))})
                MYLOGGER.debug("Finished cleaning specs for " + key)
                
        elif key=='Layers':
            # Clean Per Claim Limit and Retention Info
            def cleanLimitsRetentionsALAE(df, limfield, retfield, resultfield):

                df = df.with_columns(
                    pl.when(
                        (pl.col(limfield).is_null())
                        & (pl.col(retfield) != 0)
                        & (~pl.col(retfield).is_null())
                    )
                    .then(pl.lit(infiniteloss).alias(limfield))
                    .otherwise(pl.col(limfield).alias(limfield))
                )

                df = df.with_columns(
                    pl.when(pl.col(retfield).is_null() & (~pl.col(limfield).is_null()))
                    .then(pl.lit(0).alias(retfield))
                    .otherwise(pl.col(retfield).alias(retfield))
                )

                df = df.with_columns(
                    pl.when(pl.col(limfield).is_null())
                    .then(pl.lit(None).alias(retfield))
                    .otherwise(pl.col(retfield).alias(retfield))
                )

                df = df.with_columns(
                    pl.when((pl.col(retfield).is_nan())|(pl.col(retfield).is_null()))
                    .then(pl.lit(False).alias(resultfield))
                    .otherwise(pl.lit(True).alias(resultfield))
                )
                
                return df
            
            initialdf=result[key]
            result[key] = cleanLimitsRetentionsALAE(
                result[key],
                "Per Claim Limit",
                "Per Claim Retention",
                "HasPerClaim",
            )
            #TO DO: add some statement that checks initaldf = result[key]

            result[key]=result[key].with_columns(pl.when((pl.col('Per Claim Limit')==999999999)&(pl.col('Per Claim Retention')==0)).then(pl.lit(False)).otherwise(pl.col('HasPerClaim')).alias('HasPerClaim'))
            
            initialdf=result[key]
            result[key] = cleanLimitsRetentionsALAE(
                result[key],
                "Per Event Limit",
                "Per Event Retention",
                "HasPerEvent",
            )
            #TO DO: add some statement that checks initaldf = df

            #CHECK that code is accounting for per claim inuring into per claim layer
            
            # # If has per claim, delete any inurances
            # result[key] = result[key].with_columns(
            #     pl.when(pl.col("HasPerClaim") == False)
            #     .then(pl.col("Inuring Layers"))
            #     .otherwise(pl.lit(None))
            #     .cast(pl.Utf8)
            #     .alias("Inuring Layers")
            #                 )

            # Convert strings to lists for underlying layers and inuring layers
            result[key] = (
                result[key].with_columns(
                    [
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
                .with_columns(
                    [
                        pl.col("Underlying Layers")
                        .map_elements(lambda x: [y.strip() for y in x])
                        .cast(pl.List(pl.Utf8))
                        .alias("Underlying Layers"),
                        pl.col("Inuring Layers")
                        .map_elements(lambda x: [y.strip() for y in x])
                        .cast(pl.List(pl.Utf8))
                        .alias("Inuring Layers"),
                    ]
                )
            )
            
            #TO DO: add code that checks that underlying layers specified actually exist (is in the reinsurance strategy tool)

            if result[key].filter(pl.col('Per Claim Retention')>0).shape[0]>0:
                lowestNonZeroAttach=result[key].filter(pl.col('Per Claim Retention')>0).get_column('Per Claim Retention').min()
                result["aggregationThreshold"]=min(result["aggregationThreshold"],lowestNonZeroAttach)
            if result[key].filter(pl.col('Per Claim Retention')==0).shape[0]>0:
                lowestLimitOnZeroAttach=result[key].filter(pl.col('Per Claim Retention')==0).get_column('Per Claim Limit').min()
                result["aggregationThreshold"]=min(result["aggregationThreshold"],lowestLimitOnZeroAttach)

        elif key=="CDF Increments":
            result[key]=(result[key]
                .unique(subset=["Group Name","Starting at"], keep='first',maintain_order=True)
                .sort(["Group Name","Starting at"],descending=[False,False]))
            
        elif key == "Severity Trend":
            # NOTE: USES CLEANED RISK SOURCE DF
            result[key] = (result["Risk Sources"]
                                    .select(["Risk Source","Ind Sev Trend","Med Sev Trend",
                                                            "Exp Sev Trend","ECOXPL Sev Trend"])
                                    .melt(id_vars='Risk Source')
                                    .rename({'variable':'Attribute','value':'Severity Trend'})
                                    .filter(pl.col('Severity Trend').is_not_null())
                                    .with_columns(pl.col('Attribute').str.replace(' Sev Trend',''))
                                    .join(result["Severity Trend"]
                                            .melt(id_vars=["Year"])
                                            .rename({"variable": "SevTrendGrp", "value": "SevTrend"}),
                                            left_on="Severity Trend", right_on="SevTrendGrp", how="left")
                                    .select(["Risk Source", "Year", "Attribute", "SevTrend"])
                                    .with_columns(pl.col('SevTrend').cast(pl.Float64))
                                    .pivot(index=["Risk Source", "Year"],
                                            columns="Attribute",values="SevTrend",aggregate_function='first')
                                    .select(["Risk Source", "Year", "ECOXPL", "Exp", "Ind", "Med"]))                        
            MYLOGGER.debug("Finished cleaning specs for " + key)
            
        elif key == "CDF Specs":
            result[key].columns=['Information','Value']
            result[key]=(result[key]
                    .transpose(include_header=False,column_names=result[key].get_column("Information").to_list())
                    .filter(pl.col('Description')!="Description")
                    .filter(pl.col('Description').is_not_null())
                    .filter(pl.col('By Risk Source or RS Group').is_not_null())
                    .filter(pl.col('Metric').is_not_null())
                    .filter(pl.col('ECOXPL Treatment').is_not_null())
                    .filter(pl.col('Paid or Incurred').is_not_null())
                    .filter(pl.col('Claim Level').is_not_null())
                    .filter(pl.col('Trend').is_not_null())
                    .filter(pl.col('Period').is_not_null())
                    .filter(pl.col('Eval Date or Eval Age').is_not_null())
                    .filter(((pl.col('Eval Date or Eval Age')=="Eval Date")&(pl.col('Eval Date').is_not_null()))|((pl.col('Eval Date or Eval Age')=="Eval Age")&(pl.col('Eval Age').is_not_null())))
                    .filter(pl.col('CDF Increments Group').is_not_null())
                    .with_columns([pl.when(pl.col('First Year').is_null()).then(pl.lit(None)).otherwise(pl.col('First Year').cast(pl.Float64).round(0).cast(pl.Int64)).alias('First Year'),
                                    pl.when(pl.col('Last Year').is_null()).then(pl.lit(None)).otherwise(pl.col('Last Year').cast(pl.Float64).round(0).cast(pl.Int64)).alias('Last Year'),
                                    pl.when(pl.col('Eval Age').is_null()).then(pl.lit(None)).otherwise(pl.col('Eval Age').cast(pl.Float64).round(0).cast(pl.Int64)).alias('Eval Age'),
                                    pl.col('CDF Increments Group').str.replace(r'\.0$','').alias('CDF Increments Group'),
                                    pl.when(pl.col('Policy Limit Min').is_null()).then(pl.lit(0)).otherwise(pl.col('Policy Limit Min').cast(pl.Float64).round(0).cast(pl.Int64)).alias('Policy Limit Min'),
                                    pl.when(pl.col('Policy Limit Max').is_null()).then(pl.lit(999999999)).otherwise(pl.col('Policy Limit Max').cast(pl.Float64).round(0).cast(pl.Int64)).alias('Policy Limit Max')]))            
            MYLOGGER.debug("Finished cleaning specs for " + key)
            
        elif key=="Stacking and Sharing":
            result[key] = result[key].unique(subset=["Claim Number"], keep='first',maintain_order=True)
            MYLOGGER.debug("Finished cleaning specs for " + key)
            
        elif key=="Events":
            result[key] = result[key].unique(subset=["Claim Number"], keep="first",maintain_order=True)
            MYLOGGER.debug("Finished cleaning specs for " + key)

        elif key=="Wide Losses":
            result['Losses']=pl.DataFrame(schema={'Evaluation Date': pl.Date,'Claim Number': pl.Utf8,'Risk Source': pl.Utf8,'Coverage Type': pl.Utf8,'Other Info 1': pl.Utf8,
                'Other Info 2': pl.Utf8,'Other Info 3': pl.Utf8,'Date of Loss': pl.Date,'Report Date': pl.Date,'Policy Effective Date': pl.Date,
                'Custom A Year': pl.Int64,'Custom B Year': pl.Int64,'Insured': pl.Utf8,'State': pl.Utf8,'Policy Limit': pl.Int64,'Defense Outside Limit': pl.Boolean,'Deductible': pl.Int64,
                'Loss Data Gross or Net of Deductible': pl.Utf8,'Deductible Application': pl.Utf8,'Deductible Erodes Policy Limit': pl.Boolean,
                'Coverage Expense Constant': pl.Int64,'Indemnity Paid': pl.Int64,'Indemnity Reserves': pl.Int64,'Medical Paid': pl.Int64,'Medical Reserves': pl.Int64,'Expense Paid': pl.Int64,'Expense Reserves': pl.Int64})
            losscols=[c for c in result[key].columns if ":" in c]
            othercols=[c for c in result[key].columns if c not in losscols]
                                    
            result[key]=(result[key]
                                            .with_row_count('id')
                                            .melt(id_vars=othercols+['id'],value_vars=losscols)
                                            .with_columns(pl.col("variable").str.split_exact(":",n=1).alias("split_str"))
                                            .unnest("split_str")
                                            .pivot(
                                                index=othercols+['id','field_1'],
                                                values='value',
                                                on='field_0')
                                            .with_columns(pl.col('field_1').cast(pl.Utf8).str.strip_chars())
                                            .rename({'field_1':'Date Suffix'})
                                            .join(result['Wide Format Dates'],on='Date Suffix',how='left')
                                            .drop(['Date Suffix','id'])
                                            .filter(~(pl.col('Indemnity Paid').is_null()&pl.col('Indemnity Reserves').is_null()&pl.col('Medical Paid').is_null()&pl.col('Medical Reserves').is_null()&pl.col('Expense Paid').is_null()&pl.col('Expense Reserves').is_null()))
                                            .with_columns(pl.col(['Indemnity Paid','Indemnity Reserves','Medical Paid','Medical Reserves','Expense Paid','Expense Reserves']).cast(pl.Float64).round(0).cast(pl.Int64))
                                            .with_columns(pl.col(['Indemnity Paid','Indemnity Reserves','Medical Paid','Medical Reserves','Expense Paid','Expense Reserves']).fill_null(0)))
                                            
            collist=list(set(result[key].columns).intersection(set(result['Losses'].columns)))
            result[key]=result[key].select(collist)
            result['Losses']=pl.concat([result['Losses'],result[key]],how='diagonal')
            result[key]=None 

            
    for key in keys:
        MYLOGGER.debug(key)
        CleanStepsByKey(key)
        if result["error"] != "":
            return result
        
    return result

def createPreppedSpecs(specdict):
    global LOSSFOLDER
    MYLOGGER.debug('Start createPreppedSpecs')
    result=specdict
    keys=list(specdict.keys())

    def CleanStepsByKey(key):
        MYLOGGER.debug("Starting clean specs for " + key)
        if key == "Layers":
            # Specifies order that layers need to be run to properly handle inuring
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
            result[key] = result[key].with_columns(pl.lit(-1).alias("Level"))
            currlevel = 0
            assigned = []

            while (
                currlevel
                <= 10
            ) and result[key].filter(pl.col("Level") == -1).shape[0] > 0:
                result[key] = result[key].with_columns(
                    pl.struct(
                        [
                            "Level",
                            "Underlying Layers",
                            "Inuring Layers",
                        ]
                    )
                    .map_elements(
                        lambda x: getLevel(
                            currlevel,
                            x["Level"],
                            x["Underlying Layers"],
                            x["Inuring Layers"],
                        ),
                        skip_nulls=False,
                    )
                    .alias("Level")
                )
                assigned = result[key].filter(pl.col("Level") >= 0).get_column("Layer").to_list()
                currlevel = currlevel + 1

            # Derive Subject Risk Source IDs
            result[key] = (result[key]
                                        .with_columns(
                                            pl.col("Risk Source Group")
                                            .map_elements(
                                                lambda x: result["Risk Source Groups"]
                                                .filter(
                                                    (pl.col("Risk Source Group") == x)
                                                )
                                                .get_column("Risk Source")
                                                .to_list()
                                            )
                                            .alias("Risk Sources")
                                        )
                                        .with_columns(pl.when(pl.col('ECOXPL Treatment')=='Pro Rata with Layer Limit')
                                        .then(pl.lit(None))
                                        .otherwise(pl.col('Limit Including ECOXPL').fill_null(pl.col('Per Claim Limit')))
                                        .alias('Limit Including ECOXPL'))
                                        .drop(['Aggregate Limit','Aggregate Retention']))         
        
        elif key=="Losses":
            if result["Stacking and Sharing"].shape[0]>0:
                result['Losses']=(result['Losses']
                                        .join(result["Stacking and Sharing"],how='left',on='Claim Number')
                                        .with_columns(pl.col('Shared Limits ID').fill_null(pl.col('Claim Number')))
                                        .with_columns(pl.col('Stacked and Shared Limits ID').fill_null(pl.col('Shared Limits ID'))))
            else:
                result['Losses']=(result['Losses']
                                        .with_columns([pl.col('Claim Number').alias('Shared Limits ID'),
                                                        pl.col('Claim Number').alias('Stacked and Shared Limits ID')]))

            if result["Events"].shape[0]>0:
                result['Losses']=(result['Losses']
                                        .join(result["Events"],how='left',on='Claim Number')
                                        .with_columns(pl.col('Occurrence Number').fill_null(pl.col('Stacked and Shared Limits ID'))))
            else:
                result['Losses']=(result['Losses']
                                        .with_columns(pl.col('Stacked and Shared Limits ID').alias('Occurrence Number')))

            result['Losses']=(result['Losses']
                                    .drop('Claim Number')
                                    .rename({"Shared Limits ID": "Share","Stacked and Shared Limits ID": "Claim Number"}))

            #Create claim info dataframe
            #May be multiple rows for Claim Number due to multiple rows in data or due to stacking and sharing
            #Fill in default values for missing Coverage Types, Policy Limits, and Event Types and for 0 values for Policy Limits
            #Derive coverage loss date based on Coverage Type
            result['Claim Info']=(result['Losses']
                                            .with_columns(pl.col('Evaluation Date').max().over('Claim Number').alias('LatestVal'))
                                            .filter(pl.col('Evaluation Date')==pl.col('LatestVal'))
                                            .select(pl.col(["Occurrence Number","Claim Number","Share","Coverage Type","Risk Source",
                                                            "Other Info 1","Other Info 2","Other Info 3","Date of Loss","Report Date",
                                                            "Policy Effective Date","Custom A Year","Custom B Year","Insured","State","Policy Limit",
                                                            "Defense Outside Limit","Deductible","Loss Data Gross or Net of Deductible",
                                                            "Deductible Application","Deductible Erodes Policy Limit"]))
                                            .join(result["Risk Sources"]
                                                    .select(['Risk Source','Default Coverage Type','Default Policy Limit','Event Types','Trend Year Type']),
                                                    how='left',on='Risk Source')
                                            .with_columns([pl.col('Coverage Type').fill_null(pl.col('Default Coverage Type')),
                                                            pl.col('Policy Limit').fill_null(pl.col('Default Policy Limit'))])
                                            .with_columns([pl.when(pl.col('Policy Limit')==0)
                                                            .then(pl.col('Default Policy Limit'))
                                                            .otherwise(pl.col('Policy Limit'))
                                                            .alias('Policy Limit'),
                                                            pl.when(pl.col('Coverage Type').str.to_uppercase()=='CM')
                                                            .then(pl.col('Report Date'))
                                                            .when(pl.col('Coverage Type').str.to_uppercase()=='OCC')
                                                            .then(pl.col('Date of Loss'))
                                                            .otherwise(pl.col('Policy Effective Date'))
                                                            .alias('Coverage Loss Date')])
                                            .drop(['Report Date','Date of Loss','Default Coverage Type','Default Policy Limit'])
                                            .with_columns([pl.col('Coverage Loss Date').dt.year().cast(pl.Int64).alias('Loss Year'),
                                                            pl.col('Policy Effective Date').dt.year().cast(pl.Int64).alias('Policy Year')])
                                            .drop(['Coverage Loss Date','Policy Effective Date'])                                                
                                            .with_columns([pl.when(pl.col('Event Types').str.to_uppercase()=='CLASH')
                                                            .then(pl.when(result['Assumptions'].filter(pl.col('Field')=='Dates').get_column('Option')[0]=="Max")
                                                                        .then(pl.col('Loss Year').max().over('Occurrence Number'))
                                                                        .otherwise(pl.col('Loss Year').min().over('Occurrence Number')))
                                                            .otherwise(pl.col('Loss Year'))
                                                            .alias('Event Loss Year'),
                                                            pl.when(pl.col('Event Types').str.to_uppercase()=='CLASH')
                                                            .then(pl.when(result['Assumptions'].filter(pl.col('Field')=='Dates').get_column('Option')[0]=="Max")
                                                                        .then(pl.col('Policy Year').max().over('Occurrence Number'))
                                                                        .otherwise(pl.col('Policy Year').min().over('Occurrence Number')))
                                                            .otherwise(pl.col('Policy Year'))
                                                            .alias('Event Policy Year'),
                                                            pl.when(pl.col('Event Types').str.to_uppercase()=='CLASH')
                                                            .then(pl.when(result['Assumptions'].filter(pl.col('Field')=='Dates').get_column('Option')[0]=="Max")
                                                                        .then(pl.col('Custom A Year').max().over('Occurrence Number'))
                                                                        .otherwise(pl.col('Custom A Year').min().over('Occurrence Number')))
                                                            .otherwise(pl.col('Custom A Year'))
                                                            .alias('Event Custom A Year'),
                                                            pl.when(pl.col('Event Types').str.to_uppercase()=='CLASH')
                                                            .then(pl.when(result['Assumptions'].filter(pl.col('Field')=='Dates').get_column('Option')[0]=="Max")
                                                                        .then(pl.col('Custom B Year').max().over('Occurrence Number'))
                                                                        .otherwise(pl.col('Custom B Year').min().over('Occurrence Number')))
                                                            .otherwise(pl.col('Custom B Year'))
                                                            .alias('Event Custom B Year')])
                                            .with_columns([pl.first("Occurrence Number").over('Claim Number'),
                                                            pl.first("Coverage Type").over('Share'),
                                                            pl.first("Risk Source").over('Claim Number'),
                                                            pl.first("Other Info 1").over('Share'),
                                                            pl.first("Other Info 2").over('Share'),
                                                            pl.first("Other Info 3").over('Share'),
                                                            pl.when(result['Assumptions'].filter(pl.col('Field')=='Dates').get_column('Option')[0]=="Max")
                                                            .then(pl.max_horizontal("Loss Year").over('Claim Number'))
                                                            .otherwise(pl.min_horizontal("Loss Year").over('Claim Number')),
                                                            pl.when(result['Assumptions'].filter(pl.col('Field')=='Dates').get_column('Option')[0]=="Max")
                                                            .then(pl.max_horizontal("Policy Year").over('Claim Number'))
                                                            .otherwise(pl.min_horizontal("Policy Year").over('Claim Number')),
                                                            pl.when(result['Assumptions'].filter(pl.col('Field')=='Dates').get_column('Option')[0]=="Max")
                                                            .then(pl.max_horizontal("Custom A Year").over('Claim Number'))
                                                            .otherwise(pl.min_horizontal("Custom A Year").over('Claim Number')),
                                                            pl.when(result['Assumptions'].filter(pl.col('Field')=='Dates').get_column('Option')[0]=="Max")
                                                            .then(pl.max_horizontal("Custom B Year").over('Claim Number'))
                                                            .otherwise(pl.min_horizontal("Custom B Year").over('Claim Number')),                                                        
                                                            pl.when(result['Assumptions'].filter(pl.col('Field')=='Dates').get_column('Option')[0]=="Max")
                                                            .then(pl.max_horizontal("Event Loss Year").over('Claim Number'))
                                                            .otherwise(pl.min_horizontal("Event Loss Year").over('Claim Number')),
                                                            pl.when(result['Assumptions'].filter(pl.col('Field')=='Dates').get_column('Option')[0]=="Max")
                                                            .then(pl.max_horizontal("Event Policy Year").over('Claim Number'))
                                                            .otherwise(pl.min_horizontal("Event Policy Year").over('Claim Number')),
                                                            pl.when(result['Assumptions'].filter(pl.col('Field')=='Dates').get_column('Option')[0]=="Max")
                                                            .then(pl.max_horizontal("Event Custom A Year").over('Claim Number'))
                                                            .otherwise(pl.min_horizontal("Event Custom A Year").over('Claim Number')),
                                                            pl.when(result['Assumptions'].filter(pl.col('Field')=='Dates').get_column('Option')[0]=="Max")
                                                            .then(pl.max_horizontal("Event Custom B Year").over('Claim Number'))
                                                            .otherwise(pl.min_horizontal("Event Custom B Year").over('Claim Number')),
                                                            pl.col("Insured").unique().str.concat(', ').over('Claim Number'),
                                                            pl.first("State").over('Claim Number'),
                                                            pl.when(result['Assumptions'].filter(pl.col('Field')=='Policy Limit').get_column('Option')[0]=="Max")
                                                            .then(pl.max_horizontal("Policy Limit").over('Share'))
                                                            .otherwise(pl.min_horizontal("Policy Limit").over('Share')),                                                            
                                                            pl.first("Defense Outside Limit").over('Claim Number'),
                                                            pl.when(result['Assumptions'].filter(pl.col('Field')=='Deductible').get_column('Option')[0]=="Max")                                                            
                                                            .then(pl.max_horizontal("Deductible").over('Claim Number'))
                                                            .otherwise(pl.min_horizontal("Deductible").over('Claim Number')),
                                                            pl.first("Loss Data Gross or Net of Deductible").over('Claim Number'),
                                                            pl.first("Deductible Application").over('Claim Number'),
                                                            pl.first("Deductible Erodes Policy Limit").over('Claim Number')])
                                            .unique(subset=["Share"], keep="first",maintain_order=True)
                                            .with_columns([pl.sum('Policy Limit').over('Claim Number'),
                                                            pl.col("Coverage Type").unique().str.concat(', ').over('Claim Number'),
                                                            pl.col("Other Info 1").unique().str.concat(', ').over('Claim Number'),
                                                            pl.col("Other Info 2").unique().str.concat(', ').over('Claim Number'),
                                                            pl.col("Other Info 3").unique().str.concat(', ').over('Claim Number')])
                                            .unique(subset=["Claim Number"], keep="first",maintain_order=True)
                                            .with_columns(pl.when(pl.col('Trend Year Type')=='Policy Year')
                                                            .then(pl.col('Policy Year'))
                                                            .otherwise(pl.when(pl.col('Trend Year Type')=='Loss Year')
                                                                        .then(pl.col('Loss Year'))
                                                                        .otherwise(pl.when(pl.col('Trend Year Type')=='Custom A Year')
                                                                                .then(pl.col('Custom A Year'))
                                                                                .otherwise(pl.when(pl.col('Trend Year Type')=='Custom B Year')
                                                                                            .then(pl.col('Custom B Year')))))
                                                            .cast(pl.Int64)
                                                            .alias('Trend Year'))
                                            .drop('Trend Year Type'))

            result["Losses"]=(result['Losses'].select(['Evaluation Date','Claim Number','Indemnity Paid','Medical Paid','Expense Paid',
                                                        'Indemnity Reserves','Medical Reserves','Expense Reserves','Coverage Expense Constant'])
                                                    .group_by(['Evaluation Date','Claim Number']).sum())

            result['Prepped Losses']=calcTrendedAndUntrendedLosses(result)
            
            result['Claim Info']=result['Claim Info'].select(['Risk Source','Claim Number','Occurrence Number','Insured','State','Policy Limit','Coverage Type',
                                                                            'Other Info 1','Other Info 2','Other Info 3','Loss Year','Policy Year','Custom A Year','Custom B Year',
                                                                            'Event Loss Year','Event Policy Year','Event Custom A Year','Event Custom B Year','Trend Year',
                                                                            'Defense Outside Limit','Deductible','Loss Data Gross or Net of Deductible',
                                                                            'Deductible Application','Deductible Erodes Policy Limit'])
    
    for key in keys:
        MYLOGGER.debug(key)
        CleanStepsByKey(key)    
            
    return result

    
def calcTrendedAndUntrendedLosses(specdict):

    dfclm = specdict['Claim Info']

    dfResults=pl.DataFrame()
    dfloss = (specdict['Losses']
            .join(
                dfclm.select(
                    [
                        "Claim Number",
                        "Occurrence Number",
                        "Risk Source",
                        "Deductible",
                        "Deductible Application",
                        "Loss Data Gross or Net of Deductible",
                        "Deductible Erodes Policy Limit",
                        "Defense Outside Limit",
                        "Policy Limit",
                        "Trend Year"
                    ]
                ),
                how="left",
                on="Claim Number",
                )
            .with_columns([pl.when(pl.col('Indemnity Paid')<0)
                            .then(pl.lit(0))
                            .otherwise(pl.col('Indemnity Paid'))
                            .alias('Indemnity Paid'),
                            pl.when(pl.col('Medical Paid')<0)
                            .then(pl.lit(0))
                            .otherwise(pl.col('Medical Paid'))
                            .alias('Medical Paid'),
                            pl.when(pl.col('Expense Paid')<0)
                            .then(pl.lit(0))
                            .otherwise(pl.col('Expense Paid'))
                            .alias('Expense Paid'),
                            pl.when(pl.col('Indemnity Reserves')<0)
                            .then(pl.lit(0))
                            .otherwise(pl.col('Indemnity Reserves'))
                            .alias('Indemnity Reserves'),
                            pl.when(pl.col('Medical Reserves')<0)
                            .then(pl.lit(0))
                            .otherwise(pl.col('Medical Reserves'))
                            .alias('Medical Reserves'),
                            pl.when(pl.col('Expense Reserves')<0)
                            .then(pl.lit(0))
                            .otherwise(pl.col('Expense Reserves'))
                            .alias('Expense Reserves')])                              
            .with_columns([
                (pl.col('Indemnity Paid')+pl.col('Medical Paid')).alias('PdLoss').cast(pl.Int64),
                (pl.col('Indemnity Paid')+pl.col('Medical Paid')+pl.col('Indemnity Reserves')+pl.col('Medical Reserves')).alias('IncLoss').cast(pl.Int64)])
            .with_columns([
                (pl.col('PdLoss')+pl.col('Expense Paid')).alias('PdLALAE').cast(pl.Int64),
                (pl.col('IncLoss')+pl.col('Expense Paid')+pl.col('Expense Reserves')).alias('IncLALAE').cast(pl.Int64),
                (pl.col('Expense Paid')+pl.col('Expense Reserves')).alias('IncExp').cast(pl.Int64)])
            .with_columns([
                (pl.col('IncLALAE')-pl.col('PdLALAE')).alias('ResLALAE').cast(pl.Int64),
                (pl.col('IncLoss')-pl.col('PdLoss')).alias('ResLoss').cast(pl.Int64)])
            .with_columns(
                pl.when(pl.col('Deductible Application')=='Loss Only')
                .then(pl.when(pl.col('Loss Data Gross or Net of Deductible')=='Gross')
                        .then(pl.lit(1))
                        .otherwise(pl.lit(10)))
                .when(pl.col('Deductible Application')=='Expense Only')
                .then(pl.when(pl.col('Loss Data Gross or Net of Deductible')=='Gross')
                    .then(pl.lit(2))
                    .otherwise(pl.lit(20)))
                .when(pl.col('Loss Data Gross or Net of Deductible')=='Gross')
                .then(pl.when(pl.col('Deductible Application')=='Expense First')
                    .then(pl.lit(3))
                    .when(pl.col('Deductible Application')=='Loss First')
                    .then(pl.lit(4))                        
                    .otherwise(pl.lit(5)))
                .otherwise(pl.lit(6))
                .alias('Group'))
            .partition_by('Group',as_dict=True))
    
    for key in dfloss.keys():
        if key==(1,):
            #Grp 1 - Gross, Ded applies to Loss Only
            dfResults=pl.concat([dfResults,
                                    dfloss[key]
                                    .with_columns([
                                        pl.col('Indemnity Paid').alias('Gross Indemnity Paid'),
                                        (pl.col('Indemnity Paid') + pl.col('Indemnity Reserves')).alias('Gross Indemnity Incurred'),
                                        pl.col('Medical Paid').alias('Gross Medical Paid'),
                                        (pl.col('Medical Paid') + pl.col('Medical Reserves')).alias('Gross Medical Incurred'),   
                                        pl.col('Expense Paid').alias('Gross Expense Paid'),
                                        (pl.col('Expense Paid') + pl.col('Expense Reserves')).alias('Gross Expense Incurred'),
                                        pl.min_horizontal(['PdLoss','Deductible']).alias('Ded Pd Loss').cast(pl.Int64),
                                        pl.min_horizontal(['IncLoss','Deductible']).alias('Ded Inc Loss').cast(pl.Int64)])
                                    .with_columns(pl.when(pl.col('Ded Pd Loss')>0)
                                                    .then((pl.col('Ded Pd Loss')*(pl.col('Medical Paid')/pl.col('PdLoss'))).round(0))
                                                    .otherwise(pl.lit(0))
                                                    .alias('Ded Pd Med Loss')
                                                    .cast(pl.Int64))
                                    .with_columns(pl.when(pl.col('Ded Pd Loss')>0)
                                                    .then(pl.col('Ded Pd Loss')-pl.col('Ded Pd Med Loss'))
                                                    .otherwise(pl.lit(0))
                                                    .alias('Ded Pd Ind Loss')
                                                    .cast(pl.Int64))
                                    .with_columns(pl.when(pl.col('Ded Pd Loss')!=pl.col('Ded Inc Loss'))
                                                    .then(((pl.col('Ded Inc Loss')-pl.col('Ded Pd Loss'))*(pl.col('Medical Reserves')/(pl.col('Medical Reserves')+pl.col('Indemnity Reserves')))).round(0))
                                                    .otherwise(pl.lit(0))
                                                    .alias('Ded Res Med Loss')
                                                    .cast(pl.Int64))
                                    .with_columns(pl.when(pl.col('Ded Pd Loss')!=pl.col('Ded Inc Loss'))
                                                    .then(pl.col('Ded Inc Loss')-pl.col('Ded Pd Loss')-pl.col('Ded Res Med Loss'))
                                                    .otherwise(pl.lit(0))
                                                    .alias('Ded Res Ind Loss')
                                                    .cast(pl.Int64))
                                    .with_columns([
                                        (pl.col('Gross Indemnity Paid')-pl.col('Ded Pd Ind Loss')).cast(pl.Int64).alias('Net Indemnity Paid'),
                                        (pl.col('Gross Indemnity Incurred')-pl.col('Ded Pd Ind Loss')-pl.col('Ded Res Ind Loss')).cast(pl.Int64).alias('Net Indemnity Incurred'),
                                        (pl.col('Gross Medical Paid')-pl.col('Ded Pd Med Loss')).cast(pl.Int64).alias('Net Medical Paid'),
                                        (pl.col('Gross Medical Incurred')-pl.col('Ded Pd Med Loss')-pl.col('Ded Res Med Loss')).cast(pl.Int64).alias('Net Medical Incurred'),  
                                        pl.col('Gross Expense Paid').cast(pl.Int64).alias('Net Expense Paid'),
                                        pl.col('Gross Expense Incurred').cast(pl.Int64).alias('Net Expense Incurred')])])
        elif key==(10,): 
            #Grp 10 - Net, Ded applies to Loss Only                                                                                 
            dfResults=pl.concat([dfResults,
                                    dfloss[key]
                                    .with_columns([
                                        pl.col('Indemnity Paid').cast(pl.Int64).alias('Net Indemnity Paid'),
                                        (pl.col('Indemnity Paid') + pl.col('Indemnity Reserves')).cast(pl.Int64).alias('Net Indemnity Incurred'),
                                        pl.col('Medical Paid').cast(pl.Int64).alias('Net Medical Paid'),
                                        (pl.col('Medical Paid') + pl.col('Medical Reserves')).cast(pl.Int64).alias('Net Medical Incurred'),   
                                        pl.col('Expense Paid').cast(pl.Int64).alias('Net Expense Paid'),
                                        (pl.col('Expense Paid') + pl.col('Expense Reserves')).cast(pl.Int64).alias('Net Expense Incurred'),
                                        pl.when(pl.col('PdLoss')>0)
                                        .then(pl.col('Deductible'))
                                        .otherwise(pl.lit(0))
                                        .cast(pl.Int64)
                                        .alias('Ded Pd Loss'),
                                        pl.when(pl.col('IncLoss')>0)
                                        .then(pl.col('Deductible'))
                                        .otherwise(pl.lit(0))
                                        .cast(pl.Int64)
                                        .alias('Ded Inc Loss')])   
                                    .with_columns(pl.when(pl.col('PdLoss')>0)
                                                    .then(pl.col('Deductible')*(pl.col('Medical Paid')/pl.col('PdLoss')))
                                                    .otherwise(pl.lit(0))
                                                    .round(0)
                                                    .cast(pl.Int64)
                                                    .alias('Ded Pd Med Loss'))
                                    .with_columns(pl.when(pl.col('PdLoss')>0)
                                                    .then(pl.col('Deductible')-pl.col('Ded Pd Med Loss'))
                                                    .otherwise(pl.lit(0))
                                                    .cast(pl.Int64)
                                                    .alias('Ded Pd Ind Loss'))
                                    .with_columns(pl.when(pl.col('PdLoss')==0)
                                                    .then(pl.when(pl.col('IncLoss')>0)
                                                    .then((pl.col('Deductible')*(pl.col('Medical Reserves')/pl.col('IncLoss'))).round(0))
                                                    .otherwise(pl.lit(0)))
                                                    .otherwise(pl.lit(0))
                                                    .cast(pl.Int64)
                                                    .alias('Ded Res Med Loss'))
                                    .with_columns(pl.when(pl.col('Ded Pd Loss')!=pl.col('Ded Inc Loss'))
                                                    .then(pl.col('Ded Inc Loss')-pl.col('Ded Pd Loss')-pl.col('Ded Res Med Loss'))
                                                    .otherwise(pl.lit(0))
                                                    .cast(pl.Int64)
                                                    .alias('Ded Res Ind Loss'))                                                                         
                                    .with_columns([
                                        (pl.col('Net Indemnity Paid')+pl.col('Ded Pd Ind Loss')).cast(pl.Int64).alias('Gross Indemnity Paid'),
                                        (pl.col('Net Indemnity Incurred')+pl.col('Ded Pd Ind Loss')+pl.col('Ded Res Ind Loss')).cast(pl.Int64).alias('Gross Indemnity Incurred'),
                                        (pl.col('Net Medical Paid')+pl.col('Ded Pd Med Loss')).cast(pl.Int64).alias('Gross Medical Paid'),
                                        (pl.col('Net Medical Incurred')+pl.col('Ded Pd Med Loss')+pl.col('Ded Res Med Loss')).cast(pl.Int64).alias('Gross Medical Incurred'),  
                                        pl.col('Net Expense Paid').cast(pl.Int64).alias('Gross Expense Paid'),
                                        pl.col('Net Expense Incurred').cast(pl.Int64).alias('Gross Expense Incurred')])])
        elif key==(2,):
            #Grp 2 - Gross, Ded applies to Expense Only
            dfResults=pl.concat([dfResults,
                                    dfloss[key]
                                    .with_columns([
                                        pl.col('Indemnity Paid').cast(pl.Int64).alias('Gross Indemnity Paid'),
                                        (pl.col('Indemnity Paid') + pl.col('Indemnity Reserves')).cast(pl.Int64).alias('Gross Indemnity Incurred'),
                                        pl.col('Medical Paid').cast(pl.Int64).alias('Gross Medical Paid'),
                                        (pl.col('Medical Paid') + pl.col('Medical Reserves')).cast(pl.Int64).alias('Gross Medical Incurred'),   
                                        pl.col('Expense Paid').cast(pl.Int64).alias('Gross Expense Paid'),
                                        (pl.col('Expense Paid') + pl.col('Expense Reserves')).cast(pl.Int64).alias('Gross Expense Incurred'),
                                        pl.min_horizontal(['Expense Paid','Deductible']).cast(pl.Int64).alias('Ded Pd Exp'),
                                        pl.min_horizontal(['IncExp','Deductible']).cast(pl.Int64).alias('Ded Inc Exp')])
                                    .with_columns([
                                        (pl.col('Gross Expense Paid')-pl.col('Ded Pd Exp')).cast(pl.Int64).alias('Net Expense Paid'),
                                        (pl.col('Gross Expense Incurred')-pl.col('Ded Inc Exp')).cast(pl.Int64).alias('Net Expense Incurred'),
                                        (pl.col('Gross Medical Paid')).cast(pl.Int64).alias('Net Medical Paid'),
                                        (pl.col('Gross Medical Incurred')).cast(pl.Int64).alias('Net Medical Incurred'),  
                                        pl.col('Gross Indemnity Paid').cast(pl.Int64).alias('Net Indemnity Paid'),
                                        pl.col('Gross Indemnity Incurred').cast(pl.Int64).alias('Net Indemnity Incurred')])],how='diagonal')
        elif key==(20,):
            #Grp 20 - Net, Ded applies to Expense Only                                                                                
            dfResults=pl.concat([dfResults,
                                    dfloss[key]
                                    .with_columns([
                                        pl.col('Indemnity Paid').cast(pl.Int64).alias('Net Indemnity Paid'),
                                        (pl.col('Indemnity Paid') + pl.col('Indemnity Reserves')).cast(pl.Int64).alias('Net Indemnity Incurred'),
                                        pl.col('Medical Paid').cast(pl.Int64).alias('Net Medical Paid'),
                                        (pl.col('Medical Paid') + pl.col('Medical Reserves')).cast(pl.Int64).alias('Net Medical Incurred'),   
                                        pl.col('Expense Paid').cast(pl.Int64).alias('Net Expense Paid'),
                                        (pl.col('Expense Paid') + pl.col('Expense Reserves')).cast(pl.Int64).alias('Net Expense Incurred'),
                                        pl.when(pl.col('Expense Paid')>0)
                                        .then(pl.col('Deductible'))
                                        .otherwise(pl.lit(0))
                                        .cast(pl.Int64)
                                        .alias('Ded Pd Exp'),
                                        pl.when(pl.col('IncExp')>0)
                                        .then(pl.col('Deductible'))
                                        .otherwise(pl.lit(0))
                                        .cast(pl.Int64)
                                        .alias('Ded Inc Exp')])                                
                                    .with_columns([
                                        (pl.col('Net Expense Paid')+pl.col('Ded Pd Exp')).cast(pl.Int64).alias('Gross Expense Paid'),
                                        (pl.col('Net Expense Incurred')+pl.col('Ded Inc Exp')).cast(pl.Int64).alias('Gross Expense Incurred'),
                                        (pl.col('Net Medical Paid')).cast(pl.Int64).alias('Gross Medical Paid'),
                                        (pl.col('Net Medical Incurred')).cast(pl.Int64).alias('Gross Medical Incurred'),  
                                        pl.col('Net Indemnity Paid').cast(pl.Int64).alias('Gross Indemnity Paid'),
                                        pl.col('Net Indemnity Incurred').cast(pl.Int64).alias('Gross Indemnity Incurred')])])
        elif key==(3,):
        #Grp 3 - Gross, Ded applies to Expense First
            dfResults=pl.concat([dfResults,
                                    dfloss[key]
                                    .with_columns([
                                        pl.col('Indemnity Paid').cast(pl.Int64).alias('Gross Indemnity Paid'),
                                        (pl.col('Indemnity Paid') + pl.col('Indemnity Reserves')).cast(pl.Int64).alias('Gross Indemnity Incurred'),
                                        pl.col('Medical Paid').cast(pl.Int64).alias('Gross Medical Paid'),
                                        (pl.col('Medical Paid') + pl.col('Medical Reserves')).cast(pl.Int64).alias('Gross Medical Incurred'),   
                                        pl.col('Expense Paid').cast(pl.Int64).alias('Gross Expense Paid'),
                                        (pl.col('Expense Paid') + pl.col('Expense Reserves')).cast(pl.Int64).alias('Gross Expense Incurred'),
                                        pl.min_horizontal(['Expense Paid','Deductible']).cast(pl.Int64).alias('Ded Pd Exp')])
                                    .with_columns((pl.min_horizontal(['PdLALAE','Deductible'])-pl.col('Ded Pd Exp')).cast(pl.Int64).alias('Ded Pd Loss'))
                                    .with_columns(pl.when(pl.col('Ded Pd Loss')>0)
                                                    .then((pl.col('Ded Pd Loss')*(pl.col('Medical Paid')/pl.col('PdLoss'))).round(0))
                                                    .otherwise(pl.lit(0))
                                                    .cast(pl.Int64)
                                                    .alias('Ded Pd Med Loss'))
                                    .with_columns(pl.when(pl.col('Ded Pd Loss')>0)
                                                    .then(pl.col('Ded Pd Loss')-pl.col('Ded Pd Med Loss'))
                                                    .otherwise(pl.lit(0))
                                                    .cast(pl.Int64)
                                                    .alias('Ded Pd Ind Loss'))
                                    .with_columns((pl.col('Deductible')-pl.col('Ded Pd Loss')-pl.col('Ded Pd Exp')).cast(pl.Int64).alias('Ded Remaining'))
                                    .with_columns(pl.min_horizontal(['Expense Reserves','Ded Remaining']).cast(pl.Int64).alias('Ded Res Exp'))
                                    .with_columns((pl.min_horizontal('ResLALAE','Ded Remaining')-pl.col('Ded Res Exp')).cast(pl.Int64).alias('Ded Res Loss'))
                                    .with_columns(pl.when(pl.col('Ded Res Loss')>0)
                                                    .then((pl.col('Ded Res Loss')*(pl.col('Medical Reserves')/(pl.col('Medical Reserves')+pl.col('Indemnity Reserves')))).round(0))
                                                    .otherwise(pl.lit(0))
                                                    .cast(pl.Int64)
                                                    .alias('Ded Res Med Loss'))
                                    .with_columns(pl.when(pl.col('Ded Res Loss')>0)
                                                    .then(pl.col('Ded Res Loss')-pl.col('Ded Res Med Loss'))
                                                    .otherwise(pl.lit(0))
                                                    .cast(pl.Int64)
                                                    .alias('Ded Res Ind Loss'))                                        
                                    .with_columns([
                                        (pl.col('Gross Expense Paid')-pl.col('Ded Pd Exp')).cast(pl.Int64).alias('Net Expense Paid'),
                                        (pl.col('Gross Expense Incurred')-pl.col('Ded Pd Exp')-pl.col('Ded Res Exp')).cast(pl.Int64).alias('Net Expense Incurred'),
                                        (pl.col('Gross Medical Paid')-pl.col('Ded Pd Med Loss')).cast(pl.Int64).alias('Net Medical Paid'),
                                        (pl.col('Gross Medical Incurred')-pl.col('Ded Pd Med Loss')-pl.col('Ded Res Med Loss')).cast(pl.Int64).alias('Net Medical Incurred'),  
                                        (pl.col('Gross Indemnity Paid')-pl.col('Ded Pd Ind Loss')).cast(pl.Int64).alias('Net Indemnity Paid'),
                                        (pl.col('Gross Indemnity Incurred')-pl.col('Ded Pd Ind Loss')-pl.col('Ded Res Ind Loss')).cast(pl.Int64).alias('Net Indemnity Incurred')])],how='diagonal')
        elif key==(4,):
        #Grp 4 - Gross, Ded applies to Loss First
            dfResults=pl.concat([dfResults,
                                    dfloss[key]
                                    .with_columns([
                                        pl.col('Indemnity Paid').cast(pl.Int64).alias('Gross Indemnity Paid'),
                                        (pl.col('Indemnity Paid') + pl.col('Indemnity Reserves')).cast(pl.Int64).alias('Gross Indemnity Incurred'),
                                        pl.col('Medical Paid').cast(pl.Int64).alias('Gross Medical Paid'),
                                        (pl.col('Medical Paid') + pl.col('Medical Reserves')).cast(pl.Int64).alias('Gross Medical Incurred'),   
                                        pl.col('Expense Paid').cast(pl.Int64).alias('Gross Expense Paid'),
                                        (pl.col('Expense Paid') + pl.col('Expense Reserves')).cast(pl.Int64).alias('Gross Expense Incurred'),
                                        pl.min_horizontal(['PdLoss','Deductible']).cast(pl.Int64).alias('Ded Pd Loss')])
                                    .with_columns((pl.min_horizontal(['PdLALAE','Deductible'])-pl.col('Ded Pd Loss')).cast(pl.Int64).alias('Ded Pd Exp'))
                                    .with_columns((pl.col('Deductible')-pl.col('Ded Pd Exp')-pl.col('Ded Pd Loss')).cast(pl.Int64).alias('Ded Remaining'))
                                    .with_columns(pl.min_horizontal(['ResLoss','Ded Remaining']).cast(pl.Int64).alias('Ded Res Loss'))
                                    .with_columns((pl.min_horizontal('ResLALAE','Ded Remaining')-pl.col('Ded Res Loss')).cast(pl.Int64).alias('Ded Res Exp'))
                                    .with_columns(pl.when(pl.col('Ded Pd Loss')>0)
                                                    .then((pl.col('Ded Pd Loss')*(pl.col('Medical Paid')/pl.col('PdLoss'))).round(0))
                                                    .otherwise(pl.lit(0))
                                                    .cast(pl.Int64)
                                                    .alias('Ded Pd Med Loss'))
                                    .with_columns(pl.when(pl.col('Ded Pd Loss')>0)
                                                    .then(pl.col('Ded Pd Loss')-pl.col('Ded Pd Med Loss'))
                                                    .otherwise(pl.lit(0))
                                                    .cast(pl.Int64)
                                                    .alias('Ded Pd Ind Loss'))                                        
                                    .with_columns(pl.when(pl.col('Ded Res Loss')>0)
                                                    .then((pl.col('Ded Res Loss')*(pl.col('Medical Reserves')/(pl.col('Medical Reserves')+pl.col('Indemnity Reserves')))).round(0))
                                                    .otherwise(pl.lit(0))
                                                    .cast(pl.Int64)
                                                    .alias('Ded Res Med Loss'))
                                    .with_columns(pl.when(pl.col('Ded Res Loss')>0)
                                                    .then(pl.col('Ded Res Loss')-pl.col('Ded Res Med Loss'))
                                                    .otherwise(pl.lit(0))
                                                    .cast(pl.Int64)
                                                    .alias('Ded Res Ind Loss'))                                                       
                                    .with_columns([
                                        (pl.col('Gross Expense Paid')-pl.col('Ded Pd Exp')).cast(pl.Int64).alias('Net Expense Paid'),
                                        (pl.col('Gross Expense Incurred')-pl.col('Ded Pd Exp')-pl.col('Ded Res Exp')).cast(pl.Int64).alias('Net Expense Incurred'),
                                        (pl.col('Gross Medical Paid')-pl.col('Ded Pd Med Loss')).cast(pl.Int64).alias('Net Medical Paid'),
                                        (pl.col('Gross Medical Incurred')-pl.col('Ded Pd Med Loss')-pl.col('Ded Res Med Loss')).cast(pl.Int64).alias('Net Medical Incurred'),  
                                        (pl.col('Gross Indemnity Paid')-pl.col('Ded Pd Ind Loss')).cast(pl.Int64).alias('Net Indemnity Paid'),
                                        (pl.col('Gross Indemnity Incurred')-pl.col('Ded Pd Ind Loss')-pl.col('Ded Res Ind Loss')).cast(pl.Int64).alias('Net Indemnity Incurred')])],how='diagonal')
        elif key==(5,):
        #Grp 5 - Gross, Ded applies pro rata to loss and expenset
            dfResults=pl.concat([dfResults,
                                    dfloss[key]
                                    .with_columns([
                                        pl.col('Indemnity Paid').cast(pl.Int64).alias('Gross Indemnity Paid'),
                                        (pl.col('Indemnity Paid') + pl.col('Indemnity Reserves')).cast(pl.Int64).alias('Gross Indemnity Incurred'),
                                        pl.col('Medical Paid').cast(pl.Int64).alias('Gross Medical Paid'),
                                        (pl.col('Medical Paid') + pl.col('Medical Reserves')).cast(pl.Int64).alias('Gross Medical Incurred'),   
                                        pl.col('Expense Paid').cast(pl.Int64).alias('Gross Expense Paid'),
                                        (pl.col('Expense Paid') + pl.col('Expense Reserves')).cast(pl.Int64).alias('Gross Expense Incurred'),
                                        pl.when(pl.col('PdLALAE')>0)
                                        .then((pl.col('Expense Paid')/pl.col('PdLALAE')))
                                        .otherwise(pl.lit(0.0))
                                        .cast(pl.Float64)
                                        .alias('PdExpToLALAE'),
                                        pl.when(pl.col('ResLALAE')>0)
                                        .then((pl.col('Expense Reserves')/pl.col('ResLALAE')))
                                        .otherwise(pl.lit(0.0))
                                        .cast(pl.Float64)
                                        .alias('ResExpToLALAE')])
                                    .with_columns((pl.min_horizontal(['PdLALAE','Deductible'])*pl.col('PdExpToLALAE')).round(0).cast(pl.Int64).alias('Ded Pd Exp'))
                                    .with_columns((pl.min_horizontal(['PdLALAE','Deductible'])-pl.col('Ded Pd Exp')).cast(pl.Int64).alias('Ded Pd Loss'))                                        
                                    .with_columns((pl.col('Deductible')-pl.col('Ded Pd Exp')-pl.col('Ded Pd Loss')).cast(pl.Int64).alias('Ded Remaining'))
                                    .with_columns((pl.min_horizontal(['ResLALAE','Ded Remaining'])*pl.col('ResExpToLALAE')).round(0).cast(pl.Int64).alias('Ded Res Exp'))   
                                    .with_columns((pl.min_horizontal(['ResLALAE','Ded Remaining'])-pl.col('Ded Res Exp')).cast(pl.Int64).alias('Ded Res Loss'))
                                    .with_columns(pl.when(pl.col('Ded Pd Loss')>0)
                                                    .then((pl.col('Ded Pd Loss')*(pl.col('Medical Paid')/pl.col('PdLoss'))).round(0))
                                                    .otherwise(pl.lit(0))
                                                    .cast(pl.Int64)
                                                    .alias('Ded Pd Med Loss'))
                                    .with_columns(pl.when(pl.col('Ded Pd Loss')>0)
                                                    .then(pl.col('Ded Pd Loss')-pl.col('Ded Pd Med Loss'))
                                                    .otherwise(pl.lit(0))
                                                    .cast(pl.Int64)
                                                    .alias('Ded Pd Ind Loss'))                                        
                                    .with_columns(pl.when(pl.col('Ded Res Loss')>0)
                                                    .then((pl.col('Ded Res Loss')*(pl.col('Medical Reserves')/(pl.col('Medical Reserves')+pl.col('Indemnity Reserves')))).round(0))
                                                    .otherwise(pl.lit(0))
                                                    .cast(pl.Int64)
                                                    .alias('Ded Res Med Loss'))
                                    .with_columns(pl.when(pl.col('Ded Res Loss')>0)
                                                    .then(pl.col('Ded Res Loss')-pl.col('Ded Res Med Loss'))
                                                    .otherwise(pl.lit(0))
                                                    .cast(pl.Int64)
                                                    .alias('Ded Res Ind Loss'))                                                                                                                         
                                    .with_columns([
                                        (pl.col('Gross Expense Paid')-pl.col('Ded Pd Exp')).cast(pl.Int64).alias('Net Expense Paid'),
                                        (pl.col('Gross Expense Incurred')-pl.col('Ded Pd Exp')-pl.col('Ded Res Exp')).cast(pl.Int64).alias('Net Expense Incurred'),
                                        (pl.col('Gross Medical Paid')-pl.col('Ded Pd Med Loss')).cast(pl.Int64).alias('Net Medical Paid'),
                                        (pl.col('Gross Medical Incurred')-pl.col('Ded Pd Med Loss')-pl.col('Ded Res Med Loss')).cast(pl.Int64).alias('Net Medical Incurred'),  
                                        (pl.col('Gross Indemnity Paid')-pl.col('Ded Pd Ind Loss')).cast(pl.Int64).alias('Net Indemnity Paid'),
                                        (pl.col('Gross Indemnity Incurred')-pl.col('Ded Pd Ind Loss')-pl.col('Ded Res Ind Loss')).cast(pl.Int64).alias('Net Indemnity Incurred')])])
        elif key==(6,):
            #Grp 6 - Net, Ded applies pro rata to loss and expense (or expense first or loss first)                                                                                 
            dfResults=pl.concat([dfResults,
                                    dfloss[key]
                                    .with_columns([
                                        pl.col('Indemnity Paid').cast(pl.Int64).alias('Net Indemnity Paid'),
                                        (pl.col('Indemnity Paid') + pl.col('Indemnity Reserves')).cast(pl.Int64).alias('Net Indemnity Incurred'),
                                        pl.col('Medical Paid').cast(pl.Int64).alias('Net Medical Paid'),
                                        (pl.col('Medical Paid') + pl.col('Medical Reserves')).cast(pl.Int64).alias('Net Medical Incurred'),   
                                        pl.col('Expense Paid').cast(pl.Int64).alias('Net Expense Paid'),
                                        (pl.col('Expense Paid') + pl.col('Expense Reserves')).cast(pl.Int64).alias('Net Expense Incurred'),
                                        pl.when(pl.col('PdLALAE')>0)
                                        .then((pl.col('Expense Paid')/pl.col('PdLALAE')))
                                        .otherwise(pl.lit(0.0))
                                        .cast(pl.Float64)
                                        .alias('PdExpToLALAE'),
                                        pl.when(pl.col('ResLALAE')>0)
                                        .then((pl.col('Expense Reserves')/pl.col('ResLALAE')))
                                        .otherwise(pl.lit(0.0))
                                        .cast(pl.Float64)
                                        .alias('ResExpToLALAE')])
                                    .with_columns(
                                        pl.when(pl.col('PdLALAE')>0)
                                        .then((pl.col('Deductible')*pl.col('PdExpToLALAE')).round(0))
                                        .otherwise(pl.lit(0.0))
                                        .cast(pl.Int64)
                                        .alias('Ded Pd Exp'))
                                    .with_columns(
                                        pl.when(pl.col('PdLALAE')>0)
                                        .then(pl.col('Deductible')-pl.col('Ded Pd Exp'))
                                        .otherwise(pl.lit(0))
                                        .cast(pl.Int64)
                                        .alias('Ded Pd Loss'))
                                    .with_columns((pl.col('Deductible')-pl.col('Ded Pd Loss')-pl.col('Ded Pd Exp')).cast(pl.Int64).alias('Ded Remaining'))
                                    .with_columns(
                                        pl.when(pl.col('ResLALAE')>0)
                                        .then((pl.col('Ded Remaining')*pl.col('ResExpToLALAE')).round(0))
                                        .otherwise(pl.lit(0))
                                        .cast(pl.Int64)
                                        .alias('Ded Res Exp'))
                                    .with_columns(
                                        pl.when(pl.col('ResLALAE')>0)
                                        .then(pl.col('Ded Remaining')-pl.col('Ded Res Exp'))
                                        .otherwise(pl.lit(0))
                                        .cast(pl.Int64)
                                        .alias('Ded Res Loss'))   
                                    .with_columns(pl.when(pl.col('Ded Pd Loss')>0)
                                                    .then((pl.col('Ded Pd Loss')*(pl.col('Medical Paid')/pl.col('PdLoss'))).round(0))
                                                    .otherwise(pl.lit(0))
                                                    .cast(pl.Int64)
                                                    .alias('Ded Pd Med Loss'))
                                    .with_columns(pl.when(pl.col('Ded Pd Loss')>0)
                                                    .then(pl.col('Ded Pd Loss')-pl.col('Ded Pd Med Loss'))
                                                    .otherwise(pl.lit(0))
                                                    .cast(pl.Int64)
                                                    .alias('Ded Pd Ind Loss'))                                        
                                    .with_columns(pl.when(pl.col('Ded Res Loss')>0)
                                                    .then((pl.col('Ded Res Loss')*(pl.col('Medical Reserves')/(pl.col('Medical Reserves')+pl.col('Indemnity Reserves')))).round(0))
                                                    .otherwise(pl.lit(0))
                                                    .cast(pl.Int64)
                                                    .alias('Ded Res Med Loss'))
                                    .with_columns(pl.when(pl.col('Ded Res Loss')>0)
                                                    .then(pl.col('Ded Res Loss')-pl.col('Ded Res Med Loss'))
                                                    .otherwise(pl.lit(0))
                                                    .cast(pl.Int64)
                                                    .alias('Ded Res Ind Loss'))                                                                                                                         
                                    .with_columns([
                                        (pl.col('Net Indemnity Paid')+pl.col('Ded Pd Ind Loss')).alias('Gross Indemnity Paid'),
                                        (pl.col('Net Indemnity Incurred')+pl.col('Ded Pd Ind Loss')+pl.col('Ded Res Ind Loss')).alias('Gross Indemnity Incurred'),
                                        (pl.col('Net Medical Paid')+pl.col('Ded Pd Med Loss')).alias('Gross Medical Paid'),
                                        (pl.col('Net Medical Incurred')+pl.col('Ded Pd Med Loss')+pl.col('Ded Res Med Loss')).alias('Gross Medical Incurred'),  
                                        (pl.col('Net Expense Paid')+pl.col('Ded Pd Exp')).alias('Gross Expense Paid'),
                                        (pl.col('Net Expense Incurred')+pl.col('Ded Pd Exp')+pl.col('Ded Res Exp')).alias('Gross Expense Incurred')])], how='diagonal')                                            

    
    #Remove unnecessary columns created in last step
    colstodelete=list(set(dfResults.columns) & set(['Group','PdLoss','PdLALAE','IncLoss','IncExp','IncLALAE','Ded Pd Loss','Ded Pd Med Loss','Ded Pd Ind Loss','Ded Inc Loss','Ded Pd Exp','Ded Inc Exp',
                                                    'Ded Res Exp','Ded Res Loss','Ded Res Ind Loss','Ded Res Med Loss','Ded Remaining','ResLoss','ResLALAE','PdExpToLALAE','ResExpToLALAE','Indemnity Paid',
                                                    'Indemnity Reserves','Medical Paid','Medical Reserves','Expense Paid','Expense Reserves']))
    if len(colstodelete)>0:
        dfResults=dfResults.drop(colstodelete)

    dfResults=(dfResults
                .with_columns(pl.col('Risk Source').replace_strict(specdict["mapECOXPLALAEHandling"],default='Loss Only').alias('ECOXPL ALAE Handling'))
                #.join(mapper, on='Risk Source', how='left')
                .with_columns(pl.col(["Gross Medical Paid","Gross Indemnity Paid","Gross Medical Incurred","Gross Indemnity Incurred",
                                "Gross Expense Paid","Gross Expense Incurred","Net Medical Paid","Net Indemnity Paid","Net Medical Incurred",
                                "Net Indemnity Incurred","Net Expense Paid","Net Expense Incurred"]).fill_null(0))
                .with_columns(pl.col(["Gross Medical Paid","Gross Indemnity Paid","Gross Medical Incurred","Gross Indemnity Incurred",
                                "Gross Expense Paid","Gross Expense Incurred","Net Medical Paid","Net Indemnity Paid","Net Medical Incurred",
                                "Net Indemnity Incurred","Net Expense Paid","Net Expense Incurred"]).fill_nan(0))
                .with_columns([(pl.col('Gross Medical Paid')+pl.col('Gross Indemnity Paid')).cast(pl.Int64).alias('Gross Paid Loss'),
                                (pl.col('Gross Medical Incurred')+pl.col('Gross Indemnity Incurred')).cast(pl.Int64).alias('Gross Incurred Loss'),
                                (pl.col('Net Medical Paid')+pl.col('Net Indemnity Paid')).cast(pl.Int64).alias('Net Paid Loss'),
                                (pl.col('Net Medical Incurred')+pl.col('Net Indemnity Incurred')).cast(pl.Int64).alias('Net Incurred Loss')])
                .with_columns([(pl.col('Gross Paid Loss')+pl.col('Gross Expense Paid')).cast(pl.Int64).alias('Gross Paid LALAE'),
                                (pl.col('Gross Incurred Loss')+pl.col('Gross Expense Incurred')).cast(pl.Int64).alias('Gross Incurred LALAE'),
                                (pl.col('Net Paid Loss')+pl.col('Net Expense Paid')).cast(pl.Int64).alias('Net Paid LALAE'),
                                (pl.col('Net Incurred Loss')+pl.col('Net Expense Incurred')).cast(pl.Int64).alias('Net Incurred LALAE')]) 
                .with_columns(pl.when(pl.col('Deductible Erodes Policy Limit')==True)
                                .then(pl.max_horizontal((pl.col('Policy Limit')-pl.col('Deductible')),0))
                                .otherwise(pl.col('Policy Limit'))
                                .cast(pl.Int64)
                                .alias('Adjusted Policy Limit')) 
                .with_columns(pl.when(pl.col('Defense Outside Limit')==True)
                                .then(pl.when(pl.col('Net Incurred Loss')-pl.col('Adjusted Policy Limit')==0)
                                    .then(pl.lit(True))
                                    .otherwise(pl.lit(False)))
                                .otherwise(pl.when(pl.col('Net Incurred LALAE')-pl.col('Adjusted Policy Limit')==0)
                                            .then(pl.lit(True))
                                            .otherwise(pl.lit(False)))
                                .cast(pl.Boolean)
                                .alias('Limit Loss Inc')) 
                .with_columns(pl.when(pl.col('Defense Outside Limit')==True)
                                .then(pl.when(pl.col('Net Paid Loss')-pl.col('Adjusted Policy Limit')==0)
                                    .then(pl.lit(True))
                                    .otherwise(pl.lit(False)))
                                .otherwise(pl.when(pl.col('Net Paid LALAE')-pl.col('Adjusted Policy Limit')==0)
                                            .then(pl.lit(True))
                                            .otherwise(pl.lit(False)))
                                .cast(pl.Boolean)
                                .alias('Limit Loss Pd'))                                                                     
                .with_columns(pl.when((pl.col('Defense Outside Limit')==True)|(pl.col('ECOXPL ALAE Handling')=='Loss Only'))  
                                .then(pl.max_horizontal(0,pl.col('Net Incurred Loss')-pl.col('Adjusted Policy Limit')))
                                .otherwise(pl.max_horizontal(0,pl.col('Net Incurred LALAE')-pl.col('Adjusted Policy Limit')))
                                .cast(pl.Int64)
                                .alias('ECOXPL'))
                .with_columns(pl.when(pl.col('ECOXPL')>0)
                                .then(pl.when((pl.col('Defense Outside Limit')==True)|(pl.col('ECOXPL ALAE Handling')=='Loss Only'))  
                                    .then(pl.col('ECOXPL'))
                                    .otherwise((pl.col('ECOXPL')*(pl.col('Net Incurred Loss')/pl.col('Net Incurred LALAE'))).round(0)))
                                .otherwise(pl.lit(0))
                                .cast(pl.Int64)
                                .alias('Incurred ECOXPL Loss'))
                .with_columns(pl.when(pl.col('ECOXPL')>0)
                                .then(pl.when(pl.col('ECOXPL ALAE Handling')=='Loss Only')
                                    .then(pl.col('ECOXPL'))
                                    .when(pl.col('Defense Outside Limit')==True)
                                    .then((pl.col('ECOXPL')*(pl.col('Net Incurred LALAE')/pl.col('Net Incurred Loss'))).round(0))
                                    .otherwise(pl.col('ECOXPL')))
                                .otherwise(pl.lit(0))
                                .cast(pl.Int64)
                                .alias('Incurred ECOXPL LALAE'))  
                .with_columns(pl.when(pl.col('ECOXPL')>0)
                                .then(True)
                                .otherwise(False)
                                .cast(pl.Boolean)
                                .alias('Has ECOXPL Inc'))
                .with_columns(pl.when(((pl.col('Defense Outside Limit')==True)|(pl.col('ECOXPL ALAE Handling')=='Loss Only'))) 
                                .then(pl.max_horizontal(0,pl.col('Net Paid Loss')-pl.col('Adjusted Policy Limit')))
                                .otherwise(pl.max_horizontal(0,pl.col('Net Paid LALAE')-pl.col('Adjusted Policy Limit')))
                                .cast(pl.Int64)
                                .alias('ECOXPL Pd'))
                .with_columns(pl.when(pl.col('ECOXPL Pd')>0)
                                .then(pl.when((pl.col('Defense Outside Limit')==True)|(pl.col('ECOXPL ALAE Handling')=='Loss Only'))  
                                    .then(pl.col('ECOXPL Pd'))
                                    .otherwise((pl.col('ECOXPL Pd')*(pl.col('Net Paid Loss')/pl.col('Net Paid LALAE'))).round(0)))
                                .otherwise(pl.lit(0))
                                .cast(pl.Int64)
                                .alias('Paid ECOXPL Loss'))
                .with_columns(pl.when(pl.col('ECOXPL Pd')>0)
                                .then(pl.when(pl.col('ECOXPL ALAE Handling')=='Loss Only')
                                    .then(pl.col('ECOXPL Pd'))
                                    .when(pl.col('Defense Outside Limit')==True)
                                    .then((pl.col('ECOXPL Pd')*(pl.col('Net Paid LALAE')/pl.col('Net Paid Loss'))).round(0))
                                    .otherwise(pl.col('ECOXPL Pd')))
                                .otherwise(pl.lit(0))
                                .cast(pl.Int64)
                                .alias('Paid ECOXPL LALAE'))  
                .with_columns(pl.when(pl.col('ECOXPL Pd')>0)
                                .then(True)
                                .otherwise(False)
                                .cast(pl.Boolean)
                                .alias('Has ECOXPL Pd'))  
                .with_columns([(pl.col('Incurred ECOXPL LALAE')-pl.col('Incurred ECOXPL Loss')).cast(pl.Int64).alias('Incurred ECOXPL Expense'),
                                (pl.col('Paid ECOXPL LALAE')-pl.col('Paid ECOXPL Loss')).cast(pl.Int64).alias('Paid ECOXPL Expense')])
                .drop(["Incurred ECOXPL LALAE", "Adjusted Policy Limit","Paid ECOXPL LALAE","ECOXPL","ECOXPL Pd","ECOXPL ALAE Handling"])
                .with_columns([pl.when(pl.col('Has ECOXPL Inc')==True)
                                .then((pl.col('Gross Medical Incurred')/pl.col('Gross Incurred Loss')))
                                .otherwise(0.0)
                                .cast(pl.Float64)
                                .alias('PctGrossMedInc'),
                                pl.when(pl.col('Has ECOXPL Inc')==True)
                                .then((pl.col('Net Medical Incurred')/pl.col('Net Incurred Loss')))
                                .otherwise(0.0)
                                .cast(pl.Float64)
                                .alias('PctNetMedInc'),
                                pl.when(pl.col('Has ECOXPL Pd')==True)
                                .then((pl.col('Gross Medical Paid')/pl.col('Gross Paid Loss')))
                                .otherwise(0.0)
                                .cast(pl.Float64)
                                .alias('PctGrossMedPd'),
                                pl.when(pl.col('Has ECOXPL Pd')==True)
                                .then((pl.col('Net Medical Paid')/pl.col('Net Paid Loss')))
                                .otherwise(0.0)
                                .cast(pl.Float64)
                                .alias('PctNetMedPd')])
                .with_columns([(pl.col('Gross Incurred Loss')-pl.col('Incurred ECOXPL Loss')).cast(pl.Int64).alias('Gross Incurred Loss'),
                                (pl.col('Net Incurred Loss')-pl.col('Incurred ECOXPL Loss')).cast(pl.Int64).alias('Net Incurred Loss'),
                                (pl.col('Gross Expense Incurred')-pl.col('Incurred ECOXPL Expense')).cast(pl.Int64).alias('Gross Expense Incurred'),
                                (pl.col('Net Expense Incurred')-pl.col('Incurred ECOXPL Expense')).cast(pl.Int64).alias('Net Expense Incurred'),
                                (pl.col('Gross Paid Loss')-pl.col('Paid ECOXPL Loss')).cast(pl.Int64).alias('Gross Paid Loss'),
                                (pl.col('Net Paid Loss')-pl.col('Paid ECOXPL Loss')).cast(pl.Int64).alias('Net Paid Loss'),
                                (pl.col('Gross Expense Paid')-pl.col('Paid ECOXPL Expense')).cast(pl.Int64).alias('Gross Expense Paid'),
                                (pl.col('Net Expense Paid')-pl.col('Paid ECOXPL Expense')).cast(pl.Int64).alias('Net Expense Paid')])
                .with_columns([(pl.col('Gross Incurred Loss')*pl.col('PctGrossMedInc')).round(0).cast(pl.Int64).alias('Gross Medical Incurred'),
                                (pl.col('Net Incurred Loss')*pl.col('PctNetMedInc')).round(0).cast(pl.Int64).alias('Net Medical Incurred'),
                                (pl.col('Gross Paid Loss')*pl.col('PctGrossMedPd')).round(0).cast(pl.Int64).alias('Gross Medical Paid'),
                                (pl.col('Net Paid Loss')*pl.col('PctNetMedPd')).round(0).cast(pl.Int64).alias('Net Medical Paid')])
                .with_columns([(pl.col('Gross Incurred Loss')-pl.col('Gross Medical Incurred')).cast(pl.Int64).alias('Gross Indemnity Incurred'),
                                (pl.col('Net Incurred Loss')-pl.col('Net Medical Incurred')).cast(pl.Int64).alias('Net Indemnity Incurred'),
                                (pl.col('Gross Paid Loss')-pl.col('Gross Medical Paid')).cast(pl.Int64).alias('Gross Indemnity Paid'),
                                (pl.col('Net Paid Loss')-pl.col('Net Medical Paid')).cast(pl.Int64).alias('Net Indemnity Paid')])
                .drop(["PctGrossMedInc", "PctNetMedInc","PctGrossMedPd","PctNetMedPd"])
                .with_columns(pl.when(pl.col('Net Incurred LALAE')==pl.col('Net Paid LALAE'))
                                .then(pl.lit(True))
                                .otherwise(pl.lit(False))
                                .alias('Closed'))
                .with_columns(pl.when(pl.col('Closed')==False)
                                .then(pl.lit(1))
                                .otherwise(pl.lit(0))
                                .alias('Open Count'))   
                .with_columns(pl.sum('Open Count')
                                .over(['Occurrence Number','Evaluation Date'])
                                .alias('Event Open Count'))
                .with_columns(pl.when(pl.col('Event Open Count')>0)
                                .then(pl.lit(False))
                                .otherwise(pl.lit(True))
                                .alias('Event Closed'))
                .drop(["Loss Data Gross or Net of Deductible","Gross Incurred LALAE","Gross Incurred Loss",
                        "Net Incurred LALAE","Net Incurred Loss","Gross Paid LALAE","Gross Paid Loss",
                        "Net Paid LALAE","Net Paid Loss","Open Count",'Event Open Count']))

                #Add columns with trend factors
    dfResults=dfResults.join(pl.DataFrame({"Trend":[None,"Trended"]}),how='cross')


    dfResults=(dfResults.join((specdict["Severity Trend"]
                    .select(["Risk Source", "Year", "ECOXPL", "Exp", "Ind", "Med"])
                    .rename({'Year':'Trend Year'})),
                    how='left',
                    on=['Risk Source','Trend Year'])
                .drop(['Trend Year'])
                .with_columns(pl.when(pl.col('Trend').is_null())
                                .then(pl.lit(1.0))
                                .otherwise(pl.col('Exp'))
                                .alias('Exp'))
                .with_columns(pl.when(pl.col('Trend').is_null())
                                .then(pl.lit(1.0))
                                .otherwise(pl.col('Ind'))
                                .alias('Ind'))                                  
                .with_columns(pl.when(pl.col('Trend').is_null())
                                .then(pl.lit(1.0))
                                .otherwise(pl.col('Med'))
                                .alias('Med'))                                  
                .with_columns(pl.when(pl.col('Trend').is_null())
                                .then(pl.lit(1.0))
                                .otherwise(pl.col('ECOXPL'))
                                .alias('ECOXPL'))
                .with_columns([pl.col('Exp').fill_null(1.0),
                                pl.col('Ind').fill_null(1.0),
                                pl.col('Med').fill_null(1.0),
                                pl.col('ECOXPL').fill_null(1.0)]))

    #Split into paid and incurred
    dfResults_dict=(pl.concat([dfResults.drop(['Gross Indemnity Paid','Gross Medical Paid','Gross Expense Paid','Net Expense Paid','Net Medical Paid',
                                        'Net Indemnity Paid','Paid ECOXPL Loss','Has ECOXPL Pd','Paid ECOXPL Expense','Limit Loss Pd'])
                                .with_columns(pl.lit('Incurred').alias('Paid or Incurred'))
                                .rename({'Gross Indemnity Incurred':'Gross Indemnity','Gross Medical Incurred':'Gross Medical',
                                        'Gross Expense Incurred':'Gross Expense','Net Expense Incurred':'Net Expense',
                                        'Net Medical Incurred':'Net Medical','Net Indemnity Incurred':'Net Indemnity',
                                        'Incurred ECOXPL Loss':'ECOXPL Loss','Has ECOXPL Inc':'Has ECOXPL','Limit Loss Inc':'Limit Loss',
                                        'Incurred ECOXPL Expense':'ECOXPL Expense'}),
                            dfResults.drop(['Gross Indemnity Incurred','Gross Medical Incurred','Gross Expense Incurred','Net Expense Incurred',
                                        'Net Medical Incurred','Net Indemnity Incurred','Incurred ECOXPL Loss','Has ECOXPL Inc','Limit Loss Inc','Incurred ECOXPL Expense'])
                                .with_columns(pl.lit('Paid').alias('Paid or Incurred'))
                                .rename({'Gross Indemnity Paid':'Gross Indemnity','Gross Medical Paid':'Gross Medical',
                                        'Gross Expense Paid':'Gross Expense','Net Expense Paid':'Net Expense',
                                        'Net Medical Paid':'Net Medical','Net Indemnity Paid':'Net Indemnity',
                                        'Paid ECOXPL Loss':'ECOXPL Loss','Has ECOXPL Pd':'Has ECOXPL','Limit Loss Pd':'Limit Loss',
                                        'Paid ECOXPL Expense':'ECOXPL Expense'})])
                    .with_columns([pl.col('ECOXPL Expense').fill_null(0),
                                pl.col('ECOXPL Loss').fill_null(0)]) 
                    .with_columns(pl.when(pl.col('Has ECOXPL')==True)
                                    .then(pl.lit('Has ECOXPL'))
                                    .when(pl.col('Limit Loss')==True)
                                    .then(pl.lit('At Policy Limits'))
                                    .otherwise(pl.lit('Within Limits'))
                                    .alias('Loss Size Type'))                  
                    .partition_by('Loss Size Type',as_dict=True))

    dfResults=pl.DataFrame()
    for key in dfResults_dict.keys():
        if key=='At Policy Limits':
            #If Has ECOXPL or At Policy Limits - trend ECOXPL Loss and Expense. If Defense Outside Limit, trend gross expense
            dfResults=pl.concat([dfResults,
                            dfResults_dict[key]
                            .with_columns((pl.col('Gross Expense')-pl.col('Net Expense')).alias('DedExp'))
                            .with_columns(pl.when(pl.col('Defense Outside Limit')==False)
                                            .then(pl.col('Gross Expense'))
                                            .otherwise(pl.col('Gross Expense')*pl.col('Exp').round(0))
                                            .cast(pl.Int64)
                                            .alias('Gross Expense'))
                            .with_columns((pl.col('Gross Expense')-pl.col('DedExp')).cast(pl.Int64).alias('Net Expense'))
                            .drop(["Deductible","Deductible Application","Deductible Erodes Policy Limit","Defense Outside Limit",
                                    "Policy Limit",'Loss Size Type',"ECOXPL", "Exp", "Ind", "Med","DedExp"])])
        elif key=='Has ECOXPL':
            #If Has ECOXPL or At Policy Limits - trend ECOXPL Loss and Expense. If Defense Outside Limit, trend gross expense
            dfResults=pl.concat([dfResults,
                            dfResults_dict[key]
                            .with_columns([(((pl.col('Gross Medical')+pl.col('Gross Indemnity')+pl.col('ECOXPL Loss'))*pl.col('ECOXPL'))).round(0).cast(pl.Int64).alias('Trended Gross Loss'),
                                            ((pl.col('ECOXPL Expense')+pl.col('Gross Expense'))*pl.col('Exp')).round(0).cast(pl.Int64).alias('Trended Gross Expense')])
                            .with_columns([(pl.col('Trended Gross Loss')-pl.col('Gross Medical')-pl.col('Gross Indemnity')).alias('ECOXPL Loss'),
                                            (pl.col('Trended Gross Expense')-pl.col('Gross Expense')).alias('ECOXPL Expense')])
                            .drop(["Deductible","Deductible Application","Deductible Erodes Policy Limit","Defense Outside Limit",
                                    "Policy Limit",'Loss Size Type',"ECOXPL", "Exp", "Ind", "Med",'Trended Gross Loss','Trended Gross Expense'])])                                       
        else:
            #If doesn't have ECOXPL, trend Gross data and deductible data. Apply cap to deductibles.
            dfResults=(pl.concat([dfResults,
                            dfResults_dict[key]
                            .with_columns([(pl.col('Gross Medical')-pl.col('Net Medical')).cast(pl.Int64).alias('DedMed'),
                                            (pl.col('Gross Indemnity')-pl.col('Net Indemnity')).cast(pl.Int64).alias('DedInd'),
                                            (pl.col('Gross Expense')-pl.col('Net Expense')).cast(pl.Int64).alias('DedExp')])
                            .with_columns([(pl.col('Gross Medical')*pl.col('Med')).round(0).cast(pl.Int64).alias('Gross Medical'),
                                            (pl.col('Gross Indemnity')*pl.col('Ind')).round(0).cast(pl.Int64).alias('Gross Indemnity'),
                                            (pl.col('Gross Expense')*pl.col('Exp')).round(0).cast(pl.Int64).alias('Gross Expense'),
                                            (pl.col('DedMed')*pl.col('Med')).round(0).cast(pl.Int64).alias('DedMed'),
                                            (pl.col('DedInd')*pl.col('Ind')).round(0).cast(pl.Int64).alias('DedInd'),
                                            (pl.col('DedExp')*pl.col('Exp')).round(0).cast(pl.Int64).alias('DedExp')])
                            .with_columns(pl.when(pl.col('DedMed')+pl.col('DedInd')+pl.col('DedExp')>pl.col('Deductible'))
                                            .then((pl.col('Deductible')/(pl.col('DedMed')+pl.col('DedInd')+pl.col('DedExp'))).round(5).cast(pl.Float64))
                                            .otherwise(pl.lit(None))
                                            .alias('DedAdjFactor'))
                            .with_columns(pl.when(pl.col('DedAdjFactor').is_null())
                                            .then(pl.lit(None))
                                            .otherwise((pl.col('DedMed')*pl.col('DedAdjFactor')).round(0))
                                            .cast(pl.Int64)
                                            .alias('DedMedCapped'))
                            .with_columns(pl.when(pl.col('DedAdjFactor').is_null())
                                            .then(pl.lit(None))
                                            .otherwise(pl.col('Deductible')-pl.col('DedMedCapped'))
                                            .alias('DedRemaining'))
                            .with_columns(pl.when(pl.col('DedAdjFactor').is_null())
                                            .then(pl.lit(None))
                                            .otherwise(pl.min_horizontal(pl.col('DedRemaining'),pl.col('DedInd')*pl.col('DedAdjFactor')).round(0))
                                            .cast(pl.Int64)
                                            .alias('DedIndCapped'))
                            .with_columns(pl.when(pl.col('DedAdjFactor').is_null())
                                            .then(pl.lit(None))
                                            .otherwise(pl.col('Deductible')-pl.col('DedMedCapped')-pl.col('DedIndCapped'))
                                            .cast(pl.Int64)
                                            .alias('DedExpCapped'))
                            .with_columns([pl.when(pl.col('DedAdjFactor').is_null())
                                            .then(pl.col('DedMed'))
                                            .otherwise(pl.col('DedMedCapped'))
                                            .alias('DedMed'),
                                            pl.when(pl.col('DedAdjFactor').is_null())
                                            .then(pl.col('DedInd'))
                                            .otherwise(pl.col('DedIndCapped'))
                                            .alias('DedInd'),
                                            pl.when(pl.col('DedAdjFactor').is_null())
                                            .then(pl.col('DedExp'))
                                            .otherwise(pl.col('DedExpCapped'))
                                            .alias('DedExp')])
                            .with_columns([pl.when(pl.col('Deductible Erodes Policy Limit')==True)
                                            .then(pl.col('Policy Limit'))
                                            .otherwise(pl.when(pl.col('Defense Outside Limit')==True)
                                                        .then(pl.col('Policy Limit')+pl.col('DedInd')+pl.col('DedMed'))
                                                        .otherwise(pl.col('Policy Limit')+pl.col('Deductible')))
                                            .alias('Adj Pol Lim'),
                                            pl.when(pl.col('Defense Outside Limit')==True)
                                            .then(pl.col('Gross Medical')+pl.col('Gross Indemnity'))
                                            .otherwise(pl.col('Gross Medical')+pl.col('Gross Indemnity')+pl.col('Gross Expense'))
                                            .alias('SubjAmt')])
                            .with_columns(pl.when(pl.col('SubjAmt')<=pl.col('Adj Pol Lim'))
                                            .then(pl.lit(None))
                                            .otherwise((pl.col('Adj Pol Lim')/pl.col('SubjAmt')).round(5).cast(pl.Float64))
                                            .alias('polLimAdj'))
                            .with_columns(pl.when(pl.col('SubjAmt')<=pl.col('Adj Pol Lim'))
                                            .then(pl.col('Gross Medical'))
                                            .otherwise((pl.col('Gross Medical')*pl.col('polLimAdj')).round(0))
                                            .cast(pl.Int64)
                                            .alias('Gross Medical'))
                            .with_columns(pl.when(pl.col('SubjAmt')<=pl.col('Adj Pol Lim'))
                                            .then(pl.col('Gross Indemnity'))
                                            .otherwise(pl.when(pl.col('Defense Outside Limit')==True)
                                                        .then(pl.col('Adj Pol Lim')-pl.col('Gross Medical'))
                                                        .otherwise((pl.col('Gross Indemnity')*pl.col('polLimAdj')).round(0)))
                                            .cast(pl.Int64)
                                            .alias('Gross Indemnity'))
                            .with_columns(pl.when(pl.col('Defense Outside Limit')==True)
                                            .then(pl.col('Gross Expense'))
                                            .otherwise(pl.when(pl.col('SubjAmt')<=pl.col('Adj Pol Lim'))
                                                        .then(pl.col('Gross Expense'))
                                                        .otherwise(pl.col('Adj Pol Lim')-pl.col('Gross Medical')-pl.col('Gross Indemnity')))
                                            .alias('Gross Expense'))
                            .with_columns([(pl.col('Gross Medical')-pl.col('DedMed')).alias('Net Medical'),
                                            (pl.col('Gross Indemnity')-pl.col('DedInd')).alias('Net Indemnity'),
                                            (pl.col('Gross Expense')-pl.col('DedExp')).alias('Net Expense')])
                            .drop(["Deductible","Deductible Application","Deductible Erodes Policy Limit","Defense Outside Limit",
                                    "Policy Limit",'Loss Size Type',"ECOXPL", "Exp", "Ind", "Med",'DedMed','DedInd','DedExp',"DedRemaining",'Adj Pol Lim','SubjAmt','polLimAdj','DedAdjFactor',
                                    'DedIndCapped','DedMedCapped','DedExpCapped'])]))
        
    
    #Add Count Info
    dfResults=(dfResults
                .with_columns([pl.when(((pl.col('Net Medical')+pl.col('Net Indemnity'))==0)&(pl.col('Net Expense')>0))
                                        .then(pl.lit(True))
                                        .otherwise(pl.lit(False))
                                        .alias('ALAE Only'),
                                        pl.when(((pl.col('Net Medical')+pl.col('Net Indemnity'))==0)&(pl.col('Net Expense')>0))
                                        .then(pl.lit(1))
                                        .otherwise(pl.lit(0))
                                        .alias('Ct: ALAE Only'),  
                                        pl.when((pl.col('Net Medical')>0) & (pl.col('Net Indemnity')==0))
                                        .then(pl.lit(1))
                                        .otherwise(pl.lit(0))
                                        .alias('Ct: Med Only'),
                                        pl.when((pl.col('Net Medical')>0) & (pl.col('Net Indemnity')==0))
                                        .then(pl.lit(True))
                                        .otherwise(pl.lit(False))
                                        .alias('Med Only'),                                           
                                        pl.when(pl.col('Net Indemnity')>0)
                                        .then(pl.lit(1))
                                        .otherwise(pl.lit(0))
                                        .alias('Ct: Indemnity'),  
                                        pl.when(pl.col('Net Medical')>0)
                                        .then(pl.lit(1))
                                        .otherwise(pl.lit(0))
                                        .alias('Ct: Medical'),
                                        pl.when(pl.col('Has ECOXPL')==True)
                                        .then(pl.lit(1))
                                        .otherwise(pl.lit(0))
                                        .alias('Ct: ECO'),
                                        pl.when(pl.col('Net Medical')+pl.col('Net Indemnity')+pl.col('Net Expense')>0)
                                        .then(pl.lit(1))
                                        .otherwise(pl.lit(0))
                                        .alias('Ct: LALAE'),
                                        pl.when((pl.col('Net Medical')+pl.col('Net Indemnity')+pl.col('Net Expense')==0)&(pl.col('Closed')==True))
                                        .then(pl.lit(1))
                                        .otherwise(pl.lit(0))
                                        .alias('Ct: CWOP')])
                .with_columns([pl.sum('Ct: LALAE').over(['Evaluation Date','Paid or Incurred','Trend','Occurrence Number']).alias('Occ Ct: LALAE'),
                                pl.sum('Ct: Indemnity').over(['Evaluation Date','Paid or Incurred','Trend','Occurrence Number']).alias('Occ Ct: Indemnity'),
                                (pl.col('Gross Expense')+pl.col('Coverage Expense Constant')).alias('Gross Expense'),
                                (pl.col('Net Expense')+pl.col('Coverage Expense Constant')).alias('Net Expense')])
                .drop('Coverage Expense Constant')
                .with_columns((pl.col('Gross Indemnity')+pl.col('Gross Medical')+pl.col('Gross Expense')+pl.col('ECOXPL Loss')+pl.col('ECOXPL Expense')).alias('Total LALAE'))
                .with_columns(pl.sum('Total LALAE').over(['Evaluation Date','Paid or Incurred','Trend','Occurrence Number']).alias('Occ LALAE'))
                #When the EVENT LALAE is less than the aggregation threshold then flag claims as attritional - derived on basis of Loss AND ALAE combined 
                #Change both claim number and occurrence number to attritional
                #Reaggregate claims to claim number level to incorporate aggregation
                
                .with_columns(pl.when((specdict["aggregationThreshold"]>pl.col('Total LALAE')) & (pl.col('Occ LALAE')>0))
                                .then(pl.lit(True))
                                .otherwise(pl.lit(False))
                                .alias('Aggregate')
                                .cast(pl.Boolean))
                .join(specdict['Claim Info'].select(['Policy Limit','Loss Year','Policy Year','Custom A Year','Custom B Year','Event Loss Year',
                                                        'Event Policy Year','Event Custom A Year','Event Custom B Year','Claim Number']),on=['Claim Number'],how='left')
                .with_columns(pl.when(pl.col('Aggregate')==True)
                                .then(pl.lit('Attritional'))
                                .otherwise(pl.col('Claim Number'))
                                .alias('Claim Number'))
                .with_columns(pl.when(pl.col('Aggregate')==True)
                                .then(pl.lit('Attritional'))
                                .otherwise(pl.col('Occurrence Number'))
                                .alias('Occurrence Number'))
                .drop(['Total LALAE','Occ LALAE','Aggregate','Limit Loss'])                                                            
                .group_by(['Evaluation Date','Paid or Incurred','Trend','Claim Number','Occurrence Number','Policy Limit','Loss Year',
                            'Policy Year','Custom A Year','Custom B Year','Event Loss Year','Event Policy Year',
                            'Event Custom A Year','Event Custom B Year','Has ECOXPL','Closed','Event Closed','Risk Source','ALAE Only','Med Only']).sum())
    return dfResults

def GrossLossSummary(specdict):

        ages=calcDevAgeConversionTable(specdict)
        dfoutput=(pl.concat([specdict['Prepped Losses'].drop(['Has ECOXPL','Event Loss Year','Event Policy Year','Event Custom A Year','Event Custom B Year','Gross Indemnity','Gross Medical','Gross Expense','ECOXPL Loss','ECOXPL Expense'])
                            .with_columns(pl.lit('Excl ECO').alias('ECO'))
                            .rename({'Net Indemnity':'Indemnity','Net Medical':'Medical','Net Expense':'Expense'}),
                            specdict['Prepped Losses'].drop(['Has ECOXPL','Event Loss Year','Event Policy Year','Event Custom A Year','Event Custom B Year','Gross Indemnity','Gross Medical','Gross Expense',
                                            'Net Indemnity','Net Medical','Net Expense','Ct: ALAE Only','Ct: Medical','Ct: Med Only','Ct: Indemnity','Ct: LALAE','Ct: CWOP','Occ Ct: LALAE','Occ Ct: Indemnity'])
                            .filter(pl.col('Ct: ECO')>0)
                            .with_columns(pl.lit('ECO').alias('ECO'))
                            .rename({'ECOXPL Loss':'Indemnity','ECOXPL Expense':'Expense'})],how='diagonal')
                    .drop(['Claim Number','Occurrence Number'])
                    .with_columns(pl.col(['Ct: ALAE Only','Ct: Medical','Ct: Med Only','Ct: Indemnity','Ct: LALAE','Ct: CWOP','Occ Ct: LALAE','Occ Ct: Indemnity']).fill_null(0))
                    .group_by(['Evaluation Date','Paid or Incurred','Trend','Risk Source','Loss Year','Policy Year','Custom A Year','Custom B Year','Closed','Event Closed','ALAE Only','Med Only','ECO']).sum()
                    .join(ages.rename({'Year':'Policy Year'}),on=['Evaluation Date','Policy Year'],how='left')
                    .rename({'Age':'PY Age'})
                    .join(ages.rename({'Year':'Loss Year'}),on=['Evaluation Date','Loss Year'],how='left')
                    .rename({'Age':'LY Age'})
                    .join(ages.rename({'Year':'Custom A Year'}),on=['Evaluation Date','Custom A Year'],how='left')
                    .rename({'Age':'Custom A Age'})
                    .join(ages.rename({'Year':'Custom B Year'}),on=['Evaluation Date','Custom B Year'],how='left')
                    .rename({'Age':'Custom B Age'})
                    .filter((pl.col('PY Age').is_not_null())|(pl.col('LY Age').is_not_null())|(pl.col('Custom A Age').is_not_null())|(pl.col('Custom B Age').is_not_null())))
        return dfoutput

def calcDevAgeConversionTable(specdict):
    years = specdict['Claim Info'].select(['Loss Year','Policy Year','Custom A Year','Custom B Year'])
    years = pl.DataFrame(list(set(years.get_column("Policy Year").unique().to_list()) | set(years.get_column("Loss Year").unique().to_list())|set(years.get_column("Custom A Year").unique().to_list())|set(years.get_column("Custom B Year").unique().to_list())),schema=['Year'])
    evals = pl.DataFrame(specdict['Prepped Losses'].get_column("Evaluation Date").unique().to_list(),schema=['Evaluation Date'])
    temp=years.join(evals,how='cross')
    temp=temp.with_columns((12*(pl.col('Evaluation Date').dt.year().cast(pl.Int32)-pl.col('Year'))+pl.col('Evaluation Date').dt.month().cast(pl.Int32)).alias('Age'))
    temp=temp.filter(pl.col('Age')>0).with_columns(pl.col('Year').cast(pl.Int64))
    return temp


def CededLossesAllLayers(specdict):
    dfoutput=CreateCededLayerLosses(specdict)
 
    if specdict["cededDetailVals"]=="All":
        specdict['Ceded Loss Detail']=dfoutput
    else:
        currval=dfoutput['Evaluation Date'].max()
        specdict['Ceded Loss Detail']=dfoutput.filter(pl.col('Evaluation Date')==currval)

    dfoutput=(dfoutput.with_columns(pl.when((pl.col('Ceded Loss')+pl.col('Ceded ECOXPL Loss')==0) & (pl.col('Ceded Expense')+pl.col('Ceded ECOXPL Expense')>0))
                                        .then(pl.lit(True))
                                        .otherwise(pl.lit(False))
                                        .alias('ALAE Only')))
    
    ages=calcDevAgeConversionTable(specdict)
    dfoutput=(pl.concat([dfoutput.drop(['Ceded ECOXPL Loss','Ceded ECOXPL Expense','Clm Ct - ECOXPL Loss','Clm Ct - ECOXPL LALAE'])
                        .with_columns(pl.lit('Excl ECO').alias('ECO')),
                        dfoutput.drop(['Ceded Loss','Ceded Expense','Clm Ct - Loss','Clm Ct - LALAE'])
                        .filter(pl.col('Clm Ct - ECOXPL LALAE')>0)
                        .with_columns(pl.lit('ECO').alias('ECO'))
                        .rename({'Ceded ECOXPL Loss':'Ceded Loss','Ceded ECOXPL Expense':'Ceded Expense','Clm Ct - ECOXPL Loss':'Clm Ct - Loss',
                                'Clm Ct - ECOXPL LALAE':'Clm Ct - LALAE'})],how='diagonal')
                .drop(['Claim Number','Occurrence Number'])
                .group_by(['Layer','Evaluation Date','Paid or Incurred','Trend','Risk Source','Loss Year','Policy Year','Custom A Year','Custom B Year','Closed','Event Closed','ALAE Only','ECO']).sum()
                .join(ages.rename({'Year':'Policy Year'}),on=['Evaluation Date','Policy Year'],how='left')
                .rename({'Age':'PY Age'})
                .join(ages.rename({'Year':'Loss Year'}),on=['Evaluation Date','Loss Year'],how='left')
                .rename({'Age':'LY Age'})
                .join(ages.rename({'Year':'Custom A Year'}),on=['Evaluation Date','Custom A Year'],how='left')
                .rename({'Age':'Custom A Age'})                    
                .join(ages.rename({'Year':'Custom B Year'}),on=['Evaluation Date','Custom B Year'],how='left')
                .rename({'Age':'Custom B Age'}))

    return dfoutput

def CreateCededLayerLosses(specdict):
    # Per Claim can't have inuring. First calculate per claim recoveries for per clm with event and per clm with no event. Aggregate to event level.
    # Then get ones that have no per claim but have per event, and those that have neither per clm nor per event.. agg to per event level, apply inuring.
    # Combine the four dataframes above and apply per event terms and agg terms
    # dfSubjLosses=(self.spec_dfs['Prepped Losses']
    #                 .with_columns((pl.col('Gross Indemnity')+pl.col('Gross Medical')).alias('Loss'))
    #                 .with_columns((pl.col('Loss')+pl.col('ECOXPL Loss')).alias('Loss Incl ECOXPL'))
    #                 .with_columns((pl.col('Loss')+pl.col('Gross Expense')).alias('Loss and ALAE'))
    #                 .with_columns((pl.col('Loss and ALAE')+pl.col('ECOXPL Loss')+pl.col('ECOXPL Expense')).alias('Loss and ALAE Incl ECOXPL'))
    #                 .with_columns(pl.col('Loss and ALAE Incl ECOXPL').sum().over(['Evaluation Date','Paid or Incurred','Trend','Occurrence Number']).alias('Event LALAE Incl ECOXPL')))

    dfLossLayers = specdict["Layers"].drop(
        ["Risk Source Group"])

    dfResults = pl.DataFrame(
        schema={
            "Layer": pl.Utf8,
            "Evaluation Date":pl.Date,
            "Claim Number": pl.Utf8,
            "Occurrence Number": pl.Utf8,
            "Paid or Incurred": pl.Utf8,
            "Trend": pl.Utf8,
            "Risk Source": pl.Utf8, 
            "Ceded Loss": pl.Float64,
            "Ceded Loss and ALAE": pl.Float64,
            "Ceded ECOXPL Loss": pl.Float64,
            "Ceded ECOXPL Loss and ALAE": pl.Float64,
        }
    )

    numlevels = dfLossLayers.get_column("Level").max()

    def getSubject(df):
        result=(df.with_columns(pl.when(((pl.col('HasPerClaim')==True)&(pl.col('Per Claim Retention')>0)&(pl.col('Claim Number')=='Attritional')))
                                .then(pl.lit(False))
                                .otherwise(pl.lit(True))
                                .alias('Keep'))
                    .filter(pl.col('Keep')==True)
                    .with_columns(pl.when(pl.col('Deductible Treatment')=='Apply to losses net of deductible')
                                    .then(pl.col('Net Medical')+pl.col('Net Indemnity'))
                                    .otherwise(pl.col('Gross Medical')+pl.col('Gross Indemnity'))
                                    .alias('Subject Loss'))
                    .with_columns(pl.when(pl.col('Deductible Treatment')=='Apply to losses net of deductible')
                                    .then((pl.col('Net Expense')+pl.col('Subject Loss')))
                                    .otherwise((pl.col('Gross Expense')+pl.col('Subject Loss')))
                                    .alias('Subject Loss and ALAE'))
                    .with_columns(pl.when(((pl.col('ECOXPL Treatment')=='Pro Rata with Layer Limit')&(pl.col('HasPerClaim')==True)))
                                    .then(pl.max_horizontal(pl.min_horizontal((pl.col('Per Claim Limit')/(pl.col('Policy Limit')-pl.col('Per Claim Retention'))),1.0),0.0))
                                    .otherwise(pl.lit(1.0))
                                    .alias('ECOXPL Coverage Pct'))
                    .with_columns((pl.col('ECOXPL Coverage Pct')*pl.col('ECOXPL Loss')*pl.col('ECOXPL Pct Covered')).alias('Subject ECOXPL Loss'))
                    .with_columns((pl.col('Subject ECOXPL Loss')+((pl.col('ECOXPL Coverage Pct')*pl.col('ECOXPL Expense')*pl.col('ECOXPL Pct Covered')))).alias('Subject ECOXPL Loss and ALAE'))
                    .drop(['ECOXPL Coverage Pct','ECOXPL Pct Covered','ECOXPL Loss','ECOXPL Expense'])
                    .select(['Layer', 'Per Claim Limit', 'Per Claim Retention', 'ALAE Handling', 'Limit Including ECOXPL', 'ECOXPL Treatment',
                            'Per Event Limit', 'Per Event Retention', 'Loss Participation In','Loss Participation Out', 'HasPerClaim', 'HasPerEvent',
                            'Risk Source', 'Evaluation Date', 'Claim Number', 'Occurrence Number','Paid or Incurred', 'Trend',
                            'Subject Loss', 'Subject Loss and ALAE', 'Subject ECOXPL Loss', 'Subject ECOXPL Loss and ALAE']))        
        return result      

    def applyPerClaim(_df):
        temp= (_df
                .with_columns([pl.when(pl.col('ALAE Handling')=="Included")
                                .then(pl.col('Subject Loss and ALAE'))     
                                .otherwise(pl.col('Subject Loss'))                
                                .alias('TriggerAmt'),
                                pl.when(pl.col('ALAE Handling')=="Included")
                                .then(pl.col('Subject ECOXPL Loss and ALAE'))
                                .otherwise(pl.col('Subject ECOXPL Loss'))
                                .alias('TriggerAmtECO')])
                #if ecotreatment requires nonECOXPL loss in layer, then filter out claims where loss is less than or equal to attachment
                .with_columns(pl.when(pl.col('ECOXPL Treatment')=="Included in UNL if Policy Limit in Layer")
                                .then(pl.when(pl.col('TriggerAmt')<=pl.col('Per Claim Retention'))
                                        .then(pl.lit(-1))
                                        .otherwise(pl.col('TriggerAmt')+pl.col('TriggerAmtECO')))
                                .otherwise(pl.when(pl.col('ECOXPL Treatment')=="Included in UNL")
                                            .then(pl.col('TriggerAmt')+pl.col('TriggerAmtECO'))
                                            .otherwise(pl.col('TriggerAmt')))   #If Pro Rata with Layer Limit
                                .alias('TriggerAmtTotal'))
                .with_columns(pl.when(pl.col('TriggerAmtTotal')<=pl.col('Per Claim Retention'))
                                .then(pl.lit(-1))
                                .otherwise(pl.lit(1))
                                .alias('Keep'))
                .filter(pl.col('Keep')>0)    #If attach=0 then attritional losses are included, but this filter will be ok since attach=0
                .drop('Keep')
                .with_columns(pl.when(pl.col('Claim Number')=='Attritional')
                                .then(pl.col('TriggerAmt'))
                                .otherwise(_misc.clip(pl.col('TriggerAmt')-pl.col('Per Claim Retention'),0,pl.col('Per Claim Limit')))
                                .alias('CededAmtAfterLossTrigger'))   #excl ecoxpl
                .with_columns(pl.when(pl.col('TriggerAmtTotal')==pl.col('TriggerAmt'))
                                .then(pl.col('CededAmtAfterLossTrigger'))
                                .otherwise(_misc.clip(pl.col('TriggerAmtTotal')-pl.col('Per Claim Retention'),0,pl.col('Limit Including ECOXPL')))
                                .alias('CededAmtAfterTotalTrigger'))   #incl ecoxpl    
                .with_columns(pl.when((pl.col('ECOXPL Treatment') == 'Pro Rata with Layer Limit')&(pl.col('CededAmtAfterLossTrigger')>0))
                                .then(pl.col('TriggerAmtECO'))
                                .otherwise((pl.col('CededAmtAfterTotalTrigger')-pl.col('CededAmtAfterLossTrigger')))
                                .alias('CededAmtECO'))   #ecoxpl                            
                .with_columns([pl.when(pl.col('ALAE Handling')=="Included")   #convert to loss and alae components
                                .then(pl.when(pl.col('CededAmtAfterLossTrigger')==0)
                                        .then(pl.lit(0))
                                        .otherwise(pl.col('CededAmtAfterLossTrigger')*(pl.col('Subject Loss')/pl.col('Subject Loss and ALAE'))))
                                .otherwise(pl.col('CededAmtAfterLossTrigger'))
                                .alias('Ceded Loss'),
                                pl.when(pl.col('ALAE Handling')=="Included")   #convert to loss and alae components
                                .then(pl.when(pl.col('CededAmtECO')==0)
                                        .then(pl.lit(0))
                                        .otherwise(pl.col('CededAmtECO')*(pl.col('Subject ECOXPL Loss')/(pl.col('Subject ECOXPL Loss and ALAE')))))
                                .otherwise(pl.col('CededAmtECO'))
                                .alias('Ceded ECOXPL Loss'),
                                pl.when(pl.col('ALAE Handling')=="Pro Rata")
                                .then(pl.when(pl.col('CededAmtAfterLossTrigger')==0)
                                        .then(pl.lit(0))
                                        .otherwise(pl.col('CededAmtAfterLossTrigger')*(pl.col('Subject Loss and ALAE')/pl.col('Subject Loss'))))
                                .otherwise(pl.col('CededAmtAfterLossTrigger'))
                                .alias('Ceded Loss and ALAE'),
                                pl.when(pl.col('ALAE Handling')=="Pro Rata")
                                .then(pl.when(pl.col('CededAmtECO')==0)
                                        .then(pl.lit(0))
                                        .otherwise(pl.col('CededAmtECO')*(pl.col('Subject ECOXPL Loss and ALAE')/pl.col('Subject ECOXPL Loss'))))
                                .otherwise(pl.col('CededAmtECO'))
                                .alias('Ceded ECOXPL Loss and ALAE')])
                .drop(['TriggerAmt','TriggerAmtECO','TriggerAmtTotal','CededAmtAfterLossTrigger','CededAmtAfterTotalTrigger',
                    'CededAmtECO','Subject Loss','Subject Loss and ALAE','Subject ECOXPL Loss','Subject ECOXPL Loss and ALAE',
                    'Per Claim Limit','Per Claim Retention','Limit Including ECOXPL','ECOXPL Treatment','HasPerClaim'])
                .rename({'Ceded Loss':'Subject Loss','Ceded Loss and ALAE':'Subject Loss and ALAE','Ceded ECOXPL Loss':'Subject ECOXPL Loss','Ceded ECOXPL Loss and ALAE':'Subject ECOXPL Loss and ALAE'}))   
        
        return temp

    def applyPerEvent(_df):
        #all ecoxpl is assumed to be within limit for occurrence covers. Use ecoxpl pct to indicate if there is cover or not.
        return (_df
                .with_columns(pl.col('Per Event Retention').fill_null(0))
                .with_columns(pl.col('Per Event Limit').fill_null(999999999))
                .with_columns([pl.when(pl.col('ALAE Handling')=="Included")
                                .then(pl.col('Subject Loss and ALAE'))     
                                .otherwise(pl.col('Subject Loss'))                
                                .alias('TriggerAmt'),
                                pl.when(pl.col('ALAE Handling')=="Included")
                                .then(pl.col('Subject ECOXPL Loss and ALAE'))  #no adjustment for ECO Coverage Pct because not allowing Pro Rata with Limit for occurrence layers
                                .otherwise(pl.col('Subject ECOXPL Loss'))
                                .alias('TriggerAmtECO')])
                .with_columns([pl.col('Subject Loss').sum().over(['Layer','Evaluation Date','Paid or Incurred','Trend','Occurrence Number']).alias('EventLoss'),
                                pl.col('Subject Loss and ALAE').sum().over(['Layer','Evaluation Date','Paid or Incurred','Trend','Occurrence Number']).alias('EventLALAE'),  
                                pl.col('Subject ECOXPL Loss').sum().over(['Layer','Evaluation Date','Paid or Incurred','Trend','Occurrence Number']).alias('EventECOLoss'),
                                pl.col('Subject ECOXPL Loss and ALAE').sum().over(['Layer','Evaluation Date','Paid or Incurred','Trend','Occurrence Number']).alias('EventECOLALAE'),
                                pl.col('TriggerAmt').sum().over(['Layer','Evaluation Date','Paid or Incurred','Trend','Occurrence Number']).alias('EventTriggerAmt'),
                                pl.col('TriggerAmtECO').sum().over(['Layer','Evaluation Date','Paid or Incurred','Trend','Occurrence Number']).alias('EventTriggerAmtECO')])
                .with_columns((pl.col('EventTriggerAmt')+pl.col('EventTriggerAmtECO')).alias('EventTriggerAmtTotal'))
                .filter(pl.col('EventTriggerAmtTotal')>pl.col('Per Event Retention'))    #If attach=0 then attritional losses are included, but this filter will be ok since attach=0
                .with_columns(_misc.clip(pl.col('EventTriggerAmt')-pl.col('Per Event Retention'),0,pl.col('Per Event Limit'))
                                .alias('EventCededAmtAfterLossTrigger'))   #excl ecoxpl
                .with_columns(_misc.clip(pl.col('EventTriggerAmtTotal')-pl.col('Per Event Retention'),0,pl.col('Per Event Limit'))
                                .alias('EventCededAmtAfterTotalTrigger'))   #incl ecoxpl    
                .with_columns((pl.col('EventCededAmtAfterTotalTrigger')-pl.col('EventCededAmtAfterLossTrigger'))
                                .alias('EventCededAmtECO'))   #ecoxpl                                       
                .with_columns([pl.when(pl.col('ALAE Handling')=="Included")   #convert to loss and alae components
                                .then(pl.when(pl.col('EventCededAmtAfterLossTrigger')==0)
                                        .then(pl.lit(0))
                                        .otherwise(pl.col('EventCededAmtAfterLossTrigger')*(pl.col('EventLoss')/(pl.col('EventLALAE')))))
                                .otherwise(pl.col('EventCededAmtAfterLossTrigger'))
                                .alias('Event Ceded Loss'),
                                pl.when(pl.col('ALAE Handling')=="Included in the Limit")   #convert to loss and alae components
                                .then(pl.when(pl.col('EventCededAmtECO')==0)
                                        .then(pl.lit(0))
                                        .otherwise(pl.col('EventCededAmtECO')*(pl.col('EventECOLoss')/(pl.col('EventECOLALAE')))))
                                .otherwise(pl.col('EventCededAmtECO'))
                                .alias('Event Ceded ECOXPL Loss'),
                                pl.when(pl.col('ALAE Handling')=="Pro Rata")
                                .then(pl.when(pl.col('EventCededAmtAfterLossTrigger')==0)
                                        .then(pl.lit(0))
                                        .otherwise(pl.col('EventCededAmtAfterLossTrigger')*(pl.col('EventLALAE'))/pl.col('EventLoss')))
                                .otherwise(pl.col('EventCededAmtAfterLossTrigger'))
                                .alias('Event Ceded LALAE'),
                                pl.when(pl.col('ALAE Handling')=="Pro Rata")
                                .then(pl.when(pl.col('EventCededAmtECO')==0)
                                        .then(pl.lit(0))
                                        .otherwise(pl.col('EventCededAmtECO')*(pl.col('EventECOLALAE'))/pl.col('EventECOLoss')))
                                .otherwise(pl.col('EventCededAmtECO'))
                                .alias('Event Ceded ECOXPL LALAE')])                   
                .with_columns([(pl.col('Event Ceded LALAE')-pl.col('Event Ceded Loss')).alias('Event Ceded Expense'),
                                (pl.col('Event Ceded ECOXPL LALAE')-pl.col('Event Ceded ECOXPL Loss')).alias('Event Ceded ECOXPL Expense')])
                .drop(['TriggerAmt','TriggerAmtECO','EventTriggerAmt','EventTriggerAmtECO','EventTriggerAmtTotal','EventCededAmtAfterLossTrigger',
                    'EventCededAmtAfterTotalTrigger','EventCededAmtECO','Event Ceded LALAE','Event Ceded ECOXPL LALAE'])
                .with_columns([pl.when(pl.col('Event Ceded Loss')==0)
                                .then(pl.lit(0))
                                .otherwise(pl.col('Event Ceded Loss')*((pl.col('Subject Loss')/pl.col('EventLoss'))))
                                .alias('Ceded Loss'),
                                pl.when(pl.col('Event Ceded Expense')==0)
                                .then(pl.lit(0))
                                .otherwise((pl.col('Event Ceded Expense'))*((pl.col('Subject Loss and ALAE')-pl.col('Subject Loss'))/(pl.col('EventLALAE')-pl.col('EventLoss'))))
                                .alias('Ceded Expense'),
                                pl.when(pl.col('Event Ceded ECOXPL Loss')==0)
                                .then(pl.lit(0))
                                .otherwise(pl.col('Event Ceded ECOXPL Loss')*((pl.col('Subject ECOXPL Loss')/pl.col('EventECOLoss'))))
                                .alias('Ceded ECOXPL Loss'),
                                pl.when(pl.col('Event Ceded ECOXPL Expense')==0)
                                .then(pl.lit(0))
                                .otherwise((pl.col('Event Ceded ECOXPL Expense'))*(((pl.col('Subject ECOXPL Loss and ALAE')-pl.col('Subject ECOXPL Loss'))/(pl.col('EventECOLALAE')-pl.col('EventECOLoss')))))
                                .alias('Ceded ECOXPL Expense')])
                .drop(['Event Ceded Loss','Event Ceded Expense','Event Ceded ECOXPL Loss','Event Ceded ECOXPL Expense','EventLoss','EventLALAE','EventECOLoss','EventECOLALAE'])
                .with_columns((pl.col('Ceded Loss')+pl.col('Ceded Expense')).alias('Ceded Loss and ALAE'))
                .with_columns((pl.col('Ceded ECOXPL Loss')+pl.col('Ceded ECOXPL Expense')).alias('Ceded ECOXPL Loss and ALAE'))
                .drop(['Ceded Expense','Ceded ECOXPL Expense','Subject Loss','Subject Loss and ALAE','Subject ECOXPL Loss','Subject ECOXPL Loss and ALAE'])
                .rename({'Ceded Loss':'Subject Loss','Ceded Loss and ALAE':'Subject Loss and ALAE','Ceded ECOXPL Loss':'Subject ECOXPL Loss',
                        'Ceded ECOXPL Loss and ALAE':'Subject ECOXPL Loss and ALAE'}))           

    for i in range(1, numlevels + 1):
        #Has per claim
        dfSubjectFromGrossWithPerClm = (
            dfLossLayers.filter(
                (pl.col("Level") == i)
                & (pl.col("Underlying Layers").list.len() == 0)
                & (pl.col("HasPerClaim")==True)
            )
            .drop(["Underlying Layers", "Inuring Layers","Level"])
            .explode("Risk Sources")
            .rename({"Risk Sources": "Risk Source"})
        )

        #Has no per claim
        dfSubjectFromGrossWithNoPerClm = (
            dfLossLayers.filter(
                (pl.col("Level") == i)
                & (pl.col("Underlying Layers").list.len() == 0)
                & (pl.col("HasPerClaim")==False)
            )
            .drop(["Underlying Layers", "Inuring Layers","Level"])
            .explode("Risk Sources")
            .rename({"Risk Sources": "Risk Source"})
        )

        dfSubjectFromUL = (
            dfLossLayers.filter(
                (pl.col("Level") == i)
                & (pl.col("Underlying Layers").list.len() > 0)
            )
            .drop(["Inuring Layers","Level","HasPerClaim","Per Claim Retention","Per Claim Limit",
                'Deductible Treatment','ECOXPL Pct Covered','ECOXPL Treatment','Limit Including ECOXPL'])
            .explode("Underlying Layers")
            .explode("Risk Sources")
            .rename({"Risk Sources": "Risk Source"})
            .with_columns(pl.col("Underlying Layers").str.strip_chars().alias("Underlying Layers"))
            .rename({"Underlying Layers": "Underlying Layer"})
        )

        dfSubjectInuring = (
            (
                dfLossLayers.filter(
                    (pl.col("Level") == i)
                    & (pl.col("Inuring Layers").list.len() > 0)
                )
                .drop(["Underlying Layers","Loss Participation In","Level","HasPerClaim","Per Claim Retention","Per Claim Limit",
                    'Deductible Treatment','ECOXPL Pct Covered','ECOXPL Treatment','Limit Including ECOXPL'])
                .explode("Inuring Layers")
                .explode("Risk Sources")
                .rename({"Risk Sources": "Risk Source"})
                .with_columns(pl.col("Inuring Layers").str.strip_chars().alias("Inuring Layers"))
            )
            .rename(
                {"Inuring Layers": "Inuring Layer"}
            )
        )

        #Add to prior dfResults
            #Apply Agg after Applying Event 
                #To per clm results aggregated to event, 
                #combined with [non per-claim layers aggregated to event + underlying - inuring]

        temp1=(applyPerClaim(# First, from Gross Risk Sources filtered for event retention and apply Per Claim Layers
                                #Has Per Claim and Per Event
                                getSubject(dfSubjectFromGrossWithPerClm.join(
                                    specdict['Prepped Losses'],
                                    how="left",
                                    on="Risk Source"))
                            .with_columns([
                                ( pl.col("Subject Loss")* pl.col("Loss Participation In")).alias("Subject Loss"),
                                (pl.col("Subject Loss and ALAE") * pl.col("Loss Participation In")).alias("Subject Loss and ALAE"),
                                (pl.col("Subject ECOXPL Loss") * pl.col("Loss Participation In")).alias("Subject ECOXPL Loss"),
                                (pl.col("Subject ECOXPL Loss and ALAE") * pl.col("Loss Participation In")).alias("Subject ECOXPL Loss and ALAE")])
                            .drop('Loss Participation In'))
                            .select(['Layer', 'ALAE Handling', 'Per Event Limit', 'Per Event Retention', 'Loss Participation Out', 'HasPerEvent', 'Risk Source', 
                                    'Evaluation Date', 'Claim Number', 'Occurrence Number', 'Paid or Incurred', 'Trend', 'Subject Loss', 'Subject Loss and ALAE',
                                    'Subject ECOXPL Loss',  'Subject ECOXPL Loss and ALAE']))
    
        temp2=(getSubject(dfSubjectFromGrossWithNoPerClm.join(
                                                        specdict['Prepped Losses'],
                                                        how="left",
                                                        on="Risk Source"))
                                                        .drop(["Per Claim Limit",'Per Claim Retention','Limit Including ECOXPL','ECOXPL Treatment','HasPerClaim'])
                                                        .with_columns([
                                                            ( pl.col("Subject Loss")* pl.col("Loss Participation In")).alias("Subject Loss"),
                                                            (pl.col("Subject Loss and ALAE") * pl.col("Loss Participation In")).alias("Subject Loss and ALAE"),
                                                            (pl.col("Subject ECOXPL Loss") * pl.col("Loss Participation In")).alias("Subject ECOXPL Loss"),
                                                            (pl.col("Subject ECOXPL Loss and ALAE") * pl.col("Loss Participation In")).alias("Subject ECOXPL Loss and ALAE")])
                                                        .drop('Loss Participation In'))
        temp3=(dfSubjectFromUL.join(
                                                    dfResults,
                                                    how="left",
                                                    left_on=["Underlying Layer","Risk Source"],
                                                    right_on=["Layer","Risk Source"])
                                                .filter(pl.col("Ceded Loss").is_not_null())
                                                .rename({"Ceded Loss": "Subject Loss",
                                                        "Ceded Loss and ALAE": "Subject Loss and ALAE",
                                                        "Ceded ECOXPL Loss": "Subject ECOXPL Loss",
                                                        "Ceded ECOXPL Loss and ALAE": "Subject ECOXPL Loss and ALAE"})
                                                .with_columns( [
                                                    ( pl.col("Subject Loss")* pl.col("Loss Participation In")).alias("Subject Loss"),
                                                    (pl.col("Subject Loss and ALAE") * pl.col("Loss Participation In")).alias("Subject Loss and ALAE"),
                                                    (pl.col("Subject ECOXPL Loss") * pl.col("Loss Participation In")).alias("Subject ECOXPL Loss"),
                                                    (pl.col("Subject ECOXPL Loss and ALAE") * pl.col("Loss Participation In")).alias("Subject ECOXPL Loss and ALAE")])
                                                .drop('Loss Participation In','Underlying Layer'))   

        temp4=(dfSubjectInuring.join(
                                                    dfResults,
                                                    how="left",
                                                    left_on=["Inuring Layer","Risk Source"],
                                                    right_on=["Layer","Risk Source"],
                                                ))

        temp4=(temp4
                                                .filter(pl.col("Ceded Loss").is_not_null())
                                                .rename({"Ceded Loss": "Subject Loss",
                                                        "Ceded Loss and ALAE": "Subject Loss and ALAE",
                                                        "Ceded ECOXPL Loss": "Subject ECOXPL Loss",
                                                        "Ceded ECOXPL Loss and ALAE": "Subject ECOXPL Loss and ALAE"})
                                                .with_columns([(pl.col("Subject Loss")
                                                            * (-1.0))
                                                            .alias("Subject Loss"),
                                                            (pl.col("Subject Loss and ALAE")
                                                            * (-1.0))
                                                            .alias("Subject Loss and ALAE"),
                                                            (pl.col("Subject ECOXPL Loss")
                                                            * (-1.0))
                                                            .alias("Subject ECOXPL Loss"),
                                                            (pl.col("Subject ECOXPL Loss and ALAE")
                                                            * (-1.0))
                                                            .alias("Subject ECOXPL Loss and ALAE")])
                                                .drop('Inuring Layer'))    

        dfResults = (pl.concat([dfResults,
                                    applyPerEvent(
                                        pl.concat([temp1,
                                            #Combine non-per claim from gross LALAE, with Underlying layers,
                                            #Merge and subtract inuring
                                            pl.concat([temp2,temp3,temp4])
                                                .group_by(['Layer', 'ALAE Handling', 'Per Event Limit', 'Per Event Retention', 'Loss Participation Out', 'HasPerEvent', 
                                                        'Risk Source', 'Evaluation Date', 'Claim Number', 'Occurrence Number', 'Paid or Incurred', 'Trend']).sum()]
                                                ))
                                            #End apply event
                                        .select(['Layer',"Evaluation Date","Claim Number","Occurrence Number","Paid or Incurred","Trend","Risk Source",
                                                #"Policy Limit",'Loss Year','Policy Year','Custom A Year','Custom B Year','Has ECOXPL','Closed','Event Closed','ALAE Only',
                                                'Subject Loss','Subject Loss and ALAE',"Subject ECOXPL Loss","Subject ECOXPL Loss and ALAE"])   
                                        .rename({'Subject Loss':'Ceded Loss','Subject Loss and ALAE':'Ceded Loss and ALAE',
                                                'Subject ECOXPL Loss':'Ceded ECOXPL Loss','Subject ECOXPL Loss and ALAE':'Ceded ECOXPL Loss and ALAE'})
                                        .filter(pl.col('Ceded Loss and ALAE')+pl.col('Ceded ECOXPL Loss and ALAE')>0)]))

    dfResults=(dfResults.join(specdict['Claim Info'].select(['Claim Number','Loss Year','Policy Year','Custom A Year',
                                                                    'Custom B Year','Event Loss Year','Event Policy Year','Event Custom A Year',
                                                                    'Event Custom B Year']),
                                        how='left',on=['Claim Number'])
                                        .join(specdict['Layers'].select(['Layer','HasPerEvent']),
                                        how='left',on='Layer')
                                        .join(specdict['Prepped Losses'].select(['Claim Number','Evaluation Date','Closed','Event Closed']).unique(),
                                        how='left',on=['Claim Number','Evaluation Date'])
                                        .with_columns([pl.when(pl.col('HasPerEvent')==True)
                                                    .then(pl.col('Event Loss Year'))
                                                    .otherwise(pl.col('Loss Year'))
                                                    .alias('Loss Year'),
                                                    pl.when(pl.col('HasPerEvent')==True)
                                                    .then(pl.col('Event Policy Year'))
                                                    .otherwise(pl.col('Policy Year'))
                                                    .alias('Policy Year'),
                                                    pl.when(pl.col('HasPerEvent')==True)
                                                    .then(pl.col('Event Custom A Year'))
                                                    .otherwise(pl.col('Custom A Year'))
                                                    .alias('Custom A Year'),
                                                    pl.when(pl.col('HasPerEvent')==True)
                                                    .then(pl.col('Event Custom B Year'))
                                                    .otherwise(pl.col('Custom B Year'))
                                                    .alias('Custom B Year')])
                                        .drop(['Event Loss Year','Event Policy Year','Event Custom A Year','Event Custom B Year','HasPerEvent'])
                                        .with_columns([(pl.col('Ceded ECOXPL Loss and ALAE')-pl.col('Ceded ECOXPL Loss')).alias('Ceded ECOXPL Expense'),
                                                    (pl.col('Ceded Loss and ALAE')-pl.col('Ceded Loss')).alias('Ceded Expense')])
                                        .drop(['Ceded Loss and ALAE','Ceded ECOXPL Loss and ALAE'])
                                        .filter((pl.col('Ceded Loss')+pl.col('Ceded Expense')+pl.col('Ceded ECOXPL Loss')+pl.col('Ceded ECOXPL Expense'))>0)
                                        .with_columns([pl.when((pl.col('Ceded Loss')>0))
                                                    .then(pl.lit(1))
                                                    .otherwise(pl.lit(0))
                                                    .alias('Clm Ct - Loss'),
                                                    pl.when((pl.col('Ceded Loss')+pl.col('Ceded Expense')>0))
                                                    .then(pl.lit(1))
                                                    .otherwise(pl.lit(0))
                                                    .alias('Clm Ct - LALAE'),
                                                    pl.when((pl.col('Ceded ECOXPL Loss')>0))
                                                    .then(pl.lit(1))
                                                    .otherwise(pl.lit(0))
                                                    .alias('Clm Ct - ECOXPL Loss'),
                                                    pl.when((pl.col('Ceded ECOXPL Loss')+pl.col('Ceded ECOXPL Expense')>0))
                                                    .then(pl.lit(1))
                                                    .otherwise(pl.lit(0))
                                                    .alias('Clm Ct - ECOXPL LALAE')])
                                        .select(['Layer','Evaluation Date','Paid or Incurred','Trend','Claim Number','Occurrence Number','Risk Source',
                                                'Loss Year','Policy Year','Custom A Year','Custom B Year','Closed','Event Closed',
                                                'Ceded Loss','Ceded Expense','Ceded ECOXPL Loss','Ceded ECOXPL Expense',
                                                'Clm Ct - Loss','Clm Ct - LALAE','Clm Ct - ECOXPL Loss','Clm Ct - ECOXPL LALAE']))
    if isinstance(dfResults, pl.DataFrame):
            return dfResults.with_columns([pl.col('Ceded Loss').round(0).cast(pl.Int64),
                                            pl.col('Ceded Expense').round(0).cast(pl.Int64),
                                            pl.col('Ceded ECOXPL Loss').round(0).cast(pl.Int64),
                                            pl.col('Ceded ECOXPL Expense').round(0).cast(pl.Int64)])
    else:
        return "Error creating Ceded Layer Losses."    


def MultiClaimDetail(specdict):
    ages=calcDevAgeConversionTable(specdict)
    temp = (specdict['Prepped Losses'].filter((pl.col('Claim Number')!='Attritional')&(pl.col('Occ Ct: LALAE')>1))
            .with_columns([(pl.col('Net Indemnity')+pl.col('Net Medical')).alias('Loss'),
                        (pl.col('Net Indemnity')+pl.col('Net Medical')+pl.col('Net Expense')).alias('LALAE'),
                        (pl.col('Ct: Indemnity')+pl.col('Ct: Med Only')).alias('Ct: Loss')]))
    temp = (temp.sort(['Evaluation Date','Paid or Incurred','Trend','Occurrence Number','Event Policy Year','Event Loss Year','Event Custom A Year','Event Custom B Year','Loss'], descending=True)
            .select(['Evaluation Date','Paid or Incurred','Trend','Risk Source','Occurrence Number','Event Policy Year','Event Loss Year','Event Custom A Year','Event Custom B Year','Loss','Ct: Loss'])
            .group_by(['Evaluation Date','Paid or Incurred','Trend','Risk Source','Occurrence Number','Event Policy Year','Event Loss Year','Event Custom A Year','Event Custom B Year']).agg(pl.col('Loss').first().alias('Largest Loss'),
                                                                        pl.col('Loss').mean().round(0).cast(pl.Int64).alias('Average Loss'),                                                                      
                                                                        pl.col('Ct: Loss').sum().alias('Ct: Loss'))
            .join((temp.sort(['Evaluation Date','Paid or Incurred','Trend','Occurrence Number','Event Policy Year','Event Loss Year','Event Custom A Year','Event Custom B Year','LALAE'], descending=True)
            .select(['Evaluation Date','Paid or Incurred','Trend','Risk Source','Occurrence Number','Event Policy Year','Event Loss Year','Event Custom A Year','Event Custom B Year','LALAE','Ct: LALAE'])
            .group_by(['Evaluation Date','Paid or Incurred','Trend','Risk Source','Occurrence Number','Event Policy Year','Event Loss Year','Event Custom A Year','Event Custom B Year']).agg(pl.col('LALAE').first().alias('Largest LALAE'),
                                                                        pl.col('LALAE').mean().round(0).cast(pl.Int64).alias('Average LALAE'),                                                                      
                                                                        pl.col('Ct: LALAE').sum().alias('Ct: LALAE'))),
            how='left',on=['Evaluation Date','Paid or Incurred','Trend','Risk Source','Occurrence Number','Event Policy Year','Event Loss Year','Event Custom A Year','Event Custom B Year'])
            .with_columns(pl.lit(1).alias('Event Count'))
            .rename({'Event Policy Year':'Policy Year','Event Loss Year':'Loss Year','Event Custom A Year':'Custom A Year','Event Custom B Year':'Custom B Year'})
            .join(ages.rename({'Year':'Policy Year'}),on=['Evaluation Date','Policy Year'],how='left')
            .rename({'Age':'PY Age'})
            .join(ages.rename({'Year':'Loss Year'}),on=['Evaluation Date','Loss Year'],how='left')
            .rename({'Age':'LY Age'})
            .join(ages.rename({'Year':'Custom A Year'}),on=['Evaluation Date','Custom A Year'],how='left')
            .rename({'Age':'Custom A Age'})
            .join(ages.rename({'Year':'Custom B Year'}),on=['Evaluation Date','Custom B Year'],how='left')
            .rename({'Age':'Custom B Age'})
            .filter((pl.col('PY Age').is_not_null())|(pl.col('LY Age').is_not_null())|(pl.col('Custom A Age').is_not_null())|(pl.col('Custom B Age').is_not_null())))
    return temp

def modelSpecificAnalysisSteps(analysis):
    #analysis.preppedspecs=prepLossesAndClaimInfo(analysis.preppedspecs)
    
    analysis.preppedspecs["Gross Loss Summary"]=GrossLossSummary(analysis.preppedspecs)
    analysis.preppedspecs["Ceded Loss Summary"]=CededLossesAllLayers(analysis.preppedspecs)
    analysis.preppedspecs["Event Summary"]=MultiClaimDetail(analysis.preppedspecs)
    
    _misc.copyTableToSht(analysis.book,1,analysis.preppedspecs["Gross Loss Summary"],'Model Results','GrossLossSummary')
    _misc.copyTableToSht(analysis.book,1,analysis.preppedspecs["Ceded Loss Summary"],'Model Results','CededLossSummary')
    _misc.copyTableToSht(analysis.book,1,analysis.preppedspecs["Event Summary"],'Model Results','MultiClaimDetail')
    

