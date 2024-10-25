#Dependencies
import xlwings as xw
import _misc
import os
import modelFunctions as aFns
import modelAnalysis as Analysis
import sys

USERID=''
CONNECTIONTYPE=1
EXCELFILE=''
MODELTYPE='Reinsurance Strategy Analysis'
BOOK=None
analysis=None

#MAIN CODE BELOW
def main():
    global BOOK,EXCELFILE,CONNECTIONTYPE,MODELTYPE

    try:
        BOOK=xw.Book.caller()
        EXCELFILE=BOOK.sheets["File Paths"].range("_thisfilefullname").value
    except:
        try:
            EXCELFILE = _misc.selectAnalysisFileOrFolder(CONNECTIONTYPE,MODELTYPE,None)
            if EXCELFILE is not None:
                BOOK=xw.Book(EXCELFILE) 
        except:
            return

    try:
        if sys.argv[1] in ['RunAnalysis']:
            RunStep(sys.argv[1])
        else:
            RunStep("RunAnalysis")        
    except:
        pass

def RunAnalysis(connectiontype,modeltype,book,excelfile):
    global analysis
    excelfile=EXCELFILE.replace(os.sep,"/")
    analysis= Analysis.Analysis(connectiontype,modeltype,book,excelfile)

    if len(analysis.error)>0:
        analysis.book.sheets["Navigation"].range("runstatus").value="Model Update Failed"
        analysis.showMessageBox('Error',analysis.error,True)
    else:
        try:
            #Include function in modelFunctions.py for any model specific analysis steps
            aFns.modelSpecificAnalysisSteps(analysis)
        except:
            pass    

        analysis.book.sheets["Navigation"].range("runstatus").value="Model Update Successful"
        analysis.showMessageBox('Status','Model Run Complete.  Data Model will update after OK.',False)

# Enable this function for EXE version
def RunStep(arg1):
    if arg1 == "RunAnalysis":
        RunAnalysis(1,MODELTYPE,BOOK,EXCELFILE)
    else:
        pass
     
if __name__ == "__main__":
    main()