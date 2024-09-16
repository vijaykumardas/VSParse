import json
import requests
import ast
from os.path import exists
import csv
import logging
import progressbar
import datetime
from lxml import html
import copy
import time
from bs4 import BeautifulSoup
import dropbox
logging.basicConfig(filename="ValueStocksProcess.Log",level=logging.DEBUG,format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',datefmt='%d-%b-%y %H:%M:%S')


# Dropbox access token (replace with your actual token)
DROPBOX_ACCESS_TOKEN = 'sl.B8-Etfrh5db67j0qETTuSo8IHORhTRENqPA8LYq09i5yb6MNTNgIK2J8l5t85L-jgguBxZyOQxkZzEJYKFA_mjOF0rZEqt95LkOu4dYVQ84-YgIsWZ4PyUzyiz4paLiFMZX5uOzCKJYM'

def upload_to_dropbox(file_path, dropbox_path):
    """Uploads the file at file_path to Dropbox at dropbox_path."""
    dbx = dropbox.Dropbox(DROPBOX_ACCESS_TOKEN)
    
    with open(file_path, 'rb') as f:
        dbx.files_upload(f.read(), dropbox_path)
    
    print(f"File {file_path} successfully uploaded to Dropbox at {dropbox_path}")
    logging.debug(f"File {file_path} successfully uploaded to Dropbox at {dropbox_path}")

def GetNseEquityData():
    NSE_Equity_List_csv_url="https://archives.nseindia.com/content/equities/EQUITY_L.csv"
    nse_Master_Equity_List_File='01.MASTER_EQUITY_L.CSV'
    file_exists = exists(nse_Master_Equity_List_File)
    if(file_exists):
        print(nse_Master_Equity_List_File + " Found.")
    else:
        print(nse_Master_Equity_List_File + " not Found. Hence Downloading")
        req = requests.get(NSE_Equity_List_csv_url)
        url_content = req.content
        csv_file = open(nse_Master_Equity_List_File, 'wb')
        csv_file.write(url_content)
        csv_file.close()
        print(nse_Master_Equity_List_File + " Saved.")
    with open(nse_Master_Equity_List_File, 'r') as file:
        reader = csv.DictReader(
            file, fieldnames=['SYMBOL','NAME OF COMPANY','SERIES','DATE OF LISTING','PAID UP VALUE','MARKET LOT','ISIN NUMBER','FACE VALUE'])
        data = list(reader)
        return data[1:len(data)]



def GetStockInfoFromDLevels(NseMasterRow):
    # some JSON:
    urlFormat='https://ws.dlevels.com/get-autosearch-stock?term={NseCode}&pageName='
    url=urlFormat.format(NseCode=NseMasterRow["SYMBOL"])
    #print(url)
    response = session.get(url)
    if(response.status_code==200):
        #print(response.text)
        responseJson=response.text

        # parse x:
        y = json.loads(responseJson)
        if(y['response']!=[]):
            # the result is a Python dictionary:
            #print(y['response'][0])
            #print(y['response'][0]['Symbol_Name'])
            foundItem=None
            for item in y['response']:
                #print(item)
                if(item["EXCHANGE_NAME"]==NseMasterRow["SYMBOL"]):
                    foundItem=item
                    break
            if(foundItem is not None):
                dictInfo= {"SYMBOL":NseMasterRow["SYMBOL"],"NAME":NseMasterRow["NAME OF COMPANY"],"DLEVEL_KEY":foundItem['Symbol_Name'].replace(' ','_')}
                #print(dictInfo)
                return dictInfo
    else:
        print("Error")
def BuildAndSaveDLevelBasicInfo():
    nseEquityData=GetNseEquityData() 
    logging.debug(nseEquityData)
    Master_Equity_l_w_Dlevel_info='02.MASTER_EQUITY_L_W_DLEVEL_INFO.CSV'
    file_exists = exists(Master_Equity_l_w_Dlevel_info)
    csv_columns=['SYMBOL','NAME','DLEVEL_KEY']
    if(file_exists):
        print("DLevelBasicInfo File : "+Master_Equity_l_w_Dlevel_info + " Found.")
    else:
        print("DLevelBasicInfo File : "+Master_Equity_l_w_Dlevel_info + " Not Found. Hence Building...")
        dLevelInfo=[]
        widgets = [' [',progressbar.Timer(format= 'Building DLevel Stock Info: %(elapsed)s'),'] ', progressbar.Bar('*'),' (',progressbar.Counter(format='%(value)02d/%(max_value)d'), ') ',]
 
        bar = progressbar.ProgressBar(max_value=len(nseEquityData),widgets=widgets).start()
        logging.debug("Total Symbols to Process : "+str(len(nseEquityData)))
        progressCounter=0
        for row in nseEquityData:
            try:
                #print(row)
                if(row["SERIES"]=='EQ' or row["SERIES"]=="BE"):
                    logging.debug("Getting StockInfo from DLevel for :"+row["SYMBOL"])
                    dLevelInfoRow=GetStockInfoFromDLevels(row)
                    if(dLevelInfoRow != None):
                        dLevelInfo.append(GetStockInfoFromDLevels(row))
                else:
                    logging.debug("Skipping "+row["SYMBOL"]+" Since the Series is not EQ or BE. The Symbol is :"+row["SERIES"])
            except Exception as Argument:
                logging.debug("Exception While getting StockInfo from DLevel for "+str(row["SYMBOL"])+". Exception="+str(Argument))
            finally:
                progressCounter+=1
                bar.update(progressCounter)
                time.sleep(1/50)
                logging.debug("Symbols Processed : "+str(progressCounter))
        try:
            if(len(dLevelInfo) > 0):
                with open(Master_Equity_l_w_Dlevel_info, 'w', newline='') as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
                    writer.writeheader()
                    for data in dLevelInfo:
                        writer.writerow(data)
                print("DLevelBasicInfo has been Written to : "+Master_Equity_l_w_Dlevel_info)
                logging.debug("DLevelBasicInfo has been Written to : "+Master_Equity_l_w_Dlevel_info)
            else:
                print("DLevelBasicInfo Could not be  Written to : "+Master_Equity_l_w_Dlevel_info + ". Since No Data")
                logging.debug("DLevelBasicInfo Could not be  Written to : "+Master_Equity_l_w_Dlevel_info + ". Since No Data")
        except Exception as Argument:
            logging.debug("DLevelBasicInfo Could not be  Written to : "+Master_Equity_l_w_Dlevel_info + ". Due to Exception: "+Argument)
        #print(dLevelInfo)
    file_exists = exists(Master_Equity_l_w_Dlevel_info)
    if(file_exists):
        with open(Master_Equity_l_w_Dlevel_info, 'r') as file:
            reader = csv.DictReader(
                file, fieldnames=csv_columns)
            data = list(reader)
            return data[1:len(data)]
            
'''
Following Method is not in Use.
'''
def GetStockAdvancedInfoFromDLevels(BasicInfoRow):
    # Request the page
    pageBasicFundamentals = session.get('https://www.valuestocks.in/en/fundamentals-nse-stocks/lti_is_equity')
     
    # Parsing the page
    # (We need to use page.content rather than
    # page.text because html.fromstring implicitly
    # expects bytes as input.)
    tree = html.fromstring(pageBasicFundamentals.content) 
     
    # Get element using XPath
    Sector              =   tree.xpath('//*[@id="app"]/div[3]/div[1]/div[2]/div/div[1]/div/div/div[1]/span/text()')
    MarketCapElement    =   tree.xpath('//*[@id="app"]/div[3]/div[1]/div[2]/div/div[1]/div/div/div[2]/span/text()')
    x = MarketCapElement[0].replace('\r','').replace('\n','')
    x=" ".join(x.split()).split('(')
    MarketCapText=x[0]
    MarketCapNum=x[1].replace("Cr)",'')

    FundamentalScore    =   tree.xpath('//*[@id="app"]/div[3]/div[1]/div[2]/div/div[3]/div/div/div[1]/h4/text()')
    FundamentalsText    =   tree.xpath('//*[@id="app"]/div[3]/div[1]/div[2]/div/div[2]/div/div/div[2]/h4/text()')
    ValuationRange      =   tree.xpath('//*[@id="app"]/div[3]/div[1]/div[2]/div/div[4]/div/div/div[1]/h4/text()')
    ValuationText       =   tree.xpath('//*[@id="app"]/div[3]/div[1]/div[2]/div/div[4]/div/div/div[2]/h4/text()')
    PePs                =   tree.xpath('//*[@id="app"]/div[3]/div[1]/div[2]/div/div[4]/div/div/div[2]/h4/text()[3]')
    PePsValues=PePs[0].split('|')
    PriceToEarning      = PePsValues[0].replace("P/E: ",'').replace(' ','')
    PriceToSales        = PePsValues[1].replace("P/S: ",'').replace(' ','')
def GetStockAdvancedInfoFromDLevels1(row):
    rowBackup=copy.deepcopy(row)
    logging.debug("START: Fetching Advanced Info for :"+rowBackup["SYMBOL"]+" having dlevelKey:"+rowBackup["DLEVEL_KEY"])
    # some JSON:
    try:
        #1. Get Info from the Web Service Call.
        urlFormat='https://ws.dlevels.com/vs-api?platform=web&action=Fundamental%20Report&param_list={dLevel_Key}'
        url=urlFormat.format(dLevel_Key=rowBackup["DLEVEL_KEY"].replace("_","%20"))
        logging.debug("Fetching Advanced Info using url:"+url)
        response = session.get(url)
        if(response.status_code==200):
            responseJson=response.text
            y = json.loads(responseJson)
            if(y['response']!=[] and len(y['response'])==2):
                rowBackup.update(y['response'][1][0])
        #2. Get the Info from Parsing the Data.
        #pageBasicFundamentalsFormat = "https://www.valuestocks.in/en/fundamentals-nse-stocks/{dLevelKey}"
        #pageBasicFundamentalsUrl=pageBasicFundamentalsFormat.format(dLevelKey=rowBackup["DLEVEL_KEY"])
        #pageResponse=session.get(pageBasicFundamentalsUrl)
        #tree = html.fromstring(pageResponse.content) 
        #Sector              =   tree.xpath('//*[@id="app"]/div[3]/div[1]/div[2]/div/div[1]/div/div/div[1]/span/text()')
        ValuationRange      =   "0-0"#tree.xpath('//*[@id="app"]/div[3]/div[1]/div[2]/div/div[4]/div/div/div[1]/h4/text()')
        
        # Retrieving Additional Valuation Information
        #valuationUrlFormat="https://www.valuestocks.in/en/stocks-valuation/{dLevel_Key}"
        #valuationurl=valuationUrlFormat.format(dLevel_Key=rowBackup["DLEVEL_KEY"])
        #responseValuation=session.get(valuationurl)
        #try:
        #    if(response.status_code==200):
        #        soup=BeautifulSoup(responseValuation.content,'html.parser')
        #        s=soup.find_all('td',class_="stock_data_algnmnt")
        ValuationAsPerDCF="0"#s[2].text
        ValuationAsPerGraham="0"#s[3].text
        ValuationAsPerEarning="0"#s[4].text
        ValuationAsPerBookValue="0"#s[5].text
        ValuationAsPerSales="0"#s[6].text
        SectorPE="0"#s[10].text
        #else:
        #        logging.debug("Error Response from Url:"+valuationurl)
        #except Exception as Argument:
        #    logging.debug("Exception during Reading Valuation Data"+Argument)
        
        return {
        "SYMBOL":rowBackup["SYMBOL"],
        "NAME":rowBackup["NAME"],
        "SECTOR":rowBackup["SECTOR"],
        "CMP":rowBackup["LastClose"],
        "VALUATION":rowBackup["valuation"],
        "FAIRRANGE":ValuationRange,
        "PE":rowBackup["Pe"],
        "SECTORPE":SectorPE,
        "MARKETCAP":rowBackup["MarketCap"],
        "MKCAPTYPE":rowBackup['MkCapType'],
        "TREND":rowBackup["technical_trend"],
        "FUNDAMENTAL":rowBackup["stock_fundamental"],
        "MOMENTUM":rowBackup["price_momentum"],
        "DERATIO":rowBackup["Deratio"],
        "PRICETOSALES":rowBackup["PriceToSales"],
        "PLEDGE":rowBackup["Pledge"],
        "QBS":rowBackup["Qbs"].replace("/","(")+")" if len(rowBackup["Qbs"])>0 else rowBackup["Qbs"],
        "QBS%":rowBackup["qbs_perc"],
        "AGS":rowBackup["Ags"].replace("/","(")+")" if len(rowBackup["Ags"])>0 else rowBackup["Ags"],
        "AGS%":rowBackup["ags_perc"],
        "VALUATION_DCF":ValuationAsPerDCF,
        "VALUATION_GRAHAM":ValuationAsPerGraham,
        "VALUATION_EARNING":ValuationAsPerEarning,
        "VALUATION_BOOKVALUE":ValuationAsPerBookValue,
        "VALUATION_SALES":ValuationAsPerSales
        
        }
    except Exception as Argument:
        logging.debug("ERROR: Error Fetching Advanced Info for :"+rowBackup["SYMBOL"]+" having dlevelKey:"+rowBackup["DLEVEL_KEY"])
        logging.debug("Exception: "+str(Argument))
    finally:
        logging.debug("FINISHED: Fetching Advanced Info for :"+rowBackup["SYMBOL"]+" having dlevelKey:"+rowBackup["DLEVEL_KEY"])

def BuildAndSaveAdvancedDLevelInfo():
    now = datetime.datetime.now()
    nseEquityData = BuildAndSaveDLevelBasicInfo()
    
    if len(nseEquityData) > 0:
        print("DLevel Basic Info available, Proceeding to Build Advance Info Sheet")
        logging.debug("DLevel Basic Info available, Proceeding to Build Advance Info Sheet")
    else:
        print("DLevel Basic Info not available, Check if 02.MASTER_EQUITY_L_W_DLEVEL_INFO.CSV Exists and Contains the data")
        logging.debug("DLevel Basic Info not available, Check if 02.MASTER_EQUITY_L_W_DLEVEL_INFO.CSV Exists and Contains the data")
        return
    
    Dlevel_Advanced_info = now.strftime("%Y%m%d-%H%M%S") + '-3.DLEVEL_ADVANCED_INFO.CSV'
    Dlevel_Failed_Info = now.strftime("%Y%m%d-%H%M%S") + "-3.DLEVEL_ADVANCED_INFO_FAILURE.CSV"
    csv_columns = ["SYMBOL", "NAME", "SECTOR", "CMP", "VALUATION", "FAIRRANGE", "PE", "SECTORPE", "MARKETCAP", "MKCAPTYPE", "TREND", "FUNDAMENTAL", "MOMENTUM", "DERATIO", "PRICETOSALES", "PLEDGE", "QBS", "QBS%", "AGS", "AGS%", "VALUATION_DCF", "VALUATION_GRAHAM", "VALUATION_EARNING", "VALUATION_BOOKVALUE", "VALUATION_SALES"]
    
    dLevelInfo = []
    dLevelInfoFailure = []

    # Fetch advanced stock information
    for row in nseEquityData:
        try:
            logging.debug("Processing Advanced Data for :" + row["SYMBOL"])
            dLevelInfoRow = GetStockAdvancedInfoFromDLevels1(row)
            if dLevelInfoRow != None:
                dLevelInfo.append(dLevelInfoRow)
            else:
                dLevelInfoFailure.append(row)
                logging.debug("Unable to Get Advance Stock Info for Symbol:" + row["SYMBOL"])
        except Exception as Argument:
            dLevelInfoFailure.append(row)
            logging.debug("Some Exception while fetching the Advanced Info for :" + row["SYMBOL"])
            logging.debug("Exception: " + str(Argument))

    # Writing advanced stock info to CSV
    try:
        if len(dLevelInfo) > 0:
            with open(Dlevel_Advanced_info, 'w', newline='') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
                writer.writeheader()
                for data in dLevelInfo:
                    writer.writerow(data)
            logging.debug("DLevelAdvancedInfo has been Written to: " + Dlevel_Advanced_info)

            # Uploading the generated CSV to Dropbox
            dropbox_path = f"/NSEBSEBhavcopy/ValueStocks/"  # Adjust the Dropbox folder path as needed
            upload_to_dropbox(Dlevel_Advanced_info, dropbox_path)

        else:
            logging.debug("No data to write for Advanced Info CSV")

    except IOError:
        logging.debug("I/O error while writing to " + Dlevel_Advanced_info)

    # Handle failures (if any) for logging purposes
    try:
        if len(dLevelInfoFailure) > 0:
            with open(Dlevel_Failed_Info, 'w', newline='') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=["SYMBOL", "NAME", "DLEVEL_KEY"])
                writer.writeheader()
                for data in dLevelInfoFailure:
                    writer.writerow(data)
            logging.debug("Dlevel_Failed_Info has been Written to: " + Dlevel_Failed_Info)
    except IOError:
        logging.debug("I/O error while writing to " + Dlevel_Failed_Info)

    
    
#row={"SYMBOL":"LTIM","NAME":"LTIMindtree Limited","DLEVEL_KEY":"lti_is_equity"}
#GetStockAdvancedInfoFromDLevels1(row)
session = requests.Session()
#BuildAndSaveDLevelBasicInfo()
BuildAndSaveAdvancedDLevelInfo()