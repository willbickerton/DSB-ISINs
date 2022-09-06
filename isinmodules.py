import calendar                                     ##process_years_func
from os.path import exists                          ##process_years_func
import pandas as pd                                 ##process_window_func
import time                                         ##process_a_file
import json                                         ##process_a_file
from pandas.io.json import json_normalize           ##process_a_file
import datetime
from openfigipy import OpenFigiClient

import multiprocessing



# various funcs

# all files function - returns a list of files + a list of errors
def process_years_func(toplevel, years, asset_classes):
    out_list = []
    fails = []
    for year in years:
        for month in range(1, 13):
            year = int(year)
            monthlength = calendar.monthrange(year, month)[1]       # monthrange returns two elements, this just grabs the length of the month in days
            ## construct day_date
            for day in range(1,monthlength):
                month_str = str(month)                          ## enforce month to be a string, just to be sure.
                month_str = month_str.zfill(2)                  ## now we can pad with a zero if needed
                day_str = str(day)
                day_str = day_str.zfill(2)
                # year + month_str + day_dtr
                day_date = str(year) + month_str + day_str
                for asset_class in asset_classes:
                
                    filename = asset_class + "-" + day_date

                    final_string = toplevel + "\\" + str(year) + "\\" + day_date + "\\" + asset_class + "\\" + filename

                    #print(final_string)
                    if exists(final_string):
                        out_list.append(final_string)
                    else:
                        #print(f"{final_string} does not exist")
                        fails.append(final_string)

    #print(len(out_list))
    return (out_list, fails, day_date)

# some files function, requires a start and end date (format?) and returns a list of thes files for those dates.



def generate_fx_stats(list_of_output):

    target_currency = 'LKR'

    new_target=0
    exp_target=0

    
    Pairs_Dict = {}

    #for line in list_of_output:
        #test.append(line['NotionalCurrency'])
        #test.append(line['OtherNotionalCurrency'])

    for line in list_of_output:
        if line['Status'] == "Expired":
            continue
        if line['InstrumentType'] != "Swap":          # ie for FX skip the stats for evrything except Options.
            continue
        Not_Curr = line['NotionalCurrency']
        Other_Not_Curr = line['OtherNotionalCurrency']
        

        CurrencyPair = Not_Curr+Other_Not_Curr 
        #if (line['NotionalCurrency']==target_currency or line['OtherNotionalCurrency']==target_currency) and line['Status']=="New":
        #    new_target+=1
        #if (line['NotionalCurrency']==target_currency or line['OtherNotionalCurrency']==target_currency) and line['Status']=="Expired":
        #    exp_target+=1
        
        if not CurrencyPair in Pairs_Dict:
            Pairs_Dict[CurrencyPair] = 1
        else:
            Pairs_Dict[CurrencyPair] +=1

    #set test down to unique currencies..
    #test = list(set(test))

    #print("wait")
    
    return (Pairs_Dict)


def generate_credit_stats(list_of_output):

    CDS_types={}
    Sovereign_ISINs={}

    for line in list_of_output:
        if line['Status'] == "Expired" or line['Status'] == "Updated":
            continue
        #if line['InstrumentType'] == "Swap" and line['UnderlyingIssuerType'] =="Sovereign":
        if "ReturnorPayoutTrigger" in line:
            if line['ReturnorPayoutTrigger'] == "Credit Default" and line['UnderlyingIssuerType'] =="Corporate":
            
                if "Underlying" in line:                ### Amends just for index.
                    temp_holder = line['Underlying']       ### and this
                    if "InstrumentISIN" in temp_holder:
                        ThisISIN = temp_holder['InstrumentISIN']  ### and this
                        if not ThisISIN in Sovereign_ISINs:
                            Sovereign_ISINs[ThisISIN] = 1
                        else:
                            Sovereign_ISINs[ThisISIN] += 1
                else:
                    print(line)

            ####### filter for CDS types
            #if "UnderlyingAssetType" in line:
            #    UnderlyingType = line['UnderlyingAssetType']
            #    UnderlyingType = str(UnderlyingType)

            #    if not UnderlyingType in CDS_types:
            #        CDS_types[UnderlyingType] = 1
            #    else:
            #        CDS_types[UnderlyingType] += 1
    
    return(Sovereign_ISINs)






def generate_rates_stats(list_of_output):
    
    ISOrates={}

    #print("wait and grab list of lines here")

    for line in list_of_output:
        if line['Status'] == "Expired" or line['Status'] == "Updated":
            continue
        #print(f"isin: {line['ISIN']}")
        #if line['ISOReferenceRate'] =="" or ISOReferenceRate not in line:
        #    continue

        

        if line['InstrumentType'] == "Swap":
            if "TermofContractUnit" in line:
                if not line['TermofContractUnit'] == "MNTH":
                    continue
                if "TermofContractValue" in line:
                    if line['TermofContractValue'] == 3 or line['TermofContractValue'] == 6:
                    #if line[]
                        if "ReferenceRate" in line:
                #print(f"isin: {line['ISIN']}  rate {line['ISOReferenceRate']}")
                #for key in line.keys():
                #    print(key)
                #    print(line[key])

                            RefRate = line['ReferenceRate']  #  ISOReferenceRate
                            RefRate = str(RefRate)
                #print(ISORefRate)
                            if not RefRate in ISOrates:
                                ISOrates[RefRate] = 1
                            else:
                                ISOrates[RefRate] += 1  

    #print("maybe?")     

    return(ISOrates)


def generate_commodities_stats(list_of_output):

    Commodity_prods={}
    
    for line in list_of_output:
        if line['Status'] == "Expired" or line['Status'] == "Updated":
            continue
        #print(f"isin: {line['ISIN']}")
        #if line['ISOReferenceRate'] =="" or ISOReferenceRate not in line:
        #    continue
        if line['InstrumentType'] == "Swap":
            if "BaseProduct" in line:
                product = line['BaseProduct']
                product = str(product)

                if not product in Commodity_prods:
                    Commodity_prods[product] = 1
                else:
                    Commodity_prods[product] += 1


    return(Commodity_prods)




def generate_equity_stats(list_of_output):

    ISINDict = {}
    #new_target ="blakety blank"
    #exp_target = "mnames hank"
    #test = "and helloo"
    
    for line in list_of_output:

        ThisISIN =""   # used for troubleshooting not null values passed back through multiprocessing handler.

        #print(line['UseCase'])
        #print(line['UnderlyingAssetType'])
        if line['Status'] == "Expired":
            continue
        #Not_Curr = line['NotionalCurrency']
        #Other_Not_Curr = line['OtherNotionalCurrency']
        #if line['UseCase'] == "Portfolio_Swap" or line['UseCase'] == "Portfolio_Swap_Single_Name" or line['UseCase'] == "Price_Return_Basic_Performance_Single_Name" or line['UseCase'] == "Price_Return_Basic_Performance_Single_Name_CFD" or line['UseCase'] == "Single_Index" or line['UseCase'] == "Single_Name":
        #if line['UseCase'] == "Portfolio_Swap" or line['UseCase'] == "Portfolio_Swap_Single_Name"  or line['UseCase'] == "Single_Index" or line['UseCase'] == "Single_Name":
        if line['UnderlyingAssetType'] == "Index":        ### change between "single stock" and "single_index" for comparison.
            
            if "Underlying" in line:                ### Amends just for index.
                temp_holder = line['Underlying']       ### and this
                if "UnderlyingInstrumentISIN" in temp_holder:
                    ThisISIN = temp_holder['UnderlyingInstrumentISIN']  ### and this
                #else:
                    #print(f"Found temp_holder error{temp_holder}")     ## shows there is UnderyluyingInstrumentIndex here, with lots of Other values.

           # if "UnderlyingInstrumentISIN" in line:                 # revert this for single stock
           #     ThisISIN = line['UnderlyingInstrumentISIN']        # revert this for single stock
                ThisISIN=str(ThisISIN)
                if ThisISIN not in ISINDict:
                    ISINDict[ThisISIN] = 1
                else:
                    ISINDict[ThisISIN] +=1
            #elif "Underlying" in line:
            #    Underlying_temp=(line['Underlying'])
            #print(ISINDict)

            #print(f"underlying_temp : {Underlying_temp}")
            #    if "UnderlyingInstrumentISIN" in Underlying_temp:
            #        ThisISIN = Underlying_temp['UnderlyingInstrumentISIN']  ##DONE TO UNPACK sINGLE_INDEX UNDERLYINGS.
            #    else:
            #        continue
            #else:
            #    continue
        
        


        #print (ThisISIN)

        #CurrencyPair = Not_Curr+Other_Not_Curr 
        #if (line['NotionalCurrency']==target_currency or line['OtherNotionalCurrency']==target_currency) and line['Status']=="New":
        #    new_target+=1
        #if (line['NotionalCurrency']==target_currency or line['OtherNotionalCurrency']==target_currency) and line['Status']=="Expired":
        #    exp_target+=1
        #if ThisISIN != "":
        #    test=str(ThisISIN)
        #else:
        #    print("hit wierd empty thisISIN")
        #    print(line)
        #    continue

       # if test != "":
       #     if test in ISINDict:
       #         ISINDict[test] = 1
       #     else:
       #         ISINDict[test] +=1
        
        dummy = "dummmy"
        #print(ISINDict)
    

    #print("wait")

    #return(new_target, exp_target, test, ISINDict)
    return(ISINDict)

def equity_strike_price_stats(list_of_output):
    
    Call_list=[]
    Put_list=[]

    for line in list_of_output:

        ThisISIN =""   # used for troubleshooting not null values passed back through multiprocessing handler.

        #filter out conditions..
        if line['Status'] == "Expired":
            continue
        if not line['UseCase'] == "Single_Index":
            continue
        if not line['OptionExerciseStyle'] =="EURO":
            continue
        #print("pause")

        if "Underlying" in line:                ### Amends just for index.
                temp_holder = line['Underlying']       ### and this
                if "UnderlyingInstrumentISIN" in temp_holder:
                    #ThisISIN = temp_holder['UnderlyingInstrumentISIN']  ### and this

        #if "UnderlyingInstrumentISIN" in line:         #index removed
            #print(line['UnderlyingInstrumentISIN'])
                    trimmed=str(temp_holder['UnderlyingInstrumentISIN'])
                    trimmed=trimmed[2:]
                    trimmed=trimmed[:12]
            #print(trimmed)
                    if trimmed == "US78378X1072":     # amazon  US0231351067        S&P500 US78378X1072
            #if line["UnderlyingInstrumentISIN"] ==
                        today = line['LastUpdateDateTime']
            
            # if exp_date - line= roughly 365
                        today = today[:-9]
            #print("wait")
                        today = datetime.datetime.strptime(today,'%Y-%m-%d')

                        Expiry_date = line['ExpiryDate']
                        Expiry_date = datetime.datetime.strptime(Expiry_date,'%Y-%m-%d')

                        days_to_expiry = Expiry_date - today

            
                        if days_to_expiry.days > int("330") and days_to_expiry.days < int("390"):
                    #if "StrikePriceType" in line:
                    #    temp_holder2 = line['StrikePriceType']

                    #    if "StrikePrice" in temp_holder2:
                    #        strike_price = temp_holder2['StrikePrice']
                            #if not temp_holder2['StrikePriceCurrency']=="USD":
                            #    continue
                    ##### section for earlier dates which didnt nest their JSON with StrikePriceType
                            strike_price = line['StrikePrice']
                
            # finally append onto call or put list, & return
                            if line['OptionType'] == "PUTO":
                                Put_list.append(strike_price)
                            if line['OptionType'] == "CALL":
                                Call_list.append(strike_price) 



    return(Call_list, Put_list)





def handle_expiry_dates(all_results):
    test_df = pd.DataFrame(all_results[0],columns=['Date','Filename','AssetClass','NewIsins','Expired','Days_to_Expiry','ISINs','Call_list','Put_list'])
    columnNames = ['Date'] 
    Expirydates_df = pd.DataFrame(columns=columnNames)

    test_df['Date'] = pd.to_datetime(test_df.Date, format='%Y%m%d')

    for idx, row in enumerate(test_df.itertuples(index=False)):
        Expiry_dates = row.Days_to_Expiry

        
        Expirydates_df = Expirydates_df.append(Expiry_dates, ignore_index=True)
        


        Expirydates_df['Date'][idx] = row.Date


    Expirydates_df.to_csv("Expirydates.csv")


    return()





def Equities_Output_Function(all_results, out_file):

    test_df = pd.DataFrame(all_results[0],columns=['Date','Filename','AssetClass','NewIsins','Expired','Days_to_Expiry','ISINs','Call_list','Put_List'])

    test_df['Date'] = pd.to_datetime(test_df.Date, format='%Y%m%d')

    columnNames = ['Date']      
    ISINs_df = pd.DataFrame(columns=columnNames)

    for idx, row in enumerate(test_df.itertuples(index=False)):
        ISIN = row.ISINs

        alist = list(ISIN.keys())
        new_list =[i.strip("[']") for i in alist]
        myvalues = ISIN.values()

        Final_dict = dict(zip(new_list, myvalues))
       

        

        ISINs_df = ISINs_df.append(Final_dict, ignore_index=True)
        ISINs_df['Date'][idx] = row.Date



    

    test = ISINs_df.shape[0]
    if test > 1:                ## this craps out with a single file, so we only perofrm the sum if we have more than one row in this df.
        s = ISINs_df.sum().sort_values(ascending=False, inplace=False)
        s = s.iloc[:50]             ## the filter for how many ISINS to lookup, the highest results for thia period.
        if len(s) > 1:
            pretty_dict = openfig_isin_lookup(s)

            pretty_series = pd.Series(pretty_dict)

            new_df = pd.concat([pretty_series,s], axis=1).reset_index()
            
            new_df.to_csv("Pretty_table.csv")

            isin_list = list(ISINs_df)

            isin_series = pd.Series(isin_list).value_counts()

            ISINs_df2 = pd.DataFrame(isin_list, columns=["ISIN"])
            ISINs_df2['code']=ISINs_df2['ISIN'].str.slice(stop=2) ## puts the country code into a new column cazlled code

            results_df = ISINs_df2['code'].value_counts()

            #ISINs_df2 = isin_series.to_csv
            
            #ISINs_df2.to_csv("ISINs2.csv")
            results_df.to_csv("ISINs2.csv")

    test_df = test_df.drop('ISINs', axis=1)     #drop all the isins, they dont display in excel.
    test_df.to_csv(out_file)





    #handle_expiry_dates(all_results)

    #print("wait")

    return()


def Rates_Output_function(all_results, out_file):
    test_df = pd.DataFrame(all_results[0],columns=['Date','Filename','AssetClass','NewIsins','Expired','Days_to_Expiry','ISOReferenceRate','Call_list','Put_list'])

    test_df['Date'] = pd.to_datetime(test_df.Date, format='%Y%m%d')

    columnNames = ['Date']
    isorates_df = pd.DataFrame(columns=columnNames)

    for idx, row in enumerate(test_df.itertuples(index=False)):
    
        isorate = row.ISOReferenceRate
        isorates_df = isorates_df.append(isorate, ignore_index=True)
        isorates_df['Date'][idx] = row.Date

    test_df = test_df.drop('Days_to_Expiry', axis=1)   ## remove this later
    test_df.to_csv(out_file)

    isorates_df.to_csv("Rates.csv")

    #handle_expiry_dates(all_results)

    return()


def Credit_Output_function(all_results, out_file):

    test_df = pd.DataFrame(all_results[0],columns=['Date','Filename','AssetClass','NewIsins','Expired','Days_to_Expiry','SovereignISINS','Call_list','Put_list'])

    columnNames = ['Date']
    credittypes_df = pd.DataFrame(columns=columnNames)

    for idx, row in enumerate(test_df.itertuples(index=False)):
    
        credit_type = row.SovereignISINS
        credittypes_df = credittypes_df.append(credit_type, ignore_index=True)
        credittypes_df['Date'][idx] = row.Date
    

    #################### Fancy date consversion to fix the excel insanity w00t! #####################
    credittypes_df['Date'] = pd.to_datetime(credittypes_df['Date'])
    credittypes_df['Date'] = credittypes_df['Date'].dt.strftime('%d/%m/%Y')

    test_df.to_csv(out_file)

    credittypes_df.to_csv("Credit.csv")

    #handle_expiry_dates(all_results)

    return()



def Commodities_Output_function(all_results, out_file):

    test_df = pd.DataFrame(all_results[0],columns=['Date','Filename','AssetClass','NewIsins','Expired','Days_to_Expiry','BaseProduct','Call_list','Put_list'])

    columnNames = ['Date']

    commodity_prods_df = pd.DataFrame(columns=columnNames)

    for idx, row in enumerate(test_df.itertuples(index=False)):
    
        commod_type = row.BaseProduct
        commodity_prods_df = commodity_prods_df.append(commod_type, ignore_index=True)
        commodity_prods_df['Date'][idx] = row.Date
        
    commodity_prods_df['Date'] = pd.to_datetime(commodity_prods_df['Date'])
    commodity_prods_df['Date'] = commodity_prods_df['Date'].dt.strftime('%d/%m/%Y')

    test_df.to_csv(out_file)

    commodity_prods_df.to_csv("Commods.csv")

    handle_expiry_dates(all_results)

    return()





def FX_output_function(all_results, out_file):
    test_df = pd.DataFrame(all_results[0],columns=['Date','Filename','AssetClass','NewIsins','Expired','Days_to_Expiry','Pairs'])

    #use a little date trick to format the date and set this column to date objects.
    test_df['Date'] = pd.to_datetime(test_df.Date, format='%Y%m%d')
    #test_df['Date2'] = test_df['Date'].dt.strftime('%d-%m-%y')
    #print(test_df)
    #test_df=test_df[test_df.Date.dt.weekday<5]  # magic for dropping weekend days - fx trading days
    #quickly set up a blank df with a column of Date
    columnNames = ['Date']      
    pair_df = pd.DataFrame(columns=columnNames)

    test_df.to_csv(out_file)

    #build a data structure to count occurences of each currency pair
    for idx, row in enumerate(test_df.itertuples(index=False)):
        #print(idx)
        #print(row.Date)
        pair = row.Pairs
        pair_df = pair_df.append(pair, ignore_index=True)
        pair_df['Date'][idx] = row.Date

    pair_df.to_csv("fx_pairs_out.csv")
    ##  filter for per column 
    # print(test_df.loc[test_df['AssetClass']=='Foreign_Exchange'])

    handle_expiry_dates(all_results)
    

    #print("wait")


    return ()



# function for dumping output. possibly into a CSV 


def openfig_isin_lookup(isin_dict):
    
    openfigi_apikey = '8fb61d9f-ddcb-470f-bcc3-22beb94bd2e0'  # Put API Key here
    
    ofc = OpenFigiClient()
    
    ofc.connect()

    #my_isin = 'US78378X1072'               'idValue': [] 
    #for this_isin in list_of_isins:
    list_of_isins = isin_dict.keys()

    #df = pd.DataFrame({'idType': ['ID_ISIN']})
    df = pd.DataFrame({'idValue':list_of_isins})

    #df['new_col'] = list_of_isins           # populate the one columns with all the isins in the list
    #df = pd.concat([df, ])
    df['idType'] = 'ID_ISIN'                # then fill out the other column, based on len(list)


    #print(df)

    result = ofc.map(df)

    #result = result.drop(result[result.result_number not '0'].index)        
    filtered_result = result[result.result_number == 0]         # ignore multiple results for ISIN, takes the first.
    #print(result.head())
    filtered_result = filtered_result[['q_idValue','status_code','name','securityDescription']]
    #print(filtered_result.columns.tolist())

    final_result = dict(zip(list_of_isins, filtered_result['name']))

    


    #print("wait")
    return(final_result)



def generate_basic_stats_2(list_of_output, day_date):
## stat 1 - collect a tally for all "New" isins for each day.
# tally_count for the moment is a dict of key = date and val = tally of new for that day
   

    tally=0
    expired=0

    ExpiryDict={}

    for line in list_of_output:
        #if line['UseCase'] != "Total_Return_Swap":          # ie for FX skip the exipiry stats for evrything except Options.
            #continue
        if line['Status']=="New":
            tally+= 1
        if line['Status']=="Expired": #and line['ExpiryDate'] == day_date:      commented bit could check that the exp date is in line with current date but would need to calculate yestarday date. TBC
            expired+= 1
            continue
        
        ## Enable the same per sub class filter here as used in the asset class stat generator (if used).
        
        ## function to tally the days_to_expiry counts for each ISIN to plot, also produce typical or average stats on per asset class basis?
        # LastUpdateDateTime:"2019-05-06T02:01:46
        # ExpiryDate:"2022-05-04"
        Creation_date = line['LastUpdateDateTime'] 
        Creation_date = Creation_date[:-9]
        #print("wait")
        Creation_date = datetime.datetime.strptime(Creation_date,'%Y-%m-%d')




        Expiry_date = line['ExpiryDate']
        Expiry_date = datetime.datetime.strptime(Expiry_date,'%Y-%m-%d')

        days_to_expiry = Expiry_date - Creation_date

        #print(f"days exp {days_to_expiry.days}  {Creation_date}  {Expiry_date}  {line['ISIN']} ")

        test = days_to_expiry.days


        if test not in ExpiryDict:
            ExpiryDict[test] = 1
        else:
            ExpiryDict[test] +=1



    #tally_count[day_date]= tally

    
    return tally, expired, ExpiryDict

