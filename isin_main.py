import pandas as pd
#import isinmodules
from sys import modules
#import isinmodules

from isinmodules import *


#included temporarily
import warnings
warnings.filterwarnings("ignore")


def mp_handler(mp_filelist_today):
## multi  processing handler, calls fast_process on each target ISIN file.
# pool can either be set manually (up to 10 safely
# *or* to the size of todays set of files. this is much slower tho.
    #poolsize = len(mp_filelist_today)
    poolsize = 8
    if __name__ == '__main__':
        
        #p = multiprocessing.Pool(poolsize)

        with multiprocessing.Pool(poolsize) as pool:
            results = pool.map(fast_process_a_file, mp_filelist_today)

        ## can we tell the pool to terminate all proces at this point?

        #print(f"whats in the two vars? {results}")

        return results


def process_window_func(toplevel, start_date, end_date, asset_classes):

    out_list =[]
    fails = []
    all_results =[]

    date_list = pd.date_range(start=start_date, end=end_date)

    for day_date in date_list:
        mp_filelist_today = []
        day_date = day_date.strftime("%Y%m%d")

        #print (day_date)
        #new_list.append(x.replace('-', ''))

        # cut x in half to form year and date date
        year = day_date[:4]
        #day_date = x[4:]
        # and also construct filename
        #print(f"{year}  {day_date}")
    
        for asset_class in asset_classes:
            filename = asset_class + "-" + day_date

            final_string = toplevel + "\\" + str(year) + "\\" + day_date + "\\" + asset_class + "\\" + filename

            #print(final_string)
            if exists(final_string):
                out_list.append(final_string)
                mp_filelist_today.append(final_string)      
            else:
            #print(f"{final_string} does not exist")
                fails.append(final_string)
    
        #print(f"Files for {day_date} are {mp_filelist_today}")

        ##### will try to call multiprocessing from here somehow?
        # the target function needs to be fast_process_a_file
        # the list of jobs is mp_file_list
        # pool ? is up to 6 process.
        #

        #results = mp_handler(mp_filelist_today)       ######### Enters MP handler here !!!!!
        #all_results.append(results)
        ## something comes back - need to re-construct into a data struct 
        #print("holdup")
    results = mp_handler(out_list)
    all_results.append(results)
    #print(len(out_list))
    #print("post mp wait:")
    return (all_results, fails)



def fast_process_a_file(filename):
    start_time = time.time()

    file_open = open(filename)

    #list_of_output = [ pd.json_normalize(json.loads(f)) for f in file_open ]   ## list comprehension to rescue!
    list_of_output = []
    Call_list = []
    Put_list = []

    for f in file_open:

        temp = json.loads(f)

        header = temp['Header']
        attributes = temp['Attributes']
        ISIN = temp['ISIN']
        Derived = temp['Derived']

        #final_dict = dict(header.items() + attributes.items() + ISIN.items() + Derived.items())
        final_dict = {k: v for d in (header, attributes, ISIN, Derived) for k, v in d.items()}      ## list comp to double unpack each of the 4 dictionarys in each raw line.

        list_of_output.append(final_dict)   ## push the per line dictionary onto a list of all the lines in this file]
    
    line_num = len(list_of_output)
    elapsed1 = (time.time() - start_time)
    #print(f"filename: {filename} lines: {line_num} ")
    #should probably push elapsed into a dict in case we want to look at how long each file takes.
    data1 = "blank"
    data2 = "also_blank"
    new_target ="blakety blank"
    exp_target = "mnames hank"
    # convert list of dicts into a dataframe or not? dunno yet...
    #return(list_of_output)
    split_list=filename.split("-")
    day_date=split_list[2]

    split_list=filename.split("\\")
    asset_class=split_list[8]
    tally, expired, days_to_expiry = generate_basic_stats_2(list_of_output, day_date)

    #target = generate_fx_stats(list_of_output, day_date)
    if asset_class == "Foreign_Exchange":
        data2 = generate_fx_stats(list_of_output)
    elif asset_class == "Equity": 
        data2 = generate_equity_stats(list_of_output)
        #Call_list, Put_list = equity_strike_price_stats(list_of_output)
        #print("equity")
    elif asset_class == "Rates":
        data2 = generate_rates_stats(list_of_output)
    elif asset_class == "Credit":
        data2 = generate_credit_stats(list_of_output)
    elif asset_class == "Commodities":
        data2 = generate_commodities_stats(list_of_output)
    
       



    #print("pause") 

    return [day_date, filename, asset_class, tally, expired, days_to_expiry, data2, Call_list, Put_list]    ## list
    #return [day_date] 

# function to build statistics into a timeseries
# original stats function - deprecated 




# eventually process a set of files using multi proc handler, each calling the above process_a_file













start_time = time.time()
#################################################################################################################
# uncomment various options here - simpler than switching between launch.json and flat config file for the moment
# operation mode switch - should be a boolean
all_files_switch = False    # set to True to process all files in one or more years. 
                            #Should be deprecated if config file or cmd line params implemented.

# Root file path
toplevel = (r'C:\Users\willb\Masters-FRIA\code\isin')

# Asset classes, can be tuned down to focus on a smaller subset of assets as required.
#asset_classes = ["Commodities", "Credit", "Equity", "Foreign_Exchange", "Other", "Rates"]
#asset_classes = ["Foreign_Exchange"]
#asset_classes = ["Equity"]
#asset_classes = ["Rates"]
asset_classes = ["Credit"]
#asset_classes = ["Commodities", "Foreign_Exchange"]
#asset_classes = ["Commodities"]

# Output filename
out_file = "credit_general_stats.csv"

# Start dates if defined, used for smaller snapshot of dates
##              MM/DD/YYY !!!
start_date = '01/01/2018'
end_date = '03/06/2022'

# List of years, used to calculate one or more full years data.
years = ['2017', '2018', '2019', '2020', '2021', '2022']

tally_count = {}

#################################################################################################################

master_df = pd.DataFrame(columns=['Date', 'ISIN.dailyrecords', 'ISIN.dailyNew', 'ISIN.dailyExpired','New_Currency_Count','Exp_Currency_Count'])

#master_df

if all_files_switch == True:

    file_list, fails, day_date = process_years_func(toplevel, years, asset_classes)

else:

    all_results, fails = process_window_func(toplevel, start_date, end_date , asset_classes)

#print("ping")


#for filename in file_list:
    #print("blah")
    ###list_of_output = fast_process_a_file(filename)     ## commented out as this is done in the first func now process_window_func

    # have all that days data in a frame.
    # temp stats -

    #master_df = generate_basic_statistics(results_df, day_date, master_df)
    #split_list=filename.split("-")
    #day_date=split_list[2]
    #next
    #daily_isin_count = generate_basic_stats_2(list_of_output, day_date, tally_count)



#print(all_results)



#elapsed = (time.time() - start_time)


##########################  commenting out with 4 #
#test_df = pd.DataFrame(all_results[0],columns=['Date','Filename','AssetClass','NewIsins','Expired','New_Currency_Count','Exp_Currency_count','currencies','Pairs'])

#use a little date trick to format the date and set this column to date objects.
####test_df['Date'] = pd.to_datetime(test_df.Date, format='%Y%m%d')

#test_df['Date2'] = test_df['Date'].dt.strftime('%d-%m-%y')

#print(test_df)


#test_df=test_df[test_df.Date.dt.weekday<5]  # magic for dropping weekend days - fx trading days

####columnNames = ['Date']
####pair_df = pd.DataFrame(columns=columnNames)

####test_df.to_csv(out_file)


####for idx, row in enumerate(test_df.itertuples(index=False)):
    #print(idx)
    #print(row.Date)
####    pair = row.Pairs
####    pair_df = pair_df.append(pair, ignore_index=True)
####    pair_df['Date'][idx] = row.Date



#### pair_df.to_csv("pairs_restore.csv")
##  filter for per column 
# print(test_df.loc[test_df['AssetClass']=='Foreign_Exchange'])

if asset_classes == ["Foreign_Exchange"]:
    FX_output_function(all_results, out_file)

if asset_classes == ["Equity"]:
    Equities_Output_Function(all_results, out_file)
    print("ok")

if asset_classes == ["Rates"]:
    Rates_Output_function(all_results, out_file)
#test_df.to_csv("wtf.csv")

if asset_classes == ["Credit"]:
    Credit_Output_function(all_results, out_file)

if asset_classes == ["Commodities"]:
    Commodities_Output_function(all_results, out_file)

elapsed = (time.time() - start_time)



print("fin")
print(f"total time={elapsed}")

## right! a handler for the mp routines. stolen from SO
# https://stackoverflow.com/questions/20887555/dead-simple-example-of-using-multiprocessing-queue-pool-and-locking
