import datetime
import re
from timeit import default_timer as timer
from datetime import timedelta
import csv,os,gzip
import pandas a

now = datetime.datetime.now()
year = '{:02d}'.format(now.year)
month = '{:02d}'.format(now.month)
day = '{:02d}'.format(now.day)
output_file='log_'+year+'_'+month+'_'+day+'.csv'

#for testing, year,month, and day parameters can be passed by the user. But the name of the file will be todays date because thats the intent of this script
def main(year=year,month=month,day=day):
    #start = timer()
    #output_file='log_'+year+'_'+month+'_'+day+'.csv'
    logging_regex = re.compile(r'^(\'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z) UTC \[ db=(.+) user=(.+) pid=(.+) userid=(.+) xid=(.+) ]\' LOG: (.+)$')
    
    data={}
    data.setdefault('timestamp', [])
    data.setdefault('database', [])
    data.setdefault('user_name', [])
    data.setdefault('process_id', [])
    data.setdefault('user_id', []) 
    data.setdefault('transaction_id', []) 
    data.setdefault('query', [])
    rootDir = 'redshift_starter_log_kit/us-east-1/'
    rootDir = rootDir+year+'/'+month+'/'+day+'/'
    for dirpath, dirs, files in os.walk(rootDir):
        for filename in files:
            #read_path=dirpath + '/' + filename
            if "useractivitylog" in filename:
                file_path=dirpath + filename
                #print(file_path)
                with gzip.open(file_path,'rt') as log_file:        
                    log_data = log_file.read()

                log_data = re.sub(r'(\'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z UTC)', '\n \\1', log_data)
                list_of_logs = re.split(r'(\'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z UTC.*)', log_data)
                list_of_logs = [x for x in list_of_logs if str(x) != '\n ' ]
                cleanedList = [x for x in list_of_logs if str(x) != '\n\n ' ]
            
                for individual_log in cleanedList:
                    timestamp = find_and_return_match(logging_regex, individual_log, return_group=1).strip("''")
                    database = find_and_return_match(logging_regex, individual_log, return_group=2)
                    user_name = find_and_return_match(logging_regex, individual_log, return_group=3)
                    process_id = find_and_return_match(logging_regex, individual_log, return_group=4)
                    user_id = find_and_return_match(logging_regex, individual_log, return_group=5)
                    transaction_id = find_and_return_match(logging_regex, individual_log, return_group=6)
                    query = find_and_return_match(logging_regex, individual_log, return_group=7)
                    data['timestamp'].append(timestamp)
                    data['database'].append(database)
                    data['user_name'].append(user_name)
                    data['process_id'].append(process_id)
                    data['user_id'].append(user_id)
                    data['transaction_id'].append(transaction_id)
                    data['query'].append(query)
    #with open(output_file, "w") as outfile:
     #   writer = csv.writer(outfile)
      #  writer.writerow(data.keys())
       # writer.writerows(zip(*data.values()))          
    #end = timer()
    #print(timedelta(seconds=end-start))       
    #df=pd.DataFrame(data) 
    return pd.DataFrame(data)
                

def find_and_return_match(compiled_regex, string_to_check, return_group=0):
    regex_match_object = compiled_regex.search(string_to_check)
    if regex_match_object:
        regex_string = regex_match_object.group(return_group)
    else:
        regex_string = ''
    return regex_string

def find_schema(query_str):
    substr_index = query_str.find("rwdex_raw")
    schema = query_str[substr_index:].split(".")[0].replace("\'", "").replace("\"", "")
    
    if " " in schema:
        schema = schema.split(" ")[0]

    return schema

def split_timestamp(raw_timestamp):
    new_datetime = str(raw_timestamp).replace("\'", " ").replace("T", " ").replace("Z", "")

    return new_datetime

def extract_usage(df):
    ### Print original data shape
    print ("Original shape:", df.shape)

    ## Limit to prod database and raw datasets
    df_subset = df.loc[df["database"] == "chminer"]
    df_subset = df_subset[df_subset["query"].str.contains("rwdex_raw")] # rwdex_cdm? version3 vs history 

    ## Remove admin usernames
    df_subset = df_subset[~df_subset["user_name"].str.contains('chminer|di_ops_admin|ch_tableau_user' )]
    #df_subset = df_subset[~df_subset["user_name"].str.contains("di_ops_admin")]
    #df_subset = df_subset[~df_subset["user_name"].str.contains("ch_tableau_user")]

    ## Create new column to capture schema
    df_subset["schema"] = df_subset["query"].str.lower()
    df_subset["schema"] = df_subset["schema"].apply(lambda x: find_schema(str(x)))
    
    print ("New shape:", df_subset.shape)
    return df_subset

#When ready, script can be deployed without the parameters, so that it automatically captures the latest log files
df=main(year='2019',month='07',day='01')
df_subset=extract_usage(df)
df_subset["timestamp"] = pd.to_datetime(df_subset["timestamp"].apply(lambda x: split_timestamp(x)))
df_subset.to_csv(output_file,sep=",", index=False)  
