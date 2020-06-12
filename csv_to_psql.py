import os
import csv
from adapter import TwitterDBClient
from adapter import commands
################################################
pwd      = os.getcwd()
csv_pwd  = pwd[0:pwd.rfind('/')] 
csv_pwd += '/twitter_data/'
csv_list = os.listdir(csv_pwd)  
################################################
client = TwitterDBClient()
client.init_session()
client.create_table('tweet_data', commands)
print("Connected to database.\n")
for e in csv_list:
    entry_count = 0
    f_path      = csv_pwd + e
    result_set  = {}
    with open(f_path, newline='\n') as fp:
	    reader = csv.DictReader(fp)
	    try:
		    for row in reader:
			    keys = [key for key in row.keys()]
			    for key in keys:
				    try:
					    result_set[key] = row[key]
				    except:
					    print("Error: cannot parse row\n.")
					    pass
	    except:
		    continue
	    #client.insert_row('tweet_data', result_set)
	    client.insert_row_by_id('tweet_data', result_set['id'])
	    client.populate_row_data('tweet_data', result_set['id'], result_set)