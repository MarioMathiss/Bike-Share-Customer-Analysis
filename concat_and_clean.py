import pandas as pd
import os
import glob
import pandas as pd
from pandas.io.parsers import read_csv


## CONCAT Ride Trip Data Files (PRE Format A)
os.chdir(r"path")

file_ext = ".csv"
all_file_names_pre = [i for i in glob.glob(f"*{file_ext}")]
#df = pd.read_csv(all_file_names_pre[1])
df_pre_A = pd.concat([pd.read_csv(file) for file in all_file_names_pre])


## CONCAT Ride Trip Data Files (PRE Format B)
os.chdir(r"path")

file_ext = ".csv"
all_file_names_pre = [i for i in glob.glob(f"*{file_ext}")]
#df = pd.read_csv(all_file_names_pre[1])
df_pre_B = pd.concat([pd.read_csv(file) for file in all_file_names_pre])


## CONCAT Ride Trip Data Files (POST)
os.chdir(r"path")

file_ext = ".csv"
all_file_names_post = [i for i in glob.glob(f"*{file_ext}")]
#df = pd.read_csv(all_file_names_post[1])
df_post = pd.concat([pd.read_csv(file) for file in all_file_names_post])


## CONCAT Station Data Files (Station)

os.chdir(r"path")

file_ext = ".csv"
all_file_names_station = [i for i in glob.glob(f"*{file_ext}")]
#df = pd.read_csv(all_file_names_station[1])
df_stations = pd.concat([pd.read_csv(file) for file in all_file_names_station])



# Convert Start and stop time columns to datetime on the pre df [Jan 1, 2014 to Dec 31, 2017 formated differently]
df_pre_B['start_time'] = pd.to_datetime(df_pre_B['start_time'])
df_pre_B['starttime'] = pd.to_datetime(df_pre_B['starttime'])
df_pre_B['stoptime'] = pd.to_datetime(df_pre_B['stoptime'])
df_pre_B['end_time'] = pd.to_datetime(df_pre_B['end_time'])

# Merge Format A and B to pre df
df_pre = pd.concat([df_pre_A, df_pre_B])
df_pre.columns


## Merging Start and end time
# Merge of ride_start_time columns for PRE Dataframe (3 different column names, final column name: ride_start_time)
df_pre['col1_start_time'] = df_pre['starttime'].where(df_pre['starttime'].notnull(), df_pre['start_time'])
df_pre['ride_start_time'] = df_pre['col1_start_time'].where(df_pre['col1_start_time'].notnull(), df_pre['01 - Rental Details Local Start Time'])

df_pre = df_pre.drop(columns=['starttime', 'start_time', '01 - Rental Details Local Start Time', 'col1_start_time'])

# Merge of ride_stop_time columns for PRE Dataframe (3 different column names, final column name: ride_stop_time)
df_pre['col1_end_time'] = df_pre['stoptime'].where(df_pre['stoptime'].notnull(), df_pre['end_time'])
df_pre['ride_stop_time'] = df_pre['col1_end_time'].where(df_pre['col1_end_time'].notnull(), df_pre['01 - Rental Details Local End Time'])

df_pre = df_pre.drop(columns=['stoptime', 'end_time', '01 - Rental Details Local End Time', 'col1_end_time'])

## Mege Other Columns
# Merge of ride_id columns for PRE Dataframe (2 different column names| final column name: (ride_id) and remove columns with null values
df_pre['ride_id'] = df_pre['trip_id'].where(df_pre['trip_id'].notnull(), df_pre['01 - Rental Details Rental ID'])

df_pre = df_pre.drop(columns=['trip_id', '01 - Rental Details Rental ID'])

# Merge of bike_id columns for PRE Dataframe (2 different column names| final column name: (bike_id) and remove columns with null values
df_pre['bike_id'] = df_pre['bikeid'].where(df_pre['bikeid'].notnull(), df_pre['01 - Rental Details Bike ID'])

df_pre = df_pre.drop(columns=['bikeid', '01 - Rental Details Bike ID'])

# Merge of from_station_id columns for PRE Dataframe (2 different column names| final column name: (from_station_id) and remove columns with null values
df_pre['from_station_id'] = df_pre['from_station_id'].where(df_pre['from_station_id'].notnull(), df_pre['03 - Rental Start Station ID'])

df_pre = df_pre.drop(columns=['03 - Rental Start Station ID'])

# Merge of from_station_name columns for PRE Dataframe (2 different column names| final column name: (from_station_name) and remove columns with null values
df_pre['from_station_name'] = df_pre['from_station_name'].where(df_pre['from_station_name'].notnull(), df_pre['03 - Rental Start Station Name'])

df_pre = df_pre.drop(columns=['03 - Rental Start Station Name'])

# Merge of to_station_id columns for PRE Dataframe (2 different column names| final column name: (to_station_id) and remove columns with null values
df_pre['to_station_id'] = df_pre['to_station_id'].where(df_pre['to_station_id'].notnull(), df_pre['02 - Rental End Station ID'])

df_pre = df_pre.drop(columns=['02 - Rental End Station ID'])

# Merge of to_station_name columns for PRE Dataframe (2 different column names| final column name: (to_station_name) and remove columns with null values
df_pre['to_station_name'] = df_pre['to_station_name'].where(df_pre['to_station_name'].notnull(), df_pre['02 - Rental End Station Name'])

df_pre = df_pre.drop(columns=['02 - Rental End Station Name'])

# Merge of user_type columns for PRE Dataframe (2 different column names| final column name: (user_type) and remove columns with null values
df_pre['user_type'] = df_pre['usertype'].where(df_pre['usertype'].notnull(), df_pre['User Type'])

df_pre = df_pre.drop(columns=['User Type', 'usertype'])

# Merge of gender columns for PRE Dataframe (2 different column names| final column name: (gender) and remove columns with null values [high number of missing observations (5,004,980)]
df_pre['gender'] = df_pre['gender'].where(df_pre['gender'].notnull(), df_pre['Member Gender'])

df_pre = df_pre.drop(columns=['Member Gender'])

# Merge of trip_time_s columns for PRE Dataframe (2 different column names| final column name: (trip_time_s) and remove columns with null values
df_pre['trip_time_s'] = df_pre['tripduration'].where(df_pre['tripduration'].notnull(), df_pre['01 - Rental Details Duration In Seconds Uncapped'])

df_pre = df_pre.drop(columns=['tripduration', '01 - Rental Details Duration In Seconds Uncapped'])

# Merge of birth_year columns for PRE Dataframe (2 different column names| final column name: (birth_year) and remove columns with null values [high number of missing observations (4,976,997)]
df_pre['birth_year_col1'] = df_pre['birthyear'].where(df_pre['birthyear'].notnull(), df_pre['birthday'])
df_pre['birth_year'] = df_pre['birth_year_col1'].where(df_pre['birth_year_col1'].notnull(), df_pre['05 - Member Details Member Birthday Year'])

df_pre = df_pre.drop(columns=['birth_year_col1', 'birthyear', 'birthday', '05 - Member Details Member Birthday Year'])

## Rename the member_casual column to usertype. AND rename member to Subscriber and casual to Customer 
df_post.rename(columns = {'member_casual':'user_type'}, inplace = True)

df_post['user_type'] = df_post['user_type'].replace(['member'],'Subscriber')
df_post['user_type'] = df_post['user_type'].replace(['casual'],'Customer')


## Select only the data columns pre has in common with post, which we will later rename in order to merge
# PRE:
df_pre_common = df_pre[["ride_id","ride_start_time","ride_stop_time","from_station_id","from_station_name","to_station_id","to_station_name","user_type"]]
# POST:
df_post_common = df_post[["ride_id","started_at","ended_at","start_station_id","start_station_name","end_station_id","end_station_name","user_type"]]

# Rename columns of POST to match those of PRE
df_post_common.rename(columns = {'started_at':'ride_start_time', 'ended_at':'ride_stop_time', 'start_station_id':'from_station_id', 'start_station_name':'from_station_name', 'end_station_id':'to_station_id', 'end_station_name':'to_station_name'}, inplace = True)


# INNER JOIN
# Combine the two dataframes
df_ride_trip_combined_inner = pd.concat([df_pre_common,df_post_common], ignore_index=True, sort=False)


## Full JOIN
# Rename columns in PRE to equivalent column in POST
df_pre.rename(columns = {'trip_id':'ride_id', 'starttime':'started_at', 'stoptime':'ended_at', 'from_station_id':'start_station_id', 'from_station_name':'start_station_name', 'to_station_id':'end_station_id', 'to_station_name':'end_station_name'}, inplace = True)
# Rename columns of POST to match those of PRE
df_post_common.rename(columns = {'started_at':'ride_start_time', 'ended_at':'ride_stop_time', 'start_station_id':'from_station_id', 'start_station_name':'from_station_name', 'end_station_id':'to_station_id', 'end_station_name':'to_station_name','usertype':'user_type'}, inplace = True)

df_ride_trip_combined_full = pd.concat([df_pre, df_post])


# New ridetrip_inner_MOD dataframe with calculated columns [created or edited]
df_ridetrip_inner_MOD = df_ride_trip_combined_inner

# CALCULATED COLUMNS:
# New column ride_duration and ride_duration_s (Length of bike ride, regardless if negative time (indicating ride_start_time should be ride_stop_time ))
df_ridetrip_inner_MOD["ride_duration"] = (abs(pd.to_datetime(df_ridetrip_inner_MOD['ride_stop_time']).subtract(pd.to_datetime(df_ridetrip_inner_MOD['ride_start_time']))))
df_ridetrip_inner_MOD['ride_duration_s'] = df_ridetrip_inner_MOD['ride_duration'].dt.seconds

# new column same_station_trip (if the bike ride ended at the same station it started at)
df_ridetrip_inner_MOD["same_station_trip"] = (df_ridetrip_inner_MOD["from_station_id"] == df_ridetrip_inner_MOD["to_station_id"]).astype(int)

# New ID column
df_ridetrip_inner_MOD["ID"] = df_ridetrip_inner_MOD.index + 1 

# New Column with only the date (no time)
df_ridetrip_inner_MOD['ride_start_date'] = pd.to_datetime(df_ridetrip_inner_MOD['ride_start_time']).dt.date


# Station Flow Dataframe creation
df_station_flow_ALL = pd.DataFrame ()
df_station_flow_ALL["flow"] = (df_ridetrip_inner_MOD.groupby(["to_station_name"]).size() - df_ridetrip_inner_MOD.groupby(["from_station_name"]).size())

# Filter data by user type
df_customers = df_ridetrip_inner_MOD[df_ridetrip_inner_MOD['user_type'].str.contains("Customer")]
df_subscribers = df_ridetrip_inner_MOD[df_ridetrip_inner_MOD['user_type'].str.contains("Subscriber")]

# CUSTOMER Station Flow Dataframe creation
station_flow_CUSTOMER = pd.DataFrame ()
station_flow_CUSTOMER["flow"] = (df_customers.groupby(["to_station_name"]).size() - df_customers.groupby(["from_station_name"]).size())

# SUBSCRIBER Station Flow Dataframe creation
station_flow_SUBSCRIBER = pd.DataFrame ()
station_flow_SUBSCRIBER["flow"] = (df_subscribers.groupby(["to_station_name"]).size() - df_subscribers.groupby(["from_station_name"]).size())


# Create columns parsing the ride start time column
df_ridetrip_inner['month'] = pd.to_datetime(df_ridetrip_inner['ride_start_time']).dt.month
df_ridetrip_inner['weekday'] = pd.to_datetime(df_ridetrip_inner['ride_start_time']).dt.dayofweek
df_ridetrip_inner['quarter'] = pd.to_datetime(df_ridetrip_inner['ride_start_time']).dt.quarter
df_ridetrip_inner['year'] = pd.to_datetime(df_ridetrip_inner['ride_start_time']).dt.year


def clean_date_w(number,weekday):
    df_ridetrip_inner['weekday'] = df_ridetrip_inner['weekday'].replace([number],weekday)

def clean_date_m(number,month):
    df_ridetrip_inner['month'] = df_ridetrip_inner['month'].replace([number],month)

def clean_date_q(number,quarterN):
    df_ridetrip_inner['quarter'] = df_ridetrip_inner['quarter'].replace([number],quarterN)


clean_date_w(1,'Tuesday')
clean_date_w(2,'Wednesday')
clean_date_w(3,'Thursday')
clean_date_w(4,'Frday')
clean_date_w(5,'Saturday')
clean_date_w(6,'Sunday')
clean_date_w(7,'Monday')

clean_date_m(1,'January')
clean_date_m(1,'Febuary')
clean_date_m(1,'March')
clean_date_m(1,'April')
clean_date_m(1,'May')
clean_date_m(1,'June')
clean_date_m(1,'July')
clean_date_m(1,'August')
clean_date_m(1,'September')
clean_date_m(1,'October')
clean_date_m(1,'November')
clean_date_m(1,'December')

clean_date_q(1,'Q1')
clean_date_q(2,'Q2')
clean_date_q(3,'Q3')
clean_date_q(4,'Q4')

# Rude Count by Date
df_group_by_date = df_ridetrip_inner.groupby([pd.to_datetime(df_ridetrip_inner['ride_start_time']).dt.date]).count()
df_group_by_date = df_group_by_date['ride_id']

