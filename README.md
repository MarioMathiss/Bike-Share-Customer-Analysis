# Bike-Share-Customer-Analysis
Analysis of bike share service offered in the city of Chicago, IL. This analysis provides an understanding and comparison on how different customer groups use the service differently. 

This project uses Divvy's public data, sourced below. Divvy, a bike share company based in Chicago with a fleet of 5,824 bicycles and a network of 692 stations. The company offers pricing plans (subscription model): single-ride pass (customer), full-day pass (customer), and annual membership (members or subscribers).
## Data Source
#### Divvy Bikes public data (https://divvy-tripdata.s3.amazonaws.com/index.html). 
The original data has been modified by Divvy, removing personally identifiable information to protect customer privacy.

There are station detail and user ride files in the s3 bucket.
(As of Feb 2022)

### Station: 
  - 8 files with information on bike docking stations.
  - Columns: id, name, latitude, longitude, dpcapacity, landmark, online date, dateCreated, online_date, city.
  - 3357 observations, 660 distinct stations

### User ride: 
  - 50 csv files (~4.27GB), with dates ranging from June 2013 to January 2022 [missing dates 1/7/2014 & 1/8/2014].
  - The data files before Jan 1, 2020 contain some different columns to those after this date. (Moving forward in this analysis the files before        the date will be referenced to as pre and those after as post respectively).

#### Pre: Contains several columns with similar column definitions but different column names across different dates.
  - 27 columns (5 columns not in post: bike_id, tripduration, usertype, gender, birthyear).
  - 12 columns with same data definition.
  - Date range: January 01, 2013 to December 31, 2019

#### Post: Contains several columns with similar column definitions but different column names across different dates.
  - 13 columns (5 columns not in pre: rideable_type, start_lat, start_lng, end_lat, end_lng)
  - Date range: January 01, 2020, to January 31, 2022

#### Customer Rides (inner):
  - 8 of the 40 columns have the same definition but different column names, [range from 2013 to 2022]. (Moving forward in this analysis the           files with common column definitions will be referenced to as inner).
  - 30,483,860 observations
  - Columns: trip_id, start_time, end_time, start_station_id, start_station_name, end_station_id, end_station_name, user_type.

## Data Processing
Here the data cleaning sequence (concat_clean.py) will be explained
### Cleaning Steps:
  - Convert Start and stop time columns [Jan 1, 2014, to Dec 31, 2017 formatted differently]
  - Merge format A and B to pre df
  - Merge columns that have the same meaning but have different names. (Make one column)
  - Rename the member_casual column to usertype. AND rename member to Subscriber and casual to Customer
  - Select only the data columns pre has in common with post, which we will later rename to merge
  - Rename columns of POST to match those of PRE
  - combine data frames pre and post (INNER JOIN)
  - combine data frames pre and post (FULL JOIN)
  - Create new columns (ride_duration (minutes), ride_duration_s (seconds), ID, ride_start_date (ride_start_time with only the date))
  - Group by start and end stations, then get the station flow (end - start).
