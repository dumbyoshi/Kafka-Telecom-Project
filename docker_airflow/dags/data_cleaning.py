#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import re
import datetime

#====================Functions for Preprocessing the data==============#

# Function to separate the date and time from column 9
def datetime_divider(data):
    # data type of data is list
    # ["20190620032717.906"]
    # return type will be list of list with date, time [["20190620","032717"]]
    
    for index in range(len(data)):
        #find the string which starts with only digit
        if re.match('\d',str(data[index])):
            regex = re.compile("\d{1,8}") 
            a = regex.findall(str(data[index]))
            data[index] = [a[0],a[1]] #date and time
            
        else:
            data[index] = [np.nan, np.nan]
    
    return data
            

# Function to convert the date to desired format
def date_modifier(data):
    # data type of data is list
    # 20190620 should be converted to 2019-06-20
    for index in range(len(data)):
        if re.match('\d',str(data[index])):
            year = data[index][:4]
            month = data[index][4:6]
            day = data[index][6:]
            
            data[index] = "-".join([year,month,day])
        
        else:
            data[index] = np.nan
    
    return data

# Function to convert the time to desired timme format
def time_modifier(data):
    # data type of data is list
    # 032717 should be converted into 03:27:17 AM
    # COnvert 24 hr format to 12 hr format
    for index in range(len(data)):
        if re.match('\d',str(data[index])):
            hours = int(data[index][:2])
            minutes = data[index][2:4]
            seconds = data[index][4:]
            
            if hours >= 12:
                if hours == 12:
                    hr = str(hours)
                else:
                    hr = str(hours-12)
                    
                meridiem = "PM"
            
            else:
                if hours == 0:
                    hr = str(12)
                else:
                    hr = str(hours)
                meridiem = "AM"
            
            data[index] = ":".join([hr,minutes,seconds])+ " " + meridiem
            
        else:
            data[index] = np.nan
            
    return data


# Function to replace the terminologies
def replace_simple_with_standard_terminology(dataframe):
    dataframe[5] = dataframe[5].replace('Originating','Outgoing')
    dataframe[5] = dataframe[5].replace('Terminating','Incoming')
    
    dataframe[267] = dataframe[267].replace('Success','Voice Portal')
    
    dataframe[312] = dataframe[312].replace('Shared Call Appearance','Secondary Device')
    
    return dataframe



def remove_unwanted_data(datacolumn):
    for index in range(len(datacolumn)):
        if (datacolumn[index]== 'Secondary Device' or datacolumn[index]=='Primary Device'):
            continue
        else:
            datacolumn[index] = np.nan
            
    return datacolumn


# This function sets all the services in one column 147 for 147,312,267 columns
def combine_all_services(datacolumn147, datacolumn312, datacolumn267):
    for index in range(len(datacolumn147)):
        if datacolumn147[index] is np.nan:
            if (datacolumn312[index] is not np.nan) and (datacolumn267[index] is not np.nan):
                datacolumn147[index] = str(datacolumn312[index]) +',' + str(datacolumn267[index])
            elif (datacolumn312[index] is not np.nan):
                datacolumn147[index] = datacolumn312[index]
            else:
                datacolumn147[index] = datacolumn267[index]
               
        else:
            continue
        
    return datacolumn147


# Convert the datetime into desired format
# "20190620032717.906"  to "2019-06-20 03:27:17"
def call_time_fetcher(data):
    
    for index in range(len(data)):
        data[index] = str(data[index])
        if (data[index]!='nan'):
            year = data[index][:4]
            month = data[index][4:6]
            day = data[index][6:8]
            hours = data[index][8:10]
            minutes = data[index][10:12]
            seconds =str(round(float(data[index][12:])))
            
            if( int(seconds) >= 60):
                minutes = int(minutes) + 1 
                seconds = int(seconds) - 60
                
            if( int(minutes) >= 60):
                hours = int(hours) + 1
                minutes = int(minutes) - 60
                
            data[index] = f"{year}-{month}-{day} {hours}:{minutes}:{seconds}"
        
        else:
            data[index] = np.nan
    
    return data


# ['3:27:17 AM', '1:28:19 PM', 'nan', '7:23:52 PM']
# ['3:00 - 3:59', '13:00 - 13:59', nan, '19:00 - 19:59']
def hourly_range(data):
    
    for index in range(len(data)):
        data[index] = str(data[index])
        
        if(data[index] != 'nan'):
            if(re.search("PM",data[index])):
                time_data = re.findall('\d+',data[index])
                if time_data[0] !="12":
                    time_data = int(time_data[0])+12
                else:
                    time_data = time_data[0]
            
            else:
                time_data = re.findall('\d+', data[index])
                if time_data[0]=='12':
                    time_data = f"0{int(time_data[0]) -12}"
                else:
                    time_data = time_data[0]
            
            
            data[index] = f"{time_data}:00 - {time_data}:59"
  
        else:
            data[index] = np.nan
    
    return data
    

# ['2019-06-20', '2019-06-21', 'nan', '2019-06-25']
# ['Thursday', 'Friday', nan, 'Tuesday']
def weekly_range(data):
    
    for index in range(len(data)):
        data[index] = str(data[index])
        
        if data[index] != 'nan':
            year, month, day = [int(x) for x in data[index].split("-")] #list unpacking
            result = datetime.date(year,month,day)
            data[index] = result.strftime("%A") #full weekday name
        
        else:
            data[index] = np.nan
    
    return data


#===========================END=======================================#



# In[79]:



def get_clean_df(raw_cdr_data):
    #raw_cdr_data = pd.read_csv(dataset_name, header=None, low_memory=False)

    '''column 9 and column 13 contains date time data, "20190620032717.906". This 
    has to be split into 2 parts date and time. datetime_divider function will do this'''

    #print(raw_cdr_data[9].tolist()[3]) 


    # creates 2 new columns to store date and time
    # datetime_divider will return list of lists. * will unpack the lists, zip will create separate 2 lists for all dates and times 
    # [['2345678','46656'],['45677899','3455677']]

    raw_cdr_data["date"], raw_cdr_data["time"] = zip(*datetime_divider(raw_cdr_data[9].tolist()))


    raw_cdr_data["date"] = date_modifier(raw_cdr_data["date"].tolist())
    raw_cdr_data["time"] = time_modifier(raw_cdr_data["time"].tolist())




    #print(raw_cdr_data[5].unique()) # replace Originating,Terminating with incoming and outgoing
    #print(raw_cdr_data[267].unique()) # replace Success with voice portal
    #print(raw_cdr_data[312].unique()) # replace shared call appearance with secondary device


    raw_cdr_data = replace_simple_with_standard_terminology(raw_cdr_data)

    # 312 column should have Primary and Secondary Device only       
    raw_cdr_data[312] = remove_unwanted_data(raw_cdr_data[312].tolist())

    # If the column 147 has missing data, then create the data from 312 and 267
    raw_cdr_data[147] = combine_all_services(raw_cdr_data[147].tolist(), raw_cdr_data[312].tolist(), raw_cdr_data[267].tolist())




    # Make 2 temporary columns to find the duration
    raw_cdr_data[9].tolist()
    raw_cdr_data["starttime"] = pd.to_datetime(call_time_fetcher(raw_cdr_data[9].tolist()))
    # 2019-06-25 19:21:43

    raw_cdr_data["endtime"] = pd.to_datetime(call_time_fetcher(raw_cdr_data[13].tolist()))
    # 2019-06-25 19:24:54

    raw_cdr_data["duration"] = (raw_cdr_data["endtime"] - raw_cdr_data["starttime"]).astype("timedelta64[m]")
    #print(raw_cdr_data["duration"])


    raw_cdr_data["hourly_range"] = hourly_range(raw_cdr_data["time"].tolist())
    #print(raw_cdr_data["hourly_range"])

    raw_cdr_data["weekly_range"] = weekly_range(raw_cdr_data["date"].tolist())
    #print(raw_cdr_data["weekly_range"])



    raw_cdr_data = raw_cdr_data.drop("time",axis=1)
    return raw_cdr_data


def split_df(raw_cdr_data):
    # split into three dataset
    call_columns = ["4", "5","14", "31", "120", "147", "267", "312", "345", \
                    "date","starttime", "endtime","duration", "hourly_range","weekly_range"]

    # low_memory = False is used because the columns have mixed data types
    call_dataset = get_clean_df(raw_cdr_data)
    # call_dataset=call_dataset[call_columns]
    call_dataset.columns = call_dataset.columns.astype(str)

    # Required columns for service data
    service_columns = ["31", "120", "147", "345","date", "starttime", "endtime","duration"]
    service_dataset = call_dataset[service_columns]


    # Required columns for device data
    device_columns = ["5", "31", "120", "312", "345", "date","starttime", "endtime","duration"]
    device_dataset = call_dataset[device_columns]

    call_dataset = call_dataset.rename(columns = {"4":"Group", "5":"Call_Direction","14":"Missed_Calls",
                                            "31":"GroupID", "120":"UserID", "147":"Features", "267":"vpDialingfacResult",
                                            "312":"UsageDeviceType",
                                            "345":"UserDeviceType"})

    call_dataset=call_dataset[['Group','Call_Direction','Missed_Calls','GroupID','Features','vpDialingfacResult','UsageDeviceType','UserDeviceType',"date","starttime", "endtime","duration", "hourly_range","weekly_range"]]
    service_dataset = service_dataset.rename(columns ={"120":"UserID", 
                                                  "31":"GroupID", "147":"FeatureName",
                                                  "345":"UserDeviceType","date":"FeatureEventDate"
                                                  })

    device_dataset = device_dataset.rename(columns={"5": "DeviceEventTypeDirection", 
                                      "120":"UserID", "31":"GroupID", 
                                      "345":"UserDeviceType","date":"DeviceEventDate", 
                                      "312":"UsageDeviceType"})
    
    return (call_dataset,service_dataset,device_dataset)
