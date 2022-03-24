# from faker import Faker
# import psycopg2
from time import sleep
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine
from data_cleaning import split_df
import pandas as pd
if __name__ == '__main__':
    # conn = create_engine("postgresql://TEST:password@localhost:5432/TEST") 
    conn = create_engine('mysql://mysqluser:mysqlpw@13.127.216.182:3306/inventory') # connect to server
    engine = create_engine('sqlite:///telecom.db', echo = True)
    # dataset_name = "data/raw_cdr_data_header.csv"

    while True: 
        dataset_header_name = "./data/raw_cdr_data_header.csv"
        dataset_name = "./data/raw_cdr_data.csv"

        raw_cdr_data_header= pd.read_csv(dataset_header_name,low_memory=False)
        raw_cdr_data = pd.read_csv(dataset_name, header=None, low_memory=False)
        
        df=raw_cdr_data_header.sample(n=1)
        n=df.index[0]
        print("n=",n)
        raw_cdr_data=raw_cdr_data.iloc[n:(n+1),:]
        # raw_cdr_data=raw_cdr_data.iloc[1:]
        call_dataset,service_dataset,device_dataset=split_df(raw_cdr_data)

        df.to_sql('raw_telecom',conn, if_exists='append')
        call_dataset.to_sql('call_dataset_mysql',engine, if_exists='append')
        service_dataset.to_sql('service_dataset_mysql',engine, if_exists='append')
        device_dataset.to_sql('device_dataset_mysql',engine, if_exists='append')
        sleep(10)

