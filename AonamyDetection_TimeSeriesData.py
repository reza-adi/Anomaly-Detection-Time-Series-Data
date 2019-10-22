# -*- coding: utf-8 -*-
"""
Created on Fri Feb  8 11:42:20 2019
@author: rezaa
"""

import pyodbc
import pandas as pd

# Connect to MSSQL Server
server = 'Server Address'
database = 'Database'
username = 'Username'
password = 'Password'
conn = pyodbc.connect('DRIVER={ODBC Driver 13 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)

daily_query = "SELECT clicksTable.[cc], clicksTable.[date], clicksTable.[merchantName], clicksTable.[device], sum([ga_uniqueEvents]) as uniqueEvents, ordersTable.orders, (ordersTable.orders * 1.0 / sum([ga_uniqueEvents]))*100 as [CR] FROM [dbo].[FactAnalyticsConversion] as clicksTable LEFT JOIN (SELECT [cc], [date], [merchantName], [device], sum([order]) as orders FROM [dbo].[FactHasOfferTransaction] group by [cc], [date], [merchantName], [device]) as ordersTable ON clicksTable.cc = ordersTable.cc and clicksTable.date = ordersTable.date and clicksTable.merchantName = ordersTable.merchantName and clicksTable.device = ordersTable.device group by clicksTable.[cc], clicksTable.[date], clicksTable.[merchantName], clicksTable.[device], ordersTable.orders order by clicksTable.[cc], clicksTable.[date], clicksTable.[merchantName], clicksTable.[device]"
weekly_query = "SELECT clicksTable.[cc], clicksTable.[year_week], clicksTable.[merchantName], clicksTable.[device], sum([ga_uniqueEvents]) as uniqueEvents,ordersTable.orders, (ordersTable.orders * 1.0 / sum([ga_uniqueEvents]))*100 as [CR] FROM [dbo].[FactAnalyticsConversion] as clicksTable LEFT JOIN (SELECT [cc], [year_week], [merchantName], [device], sum([order]) as orders FROM [dbo].[FactHasOfferTransaction] group by [cc], [year_week], [merchantName], [device]) as ordersTable ON clicksTable.cc = ordersTable.cc and clicksTable.year_week = ordersTable.year_week and clicksTable.merchantName = ordersTable.merchantName and clicksTable.device = ordersTable.device group by clicksTable.[cc], clicksTable.[year_week], clicksTable.[merchantName], clicksTable.[device], ordersTable.orders order by clicksTable.[cc], clicksTable.[year_week], clicksTable.[merchantName], clicksTable.[device]"
monthly_query = "SELECT clicksTable.[cc], clicksTable.[year_month], clicksTable.[merchantName], clicksTable.[device], sum([ga_uniqueEvents]) as uniqueEvents,ordersTable.orders, (ordersTable.orders * 1.0 / sum([ga_uniqueEvents]))*100 as [CR] FROM [dbo].[FactAnalyticsConversion] as clicksTable LEFT JOIN (SELECT [cc], [year_month], [merchantName], [device], sum([order]) as orders FROM [dbo].[FactHasOfferTransaction] group by [cc], [year_month], [merchantName], [device]) as ordersTable ON clicksTable.cc = ordersTable.cc and clicksTable.year_month = ordersTable.year_month and clicksTable.merchantName = ordersTable.merchantName and clicksTable.device = ordersTable.device group by clicksTable.[cc], clicksTable.[year_month], clicksTable.[merchantName], clicksTable.[device], ordersTable.orders order by clicksTable.[cc], clicksTable.[year_month], clicksTable.[merchantName], clicksTable.[device]"

# Execute the query
daily_data = pd.read_sql(daily_query,conn)
daily_data.columns=["CC","Date","Merchant","Device","UClicks","orders","CR"]
daily_data.fillna(value={'orders':0,'CR':0}, inplace=True)
daily_data.to_csv("daily_CR.csv",index = False)

weekly_data = pd.read_sql(weekly_query,conn)
weekly_data.columns=["CC","Date","Merchant","Device","UClicks","orders","CR"]
weekly_data.fillna(value={'orders':0,'CR':0}, inplace=True)
weekly_data.to_csv("weekly_CR.csv",index = False)

monthly_data = pd.read_sql(monthly_query,conn)
monthly_data.columns=["CC","Date","Merchant","Device","UClicks","orders","CR"]
monthly_data.fillna(value={'orders':0,'CR':0}, inplace=True)
monthly_data.to_csv("monthly_CR.csv",index = False)

conn.close()
#---------------------
#You can replace the code above with just a line to read CSV by Pandas:
#daily_data=pd.read_csv("your data.csv", sep=',', encoding="ISO-8859-1",low_memory=False)
#---------------------
devices = ['mobile','desktop']
outputs = [
      {
      'merchant_names': ['Client1','client1'],
      'countries': ['HK','ID','SG','MY','PH'],
      'output_file': './output/Client1_ALL.xlsx'
      },
      {
      'merchant_names':['Client2','client2'],
      'countries': ['ID'],
      'output_file': './output/Client2_ALL.xlsx'
      },
      {
      'merchant_names': ['Client3', 'client3'],
      'countries': ['ID','SG','MY','PH','TH','VN'],
      'start_date': '2019-02-01',
      'output_file': './output/Lazada_ALL.xlsx'
      } 
]

for output in outputs:
      writer = pd.ExcelWriter(output['output_file'], engine='xlsxwriter')

      if 'frequency' in output:
            freq = output['frequency']
      else:
            freq = 'daily'

      df = pd.read_csv(freq + '_CR.csv', sep=',', encoding="ISO-8859-1",low_memory=False)
      df = df.loc[df['Merchant'].isin(output['merchant_names'])]
      
      if freq == 'daily' and 'start_date' in output:
            df = df.loc[df['Date'] >= output['start_date']]
      
      for cc in output['countries']:
            for device in devices:
                  final = df.loc[((df['CC'] == cc) & (df['Device'] == device))]
                  final.reset_index(inplace=True, drop=True)
                  #rolling 7 day window inclusive of today
                  final['rolling_mean'] = final['CR'].rolling(7, min_periods=1).mean() #min_periods 1 will force calculation even if there are NaNs in the window
                  final['rolling_stdev'] = final['CR'].rolling(7, min_periods=1).std() #normalized by N-1, add ddof=0 to normalize by N
                  final.loc[(abs(final.CR - final.rolling_mean) > 1.2*final.rolling_stdev) & (final.CR < final.rolling_mean), 'Anomaly'] = 1
                  final.to_excel(writer, sheet_name = freq + ' ' + cc + ' ' + device, index=False)
                  if not final['Anomaly'].tail(7).isnull().all(): print(final[['Date','Anomaly']].tail(7), output['merchant_names'][0] + ' ' + cc + ' ' + device)
      writer.save()


