import requests
from requests.auth import HTTPBasicAuth
import os
import hashlib
import csv
import json
import io
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import time
import simplekml 
import geopandas as gpd
import fiona
fiona.drvsupport.supported_drivers['KML'] = 'rw'
import shapely.geometry

def callMovebankAPI(params):
    # Requests Movebank API with ((param1, value1), (param2, value2),).
    # Assumes the environment variables 'mbus' (Movebank user name) and 'mbpw' (Movebank password).
    # Returns the API response as plain text.
    basic = HTTPBasicAuth('Jhala', 'Tigercell2018!')
    url = "https://www.movebank.org/movebank/service/json-auth"
    url1 = "https://www.movebank.org/movebank/service/direct-read"
    response = requests.get(url1, params=params, auth=basic)
    print("Request " + response.url)
    if response.status_code == 200:  # successful request
        if 'License Terms:' in str(response.content):
            # only the license terms are returned, hash and append them in a subsequent request.
            # See also
            # https://github.com/movebank/movebank-api-doc/blob/master/movebank-api.md#read-and-accept-license-terms-using-curl
            print("Has license terms")
            hash = hashlib.md5(response.content).hexdigest()
            params = params + (('license-md5', hash),)
            # also attach previous cookie:
            response = requests.get(url1, params=params,
                                    cookies=response.cookies, auth=basic)
            if response.status_code == 403:  # incorrect hash
                print("Incorrect hash")
                return ''
        return response.content.decode('utf-8')
    print(response.content)
    return ''

t_e=datetime.now()
t_e=f"{t_e.year}{t_e.month:02d}{t_e.day:02d}{t_e.hour:02d}{t_e.minute:02d}{t_e.second:04d}"
t_s=datetime.now() - timedelta(days=7)
t_s=f"{t_s.year}{t_s.month:02d}{t_s.day:02d}{t_s.hour:02d}{t_s.minute:02d}{t_s.second:04d}"

params_gps = (('entity_type', 'event'),
          ('study_id', '766916593'),
          ('timestamp_start', t_s), 
          ('timestamp_end', t_e),
          ('sensor_type_id', '653'),
          ('attributes', 'all'))

data = callMovebankAPI(params_gps)
data = data.split("\r\n")
cols = data[0]
row_values = data[1:]

df = pd.DataFrame(columns = cols.split(","))

for row in row_values[:-1]:
    row_vals = row.split(",")
    df.loc[len(df.index)] = row_vals

df['timestamp']=pd.to_datetime(df['timestamp']).dt.tz_localize("UTC").dt.tz_convert("Asia/Calcutta")
###################

params_acc = (('entity_type', 'event'),
          ('study_id', '766916593'),
          ('timestamp_start', t_s), 
          ('timestamp_end', t_e),
          ('sensor_type_id', '2365683'),
          ('attributes', 'all'))
data_acc = callMovebankAPI(params_acc)
data_acc = data_acc.split("\r\n")
cols = data_acc[0]
row_values = data_acc[1:]

df_acc = pd.DataFrame(columns = cols.split(","))

for row in row_values[:-1]:
    row_vals = row.split(",")
    df_acc.loc[len(df_acc.index)] = row_vals

df_acc['timestamp']=pd.to_datetime(df_acc['timestamp']).dt.tz_localize("UTC").dt.tz_convert("Asia/Calcutta")
area_poly = gpd.read_file("Area_polygons.shp" ) #
tag_to_individual = df[["tag_local_identifier","individual_local_identifier"]].drop_duplicates().rename(columns = {'tag_local_identifier':'Tag_ID'})

tag_list=[]
latest_battery_voltage_list=[]
last_location_list=[]
last_timestamp_list=[]
date_diff_list=[]
n_points_list=[]
n_acc_list=[]
last_location_list=[]
enclosure_list=[]

for tag in ['"5949"', '"5947"', '"8649"','"8650"', '"8651"','"867688031356557"']:
    try:
        df_filtered=df[df['tag_local_identifier'] == tag]
        df_acc_filtered=df_acc[df_acc['tag_local_identifier'] == tag]
        try:
            latest_battery_voltage=list(filter(None, df_filtered.eobs_battery_voltage.tolist()))[-1]
        except:
            latest_battery_voltage=int(float(list(filter(None, df_filtered.tag_voltage.tolist()))[-1]))
        last_timestamp=df_filtered.timestamp.tolist()[-1]
        date_diff=datetime.now().date()-last_timestamp.date()
        n_points=df_filtered.shape[0]
        n_acc=df_acc_filtered.shape[0]
        last_location=f"{list(filter(None, df_filtered.location_lat.tolist()))[-1]}, {list(filter(None, df_filtered.location_long.tolist()))[-1]}"
        last_location_pt=shapely.geometry.Point((last_location.split(", ")[1],last_location.split(", ")[0]))#
        try:
            Enclosure=area_poly.Name[area_poly.where(area_poly.contains(last_location_pt)).Name.dropna().index[0]]#
        except:
            Enclosure=f"NA"
    #print(f"{tag}\t{n_points}\t\t{n_acc}\t\t{latest_battery_voltage}\t\t{last_timestamp}\t\t{date_diff.days} days ago\t{last_location}")
    except:
        del(df_filtered)
        del(df_acc_filtered)
        del(latest_battery_voltage)
        del(last_location)
        del(last_timestamp)
        del(date_diff)
        del(n_points)
        del(n_acc)
        df_filtered=df[df['tag_local_identifier'] == tag]
        df_acc_filtered=df_acc[df_acc['tag_local_identifier'] == tag]
        latest_battery_voltage=f"NA"
        last_timestamp=f"NA"
        date_diff=f"More than 7 days ago"
        n_points=f"NA"
        n_acc=f"NA"
        last_location=f"NA"
        Enclosure=f"NA"
    
    latest_battery_voltage_list.append(latest_battery_voltage)
    last_timestamp_list.append(last_timestamp)
    date_diff_list.append(date_diff)
    n_points_list.append(n_points)
    n_acc_list.append(n_acc)
    last_location_list.append(last_location)
    tag_list.append(tag)
    enclosure_list.append(Enclosure) #
    
    
output=pd.DataFrame(data={'Tag_ID':tag_list, 'GPS Locations (7 days)':n_points_list,'Accelerometer recordings (7 days)':n_acc_list, 'Last Timestamp':last_timestamp_list,'Time since last location':date_diff_list,                          
                          'Last Location' : last_location_list,'Last Battery Voltage':latest_battery_voltage_list,
                         'Area' : enclosure_list}) #
output=output.merge(tag_to_individual,how="left")
output.insert(0,"individual_local_identifier",output.pop("individual_local_identifier"))
output.pop("Tag_ID")
output = output.rename(columns={'individual_local_identifier': 'Animal_Name'}).dropna()
output_html=output.to_html()

kml=simplekml.Kml()
colors = [simplekml.Color.red,simplekml.Color.blue,simplekml.Color.white,simplekml.Color.yellow,simplekml.Color.green,simplekml.Color.black]
tags= ['"5949"', '"5947"', '"8649"','"8650"', '"8651"','"867688031356557"']
for j in range(len(tags)):
    points=df[df['tag_local_identifier'] == tags[j]]
    points["time_diff"] = points['timestamp'].diff().dt.total_seconds()
    points = points[points['time_diff'] > 120]
    points['location_lat'].replace('', np.nan, inplace=True)
    points['location_long'].replace('', np.nan, inplace=True)
    points=points.dropna(subset=['location_lat', 'location_long'])
    points['coords']=points[['location_long', 'location_lat']].apply(tuple, axis=1)
    kml_folder=kml.newfolder(name=tags[j])
    color=colors[j]
    for i in range(points.shape[0]):
        kml_pt=kml_folder.newpoint(timestamp=points.timestamp.iloc[i],coords=[(points.location_long.iloc[i],points.location_lat.iloc[i])])
        kml_pt.timespan.begin=datetime.strftime(points.timestamp.iloc[i],"%Y-%m-%dT%H:%M:%SZ")
        kml_pt.timespan.end=datetime.strftime(points.timestamp.iloc[i],"%Y-%m-%dT%H:%M:%SZ")
        kml_pt.style.iconstyle.icon.href="http://maps.google.com/mapfiles/kml/pal4/icon57.png"
        kml_pt.style.iconstyle.color=color
        kml_pt.description=f"Bird ID: {tags[j]}\nTimestamp: {points.timestamp.iloc[i]}\nBattery_voltage: {points.eobs_battery_voltage.iloc[i]}"
    kml_line=kml_folder.newlinestring(name=tag[j],coords=points.coords)
    kml_line.style.linestyle.color=color
kml.save('df_kml.kml')

from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib

def send_email(send_to, subject, filename):
    send_from = "campa.gib@gmail.com"#
    password = "cirwdatzzemfuqtc"#
    message1=f"Dear User,\n\n                Please find the summarised data for GIBs tagged in Desert National Park WLS and Pokharan-Ramdeora-PFFR. \n                This is an automated mail.\n\n                Best Regards,\n                WII Bustard Recovery Program"
    message2 = output_html
    for receiver in send_to:
            multipart = MIMEMultipart()
            multipart["From"] = send_from
            multipart["To"] = receiver
            multipart["Subject"] = subject  
            filename = filename
            # Read a file and encode it into base64 format
            with open(filename,'rb') as file:
            # Attach the file with filename to the email
                multipart.attach(MIMEApplication(file.read(), Name='GIB_locations.kml'))
            multipart.attach(MIMEText(message1, "html"))
            multipart.attach(MIMEText(message2, "html"))
            server = smtplib.SMTP("smtp.gmail.com", 587)
            server.starttls()
            server.login(multipart["From"], password)
            server.sendmail(multipart["From"], multipart["To"], multipart.as_string())
            server.quit()


send_email(["varunkher23@gmail.com","martinian.manas@gmail.com","david.phinehas@gmail.com", "giavarma.923@gmail.com"], "Raptor tag locations for last week", "df_kml.kml")





