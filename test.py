import pandas as pd
import requests
import json
from datetime import datetime
import streamlit as st
from sqlalchemy import create_engine
import base64

# Function to encode the background image in Base64
def get_base64_image(image_path):
    with open(image_path, "rb") as image_file:
        return base64.b64encode(image_file.read()).decode()

# Add background image
background_image_path = "CT-Logo.jpg"  # Replace with your image file name
base64_image = get_base64_image(background_image_path)
st.markdown(
    f"""
    <style>
    .stApp {{
        background-image: url("data:image/jpeg;base64,{base64_image}");
        background-size: cover;
        background-repeat: no-repeat;
        background-attachment: fixed;
        background-position: center;
    }}
    </style>
    """,
    unsafe_allow_html=True
)

# Function to get EID
def get_eid():
    p = r'accounts.json'
    fp = open(p)
    res = json.load(fp)
    access_token = res['track3']['access_token']
    base_url = res['track3']['base_url']
    url = 'https://hst-api.wialon.com/wialon/ajax.html?svc=token/login&params={"token":"' + access_token + '"}'
    r = requests.post(url)
    data = json.loads(r.text)
    eid = data['eid']
    # Set the localization
    url2 = 'https://hst-api.wialon.com/wialon/ajax.html?svc=render/set_locale&params={"tzOffset":134228528,"language":"en","formatDate":"%E.%m.%Y %H:%M:%S"}&sid=' + eid
    r2 = requests.post(url2)
    return eid

# Function to get the Excel report
def get_excel_report(ID, FROM, TO, template, eid):
    xl_url = r'https://hst-api.wialon.com/wialon/ajax.html?svc=report/export_result&params={"format":8,"compress":0}&sid=' + eid
    cleanup_url = 'https://hst-api.wialon.com/wialon/ajax.html?svc=report/cleanup_result&params={}' +'&sid=' + eid
    params = {
        "reportResourceId":17082202,  # Replace with your resource ID
        "reportTemplateId":0,
        "reportObjectId":int(ID),
        "reportObjectSecId":0,
        "reportTemplate":template,
        "interval":{
        "from":int(FROM.timestamp()),
        "to":int(TO.timestamp()),
        "flags":0
        }
    }
    params = json.dumps(params, separators=(',', ':'))
    get_report = f'https://hst-api.wialon.com/wialon/ajax.html?svc=report/exec_report&params={params}&sid={eid}'
    report = requests.post(get_report)
    report = json.loads(report.text)
    r2 = requests.post(xl_url)
    return r2.content

# Streamlit UI components
st.title("Trips and Eco Driving Report Table")

# Get start and end datetime from the user
start_date = st.date_input("Start Date", datetime(2024, 12, 1))
end_date = st.date_input("End Date", datetime(2024, 12, 30))

# Convert to datetime with time
start_datetime = datetime.combine(start_date, datetime.min.time())
end_datetime = datetime.combine(end_date, datetime.max.time())

st.write(f"Fetching report for the period: {start_datetime} to {end_datetime}")

# Request data using the specified date range
group_name = 'Agility'
group_id = 28475360

eco_driving_file_name = f'{group_name} Eco_Driving_Report.xlsx'
trips_report_file_name = f'{group_name} Trips_Report.xlsx'

# Get the entity ID (eid)
eid = get_eid()

# # Template for the report
group_trips_stops_parkings_report_template = {"id":0,"n":"Group Trips Stops and Parkings Report","ct":"avl_unit_group","p":"{\"descr\":\"\",\"bind\":{\"avl_unit_group\":[]}}","tbl":[{"n":"unit_group_stats","l":"Statistics","c":"","cl":"","cp":"","s":"[\"address_format\",\"time_format\",\"us_units\"]","sl":"[\"Address\",\"Time Format\",\"Measure\"]","filter_order":[],"p":"{\"address_format\":\"1178599424_10_5\",\"time_format\":\"%E.%m.%Y_%H:%M:%S\",\"us_units\":0}","sch":{"f1":0,"f2":0,"t1":0,"t2":0,"m":0,"y":0,"w":0,"fl":0},"f":0},{"n":"unit_group_trips","l":"Trips","c":"[\"time_begin\",\"time_end\",\"location_begin\",\"location_end\",\"coord_begin\",\"coord_end\",\"duration\",\"duration_ival\",\"eh_duration\",\"mileage\",\"correct_mileage\",\"absolute_mileage_begin\",\"absolute_mileage_end\",\"avg_speed\",\"max_speed\",\"driver\",\"trips_count\",\"fuel_consumption_all\",\"fuel_consumption_fls\",\"fuel_level_begin\",\"fuel_level_end\",\"fuel_level_max\",\"fuel_level_min\"]","cl":"[\"Beginning\",\"End\",\"Initial location\",\"Final location\",\"Initial coordinates\",\"Final coordinates\",\"Duration\",\"Total time\",\"Engine hours\",\"Mileage\",\"Mileage (adjusted)\",\"Initial mileage\",\"Final mileage\",\"Avg speed\",\"Max speed\",\"Driver\",\"Count\",\"Consumed\",\"Consumed by FLS\",\"Initial fuel level\",\"Final fuel level\",\"Max fuel level\",\"Min fuel level\"]","cp":"[{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}]","s":"","sl":"","filter_order":["duration","mileage","base_eh_sensor","engine_hours","speed","stops","sensors","sensor_name","custom_sensors_col","driver","trailer","geozones_ex"],"p":"","sch":{"f1":0,"f2":0,"t1":0,"t2":0,"m":0,"y":0,"w":0,"fl":0},"f":256},{"n":"unit_group_stops","l":"Stops","c":"[\"time_begin\",\"time_end\",\"duration\",\"driver\",\"location\",\"coord\",\"stops_count\"]","cl":"[\"Beginning\",\"End\",\"Duration\",\"Driver\",\"Location\",\"Coordinates\",\"Count\"]","cp":"[{},{},{},{},{},{},{}]","s":"","sl":"","filter_order":["duration","sensors","sensor_name","driver","trailer","fillings","thefts","geozones_ex"],"p":"","sch":{"f1":0,"f2":0,"t1":0,"t2":0,"m":0,"y":0,"w":0,"fl":0},"f":256},{"n":"unit_group_stays","l":"Parkings","c":"[\"time_begin\",\"time_end\",\"duration\",\"location\",\"coord\",\"driver\",\"stays_count\"]","cl":"[\"Beginning\",\"End\",\"Duration\",\"Location\",\"Coordinates\",\"Driver\",\"Count\"]","cp":"[{},{},{},{},{},{},{}]","s":"","sl":"","filter_order":["duration","sensors","sensor_name","fillings","thefts","driver","trailer","geozones_ex"],"p":"","sch":{"f1":0,"f2":0,"t1":0,"t2":0,"m":0,"y":0,"w":0,"fl":0},"f":256}],"bsfl":{"ct":1675063376,"mt":1675063897}}

# # Fetch the trips report for the specified period
trips_report = get_excel_report(group_id, start_datetime, end_datetime, group_trips_stops_parkings_report_template, eid)

# # Write the trips report to a file
with open(trips_report_file_name, 'wb') as fp:
     fp.write(trips_report)

# # Load the trips report into a pandas DataFrame
trips_df = pd.read_excel(trips_report_file_name, sheet_name='Trips')

# # Select relevant columns and filter data
selected_columns = ['Grouping', 'Beginning', 'End', 'Initial location', 'Final location', 'Max speed', 'Mileage', 'Count']
trips_df = trips_df[selected_columns]

# # Filter for trips where 'Count' is 1
trips_df2 = trips_df[trips_df['Count'] == 1].copy()

# # Reset index for cleaner output
trips_df2.reset_index(drop=True, inplace=True)

# # Display the filtered trips data
st.write("Trips Report", trips_df2)


# Push data to PostgreSQL (PostgreSQL connection)
db_url = "postgresql://postgres:Mawaskii254@localhost:5432/postgres"
engine = create_engine(db_url)

# Load the trips_df into the 'trip' table
trips_df2.to_sql('trips_data2', engine, if_exists='replace', index=False)


