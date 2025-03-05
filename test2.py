# Import necessary libraries
import pandas as pd
import requests
import io
import json
from datetime import datetime
import streamlit as st
from sqlalchemy import create_engine
import base64
import pytz

# Set the app layout to wide mode
st.set_page_config(layout="wide")

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

# Eco Driving Template
eco_template = [{'id': 8,'n': '20cube - Eco Driving Per Fleet','ct': 'avl_unit_group','p': '{"descr":"","bind":{"avl_unit_group":[]}}','tbl': [{'n': 'unit_group_stats','l': 'Statistics','c': '','cl': '','cp': '','s': '["address_format","time_format","us_units","deviation"]','sl': '["Address","Time Format","Measure","Deviation"]','filter_order': [],'p': '{"address_format":"1178599424_10_5","time_format":"%E.%m.%Y_%H:%M:%S","us_units":0,"deviation":"30"}','sch': {'f1': 0,'f2': 0,'t1': 0,'t2': 0,'m': 0,'y': 0,'w': 0,'fl': 0},'f': 0},{'n': 'unit_group_ecodriving','l': 'Eco driving','c': '["violation_name","time_begin","time_end","location","location_end","mileage","violations_count","violation_value","avg_speed","max_speed","violation_mark","driver","violation_duration","violation_mileage"]','cl': '["Violation","Beginning","End","Initial location","Final location","Mileage","Count","Value","Avg speed","Max speed","Penalties","Driver","Violation duration","Violation mileage"]','cp': '[{},{},{},{},{},{},{},{},{},{},{},{},{},{}]','s': '','sl': '','filter_order': ['violation_group_name','violation_duration','show_all_trips','mileage','colors','custom_sensors_col','geozones_ex'],'p': '{"grouping":"{\\"type\\":\\"unit\\",\\"nested\\":{\\"type\\":\\"criterion\\"}}","violation_group_name":"*"}','sch': {'f1': 0,'f2': 0,'t1': 0,'t2': 0,'m': 0,'y': 0,'w': 0,'fl': 0},'f': 4198672}],'bsfl': {'ct': 1683207805, 'mt': 1721114533}}]


# Trip Template
group_trips_stops_parkings_report_template = {"id":0,"n":"Group Trips Stops and Parkings Report","ct":"avl_unit_group","p":"{\"descr\":\"\",\"bind\":{\"avl_unit_group\":[]}}","tbl":[{"n":"unit_group_stats","l":"Statistics","c":"","cl":"","cp":"","s":"[\"address_format\",\"time_format\",\"us_units\"]","sl":"[\"Address\",\"Time Format\",\"Measure\"]","filter_order":[],"p":"{\"address_format\":\"1178599424_10_5\",\"time_format\":\"%E.%m.%Y_%H:%M:%S\",\"us_units\":0}","sch":{"f1":0,"f2":0,"t1":0,"t2":0,"m":0,"y":0,"w":0,"fl":0},"f":0},{"n":"unit_group_trips","l":"Trips","c":"[\"time_begin\",\"time_end\",\"location_begin\",\"location_end\",\"coord_begin\",\"coord_end\",\"duration\",\"duration_ival\",\"eh_duration\",\"mileage\",\"correct_mileage\",\"absolute_mileage_begin\",\"absolute_mileage_end\",\"avg_speed\",\"max_speed\",\"driver\",\"trips_count\",\"fuel_consumption_all\",\"fuel_consumption_fls\",\"fuel_level_begin\",\"fuel_level_end\",\"fuel_level_max\",\"fuel_level_min\"]","cl":"[\"Beginning\",\"End\",\"Initial location\",\"Final location\",\"Initial coordinates\",\"Final coordinates\",\"Duration\",\"Total time\",\"Engine hours\",\"Mileage\",\"Mileage (adjusted)\",\"Initial mileage\",\"Final mileage\",\"Avg speed\",\"Max speed\",\"Driver\",\"Count\",\"Consumed\",\"Consumed by FLS\",\"Initial fuel level\",\"Final fuel level\",\"Max fuel level\",\"Min fuel level\"]","cp":"[{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}]","s":"","sl":"","filter_order":["duration","mileage","base_eh_sensor","engine_hours","speed","stops","sensors","sensor_name","custom_sensors_col","driver","trailer","geozones_ex"],"p":"","sch":{"f1":0,"f2":0,"t1":0,"t2":0,"m":0,"y":0,"w":0,"fl":0},"f":256},{"n":"unit_group_stops","l":"Stops","c":"[\"time_begin\",\"time_end\",\"duration\",\"driver\",\"location\",\"coord\",\"stops_count\"]","cl":"[\"Beginning\",\"End\",\"Duration\",\"Driver\",\"Location\",\"Coordinates\",\"Count\"]","cp":"[{},{},{},{},{},{},{}]","s":"","sl":"","filter_order":["duration","sensors","sensor_name","driver","trailer","fillings","thefts","geozones_ex"],"p":"","sch":{"f1":0,"f2":0,"t1":0,"t2":0,"m":0,"y":0,"w":0,"fl":0},"f":256},{"n":"unit_group_stays","l":"Parkings","c":"[\"time_begin\",\"time_end\",\"duration\",\"location\",\"coord\",\"driver\",\"stays_count\"]","cl":"[\"Beginning\",\"End\",\"Duration\",\"Location\",\"Coordinates\",\"Driver\",\"Count\"]","cp":"[{},{},{},{},{},{},{}]","s":"","sl":"","filter_order":["duration","sensors","sensor_name","fillings","thefts","driver","trailer","geozones_ex"],"p":"","sch":{"f1":0,"f2":0,"t1":0,"t2":0,"m":0,"y":0,"w":0,"fl":0},"f":256}],"bsfl":{"ct":1675063376,"mt":1675063897}}


# Function to fetch Trip Report
def get_trip_report(ID, FROM, TO, group_trips_stops_parkings_report_template, eid):
    xl_url = r'https://hst-api.wialon.com/wialon/ajax.html?svc=report/export_result&params={"format":8,"compress":0}&sid=' + eid
    params = {
        "reportResourceId": 17082202,  # Replace with your resource ID
        "reportTemplateId": 0,
        "reportObjectId": int(ID),
        "reportObjectSecId": 0,
        "reportTemplate": group_trips_stops_parkings_report_template,
        "interval": {
            "from": int(FROM.timestamp()),
            "to": int(TO.timestamp()),
            "flags": 0
        }
    }
    params = json.dumps(params, separators=(',', ':'))
    get_report_url = f'https://hst-api.wialon.com/wialon/ajax.html?svc=report/exec_report&params={params}&sid={eid}'
    requests.post(get_report_url)  # Trigger report execution
    r2 = requests.post(xl_url)  # Download report
    return r2.content

# Function to fetch Eco Driving Report
def get_eco_driving_report(ID, FROM, TO, eco_template, eid):
    xl_url = r'https://hst-api.wialon.com/wialon/ajax.html?svc=report/export_result&params={"format":8,"compress":0}&sid=' + eid
    params = {
        "reportResourceId": 26749909,  # Replace with your resource ID
        "reportTemplateId": 8,
        "reportObjectId": int(ID),
        "reportObjectSecId": 0,
        "reportTemplate": eco_template,
        "interval": {
            "from": int(FROM.timestamp()),
            "to": int(TO.timestamp()),
            "flags": 0
        }
    }
    params = json.dumps(params, separators=(',', ':'))
    get_report_url = f'https://hst-api.wialon.com/wialon/ajax.html?svc=report/exec_report&params={params}&sid={eid}'
    requests.post(get_report_url)  # Trigger report execution
    r2 = requests.post(xl_url)  # Download report
    return r2.content

def create_excel_file(utilization, df2, events_pvt, trips_df2, group_name):
    # Convert timezone-aware datetime columns to timezone-unaware
    for col in trips_df2.select_dtypes(include=["datetime64[ns, UTC]", "datetime64[ns]"]):
        trips_df2[col] = trips_df2[col].dt.tz_localize(None)
    
    # Create Excel file
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        utilization.to_excel(writer, index=False, sheet_name="Utilization")
        df2.to_excel(writer, index=False, sheet_name="Eco driving")
        events_pvt.to_excel(writer, index=False, sheet_name="Scoring")
        trips_df2.to_excel(writer, index=False, sheet_name="Trips")
    
    output.seek(0)
    return output


# Function to allow file download
def download_excel_button(data, group_name):
    b64 = base64.b64encode(data).decode()
    href = f'<a href="data:application/vnd.openxmlformats-officedocument.spreadsheetml.sheet;base64,{b64}" download="{group_name}_Report.xlsx">Download All Reports</a>'
    st.markdown(href, unsafe_allow_html=True)

# Function to get EID
def get_eid():
    with open('accounts.json') as fp:
        res = json.load(fp)
    access_token = res['track3']['access_token']
    url = f'https://hst-api.wialon.com/wialon/ajax.html?svc=token/login&params={{"token":"{access_token}"}}'
    r = requests.post(url)
    eid = json.loads(r.text)['eid']
    return eid

# Load group data from Excel
@st.cache
def load_group_data(file_path):
    try:
        df = pd.read_excel(file_path, sheet_name="Sheet1")  # Adjust to your sheet name
        return df[["id", "report_name"]]  # Ensure these columns exist in the Excel file
    except Exception as e:
        st.error(f"Error loading Excel file: {e}")
        return pd.DataFrame()

# Streamlit UI components
st.title("Trips and Eco Driving Report Table")

# Upload the Excel file containing group information
group_file = st.file_uploader("Upload Excel file with group data", type=["xlsx"])
if group_file:
    group_data = load_group_data(group_file)
    
    if not group_data.empty:
        # Display selectbox for group name
        group_name = st.selectbox("Select Group Name", group_data["report_name"])
        # Get the corresponding group_id
        group_id = group_data[group_data["report_name"] == group_name]["id"].values[0]

        # Display selected values
        st.write(f"Selected Group Name: {group_name}")
        st.write(f"Corresponding Group ID: {group_id}")

        # Fetch EID
        eid = get_eid()

        # Date range input
        from_date = st.date_input("From Date")
        to_date = st.date_input("To Date")

        # Correct timezone for Nairobi
        nairobi_tz = pytz.timezone('Africa/Nairobi')

        # Localize the dates to Nairobi time
        from_date = datetime.combine(from_date, datetime.min.time())
        to_date = datetime.combine(to_date, datetime.max.time())

        from_date = nairobi_tz.localize(from_date)
        to_date = nairobi_tz.localize(to_date)

        # Ensure both from_date and to_date are converted to pandas Timestamps for comparison
        from_date = pd.Timestamp(from_date)
        to_date = pd.Timestamp(to_date)

        if st.button("Fetch Reports"):
            if from_date and to_date:
                from_datetime = datetime.combine(from_date, datetime.min.time())
                to_datetime = datetime.combine(to_date, datetime.max.time())

                # Fetch Trip Report
                trip_report = get_trip_report(group_id, from_datetime, to_datetime, group_trips_stops_parkings_report_template, eid)
                trip_df = pd.read_excel(trip_report, engine="openpyxl", sheet_name="Trips")# For .xlsx files
                # # Filter for trips where 'Count' is 1
                trips_df2 = trip_df[trip_df['Count'] == 1].copy()
                trips_df2['Beginning'] = pd.to_datetime(trips_df2.Beginning, format='%d.%m.%Y %H:%M:%S')
                trips_df2['End'] = pd.to_datetime(trips_df2.End, format='%d.%m.%Y %H:%M:%S')

                # Ensure that 'Beginning' and 'End' are timezone-aware and set to Nairobi time
                trips_df2['Beginning'] = trips_df2['Beginning'].dt.tz_localize('Africa/Nairobi', ambiguous='NaT', nonexistent='NaT')
                trips_df2['End'] = trips_df2['End'].dt.tz_localize('Africa/Nairobi', ambiguous='NaT', nonexistent='NaT')

                # Filter trips that fall within the selected date range
                trips_df2 = trips_df2[(trips_df2['Beginning'] >= from_date) & (trips_df2['End'] <= to_date)]
                #trips_df2.rename(columns={'Mileage':'Distance(KM)'}, inplace=True)
                trips_df2['day'] = trips_df2.Beginning.dt.day
                trips_df2['month'] = trips_df2.Beginning.dt.month
                trips_df2['year'] = trips_df2.Beginning.dt.year
                trips_df2['Duration'] = trips_df2.End - trips_df2.Beginning
                utilization = pd.pivot_table(trips_df2, values='Mileage', 
                            index='Grouping', columns=['year', 'month', 'day'],
                            fill_value=0.0, aggfunc=sum)
                utilization = utilization.round(2)
                trips_df2[['Mileage']] = trips_df2[['Mileage']].round(2)  # Adjust for any other relevant numeric columns
                

                def to_utilization_headers(date):
                    return date.strftime('%a')[:1] + '-' + str(date.day)
                columns = pd.to_datetime(utilization.columns.to_frame().reset_index(drop=True)).apply(to_utilization_headers)
                utilization.columns = columns
                days_with_trips = (utilization>0.0).sum(axis=1)
                days_without_trips = (utilization==0.0).sum(axis=1)


                daily_utilization = pd.pivot_table(trips_df2, values='Mileage', 
                                                        index='Grouping', columns=trips_df2.Beginning.dt.day_name(),
                                                        fill_value=0.0, aggfunc=sum
                                                        )
                daily_utilization = daily_utilization.round(2)

                Weekends = ['Saturday', 'Sunday']

                weekdays = daily_utilization.columns.difference(Weekends)
                weekends = daily_utilization.columns.intersection(Weekends)

                utilization['Weekday Distance (km)'] = daily_utilization[weekdays].sum(axis=1).round(2)
                utilization['Weekend Distance (km)'] = daily_utilization[weekends].sum(axis=1).round(2)
                utilization['Total Distance (km)'] = daily_utilization.sum(axis=1).round(2)


                utilization['Days With Trips'] = days_with_trips
                utilization['Days Without Trips'] = days_without_trips
                utilization.sort_values(by='Total Distance (km)', ascending=True, inplace=True)
                # Reset index to include Vehicle as a column
                utilization.reset_index(inplace=True)

                # Reset index for cleaner output
                trips_df2.reset_index(drop=True, inplace=True) 
                st.subheader("Trip Report")
                st.dataframe(trips_df2)
                st.subheader("Utilization Report")
                st.dataframe(utilization)
                
                # Fetch Eco Driving Report
                eco_report = get_eco_driving_report(group_id, from_datetime, to_datetime, eco_template, eid)
                eco_df = pd.read_excel(eco_report, engine="openpyxl", sheet_name="Eco driving")
                eco_df['Count'] = 1
                df2 = eco_df[eco_df['Violation'] != '-----']

                events_pvt = df2.pivot_table(values='Count', index='Grouping', columns='Violation', fill_value=0, aggfunc='sum')
                events_pvt.reset_index(inplace=True)

                columns = ['Grouping', 'Total Distance (km)']
                mileage_df = utilization[columns]
                merged_df = mileage_df.merge(events_pvt,on='Grouping', how='outer' )
                
                # Add Advanced Score column
                violation_columns = merged_df.columns.difference(['Grouping', 'Total Distance (km)'])  # Identify violation columns
                merged_df['Advanced Score'] = merged_df[violation_columns].sum(axis=1) 

                st.subheader("Eco Driving Report")
                st.dataframe(df2)
                st.subheader("RAG Score Report")
                st.dataframe(merged_df)    

                # Generate the Excel file with all reports
                excel_file = create_excel_file(utilization, df2, events_pvt, trips_df2, group_name)
                # Convert all datetime columns to timezone-unaware
                for col in trips_df2.select_dtypes(include=["datetime64[ns, UTC]", "datetime64[ns]"]):
                    trips_df2[col] = trips_df2[col].dt.tz_localize(None)

                # Add download button for the Excel file
                st.subheader("Download All Reports")
                download_excel_button(excel_file.read(), group_name)
