# full_app.py
# Complete Streamlit app with integrated create_excel_file and report fetching

# Import necessary libraries
import pandas as pd
import requests
import io
import json
from datetime import datetime
import streamlit as st
import base64
import pytz
import os

# Set the app layout to wide mode
st.set_page_config(layout="wide")

# Function to encode the background image in Base64
def get_base64_image(image_path):
    with open(image_path, "rb") as image_file:
        return base64.b64encode(image_file.read()).decode()

# Add background image (optional — ensure the file exists)
background_image_path = "scene-with-photorealistic-logistics-operations-proceedings.jpg"
if os.path.exists(background_image_path):
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
        "reportResourceId": 17082202,
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
        "reportResourceId": 26749909,
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

import io
import pandas as pd

def create_excel_file(utilization, df2, merged_df, trips_df2, group_name, prev_month_data=None):
    """
    Create Excel report with Utilization, Eco driving, Trips, and Scoring sheets.
    """

    # --- Helper: strip timezones ---
    def remove_timezones(df):
        for col in df.select_dtypes(include=["datetimetz"]).columns:
            df[col] = df[col].dt.tz_localize(None)
        return df

    # --- Helper: convert col index → Excel letters ---
    def excel_col_letter(idx):
        letters = ""
        while idx >= 0:
            letters = chr(idx % 26 + 65) + letters
            idx = idx // 26 - 1
        return letters

    utilization = remove_timezones(utilization.copy())
    df2 = remove_timezones(df2.copy())
    merged_df = remove_timezones(merged_df.copy())
    trips_df2 = remove_timezones(trips_df2.copy())

    # Drop Change Indicator in scoring sheet if exists (you asked it not to be present)
    if "Change Indicator" in merged_df.columns:
        merged_df = merged_df.drop(columns=["Change Indicator"])

    # --- Detect Total Distance column ---
    possible_total_cols = [c for c in utilization.columns if "total" in c.lower() and "km" in c.lower()]
    if possible_total_cols:
        total_col = possible_total_cols[0]
    else:
        raise KeyError("No column found that looks like 'Total Distance (km)' in utilization DataFrame")

    # --- Prepare Utilization stats for comments ---
    least_vehicle = utilization.loc[utilization[total_col].idxmin()]
    most_vehicle = utilization.loc[utilization[total_col].idxmax()]
    avg_distance = utilization[total_col].mean()

    least_comment = f"The least utilized vehicle was {least_vehicle['Grouping']} with {least_vehicle[total_col]:.2f} KM"
    most_comment = f"The most utilized vehicle was {most_vehicle['Grouping']} with {most_vehicle[total_col]:.2f} KM"
    avg_comment = f"The average distance covered by each vehicle in the fleet was {avg_distance:.1f} KM"

    # --- Add TOTAL row (sum only up to Total Distance (km)) ---
    total_row = {}
    for col in utilization.columns:
        if col == "Grouping":
            total_row[col] = "TOTAL"
        elif col in utilization.columns[1:utilization.columns.get_loc(total_col)+1]:
            total_row[col] = utilization[col].sum(skipna=True)
        else:
            total_row[col] = ""
    utilization = pd.concat([utilization, pd.DataFrame([total_row])], ignore_index=True)

    # --- Excel writer ---
    output = io.BytesIO()
    writer = pd.ExcelWriter(output, engine="xlsxwriter")
    workbook = writer.book

    # --- Formats ---
    bold_fmt = workbook.add_format({"bold": True})
    header_fmt = workbook.add_format({"bold": True, "bg_color": "#ADD8E6", "text_wrap": True, "border": 1})
    total_fmt = workbook.add_format({"bold": True, "bg_color": "#E6E6FA"})
    border_fmt = workbook.add_format({"border": 1})

    # ---------------- Utilization ----------------
    sheet_util = "Utilization"
    utilization.to_excel(writer, index=False, sheet_name=sheet_util, startrow=10)
    ws_util = writer.sheets[sheet_util]

    ws_util.write(0, 0, "DAILY UTILIZATION REPORT", bold_fmt)

    # Legend
    ws_util.write(2, 0, "Less Than 0.1 km", workbook.add_format({"bold": True, "bg_color": "#FF0000"}))
    ws_util.write(3, 0, "Less Than 10 km", workbook.add_format({"bold": True, "bg_color": "#FFA500"}))
    ws_util.write(4, 0, "Less Than 100 km", workbook.add_format({"bold": True, "bg_color": "#FFFF00"}))
    ws_util.write(5, 0, "More Than 100 km", workbook.add_format({"bold": True, "bg_color": "#90EE90"}))

    # Comments
    ws_util.write(7, 0, least_comment, bold_fmt)
    ws_util.write(8, 0, most_comment, bold_fmt)
    ws_util.write(9, 0, avg_comment, bold_fmt)

    # Headers
    for i, col in enumerate(utilization.columns):
        ws_util.write(10, i, col, header_fmt)
        max_len = max(utilization[col].astype(str).map(len).max(), len(str(col))) + 2
        ws_util.set_column(i, i, max_len)

    # Borders
    table_range = f"A11:{excel_col_letter(len(utilization.columns)-1)}{10 + len(utilization)}"
    ws_util.conditional_format(table_range, {"type": "no_errors", "format": border_fmt})

    # Conditional formatting for daily km (before "Weekday Distance (km)")
    cutoff_idx = utilization.columns.get_loc("Weekday Distance (km)")
    for i, col in enumerate(utilization.columns[:cutoff_idx]):
        if col != "Grouping" and pd.api.types.is_numeric_dtype(utilization[col]):
            nrows = len(utilization)
            data_start = 11
            data_end = data_start + nrows - 2  # exclude TOTAL
            col_letter = excel_col_letter(i)
            cell_range = f"{col_letter}{data_start+1}:{col_letter}{data_end+1}"
            ws_util.conditional_format(cell_range, {"type": "cell", "criteria": "<", "value": 0.1,
                                                   "format": workbook.add_format({"bg_color": "#FF0000"})})
            ws_util.conditional_format(cell_range, {"type": "cell", "criteria": "<", "value": 10,
                                                   "format": workbook.add_format({"bg_color": "#FFA500"})})
            ws_util.conditional_format(cell_range, {"type": "cell", "criteria": "<", "value": 100,
                                                   "format": workbook.add_format({"bg_color": "#FFFF00"})})
            ws_util.conditional_format(cell_range, {"type": "cell", "criteria": ">=", "value": 100,
                                                   "format": workbook.add_format({"bg_color": "#90EE90"})})

    # Highlight TOTAL row
    total_row_index = 10 + len(utilization)
    ws_util.set_row(total_row_index, None, total_fmt)

    # ---------------- Eco Driving ----------------
    sheet_eco = "Eco driving"
    df2.to_excel(writer, index=False, sheet_name=sheet_eco)
    ws_eco = writer.sheets[sheet_eco]
    for i, col in enumerate(df2.columns):
        ws_eco.set_column(i, i, max(df2[col].astype(str).map(len).max(), len(str(col))) + 2)

    # ---------------- Trips ----------------
    sheet_trips = "Trips"
    trips_df2.to_excel(writer, index=False, sheet_name=sheet_trips)
    ws_trips = writer.sheets[sheet_trips]
    for i, col in enumerate(trips_df2.columns):
        ws_trips.set_column(i, i, max(trips_df2[col].astype(str).map(len).max(), len(str(col))) + 2)

    # ---------------- Scoring ----------------
    sheet_score = "Scoring"
    merged_df = merged_df.copy()

    # Ensure Advanced Score Change column (numeric)
    if "Advanced Score" in merged_df.columns and "Previous Advanced Score" in merged_df.columns:
        merged_df["Advanced Score Change"] = merged_df["Previous Advanced Score"] - merged_df["Advanced Score"]

    # ----- Prepare Top Violators narrative using merged_df violation columns -----
    # Explanations (as you requested)
    violation_explanations = {
        "Harsh Acceleration": "This violation reduces tire life and increases fuel consumption.",
        "Harsh Braking": "This violation causes damage of brake pads & Brake drums, suspension parts and may lead to tire burst and reduced tire life.",
        "Over Speeding": "This violation results in high fuel consumption, and a high risk of accidents.",
        "Free Wheeling": "Freewheeling is likely to cause Gearbox Damage and engine problems in case the driver engages the wrong gear after freewheeling, there’s also increased chances of an accident.",
        "Harsh Cornering": "This violation increases the chances of a possible rollover."
    }
    # treat these as distance-based (km)
    distance_based = ["Over Speeding", "Free Wheeling", "Over Speeding(km)", "Free Wheeling(km)"]

    # Write merged_df to sheet starting at row 25 (keeps your original layout)
    merged_df.to_excel(writer, index=False, sheet_name=sheet_score, startrow=25)
    ws_score = writer.sheets[sheet_score]

    # --- Fleet stats (top narrative) ---
    if "Advanced Score" in merged_df.columns:
        total_fleet = len(merged_df)
        green = merged_df[merged_df["Advanced Score"] <= 20]
        amber = merged_df[(merged_df["Advanced Score"] > 20) & (merged_df["Advanced Score"] <= 40)]
        red   = merged_df[merged_df["Advanced Score"] > 40]

        ws_score.write(0, 0, "This report shows a classification of drivers in 3 different categories: Green, Amber, and Red.", bold_fmt)
        ws_score.write(1, 0, "We have also made a comparison between the two months, The Red dots are an indication that a vehicle increased on the violations from the previous month, the Light green dots shows an improvement on the different drivers and amber shows no change.", bold_fmt)

        ws_score.write(3, 0, "Green Drivers (0 - 20 violations)", bold_fmt)
        ws_score.write(4, 0, "The drivers in this group can serve as mentors or coaches for the rest of the team.")
        ws_score.write(5, 0, f"This category includes {len(green)} vehicles, accounting for {len(green)/total_fleet*100:.2f}% of the total fleet.")

        ws_score.write(7, 0, "Amber Drivers (21 - 40 violations)", bold_fmt)
        ws_score.write(8, 0, "These are the average drivers...")
        ws_score.write(9, 0, f"This category includes {len(amber)} vehicles, accounting for {len(amber)/total_fleet*100:.2f}% of the total fleet")

        ws_score.write(11, 0, "Red Drivers (above 40 violations)", bold_fmt)
        ws_score.write(12, 0, "These drivers require immediate coaching and support...")
        ws_score.write(13, 0, f"This category includes {len(red)} vehicles, accounting for {len(red)/total_fleet*100:.2f}% of the total fleet")

        ws_score.write(22, 0, "The table below outlines the vehicles in the three categories:", bold_fmt)

    # ----------------------------
    # Top Violators block (narrative) - placed above the table (between rows ~15..23)
    # ----------------------------
    # Determine which columns in merged_df look like violation columns:
    cols_to_exclude = {"Grouping", total_col, "Total Distance (km)", "Previous Advanced Score", "Advanced Score", "Advanced Score Change"}
    violation_columns = [c for c in merged_df.columns if c not in cols_to_exclude]

    # Normalize names to compare to our explanation keys (case-insensitive)
    # We'll try to match by case-insensitive equality to keys in violation_explanations.
    # If your merged_df uses slightly different names, adjust the keys above accordingly.
    top_block_row = 16  # start writing top violators narrative here (keeps space from RAG text)
    ws_score.write(top_block_row - 1, 0, "Top Violators:", bold_fmt)

    for i, vcol in enumerate(violation_columns):
        display_name = vcol  # actual column name
        # match explanation key case-insensitively
        matched_key = None
        for k in violation_explanations.keys():
            if k.lower() == display_name.lower():
                matched_key = k
                break

        # If no exact match found, still include as a generic violation
        explanation = violation_explanations.get(matched_key, "")
        # Determine units
        unit = " km" if any(display_name.lower().startswith(x.lower()) for x in distance_based) else " occurrences"
        # Top 3 from merged_df for that column (if numeric)
        top_text = "No data"
        try:
            # ensure numeric sort: coerce errors to 0
            tmp = merged_df[[ "Grouping", display_name ]].copy()
            tmp[display_name] = pd.to_numeric(tmp[display_name], errors="coerce").fillna(0)
            tmp_sorted = tmp.sort_values(by=display_name, ascending=False).head(3)
            top_text = ", ".join([f"{r['Grouping']} - {r[display_name]}{unit}" for _, r in tmp_sorted.iterrows()])
        except Exception:
            top_text = "No data"

        line = f"{display_name}: {explanation} Top violators were: {top_text}"
        ws_score.write(top_block_row + i, 0, line)

    # Compact column widths for scoring sheet (for the table area)
    for i, col in enumerate(merged_df.columns):
        ws_score.set_column(i, i, len(str(col)) + 2)

    writer.close()
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

# Function to get previous month data for comparison
def get_previous_month_data(group_id, from_date, to_date, eid):
    """Fetch previous month's eco driving data for comparison"""
    try:
        # Calculate previous month date range
        prev_month_start = from_date - pd.DateOffset(months=1)
        prev_month_end = to_date - pd.DateOffset(months=1)
        
        # Fetch previous month eco driving report
        prev_eco_report = get_eco_driving_report(group_id, prev_month_start, prev_month_end, eco_template, eid)
        prev_eco_df = pd.read_excel(io.BytesIO(prev_eco_report), engine="openpyxl", sheet_name="Eco driving")
        prev_eco_df['Count'] = 1
        prev_df2 = prev_eco_df[prev_eco_df['Violation'] != '-----']
        
        # Create previous month pivot table
        prev_events_pvt = prev_df2.pivot_table(values='Count', index='Grouping', columns='Violation', fill_value=0, aggfunc='sum')
        prev_events_pvt.reset_index(inplace=True)
        
        # Calculate previous month Advanced Score
        violation_columns = prev_events_pvt.columns.difference(['Grouping'])
        prev_events_pvt['Advanced Score'] = prev_events_pvt[violation_columns].sum(axis=1)
        
        return prev_events_pvt
        
    except Exception as e:
        print(f"Error fetching previous month data: {e}")
        return None

# Load group data from Excel
@st.cache_data  # Updated from st.cache which is deprecated
def load_group_data():
    try:
        # Try multiple possible file paths
        possible_paths = [
            "track3_unit groups.xlsx",
            os.path.join(os.getcwd(), "track3_unit groups.xlsx"),
            "./track3_unit groups.xlsx"
        ]
        
        df = None
        used_path = None
        
        for file_path in possible_paths:
            try:
                if os.path.exists(file_path):
                    df = pd.read_excel(file_path, sheet_name="Sheet1")
                    used_path = file_path
                    break
            except Exception:
                continue
        
        if df is None:
            st.error("Could not load the Excel file. Please ensure 'track3_unit groups.xlsx' is in the same directory as this script.")
            st.info("Expected columns: 'id' and 'report_name'")
            return pd.DataFrame()
        
        # Check if required columns exist
        required_columns = ["id", "report_name"]
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            st.error(f"Excel file is missing required columns: {missing_columns}")
            st.info(f"Available columns: {df.columns.tolist()}")
            return pd.DataFrame()
        
        st.success(f"Successfully loaded Excel file: {used_path}")
        return df[["id", "report_name"]]
        
    except Exception as e:
        st.error(f"Error loading Excel file: {e}")
        return pd.DataFrame()

# Streamlit UI components
st.title("Trips and Eco Driving Report Table")

# Load group data from local Excel file
group_data = load_group_data()

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
            trip_df = pd.read_excel(io.BytesIO(trip_report), engine="openpyxl", sheet_name="Trips")
            # Filter for trips where 'Count' is 1
            trips_df2 = trip_df[trip_df['Count'] == 1].copy()
            trips_df2['Beginning'] = pd.to_datetime(trips_df2.Beginning, format='%d.%m.%Y %H:%M:%S')
            trips_df2['End'] = pd.to_datetime(trips_df2.End, format='%d.%m.%Y %H:%M:%S')

            # Ensure that 'Beginning' and 'End' are timezone-aware and set to Nairobi time
            trips_df2['Beginning'] = trips_df2['Beginning'].dt.tz_localize('Africa/Nairobi', ambiguous='NaT', nonexistent='NaT')
            trips_df2['End'] = trips_df2['End'].dt.tz_localize('Africa/Nairobi', ambiguous='NaT', nonexistent='NaT')

            # Filter trips that fall within the selected date range
            trips_df2 = trips_df2[(trips_df2['Beginning'] >= from_date) & (trips_df2['End'] <= to_date)]
            trips_df2['day'] = trips_df2.Beginning.dt.day
            trips_df2['month'] = trips_df2.Beginning.dt.month
            trips_df2['year'] = trips_df2.Beginning.dt.year
            trips_df2['Duration'] = trips_df2.End - trips_df2.Beginning
            utilization = pd.pivot_table(trips_df2, values='Mileage', 
                        index='Grouping', columns=['year', 'month', 'day'],
                        fill_value=0.0, aggfunc='sum')
            utilization = utilization.round(2)
            trips_df2[['Mileage']] = trips_df2[['Mileage']].round(2)

            def to_utilization_headers(date):
                return date.strftime('%a')[:1] + '-' + str(date.day)
            # Try to convert multiindex columns to readable headers; if not possible, keep as-is
            try:
                columns = pd.to_datetime(utilization.columns.to_frame().reset_index(drop=True)).apply(to_utilization_headers)
                utilization.columns = columns
            except Exception:
                # fallback: flatten multiindex to strings
                utilization.columns = ['-'.join(map(str, c)) for c in utilization.columns]

            days_with_trips = (utilization>0.0).sum(axis=1)
            days_without_trips = (utilization==0.0).sum(axis=1)

            daily_utilization = pd.pivot_table(trips_df2, values='Mileage', 
                                                    index='Grouping', columns=trips_df2.Beginning.dt.day_name(),
                                                    fill_value=0.0, aggfunc='sum'
                                                    )
            daily_utilization = daily_utilization.round(2)

            Weekends = ['Saturday', 'Sunday']

            weekdays = daily_utilization.columns.difference(Weekends)
            weekends = daily_utilization.columns.intersection(Weekends)

            if len(weekdays) > 0:
                utilization['Weekday Distance (km)'] = daily_utilization[weekdays].sum(axis=1).round(2)
            else:
                utilization['Weekday Distance (km)'] = 0.0
            if len(weekends) > 0:
                utilization['Weekend Distance (km)'] = daily_utilization[weekends].sum(axis=1).round(2)
            else:
                utilization['Weekend Distance (km)'] = 0.0

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
            eco_df = pd.read_excel(io.BytesIO(eco_report), engine="openpyxl", sheet_name="Eco driving")
            eco_df['Count'] = 1
            df2 = eco_df[eco_df['Violation'] != '-----']

            events_pvt = df2.pivot_table(values='Count', index='Grouping', columns='Violation', fill_value=0, aggfunc='sum')
            events_pvt.reset_index(inplace=True)

            columns = ['Grouping', 'Total Distance (km)']
            mileage_df = utilization[columns]
            merged_df = mileage_df.merge(events_pvt,on='Grouping', how='outer' ).fillna(0)
            
            # Add Advanced Score column
            violation_columns = merged_df.columns.difference(['Grouping', 'Total Distance (km)'])  # Identify violation columns
            merged_df['Advanced Score'] = merged_df[violation_columns].sum(axis=1) 

            st.subheader("Eco Driving Report")
            st.dataframe(df2)
            st.subheader("RAG Score Report")
            st.dataframe(merged_df)    

            # Fetch previous month data for comparison
            st.info("Fetching previous month data for comparison...")
            prev_month_data = get_previous_month_data(group_id, from_date, to_date, eid)
            
            # Display previous month data
            if prev_month_data is not None:
                st.subheader("Previous Month Eco Driving Data")
                st.dataframe(prev_month_data)
                
                # Create a lookup dictionary for previous month Advanced Scores
                prev_month_lookup = dict(zip(prev_month_data['Grouping'], prev_month_data['Advanced Score']))
                
                # Add Previous Advanced Score column to current month data
                merged_df['Previous Advanced Score'] = merged_df['Grouping'].map(prev_month_lookup).fillna(0)
                
                # Add Advanced Score Change column
                merged_df['Advanced Score Change'] = merged_df['Previous Advanced Score'] - merged_df['Advanced Score']
                
                st.dataframe(merged_df)
            else:
                st.warning("Could not fetch previous month data. Proceeding without comparison.")
            
            # Generate the Excel file with all reports including previous month comparison
            excel_file = create_excel_file(utilization, df2, merged_df, trips_df2, group_name, prev_month_data)
            
            # Convert all datetime columns to timezone-unaware
            for col in trips_df2.select_dtypes(include=["datetime64[ns, UTC]", "datetime64[ns]"]):
                trips_df2[col] = trips_df2[col].dt.tz_localize(None)

            # Add download button for the Excel file
            st.subheader("Download All Reports")
            if excel_file is not None:
                download_excel_button(excel_file.read(), group_name)
            else:
                st.error("Failed to generate Excel file. Please check the error messages above.")
