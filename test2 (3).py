# full_app_fixed.py
# Streamlit app: Trips and Eco Driving Report (consolidated & fixed)

import os
import io
import json
import time
from datetime import datetime

import base64
import pandas as pd
import pytz
import requests
import streamlit as st

# --- App config ---
st.set_page_config(layout="wide")

# -----------------------------
# Helper: background (optional)
# -----------------------------
def get_base64_image(image_path):
    with open(image_path, "rb") as image_file:
        return base64.b64encode(image_file.read()).decode()

background_image_path = "scene-with-photorealistic-logistics-operations-proceedings.jpg"
if os.path.exists(background_image_path):
    try:
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
            unsafe_allow_html=True,
        )
    except Exception:
        pass

# -----------------------------
# Report templates (constants)
# -----------------------------
# (Kept as in original file)
eco_template = [{'id': 8,'n': '20cube - Eco Driving Per Fleet','ct': 'avl_unit_group','p': '{"descr":"","bind":{"avl_unit_group":[]}}','tbl': [{'n': 'unit_group_stats','l': 'Statistics','c': '','cl': '','cp': '','s': '["address_format","time_format","us_units","deviation"]','sl': '["Address","Time Format","Measure","Deviation"]','filter_order': [],'p': '{"address_format":"1178599424_10_5","time_format":"%E.%m.%Y_%H:%M:%S","us_units":0,"deviation":"30"}','sch': {'f1': 0,'f2': 0,'t1': 0,'t2': 0,'m': 0,'y': 0,'w': 0,'fl': 0},'f': 0},{'n': 'unit_group_ecodriving','l': 'Eco driving','c': '["violation_name","time_begin","time_end","location","location_end","mileage","violations_count","violation_value","avg_speed","max_speed","violation_mark","driver","violation_duration","violation_mileage"]','cl': '["Violation","Beginning","End","Initial location","Final location","Mileage","Count","Value","Avg speed","Max speed","Penalties","Driver","Violation duration","Violation mileage"]','cp': '[{},{},{},{},{},{},{},{},{},{},{},{},{},{}]','s': '','sl': '','filter_order': ['violation_group_name','violation_duration','show_all_trips','mileage','colors','custom_sensors_col','geozones_ex'],'p': '{"grouping":"{\\"type\\":\\"unit\\",\\"nested\\":{\\"type\\":\\"criterion\\"}}","violation_group_name":"*"}','sch': {'f1': 0,'f2': 0,'t1': 0,'t2': 0,'m': 0,'y': 0,'w': 0,'fl': 0},'f': 4198672}],'bsfl': {'ct': 1683207805, 'mt': 1721114533}}]

group_trips_stops_parkings_report_template = {"id":0,"n":"Group Trips Stops and Parkings Report","ct":"avl_unit_group","p":"{\"descr\":\"\",\"bind\":{\"avl_unit_group\":[]}}","tbl":[{"n":"unit_group_stats","l":"Statistics","c":"","cl":"","cp":"","s":"[\"address_format\",\"time_format\",\"us_units\"]","sl":"[\"Address\",\"Time Format\",\"Measure\"]","filter_order":[],"p":"{\"address_format\":\"1178599424_10_5\",\"time_format\":\"%E.%m.%Y_%H:%M:%S\",\"us_units\":0}","sch":{"f1":0,"f2":0,"t1":0,"t2":0,"m":0,"y":0,"w":0,"fl":0},"f":0},{"n":"unit_group_trips","l":"Trips","c":"[\"time_begin\",\"time_end\",\"location_begin\",\"location_end\",\"coord_begin\",\"coord_end\",\"duration\",\"duration_ival\",\"eh_duration\",\"mileage\",\"correct_mileage\",\"absolute_mileage_begin\",\"absolute_mileage_end\",\"avg_speed\",\"max_speed\",\"driver\",\"trips_count\",\"fuel_consumption_all\",\"fuel_consumption_fls\",\"fuel_level_begin\",\"fuel_level_end\",\"fuel_level_max\",\"fuel_level_min\"]","cl":"[\"Beginning\",\"End\",\"Initial location\",\"Final location\",\"Initial coordinates\",\"Final coordinates\",\"Duration\",\"Total time\",\"Engine hours\",\"Mileage\",\"Mileage (adjusted)\",\"Initial mileage\",\"Final mileage\",\"Avg speed\",\"Max speed\",\"Driver\",\"Count\",\"Consumed\",\"Consumed by FLS\",\"Initial fuel level\",\"Final fuel level\",\"Max fuel level\",\"Min fuel level\"]","cp":"[{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}]","s":"","sl":"","filter_order":["duration","mileage","base_eh_sensor","engine_hours","speed","stops","sensors","sensor_name","custom_sensors_col","driver","trailer","geozones_ex"],"p":"","sch":{"f1":0,"f2":0,"t1":0,"t2":0,"m":0,"y":0,"w":0,"fl":0},"f":256},{"n":"unit_group_stops","l":"Stops","c":"[\"time_begin\",\"time_end\",\"duration\",\"driver\",\"location\",\"coord\",\"stops_count\"]","cl":"[\"Beginning\",\"End\",\"Duration\",\"Driver\",\"Location\",\"Coordinates\",\"Count\"]","cp":"[{},{},{},{},{},{},{}]","s":"","sl":"","filter_order":["duration","sensors","sensor_name","driver","trailer","fillings","thefts","geozones_ex"],"p":"","sch":{"f1":0,"f2":0,"t1":0,"t2":0,"m":0,"y":0,"w":0,"fl":0},"f":256},{"n":"unit_group_stays","l":"Parkings","c":"[\"time_begin\",\"time_end\",\"duration\",\"location\",\"coord\",\"driver\",\"stays_count\"]","cl":"[\"Beginning\",\"End\",\"Duration\",\"Location\",\"Coordinates\",\"Driver\",\"Count\"]","cp":"[{},{},{},{},{},{},{}]","s":"","sl":"","filter_order":["duration","sensors","sensor_name","fillings","thefts","driver","trailer","geozones_ex"],"p":"","sch":{"f1":0,"f2":0,"t1":0,"t2":0,"m":0,"y":0,"w":0,"fl":0},"f":256}],"bsfl":{"ct":1675063376,"mt":1675063897}}

# -----------------------------
# HTTP helpers: execute report and download with retries
# -----------------------------

def exec_and_download_report(params_dict, sid, max_retries=6, delay=2):
    """
    Executes a report (exec_report) then polls export_result until file is available.
    params_dict: dict -> will be JSON-dumped into the exec_report URL
    sid: session id / eid
    Returns: bytes (file content) or raises RuntimeError
    """
    exec_params = json.dumps(params_dict, separators=(',', ':'))
    exec_url = f'https://hst-api.wialon.com/wialon/ajax.html?svc=report/exec_report&params={exec_params}&sid={sid}'
    export_url = r'https://hst-api.wialon.com/wialon/ajax.html?svc=report/export_result&params={"format":8,"compress":0}&sid=' + sid

    # fire exec
    r_exec = requests.post(exec_url)
    if r_exec.status_code != 200:
        raise RuntimeError(f"exec_report failed: {r_exec.status_code} {r_exec.text}")

    # poll for download
    for attempt in range(max_retries):
        r_down = requests.post(export_url)
        if r_down.status_code == 200 and r_down.content and len(r_down.content) > 100:
            return r_down.content
        time.sleep(delay)
    raise RuntimeError("Failed to download report after retries")


# -----------------------------
# Report-specific wrappers
# -----------------------------

def get_trip_report(ID, FROM, TO, group_template, eid):
    params = {
        "reportResourceId": 17082202,
        "reportTemplateId": 0,
        "reportObjectId": int(ID),
        "reportObjectSecId": 0,
        "reportTemplate": group_template,
        "interval": {"from": int(FROM.timestamp()), "to": int(TO.timestamp()), "flags": 0},
    }
    return exec_and_download_report(params, eid)


def get_eco_driving_report(ID, FROM, TO, eco_template_obj, eid):
    params = {
        "reportResourceId": 26749909,
        "reportTemplateId": 8,
        "reportObjectId": int(ID),
        "reportObjectSecId": 0,
        "reportTemplate": eco_template_obj,
        "interval": {"from": int(FROM.timestamp()), "to": int(TO.timestamp()), "flags": 0},
    }
    return exec_and_download_report(params, eid)

# -----------------------------
# Safer remove_timezones
# -----------------------------

def remove_timezones(df):
    df = df.copy()
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            try:
                # if tz-aware, convert to naive UTC then drop tz
                if df[col].dt.tz is not None:
                    df[col] = df[col].dt.tz_convert(None)
                else:
                    df[col] = pd.to_datetime(df[col])
            except Exception:
                try:
                    df[col] = pd.to_datetime(df[col]).dt.tz_localize(None)
                except Exception:
                    pass
    return df

# -----------------------------
# Excel creation (mostly unchanged, with safe timezone removal)
# -----------------------------

def create_excel_file(utilization, df2, merged_df, trips_df2, group_name, prev_month_data=None):
    """Create a complete Excel report with multiple sheets"""
    import io
    import pandas as pd

    # Remove timezones safely
    def remove_timezones(df):
        df = df.copy()
        for col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                try:
                    if df[col].dt.tz is not None:
                        df[col] = df[col].dt.tz_convert(None)
                    else:
                        df[col] = pd.to_datetime(df[col])
                except Exception:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
        return df

    utilization = remove_timezones(utilization.copy())
    df2 = remove_timezones(df2.copy())
    merged_df = remove_timezones(merged_df.copy())
    trips_df2 = remove_timezones(trips_df2.copy())

    # Drop 'Change Indicator' if present
    if "Change Indicator" in merged_df.columns:
        merged_df = merged_df.drop(columns=["Change Indicator"])

    # Prepare Excel writer
    output = io.BytesIO()
    writer = pd.ExcelWriter(output, engine="xlsxwriter")
    workbook = writer.book

    # Formats
    bold_fmt = workbook.add_format({"bold": True})
    header_fmt = workbook.add_format({"bold": True, "bg_color": "#ADD8E6", "border": 1})
    total_fmt = workbook.add_format({"bold": True, "bg_color": "#E6E6FA"})
    border_fmt = workbook.add_format({"border": 1})

    # ----------------- Utilization Sheet -----------------
    utilization.to_excel(writer, index=False, sheet_name="Utilization", startrow=10)
    ws_util = writer.sheets["Utilization"]

    ws_util.write(0, 0, "DAILY UTILIZATION REPORT", bold_fmt)

    # Optional summary
    if not utilization.empty and "Total Distance (km)" in utilization.columns:
        least_vehicle = utilization.loc[utilization["Total Distance (km)"].idxmin()]
        most_vehicle = utilization.loc[utilization["Total Distance (km)"].idxmax()]
        avg_distance = utilization["Total Distance (km)"].mean()

        ws_util.write(7, 0, f"Least utilized: {least_vehicle['Grouping']} ({least_vehicle['Total Distance (km)']:.2f} km)", bold_fmt)
        ws_util.write(8, 0, f"Most utilized: {most_vehicle['Grouping']} ({most_vehicle['Total Distance (km)']:.2f} km)", bold_fmt)
        ws_util.write(9, 0, f"Average distance: {avg_distance:.2f} km", bold_fmt)

    # ----------------- Eco Driving Sheet -----------------
    df2.to_excel(writer, index=False, sheet_name="Eco Driving")
    ws_eco = writer.sheets["Eco Driving"]
    for i, col in enumerate(df2.columns):
        ws_eco.set_column(i, i, max(df2[col].astype(str).map(len).max(), len(str(col))) + 2)

    # ----------------- Trips Sheet -----------------
    trips_df2.to_excel(writer, index=False, sheet_name="Trips")
    ws_trips = writer.sheets["Trips"]
    for i, col in enumerate(trips_df2.columns):
        ws_trips.set_column(i, i, max(trips_df2[col].astype(str).map(len).max(), len(str(col))) + 2)

    # ----------------- RAG / Score Sheet -----------------
    if "Advanced Score" in merged_df.columns:
        merged_df.to_excel(writer, index=False, sheet_name="Scoring", startrow=10)
        ws_score = writer.sheets["Scoring"]
        for i, col in enumerate(merged_df.columns):
            ws_score.set_column(i, i, max(15, len(str(col)) + 2))

    writer.close()
    output.seek(0)
    return output


# -----------------------------
# Download helper for Streamlit
# -----------------------------
def download_excel_button(data_bytes, group_name="Report"):
    """Streamlit download button for Excel files"""
    import base64
    import streamlit as st

    b64 = base64.b64encode(data_bytes).decode()
    href = f'<a href="data:application/vnd.openxmlformats-officedocument.spreadsheetml.sheet;base64,{b64}" download="{group_name}_Report.xlsx">📥 Download Excel Report</a>'
    st.markdown(href, unsafe_allow_html=True)


# -----------------------------
# EID helper (safer)
# -----------------------------

def get_eid():
    try:
        with open('accounts.json') as fp:
            res = json.load(fp)
        access_token = res.get('track3', {}).get('access_token')
        if not access_token:
            raise RuntimeError('access_token not found in accounts.json')
        url = f'https://hst-api.wialon.com/wialon/ajax.html?svc=token/login&params={{"token":"{access_token}"}}'
        r = requests.post(url)
        r.raise_for_status()
        data = r.json()
        return data.get('eid')
    except Exception as e:
        st.error(f"Error obtaining EID: {e}")
        return None

# -----------------------------
# Previous month data (calendar-aware)
# -----------------------------

def get_previous_month_data(group_id, from_date, to_date, eid):
    """Fetch previous calendar month's eco driving data for comparison"""
    try:
        # Ensure timestamps
        from_date = pd.Timestamp(from_date)
        to_date = pd.Timestamp(to_date)

        # Compute current month start based on from_date
        current_month_start = from_date.replace(day=1)
        prev_month_end = current_month_start - pd.Timedelta(seconds=1)
        prev_month_start = prev_month_end.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

        # Debug log
        print(f"Fetching previous month range: {prev_month_start} -> {prev_month_end}")

        prev_eco_report = get_eco_driving_report(group_id, prev_month_start, prev_month_end, eco_template, eid)
        prev_eco_df = pd.read_excel(io.BytesIO(prev_eco_report), engine="openpyxl", sheet_name="Eco driving")
        prev_eco_df['Count'] = 1
        prev_df2 = prev_eco_df[prev_eco_df['Violation'] != '-----']

        prev_events_pvt = prev_df2.pivot_table(values='Count', index='Grouping', columns='Violation', fill_value=0, aggfunc='sum').reset_index()
        violation_columns = prev_events_pvt.columns.difference(['Grouping'])
        prev_events_pvt['Advanced Score'] = prev_events_pvt[violation_columns].sum(axis=1)
        return prev_events_pvt
    except Exception as e:
        print(f"Error fetching previous month data: {e}")
        return None

# -----------------------------
# Load groups (Excel)
# -----------------------------
@st.cache_data
def load_group_data():
    possible_paths = [
        "track3_unit groups.xlsx",
        os.path.join(os.getcwd(), "track3_unit groups.xlsx"),
        "./track3_unit groups.xlsx",
    ]
    df = None
    used = None
    for p in possible_paths:
        try:
            if os.path.exists(p):
                df = pd.read_excel(p, sheet_name="Sheet1")
                used = p
                break
        except Exception:
            continue
    if df is None:
        st.error("Could not load the Excel file. Please ensure 'track3_unit groups.xlsx' is in the same directory as this script.")
        st.info("Expected columns: 'id' and 'report_name'")
        return pd.DataFrame()
    required = ["id", "report_name"]
    if any(c not in df.columns for c in required):
        st.error(f"Excel file is missing required columns: {required}")
        return pd.DataFrame()
    st.success(f"Successfully loaded Excel file: {used}")
    return df[["id", "report_name"]]

# -----------------------------
# Streamlit UI
# -----------------------------
st.title("Trips and Eco Driving Report Table")

group_data = load_group_data()

if not group_data.empty:
    group_name = st.selectbox("Select Group Name", group_data["report_name"])
    group_id = int(group_data[group_data["report_name"] == group_name]["id"].values[0])

    st.write(f"Selected Group Name: {group_name}")
    st.write(f"Corresponding Group ID: {group_id}")

    eid = get_eid()
    if not eid:
        st.stop()

    # Single date input: select any date within the month you want
    st.subheader("📅 Select Month for Report")
    selected_date = st.date_input("Select any date within the target month")

    # Compute month ranges
    selected_month_start = pd.Timestamp(selected_date).replace(day=1)
    next_month_start = (selected_month_start + pd.DateOffset(months=1)).replace(day=1)
    selected_month_end = next_month_start - pd.Timedelta(days=1)

    prev_month_end = selected_month_start - pd.Timedelta(days=1)
    prev_month_start = prev_month_end.replace(day=1)

    st.write(f"**Main Report Period:** {selected_month_start.strftime('%d %b %Y')} → {selected_month_end.strftime('%d %b %Y')}")
    st.write(f"**Comparison Period (Previous Month):** {prev_month_start.strftime('%d %b %Y')} → {prev_month_end.strftime('%d %b %Y')}")

    # Localize to Nairobi
    nairobi_tz = pytz.timezone('Africa/Nairobi')
    from_date = nairobi_tz.localize(datetime.combine(selected_month_start, datetime.min.time()))
    to_date = nairobi_tz.localize(datetime.combine(selected_month_end, datetime.max.time()))

    # Fetch reports when button clicked
    if st.button("🚀 Fetch Monthly Eco Driving Reports"):
        try:
            st.info(f"Generating report for {selected_month_start.strftime('%B %Y')} and comparing with {prev_month_start.strftime('%B %Y')}...")

            # --- Trips report (optional) ---
            try:
                trip_bytes = get_trip_report(group_id, from_date, to_date, group_trips_stops_parkings_report_template, eid)
                trip_df = pd.read_excel(io.BytesIO(trip_bytes), engine="openpyxl", sheet_name="Trips")
            except Exception as e:
                st.warning(f"Trips report could not be fetched: {e}")
                trip_df = pd.DataFrame()

            if not trip_df.empty:
                if 'Count' in trip_df.columns:
                    trips_df2 = trip_df[trip_df['Count'] == 1].copy()
                else:
                    trips_df2 = trip_df.copy()
                # parse datetimes robustly
                trips_df2['Beginning'] = pd.to_datetime(trips_df2.get('Beginning', None), errors='coerce', dayfirst=True)
                trips_df2['End'] = pd.to_datetime(trips_df2.get('End', None), errors='coerce', dayfirst=True)
                # localize if naive
                if trips_df2['Beginning'].dt.tz is None:
                    trips_df2['Beginning'] = trips_df2['Beginning'].dt.tz_localize('Africa/Nairobi', ambiguous='NaT', nonexistent='NaT')
                if trips_df2['End'].dt.tz is None:
                    trips_df2['End'] = trips_df2['End'].dt.tz_localize('Africa/Nairobi', ambiguous='NaT', nonexistent='NaT')

                # filter within selected month
                trips_df2 = trips_df2[(trips_df2['Beginning'] >= from_date) & (trips_df2['End'] <= to_date)].copy()
                trips_df2['Duration'] = trips_df2['End'] - trips_df2['Beginning']
                trips_df2['day'] = trips_df2['Beginning'].dt.day
                trips_df2['month'] = trips_df2['Beginning'].dt.month
                trips_df2['year'] = trips_df2['Beginning'].dt.year

                utilization = pd.pivot_table(trips_df2, values='Mileage',
                                             index='Grouping', columns=['year', 'month', 'day'],
                                             fill_value=0.0, aggfunc='sum').round(2)
                # flatten or format headers
                try:
                    def to_utilization_headers(date):
                        return date.strftime('%a')[:1] + '-' + str(date.day)
                    columns = pd.to_datetime(utilization.columns.to_frame().reset_index(drop=True)).apply(to_utilization_headers)
                    utilization.columns = columns
                except Exception:
                    utilization.columns = ['-'.join(map(str, c)) for c in utilization.columns]

                daily_utilization = pd.pivot_table(trips_df2, values='Mileage', index='Grouping', columns=trips_df2.Beginning.dt.day_name(), fill_value=0.0, aggfunc='sum').round(2)
                Weekends = ['Saturday', 'Sunday']
                weekdays = daily_utilization.columns.difference(Weekends)
                weekends = daily_utilization.columns.intersection(Weekends)
                utilization['Weekday Distance (km)'] = daily_utilization[weekdays].sum(axis=1).round(2) if len(weekdays) > 0 else 0.0
                utilization['Weekend Distance (km)'] = daily_utilization[weekends].sum(axis=1).round(2) if len(weekends) > 0 else 0.0
                utilization['Total Distance (km)'] = daily_utilization.sum(axis=1).round(2)
                days_with_trips = (utilization > 0.0).sum(axis=1)
                days_without_trips = (utilization == 0.0).sum(axis=1)
                utilization['Days With Trips'] = days_with_trips
                utilization['Days Without Trips'] = days_without_trips
                utilization.sort_values(by='Total Distance (km)', ascending=True, inplace=True)
                utilization.reset_index(inplace=True)

                st.subheader("Trip Report")
                st.dataframe(trips_df2)
                st.subheader("Utilization Report")
                st.dataframe(utilization)
            else:
                # create empty placeholders
                trips_df2 = pd.DataFrame()
                utilization = pd.DataFrame()

            # --- Eco driving current month ---
            eco_bytes = get_eco_driving_report(group_id, from_date, to_date, eco_template, eid)
            eco_df = pd.read_excel(io.BytesIO(eco_bytes), engine="openpyxl", sheet_name="Eco driving")

            eco_df["Count"] = 1
            df2 = eco_df[eco_df["Violation"] != "-----"]

            # Pivot to get violation counts per vehicle
            events_pvt = df2.pivot_table(
                values="Count",
                index="Grouping",
                columns="Violation",
                fill_value=0,
                aggfunc="sum"
            ).reset_index()

            # --- Advanced Score (simple sum of all violation counts) ---
            violation_columns = [c for c in events_pvt.columns if c != "Grouping"]
            events_pvt["Advanced Score"] = events_pvt[violation_columns].sum(axis=1)

            # Prepare mileage data if available
            if not utilization.empty and "Total Distance (km)" in utilization.columns:
                mileage_df = utilization[["Grouping", "Total Distance (km)"]]
            else:
                mileage_df = events_pvt[["Grouping"]].copy()
                mileage_df["Total Distance (km)"] = 0

            # Merge violations + mileage
            merged_df = mileage_df.merge(events_pvt, on="Grouping", how="outer").fillna(0)

            # --- FIX: Do NOT recalculate Advanced Score again here ---
            # We already have it computed correctly in events_pvt
            # So just ensure data consistency
            numeric_cols = merged_df.select_dtypes(include=["int", "float"]).columns
            merged_df[numeric_cols] = merged_df[numeric_cols].fillna(0).round(2)

            # Show reports
            st.subheader("Eco Driving Report")
            st.dataframe(df2)

            st.subheader("RAG Score Report")
            st.dataframe(merged_df)


            # previous month comparison
            st.info("Fetching previous month data for comparison...")
            prev_month_data = get_previous_month_data(group_id, from_date, to_date, eid)
            if prev_month_data is not None:
                st.subheader("Previous Month Eco Driving Data")
                st.dataframe(prev_month_data)
                prev_lookup = dict(zip(prev_month_data['Grouping'], prev_month_data['Advanced Score']))
                merged_df['Previous Advanced Score'] = merged_df['Grouping'].map(prev_lookup).fillna(0)
                merged_df['Advanced Score Change'] = merged_df['Advanced Score'] - merged_df['Previous Advanced Score']
                st.dataframe(merged_df)
            else:
                st.warning("Could not fetch previous month data. Proceeding without comparison.")

            # create excel
            try:
                excel_file = create_excel_file(utilization if not utilization.empty else pd.DataFrame(columns=['Grouping','Total Distance (km)']), df2, merged_df, trips_df2 if not trips_df2.empty else pd.DataFrame(), group_name, prev_month_data)
                st.subheader("Download All Reports")
                if excel_file is not None:
                    download_excel_button(excel_file.read(), group_name)
            except Exception as e:
                st.error(f"Failed to generate Excel file: {e}")

        except Exception as e:
            st.error(f"Error while fetching or processing reports: {e}")
            import traceback
            st.text(traceback.format_exc())

else:
    st.warning("No group data loaded. Please provide 'track3_unit groups.xlsx' with 'id' and 'report_name' columns.")
