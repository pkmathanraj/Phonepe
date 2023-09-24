import pandas as pd
import streamlit as st
from configparser import ConfigParser
from mysql.connector import connect, Error
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine
from sqlalchemy import text
from urllib.request import urlopen
import json

st.set_page_config(
    page_title="Dynamic Phonepe Dashboard",
    page_icon="✅",
    layout="wide"
    )

def config(filename='database.ini', section='mysql'):
    parser = ConfigParser()
    parser.read(filename)
  
    # get section, default to mysql
    conn_param = []
    if parser.has_section(section):
        params = parser.items(section)
        for p in params:
            conn_param.append(p[1])
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))
  
    return conn_param

# Establishing MySQL Connection
def dbconnection():
    params = config()
    try:
        return create_engine(url="mysql+mysqlconnector://{0}:{1}@{2}:{3}/{4}".format(*params))
    except Error as e:
        print("Error during establishing MySQL connection: ",e)

def data_extraction(type):    
    df = pd.DataFrame()
    try:
        with dbconnection().connect() as conn:
            if type == "***Transactions***":
                df = pd.read_sql("SELECT * from map_trans_country", conn)
                #df = pd.read_sql("SELECT * from map_trans_state", conn)
            elif type == "***User***":
                df = pd.read_sql("SELECT * from map_user_country", conn)
        return df
    except Error as e:
        print("Error during reading data from MySQL: ",e)
    finally:
        conn.close()

def plot_data(type, year, quarter):
    df = data_extraction(type)
    # Data Preprocessing
    df['state'] = df['state'].str.title()
    df['state'] = df['state'].replace('Andaman & Nicobar Islands', 'Andaman & Nicobar',regex=True)
    df['state'] = df['state'].replace('Dadra & Nagar Haveli & Daman & Diu', 'Dadra and Nagar Haveli and Daman and Diu',regex=True)
    filtered_data = df[(df['year'] == year) & (df['quarter'] == quarter)]
    return filtered_data

def display_data(type, year, quarter):
    agg_trans_df = pd.DataFrame()
    top_trans_df = pd.DataFrame()
    agg_user_df = pd.DataFrame()
    top_user_df = pd.DataFrame()
    try:
        with dbconnection().connect() as conn:
            if type == "***Transactions***":
                agg_trans_df = pd.read_sql("SELECT * from agg_trans_country", conn)
                top_trans_df = pd.read_sql("SELECT * from top_trans_country", conn)
                filtered_agg_trans_df = agg_trans_df[(agg_trans_df['year'] == year) & (agg_trans_df['quarter'] == quarter)]
                filtered_top_trans_df = top_trans_df[(top_trans_df['year'] == year) & (top_trans_df['quarter'] == quarter)]
                #df = pd.read_sql("SELECT * from map_trans_state", conn)
                return filtered_agg_trans_df, filtered_top_trans_df
            elif type == "***User***":
                agg_user_df = pd.read_sql("SELECT * from map_user_country", conn)
                top_user_df = pd.read_sql("SELECT * from top_user_country", conn)
                filtered_agg_user_df = agg_user_df[(agg_user_df['year'] == year) & (agg_user_df['quarter'] == quarter)]
                filtered_top_user_df = top_user_df[(top_user_df['year'] == year) & (top_user_df['quarter'] == quarter)]
                return filtered_agg_user_df, filtered_top_user_df
        
    except Error as e:
        print("Error during reading sidebar data from MySQL: ",e)
    finally:
        conn.close()
    
def front_end():
    st.title('Phonepe Pulse Data Visualization')
    st.divider()
    topcol1, topcol2, topcol3 = st.columns([1,1,1], gap="large")
    type = topcol1.radio("Data Classification: ",["***Transactions***", "***User***"], 
                    horizontal=True)
    year = topcol2.slider("Year:", min_value=2018, max_value=2023, step=1)
    quarter = topcol3.slider("Quarter:", min_value=1, max_value=4, step=1)
    
    #st.write(type, year, quarter)
    plot_df = plot_data(type, year, quarter)
    col1, col2 = st.columns([3, 1])
    
    #with urlopen('https://raw.githubusercontent.com/plotly/datasets/master/geojson-counties-fips.json') as response:
    #    countries = json.load(response)
    #indian_states = json.load(open("gadm41_IND_2.json", 'r'))
    india_states = json.load(open("india_states.geojson",'r'))
    #rslt_df = pd.read_csv("state_oy_oq.csv")
    color_field, hover_field = None, None
    if type == "***Transactions***":
        color_field = 'transaction_count'
        hover_field = 'transaction_amount'
    else:
        color_field = 'registered_users'
        hover_field = 'app_opens'
    fig2 = px.choropleth(plot_df, geojson=india_states, featureidkey='properties.ST_NM', 
                           center={"lat": 20.5937, "lon": 78.9629},
                           locations='state', color=color_field, hover_name=hover_field, scope="asia")
    fig2.update_geos(fitbounds="locations", visible=False)
    fig2.update_layout(width=1300, height = 1000, margin={"r":0,"t":0,"l":0,"b":0})
    col1.write(fig2)
    # Side column Data
    agg_df, top_df = display_data(type, year, quarter)
    if type == "***Transactions***":
        col2.header("Transactions")
        col2.subheader("All PhonePe Transactions :", divider='rainbow')
        total = sum(agg_df['transaction_count'])
        col2.subheader(total)
        col2.subheader("Total Payment Value:", divider='rainbow')
        col2.subheader(sum(agg_df['transaction_amount']))
        col2.subheader("Categories :", divider='rainbow')
        adf = agg_df[['transaction_type', 'transaction_count']].reset_index(drop=True)
        adf.index = adf.index + 1
        col2.write(adf)
        col2.subheader("Top 10 :", divider="rainbow")
        tab1, tab2, tab3 = col2.tabs(["States", "Districts", "Pincodes"])
        with tab1:
            top_states_df = top_df[top_df['cat_type'] == 'states']
            df = top_states_df[['type_name', 'transaction_count']].reset_index(drop=True)
            df.index = df.index + 1
            st.write(df)
        with tab2:
            top_districts_df = top_df[top_df['cat_type'] == 'districts']
            df = top_districts_df[['type_name', 'transaction_count']].reset_index(drop=True)
            df.index = df.index + 1
            st.write(df)
        with tab3:
            top_pincodes_df = top_df[top_df['cat_type'] == 'pincodes']
            df = top_pincodes_df[['type_name', 'transaction_count']].reset_index(drop=True)
            df.index = df.index + 1
            st.write(df)

    else:
        col2.header("Users")
        col2.subheader("Regstrd. PhonePe Users till Q{} {}".format(quarter, year), divider='rainbow')
        col2.subheader(sum(agg_df['registered_users']))
        col2.subheader("PhonePe app opens in Q{} {}".format(quarter, year), divider='rainbow')
        col2.subheader(sum(agg_df['app_opens']))
        col2.subheader("Top 10 :", divider="rainbow")
        tab1, tab2, tab3 = col2.tabs(["States", "Districts", "Pincodes"])
        with tab1:
            top_states_df = top_df[top_df['cat_type'] == 'states']
            df = top_states_df[['type_name', 'registered_users']].reset_index(drop=True)
            df.index = df.index + 1
            st.write(df)
        with tab2:
            top_districts_df = top_df[top_df['cat_type'] == 'districts']
            df = top_districts_df[['type_name', 'registered_users']].reset_index(drop=True)
            df.index = df.index + 1
            st.write(df)
        with tab3:
            top_pincodes_df = top_df[top_df['cat_type'] == 'pincodes']
            df = top_pincodes_df[['type_name', 'registered_users']].reset_index(drop=True)
            df.index = df.index + 1
            st.write(df)

    
    # fig = px.choropleth_mapbox(rslt_df, geojson=india_states, featureidkey='properties.ST_NM',
    #                        locations='state', color='transaction_count', hover_name='transaction_amount', mapbox_style="carto-positron",
    #                       zoom=4, center={"lat": 20.5937, "lon": 78.9629})
    # fig.update_geos(fitbounds="locations", visible=False)
    # fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
    # st.write(fig)


front_end()