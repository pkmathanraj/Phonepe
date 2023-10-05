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
from millify import millify
from millify import prettify

st.set_page_config(
    page_title="Dynamic Phonepe Dashboard",
    page_icon="✅",
    layout="wide"
    )

@st.cache_data
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
        raise Exception('Section {0} not found in the {1} database config file'.format(section, filename))
  
    return conn_param

# Establishing MySQL Connection
@st.cache_resource
def dbconnection():
    params = config()
    try:
        return create_engine(url="mysql+mysqlconnector://{0}:{1}@{2}:{3}/{4}".format(*params))
    except Error as e:
        print("Error during establishing MySQL connection: ",e)

@st.cache_data
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

@st.cache_data
def plot_data(type, year, quarter):
    df = data_extraction(type)
    # Data Preprocessing
    df['state'] = df['state'].str.title()
    df['state'] = df['state'].replace('Andaman & Nicobar Islands', 'Andaman & Nicobar',regex=True)
    df['state'] = df['state'].replace('Dadra & Nagar Haveli & Daman & Diu', 'Dadra and Nagar Haveli and Daman and Diu',regex=True)
    filtered_data = df[(df['year'] == year) & (df['quarter'] == quarter)]
    return filtered_data

@st.cache_data
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
        print("Error during reading data from MySQL: ",e)
    finally:
        conn.close()

@st.cache_data
def data_processor(choice):
    if choice==1:
        try:
            with dbconnection().connect() as conn:
                agg_trans_df = pd.read_sql("SELECT * from agg_trans_country", conn)
                top_trans_df = pd.read_sql("SELECT * from top_trans_country", conn)
                filtered_agg_trans_df = agg_trans_df[(agg_trans_df['year'] == year) & (agg_trans_df['quarter'] == quarter)]
                filtered_top_trans_df = top_trans_df[(top_trans_df['year'] == year) & (top_trans_df['quarter'] == quarter)]
                #df = pd.read_sql("SELECT * from map_trans_state", conn)
                
                st.write()
                
                agg_user_df = pd.read_sql("SELECT * from map_user_country", conn)
                top_user_df = pd.read_sql("SELECT * from top_user_country", conn)
                filtered_agg_user_df = agg_user_df[(agg_user_df['year'] == year) & (agg_user_df['quarter'] == quarter)]
                filtered_top_user_df = top_user_df[(top_user_df['year'] == year) & (top_user_df['quarter'] == quarter)]
                
    
        except Error as e:
            print("Error during reading Insight Data: ",e)
        finally:
            conn.close()


def front_end():    
    #st.divider()
    #topcol1, topcol2, topcol3, tempcol = st.columns([1,1,1,1], gap="large")
    st.sidebar.subheader("PhonePe Pulse Data Visualization", divider="rainbow")
    data=st.sidebar.radio(label="Select the Classification:", options=["***Country***", "***State***"], horizontal=True)
    st.sidebar.divider()
    type = st.sidebar.radio(f"{data} wise Data Classification: ",["***Transactions***", "***User***"], 
                    horizontal=True)
    year = st.sidebar.slider("Year:", min_value=2018, max_value=2023, step=1)
    quarter = st.sidebar.slider("Quarter:", min_value=1, max_value=4, step=1)
    st.sidebar.divider()
    st.sidebar.subheader("Insights", divider="rainbow")
    insight_button = st.sidebar.button("Facts & Figures", use_container_width=True)
    plot_df = plot_data(type, year, quarter)
    
   
    if insight_button:
        st.subheader("Few Insights from the PhonePe Pulse Data...", divider="rainbow")
        q1 = "Analysis on Transaction Categories - Country Level"
        q2 = "Brand wise Analysis - Country Level"
        q3 = "Analysis on Transaction Categories - State Level"
        q4 = "Analysis on Mobile Brand - State Level"
        q5 = "--"
        q6 = "--"
        q7 = "Top 10 Transactions - Country Level"
        q8 = "Top 10 Transactions - State Level"
        q9 = "Top 10 User Analysis Report - Country Level"
        q10 = "Top 10 User Analysis Report - State Level"

        qchoice = st.selectbox("Select any of the Analysis from the List:", ['Select any', q1, q2, q3, q4, q5, q6, q7, q8, q9, q10])
        if qchoice == q1:
            st.dataframe(data_processor(1), width=None)
        elif qchoice == q2:
            st.dataframe(data_processor(2), width=None)
        elif qchoice == q3:
            st.dataframe(data_processor(3), width=None)
        elif qchoice == q4:
            st.dataframe(data_processor(4), width=None)
        elif qchoice == q5:
            st.dataframe(data_processor(5), width=None)
        elif qchoice == q6:
            st.dataframe(data_processor(6), width=None)
        elif qchoice == q7:
            st.dataframe(data_processor(7), width=None)
        elif qchoice == q8:
            st.dataframe(data_processor(8), width=None)
        elif qchoice == q9:
            st.dataframe(data_processor(9), width=None)
        elif qchoice == q10:
            st.dataframe(data_processor(10), width=None)

    else:
        st.header('PhonePe Pulse Data Visualization', divider="rainbow")
        col1, col2 = st.columns([2, 1])
        
        india_states = json.load(open("india_states.geojson",'r'))
        color_field, hover_field = None, None
        if type == "***Transactions***":
            color_field = 'transaction_count'
            hover_field = 'transaction_amount'
        else:
            color_field = 'registered_users'
            hover_field = 'app_opens'
        
        if data == "***Country***":
            fig = px.choropleth(plot_df, geojson=india_states, featureidkey='properties.ST_NM', 
                                title=f"Aggregated {type.replace('*','')} Q{quarter} {year}:",
                                color_continuous_scale="Turbo",
                                center={"lat": 20.5937, "lon": 78.9629},
                                locations='state', color=color_field, hover_name='state', 
                                hover_data={'state':False, hover_field:':.2f'}, scope="asia")
            fig.update_geos(fitbounds="locations", visible=False, 
                            bgcolor="#121216")
            fig.update_layout(width=800, height = 450, margin={"r":0,"t":30,"l":0,"b":0})

            if plot_df.empty:
                col1.write(f"<br><br><h4 align=center>Data is not available for the selected Q{quarter} in the year {year}</h4>", unsafe_allow_html=True)
            else:
                col1.write(fig)
                agg_df, top_df = display_data(type, year, quarter)
                if type == "***Transactions***":
                    col2.write("<h5 align=center>Transactions</h5>",unsafe_allow_html=True)
                    subcol1, subcol2 = col2.columns([1, 1])
                    total = sum(agg_df['transaction_count'])
                    subcol1.metric(label=":ledger: ***:violet[All PhonePe Transactions]***", value=millify(total))
                    tot = sum(agg_df['transaction_amount'])
                    subcol2.metric(label=":moneybag: ***:violet[Total Payment Value]***", value=millify(tot))
                    col2.write(":memo: **Categories :**")
                    adf = agg_df[['transaction_type', 'transaction_count']].reset_index(drop=True)
                    adf.index = adf.index + 1
                    adf.index.name = "#"
                    col2.dataframe(adf, column_config={
                        'transaction_type': st.column_config.TextColumn("Transaction Type", 
                                                                        help = "Click to Sort"),
                        'transaction_count': st.column_config.NumberColumn("Count",
                                                                        help = "Click to Sort",
                                                                        width = "medium")
                    })
                    col2.write(":keycap_ten: **Top 10 :**")
                    tab1, tab2, tab3 = col2.tabs(["States", "Districts", "Pincodes"])
                    with tab1:
                        top_states_df = top_df[top_df['cat_type'] == 'states']
                        df = top_states_df[['type_name', 'transaction_count']].reset_index(drop=True)
                        df.index = df.index + 1
                        df.index.name = "#"
                        df.columns = ['States', 'Count']
                        df['States'] = df['States'].str.title()
                        st.dataframe(df, column_config={
                            'Count' : st.column_config.NumberColumn(help = "Click to Sort",
                                                                    width = "medium")
                        })
                    with tab2:
                        top_districts_df = top_df[top_df['cat_type'] == 'districts']
                        df = top_districts_df[['type_name', 'transaction_count']].reset_index(drop=True)
                        df.index = df.index + 1
                        df.index.name = "#"
                        df.columns = ['Districts', 'Count']
                        df['Districts'] = df['Districts'].str.title()
                        st.dataframe(df, column_config={
                            'Count' : st.column_config.NumberColumn(help = "Click to Sort",
                                                                    width = "medium")
                        })
                    with tab3:
                        top_pincodes_df = top_df[top_df['cat_type'] == 'pincodes']
                        df = top_pincodes_df[['type_name', 'transaction_count']].reset_index(drop=True)
                        df.index = df.index + 1
                        df.index.name = "#"
                        df.columns = ['Pincodes', 'Count']
                        st.dataframe(df, column_config={
                            'Count' : st.column_config.NumberColumn(help = "Click to Sort",
                                                                    width = "medium")
                        })

                else:
                    col2.write("<h5 align=center>Users</h5>", unsafe_allow_html=True)
                    total = sum(agg_df['registered_users'])
                    subcol1, subcol2 = col2.columns([1, 1])
                    subcol1.metric(label=f":ledger: ***:violet[Regd. PhonePe Users Q{quarter} {year}]***", value=millify(total))
                    tot = sum(agg_df['app_opens'])
                    subcol2.metric(label=f":ledger: ***:violet[PhonePe App Opens in Q{quarter} {year}]***", value=millify(tot))
                    col2.write(":keycap_ten: **Top 10 :**")
                    tab1, tab2, tab3 = col2.tabs(["States", "Districts", "Pincodes"])
                    with tab1:
                        top_states_df = top_df[top_df['cat_type'] == 'states']
                        df = top_states_df[['type_name', 'registered_users']].reset_index(drop=True)
                        df.index = df.index + 1
                        df.columns = ['States', 'Count']
                        df.index.name = "#"
                        df['States'] = df['States'].str.title()
                        st.dataframe(df)
                    with tab2:
                        top_districts_df = top_df[top_df['cat_type'] == 'districts']
                        df = top_districts_df[['type_name', 'registered_users']].reset_index(drop=True)
                        df.index = df.index + 1
                        df.columns = ['Districts', 'Count']
                        df.index.name = "#"
                        df['Districts'] = df['Districts'].str.title()
                        st.dataframe(df)
                    with tab3:
                        top_pincodes_df = top_df[top_df['cat_type'] == 'pincodes']
                        df = top_pincodes_df[['type_name', 'registered_users']].reset_index(drop=True)
                        df.index = df.index + 1
                        df.columns = ['Pincodes', 'Count']
                        df.index.name = "#"
                        st.dataframe(df)
        else:
            pass

    
    # fig = px.choropleth_mapbox(rslt_df, geojson=india_states, featureidkey='properties.ST_NM',
    #                        locations='state', color='transaction_count', hover_name='transaction_amount', mapbox_style="carto-positron",
    #                       zoom=4, center={"lat": 20.5937, "lon": 78.9629})
    # fig.update_geos(fitbounds="locations", visible=False)
    # fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
    # st.write(fig)


front_end()