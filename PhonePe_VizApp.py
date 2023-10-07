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
from PhonePe_Data import execute_github_data_extraction

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
    params.append("phonepe")
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
                map_df = pd.read_sql("SELECT * from map_trans_country", conn)
                filtered_agg_trans_df = agg_trans_df[(agg_trans_df['year'] == year) & (agg_trans_df['quarter'] == quarter)]
                filtered_top_trans_df = top_trans_df[(top_trans_df['year'] == year) & (top_trans_df['quarter'] == quarter)]
                return filtered_agg_trans_df, filtered_top_trans_df, map_df
            elif type == "***User***":
                agg_user_df = pd.read_sql("SELECT * from agg_user_country", conn)
                top_user_df = pd.read_sql("SELECT * from top_user_country", conn)
                map_user_df = pd.read_sql("SELECT * from map_user_country", conn)                
                filtered_map_user_df = map_user_df[(map_user_df['year'] == year) & (map_user_df['quarter'] == quarter)]
                filtered_top_user_df = top_user_df[(top_user_df['year'] == year) & (top_user_df['quarter'] == quarter)]
                return agg_user_df, filtered_top_user_df, filtered_map_user_df
        
    except Error as e:
        print("Error during reading data from MySQL: ",e)
    finally:
        conn.close()

# Function to process the SQL query based on user's choice and returns dataframe
@st.cache_data
def query_processor(choice, state=None, district=None):
    query_dic = {1 : "select year, transaction_type, sum(transaction_amount) AS Amount, sum(transaction_count) AS Count from agg_trans_country group by transaction_type, year",
                 2 : "select state, year, transaction_type, sum(transaction_amount) AS Amount, sum(transaction_count) AS Count from agg_trans_state group by transaction_type, year, state",
                 3 : "SELECT year, brand_name, SUM(user_count) AS User_Count, SUM(percentage) AS Percentage FROM agg_user_country GROUP BY year, brand_name",
                 4 : "SELECT state, year, brand_name, SUM(user_count) AS User_Count, SUM(percentage) AS Percentage FROM agg_user_state GROUP BY state, year, brand_name",
                 5 : "SELECT year, cat_type, type_name, SUM(transaction_count) AS Count, SUM(transaction_amount) AS Amount from top_trans_country GROUP BY year, cat_type, type_name",
                 6 : "",
                 7 : "",
                 8 : "",
                 9 : "",
                 10 : "",
                 11 : "SELECT DISTINCT(state) FROM map_trans_state",
                 12 : f"SELECT DISTINCT(distrct) FROM map_trans_state WHERE state='{state}'",
                 13 : f"SELECT * FROM map_trans_state WHERE state='{state}' AND distrct='{district}'"
                 }
    try:
        with dbconnection().connect() as conn:
            df = pd.read_sql(query_dic[choice], conn)
            return df
    except Error as e:
        print("Error during reading Insight Data: ",e)
    finally:
        conn.close()

# Function to process the dataframe and returns chart object
@st.cache_data
def data_processor(choice):
    df = query_processor(choice)
    fig = ""
    if choice == 1:
        fig = px.bar(df, y="transaction_type", x="Amount",
                             color="Count", 
                             orientation='h',
                             hover_data= ["transaction_type","year"],
                             height=900,
                             facet_row='year',
                             barmode="stack",
                             title="Country-wise Aggregated Transactions",
                             text_auto=".3s",
                             labels = dict(year = "Year", transaction_type="Transaction Type",
                                           Amount = "Amount (Rs.)"))
        fig.update_yaxes(tickangle=10, title_standoff=25,
                         title_font=dict(size=15, family='Droid Sans', color='orange'),
                         showdividers=True,hoverformat=".2")
        fig.update_xaxes(showdividers=True, showgrid=True, showline=True)
        fig.update_layout(title_font=dict(size=18, family='Droid Sans'),
                          )
        fig.update_traces(textfont_size=13, textangle=0, textposition="outside", cliponaxis=False)
    
    elif choice == 2:
        fig=px.sunburst(df,path=['year','transaction_type','state'], 
                        values='Amount',
                        color='Count',
                        color_continuous_scale='rainbow',
                        height=800)

    elif choice == 3:
        fig = px.bar(df, y="User_Count", x="brand_name",
                             color="Percentage", 
                             hover_data= ["year","Percentage"],
                             height=600,
                             facet_col='year',
                             title="Country-wise Aggregated User Report",
                             text_auto=".3s",
                             labels = dict(year = "Year", brand_name="Brand", User_Count="User"))
        fig.update_traces(textfont_size=13, textangle=0, textposition="outside", cliponaxis=False)
            
    elif choice == 4:
        fig=px.sunburst(df,path=['year','brand_name','state'], 
                        values='User_Count',
                        color='Percentage',
                        color_continuous_scale='rainbow',
                        height=800)
    elif choice == 5:
        df = df[df['cat_type'] == 'states']
        # Sort the states_df dataframe by 'Count' in descending order within each year
        df = df.sort_values(by=['year'], ascending=[False])

        fig = px.bar(
            df,
            y='type_name',
            x='Count',
            orientation='h',
            color='Amount',
            facet_row='year',  # Create subplots for each year
            labels={'type_name': 'State', 'Count': 'Transaction Count'},
            title='Transaction Count by State (Subplots by Year)',
            height=1600
        )
        fig.update_layout(xaxis = {"categoryorder":"total descending"})
    elif choice == 6:
        pass
    elif choice == 7:
        pass
    elif choice == 8:
        pass
    elif choice == 9:
        pass
    elif choice == 10:
        pass
               
    return fig,df               
            
def front_end():    
    #st.divider()
    #topcol1, topcol2, topcol3, tempcol = st.columns([1,1,1,1], gap="large")
    st.sidebar.subheader("PhonePe Pulse Data Visualization", divider="rainbow")
    data=st.sidebar.radio(label="Select the Classification:", options=["***Country***", "***State***"], horizontal=True)
    st.sidebar.divider()
    type = st.sidebar.radio(f"{data} wise Data Classification: ",["***Transactions***", "***User***"], 
                    horizontal=True)
    # extract the from and to year from db and update the next line code
    year = st.sidebar.slider("Year:", min_value=2018, max_value=2023, step=1)
    quarter = st.sidebar.slider("Quarter:", min_value=1, max_value=4, step=1)
    st.sidebar.divider()
    st.sidebar.subheader("Insights", divider="rainbow")
    
    insight = st.sidebar.toggle("Facts & Figures")
    plot_df = plot_data(type, year, quarter)
       
    if insight:
        st.subheader("Few Insights from the PhonePe Pulse Data...", divider="rainbow")
        q1 = "Analysis on Transaction Categories - Country Level"
        q2 = "Analysis on Transaction Categories - State Level"
        q3 = "Brand wise Analysis - Country Level"
        q4 = "Brand wise Analysis - State Level"
        q5 = "Top 10 States - Transactions"

        qchoice = st.selectbox("Select any of the Analysis from the List:", ['Select any', q1, q2, q3, q4, q5])
        insight_col1, insight_col2 = st.columns([2,1])
        
        if qchoice == q1:
            fig, df = data_processor(1)
            df.index = df.index + 1
            df.index.name = "#"
            df.columns = ['Year','Transaction Type','Total Amount', 'Total Count']
            insight_col1.plotly_chart(fig, use_container_width=True)
            expander = insight_col2.expander("Click here to see the Data...", expanded=False)
            expander.dataframe(df,height=600)
            insight_col2.subheader("Facts: ",divider="rainbow")            
            insight_col2.markdown("- PhonePe app is ***:orange[mostly used]*** for ***:blue[Peer-to-Peer]*** transactions.")
            insight_col2.markdown("- ***:blue[Merchant Payment]*** category is the ***:orange[second most used]*** transaction type.")
            insight_col2.write("- ***:blue[Financial Services]*** are the ***:orange[least used]***.")
            insight_col2.write("- Year over year growth of ***:blue[Peer-to-Peer]*** transactions volume is ***:orange[more than twice]***. :rainbow[(2023 - only 2 quarters data)]")
            
        elif qchoice == q2:
            fig, df = data_processor(2)
            df.index = df.index + 1
            df.index.name = "#"
            df.columns = ['State','Year','Transaction Type','Total Amount', 'Total Count']
            st.plotly_chart(fig, use_container_width=True)
            new_col1, new_col2 = st.columns(2)
            expander = new_col1.expander("Click here to see the Data...", expanded=False)
            expander.dataframe(df,height=600)
            new_col2.subheader("Facts: ",divider="rainbow")  
            new_col2.markdown("- ***:orange[Largest]*** number of transactions across all the state is ***:blue[Peer-to-Peer]*** transaction.")
            new_col2.markdown("- From 2020 ***:orange[Telangana]*** holds the ***:blue[first]*** place in Peer-to-Peer payments")
            
        elif qchoice == q3:
            fig, df = data_processor(3)
            df.index = df.index + 1
            df.index.name = "#"
            df.columns = ['Year','Brand','User Count', 'Percentage']
            st.plotly_chart(fig, use_container_width=True)
            new_col1, new_col2 = st.columns(2)
            expander = new_col1.expander("Click here to see the Data...", expanded=False)
            expander.dataframe(df)
            new_col2.subheader("Facts: ",divider="rainbow")              
            new_col2.markdown("- ***:blue[Large]*** number of ***:orange[Xiaomi]*** mobile brand users are using PhonePe app.")
            new_col2.markdown("- ***:orange[Tecno]*** brand users are the ***:blue[least used]*** users of PhonePe app.")
            
        elif qchoice == q4:
            fig, df = data_processor(4)
            df.index = df.index + 1
            df.index.name = "#"
            df.columns = ['State','Year','Brand','User Count', 'Percentage']
            st.plotly_chart(fig, use_container_width=True)
            new_col1, new_col2 = st.columns(2)
            expander = new_col1.expander("Click here to see the Data...", expanded=False)
            expander.dataframe(df)
            new_col2.subheader("Facts: ",divider="rainbow")  
            new_col2.markdown("- ***:blue[Maharashtra]*** has the ***:orange[largest]*** PhonePe user base in the country.")
            new_col2.markdown("- ***:orange[Xiaomi]*** brand users in ***:blue[Maharashtra]*** are the ***:orange[top users]*** of PhonePe app followed by ***:orange[Samsung]*** brand users.")
            
        elif qchoice == q5:
            fig, df = data_processor(5)
            df.index = df.index
            df.index.name = "#"
            df.columns = ['Year','Type','State','Count', 'Amount']
            st.plotly_chart(fig, use_container_width=True)
            new_col1, new_col2 = st.columns(2)
            expander = new_col1.expander("Click here to see the Data...", expanded=False)
            expander.dataframe(df)
            new_col2.subheader("Facts: ",divider="rainbow")  
            new_col2.markdown("- ***:blue[Maharashtra]*** is the state with the ***:orange[largest]*** Transaction Count in the country.")

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
                agg_df, top_df, map_df = display_data(type, year, quarter)
                col1.write()

                if type == "***Transactions***":
                    # TRANSACTIONS BASED REPORT
                    col2.write("<h5 align=center>Transactions</h5>",unsafe_allow_html=True)
                    subcol1, subcol2 = col2.columns([1, 1])
                    total = sum(agg_df['transaction_count'])

                    # First Indicator
                    ifig = go.Figure(go.Indicator(
                        mode = "gauge+number",
                        value = total,
                        domain = {'x': [0, 0.3], 'y': [0.7, 1]},
                        title = {'text': "Transactions"}))
                    subcol1.write(ifig)

                    # Second Indicator
                    tot = sum(agg_df['transaction_amount'])
                    ifig = go.Figure(go.Indicator(
                        mode = "gauge+number",
                        value = tot,
                        domain = {'x': [0, 0.3], 'y': [0.7, 1]},
                        title = {'text': "Payment Value"}))
                    subcol2.write(ifig)

                    # Transaction Type Report
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

                    # Displaying Top 10 Values
                    col2.write(":keycap_ten: **Top 10 :**")
                    tab1, tab2, tab3 = col2.tabs(["States", "Districts", "Pincodes"])
                    with tab1:
                        # Top 10 State Data
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
                        # Top 10 Districts Data
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
                        # Top 10 Pincodes (Area) Data
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
                    # USER BASED REPORT
                    col2.write("<h5 align=center>Users</h5>", unsafe_allow_html=True)
                    total = sum(map_df['registered_users'])
                    subcol1, subcol2 = col2.columns([1, 1])

                    # First Indicator
                    ifig = go.Figure(go.Indicator(
                        mode = "gauge+number",
                        value = total,
                        domain = {'x': [0, 0.3], 'y': [0.7, 1]},
                        title = {'text': f"PhonePe Users Q{quarter} {year}"}))
                    subcol1.write(ifig)

                    # Second Indicator
                    tot = sum(map_df['app_opens'])
                    ifig = go.Figure(go.Indicator(
                        mode = "gauge+number",
                        value = tot,
                        domain = {'x': [0, 0.3], 'y': [0.7, 1]},
                        title = {'text': f"App Opens in Q{quarter} {year}"}))
                    subcol2.write(ifig)

                    # Top 10 Values
                    col2.write(":keycap_ten: **Top 10 :**")
                    tab1, tab2, tab3 = col2.tabs(["States", "Districts", "Pincodes"])
                    with tab1:
                        # Displaying Top 10 state data
                        top_states_df = top_df[top_df['cat_type'] == 'states']
                        df = top_states_df[['type_name', 'registered_users']].reset_index(drop=True)
                        df.index = df.index + 1
                        df.columns = ['States', 'Count']
                        df.index.name = "#"
                        df['States'] = df['States'].str.title()
                        st.dataframe(df)
                    with tab2:
                        # Displaying Top 10 districts data
                        top_districts_df = top_df[top_df['cat_type'] == 'districts']
                        df = top_districts_df[['type_name', 'registered_users']].reset_index(drop=True)
                        df.index = df.index + 1
                        df.columns = ['Districts', 'Count']
                        df.index.name = "#"
                        df['Districts'] = df['Districts'].str.title()
                        st.dataframe(df)
                    with tab3:
                        # Displaying Top 10 Pincodes (Area) data
                        top_pincodes_df = top_df[top_df['cat_type'] == 'pincodes']
                        df = top_pincodes_df[['type_name', 'registered_users']].reset_index(drop=True)
                        df.index = df.index + 1
                        df.columns = ['Pincodes', 'Count']
                        df.index.name = "#"
                        st.dataframe(df)
        else:
            # STATE WISE VISUALIZATION
            state_col1, state_col2 = st.columns(2)

            # Retrieving state list from Database
            state_choice = state_col1.selectbox("State:", 
                                                (query_processor(11).state).str.title(), 
                                                label_visibility='hidden') 
            # Retrieving District List from Database
            district_choice = state_col2.selectbox("District:", 
                                                   sorted((query_processor(12, state_choice).distrct).str.title()), 
                                                   label_visibility='hidden') 
            # Creating dataframe for the selected state and district
            df = query_processor(13, state_choice, district_choice) 
            
            # Plotting Chart based on the dataframe
            fig = px.bar(df, y="year", x="transaction_amount",
                             color="transaction_count", 
                             hover_data= ["quarter","transaction_count"],
                             height=800,
                             barmode='stack',
                             orientation='h',
                             facet_row='quarter',
                             title="Country-wise Aggregated Transactions",
                             text_auto=".3s",
                             labels = dict(year = "Year", transaction_amount="Transaction Amount"))
            fig.update_traces(textfont_size=13, textangle=0, textposition="outside", cliponaxis=False)            
            st.write(fig)

        



# Main Function 
def main():    
    if 'github_status' not in st.session_state:
        st.session_state.github_status = execute_github_data_extraction()
    
    front_end()
    
if __name__ == "__main__":
    main() 
