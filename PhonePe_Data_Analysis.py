#///////////////////////////////////////////Required Packages////////////////////////////////////////////

#Required Packages
import streamlit as st
from annotated_text import annotated_text
import time
import geopandas as gpd
import plotly.express as px
import json
import locale
import numpy as np
import os
import subprocess
import pandas as pd
from sqlalchemy import create_engine,text
import plotly.graph_objects as go



#////////////////////////////////////////Custom CSS and Radio Button and Logo/////////////////////////////

custom_css_remove_spaces_in_main_container = """
<style>
[data-testid="stAppViewContainer"] > .main {
    padding: 0 !important;
    margin: 0 !important;
}
[data-testid="stHeader"] {
    padding: 0 !important;
    margin: 0 !important;
}
[data-testid="stSidebar"] {
    padding: 0 !important;
    margin: 0 !important;
}
.block-container {
    padding: 0 !important;
    margin: 0 !important;
}
header, .css-1d391kg {  /* Streamlit's top bar container class */
    padding: 0 !important;
    margin: 0 !important;
    height: 0 !important;
}
</style>
"""
st.markdown(custom_css_remove_spaces_in_main_container, unsafe_allow_html=True)

# Sidebar Logo & Radio Buttons
st.logo('Replace with logo file location') #////////////////////Replace this with location of logo/////////////////
st.sidebar.title("Navigation")
selected_section = st.sidebar.radio("**Go to**",("**Home**", "**Data Upload**", "**Data Analytics**"))
st.write('<style>div.st-bf {margin-left: 20px; margin-right: 10px;}</style>', unsafe_allow_html=True)



#/////////////////Session Control for sql connection and navigation through entire app///////////////////

# Initiating session state & update
if 'selected_section' not in st.session_state:
  st.session_state['selected_section'] = "Home"
# Updating Session
st.session_state['selected_section'] = selected_section 

# Tracking Sql connection state from Data analytics Section
if 'new_sql_connection' not in st.session_state:
  st.session_state['new_sql_connection'] = None
  st.session_state['Data_analytics_section_engine']=None
  st.session_state['Data_analytics_section_connection']=None



#///////////////////////////////////////Data Upload Section/////////////////////////////////////////////

# Data upload
if st.session_state['selected_section'] == "**Data Upload**":
  
  # Setting background image on main container
  background_image_css = f"""
    <style>
    [data-testid="stAppViewContainer"] > .main {{
    background-image: url("https://i.postimg.cc/WbCDDrXt/gradient.jpg");
    background-size: cover;
    background-position: center center;
    background-repeat: no-repeat;
    background-attachment: local;
    }}
    [data-testid="stHeader"] {{
    background: rgba(0,0,0,0);
    }}
    </style>
    """
  st.markdown(background_image_css, unsafe_allow_html=True)  

  # Setting sidebar backround image 
  sidebar_background_image = """
    <style>
    [data-testid=stSidebar] {
    background-image: url('https://i.postimg.cc/X7nDyG1K/1312663.jpg');
    background-size: cover;
    background-position: center;
    background-repeat: no-repeat;
    }
    </style>
    """
  st.markdown(sidebar_background_image, unsafe_allow_html=True)
  
  # Dataupload Section - Session reset
  def session_reset():
    st.session_state['sql_connection_workflow']=False
    st.session_state['sql_data_upload_workflow']=False
    st.session_state['github_workflow']=False
  
  # Handling github workflow
  if 'github_workflow' not in st.session_state:
    session_reset()
    
  if not st.session_state['github_workflow']:
    def github_process():
      # creating empty element to get user data
      sub_header_data_upload_section=st.empty()
      github_input_placeholder = st.empty()
      local_path_input_placeholder = st.empty()
      enter_description=st.empty()
      
      # User input 
      sub_header_data_upload_section.subheader("**Please provide the following inputs to fetch the data:**") 
      repo_url = github_input_placeholder.text_input(('**GitHub URL to fetch data**'))
      local_path = local_path_input_placeholder.text_input("**Local path to download data**")
      enter_description.write(f'Press Enter..')

      def git_repo_clone(repo_url, local_path):

        # Progress bar update
        progress_bar.progress(0)
        new_folder_path=os.path.join(local_path, "git_clone")

        if new_folder_path:
          if '\\' in local_path:
            new_folder_path=new_folder_path.replace("\\","/")
        return_path=new_folder_path

        # Progress bar update
        for i in range(25):
          time.sleep(0.01)
          progress_bar.progress(1+i)

        try:

          # Progress bar update
          for i in range(25):
            time.sleep(0.01)
            progress_bar.progress(26+i)
          
          # check for folder existance and git clone 
          if not os.path.exists(new_folder_path):
            subprocess.run(["git", "clone", repo_url, new_folder_path], check=True)

            # Progress bar update
            for i in range(25):
              time.sleep(0.01)
              progress_bar.progress(51+i)
            

            git_clone_placeholder=st.empty()
            git_clone_placeholder.text("***Git clone successful..!***")
            time.sleep(1.7)
            git_clone_placeholder.empty()
            return return_path

          else:
            if os.path.exists(os.path.join(new_folder_path, ".git")):
              subprocess.run(["git", "-C", new_folder_path, "pull"], check=True)

              # Progress bar update
              for i in range(25):
                time.sleep(0.01)
                progress_bar.progress(51+i)

              git_clone_placeholder=st.empty()
              git_clone_placeholder.write("***Path already exists. Repository updated successfully!***")
              time.sleep(1.7)
              git_clone_placeholder.empty()
              return return_path

            else:
              st.error(f'An error occurred : {e}')
        except subprocess.CalledProcessError as e:
          st.error(f"An error occurred during git operation: {e}")
        except Exception as e:
          st.error(f"An error occurred: {e}")

      # Once inputs are received, hiding the input fields
      if repo_url and local_path:
        enter_description.empty()
        progress_bar=st.empty()
        returned_path=git_repo_clone(repo_url, local_path)

        # Progress bar update
        for i in range(25):
          time.sleep(0.01)
          progress_bar.progress(75+i)
          
        success_message=st.empty()
        progress_bar.empty()
        success_message.success("**Data fetched successfully..!**")
        time.sleep(1)
        success_message.empty()
        github_input_placeholder.empty()
        local_path_input_placeholder.empty()
        sub_header_data_upload_section.empty()

        # Updating session state 
        st.session_state['github_workflow']=True
        st.session_state['sql_connection_workflow']=False
        return returned_path
    github_return_folder_path = github_process()

    # Saving folder path to session state
    st.session_state['new_folder_path']=github_return_folder_path
  
  # Handling sql connection workflow 
  if st.session_state['github_workflow'] and not st.session_state['sql_connection_workflow']:
    def sql_connection_process():
      # creating empty element to get user data
      sub_header_data_upload_section=st.empty()
      sql_username_placeholder=st.empty()
      sql_password_placeholder=st.empty()
      enter_description=st.empty()

      sql_hostname="127.0.0.1"
      sql_port=3306
      sub_header_data_upload_section.subheader("Please provide the following inputs to connect sql server:")
      sql_username = sql_username_placeholder.text_input("**SQL Username**")
      sql_password = sql_password_placeholder.text_input("**SQL Password**", type="password")
      enter_description.text(f"Press Enter..")

      # Once inputs are received, hiding the input fields
      if sql_username and sql_password:
        enter_description.empty()
        progress_bar=st.empty()

        #mySql Connection
        def mySql_connection(sql_username,sql_password,sql_hostname,sql_port):
          progress_bar.progress(0)

          # Progress bar update
          for i in range(50):
            time.sleep(0.01)
            progress_bar.progress(1+i)

          engine = create_engine(f"mysql+pymysql://{sql_username}:{sql_password}@{sql_hostname}:{sql_port}/",echo=False)
          connection = engine.connect()
          try:
            result = connection.execute(text("SELECT 1"))
            connection_status=True
          except Exception as e:
            st.error("Connection failed:", e)
          if connection_status:
            connection_properties={"connection":connection,"engine":engine,'user_and_connection_details':{"username":sql_username,"password":sql_password,"host_name":sql_hostname,"port":sql_port}}
            return connection_properties
          else:
            return (f"Connection failed:", e)
        my_connection=mySql_connection(sql_username,sql_password,sql_hostname,sql_port)
        for i in range(50):
          time.sleep(0.01)
          progress_bar.progress(i+51)
        success_message=st.empty()
        progress_bar.empty()
        success_message.success("***Connection establishment success..!***")
        time.sleep(1)
        success_message.empty()
        sql_password_placeholder.empty()
        sql_username_placeholder.empty()
        sub_header_data_upload_section.empty()

        # Updating session state 
        st.session_state['sql_connection_workflow']=True
        st.session_state['sql_data_upload_workflow']=False
        my_connection_properties=[my_connection]
        return my_connection_properties 
    sql_my_connection_properties=sql_connection_process()

    # Saving connection properties into session state
    st.session_state['my_connection_properties']=sql_my_connection_properties

  # Handling data cleaning and upload process
  if st.session_state['sql_connection_workflow'] and not st.session_state['sql_data_upload_workflow']:

    new_folder_path=st.session_state['new_folder_path']
    my_connection_properties=st.session_state['my_connection_properties']
    
    def sql_data_upload_process(new_folder_path,my_connection_properties):
      # creating empty element to get user data
      sub_header_data_upload_section=st.empty()
      variable_cotainer=st.empty()
      enter_description=st.empty()
      progress_bar=st.empty()
      
      sub_header_data_upload_section.subheader("Please provide the below input to create/use database:")
      database_name = variable_cotainer.text_input("**Database Name**")
      enter_description.text(f'Press Enter..')

      # Once inputs are received, hiding the input fields
      if database_name:
        def empty_inputs():
          enter_description.empty()
          sub_header_data_upload_section.empty()
          variable_cotainer.empty()
        def reset_inputs():
          sub_header_data_upload_section=st.empty()
          variable_cotainer=st.empty()
          enter_description=st.empty()
          progress_bar=st.empty()
        empty_inputs()
        reset_inputs()
        in_progress=st.empty()
        in_progress.text("In Progress..")
        progress_bar.progress(0)

        # Data extraction
        def extract_data(new_folder_path):
          #==============================================================================================================================
          ###Empty dictionaries to store the data###

          #======Aggregated Data=======#
          agg_trans_data={"State":[],"Year":[],"Quater":[],"Transaction_type":[],"Transaction_count":[],"Transaction_amount":[]}
          agg_ins_data={"State":[],"Year":[],"Quater":[],"Transaction_type":[],"Transaction_count":[],"Transaction_amount":[]}
          agg_user_data={"State":[],"Year":[],"Quater":[],"RegisteredUsers":[],"AppOpens":[]}
          agg_usersByDevice_data={"State":[],"Year":[],"Quater":[],"Brand":[],"Count":[],"Percentage":[]}

          #======Top Data=======#
          top_trans_dist_data={"State":[],"Year":[],"Quater":[],"District":[],"Transaction_count":[],"Transaction_amount":[]}
          top_trans_pincode_data={"State":[],"Year":[],"Quater":[],"Pincode":[],"Transaction_count":[],"Transaction_amount":[]}
          top_ins_dist_data={"State":[],"Year":[],"Quater":[],"District":[],"Transaction_count":[],"Transaction_amount":[]}
          top_ins_pincode_data={"State":[],"Year":[],"Quater":[],"Pincode":[],"Transaction_count":[],"Transaction_amount":[]}
          top_user_dist_data={"State":[],"Year":[],"Quater":[],"District":[],"RegisteredUsers":[]}
          top_user_pincode_data={"State":[],"Year":[],"Quater":[],"Pincode":[],"RegisteredUsers":[]}

          #======Map Data=======#
          map_trans_data={"State":[],"Year":[],"Quater":[],"District":[],"Transaction_count":[],"Transaction_amount":[]}
          map_ins_data={"State":[],"Year":[],"Quater":[],"District":[],"Transaction_count":[],"Transaction_amount":[]}
          map_user_data={"State":[],"Year":[],"Quater":[],"District":[],"RegisteredUsers":[],"AppOpens":[]}

          #==============================================================================================================================
          user_input_path=new_folder_path+'/data/'
          data_directories_list=["aggregated","top","map"]
          progress_bar.progress(10)
          for dir_item in data_directories_list:
            # path=user_input_path
            path=user_input_path+dir_item+'/'
            repitative_directories_list=["transaction","insurance","user"] # Repitative directories in data_directories_list
            for directory_item in repitative_directories_list:
              path_1=path+directory_item+'/'
              navg_path_1="/country/india/state/"
              navg_path_2="/hover/country/india/state/"
              if dir_item != "map":
                path_2=path_1+navg_path_1
              else:
                path_2=path_1+navg_path_2
              agg_states_list=os.listdir(path_2)
              for state in agg_states_list:
                states_path=path_2+state+"/"
                agg_year_list=os.listdir(states_path)
                for year in agg_year_list:
                  year_path=states_path+year+'/'
                  agg_quater_list=os.listdir(year_path)
                  for file in agg_quater_list:
                    jason_file=year_path+file
                    data=open(jason_file,'r')
                    jason_data=json.load(data)
                    try:
                      if dir_item==data_directories_list[0]:
                        try:
                          if directory_item==repitative_directories_list[0]:
                            for item in jason_data['data']['transactionData']:
                              name=item['name']
                              count=item['paymentInstruments'][0]['count']
                              amount=item['paymentInstruments'][0]['amount']
                              agg_trans_data["State"].append(state)
                              agg_trans_data["Year"].append(year)
                              agg_trans_data["Quater"].append(int(file.strip('.json')))
                              agg_trans_data['Transaction_type'].append(name)
                              agg_trans_data["Transaction_count"].append(count)
                              agg_trans_data["Transaction_amount"].append(amount)
                          elif directory_item==repitative_directories_list[1]:
                            for item in jason_data['data']['transactionData']:
                              name=item['name']
                              count=item['paymentInstruments'][0]['count']
                              amount=item['paymentInstruments'][0]['amount']
                              agg_ins_data["State"].append(state)
                              agg_ins_data["Year"].append(year)
                              agg_ins_data["Quater"].append(int(file.strip('.json')))
                              agg_ins_data['Transaction_type'].append(name)
                              agg_ins_data["Transaction_count"].append(count)
                              agg_ins_data["Transaction_amount"].append(amount)
                          elif directory_item==repitative_directories_list[2]:
                            if jason_data['data']['usersByDevice'] != None:
                              agg_user_data['State'].append(state)
                              agg_user_data['Year'].append(year)
                              agg_user_data['Quater'].append(int(file.strip('.json')))
                              reg_users=jason_data['data']['aggregated']['registeredUsers']
                              app_opens=jason_data['data']['aggregated']['appOpens']
                              agg_user_data['RegisteredUsers'].append(reg_users)
                              agg_user_data['AppOpens'].append(app_opens)
                              for item in jason_data['data']['usersByDevice']:
                                agg_usersByDevice_data['State'].append(state)
                                agg_usersByDevice_data['Year'].append(year)
                                agg_usersByDevice_data['Quater'].append(int(file.strip('.json')))
                                brand=item['brand']
                                count=item['count']
                                percentage=item['percentage']
                                agg_usersByDevice_data['Brand'].append(brand)
                                agg_usersByDevice_data['Count'].append(count)
                                agg_usersByDevice_data['Percentage'].append(percentage)
                            else:
                              agg_user_data['State'].append(state)
                              agg_user_data['Year'].append(year)
                              agg_user_data['Quater'].append(int(file.strip('.json')))
                              reg_users=jason_data['data']['aggregated']['registeredUsers']
                              app_opens=jason_data['data']['aggregated']['appOpens']
                              agg_user_data['RegisteredUsers'].append(reg_users)
                              agg_user_data['AppOpens'].append(app_opens)
                          else:
                            print(f'An Error Occured {e}')
                        except Exception as e:
                          print(f'An Error Occured {e}')
                      elif dir_item==data_directories_list[1]:
                        try:
                          if directory_item==repitative_directories_list[0]:
                            for item in jason_data['data']['districts']:
                              name=item['entityName']
                              count=item['metric']['count']
                              amount=item['metric']['amount']
                              top_trans_dist_data['State'].append(state)
                              top_trans_dist_data['Year'].append(year)
                              top_trans_dist_data['Quater'].append(int(file.strip('.json')))
                              top_trans_dist_data['District'].append(name)
                              top_trans_dist_data['Transaction_count'].append(count)
                              top_trans_dist_data['Transaction_amount'].append(amount)
                            for item in jason_data['data']['pincodes']:
                              name=item['entityName']
                              count=item['metric']['count']
                              amount=item['metric']['amount']
                              top_trans_pincode_data['State'].append(state)
                              top_trans_pincode_data['Year'].append(year)
                              top_trans_pincode_data['Quater'].append(int(file.strip('.json')))
                              top_trans_pincode_data['Pincode'].append(name)
                              top_trans_pincode_data['Transaction_count'].append(count)
                              top_trans_pincode_data['Transaction_amount'].append(amount)
                          elif directory_item==repitative_directories_list[1]:
                            for item in jason_data['data']['districts']:
                              name=item['entityName']
                              count=item['metric']['count']
                              amount=item['metric']['amount']
                              top_ins_dist_data['State'].append(state)
                              top_ins_dist_data['Year'].append(year)
                              top_ins_dist_data['Quater'].append(int(file.strip('.json')))
                              top_ins_dist_data['District'].append(name)
                              top_ins_dist_data['Transaction_count'].append(count)
                              top_ins_dist_data['Transaction_amount'].append(amount)
                            for item in jason_data['data']['pincodes']:
                              name=item['entityName']
                              count=item['metric']['count']
                              amount=item['metric']['amount']
                              top_ins_pincode_data['State'].append(state)
                              top_ins_pincode_data['Year'].append(year)
                              top_ins_pincode_data['Quater'].append(int(file.strip('.json')))
                              top_ins_pincode_data['Pincode'].append(name)
                              top_ins_pincode_data['Transaction_count'].append(count)
                              top_ins_pincode_data['Transaction_amount'].append(amount)
                          elif directory_item==repitative_directories_list[2]:
                            for item in jason_data['data']['districts']:
                              name=item['name']
                              reg_users=item['registeredUsers']
                              top_user_dist_data['State'].append(state)
                              top_user_dist_data['Year'].append(year)
                              top_user_dist_data['Quater'].append(int(file.strip('.json')))
                              top_user_dist_data['District'].append(name)
                              top_user_dist_data['RegisteredUsers'].append(reg_users)
                            for item in jason_data['data']['pincodes']:
                              name=item['name']
                              reg_users=item['registeredUsers']
                              top_user_pincode_data['State'].append(state)
                              top_user_pincode_data['Year'].append(year)
                              top_user_pincode_data['Quater'].append(int(file.strip('.json')))
                              top_user_pincode_data['Pincode'].append(name)
                              top_user_pincode_data['RegisteredUsers'].append(reg_users)
                          else:
                            print(f'An Error Occured: {e}')
                        except Exception as e:
                          print(f'An Error Occured: {e}')
                      elif dir_item==data_directories_list[2]:
                        try:
                          if directory_item==repitative_directories_list[0]:
                            for item in jason_data['data']['hoverDataList']:
                              name=item['name']
                              count=item['metric'][0]['count']
                              amount=item['metric'][0]['amount']
                              map_trans_data['State'].append(state)
                              map_trans_data['Year'].append(year)
                              map_trans_data['Quater'].append(int(file.strip('.json')))
                              map_trans_data['District'].append(name)
                              map_trans_data['Transaction_count'].append(count)
                              map_trans_data['Transaction_amount'].append(amount)
                          elif directory_item==repitative_directories_list[1]:
                            for item in jason_data['data']['hoverDataList']:
                              name=item['name']
                              count=item['metric'][0]['count']
                              amount=item['metric'][0]['amount']
                              map_ins_data['State'].append(state)
                              map_ins_data['Year'].append(year)
                              map_ins_data['Quater'].append(int(file.strip('.json')))
                              map_ins_data['District'].append(name)
                              map_ins_data['Transaction_count'].append(count)
                              map_ins_data['Transaction_amount'].append(amount)
                          elif directory_item==repitative_directories_list[2]:
                            for item,values in jason_data['data']['hoverData'].items():
                              reg_users=values['registeredUsers']
                              app_opens=values['appOpens']
                              map_user_data['State'].append(state)
                              map_user_data['Year'].append(year)
                              map_user_data['Quater'].append(int(file.strip('.json')))
                              map_user_data['District'].append(item)
                              map_user_data['RegisteredUsers'].append(reg_users)
                              map_user_data['AppOpens'].append(app_opens)
                          else:
                            print(f'An Error Occured: {e}')
                        except Exception as e:
                          print(f'An Error Occured: {e}')
                      else:
                        print(f'An Error Occured: {e}')
                    except Exception as e:
                      print(f'An Error Occured: {e}')
          progress_bar.progress(15)
          # Converting into data frame
          agg_trans_data_df=pd.DataFrame(agg_trans_data)
          agg_ins_data_df=pd.DataFrame(agg_ins_data)
          agg_user_data_df=pd.DataFrame(agg_user_data)
          agg_usersByDevice_data_df=pd.DataFrame(agg_usersByDevice_data)
          top_trans_dist_data_df=pd.DataFrame(top_trans_dist_data)
          top_trans_pincode_data_df=pd.DataFrame(top_trans_pincode_data)
          top_ins_dist_data_df=pd.DataFrame(top_ins_dist_data)
          top_ins_pincode_data_df=pd.DataFrame(top_ins_pincode_data)
          top_user_dist_data_df=pd.DataFrame(top_user_dist_data)
          top_user_pincode_data_df=pd.DataFrame(top_user_pincode_data)
          map_trans_data_df=pd.DataFrame(map_trans_data)
          map_ins_data_df=pd.DataFrame(map_ins_data)
          map_user_data_df=pd.DataFrame(map_user_data)  

          #======Combined Data=======#
          combined_data={
            "Aggregated_Data":{"agg_trans_data":agg_trans_data_df,"agg_ins_data":agg_ins_data_df,"agg_user_data":agg_user_data_df,"agg_usersByDevice_data":agg_usersByDevice_data_df},
            "Top_Data":{"top_trans_dist_data":top_trans_dist_data_df,"top_trans_pincode_data":top_trans_pincode_data_df,"top_ins_dist_data":top_ins_dist_data_df,"top_ins_pincode_data":top_ins_pincode_data_df,"top_user_dist_data":top_user_dist_data_df,"top_user_pincode_data":top_user_pincode_data_df},
            "Map_Data":{"map_trans_data":map_trans_data_df,"map_ins_data":map_ins_data_df,"map_user_data":map_user_data_df}
            }

          return combined_data
        fetch_data=extract_data(new_folder_path)
        progress_bar.progress(20)
        
        # Data separation
        # Spliting into multiple Dict
        progress_bar.progress(22)
        aggregate=fetch_data['Aggregated_Data']
        top=fetch_data['Top_Data']
        map=fetch_data['Map_Data']

        #Aggregated Data
        aggregated_transaction=aggregate['agg_trans_data']
        aggregated_insurance=aggregate['agg_ins_data']
        aggregated_user=aggregate['agg_user_data']
        aggregated_usersByDevice=aggregate['agg_usersByDevice_data']

        #Top Data:
          #Top Data--> District Data
        top_district_transaction=top['top_trans_dist_data']
        top_district_insurance=top['top_ins_dist_data']
        top_district_user=top['top_user_dist_data']

          #Top Data--> Pincode Data
        top_pincode_transaction=top['top_trans_pincode_data']
        top_pincode_insurance=top['top_ins_pincode_data']
        top_pincode_user=top['top_user_pincode_data']

        #Map Data:
        map_transaction=map['map_trans_data']
        map_insurance=map['map_ins_data']
        map_user=map['map_user_data']
        progress_bar.progress(25)
        # Data cleaning
        def data_cleaning():
          '''
          ======================================================
          ++ Cleaning The Data Before Uploading to Database ++
          ======================================================
          '''
          # Data Transformation & Null Value Handling
          top_pincode_transaction.dropna(inplace=True)
          top_pincode_insurance.dropna(inplace=True)
          top_pincode_transaction['Pincode']=top_pincode_transaction['Pincode'].astype(int)
          top_pincode_insurance['Pincode']=top_pincode_insurance['Pincode'].astype(int)
          top_pincode_user['Pincode']=top_pincode_user['Pincode'].astype(int)

          # Data Transformation
          map_transaction['District']=map_transaction['District'].str.replace(' district','')
          map_insurance['District']=map_insurance['District'].str.replace(' district','')
          map_user['District']=map_user['District'].str.replace(' district','')
        data_cleaning()
        progress_bar.progress(30)

        # Creating Sql connection
        def create_database_and_tables(database_name,my_connection):
          progress_bar.progress(38)
          try:
            # Check database exsistance
            database_status=my_connection.execute(text(f"SHOW DATABASES LIKE '{database_name}'")).fetchone()    
            
            # If no exsitance create one and subsequent tables
            if not database_status:
              my_connection.execute(text(f"CREATE DATABASE {database_name}"))
              my_connection.execute(text(f"USE {database_name}"))
              table_query_dict={
                "states":"""CREATE TABLE states (State_id INT PRIMARY KEY AUTO_INCREMENT,State VARCHAR(255) NOT NULL);""",
                "years":"""CREATE TABLE years (Year_id INT PRIMARY KEY AUTO_INCREMENT,Year INT NOT NULL)""",
                "quaters":"""CREATE TABLE quaters (Quater_id INT PRIMARY KEY AUTO_INCREMENT,Quater INT NOT NULL);""",
                "transaction_types":"""CREATE TABLE transaction_types (Transaction_type_id INT PRIMARY KEY AUTO_INCREMENT,Transaction_type VARCHAR(255) NOT NULL);""",
                "aggregated_transactions":"""CREATE TABLE aggregatedtransactions (id INT PRIMARY KEY AUTO_INCREMENT,State_id INT,Year_id INT,Quater_id INT,Transaction_type_id INT,Transaction_count BIGINT,Transaction_amount VARCHAR(255) NOT NULL,FOREIGN KEY (State_id) REFERENCES States(State_id),FOREIGN KEY (Year_id) REFERENCES Years(Year_id),FOREIGN KEY (Quater_id) REFERENCES Quaters(Quater_id),FOREIGN KEY (Transaction_type_id) REFERENCES transaction_types(Transaction_type_id));""",
                "aggregated_insurance_transactions":"""CREATE TABLE aggregatedinsurancetransactions (id INT PRIMARY KEY AUTO_INCREMENT,State_id INT,Year_id INT,Quater_id INT,Transaction_type_id INT,Transaction_count INT,Transaction_amount VARCHAR(255) NOT NULL,FOREIGN KEY (State_id) REFERENCES States(State_id),FOREIGN KEY (Year_id) REFERENCES Years(Year_id),FOREIGN KEY (Quater_id) REFERENCES Quaters(Quater_id),FOREIGN KEY (Transaction_type_id) REFERENCES transaction_types(Transaction_type_id));""",
                "aggregated_user_data":"""CREATE TABLE aggregateduserdata (id INT PRIMARY KEY AUTO_INCREMENT,State_id INT,Year_id INT,Quater_id INT,RegisteredUsers BIGINT,AppOpens BIGINT,FOREIGN KEY (State_id) REFERENCES States(State_id),FOREIGN KEY (Year_id) REFERENCES Years(Year_id),FOREIGN KEY (Quater_id) REFERENCES Quaters(Quater_id));""",
                "aggregated_users_by_device":"""CREATE TABLE aggregatedusersbydevice (id INT PRIMARY KEY AUTO_INCREMENT,State_id INT,Year_id INT,Quater_id INT,Brand VARCHAR(255),Count BIGINT,Percentage FLOAT,FOREIGN KEY (State_id) REFERENCES States(State_id),FOREIGN KEY (Year_id) REFERENCES Years(Year_id),FOREIGN KEY (Quater_id) REFERENCES Quaters(Quater_id));""",
                "districts":"""CREATE TABLE districts (District_id INT PRIMARY KEY AUTO_INCREMENT,District VARCHAR(255) NOT NULL,State_id INT,FOREIGN KEY (State_id) REFERENCES States(State_id));""",
                "top_district_transactions":"""CREATE TABLE topdistricttransactions(id INT PRIMARY KEY AUTO_INCREMENT,State_id INT,Year_id INT,Quater_id INT,District_id INT,Transaction_count BIGINT,Transaction_amount VARCHAR(255) NOT NULL,FOREIGN KEY (State_id) REFERENCES States(State_id),FOREIGN KEY (Year_id) REFERENCES Years(Year_id),FOREIGN KEY (Quater_id) REFERENCES Quaters(Quater_id),FOREIGN KEY (District_id) REFERENCES Districts(District_id));""",
                "top_district_insurance_transactions":"""CREATE TABLE topdistrictinsurancetransactions (id INT PRIMARY KEY AUTO_INCREMENT,State_id INT,Year_id INT,Quater_id INT,District_id INT,Transaction_count INT,Transaction_amount VARCHAR(255) NOT NULL,FOREIGN KEY (State_id) REFERENCES States(State_id),FOREIGN KEY (Year_id) REFERENCES Years(Year_id),FOREIGN KEY (Quater_id) REFERENCES Quaters(Quater_id),FOREIGN KEY (District_id) REFERENCES Districts(District_id));""",
                "top_district_user_data":"""CREATE TABLE topdistrictuserdata (id INT PRIMARY KEY AUTO_INCREMENT,State_id INT,Year_id INT,Quater_id INT,District_id INT,RegisteredUsers BIGINT,FOREIGN KEY (State_id) REFERENCES States(State_id),FOREIGN KEY (Year_id) REFERENCES Years(Year_id),FOREIGN KEY (Quater_id) REFERENCES Quaters(Quater_id),FOREIGN KEY (District_id) REFERENCES Districts(District_id));""",
                "pincodes":"""CREATE TABLE pincodes (Pincode_id INT PRIMARY KEY AUTO_INCREMENT,Pincode INT NOT NULL);""",
                "top_pincode_transactions":"""CREATE TABLE toppincodetransactions (id INT PRIMARY KEY AUTO_INCREMENT,State_id INT,Year_id INT,Quater_id INT,Pincode_id INT,Transaction_count BIGINT,Transaction_amount VARCHAR(255) NOT NULL,FOREIGN KEY (State_id) REFERENCES States(State_id),FOREIGN KEY (Year_id) REFERENCES Years(Year_id),FOREIGN KEY (Quater_id) REFERENCES Quaters(Quater_id),FOREIGN KEY (Pincode_id) REFERENCES Pincodes(Pincode_id));""",
                "top_pincode_insurance_transactions":"""CREATE TABLE toppincodeinsurancetransactions (id INT PRIMARY KEY AUTO_INCREMENT,State_id INT,Year_id INT,Quater_id INT,Pincode_id INT,Transaction_count INT,Transaction_amount VARCHAR(255) NOT NULL,FOREIGN KEY (State_id) REFERENCES States(State_id),FOREIGN KEY (Year_id) REFERENCES Years(Year_id),FOREIGN KEY (Quater_id) REFERENCES Quaters(Quater_id),FOREIGN KEY (Pincode_id) REFERENCES Pincodes(Pincode_id));""",
                "top_pincode_user_data":"""CREATE TABLE toppincodeuserdata (id INT PRIMARY KEY AUTO_INCREMENT,State_id INT,Year_id INT,Quater_id INT,Pincode_id INT,RegisteredUsers BIGINT,FOREIGN KEY (State_id) REFERENCES States(State_id),FOREIGN KEY (Year_id) REFERENCES Years(Year_id),FOREIGN KEY (Quater_id) REFERENCES Quaters(Quater_id),FOREIGN KEY (Pincode_id) REFERENCES Pincodes(Pincode_id));""",
                "map_transactions":"""CREATE TABLE maptransactions (id INT PRIMARY KEY AUTO_INCREMENT,State_id INT,Year_id INT,Quater_id INT,District_id INT,Transaction_count BIGINT,Transaction_amount VARCHAR(255) NOT NULL,FOREIGN KEY (State_id) REFERENCES States(State_id),FOREIGN KEY (Year_id) REFERENCES Years(Year_id),FOREIGN KEY (Quater_id) REFERENCES Quaters(Quater_id),FOREIGN KEY (District_id) REFERENCES Districts(District_id));""",
                "map_insurance_transactions":"""CREATE TABLE mapinsurancetransactions (id INT PRIMARY KEY AUTO_INCREMENT,State_id INT,Year_id INT,Quater_id INT,District_id INT,Transaction_count BIGINT,Transaction_amount VARCHAR(255) NOT NULL,FOREIGN KEY (State_id) REFERENCES States(State_id),FOREIGN KEY (Year_id) REFERENCES Years(Year_id),FOREIGN KEY (Quater_id) REFERENCES Quaters(Quater_id),FOREIGN KEY (District_id) REFERENCES Districts(District_id));""",
                "map_user_data":"""CREATE TABLE mapuserdata (id INT PRIMARY KEY AUTO_INCREMENT,State_id INT,Year_id INT,Quater_id INT,District_id INT,RegisteredUsers BIGINT,AppOpens BIGINT,FOREIGN KEY (State_id) REFERENCES States(State_id),FOREIGN KEY (Year_id) REFERENCES Years(Year_id),FOREIGN KEY (Quater_id) REFERENCES Quaters(Quater_id),FOREIGN KEY (District_id) REFERENCES Districts(District_id));"""}
              for key,value in table_query_dict.items():
                my_connection.execute(text(table_query_dict[key]))        
            
            # if database exists, run use database command
            else:
              my_connection.execute(text(f"USE {database_name}"))

          except Exception as e:
            print(f'Error Occured: {e}')
        create_database_and_tables(database_name,my_connection=my_connection_properties[0]['connection'])
        progress_bar.progress(40)

        # sql connection with database
        my_connection=my_connection_properties[0]['connection']
        my_connection.execute(text(f"USE {database_name}"))
        engine=create_engine(f"mysql+pymysql://{my_connection_properties[0]['user_and_connection_details']['username']}:{my_connection_properties[0]['user_and_connection_details']['password']}@{my_connection_properties[0]['user_and_connection_details']['host_name']}:{my_connection_properties[0]['user_and_connection_details']['port']}/{database_name}", echo=False)
        
        # excel location to upload basic tables
        file_path="Replace with location of Excel File" #///////////////////////////Replace with location of Excel File////////////////////////
        progress_bar.progress(45)

        # function to upload basic tables
        def sql_basic_table_upload(file_path):
          try:
            excel_sheet_list=['Year','Quater','State','Pincode','Transaction_type']
            database_table_list=['years','quaters','states','pincodes','transaction_types']
            for i in excel_sheet_list:
              index_value=excel_sheet_list.index(i)
              file=pd.read_excel(file_path,sheet_name=i)
              check_table=pd.DataFrame(my_connection.execute(text(f'SELECT * FROM {database_table_list[index_value]}')).fetchall())
              if not check_table.empty:
                merged_df = pd.merge(file, check_table, left_on=i, right_on=i, how='left', indicator=True)
                new_df = merged_df[merged_df['_merge'] == 'left_only'].drop(columns='_merge')
                filtered_df=new_df[i]
                if not new_df.empty:
                  new_df.to_sql(database_table_list[index_value], engine, if_exists='append', index=False)
                else:
                  pass
              else:
                file.to_sql(database_table_list[index_value], engine, if_exists='append', index=False)
            my_connection.commit()       
            file=pd.read_excel(file_path,sheet_name="State_and_District")
            states_table=pd.DataFrame(my_connection.execute(text('SELECT * FROM states')).fetchall())
            districts_table=pd.DataFrame(my_connection.execute(text('SELECT * FROM districts')).fetchall())

            if not districts_table.empty:
              merged_df = pd.merge(file, districts_table, left_on='District', right_on='District', how='left', indicator=True)
              new_df = merged_df[merged_df['_merge'] == 'left_only'].drop(columns='_merge')
              new_df.drop(columns=['District_id'],inplace=True)
              if not new_df.empty:
                new_df.to_sql(database_table_list[index_value], engine, if_exists='append', index=False)
              else:
                pass
            else:
              merged_df=file.merge(states_table,on='State')
              merged_df.drop(columns=['State'],inplace=True)
              merged_df.to_sql("districts", engine, if_exists='append', index=False)
            my_connection.commit()
          except Exception as e:
            st.error(f'An Error Occured: {e}')
        sql_basic_table_upload(file_path)
        progress_bar.progress(50)

        # Function for Data upload (apart from basic tables)
        def sql_data_upload():
          progress_bar.progress(67)
          phonepe_data_list=[aggregated_transaction,aggregated_insurance,aggregated_user,aggregated_usersByDevice,top_district_transaction,top_district_insurance,top_district_user,top_pincode_transaction,top_pincode_insurance,top_pincode_user,map_transaction,map_insurance,map_user]
          states_sql_table=pd.DataFrame(my_connection.execute(text('SELECT * FROM states')).fetchall())
          years_sql_table=pd.DataFrame(my_connection.execute(text('SELECT * FROM years')).fetchall())
          quaters_sql_table=pd.DataFrame(my_connection.execute(text('SELECT * FROM quaters')).fetchall())
          districts_sql_table=pd.DataFrame(my_connection.execute(text('SELECT * FROM districts')).fetchall())
          pincodes_sql_table=pd.DataFrame(my_connection.execute(text('SELECT * FROM pincodes')).fetchall())
          transaction_type_sql_table=pd.DataFrame(my_connection.execute(text('SELECT * FROM transaction_types')).fetchall())
          try:
            for col in phonepe_data_list:
              col['Year']=col['Year'].astype(int)
            upload_dict={
              'aggregatedtransactions'            :[],
                'aggregatedinsurancetransactions' :[],
                'aggregateduserdata'              :[],
                'aggregatedusersbydevice'         :[],
                'topdistricttransactions'         :[],
                'topdistrictinsurancetransactions':[],
                'topdistrictuserdata'             :[],
                'toppincodetransactions'          :[],
                'toppincodeinsurancetransactions' :[],
                'toppincodeuserdata'              :[],
                'maptransactions'                 :[],
                'mapinsurancetransactions'        :[],
                'mapuserdata'                     :[]
              }

            for df in phonepe_data_list:
              new_df=df
              new_df=new_df.merge(states_sql_table,on='State')
              new_df=new_df.merge(years_sql_table,on='Year')
              new_df=new_df.merge(quaters_sql_table,on='Quater')
              new_df.drop(columns=['State','Year','Quater'],inplace=True)
              
              if df.equals(aggregated_transaction):
                new_df=new_df.merge(transaction_type_sql_table,on='Transaction_type')
                new_df.drop(columns=['Transaction_type'],inplace=True)
                new_df['Transaction_amount']=new_df['Transaction_amount'].astype(str)
                upload_dict['aggregatedtransactions'].append(new_df)
              elif df.equals(aggregated_insurance):
                new_df=new_df.merge(transaction_type_sql_table,on='Transaction_type')
                new_df.drop(columns=['Transaction_type'],inplace=True)
                new_df['Transaction_amount']=new_df['Transaction_amount'].astype(str)
                upload_dict['aggregatedinsurancetransactions'].append(new_df)
              elif df.equals(aggregated_user):
                upload_dict['aggregateduserdata'].append(new_df)
              elif df.equals(aggregated_usersByDevice):
                upload_dict['aggregatedusersbydevice'].append(new_df)
              elif df.equals(top_district_transaction):
                new_df=new_df.merge(districts_sql_table.drop(columns=['State_id']),on='District')
                new_df.drop(columns=['District'],inplace=True)
                new_df['Transaction_amount']=new_df['Transaction_amount'].astype(str)
                upload_dict['topdistricttransactions'].append(new_df)
              elif df.equals(top_district_insurance): 
                new_df=new_df.merge(districts_sql_table.drop(columns=['State_id']),on='District')
                new_df.drop(columns=['District'],inplace=True)
                new_df['Transaction_amount']=new_df['Transaction_amount'].astype(str)
                upload_dict['topdistrictinsurancetransactions'].append(new_df)
              elif df.equals(top_district_user):
                new_df=new_df.merge(districts_sql_table.drop(columns=['State_id']),on='District')
                new_df.drop(columns=['District'],inplace=True)
                upload_dict['topdistrictuserdata'].append(new_df)
              elif df.equals(top_pincode_transaction):
                new_df=new_df.merge(pincodes_sql_table,on='Pincode')
                new_df.drop(columns=['Pincode'],inplace=True)
                new_df['Transaction_amount']=new_df['Transaction_amount'].astype(str)
                upload_dict['toppincodetransactions'].append(new_df)
              elif df.equals(top_pincode_insurance):
                new_df=new_df.merge(pincodes_sql_table,on='Pincode')
                new_df.drop(columns=['Pincode'],inplace=True)
                new_df['Transaction_amount']=new_df['Transaction_amount'].astype(str)
                upload_dict['toppincodeinsurancetransactions'].append(new_df)
              elif df.equals(top_pincode_user):
                new_df=new_df.merge(pincodes_sql_table,on='Pincode')
                new_df.drop(columns=['Pincode'],inplace=True)
                upload_dict['toppincodeuserdata'].append(new_df)
              elif df.equals(map_transaction):
                new_df=new_df.merge(districts_sql_table.drop(columns=['State_id']),on='District')
                new_df.drop(columns=['District'],inplace=True)
                new_df['Transaction_amount']=new_df['Transaction_amount'].astype(str)
                upload_dict['maptransactions'].append(new_df)
              elif df.equals(map_insurance):
                new_df=new_df.merge(districts_sql_table.drop(columns=['State_id']),on='District')
                new_df.drop(columns=['District'],inplace=True)
                new_df['Transaction_amount']=new_df['Transaction_amount'].astype(str)
                upload_dict['mapinsurancetransactions'].append(new_df)
              else:
                new_df=new_df.merge(districts_sql_table.drop(columns=['State_id']),on='District')
                new_df.drop(columns=['District'],inplace=True)
                upload_dict['mapuserdata'].append(new_df)

            for key,value in upload_dict.items():

              if value:
                check_table=pd.read_sql(key,engine)
                upload_table=pd.concat(value)

              if not check_table.empty:
                check_table = check_table[sorted(check_table.columns)]
                upload_table = upload_table[sorted(upload_table.columns)]
                merged_df = pd.merge(upload_table, check_table, how='left', indicator=True)
                new_df = merged_df[merged_df['_merge'] == 'left_only'].drop(columns='_merge')
                if not new_df.empty:
                  new_df.to_sql(f'{key}', engine, if_exists='append', index=False)
                  my_connection.commit()
                else:
                  pass
              else:
                upload_table.to_sql(f'{key}', engine, if_exists='append', index=False)
                my_connection.commit()
            my_connection.commit()
          except Exception as e:
            error=f'An Error Occured: {e}'
            print(error)
          progress_bar.progress(87)
        sql_data_upload()


        # Progress bar update
        progress_bar.progress(90)

        # Progress bar update
        progress_bar.progress(100)
        in_progress.empty()
        progress_bar.empty()
        st.success('**All done.. Data uploaded to database..!**', icon="✅")

        # reset session state
        session_reset() 
        return database_name
      
    uploading_data_to_sql=sql_data_upload_process(new_folder_path,my_connection_properties)

    #Saving database into session state
    st.session_state['my_database_name']=uploading_data_to_sql



#////////////////////////////////////Data Analytics Section/////////////////////////////////////////////

elif st.session_state['selected_section'] == "**Data Analytics**":
  background_image_css = """
  <style>
  [data-testid="stAppViewContainer"] > .main {
    background-image: url("https://i.postimg.cc/8kfpsCnz/gradient-2.jpg");
    background-size: cover;
    background-position: center center;
    background-repeat: no-repeat;
    background-attachment: local;
    padding: 0 !important;  /* Ensure no padding */
    margin: 0 !important;   /* Ensure no margin */
  }
  </style>
  """
  st.markdown(background_image_css, unsafe_allow_html=True)

  sidebar_background_image= """
  <style>
  [data-testid=stSidebar] {
    background-image: url('https://i.postimg.cc/7PgqkWWB/purble-and-whie.jpg');
    background-size: cover;
    background-position: center;
    background-repeat: no-repeat;
    padding: 0 !important;  /* Ensure no padding */
    margin: 0 !important;   /* Ensure no margin */
  }
  </style>
  """
  st.markdown(sidebar_background_image, unsafe_allow_html=True)

  # Intiating Connection Status
  connection_value=False

    # Creating Sql Connection
  def sql_connect():
    try:
      # Checking exsistance of sql connection in session state 
      my_connection_properties = st.session_state['my_connection_properties']
      database_name=st.session_state['my_database_name']

      # Creating engine and connection if sql connection exist in session state 
      if my_connection_properties and database_name:
        engine=create_engine(f"mysql+pymysql://{my_connection_properties[0]['user_and_connection_details']['username']}:{my_connection_properties[0]['user_and_connection_details']['password']}@{my_connection_properties[0]['user_and_connection_details']['host_name']}:{my_connection_properties[0]['user_and_connection_details']['port']}/{database_name}", echo=False)
        my_connection = engine.connect()

        # Updating session state and connection status
        st.session_state['new_sql_connection']=True
        connection_value=True
        st.session_state['Data_analytics_section_engine']=engine
        st.session_state['Data_analytics_section_connection']=my_connection

        #Return incase connection establishment
        return engine,my_connection,connection_value

    # Except block in case of error creates new connection from user input
    except Exception as e:
      # Check point to return existing connection in a single session incase connection is created
      if st.session_state['new_sql_connection']:
        connection_value=True
        return st.session_state['Data_analytics_section_engine'],st.session_state['Data_analytics_section_connection'],connection_value
      
      # Incase of key error, new connection will be made
      elif isinstance(e,KeyError):
        try:
          #Empty placeholders
          empty_space_holder=st.empty()
          sub_header_data_analytics_1=st.empty()
          sub_header_data_analytics_2=st.empty()
          sql_username_placeholder=st.empty()
          sql_password_placeholder=st.empty()
          variable_cotainer=st.empty()
          enter_description=st.empty()
          empty_space_holder.write('')
          st.markdown("""<style>h3{color: white !important;}</style>""",unsafe_allow_html=True)
          sub_header_data_analytics_1.subheader("**You dont have sql connection**")
          sub_header_data_analytics_2.subheader('**Please provide the following inputs to fetch the data:**')

          sql_hostname="127.0.0.1"
          sql_port=3306
          
          # CSS for input label color
          st.markdown(
            """
            <style>
            .stTextInput label {
              color: white !important;
            }
            </style>
            """,
            unsafe_allow_html=True)

         # Getting user Input 
          sql_username = sql_username_placeholder.text_input("**SQL Username**")
          sql_password = sql_password_placeholder.text_input("**SQL Password**", type="password")
          sql_database_name = variable_cotainer.text_input("**Database Name**")
          enter_description.text("Press Enter..")

          # If username and password true connection will be created
          if sql_username and sql_password and sql_database_name:
            engine=create_engine(f"mysql+pymysql://{sql_username}:{sql_password}@{sql_hostname}:{sql_port}/{sql_database_name}", echo=False)
            my_connection = engine.connect()

            # session state update & my connection status update
            st.session_state['new_sql_connection']=True
            st.session_state['Data_analytics_section_engine']=engine
            st.session_state['Data_analytics_section_connection']=my_connection
            connection_value=True

            # Updating empty placeholders
            def empty_input_fields():
              empty_space_holder.empty()
              sub_header_data_analytics_1.empty()
              sub_header_data_analytics_2.empty()
              sql_username_placeholder.empty()
              sql_password_placeholder.empty()
              variable_cotainer.empty()
              enter_description.empty()
            empty_input_fields()

            # returning connection and engine and connection status
            return engine,my_connection,connection_value
            
        except Exception as e:
          st.error(f'An Error Occured: {e}')
  
  # Establishing Connection
  try:
    engine,my_connection,connection_value=sql_connect()
  except TypeError as e:
    pass

  # Sidebar
  with st.sidebar:
    selected_option = st.radio("**Choose the analysis type:**", ("**Transaction**", "**Insurance**", "**User**","**Trend**"))

  if connection_value:
    def display_section(option):

      def format_indian_number_for_count(value):
        value = int(value)
        
        # Format the integer with comma separation
        formatted_value = locale.format_string("%d", value, grouping=True)
        
        return formatted_value
      
      def format_indian_number(value):

        # Format into Indian numbering system with suffix (K, L, Cr) & comma separation
        if value >= 1e7:
          formatted_value = f"₹ {value / 1e7:.2f} Cr"
        elif value >= 1e5:
          formatted_value = f"₹ {value / 1e5:.2f} L"
        elif value >= 1e3:
          formatted_value = f"₹ {value / 1e3:.2f} K"
        else:
          formatted_value = f"₹ {value:.2f}"

        # Add comma separation to the integer part only
        parts = formatted_value.split('.')
        int_part = parts[0].replace('₹ ', '')
        int_part_with_commas = locale.format_string("%d", int(int_part.replace(',', '')), grouping=True)
        formatted_value = '₹ ' + int_part_with_commas + '.' + parts[1]

        return formatted_value
      
      def wrap_with_grey_background(content):
        return f'<div class="grey-background-col">{content}</div>'

      if option == '**Transaction**':

        with st.sidebar:

          custom_css_remove_spaces_radio_button = """
            <style>
            [data-testid="stRadio"] label {
            padding: 0 !important;
            margin: 0 !important;
            }
            </style>
            """
          st.markdown(custom_css_remove_spaces_radio_button,unsafe_allow_html=True)

          page_option=st.radio("**Analysis Page**",['**Map Analysis**','**Top 5 Analysis**'])

        custom_css_label_color = """
          <style>
          /* Targeting the label of st.selectbox */
          div[data-testid="stSelectbox"] label {
          color: white !important; /* Set text color to white */
          padding: 0 !important;
          margin: 0 !important;
          }
          </style>
          """
        st.markdown(custom_css_label_color, unsafe_allow_html=True)

        if page_option=='**Map Analysis**':

          years=[2018,2019,2020,2021,2022,2023,2024]
          quaters={'Q1 (Jan-Mar)':1,'Q2 (Apr-Jun)':2,'Q3 (Jul-Sep)':3,'Q4 (Oct-Dec)':4}
          quaters_2024={'Q1 (Jan-Mar)':1}
          col1, col2,col3,col4 = st.columns([0.28,0.1,0.35,0.95])
          
          with col1:
            year_key = st.selectbox("Select Year",(years))
          with col3:
            if year_key != 2024:
              quater_key = st.selectbox("Select Quater",list(quaters.keys()))
            else:
              quater_key = st.selectbox("Select Quater",list(quaters_2024.keys()))

          query=f"""
          SELECT 
            st.State,
            yt.Year,
            qt.Quater, 
            tt.Transaction_type, 
            agg_trans.Transaction_amount, 
            agg_trans.Transaction_count
          FROM 
            aggregatedtransactions as agg_trans,
            states AS st,
            years AS yt,
            quaters AS qt,
            transaction_types AS tt
          WHERE
            st.State_id=agg_trans.State_id AND
            yt.Year_id=agg_trans.Year_id AND
            qt.Quater_id=agg_trans.Quater_id AND
            tt.Transaction_type_id=agg_trans.Transaction_type_id AND
            yt.Year = {year_key} AND
            qt.Quater = {quaters[quater_key]};
            """
          
          Agg_trans = pd.read_sql(query,con=engine)
          Agg_trans['Transaction_amount']=Agg_trans['Transaction_amount'].astype(float)
          
          stati_box_count=format_indian_number_for_count(value=Agg_trans['Transaction_count'].sum())
          stati_box_amount=format_indian_number(value=Agg_trans['Transaction_amount'].sum())
          stati_box_amount_text_1=stati_box_amount[:-6]
          stati_box_amount_text_2=stati_box_amount[-3:]
          stati_box_amount=stati_box_amount_text_1+stati_box_amount_text_2
          stati_box_avg_amt=format_indian_number_for_count(value=(Agg_trans['Transaction_amount'].sum()/Agg_trans['Transaction_count'].sum()))
          stati_box_avg_amt='₹ '+str(stati_box_avg_amt)
          stati_box_quater_count_sum = Agg_trans.groupby("Transaction_type")["Transaction_count"].sum()
          sort_stati_box_quater_count_sum=stati_box_quater_count_sum.sort_values(ascending=False)
          sorted_count_dict=dict(sort_stati_box_quater_count_sum)
          sorted_count_list=list(sorted_count_dict.items())
          stati_box_quater_amt_sum = Agg_trans.groupby("Transaction_type")["Transaction_amount"].sum()
          sort_stati_box_quater_amt_sum=stati_box_quater_amt_sum.sort_values(ascending=False)
          sorted_amt_dict=dict(sort_stati_box_quater_amt_sum)
          sorted_amt_list=list(sorted_amt_dict.items())

          pan_area_1,pan_area_2=st.columns([7,3])
          with pan_area_1:
            
            def result_map(df):
              # Set locale to Indian format
              locale.setlocale(locale.LC_ALL, 'en_IN')

              # Load transaction & grouping
              Agg_trans = df
              GrouBy_data = Agg_trans.groupby('State').agg({'Transaction_amount': 'sum','Transaction_count': 'sum'})
              data_df=GrouBy_data.reset_index()
              data_df['Average_transaction_amount'] = data_df['Transaction_amount'] / data_df['Transaction_count']

              # Average Transaction count
              data_df['Average_transaction_count'] = Agg_trans.groupby('State')['Transaction_count'].sum().values

              # State Name modification 
              data_df['State'] = data_df['State'].str.replace('-', ' ').str.title()

              # Load GeoJSON data using GeoPandas
              geo_data = gpd.read_file(" Replace with json file ") #///////////////Replace with json file location///////////

              # Dissolve the GeoDataFrame by the 'State' column to combine the geometries
              geo_data = geo_data.dissolve(by='State', as_index=False)
              geo_data['State'] = geo_data['State'].str.replace('-', ' ').str.title()

              # Convert the GeoDataFrame to a GeoJSON format
              geo_json = json.loads(geo_data.to_json())

              # Merge dataframe with the GeoDataFrame
              df = geo_data.merge(data_df, how='left')

              # Formatted transaction amounts for hover data label
              df['Transaction_amount_formatted'] = df['Transaction_amount'].apply(format_indian_number)
              df['Average_transaction_amount_formatted'] = df['Average_transaction_amount'].apply(format_indian_number)
              df['Average_transaction_count_formatted']=df['Average_transaction_count'].apply(format_indian_number_for_count)

              # Creating choropleth map using Plotly Express
              fig = px.choropleth(
                df,
                geojson=geo_json,
                featureidkey="properties.State",
                locations="State",
                color="Transaction_amount",
                color_continuous_scale=["#3399FF","#FFFFFF","#FF0000"],
                projection="mercator",
                labels={'Transaction_amount': 'Transaction Amount'},
                hover_data={
                  'State': True,
                  'Transaction_amount_formatted': True,
                  'Average_transaction_amount_formatted': True,
                  'Average_transaction_count': True,
                  'Transaction_amount': False,  # Hiding Unformated transaction amount
                  'Average_transaction_amount': False  # Hiding Unformated avg transaction amount
                }
              )

              # Update the map to fit the GeoJSON data
              fig.update_geos(fitbounds="locations", visible=False)

              # Creating tick values based data range
              min_val = df['Transaction_amount'].min()
              max_val = df['Transaction_amount'].max()
              num_ticks = 8  # Number of ticks
              tickvals = np.linspace(min_val, max_val, num_ticks)

              # Formatting color bar tick values to Indian numeric system
              ticktext = [format_indian_number(tick) for tick in tickvals]

              fig.update_layout(
                margin=dict(l=0, r=0, t=0, b=0),
                geo=dict(
                  bgcolor='#210D38',
                  center=dict(lat=20.5937, lon=78.9629),  # setting Center of map 
                  projection_scale=1.2, 
                  lataxis_showgrid=False,
                  lonaxis_showgrid=False,
                ),
                paper_bgcolor='rgba(0,0,0,0)',
                
                coloraxis_colorbar=dict(
                  title='Transaction Value',
                  titleside="top",
                  x=1,
                  y=0.5,
                  title_font_color="white",
                  tickvals=tickvals,
                  ticktext=ticktext,
                  tickfont=dict(color="white"),
                  outlinecolor="white",
                  outlinewidth=0.01
                )
              )

              # Custom hover template
              fig.update_traces(
                hovertemplate=(
                  '<b style="color:white;font-size:10px">State</b><br>'
                  '<b style="color:cyan;font-size:16px">%{customdata[0]}</b><br>' +
                  '<br>'+
                  '<b style="color:white;font-size:10px">All Transactions</b><br>'+
                  '<b style="color:cyan;font-size:16px">%{customdata[3]}</b><br>'+
                  '<br>'+
                  '<b style="color:white;font-size:10px">Total Transaction Value</b><br>'+
                  '<b style="color:cyan;font-size:16px">%{customdata[1]}</b><br>'+
                  '<br>'+
                  '<b style="color:white;font-size:10px">Avg Transaction Value</b><br>'+
                  '<b style="color:cyan;font-size:16px">%{customdata[2]}</b><br>'
                ),
                customdata=df[['State', 'Transaction_amount_formatted', 'Average_transaction_amount_formatted', 'Average_transaction_count_formatted']]
              )

              # CSS styling for hover data box
              fig.update_layout(
                hoverlabel=dict(
                  bgcolor="#444444",  # Background color
                  font_size=12,  # Font size 
                  font_family="Arial",  # Font family 
                )
              )

              # view the map
              st.write(fig)

            result_map(df=Agg_trans)

          with pan_area_2:

            box_container=st.container(height=400,border=False)
            with box_container:

              # CSS styling for background color on the box
              st.markdown(
              """
              <style>
              .grey-background-col {
                background-color: #2C1942;
                padding: 10px;
                border-radius: 5px;
                height: 100%;
              }
              </style>
              """,unsafe_allow_html=True)

              content=f"""
                <p style="color: cyan; font-size: 16px; font-family: Arial; margin: 5px 0;"><strong>Transactions</strong></p>
                <p style="color: #FFFFFF; font-size: 13px; font-family: Times New Roman; margin: 10px 0;">All PhonePe transactions (UPI + Cards + Wallets)</p>
                <p style="color: cyan; font-size: 20px; font-family: 'Arial Black'; margin: 5px 0;">{stati_box_count}</p>
                <p style="color: #FFFFFF; font-size: 13px; font-family: Times New Roman; margin: 5px 0;">Total transaction value</p>
                <p style="color: cyan; font-size: 16px; font-family: Arial; margin: 5px 0;"><strong>{stati_box_amount}</strong></p>
                <p style="color: #FFFFFF; font-size: 13px; font-family: Times New Roman; margin: 5px 0;">Average transaction value</p>
                <p style="color: cyan; font-size: 16px; font-family: 'Arial'; margin: 1px 0;"><strong>{stati_box_avg_amt}</strong></p>
                <p style="color: #513F66; font-size: 13px; font-family: Arial; margin: 1px 0;">_________________________</p>
                <p style="color: #FFFFFF; font-size: 16px; font-family: Arial; margin: 5px 0;"><strong><u>Transaction in Categories</u></strong></p>
                <p style="color: #FFFFFF; font-size: 13px; font-family: Arial; margin: 1px 0;">{sorted_count_list[0][0]}</p>
                <p style="color: cyan; font-size: 13px; font-family: Arial; margin: 1px 0; padding-left: 0px;"><strong>{format_indian_number_for_count(value=sorted_count_list[0][1])}</strong></p>
                <p style="color: #FFFFFF; font-size: 13px; font-family: Arial; margin: 1px 0;">{sorted_count_list[1][0]}</p>
                <p style="color: cyan; font-size: 13px; font-family: Arial; margin: 1px 0; padding-left: 0px;"><strong>{format_indian_number_for_count(value=sorted_count_list[1][1])}</strong></p>
                <p style="color: #FFFFFF; font-size: 13px; font-family: Arial; margin: 1px 0;">{sorted_count_list[2][0]}</p>
                <p style="color: cyan; font-size: 13px; font-family: Arial; margin: 1px 0; padding-left: 0px;"><strong>{format_indian_number_for_count(value=sorted_count_list[2][1])}</strong></p>
                <p style="color: #FFFFFF; font-size: 13px; font-family: Arial; margin: 1px 0;">{sorted_count_list[3][0]}</p>
                <p style="color: cyan; font-size: 13px; font-family: Arial; margin: 1px 0; padding-left: 0px;"><strong>{format_indian_number_for_count(value=sorted_count_list[3][1])}</strong></p>
                <p style="color: #FFFFFF; font-size: 13px; font-family: Arial; margin: 1px 0;">{sorted_count_list[4][0]}</p>
                <p style="color: cyan; font-size: 13px; font-family: Arial; margin: 1px 0; padding-left: 0px;"><strong>{format_indian_number_for_count(value=sorted_count_list[4][1])}</strong></p>
                <p style="color: #513F66; font-size: 13px; font-family: Arial; margin: 1px 0;">_________________________</p>
                <p style="color: #FFFFFF; font-size: 16px; font-family: Arial; margin: 5px 0;"><strong><u>Transaction value in categories</u></strong></p>
                <p style="color: #FFFFFF; font-size: 13px; font-family: Arial; margin: 1px 0;">{sorted_amt_list[0][0]}</p>
                <p style="color: cyan; font-size: 13px; font-family: Arial; margin: 1px 0; padding-left: 0px;"><strong>{format_indian_number(value=sorted_amt_list[0][1])}</strong></p>
                <p style="color: #FFFFFF; font-size: 13px; font-family: Arial; margin: 1px 0;">{sorted_amt_list[1][0]}</p>
                <p style="color: cyan; font-size: 13px; font-family: Arial; margin: 1px 0; padding-left: 0px;"><strong>{format_indian_number(value=sorted_amt_list[1][1])}</strong></p>
                <p style="color: #FFFFFF; font-size: 13px; font-family: Arial; margin: 1px 0;">{sorted_amt_list[2][0]}</p>
                <p style="color: cyan; font-size: 13px; font-family: Arial; margin: 1px 0; padding-left: 0px;"><strong>{format_indian_number(value=sorted_amt_list[2][1])}</strong></p>
                <p style="color: #FFFFFF; font-size: 13px; font-family: Arial; margin: 1px 0;">{sorted_amt_list[3][0]}</p>
                <p style="color: cyan; font-size: 13px; font-family: Arial; margin: 1px 0; padding-left: 0px;"><strong>{format_indian_number(value=sorted_amt_list[3][1])}</strong></p>
                <p style="color: #FFFFFF; font-size: 13px; font-family: Arial; margin: 1px 0;">{sorted_amt_list[4][0]}</p>
                <p style="color: cyan; font-size: 13px; font-family: Arial; margin: 1px 0; padding-left: 0px;"><strong>{format_indian_number(value=sorted_amt_list[4][1])}</strong></p>
              """
              box_container.markdown(wrap_with_grey_background(content), unsafe_allow_html=True)
        
        elif page_option == '**Top 5 Analysis**':

          top_analysis_container=st.container(border=False)
          with top_analysis_container:

            years=[2018,2019,2020,2021,2022,2023,2024]
            quaters={'Q1 (Jan-Mar)':1,'Q2 (Apr-Jun)':2,'Q3 (Jul-Sep)':3,'Q4 (Oct-Dec)':4}
            quaters_2024={'Q1 (Jan-Mar)':1}
            
            col1, col2,col3,col4 = st.columns([11,27,17,25])
            with col1:
              year_key = st.selectbox("Select Year",(years))

            with col3:
              if year_key != 2024:
                quater_key = st.selectbox("Select Quater",list(quaters.keys()))
              else:
                quater_key = st.selectbox("Select Quater",list(quaters_2024.keys()))

            def top_states_count_and_amount_query(year,quater):

              top_states_count_and_amount_query=f"""
              SELECT 
                st.State,
                agg_trans.Transaction_amount,
                agg_trans.Transaction_count
              FROM 
                aggregatedtransactions as agg_trans,
                states AS st,
                years AS yt,
                quaters AS qt,
                transaction_types AS tt
              WHERE
                st.State_id=agg_trans.State_id AND
                yt.Year_id=agg_trans.Year_id AND
                qt.Quater_id=agg_trans.Quater_id AND
                tt.Transaction_type_id=agg_trans.Transaction_type_id AND
                yt.Year = {year} AND
                qt.Quater = {quater};
              """

              top_states_amt_count=pd.read_sql(top_states_count_and_amount_query,con=engine)
              top_states_amt_count['Transaction_amount']=top_states_amt_count['Transaction_amount'].astype(float)
              top_states_amt_count['State']=top_states_amt_count['State'].str.replace('-',' ')
              top_states_amt_count['State']=top_states_amt_count['State'].str.title()

              sum_transaction_amt = top_states_amt_count.groupby('State',as_index=False)['Transaction_amount'].sum()
              sum_transaction_amt=sum_transaction_amt.sort_values(by='Transaction_amount',ascending=False).head(5).reset_index(drop=True)
              sum_transaction_amt['Transaction_amount']=sum_transaction_amt['Transaction_amount'].apply(format_indian_number)

              sum_transaction_count = top_states_amt_count.groupby('State',as_index=False)['Transaction_count'].sum()
              sum_transaction_count=sum_transaction_count.sort_values(by='Transaction_count',ascending=False).head(5).reset_index(drop=True)
              sum_transaction_count['Transaction_count']=sum_transaction_count['Transaction_count'].apply(format_indian_number)
              sum_transaction_count['Transaction_count']=sum_transaction_count['Transaction_count'].str.replace('₹',"")
              
              top_states_amt=sum_transaction_amt
              top_states_count=sum_transaction_count
              return top_states_amt,top_states_count
            top_states_amt,top_states_count=top_states_count_and_amount_query(year=year_key,quater=quaters[quater_key])
            
            def top_Districts_count_and_amount_query(year,quater):

              top_Districts_count_and_amount_query=f"""
              SELECT 
                dt.District,
                top_district_trans.Transaction_amount,
                top_district_trans.Transaction_count
              FROM 
                topdistricttransactions as top_district_trans,
                states AS st,
                years AS yt,
                quaters AS qt,
                districts AS dt
              WHERE
                st.State_id=top_district_trans.State_id AND
                yt.Year_id=top_district_trans.Year_id AND
                qt.Quater_id=top_district_trans.Quater_id AND
                dt.District_id=top_district_trans.District_id AND
                yt.Year = {year} AND
                qt.Quater = {quater};
              """

              top_district_amt_count=pd.read_sql(top_Districts_count_and_amount_query,con=engine)

              top_district_amt_count['Transaction_amount']=top_district_amt_count['Transaction_amount'].astype(float)
              top_district_amt_count['District']=top_district_amt_count['District'].str.replace('-',' ')
              top_district_amt_count['District']=top_district_amt_count['District'].str.title()

              sum_transaction_amt = top_district_amt_count.groupby('District',as_index=False)['Transaction_amount'].sum()
              sum_transaction_amt=sum_transaction_amt.sort_values(by='Transaction_amount',ascending=False).head(5).reset_index(drop=True)
              sum_transaction_amt['Transaction_amount']=sum_transaction_amt['Transaction_amount'].apply(format_indian_number)

              sum_transaction_count = top_district_amt_count.groupby('District',as_index=False)['Transaction_count'].sum()
              sum_transaction_count=sum_transaction_count.sort_values(by='Transaction_count',ascending=False).head(5).reset_index(drop=True)
              sum_transaction_count['Transaction_count']=sum_transaction_count['Transaction_count'].apply(format_indian_number)
              sum_transaction_count['Transaction_count']=sum_transaction_count['Transaction_count'].str.replace('₹',"")
              
              top_districts_amt=sum_transaction_amt
              top_districts_count=sum_transaction_count
              return top_districts_amt,top_districts_count
            top_districts_amt,top_districts_count=top_Districts_count_and_amount_query(year=year_key,quater=quaters[quater_key])
            
            def top_pincode_count_and_amount_query(year,quater):

              top_pincode_count_and_amount_query=f"""
              SELECT 
                pt.Pincode,
                top_pin_trans.Transaction_amount,
                top_pin_trans.Transaction_count
              FROM 
                toppincodetransactions as top_pin_trans,
                states AS st,
                years AS yt,
                quaters AS qt,
                pincodes AS pt
              WHERE
                st.State_id=top_pin_trans.State_id AND
                yt.Year_id=top_pin_trans.Year_id AND
                qt.Quater_id=top_pin_trans.Quater_id AND
                pt.Pincode_id=top_pin_trans.Pincode_id AND
                yt.Year = {year} AND
                qt.Quater = {quater};
              """

              top_pincode_amt_count=pd.read_sql(top_pincode_count_and_amount_query,con=engine)

              top_pincode_amt_count['Transaction_amount']=top_pincode_amt_count['Transaction_amount'].astype(float)

              sum_transaction_amt = top_pincode_amt_count.groupby('Pincode',as_index=False)['Transaction_amount'].sum()
              sum_transaction_amt=sum_transaction_amt.sort_values(by='Transaction_amount',ascending=False).head(5).reset_index(drop=True)
              sum_transaction_amt['Transaction_amount']=sum_transaction_amt['Transaction_amount'].apply(format_indian_number)

              sum_transaction_count = top_pincode_amt_count.groupby('Pincode',as_index=False)['Transaction_count'].sum()
              sum_transaction_count=sum_transaction_count.sort_values(by='Transaction_count',ascending=False).head(5).reset_index(drop=True)
              sum_transaction_count['Transaction_count']=sum_transaction_count['Transaction_count'].apply(format_indian_number)
              sum_transaction_count['Transaction_count']=sum_transaction_count['Transaction_count'].str.replace('₹',"")
              
              top_pincode_amt=sum_transaction_amt
              top_pincode_count=sum_transaction_count
              return top_pincode_amt,top_pincode_count
            top_pincode_amt,top_pincode_count=top_pincode_count_and_amount_query(year=year_key,quater=quaters[quater_key])

            st.markdown(
              """
              <style>
              .grey-background-col {
              background-color: #2C1942;
              padding: 10px;
              border-radius: 5px;
              height: 100%;
              }
              </style>
              """,
              unsafe_allow_html=True)
            
            st.markdown('<p style="color: white; font-size: 16px; font-family: Arial; margin: 5px 0;"><strong>Transaction Values on Diffrent Levels</strong></p>',unsafe_allow_html=True)
            top_tran_1,top_tran_2,top_tran_3=st.columns([33.33,33.33,33.33])
            st.markdown('<p style="color: white; font-size: 16px; font-family: Arial; margin: 5px 0;"><strong>Transactions on Diffrent Levels</strong></p>',unsafe_allow_html=True)
            
            with top_tran_1:
              top_tran_container_1=st.container(height=190,border=False)

              with top_tran_container_1:
                
                top_tran_1_content=(f"""
                  <p style="color: cyan; font-size: 21px; font-family: Arial; margin: 5px 0;"><strong>States</strong></p>
                  <div style="display: flex; justify-content: space-between; margin: 0;">
                    <p style="color: #FFFFFF; font-size: 14px; font-family: Times New Roman; margin: 0;">{top_states_amt['State'][0]}</p>
                    <p style="color: cyan; font-size: 17px; font-family: Times New Roman; margin: 0;">{top_states_amt['Transaction_amount'][0]}</p>
                  </div>
                  <div style="display: flex; justify-content: space-between; margin: 0;">
                    <p style="color: #FFFFFF; font-size: 14px; font-family: Times New Roman; margin: 0;">{top_states_amt['State'][1]}</p>
                    <p style="color: cyan; font-size: 17px; font-family: Times New Roman; margin: 0;">{top_states_amt['Transaction_amount'][1]}</p>
                  </div>
                  <div style="display: flex; justify-content: space-between; margin: 0;">
                    <p style="color: #FFFFFF; font-size: 14px; font-family: Times New Roman; margin: 0;">{top_states_amt['State'][2]}</p>
                    <p style="color: cyan; font-size: 17px; font-family: Times New Roman; margin: 0;">{top_states_amt['Transaction_amount'][2]}</p>
                  </div>
                  <div style="display: flex; justify-content: space-between; margin: 0;">
                    <p style="color: #FFFFFF; font-size: 14px; font-family: Times New Roman; margin: 0;">{top_states_amt['State'][3]}</p>
                    <p style="color: cyan; font-size: 17px; font-family: Times New Roman; margin: 0;">{top_states_amt['Transaction_amount'][3]}</p>
                  </div>
                  <div style="display: flex; justify-content: space-between; margin: 0;">
                    <p style="color: #FFFFFF; font-size: 14px; font-family: Times New Roman; margin: 0;">{top_states_amt['State'][4]}</p>
                    <p style="color: cyan; font-size: 17px; font-family: Times New Roman; margin: 0;">{top_states_amt['Transaction_amount'][4]}</p>
                  </div>
                  """)    
                top_tran_container_1.markdown(wrap_with_grey_background(top_tran_1_content), unsafe_allow_html=True)
            
            with top_tran_2:

              top_tran_container_2=st.container(height=190,border=False)
              with top_tran_container_2:

                top_tran_2_content=(f"""
                  <p style="color: cyan; font-size: 21px; font-family: Arial; margin: 5px 0;"><strong>Districts</strong></p>
                  <div style="display: flex; justify-content: space-between; margin: 0;">
                    <p style="color: #FFFFFF; font-size: 14px; font-family: Times New Roman; margin: 0;">{top_districts_amt['District'][0]}</p>
                    <p style="color: cyan; font-size: 17px; font-family: Times New Roman; margin: 0;">{top_districts_amt['Transaction_amount'][0]}</p>
                  </div>
                  <div style="display: flex; justify-content: space-between; margin: 0;">
                    <p style="color: #FFFFFF; font-size: 14px; font-family: Times New Roman; margin: 0;">{top_districts_amt['District'][1]}</p>
                    <p style="color: cyan; font-size: 17px; font-family: Times New Roman; margin: 0;">{top_districts_amt['Transaction_amount'][1]}</p>
                  </div>
                  <div style="display: flex; justify-content: space-between; margin: 0;">
                    <p style="color: #FFFFFF; font-size: 14px; font-family: Times New Roman; margin: 0;">{top_districts_amt['District'][2]}</p>
                    <p style="color: cyan; font-size: 17px; font-family: Times New Roman; margin: 0;">{top_districts_amt['Transaction_amount'][2]}</p>
                  </div>
                  <div style="display: flex; justify-content: space-between; margin: 0;">
                    <p style="color: #FFFFFF; font-size: 14px; font-family: Times New Roman; margin: 0;">{top_districts_amt['District'][3]}</p>
                    <p style="color: cyan; font-size: 17px; font-family: Times New Roman; margin: 0;">{top_districts_amt['Transaction_amount'][3]}</p>
                  </div>
                  <div style="display: flex; justify-content: space-between; margin: 0;">
                    <p style="color: #FFFFFF; font-size: 14px; font-family: Times New Roman; margin: 0;">{top_districts_amt['District'][4]}</p>
                    <p style="color: cyan; font-size: 17px; font-family: Times New Roman; margin: 0;">{top_districts_amt['Transaction_amount'][4]}</p>
                  </div>
                  """)
                top_tran_container_2.markdown(wrap_with_grey_background(top_tran_2_content), unsafe_allow_html=True)
            
            with top_tran_3:

              top_tran_container_3=st.container(height=190,border=False)
              with top_tran_container_3:

                top_tran_3_content=(f"""
                  <p style="color: cyan; font-size: 21px; font-family: Arial; margin: 5px 0;"><strong>Postal Codes</strong></p>
                  <div style="display: flex; justify-content: space-between; margin: 0;">
                    <p style="color: #FFFFFF; font-size: 14px; font-family: Times New Roman; margin: 0;">{top_pincode_amt['Pincode'][0]}</p>
                    <p style="color: cyan; font-size: 17px; font-family: Times New Roman; margin: 0;">{top_pincode_amt['Transaction_amount'][0]}</p>
                  </div>
                  <div style="display: flex; justify-content: space-between; margin: 0;">
                    <p style="color: #FFFFFF; font-size: 14px; font-family: Times New Roman; margin: 0;">{top_pincode_amt['Pincode'][1]}</p>
                    <p style="color: cyan; font-size: 17px; font-family: Times New Roman; margin: 0;">{top_pincode_amt['Transaction_amount'][1]}</p>
                  </div>
                  <div style="display: flex; justify-content: space-between; margin: 0;">
                    <p style="color: #FFFFFF; font-size: 14px; font-family: Times New Roman; margin: 0;">{top_pincode_amt['Pincode'][2]}</p>
                    <p style="color: cyan; font-size: 17px; font-family: Times New Roman; margin: 0;">{top_pincode_amt['Transaction_amount'][2]}</p>
                  </div>
                  <div style="display: flex; justify-content: space-between; margin: 0;">
                    <p style="color: #FFFFFF; font-size: 14px; font-family: Times New Roman; margin: 0;">{top_pincode_amt['Pincode'][3]}</p>
                    <p style="color: cyan; font-size: 17px; font-family: Times New Roman; margin: 0;">{top_pincode_amt['Transaction_amount'][3]}</p>
                  </div>
                  <div style="display: flex; justify-content: space-between; margin: 0;">
                    <p style="color: #FFFFFF; font-size: 14px; font-family: Times New Roman; margin: 0;">{top_pincode_amt['Pincode'][4]}</p>
                    <p style="color: cyan; font-size: 17px; font-family: Times New Roman; margin: 0;">{top_pincode_amt['Transaction_amount'][4]}</p>
                  </div>
                  """)
                top_tran_container_3.markdown(wrap_with_grey_background(top_tran_3_content), unsafe_allow_html=True)
            
            bottom_tran_1,bottom_tran_2,bottom_tran_3=st.columns([33.33,33.33,33.33])

            with bottom_tran_1:
            
              bottom_tran_container_1=st.container(height=190,border=False)
              with bottom_tran_container_1:
            
                bottom_tran_1_content=(f"""
                  <p style="color: cyan; font-size: 21px; font-family: Arial; margin: 5px 0;"><strong>States</strong></p>
                  <div style="display: flex; justify-content: space-between; margin: 0;">
                    <p style="color: #FFFFFF; font-size: 14px; font-family: Times New Roman; margin: 0;">{top_states_count['State'][0]}</p>
                    <p style="color: cyan; font-size: 17px; font-family: Times New Roman; margin: 0;">{top_states_count['Transaction_count'][0]}</p>
                  </div>
                  <div style="display: flex; justify-content: space-between; margin: 0;">
                    <p style="color: #FFFFFF; font-size: 14px; font-family: Times New Roman; margin: 0;">{top_states_count['State'][1]}</p>
                    <p style="color: cyan; font-size: 17px; font-family: Times New Roman; margin: 0;">{top_states_count['Transaction_count'][1]}</p>
                  </div>
                  <div style="display: flex; justify-content: space-between; margin: 0;">
                    <p style="color: #FFFFFF; font-size: 14px; font-family: Times New Roman; margin: 0;">{top_states_count['State'][2]}</p>
                    <p style="color: cyan; font-size: 17px; font-family: Times New Roman; margin: 0;">{top_states_count['Transaction_count'][2]}</p>
                  </div>
                  <div style="display: flex; justify-content: space-between; margin: 0;">
                    <p style="color: #FFFFFF; font-size: 14px; font-family: Times New Roman; margin: 0;">{top_states_count['State'][3]}</p>
                    <p style="color: cyan; font-size: 17px; font-family: Times New Roman; margin: 0;">{top_states_count['Transaction_count'][3]}</p>
                  </div>
                  <div style="display: flex; justify-content: space-between; margin: 0;">
                    <p style="color: #FFFFFF; font-size: 14px; font-family: Times New Roman; margin: 0;">{top_states_count['State'][4]}</p>
                    <p style="color: cyan; font-size: 17px; font-family: Times New Roman; margin: 0;">{top_states_count['Transaction_count'][4]}</p>
                  </div>
                  """)
                bottom_tran_container_1.markdown(wrap_with_grey_background(bottom_tran_1_content), unsafe_allow_html=True)
            
            with bottom_tran_2:

              bottom_tran_container_2=st.container(height=190,border=False)
              with bottom_tran_container_2:
            
                bottom_tran_2_content=(f"""
                  <p style="color: cyan; font-size: 21px; font-family: Arial; margin: 5px 0;"><strong>Districts</strong></p>
                  <div style="display: flex; justify-content: space-between; margin: 0;">
                    <p style="color: #FFFFFF; font-size: 14px; font-family: Times New Roman; margin: 0;">{top_districts_count['District'][0]}</p>
                    <p style="color: cyan; font-size: 17px; font-family: Times New Roman; margin: 0;">{top_districts_count['Transaction_count'][0]}</p>
                  </div>
                  <div style="display: flex; justify-content: space-between; margin: 0;">
                    <p style="color: #FFFFFF; font-size: 14px; font-family: Times New Roman; margin: 0;">{top_districts_count['District'][1]}</p>
                    <p style="color: cyan; font-size: 17px; font-family: Times New Roman; margin: 0;">{top_districts_count['Transaction_count'][1]}</p>
                  </div>
                  <div style="display: flex; justify-content: space-between; margin: 0;">
                    <p style="color: #FFFFFF; font-size: 14px; font-family: Times New Roman; margin: 0;">{top_districts_count['District'][2]}</p>
                    <p style="color: cyan; font-size: 17px; font-family: Times New Roman; margin: 0;">{top_districts_count['Transaction_count'][2]}</p>
                  </div>
                  <div style="display: flex; justify-content: space-between; margin: 0;">
                    <p style="color: #FFFFFF; font-size: 14px; font-family: Times New Roman; margin: 0;">{top_districts_count['District'][3]}</p>
                    <p style="color: cyan; font-size: 17px; font-family: Times New Roman; margin: 0;">{top_districts_count['Transaction_count'][3]}</p>
                  </div>
                  <div style="display: flex; justify-content: space-between; margin: 0;">
                    <p style="color: #FFFFFF; font-size: 14px; font-family: Times New Roman; margin: 0;">{top_districts_count['District'][4]}</p>
                    <p style="color: cyan; font-size: 17px; font-family: Times New Roman; margin: 0;">{top_districts_count['Transaction_count'][4]}</p>
                  </div>
                  """)
                bottom_tran_container_2.markdown(wrap_with_grey_background(bottom_tran_2_content), unsafe_allow_html=True)
            
            with bottom_tran_3:
            
              bottom_tran_container_3=st.container(height=190,border=False)
              with bottom_tran_container_3:
            
                bottom_tran_3_content=(f"""
                  <p style="color: cyan; font-size: 21px; font-family: Arial; margin: 5px 0;"><strong>Postal Codes</strong></p>
                  <div style="display: flex; justify-content: space-between; margin: 0;">
                    <p style="color: #FFFFFF; font-size: 14px; font-family: Times New Roman; margin: 0;">{top_pincode_count['Pincode'][0]}</p>
                    <p style="color: cyan; font-size: 17px; font-family: Times New Roman; margin: 0;">{top_pincode_count['Transaction_count'][0]}</p>
                  </div>
                  <div style="display: flex; justify-content: space-between; margin: 0;">
                    <p style="color: #FFFFFF; font-size: 14px; font-family: Times New Roman; margin: 0;">{top_pincode_count['Pincode'][1]}</p>
                    <p style="color: cyan; font-size: 17px; font-family: Times New Roman; margin: 0;">{top_pincode_count['Transaction_count'][1]}</p>
                  </div>
                  <div style="display: flex; justify-content: space-between; margin: 0;">
                    <p style="color: #FFFFFF; font-size: 14px; font-family: Times New Roman; margin: 0;">{top_pincode_count['Pincode'][2]}</p>
                    <p style="color: cyan; font-size: 17px; font-family: Times New Roman; margin: 0;">{top_pincode_count['Transaction_count'][2]}</p>
                  </div>
                  <div style="display: flex; justify-content: space-between; margin: 0;">
                    <p style="color: #FFFFFF; font-size: 14px; font-family: Times New Roman; margin: 0;">{top_pincode_count['Pincode'][3]}</p>
                    <p style="color: cyan; font-size: 17px; font-family: Times New Roman; margin: 0;">{top_pincode_count['Transaction_count'][3]}</p>
                  </div>
                  <div style="display: flex; justify-content: space-between; margin: 0;">
                    <p style="color: #FFFFFF; font-size: 14px; font-family: Times New Roman; margin: 0;">{top_pincode_count['Pincode'][4]}</p>
                    <p style="color: cyan; font-size: 17px; font-family: Times New Roman; margin: 0;">{top_pincode_count['Transaction_count'][4]}</p>
                  </div>
                  """)
                bottom_tran_container_3.markdown(wrap_with_grey_background(bottom_tran_3_content), unsafe_allow_html=True)      
        else:
          print(f'Unexpected selection')
      
      elif option == '**Insurance**':

        with st.sidebar:
        
          custom_css_remove_spaces_radio_button = """
            <style>
            [data-testid="stRadio"] label {
            padding: 0 !important;
            margin: 0 !important;
            }
            </style>
            """
          st.markdown(custom_css_remove_spaces_radio_button,unsafe_allow_html=True)
          page_option=st.radio("**Analysis Page**",['**Map Analysis**','**Top 5 Analysis**'])

        custom_css_label_color = """
          <style>
          /* Targeting the label of st.selectbox */
          div[data-testid="stSelectbox"] label {
          color: white !important; /* Set text color to white */
          padding: 0 !important;
          margin: 0 !important;
          }
          </style>
          """
        st.markdown(custom_css_label_color, unsafe_allow_html=True)

        if page_option=='**Map Analysis**':

          years=[2020,2021,2022,2023,2024]
          quaters={'Q1 (Jan-Mar)':1,'Q2 (Apr-Jun)':2,'Q3 (Jul-Sep)':3,'Q4 (Oct-Dec)':4}
          quaters_2024={'Q1 (Jan-Mar)':1}
          quaters_2020={'Q2 (Apr-Jun)':2,'Q3 (Jul-Sep)':3,'Q4 (Oct-Dec)':4}

          col1, col2,col3,col4 = st.columns([0.28,0.1,0.35,0.95])
          with col1:
            year_key = st.selectbox("Select Year",(years))

          with col3:
            if  year_key == 2020:
              quater_key = st.selectbox("Select Quater",list(quaters_2020.keys()))
            elif year_key == 2024:
              quater_key = st.selectbox("Select Quater",list(quaters_2024.keys()))
            else:
              quater_key = st.selectbox("Select Quater",list(quaters.keys()))
              
          

          query=f"""
          SELECT 
            st.State,
            yt.Year,
            qt.Quater, 
            tt.Transaction_type, 
            agg_ins_trans.Transaction_amount, 
            agg_ins_trans.Transaction_count
          FROM 
            aggregatedinsurancetransactions as agg_ins_trans,
            states AS st,
            years AS yt,
            quaters AS qt,
            transaction_types AS tt
          WHERE
            st.State_id=agg_ins_trans.State_id AND
            yt.Year_id=agg_ins_trans.Year_id AND
            qt.Quater_id=agg_ins_trans.Quater_id AND
            tt.Transaction_type_id=agg_ins_trans.Transaction_type_id AND
            yt.Year = {year_key} AND
            qt.Quater = {quaters[quater_key]};
            """
          
          Agg_trans = pd.read_sql(query,con=engine)
          Agg_trans['Transaction_amount']=Agg_trans['Transaction_amount'].astype(float)
          stati_box_count=format_indian_number_for_count(value=Agg_trans['Transaction_count'].sum())
          stati_box_amount=format_indian_number(value=Agg_trans['Transaction_amount'].sum())
          stati_box_avg_amt=format_indian_number_for_count(value=(Agg_trans['Transaction_amount'].sum()/Agg_trans['Transaction_count'].sum()))
          stati_box_avg_amt='₹ '+str(stati_box_avg_amt)

          pan_area_1,pan_area_2=st.columns([7,3])
          with pan_area_1:
            
            def result_map(df):
              # Set locale to Indian format
              locale.setlocale(locale.LC_ALL, 'en_IN')

              # Load transaction data
              Agg_trans = df
              GrouBy_data = Agg_trans.groupby('State').agg({'Transaction_amount': 'sum','Transaction_count': 'sum'})
              data_df=GrouBy_data.reset_index()
              data_df['Average_transaction_amount'] = data_df['Transaction_amount'] / data_df['Transaction_count']

              # Average transaction count 
              data_df['Average_transaction_count'] = Agg_trans.groupby('State')['Transaction_count'].sum().values
              data_df['State'] = data_df['State'].str.replace('-', ' ').str.title()

              # Load GeoJSON data using GeoPandas
              geo_data = gpd.read_file(" Replace with json file ") #///////////////Replace with json file location///////////

              # Dissolve the GeoDataFrame by the 'State' column to combine the geometries
              geo_data = geo_data.dissolve(by='State', as_index=False)
              geo_data['State'] = geo_data['State'].str.replace('-', ' ').str.title()

              # Convert the GeoDataFrame to a GeoJSON 
              geo_json = json.loads(geo_data.to_json())

              # Merge dataframe with the GeoDataFrame
              df = geo_data.merge(data_df, how='left')
              
              #filling up na value with 0, while merging some state end up having null values
              df.fillna(0,inplace=True)

              # formatting transaction amounts for hover data
              df['Transaction_amount_formatted'] = df['Transaction_amount'].apply(format_indian_number)
              df['Average_transaction_amount_formatted'] = df['Average_transaction_amount'].apply(format_indian_number)
              df['Average_transaction_count_formatted']=df['Average_transaction_count'].apply(format_indian_number_for_count)

              # Creating choropleth map 
              fig = px.choropleth(
                df,
                geojson=geo_json,
                featureidkey="properties.State",
                locations="State",
                color="Transaction_amount",
                color_continuous_scale=["#3399FF","#FFFFFF","#FF0000"],
                projection="mercator",
                labels={'Transaction_amount': 'Transaction Amount'},
                hover_data={
                'State': True,
                'Transaction_amount_formatted': True,
                'Average_transaction_amount_formatted': True,
                'Average_transaction_count': True,
                'Transaction_amount': False,  
                'Average_transaction_amount': False  
                }
              )

              # Update the map to fit the GeoJSON data
              fig.update_geos(fitbounds="locations", visible=False)

              # Creating tick values based on range
              min_val = df['Transaction_amount'].min()
              max_val = df['Transaction_amount'].max()
              num_ticks = 8  # Number of ticks
              tickvals = np.linspace(min_val, max_val, num_ticks)

              # Formatting color bar tick values to Indian numeric system
              ticktext = [format_indian_number(tick) for tick in tickvals]

              fig.update_layout(
                margin=dict(l=0, r=0, t=0, b=0),
                geo=dict(
                bgcolor='#210D38',
                center=dict(lat=20.5937, lon=78.9629),  # Center of India
                projection_scale=1.2,  
                lataxis_showgrid=False,
                lonaxis_showgrid=False,
                ),
                paper_bgcolor='rgba(0,0,0,0)',
                
                coloraxis_colorbar=dict(
                title='Premium Value',
                titleside="top",
                x=1,
                y=0.5,
                title_font_color="white",
                tickvals=tickvals,
                ticktext=ticktext,
                tickfont=dict(color="white"),
                outlinecolor="white",
                outlinewidth=0.01
                )
              )

              # Custom hover template
              fig.update_traces(
                hovertemplate=(
                '<b style="color:white;font-size:10px">State</b><br>'
                '<b style="color:cyan;font-size:16px">%{customdata[0]}</b><br>' +
                '<br>'+
                '<b style="color:white;font-size:10px">Insurance Policies (Nos)</b><br>'+
                '<b style="color:cyan;font-size:16px">%{customdata[3]}</b><br>'+
                '<br>'+
                '<b style="color:white;font-size:10px">Total Premium Value</b><br>'+
                '<b style="color:cyan;font-size:16px">%{customdata[1]}</b><br>'+
                '<br>'+
                '<b style="color:white;font-size:10px">Avg Premium Value</b><br>'+
                '<b style="color:cyan;font-size:16px">%{customdata[2]}</b><br>'
                ),
                customdata=df[['State', 'Transaction_amount_formatted', 'Average_transaction_amount_formatted', 'Average_transaction_count_formatted']]
              )

              # Add CSS styling for hover data label
              fig.update_layout(
              hoverlabel=dict(
              bgcolor="#444444",  
              font_size=12,  
              font_family="Arial",)
              )

              # Show map
              st.write(fig)
            result_map(df=Agg_trans)

          with pan_area_2:

            box_container=st.container(height=400,border=False)
            with box_container:
              st.markdown(
              """
              <style>
              .grey-background-col {
              background-color: #2C1942;
              padding: 10px;
              border-radius: 5px;
              height: 100%;
              }
              </style>
              """,unsafe_allow_html=True)

              content=f"""
                <br>
                <p style="color: cyan; font-size: 16px; font-family: Arial; margin: 5px 0;"><strong>Insurance</strong></p>
                <p style="color: #FFFFFF; font-size: 13px; font-family: Times New Roman; margin: 10px 0;">All Over India Insurance Policies Purchased (Nos)</p>
                <p style="color: cyan; font-size: 25px; font-family: 'Arial Black'; margin: 5px 0;">{stati_box_count}</p>
                <p style="color: #FFFFFF; font-size: 13px; font-family: Times New Roman; margin: 5px 0;">Total Premium Value</p>
                <p style="color: cyan; font-size: 20px; font-family: Arial; margin: 5px 0;"><strong>{stati_box_amount}</strong></p>
                <p style="color: #FFFFFF; font-size: 13px; font-family: Times New Roman; margin: 5px 0;">Average Premium Value</p>
                <p style="color: cyan; font-size: 20px; font-family: 'Arial'; margin: 1px 0;"><strong>{stati_box_avg_amt}</strong></p>
                <br>
                """
              box_container.markdown(wrap_with_grey_background(content), unsafe_allow_html=True)
        
        elif page_option == '**Top 5 Analysis**':

          top_analysis_container=st.container(border=False)
          with top_analysis_container:

            years=[2020,2021,2022,2023,2024]
            quaters={'Q1 (Jan-Mar)':1,'Q2 (Apr-Jun)':2,'Q3 (Jul-Sep)':3,'Q4 (Oct-Dec)':4}
            quaters_2024={'Q1 (Jan-Mar)':1}
            quaters_2020={'Q2 (Apr-Jun)':2,'Q3 (Jul-Sep)':3,'Q4 (Oct-Dec)':4}

            col1, col2,col3,col4 = st.columns([11,27,17,25])
            with col1:
              year_key = st.selectbox("Select Year",(years))

            with col3:
              if  year_key == 2020:
                quater_key = st.selectbox("Select Quater",list(quaters_2020.keys()))
              elif year_key == 2024:
                quater_key = st.selectbox("Select Quater",list(quaters_2024.keys()))
              else:
                quater_key = st.selectbox("Select Quater",list(quaters.keys()))

            def top_states_count_and_amount_query(year,quater):

              top_states_count_and_amount_query=f"""
              SELECT 
                st.State,
                agg_ins_trans.Transaction_amount,
                agg_ins_trans.Transaction_count
              FROM 
                aggregatedinsurancetransactions as agg_ins_trans,
                states AS st,
                years AS yt,
                quaters AS qt,
                transaction_types AS tt
              WHERE
                st.State_id=agg_ins_trans.State_id AND
                yt.Year_id=agg_ins_trans.Year_id AND
                qt.Quater_id=agg_ins_trans.Quater_id AND
                tt.Transaction_type_id=agg_ins_trans.Transaction_type_id AND
                yt.Year = {year} AND
                qt.Quater = {quater};
              """

              top_states_amt_count=pd.read_sql(top_states_count_and_amount_query,con=engine)
              top_states_amt_count['Transaction_amount']=top_states_amt_count['Transaction_amount'].astype(float)
              top_states_amt_count['State']=top_states_amt_count['State'].str.replace('-',' ')
              top_states_amt_count['State']=top_states_amt_count['State'].str.title()

              sum_transaction_amt = top_states_amt_count.groupby('State',as_index=False)['Transaction_amount'].sum()
              sum_transaction_amt=sum_transaction_amt.sort_values(by='Transaction_amount',ascending=False).head(5).reset_index(drop=True)
              sum_transaction_amt['Transaction_amount']=sum_transaction_amt['Transaction_amount'].apply(format_indian_number)

              sum_transaction_count = top_states_amt_count.groupby('State',as_index=False)['Transaction_count'].sum()
              sum_transaction_count=sum_transaction_count.sort_values(by='Transaction_count',ascending=False).head(5).reset_index(drop=True)
              sum_transaction_count['Transaction_count']=sum_transaction_count['Transaction_count'].apply(format_indian_number)
              sum_transaction_count['Transaction_count']=sum_transaction_count['Transaction_count'].str.replace('₹',"")
              
              top_states_amt=sum_transaction_amt
              top_states_count=sum_transaction_count
              return top_states_amt,top_states_count
            top_states_amt,top_states_count=top_states_count_and_amount_query(year=year_key,quater=quaters[quater_key])
            
            def top_Districts_count_and_amount_query(year,quater):

              top_Districts_count_and_amount_query=f"""
              SELECT 
                dt.District,
                top_district_ins_trans.Transaction_amount,
                top_district_ins_trans.Transaction_count
              FROM 
                topdistrictinsurancetransactions as top_district_ins_trans,
                states AS st,
                years AS yt,
                quaters AS qt,
                districts AS dt
              WHERE
                st.State_id=top_district_ins_trans.State_id AND
                yt.Year_id=top_district_ins_trans.Year_id AND
                qt.Quater_id=top_district_ins_trans.Quater_id AND
                dt.District_id=top_district_ins_trans.District_id AND
                yt.Year = {year} AND
                qt.Quater = {quater};
              """

              top_district_amt_count=pd.read_sql(top_Districts_count_and_amount_query,con=engine)

              top_district_amt_count['Transaction_amount']=top_district_amt_count['Transaction_amount'].astype(float)
              top_district_amt_count['District']=top_district_amt_count['District'].str.replace('-',' ')
              top_district_amt_count['District']=top_district_amt_count['District'].str.title()

              sum_transaction_amt = top_district_amt_count.groupby('District',as_index=False)['Transaction_amount'].sum()
              sum_transaction_amt=sum_transaction_amt.sort_values(by='Transaction_amount',ascending=False).head(5).reset_index(drop=True)
              sum_transaction_amt['Transaction_amount']=sum_transaction_amt['Transaction_amount'].apply(format_indian_number)

              sum_transaction_count = top_district_amt_count.groupby('District',as_index=False)['Transaction_count'].sum()
              sum_transaction_count=sum_transaction_count.sort_values(by='Transaction_count',ascending=False).head(5).reset_index(drop=True)
              sum_transaction_count['Transaction_count']=sum_transaction_count['Transaction_count'].apply(format_indian_number)
              sum_transaction_count['Transaction_count']=sum_transaction_count['Transaction_count'].str.replace('₹',"")
              
              top_districts_amt=sum_transaction_amt
              top_districts_count=sum_transaction_count
              return top_districts_amt,top_districts_count
            top_districts_amt,top_districts_count=top_Districts_count_and_amount_query(year=year_key,quater=quaters[quater_key])
            
            def top_pincode_count_and_amount_query(year,quater):

              top_pincode_count_and_amount_query=f"""
              SELECT 
                pt.Pincode,
                top_pin_ins_trans.Transaction_amount,
                top_pin_ins_trans.Transaction_count
              FROM 
                toppincodeinsurancetransactions as top_pin_ins_trans,
                states AS st,
                years AS yt,
                quaters AS qt,
                pincodes AS pt
              WHERE
                st.State_id=top_pin_ins_trans.State_id AND
                yt.Year_id=top_pin_ins_trans.Year_id AND
                qt.Quater_id=top_pin_ins_trans.Quater_id AND
                pt.Pincode_id=top_pin_ins_trans.Pincode_id AND
                yt.Year = {year} AND
                qt.Quater = {quater};
              """

              top_pincode_amt_count=pd.read_sql(top_pincode_count_and_amount_query,con=engine)

              top_pincode_amt_count['Transaction_amount']=top_pincode_amt_count['Transaction_amount'].astype(float)

              sum_transaction_amt = top_pincode_amt_count.groupby('Pincode',as_index=False)['Transaction_amount'].sum()
              sum_transaction_amt=sum_transaction_amt.sort_values(by='Transaction_amount',ascending=False).head(5).reset_index(drop=True)
              sum_transaction_amt['Transaction_amount']=sum_transaction_amt['Transaction_amount'].apply(format_indian_number)

              sum_transaction_count = top_pincode_amt_count.groupby('Pincode',as_index=False)['Transaction_count'].sum()
              sum_transaction_count=sum_transaction_count.sort_values(by='Transaction_count',ascending=False).head(5).reset_index(drop=True)
              sum_transaction_count['Transaction_count']=sum_transaction_count['Transaction_count'].apply(format_indian_number)
              sum_transaction_count['Transaction_count']=sum_transaction_count['Transaction_count'].str.replace('₹',"")
              
              top_pincode_amt=sum_transaction_amt
              top_pincode_count=sum_transaction_count
              return top_pincode_amt,top_pincode_count
            top_pincode_amt,top_pincode_count=top_pincode_count_and_amount_query(year=year_key,quater=quaters[quater_key])

            st.markdown(
              """
              <style>
              .grey-background-col {
              background-color: #2C1942;
              padding: 10px;
              border-radius: 5px;
              height: 100%;
              }
              </style>
              """,
              unsafe_allow_html=True)
            st.markdown('<p style="color: white; font-size: 16px; font-family: Arial; margin: 5px 0;"><strong>Premium Values in Diffrent Levels</strong></p>',unsafe_allow_html=True)
            
            top_tran_1,top_tran_2,top_tran_3=st.columns([33.33,33.33,33.33])
            st.markdown('<p style="color: white; font-size: 16px; font-family: Arial; margin: 5px 0;"><strong>Purchased Insurance (Nos) in Diffrent Levels</strong></p>',unsafe_allow_html=True)
            
            with top_tran_1:

              top_tran_container_1=st.container(height=190,border=False)
              with top_tran_container_1:

                top_tran_1_content=(f"""
                  <p style="color: cyan; font-size: 21px; font-family: Arial; margin: 5px 0;"><strong>States</strong></p>
                  <div style="display: flex; justify-content: space-between; margin: 0;">
                    <p style="color: #FFFFFF; font-size: 14px; font-family: Times New Roman; margin: 0;">{top_states_amt['State'][0]}</p>
                    <p style="color: cyan; font-size: 17px; font-family: Times New Roman; margin: 0;">{top_states_amt['Transaction_amount'][0]}</p>
                  </div>
                  <div style="display: flex; justify-content: space-between; margin: 0;">
                    <p style="color: #FFFFFF; font-size: 14px; font-family: Times New Roman; margin: 0;">{top_states_amt['State'][1]}</p>
                    <p style="color: cyan; font-size: 17px; font-family: Times New Roman; margin: 0;">{top_states_amt['Transaction_amount'][1]}</p>
                  </div>
                  <div style="display: flex; justify-content: space-between; margin: 0;">
                    <p style="color: #FFFFFF; font-size: 14px; font-family: Times New Roman; margin: 0;">{top_states_amt['State'][2]}</p>
                    <p style="color: cyan; font-size: 17px; font-family: Times New Roman; margin: 0;">{top_states_amt['Transaction_amount'][2]}</p>
                  </div>
                  <div style="display: flex; justify-content: space-between; margin: 0;">
                    <p style="color: #FFFFFF; font-size: 14px; font-family: Times New Roman; margin: 0;">{top_states_amt['State'][3]}</p>
                    <p style="color: cyan; font-size: 17px; font-family: Times New Roman; margin: 0;">{top_states_amt['Transaction_amount'][3]}</p>
                  </div>
                  <div style="display: flex; justify-content: space-between; margin: 0;">
                    <p style="color: #FFFFFF; font-size: 14px; font-family: Times New Roman; margin: 0;">{top_states_amt['State'][4]}</p>
                    <p style="color: cyan; font-size: 17px; font-family: Times New Roman; margin: 0;">{top_states_amt['Transaction_amount'][4]}</p>
                  </div>
                  """)    
                top_tran_container_1.markdown(wrap_with_grey_background(top_tran_1_content), unsafe_allow_html=True)
            
            with top_tran_2:

              top_tran_container_2=st.container(height=190,border=False)
              with top_tran_container_2:
              
                top_tran_2_content=(f"""
                  <p style="color: cyan; font-size: 21px; font-family: Arial; margin: 5px 0;"><strong>Districts</strong></p>
                  <div style="display: flex; justify-content: space-between; margin: 0;">
                    <p style="color: #FFFFFF; font-size: 14px; font-family: Times New Roman; margin: 0;">{top_districts_amt['District'][0]}</p>
                    <p style="color: cyan; font-size: 17px; font-family: Times New Roman; margin: 0;">{top_districts_amt['Transaction_amount'][0]}</p>
                  </div>
                  <div style="display: flex; justify-content: space-between; margin: 0;">
                    <p style="color: #FFFFFF; font-size: 14px; font-family: Times New Roman; margin: 0;">{top_districts_amt['District'][1]}</p>
                    <p style="color: cyan; font-size: 17px; font-family: Times New Roman; margin: 0;">{top_districts_amt['Transaction_amount'][1]}</p>
                  </div>
                  <div style="display: flex; justify-content: space-between; margin: 0;">
                    <p style="color: #FFFFFF; font-size: 14px; font-family: Times New Roman; margin: 0;">{top_districts_amt['District'][2]}</p>
                    <p style="color: cyan; font-size: 17px; font-family: Times New Roman; margin: 0;">{top_districts_amt['Transaction_amount'][2]}</p>
                  </div>
                  <div style="display: flex; justify-content: space-between; margin: 0;">
                    <p style="color: #FFFFFF; font-size: 14px; font-family: Times New Roman; margin: 0;">{top_districts_amt['District'][3]}</p>
                    <p style="color: cyan; font-size: 17px; font-family: Times New Roman; margin: 0;">{top_districts_amt['Transaction_amount'][3]}</p>
                  </div>
                  <div style="display: flex; justify-content: space-between; margin: 0;">
                    <p style="color: #FFFFFF; font-size: 14px; font-family: Times New Roman; margin: 0;">{top_districts_amt['District'][4]}</p>
                    <p style="color: cyan; font-size: 17px; font-family: Times New Roman; margin: 0;">{top_districts_amt['Transaction_amount'][4]}</p>
                  </div>
                  """)
                top_tran_container_2.markdown(wrap_with_grey_background(top_tran_2_content), unsafe_allow_html=True)
            
            with top_tran_3:
            
              top_tran_container_3=st.container(height=190,border=False)
              with top_tran_container_3:
            
                top_tran_3_content=(f"""
                  <p style="color: cyan; font-size: 21px; font-family: Arial; margin: 5px 0;"><strong>Postal Codes</strong></p>
                  <div style="display: flex; justify-content: space-between; margin: 0;">
                    <p style="color: #FFFFFF; font-size: 14px; font-family: Times New Roman; margin: 0;">{top_pincode_amt['Pincode'][0]}</p>
                    <p style="color: cyan; font-size: 17px; font-family: Times New Roman; margin: 0;">{top_pincode_amt['Transaction_amount'][0]}</p>
                  </div>
                  <div style="display: flex; justify-content: space-between; margin: 0;">
                    <p style="color: #FFFFFF; font-size: 14px; font-family: Times New Roman; margin: 0;">{top_pincode_amt['Pincode'][1]}</p>
                    <p style="color: cyan; font-size: 17px; font-family: Times New Roman; margin: 0;">{top_pincode_amt['Transaction_amount'][1]}</p>
                  </div>
                  <div style="display: flex; justify-content: space-between; margin: 0;">
                    <p style="color: #FFFFFF; font-size: 14px; font-family: Times New Roman; margin: 0;">{top_pincode_amt['Pincode'][2]}</p>
                    <p style="color: cyan; font-size: 17px; font-family: Times New Roman; margin: 0;">{top_pincode_amt['Transaction_amount'][2]}</p>
                  </div>
                  <div style="display: flex; justify-content: space-between; margin: 0;">
                    <p style="color: #FFFFFF; font-size: 14px; font-family: Times New Roman; margin: 0;">{top_pincode_amt['Pincode'][3]}</p>
                    <p style="color: cyan; font-size: 17px; font-family: Times New Roman; margin: 0;">{top_pincode_amt['Transaction_amount'][3]}</p>
                  </div>
                  <div style="display: flex; justify-content: space-between; margin: 0;">
                    <p style="color: #FFFFFF; font-size: 14px; font-family: Times New Roman; margin: 0;">{top_pincode_amt['Pincode'][4]}</p>
                    <p style="color: cyan; font-size: 17px; font-family: Times New Roman; margin: 0;">{top_pincode_amt['Transaction_amount'][4]}</p>
                  </div>
                  """)
                top_tran_container_3.markdown(wrap_with_grey_background(top_tran_3_content), unsafe_allow_html=True)
            
            bottom_tran_1,bottom_tran_2,bottom_tran_3=st.columns([33.33,33.33,33.33])
            
            with bottom_tran_1:
            
              bottom_tran_container_1=st.container(height=190,border=False)
              with bottom_tran_container_1:
            
                bottom_tran_1_content=(f"""
                  <p style="color: cyan; font-size: 21px; font-family: Arial; margin: 5px 0;"><strong>States</strong></p>
                  <div style="display: flex; justify-content: space-between; margin: 0;">
                    <p style="color: #FFFFFF; font-size: 14px; font-family: Times New Roman; margin: 0;">{top_states_count['State'][0]}</p>
                    <p style="color: cyan; font-size: 17px; font-family: Times New Roman; margin: 0;">{top_states_count['Transaction_count'][0]}</p>
                  </div>
                  <div style="display: flex; justify-content: space-between; margin: 0;">
                    <p style="color: #FFFFFF; font-size: 14px; font-family: Times New Roman; margin: 0;">{top_states_count['State'][1]}</p>
                    <p style="color: cyan; font-size: 17px; font-family: Times New Roman; margin: 0;">{top_states_count['Transaction_count'][1]}</p>
                  </div>
                  <div style="display: flex; justify-content: space-between; margin: 0;">
                    <p style="color: #FFFFFF; font-size: 14px; font-family: Times New Roman; margin: 0;">{top_states_count['State'][2]}</p>
                    <p style="color: cyan; font-size: 17px; font-family: Times New Roman; margin: 0;">{top_states_count['Transaction_count'][2]}</p>
                  </div>
                  <div style="display: flex; justify-content: space-between; margin: 0;">
                    <p style="color: #FFFFFF; font-size: 14px; font-family: Times New Roman; margin: 0;">{top_states_count['State'][3]}</p>
                    <p style="color: cyan; font-size: 17px; font-family: Times New Roman; margin: 0;">{top_states_count['Transaction_count'][3]}</p>
                  </div>
                  <div style="display: flex; justify-content: space-between; margin: 0;">
                    <p style="color: #FFFFFF; font-size: 14px; font-family: Times New Roman; margin: 0;">{top_states_count['State'][4]}</p>
                    <p style="color: cyan; font-size: 17px; font-family: Times New Roman; margin: 0;">{top_states_count['Transaction_count'][4]}</p>
                  </div>
                  """)
                bottom_tran_container_1.markdown(wrap_with_grey_background(bottom_tran_1_content), unsafe_allow_html=True)
            
            with bottom_tran_2:

              bottom_tran_container_2=st.container(height=190,border=False)
              with bottom_tran_container_2:
            
                bottom_tran_2_content=(f"""
                  <p style="color: cyan; font-size: 21px; font-family: Arial; margin: 5px 0;"><strong>Districts</strong></p>
                  <div style="display: flex; justify-content: space-between; margin: 0;">
                    <p style="color: #FFFFFF; font-size: 14px; font-family: Times New Roman; margin: 0;">{top_districts_count['District'][0]}</p>
                    <p style="color: cyan; font-size: 17px; font-family: Times New Roman; margin: 0;">{top_districts_count['Transaction_count'][0]}</p>
                  </div>
                  <div style="display: flex; justify-content: space-between; margin: 0;">
                    <p style="color: #FFFFFF; font-size: 14px; font-family: Times New Roman; margin: 0;">{top_districts_count['District'][1]}</p>
                    <p style="color: cyan; font-size: 17px; font-family: Times New Roman; margin: 0;">{top_districts_count['Transaction_count'][1]}</p>
                  </div>
                  <div style="display: flex; justify-content: space-between; margin: 0;">
                    <p style="color: #FFFFFF; font-size: 14px; font-family: Times New Roman; margin: 0;">{top_districts_count['District'][2]}</p>
                    <p style="color: cyan; font-size: 17px; font-family: Times New Roman; margin: 0;">{top_districts_count['Transaction_count'][2]}</p>
                  </div>
                  <div style="display: flex; justify-content: space-between; margin: 0;">
                    <p style="color: #FFFFFF; font-size: 14px; font-family: Times New Roman; margin: 0;">{top_districts_count['District'][3]}</p>
                    <p style="color: cyan; font-size: 17px; font-family: Times New Roman; margin: 0;">{top_districts_count['Transaction_count'][3]}</p>
                  </div>
                  <div style="display: flex; justify-content: space-between; margin: 0;">
                    <p style="color: #FFFFFF; font-size: 14px; font-family: Times New Roman; margin: 0;">{top_districts_count['District'][4]}</p>
                    <p style="color: cyan; font-size: 17px; font-family: Times New Roman; margin: 0;">{top_districts_count['Transaction_count'][4]}</p>
                  </div>
                  """)
                bottom_tran_container_2.markdown(wrap_with_grey_background(bottom_tran_2_content), unsafe_allow_html=True)
            
            with bottom_tran_3:
            
              bottom_tran_container_3=st.container(height=190,border=False)
              with bottom_tran_container_3:
            
                bottom_tran_3_content=(f"""
                  <p style="color: cyan; font-size: 21px; font-family: Arial; margin: 5px 0;"><strong>Postal Codes</strong></p>
                  <div style="display: flex; justify-content: space-between; margin: 0;">
                    <p style="color: #FFFFFF; font-size: 14px; font-family: Times New Roman; margin: 0;">{top_pincode_count['Pincode'][0]}</p>
                    <p style="color: cyan; font-size: 17px; font-family: Times New Roman; margin: 0;">{top_pincode_count['Transaction_count'][0]}</p>
                  </div>
                  <div style="display: flex; justify-content: space-between; margin: 0;">
                    <p style="color: #FFFFFF; font-size: 14px; font-family: Times New Roman; margin: 0;">{top_pincode_count['Pincode'][1]}</p>
                    <p style="color: cyan; font-size: 17px; font-family: Times New Roman; margin: 0;">{top_pincode_count['Transaction_count'][1]}</p>
                  </div>
                  <div style="display: flex; justify-content: space-between; margin: 0;">
                    <p style="color: #FFFFFF; font-size: 14px; font-family: Times New Roman; margin: 0;">{top_pincode_count['Pincode'][2]}</p>
                    <p style="color: cyan; font-size: 17px; font-family: Times New Roman; margin: 0;">{top_pincode_count['Transaction_count'][2]}</p>
                  </div>
                  <div style="display: flex; justify-content: space-between; margin: 0;">
                    <p style="color: #FFFFFF; font-size: 14px; font-family: Times New Roman; margin: 0;">{top_pincode_count['Pincode'][3]}</p>
                    <p style="color: cyan; font-size: 17px; font-family: Times New Roman; margin: 0;">{top_pincode_count['Transaction_count'][3]}</p>
                  </div>
                  <div style="display: flex; justify-content: space-between; margin: 0;">
                    <p style="color: #FFFFFF; font-size: 14px; font-family: Times New Roman; margin: 0;">{top_pincode_count['Pincode'][4]}</p>
                    <p style="color: cyan; font-size: 17px; font-family: Times New Roman; margin: 0;">{top_pincode_count['Transaction_count'][4]}</p>
                  </div>
                  """)
                bottom_tran_container_3.markdown(wrap_with_grey_background(bottom_tran_3_content), unsafe_allow_html=True)      
        else:
          print(f'Unexpected selection')

      elif option == '**User**':
        with st.sidebar:
          custom_css_remove_spaces_radio_button = """
            <style>
            [data-testid="stRadio"] label {
            padding: 0 !important;
            margin: 0 !important;
            }
            </style>
            """
          st.markdown(custom_css_remove_spaces_radio_button,unsafe_allow_html=True)
          page_option=st.radio("**Analysis Page**",['**Map Analysis**','**Top 10 Analysis**'])

        custom_css_label_color = """
          <style>
          /* Targeting the label of st.selectbox */
          div[data-testid="stSelectbox"] label {
          color: white !important; /* Set text color to white */
          padding: 0 !important;
          margin: 0 !important;
          }
          </style>
          """
        st.markdown(custom_css_label_color, unsafe_allow_html=True)

        if page_option=='**Map Analysis**':

          years=[2018,2019,2020,2021,2022,2023,2024]
          quaters={'Q1 (Jan-Mar)':1,'Q2 (Apr-Jun)':2,'Q3 (Jul-Sep)':3,'Q4 (Oct-Dec)':4}
          quaters_2024={'Q1 (Jan-Mar)':1}

          col1, col2,col3,col4 = st.columns([0.28,0.1,0.35,0.95])
          with col1:
            year_key = st.selectbox("Select Year",(years))

          with col3:
            if year_key != 2024:
              quater_key = st.selectbox("Select Quater",list(quaters.keys()))
            else:
              quater_key = st.selectbox("Select Quater",list(quaters_2024.keys()))

          query=f"""
          SELECT 
            st.State,
            yt.Year,
            qt.Quater, 
            agg_user_data.RegisteredUsers, 
            agg_user_data.AppOpens
          FROM 
            aggregateduserdata as agg_user_data,
            states AS st,
            years AS yt,
            quaters AS qt
          WHERE
            st.State_id=agg_user_data.State_id AND
            yt.Year_id=agg_user_data.Year_id AND
            qt.Quater_id=agg_user_data.Quater_id AND
            yt.Year = {year_key} AND
            qt.Quater = {quaters[quater_key]};
            """
          Agg_user = pd.read_sql(query,con=engine)

          stati_box_registered_user=format_indian_number_for_count(value=Agg_user['RegisteredUsers'].sum())
          stati_box_App_Opens=format_indian_number_for_count(value=Agg_user['AppOpens'].sum())

          pan_area_1,pan_area_2=st.columns([7,3])
          
          with pan_area_1:

            def result_map(df,q_key):
              # Set locale to Indian format
              locale.setlocale(locale.LC_ALL, 'en_IN')

              # Load transaction data
              Agg_user = df
              GrouBy_data = Agg_user.groupby('State').agg({'RegisteredUsers': 'sum','AppOpens': 'sum'})
              data_df=GrouBy_data.reset_index()
              data_df['State'] = data_df['State'].str.replace('-', ' ').str.title()

              # Load GeoJSON data using GeoPandas
              geo_data = gpd.read_file(" Replace with json file ") #///////////////Replace with json file location///////////

              # Dissolve the GeoDataFrame by the 'State' column to combine the geometries
              geo_data = geo_data.dissolve(by='State', as_index=False)
              geo_data['State'] = geo_data['State'].str.replace('-', ' ').str.title()

              # Convert the GeoDataFrame to a GeoJSON format
              geo_json = json.loads(geo_data.to_json())

              # Merge dataframe with the GeoDataFrame
              df = geo_data.merge(data_df, how='left')

              # formatted registeruser and appopen data for hover data
              df['RegisteredUsers_formatted'] = df['RegisteredUsers'].apply(format_indian_number_for_count)
              df['AppOpens_formatted'] = df['AppOpens'].apply(format_indian_number_for_count)

              # Creating choropleth map 
              fig = px.choropleth(
                df,
                geojson=geo_json,
                featureidkey="properties.State",
                locations="State",
                color="RegisteredUsers",
                color_continuous_scale=["#3399FF","#FFFFFF","#FF0000"],
                projection="mercator",
                labels={'RegisteredUsers': 'Registered Users'},
                hover_data={
                'State': True,
                'RegisteredUsers_formatted': True,
                'AppOpens_formatted': True,

                }
              )

              # Update the map to fit the GeoJSON data
              fig.update_geos(fitbounds="locations", visible=False)

              # cfeating tick values based on range
              min_val = df['RegisteredUsers'].min()
              max_val = df['RegisteredUsers'].max()
              num_ticks = 8  # Number of ticks
              tickvals = np.linspace(min_val, max_val, num_ticks)

              # Formatting color bar tick values to Indian numeric system
              ticktext = [format_indian_number_for_count(tick) for tick in tickvals]

              fig.update_layout(
              margin=dict(l=0, r=0, t=0, b=0),
              geo=dict(
              bgcolor='#210D38',
              center=dict(lat=20.5937, lon=78.9629),  # Center of India
              projection_scale=1.2,  # Adjust projection scale as needed
              lataxis_showgrid=False,
              lonaxis_showgrid=False,
              ),
              paper_bgcolor='rgba(0,0,0,0)',
              
              coloraxis_colorbar=dict(
                title='Registered Users',
                titleside="top",
                x=1,
                y=0.5,
                title_font_color="white",
                tickvals=tickvals,
                ticktext=ticktext,
                tickfont=dict(color="white"),
                outlinecolor="white",
                outlinewidth=0.01)
                )

              # Custom hover template
              fig.update_traces(
                hovertemplate=(
                '<b style="color:white;font-size:10px">State</b><br>'
                '<b style="color:cyan;font-size:16px">%{customdata[0]}</b><br>' +
                '<br>'+
                '<b style="color:white;font-size:10px">Registered Users</b><br>'+
                '<b style="color:cyan;font-size:16px">%{customdata[1]}</b><br>'+
                '<br>'+
                f'<b style="color:white;font-size:10px">App Opens in {q_key}</b><br>'+
                '<b style="color:cyan;font-size:16px">%{customdata[2]}</b><br>'+
                '<br>'
                ),
                customdata=df[['State', 'RegisteredUsers_formatted', 'AppOpens_formatted']]
              )

              # CSS styling for hover data label
              fig.update_layout(
                hoverlabel=dict(
                bgcolor="#444444",  # Background color of hover label
                font_size=12,  # Font size of hover label
                font_family="Arial",  # Font family of hover label
                )
              )

              # show map
              st.write(fig)
            result_map(df=Agg_user,q_key=quater_key[0:2])

          with pan_area_2:

            box_container=st.container(height=400,border=False)
            with box_container:
              st.markdown(
              """
              <style>
              .grey-background-col {
              background-color: #2C1942;
              padding: 10px;
              border-radius: 5px;
              height: 100%;
              }
              </style>
              """,
              unsafe_allow_html=True)

              content=f"""
                <br>
                <p style="color: cyan; font-size: 18px; font-family: Arial; margin: 5px 0;"><strong>Users</strong></p>
                <p style="color: #FFFFFF; font-size: 16px; font-family: Times New Roman; margin: 10px 0;">Registered PhonePe users till {quater_key[0:2]} {year_key}</p>
                <p style="color: cyan; font-size: 20px; font-family: 'Arial Black'; margin: 5px 0;">{stati_box_registered_user}</p>
                <p style="color: #FFFFFF; font-size: 16px; font-family: Times New Roman; margin: 5px 0;">PhonePe app opens in {quater_key[0:2]} {year_key}</p>
                <p style="color: cyan; font-size: 20px; font-family: Arial Black; margin: 5px 0;"><strong>{stati_box_App_Opens}</strong></p>
                <br>
                """
              box_container.markdown(wrap_with_grey_background(content), unsafe_allow_html=True)
        
        elif page_option == '**Top 10 Analysis**':

          top_analysis_container=st.container(border=False)
          with top_analysis_container:
          
            years=[2018,2019,2020,2021,2022,2023,2024]
            quaters={'Q1 (Jan-Mar)':1,'Q2 (Apr-Jun)':2,'Q3 (Jul-Sep)':3,'Q4 (Oct-Dec)':4}
            quaters_2024={'Q1 (Jan-Mar)':1}
          
            col1, col2,col3,col4 = st.columns([11,27,17,25])
            with col1:
              year_key = st.selectbox("Select Year",(years))
          
            with col3:
              if year_key != 2024:
                quater_key = st.selectbox("Select Quater",list(quaters.keys()))
              else:
                quater_key = st.selectbox("Select Quater",list(quaters_2024.keys()))

            def top_states_reg_users(year,quater):
              top_states_reg_users=f"""
              SELECT 
                st.State,
                agg_user_data.RegisteredUsers
              FROM 
                aggregateduserdata as agg_user_data,
                states AS st,
                years AS yt,
                quaters AS qt

              WHERE
                st.State_id=agg_user_data.State_id AND
                yt.Year_id=agg_user_data.Year_id AND
                qt.Quater_id=agg_user_data.Quater_id AND
                yt.Year = {year} AND
                qt.Quater = {quater};
              """

              top_states_regusers_app_opens=pd.read_sql(top_states_reg_users,con=engine)
              top_states_regusers_app_opens['State']=top_states_regusers_app_opens['State'].str.replace('-',' ')
              top_states_regusers_app_opens['State']=top_states_regusers_app_opens['State'].str.title()

              top_states_regusers = top_states_regusers_app_opens.groupby('State',as_index=False)['RegisteredUsers'].sum()
              top_states_regusers=top_states_regusers.sort_values(by='RegisteredUsers',ascending=False).head(10).reset_index(drop=True)
              top_states_regusers['RegisteredUsers']=top_states_regusers['RegisteredUsers'].apply(format_indian_number)
              top_states_regusers['RegisteredUsers']=top_states_regusers['RegisteredUsers'].str.replace('₹',"")
              
              return top_states_regusers
            top_states_registeredusers=top_states_reg_users(year=year_key,quater=quaters[quater_key])
            
            def top_districts_reg_users(year,quater):
              top_districts_reg_users=f"""
              SELECT 
                dt.District,
                top_dist_user_data.RegisteredUsers
              FROM 
                topdistrictuserdata as top_dist_user_data,
                states AS st,
                years AS yt,
                quaters AS qt,
                districts as dt

              WHERE
                st.State_id=top_dist_user_data.State_id AND
                yt.Year_id=top_dist_user_data.Year_id AND
                qt.Quater_id=top_dist_user_data.Quater_id AND
                dt.District_id=top_dist_user_data.District_id AND
                yt.Year = {year} AND
                qt.Quater = {quater};
              """

              top_district_regusers=pd.read_sql(top_districts_reg_users,con=engine)
              top_district_regusers['District']=top_district_regusers['District'].str.replace('-',' ')
              top_district_regusers['District']=top_district_regusers['District'].str.title()

              top_district_regusers = top_district_regusers.groupby('District',as_index=False)['RegisteredUsers'].sum()
              top_district_regusers=top_district_regusers.sort_values(by='RegisteredUsers',ascending=False).head(10).reset_index(drop=True)
              top_district_regusers['RegisteredUsers']=top_district_regusers['RegisteredUsers'].apply(format_indian_number)
              top_district_regusers['RegisteredUsers']=top_district_regusers['RegisteredUsers'].str.replace('₹',"")
              
              return top_district_regusers
            top_district_registeredusers=top_districts_reg_users(year=year_key,quater=quaters[quater_key])
            
            def top_pincode_reg_users(year,quater):
              top_pincode_reg_users=f"""
              SELECT 
                pt.Pincode,
                top_pin_user_data.RegisteredUsers
              FROM 
                toppincodeuserdata as top_pin_user_data,
                states AS st,
                years AS yt,
                quaters AS qt,
                pincodes as pt

              WHERE
                st.State_id=top_pin_user_data.State_id AND
                yt.Year_id=top_pin_user_data.Year_id AND
                qt.Quater_id=top_pin_user_data.Quater_id AND
                pt.Pincode_id=top_pin_user_data.Pincode_id AND
                yt.Year = {year} AND
                qt.Quater = {quater};
              """

              top_pincode_regusers=pd.read_sql(top_pincode_reg_users,con=engine)

              top_pincode_regusers = top_pincode_regusers.groupby('Pincode',as_index=False)['RegisteredUsers'].sum()
              top_pincode_regusers=top_pincode_regusers.sort_values(by='RegisteredUsers',ascending=False).head(10).reset_index(drop=True)
              top_pincode_regusers['RegisteredUsers']=top_pincode_regusers['RegisteredUsers'].apply(format_indian_number)
              top_pincode_regusers['RegisteredUsers']=top_pincode_regusers['RegisteredUsers'].str.replace('₹',"")              
              return top_pincode_regusers
            top_pincode_registeredusers=top_pincode_reg_users(year=year_key,quater=quaters[quater_key])

            st.markdown(
              """
              <style>
              .grey-background-col {
              background-color: #2C1942;
              padding: 10px;
              border-radius: 5px;
              height: 100%;
              }
              </style>
              """,
              unsafe_allow_html=True)
            st.markdown('<p style="color: white; font-size: 16px; font-family: Arial; margin: 5px 0;"><strong>Registered Users in Diffrent Levels</strong></p>',unsafe_allow_html=True)
            
            top_tran_1,top_tran_2,top_tran_3=st.columns([33.33,33.33,33.33])
            
            with top_tran_1:
            
              top_tran_container_1=st.container(height=380,border=False)
              with top_tran_container_1:
            
                top_tran_1_content=(f"""
                  <p style="color: cyan; font-size: 21px; font-family: Arial; margin: 5px 0;"><strong>States</strong></p>
                  <div style="display: flex; justify-content: space-between; margin: 0;">
                    <p style="color: #FFFFFF; font-size: 14px; font-family: Times New Roman; margin: 0;">{top_states_registeredusers['State'][0]}</p>
                    <p style="color: cyan; font-size: 17px; font-family: Times New Roman; margin: 0;">{top_states_registeredusers['RegisteredUsers'][0]}</p>
                  </div>
                  <div style="display: flex; justify-content: space-between; margin: 0;">
                    <p style="color: #FFFFFF; font-size: 14px; font-family: Times New Roman; margin: 0;">{top_states_registeredusers['State'][1]}</p>
                    <p style="color: cyan; font-size: 17px; font-family: Times New Roman; margin: 0;">{top_states_registeredusers['RegisteredUsers'][1]}</p>
                  </div>
                  <div style="display: flex; justify-content: space-between; margin: 0;">
                    <p style="color: #FFFFFF; font-size: 14px; font-family: Times New Roman; margin: 0;">{top_states_registeredusers['State'][2]}</p>
                    <p style="color: cyan; font-size: 17px; font-family: Times New Roman; margin: 0;">{top_states_registeredusers['RegisteredUsers'][2]}</p>
                  </div>
                  <div style="display: flex; justify-content: space-between; margin: 0;">
                    <p style="color: #FFFFFF; font-size: 14px; font-family: Times New Roman; margin: 0;">{top_states_registeredusers['State'][3]}</p>
                    <p style="color: cyan; font-size: 17px; font-family: Times New Roman; margin: 0;">{top_states_registeredusers['RegisteredUsers'][3]}</p>
                  </div>
                  <div style="display: flex; justify-content: space-between; margin: 0;">
                    <p style="color: #FFFFFF; font-size: 14px; font-family: Times New Roman; margin: 0;">{top_states_registeredusers['State'][4]}</p>
                    <p style="color: cyan; font-size: 17px; font-family: Times New Roman; margin: 0;">{top_states_registeredusers['RegisteredUsers'][4]}</p>
                  </div>
                  <div style="display: flex; justify-content: space-between; margin: 0;">
                    <p style="color: #FFFFFF; font-size: 14px; font-family: Times New Roman; margin: 0;">{top_states_registeredusers['State'][5]}</p>
                    <p style="color: cyan; font-size: 17px; font-family: Times New Roman; margin: 0;">{top_states_registeredusers['RegisteredUsers'][5]}</p>
                  </div>
                  <div style="display: flex; justify-content: space-between; margin: 0;">
                    <p style="color: #FFFFFF; font-size: 14px; font-family: Times New Roman; margin: 0;">{top_states_registeredusers['State'][6]}</p>
                    <p style="color: cyan; font-size: 17px; font-family: Times New Roman; margin: 0;">{top_states_registeredusers['RegisteredUsers'][6]}</p>
                  </div>
                  <div style="display: flex; justify-content: space-between; margin: 0;">
                    <p style="color: #FFFFFF; font-size: 14px; font-family: Times New Roman; margin: 0;">{top_states_registeredusers['State'][7]}</p>
                    <p style="color: cyan; font-size: 17px; font-family: Times New Roman; margin: 0;">{top_states_registeredusers['RegisteredUsers'][7]}</p>
                  </div>
                  <div style="display: flex; justify-content: space-between; margin: 0;">
                    <p style="color: #FFFFFF; font-size: 14px; font-family: Times New Roman; margin: 0;">{top_states_registeredusers['State'][8]}</p>
                    <p style="color: cyan; font-size: 17px; font-family: Times New Roman; margin: 0;">{top_states_registeredusers['RegisteredUsers'][8]}</p>
                  </div>
                  <div style="display: flex; justify-content: space-between; margin: 0;">
                    <p style="color: #FFFFFF; font-size: 14px; font-family: Times New Roman; margin: 0;">{top_states_registeredusers['State'][9]}</p>
                    <p style="color: cyan; font-size: 17px; font-family: Times New Roman; margin: 0;">{top_states_registeredusers['RegisteredUsers'][9]}</p>
                  </div>
                  """)    
                top_tran_container_1.markdown(wrap_with_grey_background(top_tran_1_content), unsafe_allow_html=True)
            
            with top_tran_2:
            
              top_tran_container_2=st.container(height=380,border=False)
              with top_tran_container_2:
            
                top_tran_2_content=(f"""
                  <p style="color: cyan; font-size: 21px; font-family: Arial; margin: 5px 0;"><strong>Districts</strong></p>
                  <div style="display: flex; justify-content: space-between; margin: 0;">
                    <p style="color: #FFFFFF; font-size: 14px; font-family: Times New Roman; margin: 0;">{top_district_registeredusers['District'][0]}</p>
                    <p style="color: cyan; font-size: 17px; font-family: Times New Roman; margin: 0;">{top_district_registeredusers['RegisteredUsers'][0]}</p>
                  </div>
                  <div style="display: flex; justify-content: space-between; margin: 0;">
                    <p style="color: #FFFFFF; font-size: 14px; font-family: Times New Roman; margin: 0;">{top_district_registeredusers['District'][1]}</p>
                    <p style="color: cyan; font-size: 17px; font-family: Times New Roman; margin: 0;">{top_district_registeredusers['RegisteredUsers'][1]}</p>
                  </div>
                  <div style="display: flex; justify-content: space-between; margin: 0;">
                    <p style="color: #FFFFFF; font-size: 14px; font-family: Times New Roman; margin: 0;">{top_district_registeredusers['District'][2]}</p>
                    <p style="color: cyan; font-size: 17px; font-family: Times New Roman; margin: 0;">{top_district_registeredusers['RegisteredUsers'][2]}</p>
                  </div>
                  <div style="display: flex; justify-content: space-between; margin: 0;">
                    <p style="color: #FFFFFF; font-size: 14px; font-family: Times New Roman; margin: 0;">{top_district_registeredusers['District'][3]}</p>
                    <p style="color: cyan; font-size: 17px; font-family: Times New Roman; margin: 0;">{top_district_registeredusers['RegisteredUsers'][3]}</p>
                  </div>
                  <div style="display: flex; justify-content: space-between; margin: 0;">
                    <p style="color: #FFFFFF; font-size: 14px; font-family: Times New Roman; margin: 0;">{top_district_registeredusers['District'][4]}</p>
                    <p style="color: cyan; font-size: 17px; font-family: Times New Roman; margin: 0;">{top_district_registeredusers['RegisteredUsers'][4]}</p>
                  </div>
                  <div style="display: flex; justify-content: space-between; margin: 0;">
                    <p style="color: #FFFFFF; font-size: 14px; font-family: Times New Roman; margin: 0;">{top_district_registeredusers['District'][5]}</p>
                    <p style="color: cyan; font-size: 17px; font-family: Times New Roman; margin: 0;">{top_district_registeredusers['RegisteredUsers'][5]}</p>
                  </div>
                  <div style="display: flex; justify-content: space-between; margin: 0;">
                    <p style="color: #FFFFFF; font-size: 14px; font-family: Times New Roman; margin: 0;">{top_district_registeredusers['District'][6]}</p>
                    <p style="color: cyan; font-size: 17px; font-family: Times New Roman; margin: 0;">{top_district_registeredusers['RegisteredUsers'][6]}</p>
                  </div>
                  <div style="display: flex; justify-content: space-between; margin: 0;">
                    <p style="color: #FFFFFF; font-size: 14px; font-family: Times New Roman; margin: 0;">{top_district_registeredusers['District'][7]}</p>
                    <p style="color: cyan; font-size: 17px; font-family: Times New Roman; margin: 0;">{top_district_registeredusers['RegisteredUsers'][7]}</p>
                  </div>
                  <div style="display: flex; justify-content: space-between; margin: 0;">
                    <p style="color: #FFFFFF; font-size: 14px; font-family: Times New Roman; margin: 0;">{top_district_registeredusers['District'][8]}</p>
                    <p style="color: cyan; font-size: 17px; font-family: Times New Roman; margin: 0;">{top_district_registeredusers['RegisteredUsers'][8]}</p>
                  </div>
                  <div style="display: flex; justify-content: space-between; margin: 0;">
                    <p style="color: #FFFFFF; font-size: 14px; font-family: Times New Roman; margin: 0;">{top_district_registeredusers['District'][9]}</p>
                    <p style="color: cyan; font-size: 17px; font-family: Times New Roman; margin: 0;">{top_district_registeredusers['RegisteredUsers'][9]}</p>
                  </div>
                  """)
                top_tran_container_2.markdown(wrap_with_grey_background(top_tran_2_content), unsafe_allow_html=True)
          
            with top_tran_3:
          
              top_tran_container_3=st.container(height=380,border=False)
              with top_tran_container_3:
          
                top_tran_3_content=(f"""
                  <p style="color: cyan; font-size: 21px; font-family: Arial; margin: 5px 0;"><strong>Postal Codes</strong></p>
                  <div style="display: flex; justify-content: space-between; margin: 0;">
                    <p style="color: #FFFFFF; font-size: 14px; font-family: Times New Roman; margin: 0;">{top_pincode_registeredusers['Pincode'][0]}</p>
                    <p style="color: cyan; font-size: 17px; font-family: Times New Roman; margin: 0;">{top_pincode_registeredusers['RegisteredUsers'][0]}</p>
                  </div>
                  <div style="display: flex; justify-content: space-between; margin: 0;">
                    <p style="color: #FFFFFF; font-size: 14px; font-family: Times New Roman; margin: 0;">{top_pincode_registeredusers['Pincode'][1]}</p>
                    <p style="color: cyan; font-size: 17px; font-family: Times New Roman; margin: 0;">{top_pincode_registeredusers['RegisteredUsers'][1]}</p>
                  </div>
                  <div style="display: flex; justify-content: space-between; margin: 0;">
                    <p style="color: #FFFFFF; font-size: 14px; font-family: Times New Roman; margin: 0;">{top_pincode_registeredusers['Pincode'][2]}</p>
                    <p style="color: cyan; font-size: 17px; font-family: Times New Roman; margin: 0;">{top_pincode_registeredusers['RegisteredUsers'][2]}</p>
                  </div>
                  <div style="display: flex; justify-content: space-between; margin: 0;">
                    <p style="color: #FFFFFF; font-size: 14px; font-family: Times New Roman; margin: 0;">{top_pincode_registeredusers['Pincode'][3]}</p>
                    <p style="color: cyan; font-size: 17px; font-family: Times New Roman; margin: 0;">{top_pincode_registeredusers['RegisteredUsers'][3]}</p>
                  </div>
                  <div style="display: flex; justify-content: space-between; margin: 0;">
                    <p style="color: #FFFFFF; font-size: 14px; font-family: Times New Roman; margin: 0;">{top_pincode_registeredusers['Pincode'][4]}</p>
                    <p style="color: cyan; font-size: 17px; font-family: Times New Roman; margin: 0;">{top_pincode_registeredusers['RegisteredUsers'][4]}</p>
                  </div>
                  <div style="display: flex; justify-content: space-between; margin: 0;">
                    <p style="color: #FFFFFF; font-size: 14px; font-family: Times New Roman; margin: 0;">{top_pincode_registeredusers['Pincode'][5]}</p>
                    <p style="color: cyan; font-size: 17px; font-family: Times New Roman; margin: 0;">{top_pincode_registeredusers['RegisteredUsers'][5]}</p>
                  </div>
                  <div style="display: flex; justify-content: space-between; margin: 0;">
                    <p style="color: #FFFFFF; font-size: 14px; font-family: Times New Roman; margin: 0;">{top_pincode_registeredusers['Pincode'][6]}</p>
                    <p style="color: cyan; font-size: 17px; font-family: Times New Roman; margin: 0;">{top_pincode_registeredusers['RegisteredUsers'][6]}</p>
                  </div>
                  <div style="display: flex; justify-content: space-between; margin: 0;">
                    <p style="color: #FFFFFF; font-size: 14px; font-family: Times New Roman; margin: 0;">{top_pincode_registeredusers['Pincode'][7]}</p>
                    <p style="color: cyan; font-size: 17px; font-family: Times New Roman; margin: 0;">{top_pincode_registeredusers['RegisteredUsers'][7]}</p>
                  </div>
                  <div style="display: flex; justify-content: space-between; margin: 0;">
                    <p style="color: #FFFFFF; font-size: 14px; font-family: Times New Roman; margin: 0;">{top_pincode_registeredusers['Pincode'][8]}</p>
                    <p style="color: cyan; font-size: 17px; font-family: Times New Roman; margin: 0;">{top_pincode_registeredusers['RegisteredUsers'][8]}</p>
                  </div>
                  <div style="display: flex; justify-content: space-between; margin: 0;">
                    <p style="color: #FFFFFF; font-size: 14px; font-family: Times New Roman; margin: 0;">{top_pincode_registeredusers['Pincode'][9]}</p>
                    <p style="color: cyan; font-size: 17px; font-family: Times New Roman; margin: 0;">{top_pincode_registeredusers['RegisteredUsers'][9]}</p>
                  </div>
                  """)
                top_tran_container_3.markdown(wrap_with_grey_background(top_tran_3_content), unsafe_allow_html=True)
        else:
          print(f'Unexpected selection')

      elif option=='**Trend**':
        custom_css_label_color = """
          <style>
          /* Targeting the label of st.selectbox */
          div[data-testid="stSelectbox"] label {
              color: white !important; /* Set text color to white */
              padding: 0 !important;
              margin: 0 !important;
          }
          </style>
          """
        st.markdown(custom_css_label_color, unsafe_allow_html=True)
        
        col1,col2,col3,col4=st.columns([25,20,30,25])
        list_of_parameters=['Transaction','Insurance','User']
        sub_parameters_list=['Transaction value','Transaction count','Registered Users', 'App opens']
        
        with col1:
          main_parameter=st.selectbox('Select analysis parameter',(list_of_parameters))
        with col3:
          if main_parameter== 'Transaction' or main_parameter== 'Insurance':
            sub_parameters=st.selectbox('Choose criteria',(sub_parameters_list[0:2]))
          else:
            sub_parameters=st.selectbox('Choose criteria',(sub_parameters_list[2:4]))
        
        if main_parameter=='Transaction':
        
          query=f"""
            SELECT 
                  st.State,
                  yt.Year,
                  qt.Quater, 
                  tt.Transaction_type, 
                  agg_trans.Transaction_amount, 
                  agg_trans.Transaction_count
                FROM 
                  aggregatedtransactions as agg_trans,
                  states AS st,
                  years AS yt,
                  quaters AS qt,
                  transaction_types AS tt
                WHERE
                  st.State_id=agg_trans.State_id AND
                  yt.Year_id=agg_trans.Year_id AND
                  qt.Quater_id=agg_trans.Quater_id AND
                  tt.Transaction_type_id=agg_trans.Transaction_type_id;
                  """
          Agg_trans = pd.read_sql(query,con=engine)
        
          if sub_parameters=='Transaction value':
            columns_to_drop_for_trans=['State','Transaction_type','Transaction_count']
            trans_amount=Agg_trans.drop(columns=columns_to_drop_for_trans)
            trans_amount['Transaction_amount']=trans_amount['Transaction_amount'].astype(float)
            grouped_df = trans_amount.groupby(['Year', 'Quater'])['Transaction_amount'].sum().reset_index()
          else:
            columns_to_drop_for_trans=['State','Transaction_type','Transaction_amount']
            trans_count=Agg_trans.drop(columns=columns_to_drop_for_trans)
            trans_count['Transaction_count']=trans_count['Transaction_count'].astype(float)
            grouped_df = trans_count.groupby(['Year', 'Quater'])['Transaction_count'].sum().reset_index()

          st.write("")
          st.write("")
          def result_line_chart():
            # Locale to indian format
            locale.setlocale(locale.LC_ALL, '')

            # formatting number into Indian currency style with comma separation
            def format_indian_number(value):
              if value >= 1e7:
                formatted_value = f"{value / 1e7:.2f} Cr"
              elif value >= 1e5:
                formatted_value = f"{value / 1e5:.2f} L"
              elif value >= 1e3:
                formatted_value = f"{value / 1e3:.2f} K"
              else:
                formatted_value = f"{value:.2f}"

              # Add comma separation to the integer part only
              parts = formatted_value.split('.')
              int_part = parts[0]
              int_part_with_commas = locale.format_string("%d", int(int_part.replace(',', '')), grouping=True)
              formatted_value = int_part_with_commas + '.' + parts[1]

              return formatted_value

            #Preparing year & quater information
            years = grouped_df['Year']
            quaters = grouped_df['Quater']
            grouped_df['Quater']=grouped_df['Quater'].replace(1,'Q1')
            grouped_df['Quater']=grouped_df['Quater'].replace(2,'Q2')
            grouped_df['Quater']=grouped_df['Quater'].replace(3,'Q3')
            grouped_df['Quater']=grouped_df['Quater'].replace(4,'Q4')
            quaters = grouped_df['Quater']

            # Setting up parameter and setting up maptitiele and map y axis title label
            if sub_parameters=='Transaction value':
              transaction_parameter=grouped_df['Transaction_amount']
              y_axis_title_label='Transaction Amount'
              map_title=f'Quarter-wise Transaction Amount Over {years.nunique()} Years'
            else:
              transaction_parameter=grouped_df['Transaction_count']
              y_axis_title_label='Transaction Count'
              map_title=f'Quarter-wise Transactions Over {years.nunique()} Years'

            # Label combining for x axis
            combined_labels = [f'{year} {quarter}' for year, quarter in zip(years, quaters)]
            combined_labels = combined_labels[:len(transaction_parameter)]

            # Formatting transaction amount for hover label
            transaction_parameter_formatted = [format_indian_number(parameter) for parameter in transaction_parameter]

            # Create chart
            fig = go.Figure()

            # trace for line chart with formatted hover text
            fig.add_trace(go.Scatter(
              x=combined_labels,
              y=transaction_parameter,
              mode='lines+markers',
              name='Transaction Amount',
              marker=dict(color='yellow'),
              hovertext=[f"{label}<br>₹ {formatted_parameter}" if sub_parameters=='Transaction value' else f"{label}<br> {formatted_parameter}" for label, formatted_parameter in zip(combined_labels, transaction_parameter_formatted)],  # Combine default and custom hover text
              hoverinfo='text'
            ))

            # creating tick values based on range
            min_val = min(transaction_parameter)
            max_val = max(transaction_parameter)
            num_ticks = 8  # Number of ticks
            tickvals = np.linspace(min_val, max_val, num_ticks)

            # Customize layout
            fig.update_layout(
              margin=dict(l=0, r=0, t=22, b=160),
              title=map_title,
              title_font_color='white',
              xaxis_title='Year and Quarters',
              yaxis_title=y_axis_title_label,
              xaxis=dict(
              tickmode='linear',
              tick0=0,
              dtick=4,  # Show only one label per year (reduce congestion)
              tickangle=45,
              tickfont=dict(size=10,color='white'),
              title=dict(
                text='Year and Quarters',
                font=dict(
                color='white'  # Axis title color
                )
              ),
                showgrid=True,  # grid lines
                gridcolor='rgba(49,51,63,0.5)',  # Set grid color
                gridwidth=0.1,  # grid width
                zeroline=False  # Hide zero line
              ),
              yaxis=dict(
              tickvals=tickvals,  # Specify the tick values
              ticktext=[f'₹ {format_indian_number(val)}' if sub_parameters=='Transaction value' else format_indian_number(val) for val in tickvals],  # Format tick labels
              tickfont=dict(size=10, color='white'),  # Tick labels color
              title=dict(
                text=y_axis_title_label,
                font=dict(
                  color='white'  # Axis title color
                )
              ),
              showgrid=True,  # grid lines
              gridcolor='rgba(49,51,63,0.5)',  # Set grid color
              gridwidth=1  # grid width
              ),
              hovermode='closest',
              plot_bgcolor='rgba(0,0,0,0)',  # Transparent plot background
              paper_bgcolor='rgba(0,0,0,0)'  # Transparent paper background
              )
            fig.update_traces(hoverlabel=dict(bgcolor='white', bordercolor='grey', font=dict(color='black')))

            # Show chart
            st.write(fig)
          result_line_chart()

        elif main_parameter=='Insurance':

          query=f"""
            SELECT 
              st.State,
              yt.Year,
              qt.Quater, 
              tt.Transaction_type, 
              agg_ins_trans.Transaction_amount, 
              agg_ins_trans.Transaction_count
            FROM 
              aggregatedinsurancetransactions as agg_ins_trans,
              states AS st,
              years AS yt,
              quaters AS qt,
              transaction_types AS tt
            WHERE
              st.State_id=agg_ins_trans.State_id AND
              yt.Year_id=agg_ins_trans.Year_id AND
              qt.Quater_id=agg_ins_trans.Quater_id AND
              tt.Transaction_type_id=agg_ins_trans.Transaction_type_id;
              """
          Agg_trans = pd.read_sql(query,con=engine)
          
          if sub_parameters=='Transaction value':
            columns_to_drop_for_trans=['State','Transaction_type','Transaction_count']
            trans_amount=Agg_trans.drop(columns=columns_to_drop_for_trans)
            trans_amount['Transaction_amount']=trans_amount['Transaction_amount'].astype(float)
            grouped_df = trans_amount.groupby(['Year', 'Quater'])['Transaction_amount'].sum().reset_index()
          else:
            columns_to_drop_for_trans=['State','Transaction_type','Transaction_amount']
            trans_count=Agg_trans.drop(columns=columns_to_drop_for_trans)
            trans_count['Transaction_count']=trans_count['Transaction_count'].astype(float)
            grouped_df = trans_count.groupby(['Year', 'Quater'])['Transaction_count'].sum().reset_index()



          st.write("")
          st.write("")
          def result_line_chart():
            # Set the locale India
            locale.setlocale(locale.LC_ALL, '')

            # formatting number into Indian currency style with comma
            def format_indian_number(value):
                if value >= 1e7:
                    formatted_value = f"{value / 1e7:.2f} Cr"
                elif value >= 1e5:
                    formatted_value = f"{value / 1e5:.2f} L"
                elif value >= 1e3:
                    formatted_value = f"{value / 1e3:.2f} K"
                else:
                    formatted_value = f"{value:.2f}"

                # Add comma separation to the integer part only
                parts = formatted_value.split('.')
                int_part = parts[0]
                int_part_with_commas = locale.format_string("%d", int(int_part.replace(',', '')), grouping=True)
                formatted_value = int_part_with_commas + '.' + parts[1]

                return formatted_value

            # Preparing data
            years = grouped_df['Year']
            quaters = grouped_df['Quater']
            grouped_df['Quater']=grouped_df['Quater'].replace(1,'Q1')
            grouped_df['Quater']=grouped_df['Quater'].replace(2,'Q2')
            grouped_df['Quater']=grouped_df['Quater'].replace(3,'Q3')
            grouped_df['Quater']=grouped_df['Quater'].replace(4,'Q4')
            quaters = grouped_df['Quater']

            # Setting up parameter and setting up maptitiele and map y axis title label
            if sub_parameters=='Transaction value':
              transaction_parameter=grouped_df['Transaction_amount']
              y_axis_title_label='Premium Amount'
              map_title=f'Quarter-wise Premium Amount Over {years.nunique()} Years'
            else:
              transaction_parameter=grouped_df['Transaction_count']
              y_axis_title_label='Purchased Policies (Nos)'
              map_title=f'Quarter-wise Policies Purchase (Nos) Over {years.nunique()} Years'

            # Create combined labels for x-axis
            combined_labels = [f'{year} {quarter}' for year, quarter in zip(years, quaters)]
            combined_labels = combined_labels[:len(transaction_parameter)]

            # Formatting custom hover text
            transaction_parameter_formatted = [format_indian_number(parameter) for parameter in transaction_parameter]

            # Create chart
            fig = go.Figure()

            # Trace for line chart with custom hover text
            fig.add_trace(go.Scatter(
                x=combined_labels,
                y=transaction_parameter,
                mode='lines+markers',
                name='Transaction Amount',
                marker=dict(color='yellow'),
                hovertext=[f"{label}<br>₹ {formatted_parameter}" if sub_parameters=='Transaction value' else f"{label}<br> {formatted_parameter}" for label, formatted_parameter in zip(combined_labels, transaction_parameter_formatted)],  
                hoverinfo='text' 
            ))

            # creating tick values based on range
            min_val = min(transaction_parameter)
            max_val = max(transaction_parameter)
            num_ticks = 8  # Number of ticks
            tickvals = np.linspace(min_val, max_val, num_ticks)

            # Custom layout
            fig.update_layout(
               margin=dict(l=0, r=0, t=22, b=160),
              title=map_title,
              title_font_color='white',
              xaxis_title='Year and Quarters',
              yaxis_title=y_axis_title_label,
              xaxis=dict(
                  tickmode='linear',
                  tick0=0,
                  dtick=4,  # Show only one label per year (reduce congestion)
                  tickangle=45,
                  tickfont=dict(size=10,color='white'),
                  title=dict(
                      text='Year and Quarters',
                      font=dict(
                          color='white'  # Axis title color
                      )
                  ),
                  showgrid=True,  # grid lines
                  gridcolor='rgba(49,51,63,0.5)',  # grid color 
                  gridwidth=0.1,  # grid width
                  zeroline=False  # Hide zero line
              ),
              yaxis=dict(
                  tickvals=tickvals,  # Specify the tick values
                  ticktext=[f'₹ {format_indian_number(val)}' if sub_parameters=='Transaction value' else format_indian_number(val) for val in tickvals],  # Format tick labels
                  tickfont=dict(size=10, color='white'),  # Tick labels color
                  title=dict(
                      text=y_axis_title_label,
                      font=dict(
                          color='white'  # Axis title color
                      )
                  ),
                  showgrid=True,  # Show grid lines
                  gridcolor='rgba(49,51,63,0.5)',  # Set grid color 
                  gridwidth=1  # grid width
              ),
              hovermode='closest',
              plot_bgcolor='rgba(0,0,0,0)',  # Transparent plot background
              paper_bgcolor='rgba(0,0,0,0)'  # Transparent paper background
              )
            fig.update_traces(hoverlabel=dict(bgcolor='white', bordercolor='grey', font=dict(color='black')))

            # Show chart
            st.write(fig)
          result_line_chart()
        
        else:
          query=f"""
            SELECT 
                  st.State,
                  yt.Year,
                  qt.Quater, 
                  agg_user.RegisteredUsers, 
                  agg_user.AppOpens
                FROM 
                  aggregateduserdata as agg_user,
                  states AS st,
                  years AS yt,
                  quaters AS qt
                WHERE
                  st.State_id=agg_user.State_id AND
                  yt.Year_id=agg_user.Year_id AND
                  qt.Quater_id=agg_user.Quater_id 
                  """
          agg_user = pd.read_sql(query,con=engine)

          if sub_parameters=='Registered Users':
            columns_to_drop_for_trans=['State','AppOpens']
            reg_users=agg_user.drop(columns=columns_to_drop_for_trans)
            grouped_df = reg_users.groupby(['Year', 'Quater'])['RegisteredUsers'].sum().reset_index()
          else:
            columns_to_drop_for_trans=['State','RegisteredUsers']
            app_opens=agg_user.drop(columns=columns_to_drop_for_trans)
            grouped_df = app_opens.groupby(['Year', 'Quater'])['AppOpens'].sum().reset_index()
            grouped_df = grouped_df[grouped_df['AppOpens']>0]


          st.write("")
          st.write("")
          def result_line_chart():
            # locale to indian
            locale.setlocale(locale.LC_ALL, '')

            # formatting number into Indian currency style with comma separation
            def format_indian_number(value):
                if value >= 1e7:
                    formatted_value = f"{value / 1e7:.2f} Cr"
                elif value >= 1e5:
                    formatted_value = f"{value / 1e5:.2f} L"
                elif value >= 1e3:
                    formatted_value = f"{value / 1e3:.2f} K"
                else:
                    formatted_value = f"{value:.2f}"

                # Add comma separation to the integer part only
                parts = formatted_value.split('.')
                int_part = parts[0]
                int_part_with_commas = locale.format_string("%d", int(int_part.replace(',', '')), grouping=True)
                formatted_value = int_part_with_commas + '.' + parts[1]

                return formatted_value

            # Data Preparation
            years = grouped_df['Year']
            quaters = grouped_df['Quater']
            grouped_df['Quater']=grouped_df['Quater'].replace(1,'Q1')
            grouped_df['Quater']=grouped_df['Quater'].replace(2,'Q2')
            grouped_df['Quater']=grouped_df['Quater'].replace(3,'Q3')
            grouped_df['Quater']=grouped_df['Quater'].replace(4,'Q4')
            quaters = grouped_df['Quater']

            # Settting up parameter and y axis title and map title
            if sub_parameters=='Registered Users':
              transaction_parameter=grouped_df['RegisteredUsers']
              y_axis_title_label='Registered Users'
              map_title=f'Quarter-wise Registered Users Over {years.nunique()} Years'
            else:
              transaction_parameter=grouped_df['AppOpens']
              y_axis_title_label='App Opens'
              map_title=f'Quarter-wise App Opens Over {years.nunique()} Years'
            
            # Create combined labels for x-axis
            combined_labels = [f'{year} {quarter}' for year, quarter in zip(years, quaters)]
            combined_labels = combined_labels[:len(transaction_parameter)]

            # Formatting transaction amounts for hover text
            transaction_parameter_formatted = [format_indian_number(parameter) for parameter in transaction_parameter]

            # Create figure
            fig = go.Figure()

            # Trace for line chart with custom  hover text
            fig.add_trace(go.Scatter(
              x=combined_labels,
              y=transaction_parameter,
              mode='lines+markers',
              marker=dict(color='yellow'),
              hovertext=[f"{label}<br>{formatted_parameter}" for label, formatted_parameter in zip(combined_labels, transaction_parameter_formatted)],
              hoverinfo='text'  
              )
            )

            # Creating tick values based on range
            min_val = min(transaction_parameter)
            max_val = max(transaction_parameter)
            num_ticks = 8  # Number of ticks
            tickvals = np.linspace(min_val, max_val, num_ticks)

            # Customize layout
            fig.update_layout(
               margin=dict(l=0, r=0, t=22, b=160),
              title=map_title,
              title_font_color='white',
              xaxis_title='Year and Quarters',
              yaxis_title=y_axis_title_label,
              xaxis=dict(
                tickmode='linear',
                tick0=0,
                dtick=4,  # Show only one label per year (reduce congestion)
                tickangle=45,
                tickfont=dict(size=10,color='white'),
                title=dict(
                  text='Year and Quarters',
                  font=dict(
                    color='white'  # Axis title color
                  )
                ),
                showgrid=True,  # grid lines
                gridcolor='rgba(49,51,63,0.5)',  # Set grid color
                gridwidth=0.1,  # grid width
                zeroline=False  # Hide zero line
              ),
              yaxis=dict(
                tickvals=tickvals,  # Specify the tick values
                ticktext=[format_indian_number(val) for val in tickvals],  # Format tick labels
                tickfont=dict(size=10, color='white'),  # Tick labels color
                title=dict(
                  text=y_axis_title_label,
                  font=dict(
                    color='white'  # Axis title color
                  )
                ),
                showgrid=True,  # grid lines
                gridcolor='rgba(49,51,63,0.5)',  # Set grid color
                gridwidth=1  # grid width
              ),
              hovermode='closest',
              plot_bgcolor='rgba(0,0,0,0)',  # Transparent plot background
              paper_bgcolor='rgba(0,0,0,0)'  # Transparent paper background
              )
            fig.update_traces(hoverlabel=dict(bgcolor='white', bordercolor='grey', font=dict(color='black')))

            # Show chart
            st.write(fig)
          result_line_chart()

      else:
        st.write("Unexpected option selected!")
    display_section(selected_option)

  else:
    pass



#//////////////////////////////////////Home Page Section/////////////////////////////////////////////////

else:
  
  # CSS for background images and removing extra space and set fixed header on top
  custom_css = """
  <style>
  [data-testid="stAppViewContainer"] > .main {
      background-image: url("https://i.postimg.cc/8kXTyG86/4853479.jpg");
      background-size: cover;
      background-position: center center;
      background-repeat: no-repeat;
      background-attachment: local;
      padding: 0 !important;
      margin: 0 !important;
  }

  [data-testid="stSidebar"] {
      background-image: url('https://i.postimg.cc/qRdHZRqw/pexels-codioful-7130545.jpg');
      background-size: cover;
      background-position: center;
      background-repeat: no-repeat;
      padding: 0 !important;
      margin: 0 !important;
  }

  .fixed-header {
      position: fixed;
      top: 0;
      left: 18.8%;  /* Ensure header starts from the left edge */
      width: 80%;  /* Adjust this value to ensure the header takes the desired width */
      width: 100%;
      background-image: url("https://i.postimg.cc/qRdHZRqw/pexels-codioful-7130545.jpg"); /* Background image for header */
      background-size: cover;
      background-position: center;
      background-repeat: no-repeat;
      z-index: 1000;
      padding: 05px 0;
      box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);  /* Optional: add a subtle shadow for better visibility */
  }

  .header-placeholder {
      height: 0px; /* This should match the height of your fixed header */
  }
  .header-content {
      text-align: left; 
      margin-left: 0px; /* Adjust this value to move the text left or right */
  }
  </style>
  """
  st.markdown(custom_css, unsafe_allow_html=True)
  
  # function to make bold text
  def bold_word(word):
    return f"<strong>{word}</strong>"
  
  header_html = """
    <div class="fixed-header">
    <h1 style='color:#5F259F; padding:0; margin:0; margin-left: 150px;'>PhonePe <span style='color:#31333F;'>Data <span style='color:#31333F;'>Analysis</h1>
    </div>
    <div class="header-placeholder"></div>
  """
  st.markdown(header_html, unsafe_allow_html=True)

  # Coloring word & annoted pargaraph
  rainbow_word=':rainbow[Welcome to the PhonePe Data Analysis App..!]'
  st.markdown(f"<span style='padding:0; margin:0;'>{bold_word(rainbow_word)}</span>", unsafe_allow_html=True)
  annotated_text("", ("This section will guide you on how to use the app effectively.", ""))
  annotated_text("", ("Here’s how to use this app:", ""))

  # App Instruction
  st.write("""
  1. **Navigate**: Use the sidebar to navigate between different sections.
  2. **Data Upload**: Go to the 'Data Upload' section to:
    - Provide the GitHub URL to fetch data.
    - Provide the local path to download data.
    - Enter SQL username and password to establish a connection.
    - Enter the database name where the processed data will be stored.
  3. **Data Analytics**: Go to the 'Data Analytics' section to explore the data you've uploaded. You can perform:
    - **Transaction Based Data Analysis**: Analyze transactions to understand patterns and trends.
    - **Insurance Transaction Based Data Analysis**: Focus specifically on insurance-related transactions to gain insights.
    - **User Based Data Analysis**: Analyze data based on user activity to derive user behavior and engagement insights.
    - **Trend Analysis**: Find out trend of the product usage and business, based on the current data.
  4. **Get Help**: Refer to this welcome section anytime you need guidance on using the app.
  """)

