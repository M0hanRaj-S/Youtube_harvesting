import streamlit as st
from googleapiclient.discovery import build
from pprint import pprint
import json
import pymongo
import pandas as pd
import numpy as np
#from pymongo import MongoClient
import sqlite3
import time
import sqlite_migrate
from sqlite_migrate import migrate_db,migration_completed
import threading


API_KEY = "AIzaSyA6TaVEMUTmdZTg_mNyQloPOofZvmVDnnU"  #"AIzaSyDDYW_WFmYQn5xVOT3UadeZ1pQBOBSJ_Ts"  

youtube = build("youtube", "v3", developerKey=API_KEY)
#Channel_id = "UCkpgyRmcNy-aZFLUkKkWK4w" #RR -"UCkpgyRmcNy-aZFLUkKkWK4w" #KKR - "UCp10aBPqcOeBbEg7d_K9SBw" #Sony_Music_South - "UCn4rEMqKtwBQ6-oEwbd4PcA"  #rcb -"UCCq1xDJMBRF61kiOgU90_kw" csk -"UC2J_VKrAzOEJuQvFFtj3KUw"
global maxResults,channel_data_display,Details_button,Migrate_button
channel_data_display = False
Migrate_start = False
#channel_data_display = False
maxResults = 10


#Mongo_DB_Details
MONGODB_URI = "mongodb+srv://mohanraj:mohanraj@cluster0.ybtpn8r.mongodb.net/?retryWrites=true&w=majority"
DB_NAME = "youtube_data"
COLLECTION_NAME = "channel_data"
SQLITE_DB_PATH = "migrated.db"

client = pymongo.MongoClient(MONGODB_URI)
# client = MongoClient(MONGODB_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]


global Search_completed




def search_channel():
    global cchannel_name,Search_completed
    cchannel_name = st.sidebar.text_input("Enter Channel name:")
    search_response = youtube.search().list(q=cchannel_name,type='video',part='id,snippet',maxResults=10).execute()
    search_response_raw = search_response.get("items", [])

    #pprint (search_response_raw)

    search_details_formated_list = []
    for a in search_response_raw:
        if search_response_raw:
                    search_snippet = a["snippet"]
                    video_details = {
                        "Channel_Name": search_snippet["channelTitle"],
                        "Channel_Id": search_snippet["channelId"]}

                    search_details_formated_list.append(video_details)  
        else:
            None                             
    #pprint(search_details_formated_list) 
    if cchannel_name and search_details_formated_list:
        #print("@@@@@@@@@")
        st.json(search_details_formated_list,expanded=False)
        # df = pd.DataFrame(np.random.randn(10, 2), columns=("col %d" % i for i in range(2)))
    Search_completed = False    
    if cchannel_name:
         Search_completed = True
    return (search_details_formated_list)



# Search_completed = False
# if cchannel_name:
#     #print("$$$$$$$$$$$$",cchannel_name)
#     Search_completed = True






#fetch_channel_name()


# 1_Channel_details
class youtube_data_fetch():
    
    def channel_details(youtube,channel_id):
        #print ("channel-ID-------->",channel_id)
        channel_request = youtube.channels().list(
                    part="snippet,statistics,contentDetails",
                    id=channel_id
                )
        channel_response = channel_request.execute()
        # try:
        #     channel_response = channel_request.execute()
        # except Exception as e:
        #     print(e)
        #cha = channel_response.get("items",[])

        channel = channel_response.get("items", [])[0]
        #pprint(channel_response)
        
        if channel:
                    snippet = channel["snippet"]
                    statistics = channel["statistics"]

                    channel_detailss = {
                        "Channel_Name": snippet["title"],
                        "Channel_Id": channel_id,
                        "Subscription_Count": int(statistics["subscriberCount"]),
                        "Video_Count": int(statistics["videoCount"]),
                        "Channel_Views": int(statistics["viewCount"]),
                        "Channel_Description": snippet["description"],
                    }

        else:
            None   
        #print("************1111111111111111111************")  
        # try:
        #     result_1 = collection.update_one( {"Channel_Name":snippet["title"]},
        # {"$set": channel_detailss},
        # upsert=True)
        #     print(f"Inserted document ID: {result_1.upserted_id}")
        # except pymongo.errors.BulkWriteError as e:
        #     print(f"Error: {e}")  
        # #pprint(channel_detailss) 
        # client.close()    
        return channel_detailss     


    #print (channel_details_out)


    def playlist_details(youtube,channel_id):
        global play_list_details_list,play_list_list
        request = youtube.playlists().list(
            part="contentDetails,snippet",
            channelId=channel_id,
            maxResults=maxResults
        )
        range_limit = maxResults
        try:
            playlists_response = request.execute()
        except Exception as e:
            print(e)    
        play_list_list =[]
        for i in range (0,range_limit):
            play_list = playlists_response.get("items", [])[i]
            play_list_list.append(play_list)


        #pprint(play_list_list)

        play_list_details_list = []
        for j in play_list_list:
            if play_list_list:
                playlist_snippet = j["snippet"]
                playlist_contentDetails = j["contentDetails"]
                play_list_details={
                #"Channel_Name": playlist_snippet["channelTitle"],
                "Channel_Id": playlist_snippet["channelId"],
                "Channel_Name": playlist_snippet["channelTitle"],
                "playlist_title": playlist_snippet["title"],
                "playlists_id": j["id"],
                "playlist_video_count":playlist_contentDetails["itemCount"]}
                play_list_details_list.append(play_list_details)
        #print("************22222222222222222222************") 
        # try:
        #     result_2 = collection.update_one({"Channel_Id":channel_id},
        #     {"$set": play_list_details_list})
        #     print(f"Updated {result_2.modified_count} document(s) for Channel_Id {channel_id}")
        # except pymongo.errors.BulkWriteError as e:
        #     print(f"Error: {e}")       
        # client.close()      
        #pprint(play_list_details_list)        
        return play_list_details_list




    # 3_Video_details

    def video_details(youtube):
        # getting Video ID
        global video_ID_list,video_details_formated_list
        playlist_Items_list_1 = []
        #playlist_Items = ()
        for k in play_list_details_list:
            request = youtube.playlistItems().list(
                    part="contentDetails",
                    maxResults=maxResults,
                    playlistId=k["playlists_id"]
                )
            try:
                playlistItems_response = request.execute()
            except IndexError:
                pass 
            #print("################################")
            #print(type(playlistItems_response))
            #pprint (playlistItems_response)
            playlist_Items= playlistItems_response.get("items",[])
            playlist_Items_list_1.append(playlist_Items)
        #pprint(playlist_Items_list_1)
        playlist_Items_list = [item for sublist in playlist_Items_list_1 for item in sublist]
        video_ID_list =[]
        for l in playlist_Items_list:
            if playlist_Items_list:
                playlist_contentDetails = l["contentDetails"]
                video_ID = playlist_contentDetails["videoId"]
                video_ID_list.append(video_ID)
        #pprint(video_ID_list)

        # getting Video details
        video_details_list_1 =[]
        for m in video_ID_list:
            request = youtube.videos().list(
                    part="snippet,statistics",
                    id=m
                )
            video_response = request.execute()
            video_details_raw = video_response.get("items", [])
            video_details_list_1.append(video_details_raw)
            video_details_list = video_details_list_1
        #video_details_list = [item for sublist in playlist_Items_list_1 for item in sublist]    
        #pprint(video_details_list)   

        video_details_formated_list = []
        for n in video_details_list:
            for z in n:
                if video_details_list:
                            video_snippet = z["snippet"]
                            video_statistics = z["statistics"]

                            video_details = {
                                "Channel_Name": video_snippet["channelTitle"],
                                "Channel_Id": video_snippet["channelId"],
                                "Video_ID": z["id"],
                                "Video_name": video_snippet["title"],
                                "video_published_date":video_snippet["publishedAt"],
                                "Video_Description": video_snippet["description"],
                                "Video_like_count": video_statistics["likeCount"],
                                "Video_view_count": video_statistics["viewCount"]
                            }
                            video_details_formated_list.append(video_details)  

                else:
                    None  
        #print("************33333333333333333333333************") 

        return video_details_formated_list                     


    # 4_Message_details

    def cmd_details(youtube):    
        cmd_details_list = []
        cmd_details = []
        cmd_details_formated_list = []
        #print (video_ID_list)
        for q in video_ID_list:
            request = youtube.commentThreads().list(
                        part="snippet",
                        videoId=q,
                        maxResults=5

             )
            try:      
                cmd_response = request.execute()
            except Exception as e:
                print(e)
            #cmd_details = msg_response.get("items", [])
            cmd_details = cmd_response
            #print ("*****************************")
            #pprint(cmd_details)
            if cmd_details:
                    cmd_snippet = cmd_details["items"]
                    cmd_snippet_1 = cmd_snippet[0]["snippet"]
                    cmd_snippet_snippet_1 = cmd_snippet_1["topLevelComment"]
                    cmd_snippet_snippet = cmd_snippet_snippet_1["snippet"]
                    cmd_detailss = {
                        "Channel_Id": cmd_snippet_snippet["channelId"],
                        "Video_ID": cmd_snippet_snippet["videoId"],
                        "Author_name": cmd_snippet_snippet["authorDisplayName"],
                        "video_published_date":cmd_snippet_snippet["publishedAt"],
                        "Text": cmd_snippet_snippet["textOriginal"]
                    }
                    cmd_details_formated_list.append(cmd_detailss)
        #msg_details_list.append(msg_details)

        #pprint(msg_details_list)
        #print("************4444444444444444444444444444************")

        return(cmd_details_formated_list)

    def fetch_channel_name():
        global cchannel_id
        cchannel_id = st.text_input("Enter Channel ID:")
        query = {"channel_details.Channel_Id": cchannel_id}
        cursor = collection.find(query)
        data = list(cursor)

        if data:
            channel_name_fetch = data[0]['channel_details']['Channel_Name']
            #return channel_name_fetch
        else:
            return None

        if channel_name_fetch:
            st.write('The Channel --',channel_name_fetch,' is already there')
        else:
            st.write("Entered Channel ID is not already there, Data's fetching.. ") 

 

        return cchannel_id
    

    def main1():
        fetch = False
        global channel_data_display,Search_completed,Details_button,condition_ok,Migrate_start

        #print("Before-->",Search_completed)
        condition_ok = False
        if Search_completed == True and channel_data_display == False:
            condition_ok = True
        else:
            condition_ok = False     
        #print("condition_ok  --",condition_ok,", Migrate_start --",Migrate_start)     
        if condition_ok == True and Migrate_start == False:
            print("After-->",Search_completed)
            youtube_data_fetch.fetch_channel_name()
            Channel_id = cchannel_id

            #Channel_id = st.text_input("Enter Channel ID:")
            #st.button("Reset", type="primary")
            Details_button = st.button('Get Detais',key="Details_button")
            if Details_button:
                #print("clikcked Details button")
                fetch = True
                st.write('Channel details...')
            else:
                st.write('Press Get Details to get Channel details')
                fetch = False
            if fetch == True:
                #print("Channel_ID ---->",Channel_id)
                channel_details_out = youtube_data_fetch.channel_details(youtube,Channel_id ) 
                playlist_details_out = youtube_data_fetch.playlist_details(youtube,Channel_id)
                video_details_out = youtube_data_fetch.video_details(youtube)
                cmd_details_out = youtube_data_fetch.cmd_details(youtube)
                youtube_details = []
                youtube_details = {"channel_details":channel_details_out,"playlist_details":playlist_details_out,"video_details":video_details_out,"command_details":cmd_details_out}
                #print(youtube_details)



                cursor = collection.find()
                channel_ids = []
                channel_name_lists = []
                for document in cursor:
                    if "channel_details" in document and "Channel_Id" in document["channel_details"]:
                        channel_id = document["channel_details"]["Channel_Id"]
                        channel_ids.append(channel_id)
                #print(channel_ids)     

                     
                #print(Channel_id)
                if Channel_id in channel_ids:
                    result = collection.update_one(
                        {"Channel_Id": channel_id},
                        {"$push": youtube_details},
                        upsert=False
                    )
                    #print ("Updated@@@@@@@@@@@2...")
                    
                    st.json(youtube_details,expanded=False)
                else:
                    result = collection.insert_one(youtube_details)
                    #print("Inserted@@@@@@@@@@@@@@@...")
                    st.json(youtube_details,expanded=False)
                
                    #print(channel_name_list)
                    #return channel_name_fetch
                    
                channel_data_display = True 
        Search_completed = False         

    def migrate():
        global channel_name_list,Migrate_start
        channel_name_list =[]

        cursor_2 = collection.find()
        data_2 = list(cursor_2)
        if data_2:
            for channel_name_query in data_2:
                channel_names = channel_name_query["channel_details"]["Channel_Name"]
                channel_name_list.append(channel_names)
        else:
            return None     
        #if channel_data_display == True:   
        
        Channel_option = st.sidebar.selectbox('Select the channel to migrate',(channel_name_list))
        st.sidebar.write('You selected:', Channel_option)
        #print("Channel_option--->",Channel_option)

        Migrate_start = True
        #print ("Migrate_start status :::::::: ",Migrate_start)
        Migrate_button = st.sidebar.button('Migrate',key="Migrate_button")
        #print ("Migrate_button status::::::: ",Migrate_button)
        if Migrate_button == True:
            #print("clikcked Migrate button")
            migrate_db(channel_name= Channel_option)
            #print("Migrating!!!!!!!!!...")
            Migrate_start = False
            client.close()

Max_subscribers_list_query = """SELECT Channel_Name,Subscription_Count
    FROM channel_details
    ORDER BY Subscription_Count DESC"""

Max_viewed_video_query = '''SELECT Channel_Name,Video_name,Video_view_count
FROM video_details
WHERE Video_view_count = (
    SELECT MAX(Video_view_count)
    FROM video_details
)'''

Min_viewed_video_query = '''SELECT Channel_Name,Video_name,Video_view_count
FROM video_details
WHERE Video_view_count = (
    SELECT MIN(Video_view_count)
    FROM video_details
)'''

Max_liked_video_query = '''SELECT Channel_Name,Video_name,Video_like_count
FROM video_details
WHERE Video_like_count = (
    SELECT MAX(Video_like_count)
    FROM video_details
)'''

Min_liked_video_query = '''SELECT Channel_Name,Video_name,Video_like_count
FROM video_details
WHERE Video_like_count = (
    SELECT MIN(Video_like_count)
    FROM video_details
)'''

Max_playlist_count_query = '''SELECT Channel_Name,playlist_title,playlist_video_count
FROM playlist_details
WHERE playlist_video_count = (
    SELECT MAX(playlist_video_count)
    FROM playlist_details
)'''

Max_video_count_query = '''SELECT Channel_Name,Video_Count
FROM channel_details
WHERE Video_Count = (
    SELECT MAX(Video_Count)
    FROM channel_details
)'''

Min_video_count_query = '''SELECT Channel_Name,Video_Count
FROM channel_details
WHERE Video_Count = (
    SELECT MIN(Video_Count)
    FROM channel_details
)'''

Max_Channel_views_query = '''SELECT Channel_Name,Channel_Views
FROM channel_details
WHERE Channel_Views = (
    SELECT MAX(Channel_Views)
    FROM channel_details
)'''

Min_Channel_views_query = '''SELECT Channel_Name,Channel_Views
FROM channel_details
WHERE Channel_Views = (
    SELECT MIN(Channel_Views)
    FROM channel_details
)'''

Queries = ("","Get highest subscribers wise list","Get highest viewed video","Get lowest viewed video","Get highest liked video","Get lowest liked video","Get highest playlist count","Get highest video count channel","Get lowest video count channel","Get highest Channel views having channel","Get lowest Channel views having channel")

def detailize(data,keys):
    combined_data = {}
    for i in range(len(keys)):
        combined_data[keys[i]] = data[0][i]
    #print(combined_data)
    return(combined_data)


def query():
    result = []
    Query_select = st.sidebar.selectbox('Select the query',(Queries))

    st.sidebar.write('You selected query:', Query_select) 
    #print(Query_select)
    sqlite_connection = sqlite3.connect(SQLITE_DB_PATH)
    sqlite_cursor = sqlite_connection.cursor() 
    #print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    if Query_select == "Get highest subscribers wise list":
        query_out = sqlite_cursor.execute(Max_subscribers_list_query)
        query_result = (list(query_out))
        #print(query_result)
        query_1_details = ("Channel_name","Subsciption_count")
        for val_tuple in query_result:
            merged_dict = {query_1_details[0]: val_tuple[0], query_1_details[1]: val_tuple[1]}
            result.append(merged_dict)

        st.json(result,expanded=False)

    if Query_select == "Get highest viewed video":
        query_out = sqlite_cursor.execute(Max_viewed_video_query)
        query_result = (list(query_out))
        #print(query_result)
        keys = ("Channel_name", "Video_title", "View_counts")
        result = detailize(query_result,keys)  
        st.json(result,expanded=False)

    if Query_select == "Get lowest viewed video":
        query_out = sqlite_cursor.execute(Min_viewed_video_query)
        query_result = (list(query_out))
        #print(query_result)
        keys = ("Channel_name", "Video_title", "View_counts")
        result = detailize(query_result,keys)  
        st.json(result,expanded=False)  

    if Query_select == "Get highest liked video":
        query_out = sqlite_cursor.execute(Max_liked_video_query)
        query_result = (list(query_out))
        #print(query_result)
        keys = ("Channel_name", "Video_title", "Like_counts")
        result = detailize(query_result,keys)  
        st.json(result,expanded=False)   

    if Query_select == "Get lowest liked video":
        query_out = sqlite_cursor.execute(Min_liked_video_query)
        query_result = (list(query_out))
        #print(query_result)
        keys = ("Channel_name", "Video_title", "Like_counts")
        result = detailize(query_result,keys)  
        st.json(result,expanded=False)        

    if Query_select == "Get highest playlist count":
        query_out = sqlite_cursor.execute(Max_playlist_count_query)
        query_result = (list(query_out))
        #print(query_result)
        keys = ("Channel_name", "Playlist_title", "Playlist_video_count")
        result = detailize(query_result,keys)  
        st.json(result,expanded=False)

    if Query_select == "Get highest video count channel":
        query_out = sqlite_cursor.execute(Max_video_count_query)
        query_result = (list(query_out))
        #print(query_result)
        keys = ("Channel_name", "Video_Count")
        result = detailize(query_result,keys)  
        st.json(result,expanded=False)   

    if Query_select == "Get lowest video count channel":
        query_out = sqlite_cursor.execute(Min_video_count_query)
        query_result = (list(query_out))
        #print(query_result)
        keys = ("Channel_name", "Video_Count")
        result = detailize(query_result,keys)  
        st.json(result,expanded=False) 

    if Query_select == "Get highest Channel views having channel":
        query_out = sqlite_cursor.execute(Max_Channel_views_query)
        query_result = (list(query_out))
        #print(query_result)
        keys = ("Channel_name", "Channel_views_count")
        result = detailize(query_result,keys)  
        st.json(result,expanded=False)   

    if Query_select == "Get lowest Channel views having channel":
        query_out = sqlite_cursor.execute(Min_Channel_views_query)
        query_result = (list(query_out))
        #print(query_result)
        keys = ("Channel_name", "Channel_views_count")
        result = detailize(query_result,keys)  
        st.json(result,expanded=False) 

search_channel()
# t1 = threading.Thread(target=search_channel())   
# t1.start()
#t1.join()                    
                 
youtube_data_fetch.main1()
# t2 = threading.Thread(target=youtube_data_fetch.main1())
#print ("channel_data_display---",channel_data_display)
# t2.start()
#t2.join()


youtube_data_fetch.migrate()        
# t3 = threading.Thread(target=youtube_data_fetch.migrate())
# t3.start()

query()

#print("***********All Thread completed***********")


# if __name__=='__main__':
# Channel_name = fetch_channel_name()
     
