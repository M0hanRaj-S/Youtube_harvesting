import pymongo
import sqlite3
import time
from pprint import pprint
import pandas as pd
import numpy as np
from datetime import datetime


global Queries

def formate_time(timestamp_str):
    timestamp_obj = datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%SZ")
    formatted_timestamp = timestamp_obj.strftime("%Y:%m:%d %H:%M:%S")
    return formatted_timestamp

def channel_name(channel_iid):
    query_channel_name = (f'SELECT Channel_Name FROM channel_details WHERE Channel_Id = "{channel_iid}"')
    sqlite_connection = sqlite3.connect(SQLITE_DB_PATH)
    sqlite_cursor = sqlite_connection.cursor()
    sqlite_cursor.execute(query_channel_name,(channel_iid,))
    result = sqlite_cursor.fetchone()
    if result:
        channel_id = result[0]
        print("Channel Name:", channel_id)
    else:
        print("No matching channel found.")

   


MONGODB_URI = "mongodb+srv://mohanraj:mohanraj@cluster0.ybtpn8r.mongodb.net/?retryWrites=true&w=majority"
DB_NAME = "youtube_data"
COLLECTION_NAME = "channel_data"

client = pymongo.MongoClient(MONGODB_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

channel_full_details_list=[]
channel_name = "Chennai Super Kings"

global migration_completed
#print(channel_name)
# def channel_full_details(channel_id):
#     query = {"channel_details.Channel_Id": channel_id}
#     cursor = collection.find(query)
#     data = list(cursor)
#     return (data)
# channel_full_details_list = channel_full_details(channel_id)
SQLITE_DB_PATH = "migrated.db"
migration_completed = False


chanel_id = "UCkpgyRmcNy-aZFLUkKkWK4w"

#  

def migrate_db(channel_name):
    global migration_completed
    query = {"channel_details.Channel_Name": channel_name}
    cursor = collection.find(query)
    channel_full_details_list = list(cursor)

    sqlite_connection = sqlite3.connect(SQLITE_DB_PATH)
    sqlite_cursor = sqlite_connection.cursor()
    sqlite_cursor.execute('''
        CREATE TABLE IF NOT EXISTS channel_details (
        Channel_Name        CHAR (255),
        Channel_Id          CHAR (255),
        Subscription_Count  INTEGER,
        Video_Count         INTEGER,
        Channel_Views       INTEGER,
        Channel_Description CHAR
    )
                          ''')

    sqlite_cursor.execute('''
                  CREATE TABLE IF NOT EXISTS command_details (
        Channel_Id           CHAR (255) REFERENCES channel_data (Channel_Id) MATCH SIMPLE,
        Video_ID             CHAR (255),
        Author_name          CHAR (255),
        video_published_date DATETIME,
        Text                 CHAR
    ) 
                        ''')   

    sqlite_cursor.execute('''
                          CREATE TABLE IF NOT EXISTS playlist_details  (
        Channel_Id           CHAR (255) REFERENCES channel_data (Channel_Id) MATCH SIMPLE,
        Channel_Name         CHAR (255),
        playlist_title       CHAR (255),
        playlists_id         CHAR (255),
        playlist_video_count INTEGER 
    )
                          ''') 

    sqlite_cursor.execute('''
                          CREATE TABLE IF NOT EXISTS video_details (
        Channel_Name         CHAR (255) REFERENCES channel_data (Channel_Name) MATCH SIMPLE,
        Channel_Id           CHAR (255) REFERENCES channel_data (Channel_Id) MATCH SIMPLE,
        Video_ID             CHAR (255),
        Video_name           CHAR (255),
        video_published_date DATETIME,
        Video_Description    CHAR (255),
        Video_like_count     INTEGER,
        Video_view_count     INTEGER 
    )

    ''')



    sqlite_cursor.execute("SELECT MAX(rowid) FROM channel_details WHERE Channel_Name = ?", (channel_name,))
    rowid = sqlite_cursor.fetchone()[0]

    ####111111111  Channel_details
    channel_details_list = channel_full_details_list[0]['channel_details']
    
    if rowid is not None:
        print("1111 -- updating")
        sqlite_cursor.execute('''
        UPDATE channel_details
        SET Channel_Id = ?, Subscription_Count = ?, Video_Count = ?, Channel_Views = ?, Channel_Description = ?
        WHERE rowid = ?
    ''', (
        channel_details_list['Channel_Id'],
        channel_details_list['Subscription_Count'],
        channel_details_list['Video_Count'],
        channel_details_list['Channel_Views'],
        channel_details_list['Channel_Description'],
        rowid
    ))

    else:
        print("1111 -- Inserint")
        sqlite_cursor.execute('''
                INSERT OR REPLACE INTO channel_details (Channel_Name, Channel_Id, Subscription_Count, Video_Count, Channel_Views, Channel_Description)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                channel_details_list['Channel_Name'],
                channel_details_list['Channel_Id'],
                channel_details_list['Subscription_Count'],
                channel_details_list['Video_Count'],
                channel_details_list['Channel_Views'],
                channel_details_list['Channel_Description']
            ))


    ####2222222222  playlist_details
    playlist_details_list = channel_full_details_list[0]['playlist_details']
    for playlist_details_1 in playlist_details_list:

        if rowid is not None:
            print("2222 -- updating")
            sqlite_cursor.execute('''
            UPDATE playlist_details
            SET Channel_Id = ?, Channel_Name = ?, playlist_title = ?, playlists_id = ?, playlist_video_count = ?
            WHERE rowid = ?''', 
            (
            playlist_details_1['Channel_Id'],
            playlist_details_1['Channel_Name'],
            playlist_details_1['playlist_title'],
            playlist_details_1['playlists_id'],
            playlist_details_1['playlist_video_count'],
            rowid
            ))
        else:
            print("2222 -- Inserint")
            sqlite_cursor.execute('''
                    INSERT OR REPLACE INTO playlist_details (Channel_Id, Channel_Name, playlist_title, playlists_id, playlist_video_count)
                    VALUES (?, ?, ?, ?, ?)
                ''', (
                    playlist_details_1['Channel_Id'],
                    playlist_details_1['Channel_Name'],
                    playlist_details_1['playlist_title'],
                    playlist_details_1['playlists_id'],
                    playlist_details_1['playlist_video_count']
                ))

    ####333333333333  video_details
    video_details_list = channel_full_details_list[0]['video_details']
    #print (video_details_list)
    # for i in video_details_list:
    #     time_ts = str((i["video_published_date"]))
    #     timee = formate_time(time_ts)
    #     print(timee)
        
    for video_details_1 in video_details_list:
        if rowid is not None:
            print("3333 -- updating")
            sqlite_cursor.execute('''
            UPDATE video_details
            SET  Channel_Name = ?, Channel_Id = ?, Video_ID = ?, Video_name = ?, video_published_date = ?, Video_Description = ?, Video_like_count = ?, Video_view_count = ?
            WHERE rowid = ?''',     
            (
            video_details_1['Channel_Name'],
            video_details_1['Channel_Id'],
            video_details_1['Video_ID'],
            video_details_1['Video_name'],
            video_details_1['video_published_date'],
            video_details_1['Video_Description'],
            video_details_1['Video_like_count'],
            video_details_1['Video_view_count'],
            rowid
            ))  

        else:
            print("3333 -- Inserint")
            sqlite_cursor.execute('''
                    INSERT OR REPLACE INTO video_details (Channel_Name, Channel_Id, Video_ID, Video_name, video_published_date,Video_Description,Video_like_count,Video_view_count)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    video_details_1['Channel_Name'],
                    video_details_1['Channel_Id'],
                    video_details_1['Video_ID'],
                    video_details_1['Video_name'],
                    video_details_1['video_published_date'],
                    video_details_1['Video_Description'],
                    video_details_1['Video_like_count'],
                    video_details_1['Video_view_count']        
                ))

    ####    444444444444  command_details
    command_details_list = channel_full_details_list[0]['command_details']
    for command_details_1 in command_details_list:
        if rowid is not None:
            print("4444 -- updating")
            sqlite_cursor.execute('''
            UPDATE command_details
            SET Channel_Id = ?, Video_ID = ?, Author_name = ?, video_published_date = ? , Text =? 
            WHERE rowid = ?''' , 
            (
            command_details_1['Channel_Id'],
            command_details_1['Video_ID'],
            command_details_1['Author_name'],
            command_details_1['video_published_date'],
            command_details_1['Text'],
            rowid
            ))
        else:
            print("4444 -- Inserint")
            sqlite_cursor.execute('''
                    INSERT OR REPLACE INTO command_details (Channel_Id, Video_ID, Author_name, video_published_date, Text)
                    VALUES (?, ?, ?, ?, ?)
                ''', (
                    command_details_1['Channel_Id'],
                    command_details_1['Video_ID'],
                    command_details_1['Author_name'],
                    command_details_1['video_published_date'],
                    command_details_1['Text']   
                ))

    sqlite_connection.commit()
    sqlite_connection.close()
    #client.close()
    migration_completed = True
    print("Data migration completed.")


migrate_db(channel_name)


# Max_subscribers_list_query_1 = """SELECT Channel_Name,Subscription_Count
# FROM channel_details
# ORDER BY Subscription_Count DESC"""

# Max_viewed_video_query_2 = '''SELECT Channel_Name,Video_name,Video_view_count
# FROM video_details
# WHERE Video_view_count = (
#     SELECT MAX(Video_view_count)
#     FROM video_details
# )'''

# Min_viewed_video_query_3 = '''SELECT Channel_Name,Video_name,Video_view_count
# FROM video_details
# WHERE Video_view_count = (
#     SELECT MIN(Video_view_count)
#     FROM video_details
# )'''

# Max_liked_video_query_4 = '''SELECT Channel_Name,Video_name,Video_like_count
# FROM video_details
# WHERE Video_like_count = (
#     SELECT MAX(Video_like_count)
#     FROM video_details
# )'''

# Min_liked_video_query_5 = '''SELECT Channel_Name,Video_name,Video_like_count
# FROM video_details
# WHERE Video_like_count = (
#     SELECT MIN(Video_like_count)
#     FROM video_details
# )'''

# Max_playlist_count_query_6 = '''SELECT Channel_Name,playlist_title
# FROM playlist_details
# WHERE playlist_video_count = (
#     SELECT MAX(playlist_video_count)
#     FROM playlist_details
# )'''


# Queries = (Max_subscribers_list_query_1,Max_viewed_video_query_2,Min_viewed_video_query_3,Max_liked_video_query_4,Min_liked_video_query_5,Max_playlist_count_query_6)



# sqlite_connection = sqlite3.connect(SQLITE_DB_PATH)
# sqlite_cursor = sqlite_connection.cursor()
# query_1 = sqlite_cursor.execute(Max_playlist_count_query_6)
# print("*************")
# query_data = (list(query_1))
# print(query_data)



# channel_ID = channel_ID_list[0][0]
# print(channel_ID)
# print(type(channel_ID))
# channel_name(channel_ID)