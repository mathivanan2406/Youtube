from googleapiclient.discovery import build
import pandas as pd
import pymysql
import re
from datetime import datetime
import streamlit as st

#API Key extracting
def api_connect():
    api = "AIzaSyBXHYfyw1G021UjGb09r1vvP8NFE2KXBdk"
    api_service_name = "youtube"
    api_version = "v3"
    
    youtube = build(api_service_name,api_version,developerKey=api)
    
    return youtube

#storing the function another variable
youtube=api_connect()


#getting the channels details by youtube data api v3
def get_channel_info(channel_id):
    request=youtube.channels().list(
                    part ="snippet,contentDetails,statistics",
                    id=channel_id
    )
    response=request.execute()
    
    #extracting the data's to table format
    for i in response['items']:
        data = dict(Channel_Id = i['id'],
                    Channel_Name = i['snippet']['title'],
                    Channel_Subscribers = i['statistics']['subscriberCount'],
                    Channel_Views = i['statistics']['viewCount'],
                    Channel_Description = i['snippet']['description'],
                    Channel_Published = i['snippet']['publishedAt'],
                    Total_Videos=i["statistics"]["videoCount"])
        return data
    
    #get uploads by playlistId
def get_videos_ids(channel_id):
    video_ids=[]
    response = youtube.channels().list(id=channel_id,
                                    part='contentDetails').execute()
    Playlist_Id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token=None
    while True:
        videos = youtube.playlistItems().list(
                                            part='snippet',
                                            playlistId=Playlist_Id,
                                            maxResults=50,
                                            pageToken=next_page_token).execute()

        for i in range(len(videos['items'])):
            video_ids.append(videos['items'][i]['snippet']['resourceId']['videoId'])
        #get the every video id details by using next_Page_Token 
        next_page_token=videos.get('nextPageToken')

        #breaking the video details when its end
        if next_page_token is None:
            break
    return video_ids

#get the video information
def get_video_info(video_ids):
    video_data=[]
    for video_id in video_ids:
        request=youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=video_id
        )
        response=request.execute()

        for item in response['items']:
            data=dict(Channel_Name=item['snippet']['channelTitle'],
                    Channel_Id=item['snippet']['channelId'],
                    Video_Id=item['id'],
                    Video_Name=item['snippet']['title'],
                    Video_Description=item['snippet']['description'],
                    Published_Date=item['snippet']['publishedAt'],
                    View_Count=item['statistics'].get('viewCount'),
                    Like_Count=item['statistics'].get('likeCount'),
                    Dislike_Count=item['statistics'].get('dislikeCount'),
                    Favorite_Count=item['statistics'].get('favoriteCount'),
                    Comment_Count=item['statistics'].get('commentCount'),
                    Duration=item['contentDetails']['duration'],
                    Thumbnail=item['snippet']['thumbnails']['default']['url'],
                    Caption_Status=item['contentDetails']['caption'])
            video_data.append(data)

    return video_data

#get the comment information
def get_comment_info(video_ids):

    Comment_data = []
    comment_count = 0
    try:
        for video_id in video_ids:
            request=youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=150
            )
            response=request.execute()

            for item in response['items']:
                data = dict(Comment_Id=item['snippet']['topLevelComment']['id'],
                            Video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                            Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                            Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            Comment_Published_Date=item['snippet']['topLevelComment']['snippet']['publishedAt'])
                Comment_data.append(data)
                comment_count += 1
                if comment_count >= 150:
                    break
            if comment_count >= 150:
                break
    except:
        pass
    return Comment_data

#get _playlists_details
def get_playlist_details(channel_id):
    next_page_token=None
    All_data = []
    while True: 
            request=youtube.playlists().list(
                part='snippet,contentDetails',
                channelId=channel_id,
                maxResults=50,
                pageToken=next_page_token
            )
            response=request.execute()

            for item in response['items']:
                data = dict(Playlist_Id=item['id'],
                            Channel_Id=item['snippet']['channelId'],
                            Playlist_Name=item['snippet']['title'],
                            Channel_Name=item['snippet']['channelTitle'],
                            Video_Count=item['contentDetails']['itemCount'])
                
                All_data.append(data)
            next_page_token=response.get('nextPageToken')
            if next_page_token is None:
                break
    return All_data

#Streamlit Page
#CREATING OPTION MENU
with st.sidebar:
    st.title(":red[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    st.header("SKILL TAKE AWAY")
    st.button("Python Scripting")
    st.button("API integaration")
    st.button("Data Collection")
    st.button("SQL Connection and Data management")
    st.button("Streamlit Page")

def get_channel_id():
    return st.text_input("# :red[Enter the Channel ID]")
channel_id = get_channel_id()

#Creating a button to migrate to SQL in streamlit
st.button("Migrate to SQL")

#Assigning function to a variable    
ch_info = get_channel_info(channel_id)
pl_info = get_playlist_details(channel_id)
vi_ids = get_videos_ids(channel_id)
vi_info = get_video_info(vi_ids)
com_info = get_comment_info(vi_ids)

#MySQL connect to table
mydb = pymysql.connect(host='3306',user='root',passwd='Tspjgoge@5',database='youtube')
cur = mydb.cursor()

#table creation for sql database
def channel_table():
    try:
        #Create table in SQL
        create_query = '''create table if not exists channel(Channel_Id varchar(100),
                                                            Channel_Name varchar(100),
                                                            Channel_Subscribers varchar(100),
                                                            Channel_Views varchar(100),
                                                            Channel_Description text,
                                                            Channel_Published varchar(100),
                                                            Total_Videos varchar(100))'''
        cur.execute(create_query)
        mydb.commit()
    except:
        print ('Channel tables are created')
channel_table()

#inserting values to table
cur = mydb.cursor()
sql = '''INSERT INTO channel(Channel_Id,Channel_Name,Channel_Subscribers,Channel_Views,Channel_Description,Channel_Published,Total_Videos) VALUES (%s,%s,%s,%s,%s,%s,%s)'''
Channel_Published = datetime.strptime(ch_info["Channel_Published"],"%Y-%m-%dT%H:%M:%SZ")
val = (ch_info["Channel_Id"],ch_info["Channel_Name"],ch_info["Channel_Subscribers"],ch_info["Channel_Views"],ch_info["Channel_Description"],Channel_Published,ch_info["Total_Videos"])
cur.execute(sql, val)
mydb.commit()

#table creation for playlist details
def playlist_table():
    try:
        #Create table in SQL
        create_query = '''create table if not exists playlist(Playlist_Id varchar(100),
                                                                Channel_Id varchar(100),
                                                                Playlist_Name varchar(100),
                                                                Channel_Name varchar(100),
                                                                Video_Count varchar(100))'''
        cur.execute(create_query)
        mydb.commit()

    except:
        print('Creating channel table')
playlist_table()

#converting from list to tuple
playlist = []
for i in pl_info:
    playlist.append(tuple(i.values()))

#inserting values to table
cur = mydb.cursor()
sql = '''INSERT INTO playlist(Playlist_Id,Channel_Id,Playlist_Name,Channel_Name,Video_Count) VALUES (%s,%s,%s,%s,%s)'''
val = playlist
cur.executemany(sql, val)
mydb.commit()

#table creation for video details
def video_table():
    try:
        create_query = '''create table if not exists videos(Channel_Name varchar(100),
                                                            Channel_Id varchar(100),
                                                            Video_Id varchar(100),
                                                            Video_Name varchar(100),
                                                            Video_Description text,
                                                            Published_Date varchar(100),
                                                            View_Count varchar(100),
                                                            Like_Count varchar(100),
                                                            Dislike_Count varchar(100),
                                                            Favorite_Count varchar(100),
                                                            Comment_Count varchar(100),
                                                            Duration varchar(100),
                                                            Thumbnail varchar(100),
                                                            Caption_Status varchar(100))'''
        cur.execute(create_query)
        mydb.commit()

    except:
        print("Creating video table")

video_table()


#inserting values to table
cur = mydb.cursor()    
for videos in vi_info:
    try:
        sql = """INSERT INTO videos(Channel_Name,Channel_Id,Video_Id,Video_Name,Video_Description,Published_Date,View_Count,Like_Count,Dislike_Count,Favorite_Count,Comment_Count,Duration,Thumbnail,Caption_Status) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
        published_date = datetime.strptime(videos['Published_Date'], "%Y-%m-%dT%H:%M:%SZ")
        
        match = re.match(r'^PT(\d+H)?(\d+M)?(\d+S)?$',videos['Duration'])
        if match:
            hours = int(match.group(1)[:-1]) if match.group(1) else 0
            minutes = int(match.group(2)[:-1]) if match.group(2) else 0
            seconds = int(match.group(3)[:-1]) if match.group(3) else 0
            total_seconds = hours * 3600 + minutes * 60 + seconds
            duration_formatted = '{:02}:{:02}:{:02}'.format(hours, minutes, seconds)
        else:
            duration_formatted = None
 
        val = (videos['Channel_Name'], videos['Channel_Id'], videos['Video_Id'],videos['Video_Name'],videos['Video_Description'],published_date,
               videos['View_Count'],videos['Like_Count'],videos['Dislike_Count'], videos['Favorite_Count'], videos['Comment_Count'],duration_formatted,
               videos['Thumbnail'],videos['Caption_Status'])
        cur.execute(sql, val)
        mydb.commit()
    
    except Exception as e:
        print("Error inserting records:", e)

#table creation for comment details
def comments_table():
    try:
        create_query = '''create table if not exists comments(Comment_Id varchar(100),
                                                                Video_Id varchar(100),
                                                                Comment_Text text,
                                                                Comment_Author varchar(100),
                                                                Comment_Published_Date varchar(100))'''
        cur.execute(create_query)
        mydb.commit()

    except:
        print("Creating channel table")
comments_table()

#converting from list to tuple
comments = []
for i in com_info:
    comments.append(tuple(i.values()))

cur = mydb.cursor()
sql = "INSERT INTO comments(Comment_Id,Video_Id,Comment_Text,Comment_Author,Comment_Published_Date) VALUES (%s,%s,%s,%s,%s)"
val= comments
cur.executemany(sql, val)
mydb.commit()

def tables(channel_name):
    
    channel_table(channel_name)
    playlist_table(channel_name)
    video_table(channel_name)
    comments_table(channel_name)

    return "Tables created Successfully"

# Function to fetch channel details from the database and convert to DataFrame
def fetch_ch_db():
    cur.execute("SELECT * FROM channel")
    data = cur.fetchall()
    df1 = pd.DataFrame(data, columns=["Channel_Id", "Channel_Name", "Channel_Subscribers", "Channel_Views", "Channel_Description", "Channel_Published","Total_Video"])

    for index,row in df1.iterrows():
        insert_query = '''insert into channel(Channel_Id,
                                            Channel_Name,
                                            Channel_Subscribers,
                                            Channel_Views,
                                            Channel_Description,
                                            Channel_Published,
                                            Total_Video)'''
    st.dataframe(df1)

# Function to fetch comment details from the database and convert to DataFrame
def fetch_comm_db():
    cur.execute("SELECT * FROM comments")
    data = cur.fetchall()
    df2 = pd.DataFrame(data, columns=["Comment_Id", "Video_Id", "Comment_Text", "Comment_Author", "Comment_Published_Date"])
    
    for index,row in df2.iterrows():
        insert_query = '''insert into comments(Comment_Id,
                                            Video_Id,
                                            Comment_Text,
                                            Comment_Author,
                                            Comment_Published_Date)'''
    
    st.dataframe(df2)

# Function to fetch playlist details from the database and convert to DataFrame
def fetch_pl_db():
    cur.execute("SELECT * FROM playlist")
    data = cur.fetchall()
    df3 = pd.DataFrame(data, columns=["Playlist_Id", "Channel_Id", "Playlist_Name","Channel_Name","Video_Count"])
    
    for index,row in df3.iterrows():
        insert_query = '''insert into playlist(Playlist_Id,
                                                Channel_Id,
                                                Playlist_Name,
                                                Channel_Name,
                                                Video_Count)'''
    
    st.dataframe(df3)

    # Function to fetch video details from the database and convert to DataFrame
def fetch_vi_db():
    cur.execute("SELECT * FROM videos")
    data = cur.fetchall()
    df4 = pd.DataFrame(data, columns=["Channel_Name","Channel_Id","Video_Id", "Video_Name", "Video_Description","Published_Date","View_Count","Like_Count","Dislike_Count","Favorite_Count","Comment_Count","Duration","Thumbnail","Caption_Status"])
    
    for index,row in df4.iterrows():
        insert_query = '''insert into videos(Channel_Name,
                                            Channel_Id,
                                            Video_Id,
                                            Video_Name,
                                            Video_Description,
                                            Published_Date,
                                            View_Count,
                                            Like_Count,
                                            Dislike_Count,
                                            Favorite_Count,
                                            Comment_Count,
                                            Duration,
                                            Thumbnail,
                                            Caption_Status)'''
    st.dataframe(df4)


show_table=st.radio("# :green[Select the table for View:-]",("CHANNEL","PLAYLIST","VIDEOS","COMMENTS"))

if show_table=="CHANNEL":
    fetch_ch_db()

elif show_table=="PLAYLIST":
    fetch_pl_db()

elif show_table=="VIDEOS":
    fetch_vi_db()

elif show_table=="COMMENTS":
    fetch_comm_db()

#Creating queries 
ques=st.selectbox("# :blue[Select the questions that you would like to query:]",
                                    ["1.What are the names of all videos and their corresponding channels?",
                                    "2.Which channels have the most number of videos, and how many videos do they have?",
                                    "3.What are the top 10 most viewed videos and their respective channels?",
                                    "4.How many comments were made on each video, and what are their corresponding video names?",
                                    "5.Which videos have the highest number of likes, and what are their corresponding channel names?",
                                    "6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
                                    "7.What is the total number of views for each channel, and what are their corresponding channel names?",
                                    "8.What are the names of all the channels that have published videos in the year 2022?",
                                    "9.What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                                    "10.Which videos have the highest number of comments, and what are their corresponding channel names?"])

if ques=="1.What are the names of all videos and their corresponding channels?":
    query1 = '''SELECT Channel_Name AS Channel_Title,Video_Name AS Video_Title  from videos'''
    cur.execute(query1)
    mydb.commit()
    q1=cur.fetchall()
    df=pd.DataFrame(q1,columns=["Channel_Title","Video_Title"])
    st.write(df)

elif ques=="2.Which channels have the most number of videos, and how many videos do they have?":
    query2 = '''SELECT Channel_Name AS Channel_Tile,Total_Videos AS No_Of_Videos from channel order by Total_Videos desc'''
    cur.execute(query2)
    mydb.commit()
    q2=cur.fetchall()
    df2=pd.DataFrame(q2,columns=["Channel_Tile","No_Of_Videos"])
    st.write(df2)
    
elif ques=="3.What are the top 10 most viewed videos and their respective channels?":
    query3 = '''SELECT Channel_Name AS Channel_Title,Video_Name AS Video_Title,View_Count AS Views from videos where 
                View_Count is not null order by View_Count desc limit 10;'''
    cur.execute(query3)
    mydb.commit()
    q3=cur.fetchall()
    df3=pd.DataFrame(q3,columns=["Channel_Title","Video_Title","Views"])
    st.write(df3)

elif ques=="4.How many comments were made on each video, and what are their corresponding video names?":
    query4 = '''SELECT Video_Name AS Videos_title ,Comment_Count AS No_Of_Comments FROM videos where Comment_Count is not null;'''
    cur.execute(query4)
    mydb.commit()
    q4=cur.fetchall()
    df4=pd.DataFrame(q4,columns=["Videos_title","No_Of_Comments"])
    st.write(df4)

elif ques=="5.Which videos have the highest number of likes, and what are their corresponding channel names?":
    query5 = '''SELECT Channel_Name AS Channel_Title,Video_Name AS Video_Title, Like_Count AS Likes FROM videos where Like_Count is not null order by Like_Count desc;'''
    cur.execute(query5)
    mydb.commit()
    q5=cur.fetchall()
    df5=pd.DataFrame(q5,columns=["Channel_Title","Videos_title","Likes"])
    st.write(df5)
    
elif ques=="6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
    query6 = '''SELECT Video_Name AS Video_Title,Like_Count AS Likes ,Dislike_Count AS Dislikes FROM videos;'''
    cur.execute(query6)
    mydb.commit()
    q6=cur.fetchall()
    df6=pd.DataFrame(q6,columns=["Videos_title","Likes","Dislikes"])
    st.write(df6)

elif ques=="7.What is the total number of views for each channel, and what are their corresponding channel names?":
    query7 ='''SELECT Channel_Name AS Channel_Title,Channel_Views AS Channel_Views FROM channel;'''
    cur.execute(query7)
    mydb.commit()
    q7=cur.fetchall()
    df7=pd.DataFrame(q7,columns=["Channel_Title","Channel_Views"])
    st.write(df7)

elif ques=="8.What are the names of all the channels that have published videos in the year 2022?":
    query8 = '''SELECT Channel_Name AS Channel_Title,Video_Name AS Video_Title,Published_Date AS Video_Published FROM videos where extract(year from Published_Date)=2022;'''
    cur.execute(query8)
    mydb.commit()
    q8=cur.fetchall()
    df8=pd.DataFrame(q8,columns=["Channel_Title","Video_Title","Video_Published"])
    st.write(df8)

elif ques=="9.What is the average duration of all videos in each channel, and what are their corresponding channel names?":
    query9 = '''SELECT Channel_Name AS Channel_Title,AVG(Duration) AS Avg_dur FROM videos GROUP BY Channel_Name;'''
    cur.execute(query9)
    mydb.commit()
    q9=cur.fetchall()
    df9=pd.DataFrame(q9,columns=["Channel_Title","Avg_Dur"])
    
    T9 = []
    for index,row in df9.iterrows():
        channelname=row["Channel_Title"]
        avg_dur=row["Avg_Dur"]
        avg_dur_str=row["Avg_Dur"]
        T9.append(dict(Channel_Title=channelname,Avg_Dur=avg_dur_str)) 
    df1=pd.DataFrame(T9)
    st.write(df9)

elif ques == "10.Which videos have the highest number of comments, and what are their corresponding channel names?":
    query10 = '''SELECT Channel_Name AS Channel_Title, Video_Name AS Video_Title, Comment_Count AS Comments FROM videos WHERE Comment_Count IS NOT NULL ORDER BY Comment_Count DESC;'''
    cur.execute(query10)
    mydb.commit()
    q10 = cur.fetchall()
    df10 = pd.DataFrame(q10, columns=["Channel_Title", "Video_Title", "Comments"])
    st.write(df10)
