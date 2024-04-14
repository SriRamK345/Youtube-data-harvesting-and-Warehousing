
import googleapiclient.discovery #Youtube API libraries
import pymongo #mongoDB import
import mysql.connector #MY SQL 
import pandas as pd #pandas
import streamlit as st #streamlit
from streamlit_option_menu import option_menu


# BUILDING CONNECTION WITH YOUTUBE API

Api_Key="AIzaSyC6g5XUQOSSOjmqWznbvzFD-leZEHyx_BQ"
api_service_name = "youtube"
api_version = "v3"
youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=Api_Key)

# FUNCTION TO GET CHANNEL DETAILS
def channels_det(channel_id):
    request = youtube.channels().list(part="snippet,contentDetails,statistics", id=channel_id)
    details = request.execute()
    
    for i in details.get("items"):
        channel_details = dict(
            Channel_Name=i["snippet"]["title"],
            Channel_Id=i["id"],
            Sub_Count=i["statistics"]["subscriberCount"],
            Views=i["statistics"]["viewCount"],
            Total_Videos=i['statistics']['videoCount'],
            Description=i["snippet"]["localized"]["description"],
            Playlist_Id=i["contentDetails"]["relatedPlaylists"]["uploads"]
        )
    return channel_details

def videos_id(channel_id):                          # FUNCTION TO GET VIDEO IDS
    
    video_ids_list = []
    vid_details = youtube.channels().list(          # get playlist id from channel_id
        id=channel_id, 
        part="contentDetails"
    ).execute()                                     # excute all datas in channel list
                                    
    get_plist_id = vid_details['items'][0]['contentDetails']['relatedPlaylists']['uploads']  # get playlist id from channel list
    
    next_page_token = None                       # To execute all videos id
    
    while True:
        videos = youtube.playlistItems().list(                   
            playlistId=get_plist_id, 
            part="snippet", 
            maxResults=50,
            pageToken=next_page_token
        ).execute()
        
        for i in videos["items"]:
            video_ids_list.append(i["snippet"]["resourceId"]["videoId"])
        
        next_page_token = videos.get('nextPageToken')
        
        if not next_page_token:
            break

    return video_ids_list

def video_detail(Videos_ids):                       # FUNCTION TO GET VIDEO DETAILS
        video_list=[]
        for video_id in Videos_ids:         
                video_response = youtube.videos().list(
                        part="snippet,contentDetails,statistics",
                        id=video_id,
                        ).execute()

                for i in video_response["items"]:
                        video_data  = dict(
                        Channel_Name = i ["snippet"]["channelTitle"],
                        Channel_id = i['snippet']['channelId'],
                        Video_id = i['id'],
                        Video_Name = i['snippet']['title'],
                        Thumbnail = i ['snippet']['thumbnails']['default']['url'],
                        Description = i ['snippet']['description'],
                        PublishedAt = i ['snippet']['publishedAt'],
                        Duration = i ['contentDetails']['duration'],
                        Views_count = i ['statistics']['viewCount'],
                        Like_count = i ['statistics'].get('likeCount'),
                        Comments = i ['statistics'].get('commentCount'),
                        Definition = i ['contentDetails']['definition'],
                        Caption_status = i ['contentDetails']['caption']
                        )
                        video_list.append(video_data)
        return video_list

def comment_details(Videos_ids):                    # FUNCTION TO GET COMMENT DETAILS
    comm_list = []
    try:
        for i in Videos_ids:      #Total_Videos
            comm_response = youtube.commentThreads().list(
                                    part="snippet",
                                    videoId= i,
                                    maxResults=100).execute()

            for j in comm_response['items']:
                data = dict(Comment_id = j ['id'],
                            Video_id = j ['snippet']['videoId'],
                            Comment_text = j ['snippet']['topLevelComment']['snippet']['textDisplay'],
                            author = j ['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            posted_date = j ['snippet']['topLevelComment']['snippet']['publishedAt']
                )
                comm_list.append(data)

    except:
        pass
    return comm_list

#connection with MongoDB Atlas
client = pymongo.MongoClient("mongodb+srv://cadberry:India123@cluster0.14uuekr.mongodb.net/?retryWrites=true&w=majority")
#creating a new database to store all the colections
dbase = client["youtube_details"] 

# Function to store all details in Mongo DB
def combine_data(channel_id,selected_channel): 
    Videos_ids = videos_id(channel_id)
    ch_details = channels_det(channel_id)
    v_details = video_detail(Videos_ids)
    comm_details = comment_details(Videos_ids)

    # Creating a dictionary to store combined data
    collect1 = dbase["combined_data"]
    collect1.insert_one({                       # Inserting the combined data into MongoDB
        'Channel_Details': ch_details,
        'Video_Details': v_details, 
        'Comments_details': comm_details
    })

    return "Data Combined and Uploaded successfully"


# Establish a connection to the MySQL server
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="India@123",
    database="Youtube_DB",
    port=3306
)
# Create a cursor object to execute SQL queries
cursor = conn.cursor()

#creating the table for the channel details and the same is done for videos and comments
def channels_table(selected_channel):
    # Establish a connection to the MySQL server
    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="India@123",
        database="Youtube_DB",
        port=3306
    )
    
    # Create a cursor object to execute SQL queries
    cursor = conn.cursor()

    try:
        cursor.execute(
            '''create table if not exists channels(
                channelName varchar(80),
                channelId varchar(80) primary key, 
                subscribers bigint, 
                views bigint,
                totalVideos int, 
                channel_description text,
                playlistId varchar(80) 
                )'''
                )
        conn.commit()
    except:
        conn.rollback()

 #  Channel details FROM MONGODB       
    channel= []
    dbase = client["youtube_details"]
    collect1 = dbase["combined_data"]
    data = collect1.find({"Channel_Details.Channel_Name": selected_channel}, {"_id": 0, "Channel_Details": 1})

    for i in data:
        channel.append(i["Channel_Details"])
    df=pd.DataFrame(channel)

    #Inserting channel data into table
    for _ , row in df.iterrows():
        insert_qry = """                     
            INSERT INTO channels (channelName, channelId, subscribers, views, totalVideos, channel_description,playlistId)
            VALUES (%s, %s, %s, %s, %s, %s, %s)               
        """
        values = (                              # insert into (column in sql)
            row['Channel_Name'],                # values (column name in mongoDB)
            row['Channel_Id'],
            row['Sub_Count'],
            row['Views'],
            row['Total_Videos'],
            row['Description'],
            row['Playlist_Id']
        )
        
        try:
            cursor.execute(insert_qry,values)
            conn.commit()
        except:
            print("values already exists in the channel table")

from datetime import datetime

def videos_table(selected_channel):
    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="India@123",
        database="Youtube_DB",
        port=3306
    )

    # Create a cursor object to execute SQL queries
    cursor = conn.cursor()
    cursor.execute('''create table if not exists videos
                    (
                    channelName varchar(80),
                    channelId varchar(80),
                    videoId varchar(50) primary key, 
                    VideoName varchar(200),
                    Thumbnail varchar(200),
                    description text,
                    publishedAt timestamp,
                    duration varchar(20),
                    viewCount bigint, 
                    likeCount bigint,
                    commentCount int,
                    definition varchar(10), 
                    caption varchar(50)
                    )'''
                    )
    conn.commit()
    
    #  Video details FROM MONGODB
   
    video= []
    dbase = client["youtube_details"]
    collect1 = dbase["combined_data"]
    data = collect1.find({"Channel_Details.Channel_Name": selected_channel}, {"_id": 0, "Video_Details": 1})

    for i in data:
        for j in range(len(i["Video_Details"])):
            video.append(i["Video_Details"][j])
    df=pd.DataFrame(video)

    for _ , row in df.iterrows():    # Assume df1 is DataFrame
        published_at = datetime.strptime(row['PublishedAt'], '%Y-%m-%dT%H:%M:%SZ') 
        duration = row['Duration']  # Original duration string from MongoDB
        # Convert duration format
        duration = duration[2:]  # Remove the 'PT' prefix
        hours, minutes, seconds = 0, 0, 0
        if 'H' in duration:
            hours = int(duration.split('H')[0])
            duration = duration.split('H')[1]
        if 'M' in duration:
            minutes = int(duration.split('M')[0])
            duration = duration.split('M')[1]
        if 'S' in duration:
            seconds = int(duration.split('S')[0])
        formatted_duration = '{:02d}:{:02d}:{:02d}'.format(hours, minutes, seconds)  # Format into HH:MM:SS
        
        insert_qry = """
        INSERT INTO videos ( channelName, channelId,  videoId, VideoName, Thumbnail, 
        description, publishedAt, duration, viewCount, likeCount, commentCount, definition, caption)
        
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

        values = (
        row['Channel_Name'],
        row['Channel_id'],
        row['Video_id'],
        row['Video_Name'],
        row['Thumbnail'],
        row['Description'],
        published_at,
        formatted_duration,
        row['Views_count'],
        row['Like_count'],
        row['Comments'],
        row['Definition'],
        row['Caption_status']
    )
        cursor.execute(insert_qry,values)
        conn.commit()
        
def comment_table(selected_channel):
    conn = mysql.connector.connect(
            host="localhost",
            user="root",
            password="India@123",
            database="Youtube_DB",
            port=3306
        )


    # Create a cursor object to execute SQL queries
    cursor = conn.cursor()
    cursor.execute("""create table if not exists comments(
                        Comment_id varchar(80) primary key,
                        video_id varchar(80),
                        Comment_text text,
                        author varchar(80),
                        posted_date timestamp)
                        """)
    conn.commit()

 #  comments details FROM MONGODB

    comments = []
    dbase = client["youtube_details"]
    collect = dbase["combined_data"]
    data = collect.find({"Channel_Details.Channel_Name": selected_channel}, {"_id": 0, "Comments_details": 1})

    for i in data:
        for j in range(len(i["Comments_details"])):
            comments.append(i["Comments_details"][j])
    df=pd.DataFrame(comments)

    for _ , row in df.iterrows():
        posted_date = datetime.strptime(row['posted_date'], '%Y-%m-%dT%H:%M:%SZ')
        
        insert_qry = """                     
            INSERT INTO comments (Comment_id, Video_id, Comment_text, author, posted_date)
            VALUES (%s, %s, %s, %s, %s)"""                  # insert into (column in sql)
                                                            # values (column name in mongoDB)      
        
        values = (
            row['Comment_id'],
            row['Video_id'],
            row['Comment_text'],
            row['author'],
            posted_date
        )
        
        cursor.execute(insert_qry,values)
        conn.commit()

# Function to run all tables
def all_tables(selected_channel):
    channels_table(selected_channel)
    videos_table(selected_channel)
    comment_table(selected_channel)
    return ("Migatred to SQL sucessfully")

def display_channels():                         # Function to display channels table
    channel= []
    dbase = client["youtube_details"]
    collect1 = dbase["combined_data"]
    data=collect1.find({},{"_id":0,"Channel_Details":1})
    for i in data:
        channel.append(i["Channel_Details"])
    df=st.table(channel)
    return df

def display_videos():                           # Function to display videos table
    video= []
    dbase = client["youtube_details"]
    collect1 = dbase["combined_data"]
    data=collect1.find({},{"_id":0,"Video_Details":1})
    for i in data:
        for j in range(len(i["Video_Details"])):
            video.append(i["Video_Details"][j])
    df1=st.table(video)
    return df1

def display_comments():                         # Function to display comments table
    comments = []
    dbase = client["youtube_details"]
    collect = dbase["combined_data"]
    data=collect.find({},{"_id":0,"Comments_details":1})
    for i in data:
        for j in range(len(i["Comments_details"])):
            comments.append(i["Comments_details"][j])
    df2=st.table(comments)
    return df2

# Set page title in Streamlit

st.title(":red[YouTube Data Harvesting and Warehousing using SQL, MongoDB and Streamlit]")

with st.sidebar:  
    selected = option_menu("Menu", ["Data Collection","Table View","SQL Query"])
    if selected == "Data Collection":
        #st.subheader("**:green[Data Collection]**")
        channel_id = st.text_input("Enter the Channel ID:")
        if st.button("**Collect and Store Data**"):
            if not channel_id:
                st.warning("Please enter a valid Channel ID")
            else:
                channel_ids = []
                dbase = client["youtube_details"]
                collect1 = dbase["combined_data"]
                data = collect1.find({}, {"_id": 0, "Channel_Details": 1})
                for i in data:
                    channel_ids.append(i["Channel_Details"]["Channel_Id"])

                if channel_id in channel_ids:
                    st.warning(f"Channel details of the given channel id '{channel_id}' already exist")
                else:
                    channels = [i["Channel_Details"]["Channel_Name"] for i in data]
                    selected_channel = ("Select Channel:", channels)
                    combine_data(channel_id, selected_channel)
                    st.success(f"Data for the given channel ID '{channel_id}' collected and stored in Mongo DB.")

if selected =="Table View":
    # Dropdown to view existing channels
    st.subheader("**:green[Channels list in Mongo DB]**")
    dbase = client["youtube_details"]
    collect1 = dbase["combined_data"]
    data=collect1.find({},{"_id":0,"Channel_Details":1})
    channels = [i["Channel_Details"]["Channel_Name"] for i in data]
    selected_channel = st.selectbox("Select Channel:", channels, index=0)
    # Migrate from MongoDB to SQL
    if st.sidebar.button("**Migrate to SQL**"):
        try:
            table = all_tables(selected_channel)
            st.success(table)
        except:
            #pass
            st.warning(f"The channel '{selected_channel}' already exists in SQL database.")

    st.subheader("**:green[Select Table and Section]**")
    # Show date in MONGO DB
    show_table = st.selectbox("Data's in MONGO DB in Table format", ("------","CHANNELS", "VIDEOS", "COMMENTS"))
    # Show date in SQL
    selected_section = st.selectbox("Data's in SQL in Table format", ("------","CHANNELS TABLE", "VIDEOS TABLE", "COMMENTS TABLE"))

    if show_table == "CHANNELS":
        st.subheader("**:blue[CHANNEL DETAILS IN MONGO DB]**")
        display_channels()
    elif show_table == "VIDEOS":
        st.subheader("**:blue[VIDEO DETAILS IN MONGO DB]**")
        display_videos()
    elif show_table == "COMMENTS":
        st.subheader("**:blue[COMMENT DETAILS IN MONGO DB]**")
        display_comments()

    # Display SQL tables
    if selected_section == "CHANNELS TABLE":
        st.subheader("**:blue[CHANNEL DETAILS IN SQL]**")
        cursor.execute("SELECT * FROM channels")
        df_channels = pd.DataFrame(cursor.fetchall(), columns=["channelName", "channelId", "subscribers", "views", "totalVideos", "channel_description", "playlistId"])
        st.write(df_channels)

    if selected_section == "VIDEOS TABLE":
        st.subheader("**:blue[VIDEOS DETAILS IN SQL]**")
        cursor.execute("SELECT * FROM videos")
        df_videos = pd.DataFrame(cursor.fetchall(), columns=["channelName", "channelId",  "videoId", "VideoName", "Thumbnail", 
                "description", "publishedAt", "duration", "viewCount", "likeCount", "commentCount", "definition", "caption"])
        st.write(df_videos)

    if selected_section == "COMMENTS TABLE":
        st.subheader("**:blue[COMMENTS DETAILS IN SQL]**")
        cursor.execute("SELECT * FROM comments")
        df_comments = pd.DataFrame(cursor.fetchall(), columns=["Comment_id","video_id","Comment_text","author","posted_date"])
        st.write(df_comments)                                       

elif selected == "SQL Query":
 
    st.subheader("**:blue[SQL QUERY]**")
    question_tosql = st.selectbox('**Select any question**',
('1. What are the names of all the videos and their corresponding channels?',
'2. Which channels have the most number of videos, and how many videos do they have?',
'3. What are the top 10 most viewed videos and their respective channels?',
'4. How many comments were made on each video, and what are their corresponding video names?',
'5. Which videos have the highest number of likes, and what are their corresponding channel names?',
'6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
'7. What is the total number of views for each channel, and what are their corresponding channel names?',
'8. What are the names of all the channels that have published videos in the year 2022?',
'9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
'10. Which videos have the highest number of comments, and what are their corresponding channel names?'))

    if question_tosql=="1. What are the names of all the videos and their corresponding channels?":
        cursor.execute("""SELECT VideoName AS Video_Title, channelName AS Channel_Name FROM videos ORDER BY channelName""")  
        df = pd.DataFrame(cursor.fetchall(),columns=["Video_Title","Channel_Name"])
        st.write(df)

    elif question_tosql=="2. Which channels have the most number of videos, and how many videos do they have?":
        cursor.execute("""SELECT channelName AS Channel_Name, totalVideos AS Total_Videos FROM channels ORDER BY totalVideos DESC""")
        df1= pd.DataFrame(cursor.fetchall(),columns=["Channel_Name","Total_Videos"])
        st.write(df1)

    elif question_tosql == "3. What are the top 10 most viewed videos and their respective channels?":
        cursor.execute("""SELECT channelName AS Channel_Name, VideoName AS Video_Title, viewCount AS Views  FROM videos ORDER BY viewCount DESC LIMIT 10""")
        df2= pd.DataFrame(cursor.fetchall(),columns=["Channel_Name","Video_Title","Views"])
        st.write(df2)

    elif question_tosql == "4. How many comments were made on each video, and what are their corresponding video names?":
        cursor.execute("""SELECT commentCount as Comments, VideoName as Video_Name from videos where commentCount is not null AND commentCount <> 0""")
        df3= pd.DataFrame(cursor.fetchall(),columns=["Comments","Video_Name"])
        st.write(df3)

    elif question_tosql == "5. Which videos have the highest number of likes, and what are their corresponding channel names?":
        cursor.execute("""SELECT channelName AS Channel_Name, VideoName AS Title, likeCount AS Likes_Count FROM videos ORDER BY likeCount DESC LIMIT 10""")
        df4= pd.DataFrame(cursor.fetchall(),columns=["Channel_Name","Title","Likes_Count"])
        st.write(df4)

    elif question_tosql == "6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
        cursor.execute("""SELECT VideoName AS Title, likeCount AS Likes_Count FROM videos ORDER BY likeCount DESC""")
        df5= pd.DataFrame(cursor.fetchall(),columns=["Title","Likes_Count"])
        st.write(df5)

    elif question_tosql == "7. What is the total number of views for each channel, and what are their corresponding channel names?":
        cursor.execute("""SELECT channelName AS Channel_Name, views AS Views FROM channels ORDER BY views DESC""")
        df6= pd.DataFrame(cursor.fetchall(),columns=["Channel_Name","Views"])
        st.write(df6)

    elif question_tosql == "8. What are the names of all the channels that have published videos in the year 2022?":
        cursor.execute("""SELECT channelName AS Channel_Name FROM videos WHERE publishedAt LIKE '2022%' GROUP BY channelName ORDER BY channelName""")
        df7= pd.DataFrame(cursor.fetchall(),columns=["Channel_Name"])
        st.write(df7)

    elif question_tosql == "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?":
        cursor.execute("""SELECT videos.channelName, MAX(videos.duration) AS duration FROM videos JOIN channels ON channels.channelId = videos.channelId GROUP BY videos.channelName ORDER BY duration DESC""")
        df8= pd.DataFrame(cursor.fetchall(),columns=["Channel_Name","duration"])
        st.write(df8)

    elif question_tosql == "10. Which videos have the highest number of comments, and what are their corresponding channel names?":
        cursor.execute("""SELECT channelName AS Channel_Name,videoId AS Video_ID, commentCount AS Comments FROM videos ORDER BY commentCount DESC LIMIT 10""")
        df9= pd.DataFrame(cursor.fetchall(),columns=["Channel_Name","Video_ID","Comments"])
        st.write(df9)
