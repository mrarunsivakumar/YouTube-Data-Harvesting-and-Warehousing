import streamlit as st
import pandas as pd
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from pymongo import MongoClient
from sqlalchemy import create_engine
# Create a connection to the MongoDB server
conn = MongoClient("mongodb://datascience:datadw34@ac-w9az9wo-shard-00-00.8r8qjvh.mongodb.net:27017,ac-w9az9wo-shard-00-01.8r8qjvh.mongodb.net:27017,ac-w9az9wo-shard-00-02.8r8qjvh.mongodb.net:27017/?ssl=true&replicaSet=atlas-ub3j2r-shard-0&authSource=admin&retryWrites=true&w=majority")
# Create a new database
db = conn["youtube_data"]
# Create a new collection
collection = db["channel_info"]

# Define YouTube API key and API service
API_KEY = 'AIzaSyBqvFI-8FMxfP_Hxva0-ftNCyGi0kTO9rU'
API_SERVICE_NAME = 'youtube'
API_VERSION = 'v3'

# Define function to search channels
def search_channels(query):
    youtube = build(API_SERVICE_NAME, API_VERSION, developerKey=API_KEY)

    # Call the search.list method to retrieve channels matching the specified query term.
    search_response = youtube.search().list(
        q=query,
        type='channel',
        part='id,snippet',
        maxResults=10
    ).execute()

    channels = []
    for search_result in search_response.get('items', []):
        if search_result['id']['kind'] == 'youtube#channel':
            channels.append({'channel_id': search_result['id']['channelId'],
                             'channel_name': search_result['snippet']['title']})

    return channels

# Define function to get channel information
def get_channel_info(channel_id):
    youtube = build(API_SERVICE_NAME, API_VERSION, developerKey=API_KEY)

    # Call the channels.list method to retrieve channel details
    channels_response = youtube.channels().list(
        id=channel_id,
        part='snippet,statistics'
    ).execute()

    channel_info = {}
    for channel in channels_response.get('items', []):
        if channel['kind'] == 'youtube#channel':
            channel_info = {'channel_name': channel['snippet']['title'],
                            'channel_id': channel['id'],
                            'subscription_count': channel['statistics']['subscriberCount'],
                            'channel_views': channel['statistics']['viewCount'],
                            'channel_description': channel['snippet']['description']}

    return channel_info

def search_videos(query):
    youtube = build(API_SERVICE_NAME, API_VERSION, developerKey=API_KEY)

    # Call the search.list method to retrieve videos matching the specified query term.
    search_response = youtube.search().list(
        q=query,
        type='video',
        part='id,snippet',
        maxResults=10
    ).execute()

    videos = []
    for search_result in search_response.get('items', []):
        if search_result['id']['kind'] == 'youtube#video':
            videos.append({'video_id': search_result['id']['videoId'],
                           'video_name': search_result['snippet']['title']})

    return videos

# Define function to get video details
def get_video_details(video_id):
    youtube = build(API_SERVICE_NAME, API_VERSION, developerKey=API_KEY)

    # Call the videos.list method to retrieve video details
    videos_response = youtube.videos().list(
        id=video_id,
        part='snippet,contentDetails,statistics'
    ).execute()

    video_details = {}
    for video in videos_response.get('items', []):
        if video['kind'] == 'youtube#video':
            video_details = {'video_id': video['id'],
                             'channel_id': video['snippet']['channelId'],
                             'video_name': video['snippet']['title'],
                             'video_description': video['snippet']['description'],
                             'tags': video['snippet'].get('tags', []),
                             'published_at': video['snippet']['publishedAt'],
                             'view_count': video['statistics']['viewCount'],
                             'like_count': video['statistics']['likeCount'],
                             'dislike_count' : video['statistics'].get('dislikeCount', 0),
                             'favorite_count': video['statistics']['favoriteCount'],
                             'comment_count': video['statistics']['commentCount'],
                             'duration': video['contentDetails']['duration'],
                             'thumbnail': video['snippet']['thumbnails']['high']['url'],
                             'caption_status': video['contentDetails']['caption']}

    return video_details

     
# Function to get comments for a given video ID
def get_video_comments(video_id):
    comments = []
    youtube = build(API_SERVICE_NAME, API_VERSION, developerKey=API_KEY)
    response = youtube.commentThreads().list(
        part="snippet",
        videoId=video_id,
        textFormat="plainText"
    ).execute()
    while response:
        for item in response["items"]:
            comment = item["snippet"]["topLevelComment"]["snippet"]
            comment_id = comment.get("id")
            comment_text = comment.get("textDisplay")
            comment_author = comment.get("authorDisplayName")
            comment_published_at = comment.get("publishedAt")
            comments.append({
                "Comment_Id": comment_id,
                "Comment_Text": comment_text,
                "Comment_Author": comment_author,
                "Comment_PublishedAt": comment_published_at
            })
        if "nextPageToken" in response:
            response = youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                textFormat="plainText",
                pageToken=response["nextPageToken"]
            ).execute()
        else:
            break
    return comments
# Connect to SQL database
sql_engine = create_engine('mysql://root:arunsiva@localhost/youtube')
# Define tables
videos_table = "videos"
comments_table = "comments"
channels_table = "channel"


# Define function to migrate data to SQL database

def migrate_to_sql(channel_info):
    # Insert data into SQL table
    sql_connection = sql_engine.connect()
    sql_connection.execute(f"INSERT INTO {channels_table} (`Channel Information`, `Channel Name`, `Channel Id`, `Subscription Count`, `Channel Views`, `Channel Description`) VALUES (\"{str(channel_info)}\", '{channel_info['channel_name']}', '{channel_info['channel_id']}', {channel_info['subscription_count']}, {channel_info['channel_views']}, '{channel_info['channel_description']}')")
    sql_connection.close()


    
def migrate_video_details_to_sql(video):
    # Insert data into SQL table
    sql_connection = sql_engine.connect()
    sql_connection.execute(f"INSERT INTO {videos_table} (`video_id`, `video_name`, `channel_id`, `video_description`, `tags`, `published_at`, `view_count`, `like_count`, `dislike_count`, `favorite_count`, `comment_count`, `duration`, `thumbnail`, `caption_status`) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", 
                                (video['video_id'], 
                                 video['video_name'], 
                                 video['channel_id'],
                                 video['video_description'],
                                 str(video['tags']),
                                 video['published_at'], 
                                 video['view_count'], 
                                 video['like_count'], 
                                 video['dislike_count'], 
                                 video['favorite_count'], 
                                 video['comment_count'], 
                                 video['duration'],
                                 video['thumbnail'], 
                                 video['caption_status']))
    sql_connection.close()

def migrate_comments_to_sql(comments):
    # Insert data into SQL table
    sql_connection = sql_engine.connect()
    for comment in comments:
        sql_connection.execute(f"INSERT INTO {comments_table} (`Comment_Id`, `Comment_Text`, `Comment_Author`, `Comment_PublishedAt`, `Video_Id`) VALUES ('{comment['Comment_Id']}', '{comment['Comment_Text']}', '{comment['Comment_Author']}', '{comment['Comment_PublishedAt']}', '{comment['Video_Id']}')")
    sql_connection.close()

def app():
    st.title('YouTube Data Harvesting')
    option = st.sidebar.selectbox('Select an option', ['Search YouTube Channels', 'Search YouTube Videos', 'Search YouTube Video Comments'])

    if option == 'Search YouTube Channels':
        st.write('<h1 style="color: red;">Search YouTube Channels</h1>', unsafe_allow_html=True)
        query = st.text_input('Enter a search query:')
        if query:
            channels = search_channels(query)
            if channels:
                st.write('Search Results:')
                for channel in channels:
                    if st.button(channel['channel_name']):
                        channel_info = get_channel_info(channel['channel_id'])
                        st.write('Channel Information:')
                        st.write('Channel Name:', channel_info['channel_name'])
                        st.write('Channel Id:', channel_info['channel_id'])
                        st.write('Subscription Count:', channel_info['subscription_count'])
                        st.write('Channel Views:', channel_info['channel_views'])
                        st.write('Channel Description:', channel_info['channel_description'])

                        # Convert the dictionary into a pandas DataFrame
                        channel_df = pd.DataFrame(channel_info, index=[0])
                        st.write('Channel DataFrame:')
                        st.write(channel_df)
                        # Insert the dictionary into the MongoDB collection
                        collection.insert_one(channel_info)
                        st.success('Channel data inserted into MongoDB')
                    
                        # Migrate channel name to SQL database
                        # Call migrate_to_sql function after retrieving channel information
                        migrate_to_sql(channel_info)
                        st.success('Channel name migrated to SQL database')
    elif option == 'Search YouTube Videos':
        st.title('Search YouTube Videos')
        query = st.text_input('Enter a search query:')
        if query:
            videos = search_videos(query)
            if videos:
                st.write('Search Results:')
                table_columns = ['Video_Id_1', 'Video_Name', 'Video_Description', 'Tags', 'PublishedAt', 'View_Count',
                                 'Like_Count', 'Dislike_Count', 'Favorite_Count', 'Comment_Count', 'Duration',
                                 'Thumbnail', 'Caption_Status']
                table_data = []
                for video in videos:
                    if st.button(video['video_name']):
                        video_details = get_video_details(video['video_id'])
                        row = [video_details['video_id'], video_details['video_name'], video_details['video_description'],
                               ', '.join(video_details['tags']), video_details['published_at'], video_details['view_count'],
                               video_details['like_count'], video_details['dislike_count'], video_details['favorite_count'],
                               video_details['comment_count'], video_details['duration'], video_details['thumbnail'],
                               video_details['caption_status']]
                        table_data.append(row)

                
                    
                    video_df = pd.DataFrame(table_data, columns=table_columns)
                    st.write(video_df)
                    # Insert the dictionary into the MongoDB collection
                    collection.insert_one(video)
                    st.success('Video data inserted into MongoDB')

                    # Migrate video details to SQL database
                    # Call migrate_video_details_to_sql function after retrieving video details
                    migrate_video_details_to_sql(video)
                    st.success('Video details migrated to SQL database')
    else:
        st.title('Search for YouTube Video Comments')
        video_url = st.text_input('Enter YouTube video URL')
        if video_url:
            video_id = video_url.split('v=')[-1]
            comments = get_video_comments(video_id)
            if comments:
                st.write(f'Found {len(comments)} comments for video ID {video_id}:')
                table_data = []
                for comment in comments:
                    row = [comment['Comment_Id'], comment['Comment_Text'], comment['Comment_Author'], comment['Comment_PublishedAt']]
                    table_data.append(row)
                # Insert the dictionary into the MongoDB collection
                collection.insert_many(comments)
                st.success('Comment data inserted into MongoDB')
                
                table_columns = ['Comment_Id', 'Comment_Text', 'Comment_Author', 'Comment_PublishedAt']
                comment_df= pd.DataFrame(table_data, columns=table_columns)
                st.write(comment_df)
            else:
                st.write(f'No comments found for video ID {video_id}')
                # Migrate comments to SQL database
                # Call migrate_comments_to_sql function after retrieving comments
                migrate_comments_to_sql(comments)
                st.success('Comments migrated to SQL database')
               # Close the connection to the MongoDB server
    conn.close()                 

if __name__ == '__main__':
    app()
