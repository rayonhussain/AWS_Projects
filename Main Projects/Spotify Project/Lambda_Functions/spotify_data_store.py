import json
import pandas as pd
import boto3
from io import StringIO
from datetime import datetime

def lambda_handler(event, context):

    s3 = boto3.client('s3')
    bucket_name = 'spotifyapiuseast1'
    object_key = 'raw_data/to_process/spotify_raw'+str(datetime.today().date())+'.json'  # Example: 'folder/subfolder/file.txt'
    response = s3.get_object(Bucket=bucket_name, Key=object_key)
    data = response['Body'].read().decode('utf-8')
    #print(type(data))
    #print(data)
    dict= json.loads(data)
    #print(dict['href'])
    data=dict
    
    
    #Album Dataframe
    album_id=[]
    album_track=[]
    album_rd=[]
    album_ext_url=[]
    album_tt=[]

    for row in data['items']:
        album_id.append(row['track']['album']['id'])
        album_track.append(row['track']['album']['name'])
        album_rd.append(row['track']['album']['release_date'])
        album_tt.append(row['track']['album']['total_tracks'])
        album_ext_url.append(row['track']['album']['external_urls']['spotify'])
        
    album_dict={'album_id':album_id,'album_track':album_track,'album_release_date':album_rd,'album_total_tracks':album_tt,'album_ext_url':album_ext_url}
    album_df=pd.DataFrame(album_dict)
    album_df.drop_duplicates(subset=['album_id'],inplace=True)
    album_df['album_release_date']=pd.to_datetime(album_df['album_release_date'])
    #print(album_df.head())
    
    
    #Artist Dataframe
    artist_list=[]
    for row in data['items']:
        for key,value in row.items():
            if key=='track':
                for artist in value['artists']:
                     artist_dict = {'artist_id':artist['id'], 'artist_name':artist['name'], 'external_url': artist['href']}
                     artist_list.append(artist_dict)
                     
    artist_df=pd.DataFrame(artist_list)
    
    #Song Dataframe
    song_list = []
    for row in data['items']:
        song_id = row['track']['id']
        song_name = row['track']['name']
        song_duration = row['track']['duration_ms']
        song_url = row['track']['external_urls']['spotify']
        song_popularity = row['track']['popularity']
        song_added = row['added_at']
        album_id = row['track']['album']['id']
        artist_id = row['track']['album']['artists'][0]['id']
        song_element = {'song_id':song_id,'song_name':song_name,'duration_ms':song_duration,'url':song_url,
                        'popularity':song_popularity,'song_added':song_added,'album_id':album_id,
                        'artist_id':artist_id
                        }
        song_list.append(song_element)

    song_df=pd.DataFrame(song_list)
    song_df['song_added']=pd.to_datetime(song_df['song_added'])
    
    #Pushing the data in S3 bucket
        
    csv_buffer = StringIO()
    album_df.to_csv(csv_buffer, index=False)
    filename='album_data'+str(datetime.today().date())+'.csv'
    bucket_name = 'spotifyapiuseast1'
    object_key = 'transform_data/album_data/'+filename
    # Upload the CSV content to S3
    s3.put_object(Bucket=bucket_name, Key=object_key, Body=csv_buffer.getvalue())
    
    csv_buffer = StringIO()
    artist_df.to_csv(csv_buffer, index=False)
    filename='artist_data'+str(datetime.today().date())+'.csv'
    bucket_name = 'spotifyapiuseast1'
    object_key = 'transform_data/artist_data/'+filename
    # Upload the CSV content to S3
    s3.put_object(Bucket=bucket_name, Key=object_key, Body=csv_buffer.getvalue())
    
    csv_buffer = StringIO()
    song_df.to_csv(csv_buffer, index=False)
    filename='song_data'+str(datetime.today().date())+'.csv'
    bucket_name = 'spotifyapiuseast1'
    object_key = 'transform_data/song_data/'+filename
    # Upload the CSV content to S3
    s3.put_object(Bucket=bucket_name, Key=object_key, Body=csv_buffer.getvalue()) 
    
    
    #Moving the origin file for the day
    bucket_name = 'spotifyapiuseast1'
    # Source and destination paths
    source_path = 'raw_data/to_process/spotify_raw'+str(datetime.today().date())+'.json'
    destination_path = 'raw_data/processed/spotify_raw'+str(datetime.today().date())+'.json'
    
    # Move object within the same bucket
    s3.copy_object(
        Bucket='spotifyapiuseast1',
        CopySource={'Bucket': bucket_name, 'Key': source_path},
        Key=destination_path
    )
    
    # Delete the original object
    s3.delete_object(Bucket='spotifyapiuseast1', Key=source_path)

    

    """ 
        dfs=[album_df,artist_df,song_df]
        for i in dfs:
            csv_buffer = StringIO()
            i.to_csv(csv_buffer, index=False)
            filename=str(i).split('_')[0]+'_data'+str(datetime.today().date())+'.csv'
            subdir=str(i).split('_')[0]+'_data/'
            bucket_name = 'spotifyapiuseast1'
            object_key = 'transform_data/'+subdir+filename
            # Upload the CSV content to S3
            s3.put_object(Bucket=bucket_name, Key=object_key, Body=csv_buffer.getvalue()) 
            
            
            """
    
    
    
