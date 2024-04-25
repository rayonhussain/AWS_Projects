import json
import os
import spotipy
import boto3
from datetime import datetime
from spotipy.oauth2 import SpotifyClientCredentials 

def lambda_handler(event, context):
        # TODO implement
    client_id = os.environ.get('client_key')
    client_secret = os.environ.get('client_secret')
    
    #add your keys here
    client_credentials_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
    
    sp = spotipy.Spotify(client_credentials_manager = client_credentials_manager)
    
    playlist_link = "https://open.spotify.com/playlist/37i9dQZEVXbNG2KDcFcKOF?si=1333723a6eff4b7f"
    playlist_URI = playlist_link.split("/")[-1].split('?')[0]
    
    data = sp.playlist_tracks(playlist_URI)
    #print(data)
    
    filename = "spotify_raw" + str(datetime.today().date()) + ".json"
    client = boto3.client('s3')
    client.put_object(
        Bucket="spotifyapiuseast1",
        Key="raw_data/to_process/" + filename,
        Body=json.dumps(data)
        
        )
        

    
    
