import os
import pandas as pd
import librosa
import numpy as np 
from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['mfcc_database']
collection = db['mfcc_collection']

# Path to the main folder
main_folder = 'fma_large'

# Load tracks.csv into a DataFrame, skipping first two rows and using the third row as column names
tracks_df = pd.read_csv('tracks.csv', skiprows=2, header=0)

# Select only the desired columns: title, track_id, and genres_all
tracks_df = tracks_df[['title', 'track_id', 'genres_all']]
print ( tracks_df.head())
# Function to calculate MFCC features

def calculate_mfcc(audio_path):
    audio_data, sampling_rate = librosa.load(audio_path, sr=20000)
    mfcc = librosa.feature.mfcc(y=audio_data, sr=sampling_rate, n_mfcc=13)
    mfcc_means = np.mean(mfcc, axis=1)
    return mfcc_means.tolist()


# Iterate over subfolders and MP3 files
for root, dirs, files in os.walk(main_folder):
    for file in files:
        if file.endswith('.mp3'):
            try:
                # Extract track ID from the filename
                track_id = str(file.split('.')[0].lstrip('0'))

                # Filter tracks DataFrame to find matching track ID
                track_info = tracks_df[tracks_df['track_id'] == track_id]
                
                print ( track_info.head() ) 
                
                if not track_info.empty:
                    genre_all = track_info['genres_all'].iloc[0]
                    title = track_info['title'].iloc[0]

                    audio_path = os.path.join(root, file)
                    mfcc_features = calculate_mfcc(audio_path)

                    data = {
                        'track_id': file ,
                        'genre_all': genre_all,
                        'title': title,
                        'mfcc_features': mfcc_features
                    }
                    # Store data in MongoDB
                    collection.insert_one(data)
                    print(f"Stored MFCC features for {audio_path} in MongoDB.")
                else:
                    print(f"No matching track info found for {track_id}")
            except Exception as e:
                print(f"Error processing {file}: {e}")