import os

import pandas as pd
import requests
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get the API key from the .env file
API_KEY = os.getenv('GOOGLE_API_KEY')

# Function to get latitude and longitude using Google Geocoding API
def get_lat_lng(address):
    base_url = "https://maps.googleapis.com/maps/api/geocode/json"
    params = {
        "address": address,
        "key": API_KEY
    }
    response = requests.get(base_url, params=params)
    if response.status_code == 200:
        data = response.json()
        print(data)
        if data['status'] == 'OK':
            location = data['results'][0]['geometry']['location']
            return location['lat'], location['lng']
    return None, None

# Read the CSV file
input_csv = 'real_estate_listings.csv'
output_csv = 'real_estate_listings_with_coordinates.csv'

# Load the CSV into a pandas DataFrame
df = pd.read_csv(input_csv)

# Add new columns for latitude and longitude
df['latitude'] = None
df['longitude'] = None

# Iterate through each row and get coordinates
for index, row in df.iterrows():
    address = row['address']
    if pd.notna(address):  # Check if the address is not empty
        lat, lng = get_lat_lng(address)
        df.at[index, 'latitude'] = lat
        df.at[index, 'longitude'] = lng
        print(f"Processed: {address} -> Latitude: {lat}, Longitude: {lng}")
        
        # Break after processing the first row
        break
    else:
        print(f"Skipping empty address at row {index}")

# Save the updated DataFrame to a new CSV file
df.to_csv(output_csv, index=False)
print(f"Updated CSV saved to {output_csv}")