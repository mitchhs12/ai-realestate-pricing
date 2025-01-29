import os

import pandas as pd
import requests
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get the API key from the .env file
API_KEY = os.getenv('GOOGLE_API_KEY')

# Define the types of amenities to search for
AMENITY_TYPES = [
    "gym",
    "supermarket",
    "school",
    "restaurant",
    "cafe",
    "transit_station",  
    "park"              
]

# Function to find nearby amenities using Places API (New) Nearby Search
def find_nearby_amenities(latitude, longitude, radius=2000):
    base_url = "https://places.googleapis.com/v1/places:searchNearby"
    headers = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": API_KEY,
        "X-Goog-FieldMask": "places.displayName,places.location"  # Fields to return
    }
    amenities = {}
    
    for amenity_type in AMENITY_TYPES:
        payload = {
            "includedPrimaryTypes": [amenity_type],
            "rankPreference": "DISTANCE",
            "maxResultCount": 1,  # Get only the closest result
            "locationRestriction": {
                "circle": {
                    "center": {
                        "latitude": latitude,
                        "longitude": longitude
                    },
                    "radius": radius
                }
            }
        }
        response = requests.post(base_url, headers=headers, json=payload)
        if response.status_code == 200:
            data = response.json()
            print(amenity_type, data)
            if 'places' in data and len(data['places']) > 0:
                closest_amenity = data['places'][0]
                amenities[amenity_type] = {
                    "name": closest_amenity['displayName']['text'],
                    "location": closest_amenity['location'],
                    "distance": None  # Distance will be calculated later
                }
        else:
            print(f"Error fetching data for {amenity_type}: {response.status_code}")
            print(response.json())  # Print error details
    
    return amenities


# Read the CSV file with property data
input_csv = 'real_estate_listings_with_coordinates.csv'
output_csv = 'real_estate_listings_with_amenities.csv'

# Load the CSV into a pandas DataFrame
df = pd.read_csv(input_csv)

# Add columns for each amenity type
for amenity_type in AMENITY_TYPES:
    df[f'nearest_{amenity_type}'] = None
    df[f'distance_to_{amenity_type}'] = None

# Iterate through each row and find nearby amenities
for index, row in df.iterrows():
    latitude = row['latitude']
    longitude = row['longitude']
    
    if pd.notna(latitude) and pd.notna(longitude):
        print(f"Processing property at ({latitude}, {longitude})")
        
        # Find nearby amenities
        amenities = find_nearby_amenities(latitude, longitude)
        
        # Calculate distances and update the DataFrame
        for amenity_type, details in amenities.items():
            destination_lat = details['location']['latitude']
            destination_lng = details['location']['longitude']
                        
            df.at[index, f'nearest_{amenity_type}'] = details['name']
            
            print(f"Found {amenity_type}: {details['name']}")
    else:
        print(f"Skipping property at row {index} due to missing coordinates")

# Save the updated DataFrame to a new CSV file
df.to_csv(output_csv, index=False)
print(f"Updated CSV saved to {output_csv}")