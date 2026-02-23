import pandas as pd
import requests

def fetch_chicago_new_construction():
    print("Fetching recent new construction permits from Chicago Data Portal...")
    
    # Chicago Building Permits API endpoint
    url = "https://data.cityofchicago.org/resource/ydr8-5enu.json"
    
    # Query: Get the latest 2000 "PERMIT - NEW CONSTRUCTION" records
    params = {
        "permit_type": "PERMIT - NEW CONSTRUCTION",
        "$limit": 2000,
        "$order": "issue_date DESC"
    }
    
    response = requests.get(url, params=params)
    
    if response.status_code == 200:
        permits_df = pd.DataFrame(response.json())
        
        # Clean up community area names if you have a mapping, or just group by community_area number
        # Community areas 33-76 roughly cover the South Side
        permits_df['community_area'] = pd.to_numeric(permits_df['community_area'], errors='coerce')
        
        # Create a flag for South Side (Community Areas >= 33, excluding some west/southwest)
        permits_df['is_south_side'] = permits_df['community_area'] >= 33
        
        print("\n--- Recent New Construction Permits ---")
        print(f"Total New Construction Permits analyzed: {len(permits_df)}")
        print(f"Permits explicitly located on the South Side: {permits_df['is_south_side'].sum()}")
        
        # Show a sample of the actual work being done
        print("\nSample of South Side Development:")
        south_sample = permits_df[permits_df['is_south_side']][['street_name', 'work_description']].head(3)
        for index, row in south_sample.iterrows():
            print(f"- {row['street_name']}: {row['work_description'][:100]}...")
            
    else:
        print(f"Failed to fetch data. Status code: {response.status_code}")

fetch_chicago_new_construction()
