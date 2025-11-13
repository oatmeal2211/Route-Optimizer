import requests
import pandas as pd
import json
import sqlite3
from datetime import datetime
import os

# Configuration - Get API key from environment (GitHub will provide this)
HERE_API_KEY = os.environ.get('HERE_API_KEY')
BBOX = (101.67, 3.13, 101.70, 3.16)  # Kuala Lumpur
LOCATION_NAME = "Kuala_Lumpur"
DB_NAME = "traffic_historical.db"

# API Endpoints
TRAFFIC_FLOW_URL = "https://data.traffic.hereapi.com/v7/flow"
TRAFFIC_INCIDENTS_URL = "https://data.traffic.hereapi.com/v7/incidents"

def fetch_traffic_flow(bbox, api_key):
    """Fetch traffic flow data"""
    params = {
        'apiKey': api_key,
        'in': f'bbox:{bbox[0]},{bbox[1]},{bbox[2]},{bbox[3]}',
        'locationReferencing': 'shape'
    }
    
    print(f"Fetching traffic flow data...")
    response = requests.get(TRAFFIC_FLOW_URL, params=params)
    
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error: {response.status_code}")
        return None

def fetch_traffic_incidents(bbox, api_key):
    """Fetch traffic incidents"""
    params = {
        'apiKey': api_key,
        'in': f'bbox:{bbox[0]},{bbox[1]},{bbox[2]},{bbox[3]}',
        'locationReferencing': 'shape'
    }
    
    print(f"Fetching traffic incidents...")
    response = requests.get(TRAFFIC_INCIDENTS_URL, params=params)
    
    if response.status_code == 200:
        return response.json()
    else:
        return None

def parse_traffic_flow_to_dataframe(traffic_data):
    """Convert flow JSON to DataFrame"""
    if not traffic_data or 'results' not in traffic_data:
        return None
    
    records = []
    for result in traffic_data['results']:
        location = result.get('location', {})
        current_flow = result.get('currentFlow', {})
        
        record = {
            'location_description': location.get('description', ''),
            'speed': current_flow.get('speed', None),
            'speed_limit': current_flow.get('speedLimit', None),
            'jam_factor': current_flow.get('jamFactor', None),
            'confidence': current_flow.get('confidence', None),
            'free_flow_speed': current_flow.get('freeFlowSpeed', None),
            'traversability': current_flow.get('traversability', ''),
        }
        
        # Store geometry as string
        if 'shape' in location and 'links' in location['shape']:
            coords = []
            for link in location['shape']['links']:
                if 'points' in link:
                    for point in link['points']:
                        coords.append((point.get('lng'), point.get('lat')))
            record['geometry'] = str(coords)
        
        records.append(record)
    
    return pd.DataFrame(records)

def parse_incidents_to_dataframe(incidents_data):
    """Convert incidents JSON to DataFrame"""
    if not incidents_data or 'results' not in incidents_data:
        return None
    
    records = []
    for incident in incidents_data['results']:
        location = incident.get('location', {})
        incident_details = incident.get('incidentDetails', {})
        
        record = {
            'incident_id': incident.get('incidentId', ''),
            'type': incident_details.get('type', ''),
            'description': incident_details.get('description', {}).get('value', ''),
            'criticality': incident_details.get('criticality', ''),
            'start_time': incident_details.get('startTime', ''),
        }
        
        if 'shape' in location and 'links' in location['shape']:
            coords = []
            for link in location['shape']['links']:
                if 'points' in link:
                    for point in link['points']:
                        coords.append((point.get('lng'), point.get('lat')))
            record['geometry'] = str(coords)
        
        records.append(record)
    
    return pd.DataFrame(records)

def save_to_sqlite(flow_df, incidents_df, db_name='traffic_historical.db'):
    """Save to SQLite database"""
    conn = sqlite3.connect(db_name)
    
    try:
        if flow_df is not None and len(flow_df) > 0:
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='traffic_flow'")
            table_exists = cursor.fetchone() is not None
            
            if_exists = 'replace' if not table_exists else 'append'
            flow_df.to_sql('traffic_flow', conn, if_exists=if_exists, index=False)
            print(f"✓ Saved {len(flow_df)} flow records")
        
        if incidents_df is not None and len(incidents_df) > 0:
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='traffic_incidents'")
            table_exists = cursor.fetchone() is not None
            
            if_exists = 'replace' if not table_exists else 'append'
            incidents_df.to_sql('traffic_incidents', conn, if_exists=if_exists, index=False)
            print(f"✓ Saved {len(incidents_df)} incident records")
    
    finally:
        conn.close()

def main():
    """Main collection function"""
    timestamp = datetime.now()
    
    print(f"\n{'='*60}")
    print(f"Collecting data at: {timestamp.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}\n")
    
    # Fetch data
    flow_data = fetch_traffic_flow(BBOX, HERE_API_KEY)
    incidents_data = fetch_traffic_incidents(BBOX, HERE_API_KEY)
    
    # Parse data
    flow_df = parse_traffic_flow_to_dataframe(flow_data)
    incidents_df = parse_incidents_to_dataframe(incidents_data)
    
    # Add timestamps
    if flow_df is not None:
        flow_df['timestamp'] = timestamp
        flow_df['hour'] = timestamp.hour
        flow_df['day_of_week'] = timestamp.strftime('%A')
        flow_df['date'] = timestamp.date()
        print(f"✓ Collected {len(flow_df)} flow records")
    
    if incidents_df is not None and len(incidents_df) > 0:
        incidents_df['timestamp'] = timestamp
        incidents_df['hour'] = timestamp.hour
        incidents_df['day_of_week'] = timestamp.strftime('%A')
        incidents_df['date'] = timestamp.date()
        print(f"✓ Collected {len(incidents_df)} incident records")
    
    # Save to database
    save_to_sqlite(flow_df, incidents_df, DB_NAME)
    print(f"\n✓ Completed at {datetime.now()}")

if __name__ == "__main__":
    main()