import requests
import pandas as pd
import json
from sqlalchemy import create_engine, text
from datetime import datetime
import os

# Configuration - Get API key from environment (GitHub will provide this)
HERE_API_KEY = os.environ.get('HERE_API_KEY')
DB_CONNECTION_STRING = os.environ.get('DB_CONNECTION_STRING')
BBOX = (101.67, 3.13, 101.70, 3.16)  # Kuala Lumpur
LOCATION_NAME = "Kuala_Lumpur"

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

def save_to_postgres(flow_df, incidents_df, connection_string):
    """Save to PostgreSQL database"""
    if not connection_string:
        print("❌ Error: DB_CONNECTION_STRING not found in environment variables.")
        return

    try:
        engine = create_engine(connection_string)
        
        with engine.connect() as conn:
            if flow_df is not None and len(flow_df) > 0:
                flow_df.to_sql('traffic_flow', engine, if_exists='append', index=False)
                
                # Count total records
                result = conn.execute(text("SELECT COUNT(*) FROM traffic_flow"))
                total_records = result.scalar()
                print(f"✓ Saved {len(flow_df)} new flow records (Total in DB: {total_records})")
            
            if incidents_df is not None and len(incidents_df) > 0:
                incidents_df.to_sql('traffic_incidents', engine, if_exists='append', index=False)
                
                result = conn.execute(text("SELECT COUNT(*) FROM traffic_incidents"))
                total_records = result.scalar()
                print(f"✓ Saved {len(incidents_df)} new incident records (Total in DB: {total_records})")
                
    except Exception as e:
        print(f"❌ Error saving to database: {e}")

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
    save_to_postgres(flow_df, incidents_df, DB_CONNECTION_STRING)
    print(f"\n✓ Completed at {datetime.now()}")

if __name__ == "__main__":
    main()