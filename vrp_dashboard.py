"""
VRP Optimizer Dashboard - Streamlit Application
A comprehensive Vehicle Routing Problem solver with multiple optimization algorithms.

Run with: streamlit run vrp_dashboard.py
"""

import streamlit as st
import pandas as pd
import numpy as np
import networkx as nx
import osmnx as ox
import folium
from folium.plugins import PolyLineTextPath, AntPath
from streamlit_folium import st_folium
from ortools.constraint_solver import routing_enums_pb2
from ortools.constraint_solver import pywrapcp
import pickle
from pathlib import Path
import time
from functools import lru_cache
from datetime import datetime, timezone, timedelta
import ast
import json

# Malaysia timezone (UTC+8)
MYT = timezone(timedelta(hours=8))

# ============================================================================
# PAGE CONFIGURATION
# ============================================================================

st.set_page_config(
    page_title="VRP Optimizer",
    page_icon="🚚",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin-bottom: 1rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        border-radius: 10px;
        padding: 1rem;
        margin: 0.5rem 0;
    }
    .success-box {
        background: linear-gradient(135deg, #28a745 0%, #20c997 100%);
        color: white;
        font-size: 1.5rem;
        font-weight: bold;
        padding: 1.5rem 2rem;
        border-radius: 12px;
        margin: 1.5rem 0;
        text-align: center;
        box-shadow: 0 4px 15px rgba(40, 167, 69, 0.4);
        animation: pulse 2s ease-in-out infinite;
    }
    @keyframes pulse {
        0%, 100% { transform: scale(1); }
        50% { transform: scale(1.02); }
    }
    .info-box {
        background: linear-gradient(135deg, #17a2b8 0%, #2E86AB 100%);
        color: white;
        font-size: 1.1rem;
        font-weight: 600;
        padding: 1rem 1.5rem;
        border-radius: 10px;
        margin: 1rem 0;
        box-shadow: 0 3px 10px rgba(46, 134, 171, 0.3);
    }
    .info-box b {
        font-size: 1.2rem;
    }
    .warning-box {
        background-color: #fff3cd;
        border-left: 4px solid #ffc107;
        padding: 1rem;
        border-radius: 5px;
        margin: 1rem 0;
    }
    .error-box {
        background: linear-gradient(135deg, #dc3545 0%, #c82333 100%);
        color: white;
        font-size: 1.3rem;
        font-weight: bold;
        padding: 1.5rem 2rem;
        border-radius: 12px;
        margin: 1.5rem 0;
        text-align: center;
        box-shadow: 0 4px 15px rgba(220, 53, 69, 0.4);
    }
</style>
""", unsafe_allow_html=True)

# ============================================================================
# CONSTANTS AND PRESETS
# ============================================================================

PRESET_LOCATIONS = {
    "Kuala Lumpur CBD": [
        {"name": "KL Sentral (Depot)", "lat": 3.1342, "lon": 101.6866, "is_depot": True, "demand": 0},
        {"name": "Suria KLCC", "lat": 3.1588, "lon": 101.7118, "is_depot": False, "demand": 15},
        {"name": "Pavilion KL", "lat": 3.1492, "lon": 101.7137, "is_depot": False, "demand": 20},
        {"name": "Berjaya Times Square", "lat": 3.1421, "lon": 101.7108, "is_depot": False, "demand": 12},
        {"name": "Lot 10", "lat": 3.1463, "lon": 101.7101, "is_depot": False, "demand": 18},
        {"name": "Central Market", "lat": 3.1451, "lon": 101.6944, "is_depot": False, "demand": 10},
        {"name": "Petaling Street", "lat": 3.1426, "lon": 101.6965, "is_depot": False, "demand": 8},
        {"name": "KL Tower", "lat": 3.1528, "lon": 101.7038, "is_depot": False, "demand": 14},
    ],
    "KL Mixed (CBD + Suburbs)": [
        {"name": "KL Sentral (Depot)", "lat": 3.1342, "lon": 101.6866, "is_depot": True, "demand": 0},
        {"name": "KLCC", "lat": 3.1588, "lon": 101.7118, "is_depot": False, "demand": 20},
        {"name": "Bukit Bintang", "lat": 3.1465, "lon": 101.7105, "is_depot": False, "demand": 25},
        {"name": "Mid Valley", "lat": 3.1178, "lon": 101.6775, "is_depot": False, "demand": 15},
        {"name": "Bangsar", "lat": 3.1290, "lon": 101.6710, "is_depot": False, "demand": 12},
        {"name": "Damansara", "lat": 3.1380, "lon": 101.6180, "is_depot": False, "demand": 18},
        {"name": "Subang Jaya", "lat": 3.0565, "lon": 101.5855, "is_depot": False, "demand": 22},
        {"name": "Puchong", "lat": 3.0245, "lon": 101.6170, "is_depot": False, "demand": 16},
    ],
    "Small Test (5 locations)": [
        {"name": "Depot", "lat": 3.1342, "lon": 101.6866, "is_depot": True, "demand": 0},
        {"name": "Location A", "lat": 3.1500, "lon": 101.7000, "is_depot": False, "demand": 10},
        {"name": "Location B", "lat": 3.1400, "lon": 101.7100, "is_depot": False, "demand": 15},
        {"name": "Location C", "lat": 3.1450, "lon": 101.6900, "is_depot": False, "demand": 12},
        {"name": "Location D", "lat": 3.1550, "lon": 101.7050, "is_depot": False, "demand": 8},
    ],
}

ROUTING_ALGORITHMS = {
    "Static (Distance-based)": {
        "id": "static",
        "description": "Optimizes for shortest total distance. Best for CBD-concentrated deliveries where all destinations are in congested zones.",
        "uses_traffic": False,
    },
    "Dynamic (Traffic-aware)": {
        "id": "dynamic",
        "description": "Optimizes for shortest travel time considering real-time traffic conditions. Routes around congested areas when possible.",
        "uses_traffic": True,
    },
    "Hybrid (Distance + Traffic)": {
        "id": "hybrid",
        "description": "Balances distance and traffic using weighted cost function. Recommended for mixed urban-suburban deliveries.",
        "uses_traffic": True,
    },
}

TRAFFIC_SCENARIOS = {
    "🔄 Auto (Current Time)": {"speed_factor": None, "congestion_mult": None, "icon": "🔄", "auto": True},
    "Night (12AM-6AM)": {"speed_factor": 1.0, "congestion_mult": 0.1, "icon": "🌙", "hours": (0, 6)},
    "Morning Peak (7AM-10AM)": {"speed_factor": 0.45, "congestion_mult": 1.5, "icon": "🌅", "hours": (7, 10)},
    "Midday (10AM-4PM)": {"speed_factor": 0.70, "congestion_mult": 0.6, "icon": "☀️", "hours": (10, 16)},
    "Evening Peak (5PM-8PM)": {"speed_factor": 0.40, "congestion_mult": 1.8, "icon": "🌆", "hours": (17, 20)},
    "Late Evening (8PM-12AM)": {"speed_factor": 0.85, "congestion_mult": 0.3, "icon": "🌃", "hours": (20, 24)},
}

TIME_WINDOW_PRESETS = {
    "Any time (0-240 min)": (0, 240),
    "Morning (0-120 min)": (0, 120),
    "Afternoon (120-240 min)": (120, 240),
    "Early slot (0-60 min)": (0, 60),
    "Mid slot (60-120 min)": (60, 120),
    "Late slot (120-180 min)": (120, 180),
    "Final slot (180-240 min)": (180, 240),
}

# Static fallback congestion zones (used when historical data unavailable)
STATIC_CONGESTION_ZONES = {
    'bukit_bintang': {
        'center': (101.7105, 3.1465),
        'radius_m': 800,
        'congestion_factor': 2.5,
        'description': 'Bukit Bintang Shopping District'
    },
    'klcc': {
        'center': (101.7136, 3.1569),
        'radius_m': 600,
        'congestion_factor': 2.0,
        'description': 'KLCC Business District'
    },
    'chinatown': {
        'center': (101.6968, 3.1440),
        'radius_m': 500,
        'congestion_factor': 1.8,
        'description': 'Chinatown/Petaling Street'
    },
    'central_market': {
        'center': (101.6954, 3.1455),
        'radius_m': 400,
        'congestion_factor': 1.6,
        'description': 'Central Market Area'
    },
    'free_flow_outskirts': {
        'center': (101.6850, 3.1350),
        'radius_m': 1000,
        'congestion_factor': 0.7,
        'description': 'Free-flow Outskirts'
    }
}

# Dynamic congestion zones - will be populated from historical data
CONGESTION_ZONES = {}

# ============================================================================
# HISTORICAL TRAFFIC DATA LOADING
# ============================================================================

@st.cache_data(ttl=3600)
def load_historical_traffic_data():
    """Load and parse historical traffic data from CSV."""
    try:
        traffic_file = Path(__file__).parent / "traffic_flow_Kuala_Lumpur.csv"
        if not traffic_file.exists():
            return None, None
        
        df = pd.read_csv(traffic_file)
        
        # Parse geometry strings into coordinate lists
        def parse_geometry(geom_str):
            try:
                return ast.literal_eval(geom_str)
            except:
                return []
        
        df['coords'] = df['geometry'].apply(parse_geometry)
        
        # Calculate centroid for each road segment
        def get_centroid(coords):
            if not coords:
                return None, None
            lons = [c[0] for c in coords]
            lats = [c[1] for c in coords]
            return np.mean(lons), np.mean(lats)
        
        df['centroid'] = df['coords'].apply(get_centroid)
        df['centroid_lon'] = df['centroid'].apply(lambda x: x[0] if x else None)
        df['centroid_lat'] = df['centroid'].apply(lambda x: x[1] if x else None)
        
        # Drop rows with missing data
        df = df.dropna(subset=['centroid_lon', 'centroid_lat', 'jam_factor', 'speed'])
        
        return df, traffic_file
    except Exception as e:
        st.warning(f"Could not load historical traffic data: {e}")
        return None, None

def build_dynamic_congestion_zones(traffic_df, grid_size=0.005):
    """
    Build dynamic congestion zones from historical traffic data.
    Creates a grid-based congestion map from real traffic measurements.
    
    Args:
        traffic_df: DataFrame with traffic flow data
        grid_size: Size of grid cells in degrees (~500m)
    
    Returns:
        dict: Dynamic congestion zones with real data
    """
    if traffic_df is None or traffic_df.empty:
        return STATIC_CONGESTION_ZONES.copy()
    
    zones = {}
    
    # Group traffic data by grid cells
    traffic_df['grid_lon'] = (traffic_df['centroid_lon'] / grid_size).round() * grid_size
    traffic_df['grid_lat'] = (traffic_df['centroid_lat'] / grid_size).round() * grid_size
    
    # Aggregate by grid cell
    grid_stats = traffic_df.groupby(['grid_lon', 'grid_lat']).agg({
        'jam_factor': 'mean',
        'speed': 'mean',
        'location_description': 'first',
        'confidence': 'mean'
    }).reset_index()
    
    # Convert jam_factor (0-10) to congestion_factor
    # jam_factor 0 = free flow = congestion_factor 0.7
    # jam_factor 5 = moderate = congestion_factor 1.5
    # jam_factor 10 = standstill = congestion_factor 3.0
    def jam_to_congestion(jam):
        if jam <= 1:
            return 0.7 + (jam / 1) * 0.3  # 0.7 to 1.0
        elif jam <= 3:
            return 1.0 + ((jam - 1) / 2) * 0.5  # 1.0 to 1.5
        elif jam <= 6:
            return 1.5 + ((jam - 3) / 3) * 0.7  # 1.5 to 2.2
        else:
            return 2.2 + ((jam - 6) / 4) * 0.8  # 2.2 to 3.0
    
    grid_stats['congestion_factor'] = grid_stats['jam_factor'].apply(jam_to_congestion)
    
    # Create zones from grid cells
    for idx, row in grid_stats.iterrows():
        zone_id = f"grid_{idx}"
        zones[zone_id] = {
            'center': (row['grid_lon'], row['grid_lat']),
            'radius_m': grid_size * 111000 / 2,  # Half grid size in meters
            'congestion_factor': row['congestion_factor'],
            'jam_factor': row['jam_factor'],
            'avg_speed': row['speed'],
            'description': row['location_description'][:50] if pd.notna(row['location_description']) else 'Traffic Zone',
            'from_historical': True
        }
    
    return zones

def get_auto_traffic_scenario():
    """
    Automatically determine traffic scenario based on current time.
    Uses Malaysia timezone (UTC+8).
    
    Returns:
        tuple: (scenario_name, speed_factor, congestion_mult, malaysia_time)
    """
    # Get current time in Malaysia timezone (UTC+8)
    now_utc = datetime.now(timezone.utc)
    now_myt = now_utc.astimezone(MYT)
    current_hour = now_myt.hour
    
    # Match to appropriate scenario
    if 0 <= current_hour < 6:
        return "Night (12AM-6AM)", 1.0, 0.1, now_myt
    elif 7 <= current_hour < 10:
        return "Morning Peak (7AM-10AM)", 0.45, 1.5, now_myt
    elif 10 <= current_hour < 16:
        return "Midday (10AM-4PM)", 0.70, 0.6, now_myt
    elif 17 <= current_hour < 20:
        return "Evening Peak (5PM-8PM)", 0.40, 1.8, now_myt
    elif 20 <= current_hour < 24:
        return "Late Evening (8PM-12AM)", 0.85, 0.3, now_myt
    else:
        # Transition hours (6AM, 4-5PM)
        if current_hour == 6:
            return "Morning Peak (7AM-10AM)", 0.55, 1.2, now_myt  # Transition
        else:  # 16
            return "Evening Peak (5PM-8PM)", 0.55, 1.4, now_myt  # Transition

def get_traffic_segments_for_edge(traffic_df, u_lon, u_lat, v_lon, v_lat, search_radius=0.002):
    """
    Find traffic segments near an edge and return average jam factor.
    
    Args:
        traffic_df: Historical traffic DataFrame
        u_lon, u_lat: Start node coordinates
        v_lon, v_lat: End node coordinates  
        search_radius: Search radius in degrees (~200m)
    
    Returns:
        float: Average jam factor (0-10) or None if no data
    """
    if traffic_df is None or traffic_df.empty:
        return None
    
    # Edge midpoint
    mid_lon = (u_lon + v_lon) / 2
    mid_lat = (u_lat + v_lat) / 2
    
    # Find nearby traffic segments
    dist = np.sqrt(
        (traffic_df['centroid_lon'] - mid_lon)**2 + 
        (traffic_df['centroid_lat'] - mid_lat)**2
    )
    
    nearby = traffic_df[dist <= search_radius]
    
    if nearby.empty:
        return None
    
    # Weight by confidence and proximity
    weights = nearby['confidence'] * (1 / (dist[nearby.index] + 0.0001))
    if weights.sum() > 0:
        return (nearby['jam_factor'] * weights).sum() / weights.sum()
    return nearby['jam_factor'].mean()

# Traffic model parameters
URBAN_EFFICIENCY_FACTOR = 0.65
SIGNAL_DELAY_MEAN = 30
TURN_DELAY_SECONDS = 5
SIGNALS_PER_KM = {'primary': 2.0, 'secondary': 2.5, 'tertiary': 3.0, 'residential': 1.5, 'unclassified': 1.5}
INTERSECTIONS_PER_KM = {'primary': 2.0, 'secondary': 3.0, 'tertiary': 4.0, 'residential': 5.0, 'unclassified': 3.0}

# ============================================================================
# GEOCODING FUNCTIONS
# ============================================================================

@st.cache_data(ttl=3600)  # Cache for 1 hour
def geocode_location(query: str, region: str = "Kuala Lumpur, Malaysia"):
    """
    Geocode a location name to coordinates using OSMnx/Nominatim.
    Returns (lat, lon, display_name) or (None, None, error_message)
    """
    try:
        # Add region context for better results
        search_query = f"{query}, {region}" if region and region.lower() not in query.lower() else query
        
        # Use OSMnx geocoding (Nominatim)
        result = ox.geocode(search_query)
        
        if result:
            lat, lon = result
            return lat, lon, f"Found: {query}"
        else:
            return None, None, f"Location not found: {query}"
    except Exception as e:
        return None, None, f"Geocoding error: {str(e)}"

@st.cache_data(ttl=3600)
def geocode_multiple(query: str, region: str = "Kuala Lumpur, Malaysia", limit: int = 5):
    """
    Search for multiple matching locations.
    Returns list of (name, lat, lon) tuples.
    """
    try:
        search_query = f"{query}, {region}" if region and region.lower() not in query.lower() else query
        
        # Use Nominatim directly for multiple results
        from geopy.geocoders import Nominatim
        geolocator = Nominatim(user_agent="vrp_optimizer_dashboard")
        locations = geolocator.geocode(search_query, exactly_one=False, limit=limit)
        
        if locations:
            return [(loc.address, loc.latitude, loc.longitude) for loc in locations]
        return []
    except ImportError:
        # Fallback to single result from OSMnx
        lat, lon, msg = geocode_location(query, region)
        if lat is not None:
            return [(query, lat, lon)]
        return []
    except Exception as e:
        return []

# ============================================================================
# GRAPH LOADING (CACHED)
# ============================================================================

@st.cache_resource
def load_road_network():
    """Load or create the road network graph - uses same graph as notebook"""
    # First try OSMnx cache (handles overpasses correctly)
    osmnx_cache = Path("cache/kl_osmnx_accurate.pkl")
    
    if osmnx_cache.exists():
        try:
            with open(osmnx_cache, 'rb') as f:
                G = pickle.load(f)
            if G.number_of_nodes() > 1000:
                return G, f"Loaded OSMnx graph ({G.number_of_nodes()} nodes) - accurate overpass routing"
        except Exception:
            pass
    
    # Try notebook's pyrosm graphs as fallback (may have overpass issues)
    cache_paths = [
        Path("cache/road_graph_pyrosm_v2_101.6800_3.1200_101.7200_3.1700.pkl"),
        Path("cache/road_graph_drive_101.6800_3.1200_101.7200_3.1700.pkl"),
    ]
    
    pyrosm_graph = None
    for cache_path in cache_paths:
        if cache_path.exists():
            try:
                with open(cache_path, 'rb') as f:
                    pyrosm_graph = pickle.load(f)
                if pyrosm_graph.number_of_nodes() > 1000:
                    break
            except Exception:
                continue
    
    # Download fresh from OSMnx (this properly handles overpasses/underpasses)
    # Use a LARGER bounding box to cover Greater KL area
    bbox = (101.60, 3.05, 101.78, 3.22)  # Expanded Greater KL area
    
    try:
        G = ox.graph_from_bbox(
            bbox=bbox,
            network_type='drive',
            simplify=True
        )
        
        # Add travel times
        G = ox.add_edge_speeds(G)
        G = ox.add_edge_travel_times(G)
        
        # Save to cache
        osmnx_cache.parent.mkdir(exist_ok=True)
        with open(osmnx_cache, 'wb') as f:
            pickle.dump(G, f)
        
        return G, f"Downloaded OSMnx graph ({G.number_of_nodes()} nodes) - accurate overpass routing"
    except Exception as e:
        # Fall back to pyrosm if download fails
        if pyrosm_graph is not None:
            return pyrosm_graph, f"Using cached graph ({pyrosm_graph.number_of_nodes()} nodes) - ⚠️ may have overpass inaccuracies"
        return None, str(e)

@st.cache_data
def get_graph_nodes(_G):
    """Get nodes in the largest strongly connected component"""
    # Use largest SCC to ensure all nodes are reachable from each other
    largest_scc = max(nx.strongly_connected_components(_G), key=len)
    return largest_scc

def find_nearest_valid_node(G, lon, lat, graph_nodes, max_attempts=5):
    """
    Find the nearest node that's in the valid graph_nodes set.
    If the closest node isn't valid, try finding alternatives nearby.
    """
    try:
        # First try: direct nearest node
        nearest = ox.distance.nearest_nodes(G, lon, lat)
        if nearest in graph_nodes:
            return nearest, "direct"
        
        # Second try: get multiple nearest nodes and find first valid one
        # Use a small search radius expanding outward
        for radius_mult in [1, 2, 5, 10]:
            # Offset search slightly in different directions
            offsets = [
                (0, 0),
                (0.001 * radius_mult, 0),
                (-0.001 * radius_mult, 0),
                (0, 0.001 * radius_mult),
                (0, -0.001 * radius_mult),
            ]
            for lon_off, lat_off in offsets:
                try:
                    candidate = ox.distance.nearest_nodes(G, lon + lon_off, lat + lat_off)
                    if candidate in graph_nodes:
                        return candidate, f"offset_{radius_mult}"
                except:
                    continue
        
        # Last resort: return the nearest even if not in SCC
        # This allows routing to work, though path might not exist
        return nearest, "fallback"
    except Exception as e:
        return None, str(e)

def haversine_distance(lat1, lon1, lat2, lon2):
    """Calculate haversine distance in meters between two points."""
    from math import radians, sin, cos, sqrt, atan2
    R = 6371000  # Earth radius in meters
    
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * atan2(sqrt(a), sqrt(1-a))
    
    return R * c

# ============================================================================
# VRP HELPER FUNCTIONS
# ============================================================================

def get_zone_congestion_factor(lon, lat):
    """Get congestion factor for a location based on zone."""
    for zone_name, zone in CONGESTION_ZONES.items():
        center_lon, center_lat = zone['center']
        dist_deg = ((lon - center_lon)**2 + (lat - center_lat)**2)**0.5
        dist_m = dist_deg * 111000
        if dist_m <= zone['radius_m']:
            return zone['congestion_factor']
    return 1.0

def get_edge_zone_factor(G, u, v):
    """Get average zone congestion factor for an edge."""
    try:
        u_data = G.nodes[u]
        v_data = G.nodes[v]
        mid_lon = (u_data.get('x', 0) + v_data.get('x', 0)) / 2
        mid_lat = (u_data.get('y', 0) + v_data.get('y', 0)) / 2
        return get_zone_congestion_factor(mid_lon, mid_lat)
    except:
        return 1.0

def compute_travel_time_realistic(G, path, speed_factor, apply_asymmetric=True):
    """Compute realistic travel time along a path."""
    total_tt = 0
    total_dist = 0
    
    for j in range(len(path) - 1):
        u, v = path[j], path[j + 1]
        edge_data = G[u][v][0] if G.has_edge(u, v) else {}
        
        base_tt = edge_data.get('travel_time', 0)
        length = edge_data.get('length', 0)
        
        adjusted_tt = base_tt / URBAN_EFFICIENCY_FACTOR
        adjusted_tt = adjusted_tt / max(0.1, speed_factor)
        
        if apply_asymmetric:
            zone_factor = get_edge_zone_factor(G, u, v)
            adjusted_tt *= zone_factor
        
        length_km = length / 1000
        highway = edge_data.get('highway', 'unclassified')
        if isinstance(highway, list):
            highway = highway[0]
        
        signals = SIGNALS_PER_KM.get(highway, 1.5) * length_km * 0.5 * SIGNAL_DELAY_MEAN
        intersections = INTERSECTIONS_PER_KM.get(highway, 3.0) * length_km * TURN_DELAY_SECONDS
        
        adjusted_tt += signals + intersections
        total_tt += adjusted_tt
        total_dist += length
    
    return total_tt, total_dist

def build_distance_matrix(G, location_nodes):
    """Build distance-only matrix with haversine fallback."""
    n = len(location_nodes)
    matrix = []
    failed_paths = []
    
    for i, start_node in enumerate(location_nodes):
        row = []
        for j, end_node in enumerate(location_nodes):
            if i == j:
                row.append(0)
            else:
                try:
                    path = nx.shortest_path(G, start_node, end_node, weight='length')
                    dist = sum(G[path[k]][path[k+1]][0].get('length', 0) 
                              for k in range(len(path)-1))
                    row.append(int(dist))
                except nx.NetworkXNoPath:
                    # Fallback to haversine distance * 1.4 (road factor)
                    lat1, lon1 = G.nodes[start_node]['y'], G.nodes[start_node]['x']
                    lat2, lon2 = G.nodes[end_node]['y'], G.nodes[end_node]['x']
                    dist = haversine_distance(lat1, lon1, lat2, lon2) * 1.4
                    row.append(int(dist))
                    failed_paths.append((i, j))
                except Exception as e:
                    # Fallback to large but not impossible distance
                    row.append(50000)  # 50km fallback
                    failed_paths.append((i, j))
        matrix.append(row)
    
    return matrix

def build_traffic_time_matrix(G, location_nodes, speed_factor, apply_asymmetric=True):
    """Build traffic-aware time matrix with fallback."""
    n = len(location_nodes)
    matrix = []
    
    if apply_asymmetric:
        for u, v, key, data in G.edges(keys=True, data=True):
            base_tt = data.get('travel_time', 0)
            zone_factor = get_edge_zone_factor(G, u, v)
            adjusted_tt = base_tt / max(0.1, speed_factor) * zone_factor
            data['adjusted_travel_time'] = adjusted_tt
        weight_key = 'adjusted_travel_time'
    else:
        weight_key = 'travel_time'
    
    for i, start_node in enumerate(location_nodes):
        row = []
        for j, end_node in enumerate(location_nodes):
            if i == j:
                row.append(0)
            else:
                try:
                    path = nx.shortest_path(G, start_node, end_node, weight=weight_key)
                    tt, _ = compute_travel_time_realistic(G, path, speed_factor, apply_asymmetric)
                    row.append(int(tt))
                except nx.NetworkXNoPath:
                    # Fallback: haversine distance / speed (in seconds)
                    lat1, lon1 = G.nodes[start_node]['y'], G.nodes[start_node]['x']
                    lat2, lon2 = G.nodes[end_node]['y'], G.nodes[end_node]['x']
                    dist = haversine_distance(lat1, lon1, lat2, lon2) * 1.4
                    speed_ms = 11.1 * speed_factor  # ~40 km/h adjusted for traffic
                    tt = dist / max(1, speed_ms)
                    row.append(int(tt))
                except Exception:
                    # Fallback to 30 min travel time
                    row.append(1800)
        matrix.append(row)
    
    return matrix

def build_hybrid_cost_matrix(G, location_nodes, speed_factor, alpha=0.5, apply_asymmetric=True):
    """Build hybrid cost matrix combining distance and traffic time."""
    n = len(location_nodes)
    
    if apply_asymmetric:
        for u, v, key, data in G.edges(keys=True, data=True):
            base_tt = data.get('travel_time', 0)
            zone_factor = get_edge_zone_factor(G, u, v)
            adjusted_tt = base_tt / max(0.1, speed_factor) * zone_factor
            data['adjusted_travel_time'] = adjusted_tt
    
    raw_distances = []
    raw_times = []
    max_dist = 0
    max_time = 0
    
    for i, start_node in enumerate(location_nodes):
        dist_row = []
        time_row = []
        
        for j, end_node in enumerate(location_nodes):
            if i == j:
                dist_row.append(0)
                time_row.append(0)
            else:
                try:
                    path_dist = nx.shortest_path(G, start_node, end_node, weight='length')
                    dist = sum(G[path_dist[k]][path_dist[k+1]][0].get('length', 0) 
                              for k in range(len(path_dist)-1))
                    
                    weight_key = 'adjusted_travel_time' if apply_asymmetric else 'travel_time'
                    path_time = nx.shortest_path(G, start_node, end_node, weight=weight_key)
                    tt, _ = compute_travel_time_realistic(G, path_time, speed_factor, apply_asymmetric)
                    
                    dist_row.append(dist)
                    time_row.append(tt)
                    
                    if dist > max_dist:
                        max_dist = dist
                    if tt > max_time:
                        max_time = tt
                except (nx.NetworkXNoPath, nx.NodeNotFound):
                    # Fallback to haversine
                    lat1, lon1 = G.nodes[start_node]['y'], G.nodes[start_node]['x']
                    lat2, lon2 = G.nodes[end_node]['y'], G.nodes[end_node]['x']
                    dist = haversine_distance(lat1, lon1, lat2, lon2) * 1.4
                    speed_ms = 11.1 * speed_factor
                    tt = dist / max(1, speed_ms)
                    
                    dist_row.append(dist)
                    time_row.append(tt)
                    
                    if dist > max_dist:
                        max_dist = dist
                    if tt > max_time:
                        max_time = tt
                except Exception:
                    dist_row.append(50000)
                    time_row.append(1800)
        
        raw_distances.append(dist_row)
        raw_times.append(time_row)
    
    # Ensure max values are valid
    if max_dist == 0:
        max_dist = 1
    if max_time == 0:
        max_time = 1
    
    # Normalize and combine
    distance_matrix = []
    time_matrix = []
    hybrid_matrix = []
    
    for i in range(n):
        dist_row = []
        time_row = []
        hybrid_row = []
        
        for j in range(n):
            if i == j:
                dist_row.append(0)
                time_row.append(0)
                hybrid_row.append(0)
            else:
                norm_dist = raw_distances[i][j] / max_dist
                norm_time = raw_times[i][j] / max_time
                hybrid_cost = int((alpha * norm_dist + (1 - alpha) * norm_time) * 10000)
                
                dist_row.append(int(raw_distances[i][j]))
                time_row.append(int(raw_times[i][j]))
                hybrid_row.append(hybrid_cost)
        
        distance_matrix.append(dist_row)
        time_matrix.append(time_row)
        hybrid_matrix.append(hybrid_row)
    
    return hybrid_matrix, distance_matrix, time_matrix

# ============================================================================
# VRP SOLVERS
# ============================================================================

def solve_static_vrp(G, locations, demands, service_times_s, vehicle_capacities,
                     time_windows_s, max_duration_s, num_vehicles, speed_factor=1.0):
    """Solve VRP using distance optimization (but display traffic-adjusted travel times)."""
    n = len(locations)
    distance_matrix = build_distance_matrix(G, locations)
    
    # Build time matrix with speed_factor=1.0 for constraints (free-flow for time windows)
    constraint_time_matrix = build_traffic_time_matrix(G, locations, speed_factor=1.0, apply_asymmetric=True)
    
    # Build traffic-adjusted time matrix for DISPLAY (so static and dynamic can be compared fairly)
    display_time_matrix = build_traffic_time_matrix(G, locations, speed_factor=speed_factor, apply_asymmetric=True)
    
    manager = pywrapcp.RoutingIndexManager(n, num_vehicles, 0)
    routing = pywrapcp.RoutingModel(manager)
    
    def distance_callback(from_index, to_index):
        from_node = manager.IndexToNode(from_index)
        to_node = manager.IndexToNode(to_index)
        return distance_matrix[from_node][to_node]
    
    distance_callback_idx = routing.RegisterTransitCallback(distance_callback)
    
    # Use free-flow time matrix for time window constraints
    def time_callback(from_index, to_index):
        from_node = manager.IndexToNode(from_index)
        to_node = manager.IndexToNode(to_index)
        return constraint_time_matrix[from_node][to_node] + service_times_s[from_node]
    
    time_callback_idx = routing.RegisterTransitCallback(time_callback)
    
    # Optimize by DISTANCE
    routing.SetArcCostEvaluatorOfAllVehicles(distance_callback_idx)
    
    # Add Time dimension with realistic times for proper time window enforcement
    routing.AddDimension(time_callback_idx, 30 * 60, max_duration_s, False, "Time")
    time_dimension = routing.GetDimensionOrDie("Time")
    
    for location_idx, (start_s, end_s) in enumerate(time_windows_s):
        index = manager.NodeToIndex(location_idx)
        time_dimension.CumulVar(index).SetRange(int(start_s), int(end_s))
    
    for vehicle_id in range(num_vehicles):
        routing.AddVariableMinimizedByFinalizer(time_dimension.CumulVar(routing.Start(vehicle_id)))
        routing.AddVariableMinimizedByFinalizer(time_dimension.CumulVar(routing.End(vehicle_id)))
    
    def demand_callback(from_index):
        return demands[manager.IndexToNode(from_index)]
    
    demand_callback_idx = routing.RegisterUnaryTransitCallback(demand_callback)
    routing.AddDimensionWithVehicleCapacity(demand_callback_idx, 0, vehicle_capacities, True, "Capacity")
    
    search_params = pywrapcp.DefaultRoutingSearchParameters()
    search_params.first_solution_strategy = routing_enums_pb2.FirstSolutionStrategy.PATH_CHEAPEST_ARC
    search_params.local_search_metaheuristic = routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH
    search_params.time_limit.seconds = 10
    
    solution = routing.SolveWithParameters(search_params)
    
    if solution:
        routes = []
        for vehicle_id in range(num_vehicles):
            route = []
            index = routing.Start(vehicle_id)
            while not routing.IsEnd(index):
                route.append(manager.IndexToNode(index))
                index = solution.Value(routing.NextVar(index))
            route.append(manager.IndexToNode(index))
            if len(route) > 2 or any(idx != 0 for idx in route):
                routes.append(route)
        # Return traffic-adjusted time matrix for display (fair comparison with dynamic)
        return {'routes': routes, 'feasible': True, 'distance_matrix': distance_matrix, 'time_matrix': display_time_matrix}
    
    return {'routes': [], 'feasible': False}

def solve_dynamic_vrp(G, locations, demands, service_times_s, vehicle_capacities,
                      time_windows_s, max_duration_s, num_vehicles, speed_factor,
                      apply_asymmetric=True):
    """Solve VRP using traffic-aware time optimization."""
    n = len(locations)
    time_matrix = build_traffic_time_matrix(G, locations, speed_factor, apply_asymmetric)
    distance_matrix = build_distance_matrix(G, locations)  # Also build distance matrix for display
    
    manager = pywrapcp.RoutingIndexManager(n, num_vehicles, 0)
    routing = pywrapcp.RoutingModel(manager)
    
    def time_callback(from_index, to_index):
        from_node = manager.IndexToNode(from_index)
        to_node = manager.IndexToNode(to_index)
        return time_matrix[from_node][to_node] + service_times_s[from_node]
    
    time_callback_idx = routing.RegisterTransitCallback(time_callback)
    routing.SetArcCostEvaluatorOfAllVehicles(time_callback_idx)
    
    routing.AddDimension(time_callback_idx, 30 * 60, max_duration_s, False, "Time")
    time_dimension = routing.GetDimensionOrDie("Time")
    
    for location_idx, (start_s, end_s) in enumerate(time_windows_s):
        index = manager.NodeToIndex(location_idx)
        time_dimension.CumulVar(index).SetRange(int(start_s), int(end_s))
    
    for vehicle_id in range(num_vehicles):
        routing.AddVariableMinimizedByFinalizer(time_dimension.CumulVar(routing.Start(vehicle_id)))
        routing.AddVariableMinimizedByFinalizer(time_dimension.CumulVar(routing.End(vehicle_id)))
    
    def demand_callback(from_index):
        return demands[manager.IndexToNode(from_index)]
    
    demand_callback_idx = routing.RegisterUnaryTransitCallback(demand_callback)
    routing.AddDimensionWithVehicleCapacity(demand_callback_idx, 0, vehicle_capacities, True, "Capacity")
    
    search_params = pywrapcp.DefaultRoutingSearchParameters()
    search_params.first_solution_strategy = routing_enums_pb2.FirstSolutionStrategy.PATH_CHEAPEST_ARC
    search_params.local_search_metaheuristic = routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH
    search_params.time_limit.seconds = 10
    
    solution = routing.SolveWithParameters(search_params)
    
    if solution:
        routes = []
        for vehicle_id in range(num_vehicles):
            route = []
            index = routing.Start(vehicle_id)
            while not routing.IsEnd(index):
                route.append(manager.IndexToNode(index))
                index = solution.Value(routing.NextVar(index))
            route.append(manager.IndexToNode(index))
            if len(route) > 2 or any(idx != 0 for idx in route):
                routes.append(route)
        return {'routes': routes, 'feasible': True, 'time_matrix': time_matrix, 'distance_matrix': distance_matrix}
    
    return {'routes': [], 'feasible': False}

def solve_hybrid_vrp(G, locations, demands, service_times_s, vehicle_capacities,
                     time_windows_s, max_duration_s, num_vehicles, speed_factor,
                     alpha=0.5, apply_asymmetric=True):
    """Solve VRP using hybrid cost function."""
    n = len(locations)
    hybrid_matrix, distance_matrix, time_matrix = build_hybrid_cost_matrix(
        G, locations, speed_factor, alpha, apply_asymmetric
    )
    
    manager = pywrapcp.RoutingIndexManager(n, num_vehicles, 0)
    routing = pywrapcp.RoutingModel(manager)
    
    def hybrid_callback(from_index, to_index):
        from_node = manager.IndexToNode(from_index)
        to_node = manager.IndexToNode(to_index)
        return hybrid_matrix[from_node][to_node]
    
    hybrid_callback_idx = routing.RegisterTransitCallback(hybrid_callback)
    
    def time_callback(from_index, to_index):
        from_node = manager.IndexToNode(from_index)
        to_node = manager.IndexToNode(to_index)
        return time_matrix[from_node][to_node] + service_times_s[from_node]
    
    time_callback_idx = routing.RegisterTransitCallback(time_callback)
    routing.SetArcCostEvaluatorOfAllVehicles(hybrid_callback_idx)
    
    routing.AddDimension(time_callback_idx, 30 * 60, max_duration_s, False, "Time")
    time_dimension = routing.GetDimensionOrDie("Time")
    
    for location_idx, (start_s, end_s) in enumerate(time_windows_s):
        index = manager.NodeToIndex(location_idx)
        time_dimension.CumulVar(index).SetRange(int(start_s), int(end_s))
    
    for vehicle_id in range(num_vehicles):
        routing.AddVariableMinimizedByFinalizer(time_dimension.CumulVar(routing.Start(vehicle_id)))
        routing.AddVariableMinimizedByFinalizer(time_dimension.CumulVar(routing.End(vehicle_id)))
    
    def demand_callback(from_index):
        return demands[manager.IndexToNode(from_index)]
    
    demand_callback_idx = routing.RegisterUnaryTransitCallback(demand_callback)
    routing.AddDimensionWithVehicleCapacity(demand_callback_idx, 0, vehicle_capacities, True, "Capacity")
    
    search_params = pywrapcp.DefaultRoutingSearchParameters()
    search_params.first_solution_strategy = routing_enums_pb2.FirstSolutionStrategy.PATH_CHEAPEST_ARC
    search_params.local_search_metaheuristic = routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH
    search_params.time_limit.seconds = 15
    
    solution = routing.SolveWithParameters(search_params)
    
    if solution:
        routes = []
        for vehicle_id in range(num_vehicles):
            route = []
            index = routing.Start(vehicle_id)
            while not routing.IsEnd(index):
                route.append(manager.IndexToNode(index))
                index = solution.Value(routing.NextVar(index))
            route.append(manager.IndexToNode(index))
            if len(route) > 2 or any(idx != 0 for idx in route):
                routes.append(route)
        return {'routes': routes, 'feasible': True, 'distance_matrix': distance_matrix, 'time_matrix': time_matrix}
    
    return {'routes': [], 'feasible': False}

# ============================================================================
# MAP GENERATION
# ============================================================================

def get_edge_data(G, u, v):
    """Get edge data, checking both directions for directed graphs."""
    is_multi = isinstance(G, (nx.MultiDiGraph, nx.MultiGraph))
    
    if is_multi:
        if G.has_edge(u, v):
            keys = list(G[u][v].keys())
            if keys:
                return G[u][v][keys[0]], False
        if G.has_edge(v, u):
            keys = list(G[v][u].keys())
            if keys:
                return G[v][u][keys[0]], True
    else:
        if G.has_edge(u, v):
            return G[u][v], False
        if G.has_edge(v, u):
            return G[v][u], True
    return None, False

def get_edge_coords(G, u, v):
    """Get the actual road geometry coordinates for an edge."""
    edge_data, is_reversed = get_edge_data(G, u, v)
    
    coords = []
    
    if edge_data is not None and 'geometry' in edge_data:
        geom = edge_data['geometry']
        if hasattr(geom, 'coords'):
            coords = [(lat, lon) for lon, lat in geom.coords]
        elif isinstance(geom, str):
            try:
                from shapely import wkt
                line = wkt.loads(geom)
                coords = [(lat, lon) for lon, lat in line.coords]
            except:
                pass
        elif isinstance(geom, (list, tuple)):
            try:
                coords = [(pt[1], pt[0]) for pt in geom]
            except:
                pass
    
    if not coords:
        start_lat = G.nodes[u].get('y', G.nodes[u].get('lat'))
        start_lon = G.nodes[u].get('x', G.nodes[u].get('lon'))
        end_lat = G.nodes[v].get('y', G.nodes[v].get('lat'))
        end_lon = G.nodes[v].get('x', G.nodes[v].get('lon'))
        coords = [(start_lat, start_lon), (end_lat, end_lon)]
    
    if is_reversed:
        coords = coords[::-1]
    
    return coords

def get_congestion_color(congestion_level):
    """Get color based on congestion level (0-1 scale, where 1 is most congested)."""
    if congestion_level < 0.2:
        return '#00FF00'  # Green - free flow
    elif congestion_level < 0.4:
        return '#7FFF00'  # Light green - light traffic
    elif congestion_level < 0.6:
        return '#FFFF00'  # Yellow - moderate traffic
    elif congestion_level < 0.8:
        return '#FF8C00'  # Orange - heavy traffic
    else:
        return '#FF0000'  # Red - severe congestion

def get_congestion_label(congestion_level):
    """Get text label for congestion level."""
    if congestion_level < 0.2:
        return 'Free Flow'
    elif congestion_level < 0.4:
        return 'Light Traffic'
    elif congestion_level < 0.6:
        return 'Moderate Traffic'
    elif congestion_level < 0.8:
        return 'Heavy Traffic'
    else:
        return 'Severe Congestion'

def create_route_map(locations_df, routes, center_lat, center_lon, time_matrix=None, distance_matrix=None, speed_factor=1.0, G=None, location_nodes=None, selected_vehicles=None, show_direction=True, traffic_df=None, show_traffic_overlay=False):
    """Create a Folium map with routes and traffic congestion indicators."""
    m = folium.Map(location=[center_lat, center_lon], zoom_start=13)
    
    # Draw historical traffic overlay if enabled
    if show_traffic_overlay and traffic_df is not None:
        def jam_to_color(jam_factor):
            """Convert jam factor (0-10) to color."""
            if jam_factor <= 1:
                return '#00C853'  # Green - free flow
            elif jam_factor <= 3:
                return '#7FFF00'  # Light green
            elif jam_factor <= 5:
                return '#FFFF00'  # Yellow - moderate
            elif jam_factor <= 7:
                return '#FF8C00'  # Orange - heavy
            else:
                return '#FF0000'  # Red - severe
        
        # Create a feature group for traffic overlay
        traffic_layer = folium.FeatureGroup(name="Historical Traffic", show=True)
        
        for _, row in traffic_df.iterrows():
            if row['coords'] and len(row['coords']) >= 2:
                # Convert coords to [lat, lon] format
                path_coords = [[c[1], c[0]] for c in row['coords']]
                color = jam_to_color(row['jam_factor'])
                
                folium.PolyLine(
                    path_coords,
                    weight=4,
                    color=color,
                    opacity=0.6,
                    popup=f"{row['location_description']}<br>Jam: {row['jam_factor']:.1f}<br>Speed: {row['speed']:.1f} m/s"
                ).add_to(traffic_layer)
        
        traffic_layer.add_to(m)
        folium.LayerControl().add_to(m)
    
    # Counters for debugging
    road_paths_drawn = 0
    fallback_paths_drawn = 0
    
    # Reset index to ensure 0-based positional access
    locations_df = locations_df.reset_index(drop=True)
    
    colors = ['#E74C3C', '#3498DB', '#27AE60', '#9B59B6', '#F39C12', '#C0392B', '#2980B9', '#1ABC9C', '#8E44AD', '#E91E63']
    
    # Filter routes by selected vehicles
    if selected_vehicles is not None:
        display_routes = [(v_id, routes[v_id]) for v_id in selected_vehicles if v_id < len(routes)]
    else:
        display_routes = list(enumerate(routes))
    
    # Determine if single vehicle mode (cleaner visualization)
    single_vehicle_mode = len(display_routes) == 1
    
    # Build legend based on mode
    if single_vehicle_mode:
        # Simple traffic-only legend for single vehicle
        legend_html = '''
        <div style="position: fixed; bottom: 30px; left: 10px; z-index: 1000; 
                    background-color: rgba(255,255,255,0.95); padding: 12px 14px; border-radius: 4px;
                    border: 1px solid #ccc; font-size: 12px; font-family: Arial, sans-serif;
                    color: #333; box-shadow: 0 1px 4px rgba(0,0,0,0.2);">
            <div style="font-weight: 600; margin-bottom: 8px;">Traffic Congestion</div>
            <div style="display: flex; align-items: center; gap: 6px; margin: 4px 0;">
                <div style="width: 24px; height: 6px; background-color: #00C853; border-radius: 2px;"></div>
                <span>Free flow</span>
            </div>
            <div style="display: flex; align-items: center; gap: 6px; margin: 4px 0;">
                <div style="width: 24px; height: 6px; background-color: #FFEB3B; border-radius: 2px;"></div>
                <span>Light traffic</span>
            </div>
            <div style="display: flex; align-items: center; gap: 6px; margin: 4px 0;">
                <div style="width: 24px; height: 6px; background-color: #FF9800; border-radius: 2px;"></div>
                <span>Moderate</span>
            </div>
            <div style="display: flex; align-items: center; gap: 6px; margin: 4px 0;">
                <div style="width: 24px; height: 6px; background-color: #F44336; border-radius: 2px;"></div>
                <span>Heavy traffic</span>
            </div>
        </div>
        '''
    else:
        # Multi-vehicle legend with vehicle colors
        vehicle_items = ""
        for v_id in range(len(routes)):
            color = colors[v_id % len(colors)]
            vehicle_items += f'<div style="display: flex; align-items: center; gap: 6px; margin: 2px 0;"><div style="width: 20px; height: 4px; background-color: {color}; border-radius: 2px;"></div><span>Vehicle {v_id + 1}</span></div>'
        
        legend_html = f'''
        <div style="position: fixed; bottom: 30px; left: 10px; z-index: 1000; 
                    background-color: rgba(255,255,255,0.95); padding: 10px 12px; border-radius: 4px;
                    border: 1px solid #ccc; font-size: 12px; font-family: Arial, sans-serif;
                    color: #333; box-shadow: 0 1px 4px rgba(0,0,0,0.2);">
            <div style="font-weight: 600; margin-bottom: 6px;">Traffic</div>
            <div style="display: flex; align-items: center; gap: 4px;">
                <div style="width: 40px; height: 6px; background: linear-gradient(to right, #00FF00, #7FFF00, #FFFF00, #FF8C00, #FF0000); border-radius: 2px;"></div>
            </div>
            <div style="display: flex; justify-content: space-between; font-size: 10px; color: #666; margin-top: 2px;">
                <span>Fast</span>
                <span>Slow</span>
            </div>
            <div style="border-top: 1px solid #ddd; margin-top: 8px; padding-top: 8px;">
                <div style="font-weight: 600; margin-bottom: 4px;">Vehicles</div>
                {vehicle_items}
            </div>
        </div>
        '''
    m.get_root().html.add_child(folium.Element(legend_html))
    
    # Add markers
    for idx, row in locations_df.iterrows():
        if row.get('is_depot', False):
            folium.Marker(
                [row['lat'], row['lon']],
                popup=f"🏭 {row['name']} (Depot)",
                icon=folium.Icon(color='black', icon='home')
            ).add_to(m)
        else:
            folium.Marker(
                [row['lat'], row['lon']],
                popup=f"📦 {row['name']}<br>Demand: {row.get('demand', 0)}",
                icon=folium.Icon(color='gray', icon='info-sign')
            ).add_to(m)
    
    # Draw routes with congestion colors
    for v_id, route in display_routes:
        if len(route) < 2:
            continue  # Skip empty routes
            
        vehicle_color = colors[v_id % len(colors)]
        
        # Collect all coordinates for this vehicle's route (for direction arrows)
        vehicle_route_coords = []
        
        for i in range(len(route) - 1):
            from_idx, to_idx = route[i], route[i + 1]
            
            # Safety check for valid indices
            if from_idx >= len(locations_df) or to_idx >= len(locations_df):
                continue
                
            from_loc = locations_df.iloc[from_idx]
            to_loc = locations_df.iloc[to_idx]
            
            # Calculate congestion level for this segment
            congestion_level = 0.5  # Default moderate
            
            if time_matrix and distance_matrix:
                travel_time = time_matrix[from_idx][to_idx]  # seconds
                distance = distance_matrix[from_idx][to_idx]  # meters
                
                if distance > 0 and distance < 999999:
                    # Calculate actual speed vs expected free-flow speed
                    actual_speed = distance / max(travel_time, 1)  # m/s
                    free_flow_speed = 11.1  # ~40 km/h free flow assumption
                    
                    # Lower speed = higher congestion
                    speed_ratio = actual_speed / free_flow_speed
                    congestion_level = max(0, min(1, 1 - speed_ratio))
            else:
                # Use zone-based congestion as fallback
                mid_lon = (from_loc['lon'] + to_loc['lon']) / 2
                mid_lat = (from_loc['lat'] + to_loc['lat']) / 2
                zone_factor = get_zone_congestion_factor(mid_lon, mid_lat)
                congestion_level = (zone_factor - 0.7) / 1.8  # Normalize to 0-1
            
            congestion_color = get_congestion_color(congestion_level)
            congestion_label = get_congestion_label(congestion_level)
            
            # Try to get actual road path from graph
            road_path_drawn = False
            if G is not None and location_nodes is not None:
                try:
                    from_node = location_nodes[from_idx]
                    to_node = location_nodes[to_idx]
                    
                    # Get shortest path nodes
                    path_nodes = nx.shortest_path(G, from_node, to_node, weight='length')
                    
                    # Draw each edge segment with actual road geometry
                    all_segment_coords = []
                    for j in range(len(path_nodes) - 1):
                        node_a = path_nodes[j]
                        node_b = path_nodes[j + 1]
                        
                        # Get actual road geometry for this edge
                        edge_coords = get_edge_coords(G, node_a, node_b)
                        
                        # Calculate congestion for THIS specific edge based on its location
                        if edge_coords:
                            mid_lat = sum(c[0] for c in edge_coords) / len(edge_coords)
                            mid_lon = sum(c[1] for c in edge_coords) / len(edge_coords)
                            zone_factor = get_zone_congestion_factor(mid_lon, mid_lat)
                            # Convert zone factor to congestion level (0-1)
                            # zone_factor ranges from 0.7 (fast) to 2.5 (slow)
                            edge_congestion = max(0, min(1, (zone_factor - 0.7) / 1.8))
                            edge_color = get_congestion_color(edge_congestion)
                            edge_label = get_congestion_label(edge_congestion)
                        else:
                            edge_color = congestion_color
                            edge_label = congestion_label
                        
                        if single_vehicle_mode:
                            # Single vehicle: clean traffic-only view with thicker lines
                            folium.PolyLine(
                                edge_coords,
                                weight=8,
                                color=edge_color,
                                opacity=0.9,
                                popup=f"{edge_label}"
                            ).add_to(m)
                        else:
                            # Multi-vehicle: vehicle-colored border + traffic color inner
                            folium.PolyLine(
                                edge_coords,
                                weight=10,
                                color=vehicle_color,
                                opacity=0.6,
                                popup=f"Vehicle {v_id + 1}"
                            ).add_to(m)
                            folium.PolyLine(
                                edge_coords,
                                weight=5,
                                color=edge_color,
                                opacity=0.95,
                                popup=f"Vehicle {v_id + 1}: {edge_label}"
                            ).add_to(m)
                        
                        # Collect coords for direction arrows
                        if vehicle_route_coords:
                            vehicle_route_coords.extend(edge_coords[1:])
                        else:
                            vehicle_route_coords.extend(edge_coords)
                        
                        # Collect coords for overall segment
                        if all_segment_coords:
                            all_segment_coords.extend(edge_coords[1:])
                        else:
                            all_segment_coords.extend(edge_coords)
                    
                    road_path_drawn = True
                    road_paths_drawn += 1
                    
                except (nx.NetworkXNoPath, nx.NodeNotFound, KeyError) as e:
                    road_path_drawn = False
            
            # Fallback: straight dashed line if no road path found
            if not road_path_drawn:
                fallback_paths_drawn += 1
                fallback_coords = [[from_loc['lat'], from_loc['lon']], [to_loc['lat'], to_loc['lon']]]
                
                if single_vehicle_mode:
                    # Single vehicle: clean traffic-only dashed line
                    folium.PolyLine(
                        fallback_coords,
                        weight=6,
                        color=congestion_color,
                        opacity=0.8,
                        dash_array='10, 10',
                        popup=f"⚠️ {from_loc['name'][:15]} → {to_loc['name'][:15]} (estimated)"
                    ).add_to(m)
                else:
                    # Multi-vehicle: vehicle-colored border + traffic color
                    folium.PolyLine(
                        fallback_coords,
                        weight=8,
                        color=vehicle_color,
                        opacity=0.5,
                        dash_array='15, 10',
                    ).add_to(m)
                    folium.PolyLine(
                        fallback_coords,
                        weight=4,
                        color=congestion_color,
                        opacity=0.8,
                        dash_array='10, 10',
                        popup=f"⚠️ Vehicle {v_id + 1}: {from_loc['name'][:15]} → {to_loc['name'][:15]} (estimated)"
                    ).add_to(m)
                
                # Collect coords for direction arrows
                if vehicle_route_coords:
                    vehicle_route_coords.append(fallback_coords[1])
                else:
                    vehicle_route_coords.extend(fallback_coords)
        
        # Add direction arrows for this vehicle's route
        if show_direction and len(vehicle_route_coords) >= 2:
            # Use AntPath for animated direction arrows
            arrow_color = '#2196F3' if single_vehicle_mode else vehicle_color
            AntPath(
                vehicle_route_coords,
                delay=1000,
                weight=3,
                color=arrow_color,
                pulse_color='white',
                dash_array=[10, 20],
                opacity=0.8
            ).add_to(m)
    
    return m, road_paths_drawn, fallback_paths_drawn

# ============================================================================
# MAIN APPLICATION
# ============================================================================

def main():
    # Header
    st.markdown('<p class="main-header">🚚 VRP Optimizer Dashboard</p>', unsafe_allow_html=True)
    st.markdown("*Optimize your delivery routes with multiple algorithms*")
    
    # Load road network
    with st.spinner("Loading road network..."):
        G, load_status = load_road_network()
    
    if G is None:
        st.error("Failed to load road network. Please check your internet connection.")
        return
    
    graph_nodes = get_graph_nodes(G)
    st.sidebar.success(f"✅ Road network: {len(graph_nodes):,} nodes")
    st.sidebar.caption(f"📂 {load_status}")
    
    # Sidebar Configuration
    st.sidebar.header("⚙️ Configuration")
    
    # Fleet Configuration
    st.sidebar.subheader("🚛 Fleet Settings")
    num_vehicles = st.sidebar.slider("Number of Vehicles", 1, 10, 3)
    vehicle_capacity = st.sidebar.slider("Vehicle Capacity (units)", 20, 500, 100, step=10)
    max_duration = st.sidebar.slider("Max Route Duration (minutes)", 60, 480, 240, step=30)
    
    # Algorithm Selection
    st.sidebar.subheader("🧮 Algorithm")
    algorithm = st.sidebar.selectbox("Routing Algorithm", list(ROUTING_ALGORITHMS.keys()))
    algo_info = ROUTING_ALGORITHMS[algorithm]
    st.sidebar.info(algo_info['description'])
    
    # Alpha slider for hybrid
    alpha = 0.5
    if algo_info['id'] == 'hybrid':
        alpha = st.sidebar.slider("α (Distance vs Traffic)", 0.0, 1.0, 0.5, 0.1,
                                  help="α=0: Pure traffic | α=1: Pure distance")
    
    # Traffic Configuration
    st.sidebar.subheader("🚦 Traffic Scenario")
    
    # Load historical traffic data
    traffic_df, traffic_file = load_historical_traffic_data()
    
    # Show data source info
    if traffic_df is not None:
        st.sidebar.success(f"📊 Historical data: {len(traffic_df)} road segments")
    else:
        st.sidebar.warning("⚠️ Using static traffic model")
    
    traffic_scenario = st.sidebar.selectbox(
        "Time of Day",
        list(TRAFFIC_SCENARIOS.keys()),
        format_func=lambda x: f"{TRAFFIC_SCENARIOS[x]['icon']} {x}"
    )
    
    # Handle auto time detection
    if TRAFFIC_SCENARIOS[traffic_scenario].get('auto', False):
        detected_scenario, speed_factor, congestion_mult, myt_time = get_auto_traffic_scenario()
        st.sidebar.info(f"🕐 Malaysia Time: {myt_time.strftime('%I:%M %p')} (UTC+8)")
        st.sidebar.caption(f"Detected: {detected_scenario}")
    else:
        speed_factor = TRAFFIC_SCENARIOS[traffic_scenario]['speed_factor']
        congestion_mult = TRAFFIC_SCENARIOS[traffic_scenario].get('congestion_mult', 1.0)
    
    st.sidebar.metric("Speed Factor", f"{speed_factor*100:.0f}%")
    
    # Build dynamic congestion zones from historical data
    global CONGESTION_ZONES
    if traffic_df is not None:
        CONGESTION_ZONES = build_dynamic_congestion_zones(traffic_df)
        st.sidebar.caption(f"📍 {len(CONGESTION_ZONES)} dynamic zones from data")
    else:
        CONGESTION_ZONES = STATIC_CONGESTION_ZONES.copy()
        st.sidebar.caption(f"📍 {len(CONGESTION_ZONES)} static zones")
    
    # Main content area
    col1, col2 = st.columns([1, 1])
    
    with col1:
        st.subheader("📍 Delivery Locations")
        
        # Preset selection
        preset_options = ["-- Custom --"] + list(PRESET_LOCATIONS.keys())
        preset = st.selectbox("Load Preset", preset_options, key="preset_select")
        
        # Load preset button
        if st.button("🔄 Load Selected Preset", key="load_preset_btn"):
            if preset != "-- Custom --":
                locations_data = []
                for loc in PRESET_LOCATIONS[preset]:
                    new_loc = loc.copy()
                    new_loc['time_window_start'] = 0
                    new_loc['time_window_end'] = 240
                    locations_data.append(new_loc)
                st.session_state.locations_df = pd.DataFrame(locations_data)
                st.rerun()
        
        # Initialize with default if not exists
        if 'locations_df' not in st.session_state:
            locations_data = []
            for loc in PRESET_LOCATIONS["Small Test (5 locations)"]:
                new_loc = loc.copy()
                new_loc['time_window_start'] = 0
                new_loc['time_window_end'] = 240
                locations_data.append(new_loc)
            st.session_state.locations_df = pd.DataFrame(locations_data)
        
        # Editable dataframe with add/delete rows
        st.markdown("**Edit locations, demands, and time windows:**")
        st.caption("💡 Click + to add rows, select rows and press Delete to remove")
        
        edited_df = st.data_editor(
            st.session_state.locations_df,
            column_config={
                "name": st.column_config.TextColumn("Location Name", width="medium"),
                "lat": st.column_config.NumberColumn("Latitude", format="%.4f", min_value=-90.0, max_value=90.0),
                "lon": st.column_config.NumberColumn("Longitude", format="%.4f", min_value=-180.0, max_value=180.0),
                "demand": st.column_config.NumberColumn("Demand", min_value=0, max_value=500, default=10),
                "is_depot": st.column_config.CheckboxColumn("Depot?", default=False),
                "time_window_start": st.column_config.NumberColumn("TW Start (min)", min_value=0, max_value=480, default=0),
                "time_window_end": st.column_config.NumberColumn("TW End (min)", min_value=0, max_value=480, default=240),
            },
            hide_index=True,
            use_container_width=True,
            num_rows="dynamic",  # Allow adding/removing rows
            key="locations_editor"
        )
        
        # Save edits to session state
        st.session_state.locations_df = edited_df
        
        # Search and add location form (like Google Maps)
        with st.expander("🔍 Search & Add Location", expanded=True):
            st.markdown("**Search by name (like Google Maps)**")
            
            search_col1, search_col2 = st.columns([3, 1])
            with search_col1:
                search_query = st.text_input(
                    "🔍 Search location", 
                    placeholder="e.g., KLCC, Mid Valley, Sunway Pyramid...",
                    key="location_search",
                    label_visibility="collapsed"
                )
            with search_col2:
                search_btn = st.button("🔍 Search", key="search_btn", use_container_width=True)
            
            # Search results
            if search_btn and search_query:
                with st.spinner("Searching..."):
                    results = geocode_multiple(search_query, "Kuala Lumpur, Malaysia", limit=5)
                
                if results:
                    st.session_state.search_results = results
                    st.success(f"Found {len(results)} result(s)")
                else:
                    st.warning(f"No results found for '{search_query}'. Try a different search term.")
                    st.session_state.search_results = []
            
            # Display search results for selection
            if 'search_results' in st.session_state and st.session_state.search_results:
                st.markdown("**Select a location to add:**")
                for i, (address, lat, lon) in enumerate(st.session_state.search_results):
                    # Truncate long addresses
                    display_addr = address[:60] + "..." if len(address) > 60 else address
                    
                    result_col1, result_col2 = st.columns([4, 1])
                    with result_col1:
                        st.caption(f"📍 {display_addr}")
                        st.caption(f"   ({lat:.4f}, {lon:.4f})")
                    with result_col2:
                        if st.button("Add", key=f"add_result_{i}"):
                            # Get short name from address
                            short_name = address.split(",")[0] if "," in address else address
                            new_row = pd.DataFrame([{
                                "name": short_name[:30],
                                "lat": lat,
                                "lon": lon,
                                "demand": 10,
                                "is_depot": False,
                                "time_window_start": 0,
                                "time_window_end": 240
                            }])
                            st.session_state.locations_df = pd.concat([st.session_state.locations_df, new_row], ignore_index=True)
                            st.session_state.search_results = []  # Clear results
                            st.rerun()
            
            st.divider()
            
            # Manual entry fallback
            with st.expander("📝 Or enter coordinates manually"):
                add_col1, add_col2 = st.columns(2)
                with add_col1:
                    new_name = st.text_input("Location Name", value="New Location", key="new_loc_name")
                    new_lat = st.number_input("Latitude", value=3.1400, format="%.4f", key="new_loc_lat")
                    new_demand = st.number_input("Demand", value=10, min_value=0, key="new_loc_demand")
                with add_col2:
                    new_is_depot = st.checkbox("Is Depot?", value=False, key="new_loc_depot")
                    new_lon = st.number_input("Longitude", value=101.7000, format="%.4f", key="new_loc_lon")
                    new_tw = st.slider("Time Window (min)", 0, 240, (0, 240), key="new_loc_tw")
                
                if st.button("Add Location", key="add_loc_btn"):
                    new_row = pd.DataFrame([{
                        "name": new_name,
                        "lat": new_lat,
                        "lon": new_lon,
                        "demand": new_demand,
                        "is_depot": new_is_depot,
                        "time_window_start": new_tw[0],
                        "time_window_end": new_tw[1]
                    }])
                    st.session_state.locations_df = pd.concat([st.session_state.locations_df, new_row], ignore_index=True)
                    st.rerun()
        
        # Summary metrics
        if not edited_df.empty:
            total_demand = edited_df['demand'].sum()
            total_capacity = num_vehicles * vehicle_capacity
            depot_count = edited_df['is_depot'].sum() if 'is_depot' in edited_df.columns else 0
            
            if depot_count == 0:
                st.warning("⚠️ No depot selected! Mark one location as depot.")
            elif depot_count > 1:
                st.warning("⚠️ Multiple depots selected! Only the first will be used.")
            
            st.markdown(f"""
            <div class="info-box">
                <b>Summary:</b> {len(edited_df)} locations | {total_demand} units demand | 
                {total_capacity} units capacity ({num_vehicles} × {vehicle_capacity})
            </div>
            """, unsafe_allow_html=True)
    
    with col2:
        st.subheader("🗺️ Location Preview")
        
        # Preview map
        if not edited_df.empty:
            center_lat = edited_df['lat'].mean()
            center_lon = edited_df['lon'].mean()
            
            preview_map = folium.Map(location=[center_lat, center_lon], zoom_start=13)
            for idx, row in edited_df.iterrows():
                color = 'black' if row.get('is_depot', False) else 'blue'
                icon = 'home' if row.get('is_depot', False) else 'info-sign'
                folium.Marker(
                    [row['lat'], row['lon']],
                    popup=f"{row['name']}<br>Demand: {row.get('demand', 0)}",
                    icon=folium.Icon(color=color, icon=icon)
                ).add_to(preview_map)
            
            st_folium(preview_map, width=None, height=400)
    
    # Solve Button
    st.markdown("---")
    
    solve_col1, solve_col2, solve_col3 = st.columns([1, 2, 1])
    with solve_col2:
        solve_button = st.button("🚀 Solve VRP", use_container_width=True, type="primary")
    
    if solve_button:
        # Validate inputs first
        if edited_df.empty:
            st.error("❌ No locations defined. Add locations first.")
            st.stop()
        
        if 'is_depot' not in edited_df.columns or edited_df['is_depot'].sum() == 0:
            st.error("❌ No depot selected. Mark one location as depot (check the 'Depot?' box).")
            st.stop()
        
        if len(edited_df) < 2:
            st.error("❌ Need at least 2 locations (1 depot + 1 delivery).")
            st.stop()
        
        # Reorder so depot is first
        depot_mask = edited_df['is_depot'] == True
        if depot_mask.any():
            depot_rows = edited_df[depot_mask]
            other_rows = edited_df[~depot_mask]
            ordered_df = pd.concat([depot_rows.head(1), other_rows], ignore_index=True)
        else:
            ordered_df = edited_df.copy()
        
        with st.spinner("Solving VRP... This may take a moment."):
            try:
                # Map locations to graph nodes
                location_nodes = []
                valid_indices = []
                skipped_locations = []
                
                for idx, row in ordered_df.iterrows():
                    try:
                        nearest, method = find_nearest_valid_node(G, row['lon'], row['lat'], graph_nodes)
                        if nearest is not None:
                            location_nodes.append(nearest)
                            valid_indices.append(idx)
                        else:
                            skipped_locations.append(f"{row['name']} (no nearby road)")
                    except Exception as e:
                        skipped_locations.append(f"{row['name']} (error: {str(e)[:50]})")
                
                if skipped_locations:
                    st.warning(f"⚠️ Skipped locations outside road network: {', '.join(skipped_locations)}")
                
                if len(location_nodes) < 2:
                    st.error("❌ Not enough valid locations found on the road network. Try locations within Central KL (lat: 3.12-3.17, lon: 101.68-101.72)")
                    st.stop()
                
                valid_df = ordered_df.iloc[valid_indices].reset_index(drop=True)
                
                # Ensure depot demand is 0
                demands = []
                for idx, row in valid_df.iterrows():
                    if idx == 0:  # Depot
                        demands.append(0)
                    else:
                        demands.append(int(row.get('demand', 10)))
                
                time_windows_s = [
                    (int(row.get('time_window_start', 0)) * 60, int(row.get('time_window_end', 240)) * 60)
                    for _, row in valid_df.iterrows()
                ]
                service_times = [0 if idx == 0 else 300 for idx in range(len(valid_df))]
                vehicle_caps = [vehicle_capacity] * num_vehicles
                max_duration_s = max_duration * 60
                
                # Debug info
                total_demand = sum(demands)
                total_capacity = sum(vehicle_caps)
                
                with st.expander("🔍 Debug Info", expanded=False):
                    st.write(f"**Locations:** {len(valid_df)}")
                    st.write(f"**Demands:** {demands} (Total: {total_demand})")
                    st.write(f"**Vehicles:** {num_vehicles} x {vehicle_capacity} = {total_capacity} capacity")
                    st.write(f"**Max duration:** {max_duration} min ({max_duration_s} sec)")
                    st.write(f"**Time windows:** {[(tw[0]//60, tw[1]//60) for tw in time_windows_s]} (in minutes)")
                    st.write(f"**Service times:** {[s//60 for s in service_times]} min per stop")
                    
                    if total_demand > total_capacity:
                        st.error(f"⚠️ Total demand ({total_demand}) exceeds total capacity ({total_capacity})!")
                
                st.info(f"🔍 Solving with {len(valid_df)} locations, {num_vehicles} vehicles, {algo_info['id']} algorithm...")
                
                # Check capacity feasibility
                if total_demand > total_capacity:
                    st.error(f"❌ Total demand ({total_demand}) exceeds total vehicle capacity ({total_capacity}). Add more vehicles or increase capacity.")
                    st.stop()
                
                # Solve
                start_time = time.time()
                
                if algo_info['id'] == 'static':
                    result = solve_static_vrp(
                        G, location_nodes, demands, service_times, vehicle_caps,
                        time_windows_s, max_duration_s, num_vehicles, speed_factor
                    )
                elif algo_info['id'] == 'dynamic':
                    result = solve_dynamic_vrp(
                        G, location_nodes, demands, service_times, vehicle_caps,
                        time_windows_s, max_duration_s, num_vehicles, speed_factor
                    )
                else:  # hybrid
                    result = solve_hybrid_vrp(
                        G, location_nodes, demands, service_times, vehicle_caps,
                        time_windows_s, max_duration_s, num_vehicles, speed_factor, alpha
                    )
                
                solve_time = time.time() - start_time
                
                # Store results in session state so they persist
                if result['feasible']:
                    st.session_state.vrp_result = {
                        'feasible': True,
                        'routes': result['routes'],
                        'valid_df': valid_df,
                        'demands': demands,
                        'solve_time': solve_time,
                        'algorithm': algo_info['id'],
                        'time_matrix': result.get('time_matrix'),
                        'distance_matrix': result.get('distance_matrix'),
                        'service_times': service_times,
                        'location_nodes': location_nodes  # Store for route visualization
                    }
                    # Store graph reference separately (not in dict to avoid serialization issues)
                    st.session_state.vrp_graph = G
                else:
                    st.session_state.vrp_result = {'feasible': False}
                    
            except Exception as e:
                st.error(f"Error solving VRP: {str(e)}")
                import traceback
                with st.expander("🔍 Error Details"):
                    st.code(traceback.format_exc())
    
    # Display results from session state (persists across reruns)
    if 'vrp_result' in st.session_state and st.session_state.vrp_result:
        result_data = st.session_state.vrp_result
        
        if result_data.get('feasible'):
            st.markdown('<div class="success-box">✅ Solution Found!</div>', unsafe_allow_html=True)
            
            # Results section
            st.subheader("📊 Results")
            
            routes = result_data['routes']
            valid_df = result_data['valid_df']
            demands = result_data['demands']
            solve_time = result_data['solve_time']
            algorithm = result_data['algorithm']
            
            # Metrics
            metric_cols = st.columns(4)
            with metric_cols[0]:
                st.metric("Active Vehicles", len(routes))
            with metric_cols[1]:
                total_stops = sum(len(r) - 2 for r in routes)
                st.metric("Total Stops", total_stops)
            with metric_cols[2]:
                st.metric("Algorithm", algorithm.title())
            with metric_cols[3]:
                st.metric("Solve Time", f"{solve_time:.2f}s")
            
            # Routes table
            st.subheader("🚛 Vehicle Routes")
            
            time_matrix = result_data.get('time_matrix')
            distance_matrix = result_data.get('distance_matrix')
            service_times = result_data.get('service_times', [0] * len(valid_df))
            
            total_travel_time = 0
            total_distance = 0
            
            for v_id, route in enumerate(routes):
                route_names = [valid_df.iloc[i]['name'] for i in route]
                route_demand = sum(demands[i] for i in route)
                
                # Calculate route travel time
                route_time_s = 0
                route_dist_m = 0
                if time_matrix:
                    for i in range(len(route) - 1):
                        from_idx, to_idx = route[i], route[i + 1]
                        route_time_s += time_matrix[from_idx][to_idx]
                        route_time_s += service_times[from_idx] if i > 0 else 0  # Add service time except at depot
                
                if distance_matrix:
                    for i in range(len(route) - 1):
                        from_idx, to_idx = route[i], route[i + 1]
                        if distance_matrix[from_idx][to_idx] < 999999:
                            route_dist_m += distance_matrix[from_idx][to_idx]
                
                total_travel_time += route_time_s
                total_distance += route_dist_m
                
                # Format time display
                route_mins = route_time_s // 60
                route_hours = route_mins // 60
                remaining_mins = route_mins % 60
                
                if route_hours > 0:
                    time_str = f"{int(route_hours)}h {int(remaining_mins)}m"
                else:
                    time_str = f"{int(route_mins)}m"
                
                # Format distance
                if route_dist_m >= 1000:
                    dist_str = f"{route_dist_m/1000:.1f} km"
                else:
                    dist_str = f"{int(route_dist_m)} m"
                
                with st.expander(f"🚚 Vehicle {v_id + 1} - {len(route)-2} stops | ⏱️ {time_str} | 📏 {dist_str} | 📦 {route_demand} units"):
                    # Route sequence
                    st.markdown("**Route:**")
                    st.write(" → ".join(route_names))
                    
                    # Detailed breakdown
                    if time_matrix or distance_matrix:
                        st.markdown("**Segment Details:**")
                        segment_data = []
                        for i in range(len(route) - 1):
                            from_idx, to_idx = route[i], route[i + 1]
                            from_name = valid_df.iloc[from_idx]['name']
                            to_name = valid_df.iloc[to_idx]['name']
                            
                            seg_time = time_matrix[from_idx][to_idx] if time_matrix else 0
                            seg_dist = distance_matrix[from_idx][to_idx] if distance_matrix and distance_matrix[from_idx][to_idx] < 999999 else 0
                            
                            segment_data.append({
                                'From': from_name[:20],
                                'To': to_name[:20],
                                'Time': f"{int(seg_time//60)}m" if seg_time else "-",
                                'Distance': f"{seg_dist/1000:.1f}km" if seg_dist >= 1000 else f"{int(seg_dist)}m"
                            })
                        
                        st.dataframe(pd.DataFrame(segment_data), hide_index=True, use_container_width=True)
            
            # Total summary
            total_mins = total_travel_time // 60
            total_hours = total_mins // 60
            total_remaining_mins = total_mins % 60
            
            if total_hours > 0:
                total_time_str = f"{int(total_hours)}h {int(total_remaining_mins)}m"
            else:
                total_time_str = f"{int(total_mins)}m"
            
            total_dist_str = f"{total_distance/1000:.1f} km" if total_distance >= 1000 else f"{int(total_distance)} m"
            
            st.markdown(f"""
            <div class="info-box">
                <b>📊 Total Summary:</b> {len(routes)} vehicles | ⏱️ {total_time_str} total travel | 📏 {total_dist_str} total distance
            </div>
            """, unsafe_allow_html=True)
            
            # Route Map
            st.subheader("🗺️ Route Map")
            
            # Vehicle selector and display options
            map_col1, map_col2 = st.columns([2, 1])
            with map_col1:
                vehicle_options = [f"Vehicle {i+1}" for i in range(len(routes))]
                selected_vehicles_names = st.multiselect(
                    "🚛 Select vehicles to display",
                    options=vehicle_options,
                    default=vehicle_options,
                    key="vehicle_selector"
                )
                # Convert names to indices
                selected_vehicle_ids = [int(v.split()[-1]) - 1 for v in selected_vehicles_names]
            
            with map_col2:
                show_direction = st.checkbox("🧭 Show direction arrows", value=True, key="show_direction")
                show_traffic_overlay = st.checkbox("📊 Show historical traffic", value=False, key="show_traffic_overlay")
            
            st.caption("🚦 Outer band = Vehicle color | Inner color = Traffic (Green=Free → Red=Heavy)")
            
            # Debug: Show route info
            if not routes:
                st.warning("No routes to display")
            elif not selected_vehicle_ids:
                st.info("Select at least one vehicle to display routes")
            else:
                st.caption(f"Showing {len(selected_vehicle_ids)} of {len(routes)} vehicle routes")
            
            # Get graph and location nodes for actual road routing
            graph_for_map = st.session_state.get('vrp_graph')
            location_nodes = result_data.get('location_nodes')
            
            # Debug info
            if graph_for_map is not None:
                st.caption(f"✓ Graph loaded with {graph_for_map.number_of_nodes()} nodes")
            else:
                st.warning("⚠️ No graph available for road routing")
            
            if location_nodes is not None:
                st.caption(f"✓ {len(location_nodes)} location nodes mapped")
            else:
                st.warning("⚠️ No location nodes available")
            
            route_map, road_paths, fallback_paths = create_route_map(
                valid_df, routes,
                valid_df['lat'].mean(), valid_df['lon'].mean(),
                time_matrix=time_matrix,
                distance_matrix=distance_matrix,
                G=graph_for_map,
                location_nodes=location_nodes,
                selected_vehicles=selected_vehicle_ids if selected_vehicle_ids else None,
                show_direction=show_direction,
                traffic_df=traffic_df,
                show_traffic_overlay=show_traffic_overlay
            )
            
            # Show routing debug info
            total_segments = road_paths + fallback_paths
            if total_segments > 0:
                if fallback_paths == 0:
                    st.caption(f"✓ All {road_paths} route segments follow actual roads")
                else:
                    st.caption(f"🛣️ Road segments: {road_paths} | ⚠️ Straight-line fallback: {fallback_paths}")
            
            # Use unique key based on solve_time to force map refresh
            st_folium(route_map, width=None, height=500, key=f"result_map_{int(solve_time*1000)}")
            
            # Clear results button
            if st.button("🔄 Clear Results"):
                del st.session_state.vrp_result
                st.rerun()
        else:
            st.error("❌ No feasible solution found. Try:")
            st.markdown("""
            - Adding more vehicles
            - Increasing vehicle capacity
            - Extending max route duration
            - Widening time windows
            - Reducing demand at locations
            """)
    
    # Footer
    st.markdown("---")
    
    # Clear data button
    if st.button("🗑️ Clear All Locations", key="clear_all"):
        st.session_state.locations_df = pd.DataFrame([{
            "name": "Depot",
            "lat": 3.1342,
            "lon": 101.6866,
            "demand": 0,
            "is_depot": True,
            "time_window_start": 0,
            "time_window_end": 240
        }])
        st.rerun()
    
    st.markdown("""
    <div style="text-align: center; color: gray; font-size: 0.8rem;">
        VRP Optimizer Dashboard | Built with Streamlit, OR-Tools, and OSMnx<br>
        📍 Valid area: Greater KL (lat: 3.05-3.20, lon: 101.60-101.75)
    </div>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    main()
