import pickle
import networkx as nx

# Check if cached graph exists
try:
    G = pickle.load(open('cache/kl_osmnx_accurate.pkl', 'rb'))
    print("Loaded: kl_osmnx_accurate.pkl")
except:
    try:
        G = pickle.load(open('cache/road_graph_pyrosm_v2_101.6800_3.1200_101.7200_3.1700.pkl', 'rb'))
        print("Loaded: pyrosm graph")
    except:
        print("No graph found")
        exit()

nodes = list(G.nodes(data=True))
lats = [n[1]['y'] for n in nodes]
lons = [n[1]['x'] for n in nodes]

print(f"Total nodes: {len(nodes)}")
print(f"Graph bounds:")
print(f"  Lat: {min(lats):.4f} to {max(lats):.4f}")
print(f"  Lon: {min(lons):.4f} to {max(lons):.4f}")

# Check SCC
scc = max(nx.strongly_connected_components(G), key=len)
print(f"Largest SCC: {len(scc)} nodes ({100*len(scc)/len(nodes):.1f}%)")

# Check if KL Sentral and KL Tower are in bounds
# KL Sentral: approx 3.1344, 101.6861
# KL Tower: approx 3.1528, 101.7038
print("\nLocation checks:")
print(f"  KL Sentral (3.1344, 101.6861): lat in range = {min(lats) <= 3.1344 <= max(lats)}, lon in range = {min(lons) <= 101.6861 <= max(lons)}")
print(f"  KL Tower (3.1528, 101.7038): lat in range = {min(lats) <= 3.1528 <= max(lats)}, lon in range = {min(lons) <= 101.7038 <= max(lons)}")

# Check some edges for geometry
edges = list(G.edges(data=True))[:5]
print("\nSample edges:")
for e in edges:
    has_geom = 'geometry' in e[2]
    keys = list(e[2].keys())
    print(f"  {e[0]} -> {e[1]}")
    print(f"    has geometry: {has_geom}")
    print(f"    keys: {keys}")
    if has_geom:
        print(f"    geometry type: {type(e[2]['geometry'])}")
        print(f"    geometry: {e[2]['geometry']}")

# Check node attributes
nodes = list(G.nodes(data=True))[:3]
print("\nSample nodes:")
for n in nodes:
    print(f"  Node {n[0]}: {n[1]}")
