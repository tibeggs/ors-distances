from geopy.distance import geodesic
import openrouteservice
from typing import Dict, Tuple, List
import openrouteservice.exceptions
import pandas as pd
import json
from concurrent.futures import ThreadPoolExecutor, as_completed, wait
import time

# Coordinates (latitude, longitude)
LOCAL_ORS_URL = "http://localhost:8080/ors" 

# uuid_list = ['d25d2bdd-8134-43e5-a21a-f3e9f56760e6', '2c97491d-c1ed-48fd-9265-c373d45eeb50', '88765e22-1673-4f21-922e-d0e69b2b3e1b', '9d0bf01f-cfba-4c14-be2d-01254612db97', '34055d38-51b3-4f73-a7ac-1c3afe05cfc7']
uuid_list = ['dd2b6117-32a9-4483-b4a4-67fc3c0ab19c', '80d627de-594b-4e5d-9b84-1c0e39124ef7', '2c97491d-c1ed-48fd-9265-c373d45eeb50', 'd25d2bdd-8134-43e5-a21a-f3e9f56760e6', 'a252df0c-6ee4-4783-a4b8-e04a2b0fced4']
class ors_distance:
    def __init__(self, data: Dict, end: Tuple[float, float], keep_route_uuids: List):
        self.client = openrouteservice.Client(base_url=LOCAL_ORS_URL)
        self.data = data
        self.end = end
        self.geojson = None
        self.keep_route_uuids = keep_route_uuids
    
    def calculate_distance(self, start: Tuple[float, float], end: Tuple[float, float], keep_route=False) -> float:
        # Request the route
        try:
            route = self.client.directions(
                coordinates=[start, end],
                profile="driving-car",  # Options: 'driving-car', 'cycling-regular', 'foot-walking', etc.
                format="geojson",
            )
            
            # Extract distance (in meters) from the response
            distance_meters = route["features"][0]["properties"]["segments"][0]["distance"]
            distance_km = distance_meters / 1000
            if keep_route:
                return distance_km, route
        except openrouteservice.exceptions.ApiError as e:
            print(f"Error calculating distance: {e}")
            # distance_meters = 2000001
            return None, None
            
        distance_km = distance_meters / 1000
        return distance_km, None
    
    def analyze_distance(self):
        def process_entry(key, value):
            start = (value['longitude'], value['latitude'])
            uuid = key
            # print(uuid)
            keep_route = uuid in self.keep_route_uuids

            osr_distance, geojson = self.calculate_distance(start, self.end, keep_route)
            bf_distance = self.bf_calculate_distance(start, self.end)

            # Update shared data structure (synchronized section)
            self.data[key]['osr_distance'] = osr_distance
            self.data[key]['bf_distance'] = bf_distance

            # Save GeoJSON if present
            if geojson:
                with open(f"route_{key}.geojson", "w") as f:
                    json.dump(geojson, f)

            # print(f"Distance from {key} to point: {osr_distance:.2f} km")

        with ThreadPoolExecutor() as executor:
            futures = [executor.submit(process_entry, key, value) for key, value in self.data.items()]
            
            # Ensure all tasks are completed
            wait(futures)

        return self.data
    
    def bf_calculate_distance(self, point1, point2):
        return geodesic(reversed(point1), reversed(point2)).kilometers
    

def main(args):
    df = pd.read_csv(args.input_file)
    data_dict = {}
    for index, row in df.iterrows():
        data_dict[row[0]] = {'latitude': row['latitude'], 'longitude': row['longitude']}
    uuid_list = args.uuid_list.split(',') if args.uuid_list else []
    ors = ors_distance(data_dict, end= args.end, keep_route_uuids=uuid_list)
    distance_data = ors.analyze_distance()
    ndf = pd.DataFrame.from_dict(distance_data, orient='index')
    ndf.to_csv(args.output_file)


def parse_lat_long(value):
    try:
        lat, lon = map(float, value.split(','))
        return lon, lat
    except ValueError:
        raise argparse.ArgumentTypeError("Latitude and Longitude must be in the format: lat,lon")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input_file", type=str, help="Input CSV file")
    parser.add_argument("-o", "--output_file", type=str, help="Output CSV file")
    parser.add_argument("-u","--uuid_list", default=None ,type=str, help="List of UUIDs to keep route data for")
    parser.add_argument("-e", "--end", default="40.239029,-76.846428", type=parse_lat_long, help="End location as 'longitude,latitude'")
    args = parser.parse_args()
    print(args)
    start_time = time.perf_counter()
    main(args)
    end_time = time.perf_counter() - start_time
    print(end_time)


