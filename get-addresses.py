import openrouteservice
import pandas as pd
import uuid
import random
import math
import requests
import pandas as pd
from typing import List, Tuple, Dict
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

LOCAL_ORS_URL = "http://localhost:8080/ors" 

class TestLocationGenerator:
    def __init__(self, radius_miles: float = 150):
        self.CENTER_LAT = 40.2732  # Harrisburg, PA
        self.CENTER_LON = -76.8867
        self.MILES_TO_DEGREES = 1/69  # approximate at this latitude
        self.radius_miles = radius_miles
        self.base_url = LOCAL_ORS_URL
        self.client = openrouteservice.Client(base_url=LOCAL_ORS_URL)
        
    
    def generate_random_point(self, radius_miles: float) -> Tuple[float, float]:
        """Generate a random point within radius_miles of center."""
        radius_deg = radius_miles * self.MILES_TO_DEGREES
        
        # Use square root for uniform distribution in circular area
        r = math.sqrt(random.random()) * radius_deg
        theta = random.random() * 2 * math.pi
        
        lat = self.CENTER_LAT + r * math.cos(theta)
        lon = self.CENTER_LON + r * math.sin(theta)
        
        return (lat, lon)
    
    def validate_distance_m(self, point: Tuple[float, float]) -> bool:
        """Validate that point is reachable within 150 miles by road."""
        try:
            response = self.client.distance_matrix(
                locations=[
                    (self.CENTER_LON, self.CENTER_LAT),  # Center point
                    (point[1], point[0])  # Destination point
                ],
                profile='driving-car',
                metrics=['distance'],
                units='mi'
            )
            
            # Extract the distance
            distance = response['distances'][0][1]
            if distance is not None and distance <= self.radius_miles:
                ret = {'latitude': point[0],
                    'longitude': point[1]}
                return ret 
        
        except openrouteservice.exceptions.ApiError as e:
            print(f"API error: {e}")
        except (KeyError, IndexError, TypeError) as e:
            print(f"Parsing error: {e}")
        return False
    
    def validate_multiple_distances(self, points: List[Tuple[float, float]], max_workers: int = 5) -> List[bool]:
        """Validate multiple points concurrently using multithreading."""
        results = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_point = {executor.submit(self.validate_distance_m, point): point for point in points}

            for future in as_completed(future_to_point):
                point = future_to_point[future]
                if future.result():
                    try:
                        results.append(future.result())
                    except Exception as e:
                        print(f"Error processing point {point}: {e}")

        return results
    
    def generate_test_locations(self, count: int = 10) -> List[Dict]:
        """Generate test locations with road snapping and validation."""
        locations = []
        attempts = 0
        max_attempts = count * 3  
        
        random_points = [self.generate_random_point(self.radius_miles) for _ in range(count)]
        locations = self.validate_multiple_distances(random_points)
        
        
        while len(locations) < count and attempts < max_attempts:
            print(f"Not enought locations currently {len(locations)} need {count}")
            lat, lon = self.generate_random_point(self.radius_miles)
            
            if self.validate_distance_m((lat, lon)):
                locations.append({
                    'latitude': lat,
                    'longitude': lon
                })
            
            attempts += 1
        
        return locations

def parse_lat_long(value):
    try:
        lat, lon = map(float, value.split(','))
        return lon, lat
    except ValueError:
        raise argparse.ArgumentTypeError("Latitude and Longitude must be in the format: lat,lon")
    
# Usage example:
if __name__ == "__main__":
    import argparse
    start_time = time.perf_counter()
    parser = argparse.ArgumentParser()
    parser.add_argument("-r", "--radius", type=float, default=150, help="Search radius in miles")
    parser.add_argument("-n", "--number", type=int, default=10, help="Number of test locations")
    parser.add_argument("-e", "--end", type=parse_lat_long, default="40.239029,-76.846428", help="End location as 'longitude,latitude'")
    args=parser.parse_args()
    
    radius=args.radius
    number_of_locations = args.number
    treatment_center = args.end
    
    generator = TestLocationGenerator(radius_miles=radius)
    
    # # Generate 10 test locations
    test_locations = generator.generate_test_locations(number_of_locations)
    print(test_locations)
    
    # # Convert to DataFrame for easy handling
    df = pd.DataFrame(test_locations)
    df['ID'] = [uuid.uuid4() for _ in range(len(df))]
    # # Save to CSV
    df.to_csv(f'test_locations_radius_{radius}_local_auto_{number_of_locations}_{uuid.uuid4()}.csv', index=False)
    
    print(f"Generated {len(test_locations)} test locations:")
    # print(df.head())
    end_time = time.perf_counter() - start_time
    print(f"Time taken: {end_time:.2f} seconds")
    # df = pd.read_csv('test_locations.csv')
    # print(df.head())