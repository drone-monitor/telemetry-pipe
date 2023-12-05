import pandas as pd

# Sample DataFrame with columns: 'latitude', 'longitude', 'altitude'
# Add other necessary columns according to your mission requirements
data = {
    "latitude": [37.7749, 37.7748, 37.7750],
    "longitude": [-122.4194, -122.4195, -122.4196],
    "altitude": [100, 150, 120],
}
df = pd.DataFrame(data)

# Create a mission file (waypoints) in MAVLink format
mission_file = "QGC WPL 110\n"
for index, row in df.iterrows():
    mission_file += f'{index+1}\t1\t0\t16\t0\t0\t0\t0\t{row["latitude"]}\t{row["longitude"]}\t{row["altitude"]}\t1\n'

# Save the mission file
with open("mission.txt", "w") as file:
    file.write(mission_file)

# 'mission.txt' now contains the waypoints in MAVLink format
