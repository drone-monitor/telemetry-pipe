import requests
from pymavlink import mavutil
import threading


def fetch_and_publish_gps(
        drone_address: str, drone_port: int, publish_url: str
) -> None:
    """
    Connects to a drone using MAVProxy, fetches GPS data, and sends a POST request with the GPS data.

    Parameters:
    - drone_address (str): The IP address of the drone.
    - drone_port (int): The port number for connecting to the drone.
    - publish_url (str): The URL for sending the POST request.

    Returns:
    - None
    """
    # Connect to the drone using MAVProxy
    master = mavutil.mavlink_connection(f"udpin:{drone_address}:{drone_port}")
    master.wait_heartbeat()
    print(
        "Heartbeat from system (system %u component %u)"
        % (master.target_system, master.target_component)
    )

    def gps_publish_thread():
        while True:
            # Fetch GPS data from the drone
            msg = master.recv_match(type="GLOBAL_POSITION_INT", blocking=True)
            if msg is not None:
                # Extract GPS coordinates and altitude
                lat = msg.lat / 1e7
                lon = msg.lon / 1e7
                alt = msg.alt / 1000.0  # Convert altitude from millimeters to meters

                # Prepare payload for POST request
                payload = {
                    "droneId": 1,
                    "missionId": 18,
                    "height": alt,
                    "coordinate": [lon, lat]
                }

                # Print the payload for debugging
                print("Sending payload:", payload)

                # Send a POST request to the specified URL
                response = requests.post(publish_url, json=payload)

                # Print the response for debugging
                print("Response:", response.text)

    # Start a thread for publishing GPS data
    publish_thread = threading.Thread(target=gps_publish_thread)
    publish_thread.start()


if __name__ == "__main__":
    # Example usage:
    fetch_and_publish_gps("127.0.0.1", 14550, "http://localhost:12345/status")
