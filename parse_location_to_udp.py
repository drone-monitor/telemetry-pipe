from pymavlink import mavutil
import socket
import threading


def fetch_and_publish_gps(
    drone_address: str, drone_port: int, publish_port: int
) -> None:
    """
    Connects to a drone using MAVProxy, fetches GPS data, and publishes it on a specified port.

    Parameters:
    - drone_address (str): The IP address of the drone.
    - drone_port (int): The port number for connecting to the drone.
    - publish_port (int): The port number for publishing GPS data.

    Returns:
    - None
    """
    # Connect to the drone using MAVProxy
    master = mavutil.mavlink_connection(f"udpin:{drone_address}:{drone_port}")
    # master = mavutil.mavlink_connection(f"tcpin:{drone_address}:5760")
    master.wait_heartbeat()
    print(
        "Heartbeat from system (system %u component %u)"
        % (master.target_system, master.target_component)
    )

    # Create a UDP socket for publishing GPS data
    publish_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    def gps_publish_thread():
        while True:
            # Fetch GPS data from the drone
            msg = master.recv_match(type="GLOBAL_POSITION_INT", blocking=True)
            msg_baro = master.recv_match(type="VFR_HUD", blocking=True)

            if msg is not None:
                # Extract GPS coordinates
                lat = msg.lat / 1e7
                lon = msg.lon / 1e7

                if msg_baro is not None:
                    # Extract barometric altitude
                    altitude_baro = msg_baro.alt
                else:
                    altitude_baro = -1
                # Prepare data to publish
                gps_data = f"Latitude: {lat}, Longitude: {lon}, Altitude (Baro): {altitude_baro} meters"
                print(gps_data)
                # Publish GPS data on another port
                publish_socket.sendto(gps_data.encode(), ("localhost", publish_port))

    # Start a thread for publishing GPS data
    publish_thread = threading.Thread(target=gps_publish_thread)
    publish_thread.start()


if __name__ == "__main__":
    # Example usage:
    fetch_and_publish_gps("127.0.0.1", 14550, 9000)
