from handlers.HandleToHttp import fetch_and_publish_gps
import webbrowser


if __name__ == "__main__":
    webbrowser.open_new('http://localhost:3000')
    fetch_and_publish_gps("127.0.0.1", 14550, "http://localhost:12345/status/telemetry")