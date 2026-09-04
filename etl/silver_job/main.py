from .silver_train_events import main as rail_silver_main
from .silver_weather_current import main as current_weather_silver_main
from .silver_weather_forecast import main as forecast_weather_silver_main



def main():
    rail_silver_main()
    rail_silver_main()
    current_weather_silver_main()
    forecast_weather_silver_main()

    # Other jobs...


if __name__ == "__main__":
    main()