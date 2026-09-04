from .rail_bronze_job import main as rail_main
from .weather_bronze_job import main as weather_main
from .weather_forecast_bronze_job import main as weather_forecast_main

def main():
    rail_main()
    weather_main()
    weather_forecast_main()


if __name__ == "__main__":
    main()