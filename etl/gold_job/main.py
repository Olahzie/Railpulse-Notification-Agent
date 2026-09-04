from .gold_weather_train_enriched import main as train_weather_main
from .gold_forecast_train_enriched import main as forecast_weather_main
from .gold_station_reliability import main as build_station_metrics
from .gold_route_reliability import main as build_routes_metrics
from .ai_alert_context import main as ai_alert_main


def main():
    train_weather_main()
    forecast_weather_main()
    build_station_metrics()
    build_routes_metrics()
    ai_alert_main()


if __name__ == "__main__":
    main()