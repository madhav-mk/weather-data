# REQUIREMENTS
# pip3 install openmeteo-requests
# pip3 install requests-cache retry-requests numpy pandas requests

import openmeteo_requests
import pandas as pd
import requests
import requests_cache
from retry_requests import retry
from pathlib import Path

OUTPUT_DIR = Path(__file__).parent / "output"


def geocode_city(name: str) -> dict:
    """Resolve a city name to coordinates via Open-Meteo Geocoding API."""
    resp = requests.get(
        "https://geocoding-api.open-meteo.com/v1/search",
        params={"name": name, "count": 5, "language": "en"},
        timeout=10,
    )
    resp.raise_for_status()
    results = resp.json().get("results", [])

    if not results:
        raise ValueError(f"No locations found for '{name}'.")

    if len(results) == 1:
        r = results[0]
        print(f"    Matched: {r['name']}, {r.get('admin1', '')}, {r['country']}  ({r['latitude']:.4f}, {r['longitude']:.4f})")
        return r

    # Multiple hits — let the user disambiguate
    print(f"\n  Multiple matches for '{name}':")
    for i, r in enumerate(results, 1):
        admin = r.get("admin1", "")
        print(f"    {i}. {r['name']}{', ' + admin if admin else ''}, {r['country']}  ({r['latitude']:.4f}, {r['longitude']:.4f})")

    while True:
        pick = input("  Select number: ").strip()
        if pick.isdigit() and 1 <= int(pick) <= len(results):
            return results[int(pick) - 1]
        print(f"  Enter a number between 1 and {len(results)}.")


def select_cities() -> list[dict]:
    """Prompt user to enter one or more city names and geocode each."""
    print()
    print("  Enter city names separated by commas (e.g. Sydney, Tokyo, London).")
    print()

    while True:
        raw = input("Cities: ").strip()
        tokens = [t.strip() for t in raw.split(",") if t.strip()]

        if not tokens:
            print("  Please enter at least one city name.")
            continue

        cities = []
        failed = []

        for name in tokens:
            try:
                result = geocode_city(name)
                cities.append(result)
            except Exception as e:
                print(f"  ✗ {e}")
                failed.append(name)

        # Deduplicate by (lat, lon)
        seen, unique = set(), []
        for c in cities:
            key = (round(c["latitude"], 4), round(c["longitude"], 4))
            if key not in seen:
                seen.add(key)
                unique.append(c)

        if not unique:
            print("  No valid cities resolved. Try again.\n")
            continue

        if failed:
            proceed = input(f"\n  {len(failed)} city/cities failed. Proceed with the rest? (y/n): ").strip().lower()
            if proceed != "y":
                continue

        return unique


def get_date_range() -> tuple[str, str]:
    print()
    while True:
        start = input("Start date (YYYY-MM-DD): ").strip()
        end   = input("End date   (YYYY-MM-DD): ").strip()
        try:
            pd.to_datetime(start)
            pd.to_datetime(end)
            if start <= end:
                return start, end
            print("  Start date must be before end date.")
        except ValueError:
            print("  Invalid date format. Use YYYY-MM-DD.")


def fetch_weather(city: dict, start_date: str, end_date: str, client) -> pd.DataFrame:
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude":   city["latitude"],
        "longitude":  city["longitude"],
        "start_date": start_date,
        "end_date":   end_date,
        "hourly": ["temperature_2m", "relative_humidity_2m", "wind_speed_100m", "surface_pressure"],
        "timezone": "auto",
    }

    label = city["name"]
    print(f"\n  Fetching {label}...")
    responses = client.weather_api(url, params=params)
    response  = responses[0]

    print(f"    Coordinates : {response.Latitude():.4f}°N  {response.Longitude():.4f}°E")
    print(f"    Elevation   : {response.Elevation()} m asl")
    print(f"    Timezone    : {response.Timezone()} ({response.TimezoneAbbreviation()})")

    hourly = response.Hourly()
    dates  = pd.date_range(
        start=pd.to_datetime(hourly.Time()    + response.UtcOffsetSeconds(), unit="s", utc=True),
        end  =pd.to_datetime(hourly.TimeEnd() + response.UtcOffsetSeconds(), unit="s", utc=True),
        freq =pd.Timedelta(seconds=hourly.Interval()),
        inclusive="left",
    )

    return pd.DataFrame({
        "date":                 dates,
        "temperature_2m":       hourly.Variables(0).ValuesAsNumpy(),
        "relative_humidity_2m": hourly.Variables(1).ValuesAsNumpy(),
        "wind_speed_100m":      hourly.Variables(2).ValuesAsNumpy(),
        "surface_pressure":     hourly.Variables(3).ValuesAsNumpy(),
    })


def save_csv(df: pd.DataFrame, city: dict, start_date: str, end_date: str) -> Path:
    OUTPUT_DIR.mkdir(exist_ok=True)
    city_slug = city["name"].replace(" ", "_")
    filename  = OUTPUT_DIR / f"{city_slug}_{start_date}_{end_date}.csv"
    df.to_csv(filename, index=False)
    print(f"    Saved → output/{filename.name}  ({len(df)} rows)")
    return filename


def main():
    print("╔══════════════════════════════╗")
    print("║   Open-Meteo Weather Fetcher ║")
    print("╚══════════════════════════════╝")

    cities     = select_cities()
    start, end = get_date_range()

    cache_session = requests_cache.CachedSession('.cache', expire_after=-1)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo     = openmeteo_requests.Client(session=retry_session) # type: ignore

    print(f"\n=== Fetching {len(cities)} city/cities ===")
    saved  = []
    failed = []

    for city in cities:
        try:
            df   = fetch_weather(city, start, end, openmeteo)
            path = save_csv(df, city, start, end)
            saved.append(path.name)
        except Exception as e:
            print(f"    ✗ {city['name']} failed: {e}")
            failed.append(city["name"])

    print(f"\n=== Done ===")
    print(f"  ✓ {len(saved)} file(s) saved to /output/")
    if failed:
        print(f"  ✗ {len(failed)} failed: {', '.join(failed)}")


if __name__ == "__main__":
    main()