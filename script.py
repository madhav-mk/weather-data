import subprocess
import pandas as pd
import sys
import os

def to_api_datetime(date_str, end=False):
    from datetime import datetime
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    hour = "00" if not end else "23"
    return dt.strftime("%Y%m%d") + hour


def main():
    if len(sys.argv) != 4:
        print("Usage: python3 script.py <WMO_ID> <START_DATE> <END_DATE>")
        print("Example: python3 script.py 94768 2026-01-01 2026-03-01")
        sys.exit(1)

    wmo = sys.argv[1]
    start = sys.argv[2]
    end = sys.argv[3]

    # create output directory
    output_dir = "output"
    os.makedirs(output_dir, exist_ok=True)

    start_fmt = to_api_datetime(start, end=False)
    end_fmt = to_api_datetime(end, end=True)

    url = (
        "https://www.meteomanz.com/rethours"
        f"?ind={wmo}&start={start_fmt}&end={end_fmt}&lang=eng"
    )

    raw_file = os.path.join(output_dir, f"{wmo}_raw.csv")
    clean_file = os.path.join(output_dir, f"{wmo}_clean.csv")

    subprocess.run(["wget", url, "-O", raw_file], check=True)

    df = pd.read_csv(raw_file)

    # keep only required columns
    df = df[[
        "Station",
        "Date",
        "UTC time",
        "Temp.(ºC)",
        "Rel. Hum.(%)",
        "Pressure/Geopot.",
        "Wind speed(Km/h)"
    ]]

    # rename columns
    df.columns = [
        "station",
        "date",
        "utc_time",
        "temperature_c",
        "humidity_percent",
        "pressure",
        "wind_speed_kmh"
    ]

    df.to_csv(clean_file, index=False)

    # delete raw file after successful conversion
    os.remove(raw_file)

    print(f"Saved → {clean_file}")
    print(f"Rows: {len(df)}")


if __name__ == "__main__":
    main()