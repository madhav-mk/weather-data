import subprocess
import pandas as pd
import numpy as np

def to_api_datetime(date_str, end=False):
    from datetime import datetime
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    hour = "00" if not end else "23"
    return dt.strftime("%Y%m%d") + hour


def clean_col(series):
    return (
        series.astype(str)
        .str.replace("%", "", regex=False)
        .str.replace("Hpa", "", regex=False)
        .str.replace("hPa", "", regex=False)
        .str.replace("-", "", regex=False)
        .str.strip()
        .replace("", np.nan)
    )


def main():
    wmo = input("WMO ID: ").strip()
    start = input("Start date (YYYY-MM-DD): ").strip()
    end = input("End date (YYYY-MM-DD): ").strip()

    start_fmt = to_api_datetime(start, end=False)
    end_fmt = to_api_datetime(end, end=True)

    url = (
        "https://www.meteomanz.com/rethours"
        f"?ind={wmo}&start={start_fmt}&end={end_fmt}&lang=eng"
    )

    raw_file = f"{wmo}_raw.csv"
    clean_file = f"{wmo}_clean.csv"

    subprocess.run(["wget", url, "-O", raw_file], check=True)

    df = pd.read_csv(raw_file, sep=",", engine="python")
    df.columns = df.columns.str.strip()


    df = df[df["Date"].notna()]
    df = df[df["Date"] != "Date"]

    df["Temp.(ºC)"] = pd.to_numeric(df["Temp.(ºC)"], errors="coerce")

    df["Rel. Hum.(%)"] = clean_col(df["Rel. Hum.(%)"])
    df["Pressure/Geopot."] = clean_col(df["Pressure/Geopot."])
    df["Wind speed(Km/h)"] = clean_col(df["Wind speed(Km/h)"])

    df["Rel. Hum.(%)"] = pd.to_numeric(df["Rel. Hum.(%)"], errors="coerce")
    df["Pressure/Geopot."] = pd.to_numeric(df["Pressure/Geopot."], errors="coerce")
    df["Wind speed(Km/h)"] = pd.to_numeric(df["Wind speed(Km/h)"], errors="coerce")


    df["Date"] = df["Date"].astype(str).str.strip()
    df["UTC time"] = df["UTC time"].astype(str).str.strip()

    df["datetime_utc"] = pd.to_datetime(
        df["Date"] + " " + df["UTC time"],
        errors="coerce",
        utc=True
    )

    df = df[df["datetime_utc"].notna()]


    df = df[[
        "datetime_utc",
        "Temp.(ºC)",
        "Rel. Hum.(%)",
        "Pressure/Geopot.",
        "Wind speed(Km/h)"
    ]]

    df.columns = [
        "datetime_utc",
        "temperature_c",
        "humidity_percent",
        "pressure_hpa",
        "wind_speed_kmh"
    ]

    df.to_csv(clean_file, index=False)

    print(f"\nSaved → {clean_file}")
    print(f"Rows: {len(df)}")


if __name__ == "__main__":
    main()