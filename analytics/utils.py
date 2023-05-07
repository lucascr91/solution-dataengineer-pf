'''Utils for ETL process.'''
# pylint: disable=line-too-long,invalid-name
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from geopy import distance
import pandas as pd


def extract_temp_data_points(session: sessionmaker) -> pd.DataFrame:
    """
    Retrieve hourly maximum temperature and data point count from the devices table.

    This function queries the devices table to fetch the device_id, maximum temperature, and the number of data points
    for each device grouped by the device_id and hour. The data is then returned as a pandas DataFrame with columns:
    device_id, hour, max_temperature, and data_points.

    Returns:
        df (pd.DataFrame): A DataFrame containing device_id, hour, max_temperature, and data_points.
    """

    query = text(
        """
    SELECT 
    device_id, 
    date_trunc('hour', to_timestamp(time::bigint)) as hour, 
    MAX(temperature) as max_temperature,
    COUNT(*) as data_points
    FROM devices
    GROUP BY device_id, hour
    ORDER BY device_id, hour
    """
    )

    result = session.execute(query)

    df = pd.DataFrame(
        result, columns=["device_id", "hour", "max_temperature", "data_points"]
    )

    session.close()

    return df


def extract_distances(session: sessionmaker) -> pd.DataFrame:
    """
    Calculate the total distance traveled by each device, grouped by device_id and hour.

    css

    This function queries the devices table to fetch the device_id, hour, latitude, and longitude for each device.
    It then calculates the total distance traveled by each device by summing up the distances between consecutive
    location points, grouped by device_id and hour. The resulting data is returned as a pandas DataFrame with columns:
    device_id, hour, and total_distance.

    Returns:
        df (pd.DataFrame): A DataFrame containing device_id, hour, and total_distance traveled by each device.
    """

    query = text(
        """
        SELECT device_id, date_trunc('hour', to_timestamp(time::bigint)) as hour,
            CAST((location::json)->>'latitude' AS double precision) as latitude,
            CAST((location::json)->>'longitude' AS double precision) as longitude
        FROM devices
        ORDER BY device_id, hour;
        """
    )

    results = session.execute(query)

    distances = {}
    for row in results:
        device_id, hour, lat, lon = row
        key = (device_id, hour)
        current_location = (lat, lon)

        if key not in distances:
            distances[key] = {"total_distance": 0, "last_location": current_location}
        else:
            last_location = distances[key]["last_location"]
            dist = distance.distance(current_location, last_location).km
            distances[key]["total_distance"] += dist
            distances[key]["last_location"] = current_location

    df = pd.DataFrame.from_dict(distances, orient="index")
    df.reset_index(inplace=True)
    df.columns = ["device_id", "hour", "total_distance", "last_location"]
    df.drop("last_location", axis=1, inplace=True)

    session.close()

    return df


def load_data2mysql(data: pd.DataFrame, connection_string: str) -> None:
    """
    Load aggregated device data into a MySQL table.

    This function retrieves temperature data points and distance data from the get_temp_data_points() and get_distances()
    functions respectively. It then merges both DataFrames on device_id and hour. The merged DataFrame is then stored in a
    MySQL table called 'devices_agg' using the provided connection string. If the table already exists, its content will be
    replaced with the new data.

    Returns:
    None
    """
    engine = create_engine(connection_string)

    data.to_sql(name="devices_agg", con=engine, if_exists="replace", index=False)


def print_results() -> None:
    """
    Print the contents of the 'devices_agg' MySQL table.

    This function establishes a connection to the MySQL database using the provided connection string. It then reads the
    contents of the 'devices_agg' table and stores them in a pandas DataFrame. Finally, the contents of the DataFrame are
    printed to the console.

    Returns:
    None
    """
    connection_string = (
        "mysql+pymysql://nonroot:nonroot@mysql_db/analytics?charset=utf8"
    )
    engine = create_engine(connection_string)

    df = pd.read_sql_table("devices_agg", engine)

    print(df.to_markdown(index=False))
