"""
    Name: main.py
    Author: Damian Brito
    Date: July 2021
    Description: Main file to run and test PCAP ETL.

    Modules:
        glob - used to find pcap and csv files in directory
        time - used to measure ETL duration
        sys - used to grab command line arguments
        pcap_etl - WiFi PCAP ETL module
            Extract - used to extract pcap files into csv files
            Transform - used to clean and mung pcap csv files
            Load - loads dataframe into postgreSQL database

"""
import glob
import time
import sys
from pcap_etl import Transform, Extract, Load


if __name__ == '__main__':
    # variables declared
    # packet capture files directory
    pcap_dir = sys.argv[1]
    # csv files directory
    c_dir = sys.argv[2]

    # pcap_etl classes declared
    extractor = Extract()
    transform = Transform()
    loader = Load(c_dir)

    # prints out file directories
    print(f"\n{pcap_dir}")
    print(f"{c_dir}")

    # runs and measures extracting pcap to csv files
    # using Extract instance
    start_time = time.time()
    extractor.convert_pcap(pcap_dir, c_dir)
    end_time = time.time()
    print(f"\nExtractor start time: {start_time}")
    print(f"Extractor end time: {end_time}")
    print(f"duration: {(end_time - start_time) / 60}")

    # runs and measures pcap data munging using Transform
    # and Load instances
    start_time = time.time()
    for file in glob.glob(fr"{c_dir}\*.csv"):
        df = transform.mung(file)
        loader.save_file(df)
    end_time = time.time()
    print(f"\nTransform start time: {start_time}")
    print(f"Transform end time: {end_time}")
    print(f"duration: {(end_time - start_time) / 60}")

    # runs and measures pcap dataframes into postgreSQL
    # database
    start_time = time.time()
    loader.update_pg("pcap")
    end_time = time.time()
    print(f"\nLoad start time: {start_time}")
    print(f"Load end time: {end_time}")
    print(f"duration: {(end_time - start_time) / 60}")

    print("completed")
