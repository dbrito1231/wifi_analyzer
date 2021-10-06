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
from datetime import datetime
import json
import sys
from pcap_etl import Transform, Extract, Load


if __name__ == '__main__':
    # variables declared
    # packet capture files directory
    try:
        with open(sys.argv[1], "r") as file:
            config_dict = json.loads(file.read())

        pcap_dir = config_dict['PCAP_DIR']
        # csv files directory
        c_dir = config_dict['CSV_DIR']

        # pcap_etl classes declared
        extractor = Extract()
        transform = Transform()
        loader = Load(c_dir)

        # prints out file directories
        print(f"\n{pcap_dir}")
        print(f"{c_dir}")

        # runs and measures extracting pcap to csv files
        # using Extract instance
        start_time = datetime.now()
        print(f"\nExtractor start time: {start_time}")
        extractor.convert_pcap(pcap_dir, c_dir)
        end_time = datetime.now()
        print(f"Extractor end time: {end_time}")
        print(f"duration: {(end_time - start_time)}")

        # runs and measures pcap data munging using Transform
        # and Load instances
        start_time = datetime.now()
        print(f"\nTransform start time: {start_time}")
        for file in glob.glob(fr"{c_dir}\*.csv"):
            df = transform.mung(file)
            loader.save_file(df)
        end_time = datetime.now()
        print(f"Transform end time: {end_time}")
        print(f"duration: {(end_time - start_time)}")

        # runs and measures pcap dataframes into postgreSQL
        # database
        start_time = datetime.now()
        print(f"\nLoad start time: {start_time}")
        loader.update_pg(config_dict['DATABASE'],
                         config_dict['DB_USER'],
                         config_dict['DB_PASSW'])
        end_time = datetime.now()
        print(f"Load end time: {end_time}")
        print(f"duration: {(end_time - start_time)}")
        print("completed")
    except IndexError:
        print("\nMissing config file!!! Try again.")
    sys.exit(1)
