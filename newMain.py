import glob

from pcap_etl import Transform, Extract, Load
import sys
import os
import pandas as pd


if __name__ == '__main__':
    pcap_dir = sys.argv[1]
    c_dir = sys.argv[2]
    print(f"\n{pcap_dir}")
    print(f"\n{c_dir}")
    Extract().convert_pcap(pcap_dir, c_dir)
    loader = Load(c_dir)
    for file in glob.glob(fr"{c_dir}\*.csv"):
        print(file)
        print("Munging data")
        df = Transform().mung(file)
        print(df)
        print("saving file to csv")
        loader.save_file(df)
    print("adding to database")
    loader.update_pg("pcap")
    print("completed")
    print("")
