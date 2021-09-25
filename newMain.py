import glob

from pcap_etl import Transform, Extract, Load
import sys
import os
import pandas as pd


if __name__ == '__main__':
    pcap_dir = sys.argv[1]
    c_dir = sys.argv[2]
    Extract().convert_pcap(pcap_dir,c_dir)
    loader = Load(c_dir)
    for file in glob.glob(fr"{c_dir}\*.csv"):
        print(file)
        df = Transform().mung(file)
        print(df)
        loader.save_file(df)
    loader.update_pg("pcap")
    print("")
