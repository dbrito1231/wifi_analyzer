import glob
import time
from pcap_etl import Transform, Extract, Load
import sys


if __name__ == '__main__':
    pcap_dir = sys.argv[1]
    c_dir = sys.argv[2]
    print(f"\n{pcap_dir}")
    print(f"{c_dir}")
    start_time = time.time()
    Extract().convert_pcap(pcap_dir, c_dir)
    end_time = time.time()
    print(f"\nExtractor start time: {start_time}")
    print(f"Extractor end time: {end_time}")
    print(f"duration: {(end_time - start_time) / 60}")
    loader = Load(c_dir)
    start_time = time.time()
    for file in glob.glob(fr"{c_dir}\*.csv"):
        df = Transform().mung(file)
        loader.save_file(df)
    end_time = time.time()
    print(f"\nTransform start time: {start_time}")
    print(f"Transform end time: {end_time}")
    print(f"duration: {(end_time - start_time) / 60}")
    start_time = time.time()
    loader.update_pg("pcap")
    end_time = time.time()
    print(f"\nLoad start time: {start_time}")
    print(f"Load end time: {end_time}")
    print(f"duration: {(end_time - start_time) / 60}")
    print("completed")
    print("")
