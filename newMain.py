import glob

from pcap_etl import Transform, Extract, Load
import pandas as pd



if __name__ == '__main__':
    # Extract().convert_pcap(r"C:\Users\domin\Desktop\Folders\pcap_project\Post_Processing\sample",
    #                          r"C:\Users\domin\Desktop\github\wifi_analyzer\wifi_analyzer\csv")
    # processor = Transform()
    # test = pd.read_csv(r"csv/ClubBowl_1_A.csv", error_bad_lines=False)
    loader = Load(".")
    for file in glob.glob(r"C:\Users\domin\Desktop\github\wifi_analyzer\wifi_analyzer\csv\*.csv"):
        df = Transform().mung(file)
        loader.save_file(df)
    print("")
