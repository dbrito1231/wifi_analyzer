from collections import Counter
import sqlite3, sys, os
from glob import glob
from concurrent.futures import ProcessPoolExecutor
from subprocess import Popen
import pandas as pd
from pcap_cleaner import pCleaner, pStats


cleaner = pCleaner()
stats = pStats()


def createSSIDData(dataframe):
    data = dataframe[
        ["frame.time_delta",
            "frame.number",
            "wlan.fcs.status",
            "wlan.ssid",
            "wlan_radio.channel",
            "radiotap.dbm_antsignal",
            "radiotap.dbm_antnoise",
            "wlan.fc.type",
            "wlan.fc.type_subtype",
            "wlan_radio.data_rate",
            "wlan.fc.retry",
            "wlan.qbss.cu"]]
    dtypes = {
        "frame.time_delta": "float",
        "frame.number": "int",
        "wlan.fcs.status": "int",
        "wlan.ssid": "str",
        "wlan_radio.channel": "int",
        "radiotap.dbm_antsignal": "int",
        "radiotap.dbm_antnoise": "int",
        "wlan.fc.type": "int",
        "wlan.fc.type_subtype": "int",
        "wlan_radio.data_rate": "int",
        "wlan.fc.retry": "int",
        "wlan.qbss.cu": "int"}
    data = data.astype(dtypes)
    return data


def cleanDataframeData(dataframe):
    data = dataframe[
        ["frame.number",
         "frame.time_delta",
         "wlan_radio.channel",
         "wlan.fc.ds",
         "wlan.ta",
         "wlan.ra",
         "radiotap.dbm_antsignal",
         "radiotap.dbm_antnoise",
         "wlan_radio.data_rate",
         "data.len",
         "wlan_radio.duration",
         "wlan.fc.retry",
         "wlan.fcs.status"]]
    dtypes = {"floor": "str",
              "location": "str",
              "frame.number": "int",
              "wlan_radio.channel": 'int',
              "wlan.fc.ds": "int16",
              "wlan.ta": "str",
              "wlan.ra": "str",
              "radiotap.dbm_antsignal": "int",
              "radiotap.dbm_antnoise": "int",
              "wlan_radio.data_rate": "int",
              "data.len": "int",
              "wlan_radio.duration": "int",
              "frame.time_delta": "float",
              "wlan.fc.retry": "int",
              "wlan.fcs.status": "int"}
    data = data.astype(dtypes)
    return data

# TODO: work out table entries for database
#  ssids, channels, beacons, association, authentication

# TODO: create channel df that contains rssi, noise, utilization
# TODO: create SSID df that contains rssi, noise, utilization, channel, client counts



def writeToDB(dataframe, parquet_store, fl, tp):
    raw_data = dataframe.copy()
    raw_data["floor"] = fl
    raw_data["location"] = tp
    ssids = stats.getSSIDs(dataframe)
    chanLst = pStats.getChannels(dataframe)

    chanDict = dict(map(stats.getChannelDf, dataframe, chanLst))
    beacons = stats.getBeacons(dataframe)
    packet_types = ["mgmt", "data", "ctrl"]
    pTypeDict = dict(map(stats.getTypeFrames, dataframe, packet_types))

    # try:
    #     ssids = stats.getSSIDs(dataframe)
    #     chanLst = pStats.getChannels(dataframe)
    #     chanDict = dict(map(stats.getChannelDf, dataframe, chanLst))
    #     beacons = stats.getBeacons(dataframe)
    #     packet_types = ["mgmt", "data", "ctrl"]
    #     pTypeDict = dict(map(stats.getTypeFrames, dataframe, packet_types))
    #     print("")
    #     print("")
    # except:
    #     dataframe["frame.time_relative"] = pd.to_numeric(dataframe["frame.time_relative"])
    #     print(fl, tp)
    #     print(dataframe["frame.time_relative"].dtype)


def get_raw_dataframe_by_id(folder_path, level, tp):
    file_name = f'{level}_{tp}'
    column_dtypes = {'wlan_radio.channel': 'str',
                     'wlan.qbss.scount': 'str',
                     'wlan.fc.type_subtype': 'str',
                     'frame.len': 'str',
                     'frame.number': 'str',
                     'wlan.fcs.status': 'str',
                     'radiotap.datarate': 'str',
                     'data.len': 'str',
                     'radiotap.dbm_antnoise': 'str',
                     'radiotap.dbm_antsignal': 'str',
                     'wlan.fc.type': 'str',
                     'wlan.cisco.ccx1.name': 'str'}
    dataframe = pd.read_csv(folder_path,
                            dtype=column_dtypes,
                            error_bad_lines=False,
                            warn_bad_lines=False,
                            engine='python')

    dataframe = cleaner.return_cleanDf(dataframe)
    return dataframe


def get_merged_test_point(csv_file):
    level = csv_file.split('\\')[-1].split('_')[0]
    testpoint = csv_file.split('\\')[-1].split('_')[1]
    tp_df = get_raw_dataframe_by_id(csv_file, level, testpoint)
    cleaned_tp = (level, testpoint)
    return tp_df, cleaned_tp


def get_levels_and_test_points_csv(csv_dir):
    csv_files = glob(os.path.join(csv_dir, "*.csv"))
    return Counter([f.split('\\')[-1].split('_')[0] for f in csv_files])


def csv_converter(csv_folder, par_dir):
    level_dict = get_levels_and_test_points_csv(csv_folder)
    for level in level_dict.keys():
        level_files = glob(os.path.join(csv_folder, f'{level}*'))
        for file in level_files:
            tp_dataframe, testpoint = get_merged_test_point(file)
            # write_raw_dataframe_to_parquet(tp_dataframe, par_dir, *testpoint)
            try:
                write_raw_dataframe_to_parquet(tp_dataframe, par_dir, *testpoint)
            except ValueError:
                continue
        print("")


def tshark_tool(pcap_dir, csv_dir):
    p = Popen(["tshark_cmd_tool.bat", pcap_dir, csv_dir])
    stdout, stderr = p.communicate()
    return stdout, stderr


def PCAP_converter_dist(pcap_dir, csv_folder):
    pcaps = glob(os.path.join(pcap_dir, '*.pcap'))
    csvs = [os.path.join(csv_folder, i.split('\\')[-1].replace('pcap', 'csv')) for i in pcaps]
    with ProcessPoolExecutor() as executor:
        executor.map(tshark_tool, pcaps, csvs)


def create_database(dbName):
    db_filename = dbName
    db = sqlite3.connect(db_filename)
    cursor = db.cursor()
    beaconsQuery = """
	CREATE TABLE beacons (
		id INTEGER PRIMARY KEY ASC,
		floor_id TEXT,
		test_point INT,
		BSSID TEXT,
		SSID TEXT,
		channel INT,
		band TEXT,
		utilization REAL,
		ap_name TEXT,
		num_clients INT, 
		rssi INT, 
		fcs TEXT)"""
    dataframesQuery = """
	CREATE TABLE data_frames (
		id INTEGER PRIMARY KEY ASC,
		floor_id TEXT,
		test_point TEXT,
		channel INT,
		band TEXT,
		DS TEXT,
		TA TEXT, 
		RA TEXT,
		data_rate INT, 
		data_size INT, 
		duration REAL,
		retry TEXT, 
		fcs TEXT)
	"""
    frameinfoQuery = """
	CREATE TABLE frame_info (
		id INTEGER PRIMARY KEY ASC,
		floor_id TEXT,
		test_point TEXT,
		band TEXT,
		source TEXT, 
		dest TEXT, 
		frameType TEXT, 
		channel INT,
		retry INT,
		fcs TEXT)
	"""

    try:
        for table in [beaconsQuery, dataframesQuery, frameinfoQuery]:
            cursor.execute(table)
    except sqlite3.OperationalError:
        raise sqlite3.OperationalError
    db.commit()
    db.close()



if __name__ == '__main__':
    output = sys.argv[2]
    sourceDir = sys.argv[1]
    csv_dir = os.path.join(output, 'csv')
    db_path = os.path.join(output, 'pcap_db.db')

    try:
        os.mkdir(csv_dir)
    except FileExistsError:
        pass
    try:
        create_database(db_path)
    except sqlite3.OperationalError:
        pass
    PCAP_converter_dist(sourceDir, csv_dir)
    csv_converter(csv_dir, output)
    print("")
