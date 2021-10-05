"""
    Name: WiFi PCAP ETL
    Author: Damian Brito
    Date: July 2021
    Description: Consists of classes used to perform ETL on WiFi
                 packet capture files.

    Modules:
        os - used for filename inspection and creation
        glob - used to find pcap and csv files in directory
        subprocess.Popen - used to run tshark in command line
        numpy - used to manipulate series in dataframe
        pandas - used to convert pcap csv into dataframe
        pg_connect.pg_admin = class used to connect and edit
                             postgres database.
"""
import os
from concurrent.futures import ProcessPoolExecutor
from glob import glob
from subprocess import Popen
import numpy as np
import pandas as pd
from pg_connect import PgAdmin


class Extract:
    """
    Used to extract pcap files and convert them into csv files.
    """
    def __init__(self):
        pass

    @staticmethod
    def tshark_tool(pcap_dir, csv_dir):
        """
        Used to convert pcap files into csv files using tshark and
        windows cmd.

        :param pcap_dir: directory containing pcap files
        :param csv_dir: directory containing csv files
        :return: returns command line output and any errors thrown by tshark
        """
        process = Popen(["tshark_cmd_tool.bat", pcap_dir, csv_dir])
        stdout, stderr = process.communicate()
        return stdout, stderr

    def convert_pcap(self, pcap_dir, csv_folder):
        """
        Used to convert pcap files into csv files using multiprocessing

        :param pcap_dir: directory containing pcap files
        :param csv_folder: directory containing csv files
        """
        pcaps = glob(os.path.join(pcap_dir, '*.pcap'))
        csvs = [os.path.join(csv_folder, i.split('\\')[-1].replace('pcap', 'csv')) for i in pcaps]
        with ProcessPoolExecutor() as executor:
            executor.map(self.tshark_tool, pcaps, csvs)


class Transform:
    """
    Converts pcap csv files and performs data munging to dataframe.
    """

    def __init__(self):
        self.dataframe = None
        self.columns = {'frame.number': 'frame_number',
                        'frame.time': 'time',
                        'wlan.fcs.status': 'fcs',
                        'frame.time_relative': 'relative_time',
                        'frame.time_delta': 'timedelta',
                        'wlan.ta': 'transmit_address',
                        'wlan.ra': 'receiving_address',
                        'wlan.ssid': 'ssid',
                        'wlan_radio.channel': 'channel',
                        'radiotap.dbm_antsignal': 'rssi',
                        'radiotap.dbm_antnoise': 'noise',
                        'wlan.fc.type': 'fc_type',
                        'wlan.fc.type_subtype': 'fc_subtype',
                        'wlan_radio.data_rate': 'data_rate',
                        'wlan.qbss.scount': 'client_counts',
                        'wlan.fc.retry': 'retries'}

    def mung(self, filename):
        """
        Takes in csv file and returns a cleaned dataframe.

        :param filename: csv filename
        :return: packet capture dataframe
        """
        self.dataframe = pd.read_csv(filename, error_bad_lines=False).query("`wlan.fcs.status` == 1")
        name = filename.split("\\")[-1].removesuffix('.csv')
        self.dataframe.insert(2, "filename", name, False)
        self.dataframe["wlan.ta"] = self.clean_addr("wlan.ta")
        self.dataframe["wlan.ra"] = self.clean_addr("wlan.ra")
        self.dataframe = self.clean_ssid(self.dataframe)
        self.dataframe = self.clean_frame_time(self.dataframe)
        self.dataframe = self.clean_subtype(self.dataframe)
        self.dataframe = self.clean_type(self.dataframe)
        self.dataframe = self.clean_channel(self.dataframe)
        self.dataframe = self.clean_counts(self.dataframe)
        self.dataframe = self.clean_signal(self.dataframe)
        self.dataframe = self.clean_noise(self.dataframe)
        self.dataframe.convert_dtypes()
        self.dataframe = self.clean_retrys(self.dataframe)
        self.dataframe.dropna(subset=['wlan.fcs.status'], inplace=True)
        self.dataframe.rename(columns=self.columns, inplace=True)
        return self.dataframe.reset_index(drop=True)

    def clean_addr(self, address_type):
        """
        Removes any null bssid addresses and returns a clean column.

        :param address_type: column name [wlan.ta or wlan.ra]
        :return: address series with no null values
        """
        return self.dataframe[f"{address_type}"].fillna(np.nan).replace([np.nan], ["None"])

    @staticmethod
    def clean_hex(val):
        """
        Takes in hexadecimal value and returns integer value.

        :param val: hexadecimal value
        :return: integer or none
        """
        try:
            return int(val, 16)
        except TypeError:
            pass

    @staticmethod
    def remove_dt_str(item):
        """
        Converts pcap datetime string and returns date and time only.

        :param item: datetime string
        :return: date and time
        """
        return item.split('Eastern Daylight Time')[0]

    def clean_frame_time(self, dataframe):
        """
        Converts and cleans pcap frame time.

        :param dataframe: pcap dataframe
        :return: dataframe with clean frame time series
        """
        dataframe['frame.time'] = dataframe.apply(
            lambda row: self.remove_dt_str(row['frame.time']), axis=1
        )
        dataframe['frame.time'] = pd.to_datetime(dataframe['frame.time'])
        dataframe['frame.time'] = dataframe['frame.time'].fillna(0)
        return dataframe

    @staticmethod
    def clean_ssid(dataframe):
        """
        Replaces unknown ssids with Hidden SSID label.

        :param dataframe: pcap dataframe
        :return: dataframe with clean ssid series
        """
        dataframe["wlan.ssid"] = dataframe["wlan.ssid"].fillna("Hidden SSID")
        return dataframe

    @staticmethod
    def clean_fcs_status(dataframe):
        """
        Cleans up fcs values and converts into integers from dataframe.

        :param dataframe: pcap dataframe
        :return: dataframe with fcs status series with integer datatype.
        """
        dataframe = dataframe.query('`wlan.fcs.status` => 0 and `wlan.fcs.status` <= 1')
        dataframe["wlan.fcs.status"] = dataframe["wlan.fcs.status"].astype("Int32")
        return dataframe

    @staticmethod
    def clean_retrys(dataframe):
        """
        Converts retries column into integer datatype.

        :param dataframe: pcap dataframe
        :return: dataframe with clean retries series
        """
        dataframe["wlan.fc.retry"] = dataframe["wlan.fc.retry"].astype("Int64")
        return dataframe

    def clean_ds(self, dataframe):
        """
        Converts ds hexadecimal numbers and converts to integer.

        :param dataframe: pcap dataframe
        :return: dataframe with integer datatype ds values.
        """
        dataframe["wlan.fc.ds"] = dataframe.apply(
            lambda row: self.clean_hex(row["wlan.fc.ds"]), axis=1)
        return dataframe

    @staticmethod
    def convert_to_dbm(mw_array):
        """
        Takes in array with mW and converts into dBm values.

        :param mw_array: array with mW values
        :return: array with dBm values
        """
        dbm_array = 10 * np.log10(mw_array)
        return dbm_array

    @staticmethod
    def convert_to_mw(dbm_array):
        """
        Converts array with dBm values into mW values.

        :param dbm_array: array with dBm values
        :return: mW array
        """
        mw_array = 10 ** (dbm_array / 10)
        return mw_array

    @staticmethod
    def clean_subtype(dataframe):
        """
        Takes in subtypes from pcap csv and replaces null values
        with a default value.

        :param dataframe: pcap dataframe
        :return: dataframe with new fc subtype series
        """
        dataframe['wlan.fc.type_subtype'] = pd.to_numeric(
            dataframe['wlan.fc.type_subtype'], errors='coerce'
        ).fillna(352).astype(int)
        return dataframe

    @staticmethod
    def clean_type(dataframe):
        """
        Cleans fc type column in pcap dataframe.

        :param dataframe: pcap dataframe
        :return: dataframe with clean fc type series
        """
        dataframe['wlan.fc.type'] = pd.to_numeric(dataframe['wlan.fc.type'],
                                                  errors='coerce').fillna(352).astype(int)
        return dataframe

    @staticmethod
    def clean_channel(dataframe):
        """
        Cleans channel from dataframe column.

        :param dataframe: pcap dataframe
        :return: dataframe with clean channel series
        """
        dataframe = dataframe.astype({'wlan_radio.channel': 'int32'})
        dataframe = dataframe.query('`wlan_radio.channel` >= 1')
        return dataframe

    @staticmethod
    def clean_counts(dataframe):
        """
        Cleans up client counts from pcap dataframe.

        :param dataframe: pcap dataframe
        :return: dataframe with clean and non-nullable
                 client count series
        """
        dataframe['wlan.qbss.scount'] = dataframe['wlan.qbss.scount'].fillna(0)
        dataframe = dataframe.astype({'wlan.qbss.scount': 'int'})
        return dataframe

    @staticmethod
    def clean_signal(dataframe):
        """
        Cleans signal strength or rssi values and removes nullable
        non-existent values.

        :param dataframe: pcap dataframe
        :return: dataframe with clean rssi series
        """
        dataframe = dataframe.astype({'radiotap.dbm_antsignal': 'int'})
        dataframe['radiotap.dbm_antsignal'] = dataframe['radiotap.dbm_antsignal'].fillna(0)
        dataframe = dataframe[dataframe['radiotap.dbm_antsignal'] < -20]
        return dataframe

    @staticmethod
    def clean_noise(dataframe):
        """
        Cleans noise values and removes nullable
        non-existent values.

        :param dataframe: pcap dataframe
        :return: dataframe with clean noise series
        """
        dataframe = dataframe.astype({'radiotap.dbm_antnoise': 'int'})
        dataframe['radiotap.dbm_antnoise'] = dataframe['radiotap.dbm_antnoise'].fillna(0)
        return dataframe

    @staticmethod
    def clean_frame_len(dataframe):
        """
        Cleans frame length values from pcap dataframe.

        :param dataframe: pcap dataframe
        :return: dataframe with clean frame length values
        """
        dataframe['frame.len'] = pd.to_numeric(dataframe['frame.len'], errors='coerce')
        dataframe.dropna(subset=['frame.len'])
        return dataframe

    def dbm_to_mw(self, dataframe):
        """
        Converts noise and rssi values from mW to dBm.

        :param dataframe: pcap dataframe
        :return: dataframe with rssi and noise series in mW
        """
        dataframe['rssi(mW)'] = dataframe['radiotap.dbm_antsignal'].apply(self.convert_to_mw)
        dataframe['noise(mW)'] = dataframe['radiotap.dbm_antnoise'].apply(self.convert_to_mw)
        return dataframe


class Load:
    """
    Saves clean dataframe into temporary csvs and loads
    files into postgresSQL database. Can also create the
    pcap table if missing.

    :param save_dir: directory to save clean pcap csv files
    """

    def __init__(self, save_dir):
        self.save_dir = save_dir

    @staticmethod
    def create_query(dataframe, table):
        """
        Creates query from dataframe for postgres database.

        :param dataframe: pcap dataframe
        :param table: name of table to insert data
        :return: postgreSQL query with dataframe data as string.
        """
        if table == 'good_pkts':
            columns = ["frame_number", "time",
                       "filename", "fcs",
                       "relative_time", "timedelta",
                       "transmit_address", "receiving_address",
                       "ssid, channel", "rssi", "noise",
                       "fc_type", "fc_subtype", "data_rate",
                       "client_counts", "retries"]
        data = f"{', '.join(str(val) for val in dataframe.to_records(index=False))}"
        return f"""INSERT INTO {table}({', '.join(str(val) for val in columns)}) VALUES {data}"""

    def save_file(self, dataframe):
        """
        Saves cleaned dataframe into csv file.

        :param dataframe: pcap dataframe
        :return: csv file saved in directory
        """
        filename = dataframe.filename[1]
        csv_file = f"{os.path.join(self.save_dir, filename)}.csv"
        dataframe.to_csv(csv_file, index=False)

    @staticmethod
    def create_table(db_obj):
        """
        Creates empty pcap table to postgreSQL database.

        :param db_obj: database connection target
        :return: None
        """
        commands = """
        CREATE TABLE IF NOT EXISTS good_pkts (
            id SERIAL PRIMARY KEY,
            frame_number INT NOT NULL,
            time TIMESTAMP NOT NULL,
            filename VARCHAR(100) NOT NULL,
            fcs INT,
            relative_time DECIMAL,
            timedelta DECIMAL,
            transmit_address VARCHAR(18),
            receiving_address VARCHAR(18),
            ssid VARCHAR(100),
            channel INT,
            rssi INT,
            noise INT,
            fc_type INT,
            fc_subtype INT,
            data_rate FLOAT,
            client_counts INT,
            retries INT
        )
        """
        db_obj.write(commands)

    def update_pg(self, database_name):
        """
        Connects and writes pcap data into postgreSQL database.

        :param database_name: name of database to insert pcap table.
        :return: None
        """
        db_conn = PgAdmin(database_name, ("postgres", "aBcd@1234"))
        for file in glob(f"{self.save_dir}/*.csv"):
            dataframe = pd.read_csv(file)
            good_sql = self.create_query(dataframe, "good_pkts")
            existing_tables = db_conn.get_tables()
            if "good_pkts" not in existing_tables:
                self.create_table(db_conn)
            db_conn.write(good_sql)
        db_conn.close()
