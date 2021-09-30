import os
from concurrent.futures import ProcessPoolExecutor
from pg_connect import pg_admin
from glob import glob
from subprocess import Popen
import numpy as np
import pandas as pd


class PcapStats:

    @staticmethod
    def get_chans(packet_dataframe):
        channels = packet_dataframe['wlan_radio.channel'].unique()
        unique_channels = [x for x in channels if (np.isnan(x) is not True)]
        return unique_channels

    @staticmethod
    def get_ssid(dataframe):
        return dataframe["wlan.ssid"].unique()

    @staticmethod
    def get_chan_df(dataframe, ch):
        return dataframe.query(f'`wlan_radio.channel` == {ch}')

    @staticmethod
    def get_beacons(dataframe):
        return dataframe.query('`wlan.fc.type_subtype` == "8"')

    @staticmethod
    def get_frame_types(dataframe, pkt_id):
        types = {"mgmt": 0,
                 "ctrl": 1,
                 "data": 2}
        return dataframe.query(f'`wlan.fc.type` == "{types[pkt_id]}"')

    @staticmethod
    def get_good_pkts(dataframe):
        good = dataframe.query(f'`wlan.fcs.status` == "1"')
        return good

    @staticmethod
    def get_bad_pkts(dataframe):
        bad = dataframe.query(f'`wlan.fcs.status` == "0"')
        return bad


class Extract:

    @staticmethod
    def tshark_tool(pcap_dir, csv_dir):
        p = Popen(["tshark_cmd_tool.bat", pcap_dir, csv_dir])
        stdout, stderr = p.communicate()
        return stdout, stderr

    def convert_pcap(self, pcap_dir, csv_folder):
        pcaps = glob(os.path.join(pcap_dir, '*.pcap'))
        csvs = [os.path.join(csv_folder, i.split('\\')[-1].replace('pcap', 'csv')) for i in pcaps]
        with ProcessPoolExecutor() as executor:
            executor.map(self.tshark_tool, pcaps, csvs)


class Transform:

    def mung(self, filename):
        self.df = pd.read_csv(filename, error_bad_lines=False)
        name = filename.split("\\")[-1].removesuffix('.csv')
        self.df.insert(2, "filename", name, False)
        self.df["wlan.ta"] = self.df["wlan.ta"].fillna(np.nan).replace([np.nan], ["None"])
        self.df["wlan.ra"] = self.df["wlan.ra"].fillna(np.nan).replace([np.nan], ["None"])
        self.df = self.clean_ssid(self.df)
        self.df = self.clean_frame_time(self.df)
        self.df = self.clean_subtype(self.df)
        self.df = self.clean_type(self.df)
        self.df = self.clean_channel(self.df)
        self.df = self.clean_counts(self.df)
        self.df = self.clean_signal(self.df)
        self.df = self.clean_noise(self.df)
        self.df.convert_dtypes()
        self.df = self.clean_retrys(self.df)
        self.df.dropna(subset=['wlan.fcs.status'], inplace=True)
        self.df.rename(columns={'frame.number': 'frame_number',
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
                                'wlan.fc.retry': 'retries'}, inplace=True)
        # self.df = self.df.fillna("None")
        return self.df

    @staticmethod
    def clean_hex(val):
        try:
            return int(val, 16)
        except TypeError:
            pass

    @staticmethod
    def remove_dt_str(item):
        return item.split('Eastern Daylight Time')[0]

    def clean_frame_time(self, df):
        df['frame.time'] = df.apply(
            lambda row: self.remove_dt_str(row['frame.time']), axis=1
        )
        df['frame.time'] = pd.to_datetime(df['frame.time'])
        df['frame.time'] = df['frame.time'].fillna(0)
        return df

    @staticmethod
    def clean_ssid(df):
        df["wlan.ssid"] = df["wlan.ssid"].fillna("Hidden SSID")
        return df

    @staticmethod
    def clean_fcs_status(df):
        df = df.query('`wlan.fcs.status` => 0 and `wlan.fcs.status` <= 1')
        df["wlan.fcs.status"] = df["wlan.fcs.status"].astype("Int32")
        return df

    @staticmethod
    def clean_retrys(df):
        df["wlan.fc.retry"] = df["wlan.fc.retry"].astype("Int64")
        return df

    def clean_ds(self, df):
        df["wlan.fc.ds"] = df.apply(
            lambda row: self.clean_hex(row["wlan.fc.ds"]), axis=1)
        return df

    @staticmethod
    def convert_to_dbm(mw_array):
        dbm_array = 10 * np.log10(mw_array)
        return dbm_array

    @staticmethod
    def convert_to_mw(self, dbm_array):
        mw_array = 10 ** (dbm_array / 10)
        return mw_array

    @staticmethod
    def clean_subtype(df):
        df['wlan.fc.type_subtype'] = pd.to_numeric(df['wlan.fc.type_subtype'],
                                                   errors='coerce').fillna(352).astype(int)
        return df

    @staticmethod
    def clean_type(df):
        df['wlan.fc.type'] = pd.to_numeric(df['wlan.fc.type'],
                                           errors='coerce').fillna(352).astype(int)
        return df

    @staticmethod
    def clean_channel(df):
        try:
            df = df.astype({'wlan_radio.channel': 'int32'})
            df = df.query('`wlan_radio.channel` >= 1')
        except ValueError:
            for index, row in df.iterrows():
                if isinstance(df.at[1,'wlan_radio.channel'], str):
                    df.at[index, 'wlan_radio.channel'] = df[index, 'wlan_radio.channel'].strip(",").strip('"')
        return df

    @staticmethod
    def clean_counts(df):
        df['wlan.qbss.scount'] = df['wlan.qbss.scount'].fillna(0)
        df = df.astype({'wlan.qbss.scount': 'int'})
        return df

    @staticmethod
    def clean_signal(df):
        df = df.astype({'radiotap.dbm_antsignal': 'int'})
        df['radiotap.dbm_antsignal'] = df['radiotap.dbm_antsignal'].fillna(0)
        df = df[df['radiotap.dbm_antsignal'] < -20]
        return df

    @staticmethod
    def clean_noise(df):
        df = df.astype({'radiotap.dbm_antnoise': 'int'})
        df['radiotap.dbm_antnoise'] = df['radiotap.dbm_antnoise'].fillna(0)
        return df

    @staticmethod
    def clean_frame_len(df):
        df['frame.len'] = pd.to_numeric(df['frame.len'], errors='coerce')
        df.dropna(subset=['frame.len'])
        return df

    def dbm_to_mw(self, df):
        df['rssi(mW)'] = df['radiotap.dbm_antsignal'].apply(self.convert_to_mw)
        df['noise(mW)'] = df['radiotap.dbm_antnoise'].apply(self.convert_to_mw)
        return df


class Load:

    # TODO: test multiple files with postgres
    # TODO: check current data in postgres to see if
    #  invalid characters are being entered into db.

    def __init__(self, save_dir):
        self.save_dir = save_dir

    @staticmethod
    def create_query(dataframe, table):
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
        filename = dataframe.filename[1]
        csv_file = f"{os.path.join(self.save_dir, filename)}.csv"
        dataframe.to_csv(csv_file, index=False)

    def create_table(self, db_obj):
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
            rssi SMALLINT,
            noise SMALLINT,
            fc_type SMALLINT,
            fc_subtype SMALLINT,
            data_rate FLOAT,
            client_counts SMALLINT,
            retries SMALLINT
        )
        """
        db_obj.write(commands)

    def update_pg(self, database_name):
        db = pg_admin(database_name, ("postgres", "aBcd@1234"))
        for file in glob(f"{self.save_dir}/*.csv"):
            df = pd.read_csv(file)
            good_sql = self.create_query(df.query("fcs == 1"), "good_pkts")
            existing_tables = db.get_tables()
            if "good_pkts" not in existing_tables:
                self.create_table(db)
            db.write(good_sql)




