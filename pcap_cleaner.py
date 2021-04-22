import numpy as np
import pandas as pd


class pCleaner:

    def __init__(self, dataframe):
        self.df_rawCount = dataframe.shape[0]
        self.df = self.clean_subtype(dataframe)
        self.df = self.clean_type(self.df)
        self.df = self.clean_channel(self.df)
        self.df = self.clean_counts(self.df)
        self.df = self.clean_signal(self.df)
        self.df = self.clean_noise(self.df)
        self.df = self.clean_frame_len(self.df)
        self.df = self.add_dBm_conversion_to_mW(self.df)

    def return_cleanDf(self):
        return self.df

    def convert_to_dBm(self, mw_array):
        dBm_array = 10 * np.log10(mw_array)
        return dBm_array

    def convert_to_mW(self, dBm_array):
        mWarray = 10 ** (dBm_array / 10)
        return mWarray

    def clean_subtype(self, df):
        numeric_subtype = pd.to_numeric(df['wlan.fc.type_subtype'],
                                        errors='coerce').fillna(352).astype(int)
        df['wlan.fc.type_subtype'] = numeric_subtype
        return df

    def clean_type(self, df):
        df = df[df['wlan.fc.type'].str.len() < 4]
        numeric_subtype = pd.to_numeric(df['wlan.fc.type'],
                                        errors='coerce').fillna(352).astype(int)
        df['wlan.fc.type'] = numeric_subtype
        return df

    def clean_channel(self, df):
        df = df[df['wlan_radio.channel'].str.len() < 4]
        df = df.astype({'wlan_radio.channel': 'int'})
        df = df[df['wlan_radio.channel'] >= 1]
        return df

    def clean_counts(self, df):
        df['wlan.qbss.scount'] = df['wlan.qbss.scount'].fillna(0)
        df = df.astype({'wlan.qbss.scount': 'int'})
        return df

    def clean_signal(self, df):
        df = df[df['radiotap.dbm_antsignal'].str.len() < 4]
        df = df.astype({'radiotap.dbm_antsignal': 'int'})
        df = df[df['radiotap.dbm_antsignal'] < -20]
        return df

    def clean_noise(self, df):
        df = df[df['radiotap.dbm_antnoise'].str.len() < 4]
        df = df.astype({'radiotap.dbm_antnoise': 'int'})
        return df

    def clean_frame_len(self, df):
        df['frame.len'] = pd.to_numeric(df['frame.len'], errors='coerce')
        df.dropna(subset=['frame.len'])
        return df

    def clean_channel_bandwidth_below_40(self, df):
        df = df[df['wlan.ht.info.chanwidth'].str.len() < 2]
        df = df.astype({'wlan.ht.info.chanwidth': 'int'})
        df = df[df['wlan.ht.info.chanwidth'] >= 0]
        print('bandwidth under 40')
        return df

    def clean_channel_bandwidth_above_40(self, df):
        df = df[df['wlan.vht.op.channelwidth'].str.len() > 4]
        df['wlan.vht.op.channelwidth'] = df['wlan.vht.op.channelwidth'].apply(lambda x: int(x, base=16)).fillna(0)
        print('bandwidth over 40')
        return df

    def add_dBm_conversion_to_mW(self, df):
        df['rssi(mW)'] = df['radiotap.dbm_antsignal'].apply(self.convert_to_mW)
        df['noise(mW)'] = df['radiotap.dbm_antnoise'].apply(self.convert_to_mW)
        return df


