

set Path_to_Pcap=%1

set Path_to_csv=%2

"C:\Program Files\Wireshark\tshark.exe" -r %Path_to_Pcap% -T ^
fields ^
-e frame.number ^
-e frame.time ^
-e wlan.fcs.status ^
-e frame.time_relative ^
-e frame.time_delta ^
-e wlan.ta ^
-e wlan.ra ^
-e wlan.ssid ^
-e wlan_radio.channel ^
-e radiotap.dbm_antsignal ^
-e radiotap.dbm_antnoise ^
-e wlan.fc.type ^
-e wlan.fc.type_subtype ^
-e wlan_radio.data_rate ^
-e wlan.qbss.scount ^
-e wlan.fc.retry ^
-E header=y -E separator=, -E quote=d -E occurrence=f > %Path_to_csv%