# WiFi PCAP ETL
This project serves to process WiFi packet capture files into a database for data analysis. Packet captures
must be captured using promiscuous mode on network adapters to collect necessary headers for 
this program to work.

### Prerequisites
* numpy v1.21.2
* pandas v1.3.2
* py4j v0.10.9
* python-dateutil v2.8.2
* pytz v2021.1
* six v1.16.0

### Headers Used
| Field | Description |
| --- | --- |
| frame.number | Sequential number of captured packet |
| frame.time | Time of packet captured |
| wlan.fcs.status | FCS status |
| frame.time_relative | Relative time since first packet received |
| frame.time_delta | Calculated time from previous packet |
| wlan.ta | Transmit address |
| wlan.ra | Receiving address |
| wlan.ssid | SSID |
| wlan_radio.channel | Channel |
| radiotap.dbm_antsignal | Signal strength |
| radiotap.dbm_antnoise | Noise |
| wlan.fc.type | Type of packet
| wlan.fc.type_subtype | Packet subtype |
| wlan_radio.data_rate | Data rate
| wlan.fc.retry | How many times it took to retry sending packet |

### Files
* pcap_etl.py: Custom ETL built to transform pcap files into a database
    * Classes
      * Extract - used to transform pcap data into csv files.
      * Transform - Loads csv files for data munging.
      * Load - Writes data into PostgreSQL formats.