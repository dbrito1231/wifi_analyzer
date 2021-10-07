# WiFETL
This project serves to process WiFi packet capture files into a database for data analysis. Packet captures
must be captured using promiscuous mode on network adapters to collect necessary headers for 
this program to work.

### Prerequisites
* numpy v1.21.2
* psycopg2
* pandas v1.3.2
* py4j v0.10.9
* python-dateutil v2.8.2
* pytz v2021.1
* six v1.16.0
* PostgreSQL installed
* Windows PC (Linux coming soon!)
* Wireshark installed (3.4+)

### Headers Used
| Field | Entry | Description |
| --- | --- | --- |
| frame.number | frame_number | Sequential number of captured packet |
| frame.time | time | Time of packet captured |
| wlan.fcs.status | fcs | FCS status |
| frame.time_relative | relative_time | Relative time since first packet received |
| frame.time_delta | timedelta | Calculated time from previous packet |
| wlan.ta | transmit_address | Transmit address |
| wlan.ra | receiving_address | Receiving address |
| wlan.ssid | ssid | SSID |
| wlan_radio.channel | channel | Channel |
| radiotap.dbm_antsignal | rssi | Signal strength |
| radiotap.dbm_antnoise | noise | Noise |
| wlan.fc.type | fc_type | Type of packet
| wlan.fc.type_subtype | fc_subtype | Packet subtype |
| wlan_radio.data_rate | data_rate | Data rate |
| wlan.qbss.scount | client_counts | Client counts |
| wlan.fc.retry | retries | How many times it took to retry sending packet |

### Files
* pcap_etl.py: Custom ETL built to transform pcap files into a database
    * Classes
      * Extract - used to transform pcap data into csv files.
      * Transform - Loads csv files for data munging.
      * Load - Writes data into PostgreSQL formats.
* pg_connect.py: PostgresSQL database adapter class
* main.py: used to run script or can be used to test pcap_etl and pg_connect.
* config.json: used to pass in parameters into script.

### Config.json file
```json
{
  "PCAP_DIR": "PCAP directory",
  "CSV_DIR": "Directory to save converted packet capture files",
  "DATABASE": "Database name",
  "DB_USER": "Database username",
  "DB_PASSW": "Database password"
}
```

### Running the script
```
> main.py config.json
```

### Steps
1. Download or clone WiFETL onto a Windows based computer.
2. Create virtualenv and install modules from requirements.txt.
3. Update config.json with required directories and credentials.
4. Run script
5. Once completed, check PostgreSQL to verify packets table is designated database.