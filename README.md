# CARTO-Waze

*Connect Waze data sources and CARTO*

Supported backends:

* Waze's CCP feeds
* [Waze CCP Processor](https://github.com/LouisvilleMetro/WazeCCPProcessor) instances

## Installation

```
$ pip install cartowaze
```

## Usage

More formal documentation still needs to be produced. In the meantime, please take a look at the code to find out about different ways to tailor the behavior of the connector.

### Waze CCP feeds

```python
from carto_waze.backends.waze import Alert, Jam
from carto.auth import APIKeyAuthClient


auth_client = APIKeyAuthClient("https://mycartouser.carto.com/", "mycartoapikey")

waze_url = "https://mywazeccpurl"

alerts = Alert(auth_client, url=waze_url)
with open("alerts.csv", "w") as csvfile:
    alerts.get_values(csvfile)
with open("alerts.csv", "rb") as csvfile:
    alerts.create_table(table_name="myalertstable", cartodbfy=True)
    alerts.append_data(csvfile, table_name="myalertstable")

jams = Jam(auth_client, url=waze_url)
with open('jams.csv', 'w') as jams_csv:
    jams.get_values(jams_csv)
with open("jams.csv", "rb") as csvfile:
    jams.create_table(table_name="myjamstable", cartodbfy=True)
    jams.append_data(csvfile, table_name="myjamstable")
```

### Waze CCP Processor

```python
from datetime import datetime
from carto_waze.backends.waze_ccp_processor import AlertProcessor, JamProcessor
from carto.auth import APIKeyAuthClient


auth_client = APIKeyAuthClient("https://mycartouser.carto.com/", "mycartoapikey")

alerts = AlertProcessor(auth_client, password="mywazeccpprocessorpassword", host="myccpprocessorhost.rds.amazonaws.com")
with open('alerts.csv', 'w') as csvfile:
    alerts.get_values(csvfile, pub_utc_date__gt=datetime(2018, 9, 20), pub_utc_date__lt=datetime(2018, 9, 21))
with open("alerts.csv", "rb") as csvfile:
    alerts.create_table(table_name="myalertstable")
    alerts.append_data(csvfile, table_name="myalertstable")

jams = JamProcessor(auth_client, password="mywazeccpprocessorpassword", host="myccpprocessorhost.rds.amazonaws.com")
with open('jams.csv', 'w') as csvfile:
    jams.get_values(csvfile, level=5)
with open("jams.csv", "rb") as csvfile:
    jams.create_table(table_name="myjamstable")
    jams.append_data(csvfile, table_name="myjamstable")
```
