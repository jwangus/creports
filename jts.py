import pandas as pd
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from _secrets import INFLUX_DB_TOKEN, INFLUX_DB_ORG


def request_grayscale_data():
    url = 'https://otcnode.com/'
    df = pd.read_html(url)[0]
    df.set_index("Asset", inplace=True)

    gbtc_price = df.loc['Grayscale Bitcoin Trust', 'Asset Price'][1:]
    gbtc_premium = df.loc['Grayscale Bitcoin Trust', 'Premium %'][:-1]
    ethe_price = df.loc['Grayscale Ethereum Trust', 'Asset Price'][1:]
    ethe_premium = df.loc['Grayscale Ethereum Trust', 'Premium %'][:-1]

    return [f"asset=gbtc price={gbtc_price}", f"asset=gbtc premium={gbtc_premium}",
        f"asset=ethe price={ethe_price}", f"asset=ethe premium={ethe_premium}"
       ]

class InfluxDb:
    my_token = INFLUX_DB_TOKEN
    my_org = INFLUX_DB_ORG
    bucket = "Jts"
    client_url = "http://jim-macmini:9999"

    def __init__(self):
        self.client = InfluxDBClient(url=self.client_url, token=self.my_token)

    def save(self, data):
        write_api = self.client.write_api(write_options=SYNCHRONOUS)
        #data = "asset=gbtc price=26.24"
        write_api.write(self.bucket, self.my_org, data)

def run():
    tsdb = InfluxDb()
    data = request_grayscale_data ()
    tsdb.save(data)

if __name__ == '__main__':
    run()
