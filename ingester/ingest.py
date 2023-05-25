import os
import re
import socket
from datetime import datetime
from pickle import loads
from typing import List

from asyncpg import connect
from confluent_kafka import Consumer
from dotenv import load_dotenv


class Ingest():
    def __init__(self, test=False) -> None:
        self._load_env()
        if not test:
            self._consumer = Consumer({
                'bootstrap.servers': self._KAFKA,
                'group.id': 'INGESTER',
                'auto.offset.reset': 'earliest'
            })
        self._areas = ["postcode", "street", "town", "district", "county", "outcode", "area", "sector"]
        # Regex to split postcode into inward, outward & area
        self._postcode_re = re.compile("^(?:(?P<a1>[Gg][Ii][Rr])(?P<d1>) (?P<s1>0)(?P<u1>[Aa]{2}))|(?:(?:(?:(?P<a2>[A-Za-z])(?P<d2>[0-9]{1,2}))|(?:(?:(?P<a3>[A-Za-z][A-Ha-hJ-Yj-y])(?P<d3>[0-9]{1,2}))|(?:(?:(?P<a4>[A-Za-z])(?P<d4>[0-9][A-Za-z]))|(?:(?P<a5>[A-Za-z][A-Ha-hJ-Yj-y])(?P<d5>[0-9]?[A-Za-z]))))) (?P<s2>[0-9])(?P<u2>[A-Za-z]{2}))$", flags=re.IGNORECASE)

    def _load_env(self):
        # Loads the enviroment variables
        load_dotenv()
        self._DB = self.manage_sensitive("DBNAME", "house_data")
        self._USERNAME = self.manage_sensitive("POSTGRES_USER")
        self._PASSWORD = self.manage_sensitive("POSTGRES_PASSWORD")
        self._HOST = self.manage_sensitive("POSTGRES_HOST")
        self._KAFKA = self.manage_sensitive("KAFKA")

    def manage_sensitive(self, name, default= None):
        v1 = os.environ.get(name)

        secret_fpath = f'/run/secrets/{name}'
        existence = os.path.exists(secret_fpath)

        if v1 is not None:
            return v1

        if existence:
            v2 = open(secret_fpath).read().rstrip('\n')
            return v2

        if all([v1 is None, not existence]) and default is None:
            raise KeyError(f'{name} environment variable is not defined')
        elif default is not None:
            return default

    async def _connect_db(self):
        self._conn = await connect(f"postgresql://{self._USERNAME}:{self._PASSWORD}@{self._HOST}/{self._DB}")

    def extract_parts(self, postcode: str) -> List[str]:
        try:
            if (parts := self._postcode_re.findall(postcode)[0]) != None:  # splits postcode & checks if it is valid
                parts = list(filter(lambda x : x != '', parts))  # Removes empty parts from postcode
                outcode = parts[0] + parts[1]
                area = parts[0]
                sector = parts[0] + parts[1] + " " + parts[2]
                return [outcode, area, sector]  # Returns the parts of the postcode
            else:
                return ["","",""]
        except IndexError:
            return ["","",""]

    async def _set_status(self, status: str) -> None:
        consumer_id = socket.gethostname()
        try:
            await self._conn.execute("INSERT INTO settings (name, data) VALUES ($1, $2);", consumer_id, status)
        except:
            await self._conn.execute("DELETE FROM settings WHERE name = $1", consumer_id)
            await self._conn.execute("INSERT INTO settings (name, data) VALUES ($1, $2);", consumer_id, status)

    async def remove_status(self):
        consumer_id = socket.gethostname()
        print("DELETING")
        await self._conn.execute("DELETE FROM settings WHERE name = $1", consumer_id)

    async def _insert_areas(self, sale: List, postcode_parts: List[str]):
        areas = [sale[3], sale[9], sale[11], sale[12], sale[13],
                 postcode_parts[0], postcode_parts[1], postcode_parts[2]] # Extracts areas values from sale
        values = []
        for idx, area_type in enumerate(self._areas):
            area_data = (area_type, areas[idx])
            values.append(area_data)
        await self._conn.executemany("""INSERT INTO areas (area_type, area) 
                                VALUES ($1,$2) ON CONFLICT (area_type, area) DO NOTHING;""",values)

    async def main_loop(self):
        print("Waiting for messages")
        await self._connect_db()
        self._consumer.subscribe(["new_sales"])
        while True:
            await self._set_status("WAITING")
            msg = self._consumer.poll(1.0)  # Fetches the latest message from kafka
            await self._set_status("PROCESSING")
            if msg is None:  #Checks the message isnt empty
                continue
            if msg.error():  # Checks there are no errors
                print("Consumer error: {}".format(msg.error()))
                continue
            sale: List = loads(msg.value())  # Converts the bytes into a python list
            await self._process_sale(sale)
    
    async def _process_sale(self, sale):
        async with self._conn.transaction():
            if sale[-1] in ["C", "D"]:
                await self._conn.execute("DELETE FROM sales WHERE tui=$1", sale[0]) # Delete sale
            if sale[-1] in ["A", "C"]:
                postcode_parts = self.extract_parts(sale[3])  # Fetches the postcode parts
                await self._insert_areas(sale, postcode_parts)

                houseID = str(sale[7]) + str(sale[8]) + str(sale[3])
                await self._conn.execute("INSERT INTO postcodes \
                                (postcode, street, town, district, county, outcode, area, sector) \
                                VALUES ($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT (postcode) DO NOTHING;", 
                                sale[3], sale[9], sale[11], sale[12], sale[13], postcode_parts[0], 
                                postcode_parts[1], postcode_parts[2])  # Insert into postcode table

                await self._conn.execute("INSERT INTO houses (houseID, PAON, SAON, type, postcode) \
                                VALUES ($1,$2,$3,$4,$5) ON CONFLICT (houseID) DO NOTHING;", 
                                houseID, sale[7], sale[8], sale[4], sale[3])  # Insert into house table

                new = True if sale[5] == "Y" else False # Convets to boolean type
                freehold = True if sale[6] == "F" else False  # Converts to boolean type
                date = datetime.strptime(sale[2], "%Y-%m-%d %H:%M")  # Converts string to datetime object
                await self._conn.execute("INSERT INTO sales (tui, price, date, new, freehold, ppd_cat, houseID) \
                            VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT (tui) DO NOTHING;", 
                            sale[0], int(sale[1]), date, new, freehold, sale[14], houseID) # Insert into sales table


if __name__ == "__main__":
    import asyncio
    x = Ingest()
    asyncio.get_event_loop().run_until_complete(x.main_loop())
