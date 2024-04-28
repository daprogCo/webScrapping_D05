import requests
from bs4 import BeautifulSoup
from collections import namedtuple
from random import randint
import time
import ssl
from datetime import datetime
import sqlite3
import math
import certifi
from pymongo import MongoClient

### Constants
DATABASE = 'SAQ.db'
URL_WINES = "wine_url_list.txt"
URI_ATLAS_MONGODB = "mongodb+srv://<username>:<password>.<server_name>.mongodb.net/"
TABLE_LISTING = 'list_SAQ'
TABLE_WINES = 'wine_SAQ'
URL_LISTING = "https://www.saq.com/fr/produits/vin"
ITEMS_PER_PAGE = 24


### Catch errors wrapper function
def catch_errors(func: callable):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            print(f"Error from <{func.__name__}> function: {e}")
            return None
    return wrapper

# SSL context function
def create_ssl_context():
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE

# Helpers sqlite3 functions
def copy_to_sqlite(url, response, table):
    conn = sqlite3.connect(DATABASE)
    cur = conn.cursor()
    cur.execute(f"CREATE TABLE if not exists {table} ( \
                id integer primary key autoincrement, \
                url TEXT, response TEXT, timestamp TEXT \
                )")
    cur.execute(f"INSERT INTO {table} (url, response, timestamp) VALUES (?, ?, ?)", (url, response, datetime.now()))
    conn.commit()
    conn.close()

def fetch_from_sqlite(value, table, id):
    conn = sqlite3.connect(DATABASE)
    cur = conn.cursor()
    cur.execute(f"SELECT {value} FROM {table} WHERE id = {id}")
    row = cur.fetchone()[0]
    conn.close()
    return row

def count_table_rows(table):
    conn = sqlite3.connect(DATABASE)
    cur = conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {table}")
    count = cur.fetchone()[0]
    conn.close()
    return count

# Helpers Listing functions
def save_to_file(filename, data):
    with open(filename, "a") as file:
        for d in data:
            file.write(f"{d}\n")

def all_wines_urls():
    with open(URL_WINES, "r") as file:
        urls = file.readlines()
    return [url.strip() for url in urls]

# Parsing trough html functions
def get_number_of_pages():
    response = requests.get(URL_LISTING).content
    soup = BeautifulSoup(response, "html.parser")
    toolbar = soup.findAll("span", attrs={"class": "toolbar-number"})
    return math.ceil(int(toolbar[-1].get_text()) / ITEMS_PER_PAGE)

def get_urls_product(html):
    li = html.find_all("li",attrs={"class": "item product product-item"})
    urls = []
    for l in li:
        urls.append(l.find("a")["href"])
    return urls

# Helpers wine functions
def get_wine_attr(html, attr):
    try:
        return html.find("strong",attrs={"data-th": attr}).get_text().strip()
    except:
        return "N/A"

nom = lambda html: \
    html.find("h1",attrs={"class": "page-title"}).get_text().strip()
prix = lambda html: \
    html.find("span",attrs={"class": "price"}).get_text().strip()

couleur = lambda html: get_wine_attr(html, "Couleur")
pays = lambda html: get_wine_attr(html, "Pays")
region = lambda html: get_wine_attr(html, "Région")
appelation = lambda html: get_wine_attr(html, "Appellation d'origine")
designation = lambda html: get_wine_attr(html, "Désignation réglementée")
cepage = lambda html: get_wine_attr(html, "Cépage")
alcool = lambda html: get_wine_attr(html, "Degré d'alcool")
sucre = lambda html: get_wine_attr(html, "Taux de sucre")
frmt = lambda html: get_wine_attr(html, "Format")
producteur = lambda html: get_wine_attr(html, "Producteur")
particularite = lambda html: get_wine_attr(html, "Particularité")
code = lambda html: get_wine_attr(html, "Code SAQ")

# Namedtuple wine function
def get_wine_infos(html):
    Wine = namedtuple('Wine', ["Nom", "Couleur", "Prix", "Pays", "Région", "Appelation", "Désignation", "Cépages", "Alcool", "Sucre", "Format", "Producteur","Particularités", "Code"])
    return Wine(nom(html), couleur(html), prix(html), pays(html), region(html),
                appelation(html), designation(html), cepage(html), alcool(html), sucre(html), frmt(html), producteur(html),particularite(html),code(html))

# Listing wine function
def list_all_wines_urls(start=1, count=count_table_rows(TABLE_LISTING)):
    for i in range(start, count + 1):
        response = fetch_from_sqlite("response", TABLE_LISTING, i)
        soup = BeautifulSoup(response, "html.parser")
        urls = get_urls_product(soup)
        save_to_file(URL_WINES, urls)

@catch_errors
def list_all_wines(start=1,count=count_table_rows(TABLE_WINES)):
    for i in range(start, count + 1):
        response = fetch_from_sqlite("response", TABLE_WINES, i)
        soup = BeautifulSoup(response, "html.parser")
        wine = get_wine_infos(soup)
        print("=" * 50)
        for attr in wine._fields:
            print(f'{attr}: {getattr(wine, attr)}')
        print(f"Copying to mongodb from sqlite line{i}...")
        copy_to_mongodb(wine)

# Webcrawler functions
def webcrawler_listing(start=1, num_pages=get_number_of_pages()):
    for i in range(start, num_pages + 1):
        url = f"{URL_LISTING}?p={i}"
        print(f"Fetching {url}...")
        response = requests.get(url).content
        print(f"Copying to sqlite...")
        copy_to_sqlite(url, response, TABLE_LISTING)
        print(50 * "=")
        time.sleep(randint(1, 3))

def webcrawler_wines(start=0, end=len(all_wines_urls())):
    urls = all_wines_urls()
    for i in range(start, end):
        print(f"Fetching #{i}: {urls[i]}...")
        response = requests.get(urls[i]).content
        print(f"Copying to sqlite...")
        copy_to_sqlite(urls[i], response, TABLE_WINES)
        print(50 * "=")
        time.sleep(randint(1, 3))

# MongoDB functions
def copy_to_mongodb(wine):
    client = MongoClient(URI_ATLAS_MONGODB, tlsCAFile=certifi.where())
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
    db = client['SAQ']
    collection = db['wines']
    collection.insert_one(wine._asdict())
    client.close()


# Main function to perform all the tasks
def main():
    try: 
        create_ssl_context()
    except:
        return
    webcrawler_listing()
    list_all_wines_urls()
    webcrawler_wines()
    list_all_wines()

if __name__ == "__main__":
    main()
