import requests
from bs4 import BeautifulSoup
from collections import namedtuple
from random import randint
import time
import ssl

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE


def get_html_from_url(url):
    response = requests.get(url).content
    soup =  BeautifulSoup(response, "html.parser")
    return soup

def get_urls_product(html):
    li = html.find_all("li",attrs={"class": "item product product-item"})
    urls = []
    for l in li:
        urls.append(l.find("a")["href"])
    return urls

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

def get_wine_infos(url):
    html = get_html_from_url(url)
    Wine = namedtuple('Wine', ["Nom", "Couleur", "Prix", "Pays", "Région", "Appelation", "Désignation", "Cépages", "Alcool", "Sucre", "Format", "Producteur","Particularités", "Code"])
    return Wine(nom(html), couleur(html), prix(html), pays(html), region(html),
                appelation(html), designation(html), cepage(html), alcool(html), sucre(html), frmt(html), producteur(html),particularite(html),code(html))

urls = get_urls_product(get_html_from_url("https://www.saq.com/fr/produits/vin?p=3"))

for url in urls:
    wine = get_wine_infos(url)
    print("=" * 50)
    for attr in wine._fields:
        print(f'{attr}: {getattr(wine, attr)}')