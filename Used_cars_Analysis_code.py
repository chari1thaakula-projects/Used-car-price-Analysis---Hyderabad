import time
import re
import pandas as pd
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor

BASE_URL = "https://www.cardekho.com"

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/115.0 Safari/537.36"
}

def format_price(price_text):
    """Convert Lakh / Crore into numeric ₹ format."""
    if not price_text:
        return ""
    price_text = price_text.replace("₹", "").replace("\n", " ").replace("Compare", "").strip()

    match_lakh = re.match(r"([\d\.]+)\s*Lakh", price_text, re.IGNORECASE)
    if match_lakh:
        value = float(match_lakh.group(1)) * 100000
        return f"₹{int(value):,}"

    match_crore = re.match(r"([\d\.]+)\s*Crore", price_text, re.IGNORECASE)
    if match_crore:
        value = float(match_crore.group(1)) * 10000000
        return f"₹{int(value):,}"

    try:
        value = float(price_text.replace(",", ""))
        return f"₹{int(value):,}"
    except:
        return price_text
all_cars = []
# Phase 1: scrape listing pages
for page in range(1, 4):
    list_url = f"https://www.cardekho.com/used-cars+in+hyderabad-page{page}"
    print(f"\n🔎 Scraping listing page {page}")
    res = requests.get(list_url, headers=headers, timeout=10)
    soup = BeautifulSoup(res.text, "html.parser")
    cars = soup.select(".NewUcExCard")
    print(f"Found {len(cars)} cars on page {page}")
    for car in cars:
        try:
            title = car.find("img")["alt"].strip()
            price = car.select_one(".Price").get_text(strip=True)
            price = format_price(price)
            full_text = car.get_text(" ", strip=True).lower()
            km_match = re.search(r'[\d,]+ ?km', full_text)
            km_driven = km_match.group().replace("km", "").strip() if km_match else ""
            fuel = "Diesel" if "diesel" in full_text else \
                   "Petrol" if "petrol" in full_text else \
                   "CNG" if "cng" in full_text else \
                   "Electric" if "electric" in full_text else ""
            transmission = "Manual" if "manual" in full_text else \
                           "Automatic" if "automatic" in full_text else ""
            link_elem = car.find("a", href=True)
            link = BASE_URL + link_elem["href"] if link_elem else ""
            all_cars.append({
                "Title": title,"Price": price,"KM Driven": km_driven,"Fuel": fuel,"Transmission": transmission,"URL": link,"Owner": "","EMI": ""
            })
        except Exception as e:
            print(f"Skipped a car: {e}")

# Phase 2: fetch Owner & EMI in parallel
def scrape_owner_emi(url):
    """Fetch Owner & EMI from the detail page."""
    try:
        res = requests.get(url, headers=headers, timeout=10)
        soup = BeautifulSoup(res.text, "html.parser")
        # Owner - look for multiple possibilities in full text
        owner = ""
        owner_patterns = ["First Owner", "Second Owner", "Third Owner", "Fourth Owner", "Unregistered"]
        # Search in all text nodes
        for pat in owner_patterns:
            found = soup.find(string=re.compile(pat, re.IGNORECASE))
            if found:
                owner = pat
                break
        # EMI
        emi_elem = soup.select_one("div.monthly-emi-info div.emi")
        emi = emi_elem.get_text(strip=True) if emi_elem else ""
        return owner, emi
    except Exception as e:
        print(f"Error fetching {url}: {e}")
        return "", ""
print("\nFetching Owner & EMI details...")
with ThreadPoolExecutor(max_workers=10) as executor:
    results = list(executor.map(lambda car: scrape_owner_emi(car["URL"]), all_cars))

# Merge results back
for car, (owner, emi) in zip(all_cars, results):
    car["Owner"] = owner
    car["EMI"] = emi

# Save raw CSV
df = pd.DataFrame(all_cars)
df.drop(columns=["URL"], inplace=True)
df.to_csv("cardekho_used_cars_fixed_final.csv", index=False, encoding="utf-8-sig")

# ----------------------------
# Data Cleaning
# ----------------------------

# clean KM Driven
df["KM Driven"] = df["KM Driven"].astype(str).str.replace(",","").str.strip()
df["KM Driven"] = pd.to_numeric(df["KM Driven"], errors="coerce")

# overwrite Price column with numeric values
def extract_numeric_price(p):
    if not p: return None
    return pd.to_numeric(re.sub(r"[^\d]","",p), errors="coerce")

df["Price"] = df["Price"].apply(extract_numeric_price)

# convert EMI to numeric value
df["EMI"] = df["EMI"].astype(str).str.replace("₹","").str.replace(",","").str.extract(r"(\d+)")[0]
df["EMI"] = pd.to_numeric(df["EMI"], errors="coerce")

# clean categorical text
for col in ["Fuel","Transmission","Owner"]:
    df[col] = df[col].fillna("Unknown").str.strip().str.title()

# ----------------------------
# Handle Empty Columns
# ----------------------------
for col in df.columns:
    if pd.api.types.is_numeric_dtype(df[col]):
        df[col] = df[col].fillna(0)  # replace NaN with 0
    else:
        df[col] = df[col].fillna("nil")  # replace NaN with 'nil'
        df[col] = df[col].replace("", "nil")  # replace empty string with 'nil'

# ----------------------------
# Data Manipulation Examples
# ----------------------------

# cars under 10 lakh
cars_under_10_lakh = df[df["Price"] < 1000000]

# average price by fuel type
avg_price_by_fuel = df.groupby("Fuel")["Price"].mean()

# top 5 most driven cars
top5_driven = df.nlargest(5,"KM Driven")[["Title","KM Driven"]]

# owner type distribution
owner_distribution = df["Owner"].value_counts()

# Save cleaned data
df.to_csv("cardekho_used_cars_cleaned.csv", index=False, encoding="utf-8-sig")

def extract_brand(title):
    if not isinstance(title, str):
        return "Unknown"
    title_lower = title.lower()
    brands = ["hyundai", "kia", "maruti", "suzuki", "honda", "toyota", "tata", "mahindra", "ford", "renault", "skoda",
              "volkswagen", "nissan", "mg", "jeep", "mercedes", "bmw", "audi"]
    for brand in brands:
        if brand in title_lower:
            return brand.capitalize()
    return "Other"

df["Brand"] = df["Title"].apply(extract_brand)

# Save Brand data
df.to_csv("cardekho_used_cars_cleaned_with_brand.csv", index=False, encoding="utf-8-sig")


print(f"\n Data cleaned & saved — {len(df)} cars scraped in total.")
print("\n Sample Data Preview:")
print(df.head())
