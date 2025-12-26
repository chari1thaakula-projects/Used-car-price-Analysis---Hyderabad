import pandas as pd
import matplotlib.pyplot as plt

# Load your actual dataset (exported from PBIX or scraping pipeline)
df = pd.read_csv("cardekho_used_cars_cleaned_with_brand.csv")

# ========== 1. Clustered Column Chart (Avg Price by Fuel) ==========
avg_price_fuel = df.groupby("Fuel")["Price"].mean()
plt.figure(figsize=(8,6))
avg_price_fuel.plot(kind="bar", color="skyblue")
plt.title("Average Price by Fuel Type")
plt.ylabel("Average Price (INR)")
plt.xlabel("Fuel Type")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# ========== 2. Scatter Chart (Price vs KM Driven) ==========
plt.figure(figsize=(8,6))
plt.scatter(df["KM Driven"], df["Price"], c="blue", s=40, alpha=0.7)
plt.title("Price vs KM Driven")
plt.xlabel("KM Driven")
plt.ylabel("Price (INR)")
plt.tight_layout()
plt.show()

# ========== 3. Stacked Column Chart (Avg Price vs Brand) ==========
avg_price_brand = df.groupby("Brand")["Price"].mean().sort_values(ascending=False)
plt.figure(figsize=(12,6))
avg_price_brand.plot(kind="bar", stacked=True, color="lightgreen")
plt.title("Average Price by Brand")
plt.ylabel("Average Price (INR)")
plt.xlabel("Brand")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# ========== 4. Donut Chart (Count of Title by Owner) ==========
owner_counts = df["Owner"].value_counts()
plt.figure(figsize=(6,6))
plt.pie(owner_counts, labels=owner_counts.index, autopct='%1.0f%%',
        startangle=90, wedgeprops=dict(width=0.4))
plt.title("Distribution of Cars by Owner Type")
plt.tight_layout()
plt.show()

# ========== 5. Clustered Column Chart (Avg Price vs Transmission) ==========
avg_price_transmission = df.groupby("Transmission")["Price"].mean()
plt.figure(figsize=(8,6))
avg_price_transmission.plot(kind="bar", color="orange")
plt.title("Average Price by Transmission")
plt.ylabel("Average Price (INR)")
plt.xlabel("Transmission Type")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# ========== 6. Clustered Bar Chart (Count of Title by Transmission) ==========
transmission_counts = df["Transmission"].value_counts()
plt.figure(figsize=(8,6))
transmission_counts.plot(kind="barh", color="teal")
plt.title("Count of Cars by Transmission Type")
plt.xlabel("Count of Cars")
plt.ylabel("Transmission Type")
plt.tight_layout()
plt.show()
