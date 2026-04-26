import pandas as pd
import os

# -----------------------
# 🔹 STEP 1: CREATE DATA LAKE STRUCTURE
# -----------------------
os.makedirs("data_lake/raw", exist_ok=True)
os.makedirs("data_lake/processed", exist_ok=True)
os.makedirs("data_lake/curated", exist_ok=True)

# -----------------------
# 🔹 STEP 2: LOAD DATA (RAW)
# -----------------------
df = pd.read_csv("data.csv")

# Save raw copy
df.to_csv("data_lake/raw/raw_data.csv", index=False)

print("🔴 Raw Data:\n", df.head())

# -----------------------
# 🔹 STEP 3: DATA WRANGLING
# -----------------------

# Handle missing values (new pandas version)
df = df.ffill()

# Remove duplicates
df.drop_duplicates(inplace=True)

# Filter useful data
if "Salary" in df.columns:
    df = df[df["Salary"] > 50000]

# Save processed data
df.to_csv("data_lake/processed/processed_data.csv", index=False)

# -----------------------
# 🔹 STEP 4: DATA CURATION
# -----------------------

# Add metadata
df["Processed_Date"] = pd.Timestamp.now()

# Add partitioning (real-world concept)
df["Year"] = df["Processed_Date"].dt.year
df["Month"] = df["Processed_Date"].dt.month

# Save curated data
curated_path = "data_lake/curated/cleaned_data.csv"
df.to_csv(curated_path, index=False)

print("\n🟢 Curated Data Saved At:", curated_path)
print(df.head())