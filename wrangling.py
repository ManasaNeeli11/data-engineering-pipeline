import pandas as pd
import os
from datetime import datetime

print("🚀 Pipeline Started...")

# -----------------------
# CREATE DATA LAKE
# -----------------------
os.makedirs("data_lake/raw", exist_ok=True)
os.makedirs("data_lake/processed", exist_ok=True)
os.makedirs("data_lake/curated", exist_ok=True)

# -----------------------
# LOAD DATA
# -----------------------
df = pd.read_csv("data.csv")
print("📥 Loaded Data:", df.shape)

# Save raw
df.to_csv("data_lake/raw/raw_data.csv", index=False)

# -----------------------
# DATA WRANGLING
# -----------------------
df = df.ffill()
df.drop_duplicates(inplace=True)

if "Salary" in df.columns:
    df = df[df["Salary"] > 50000]

print("🧹 After Cleaning:", df.shape)

# -----------------------
# DATA CURATION
# -----------------------
current_time = datetime.now()

df["Processed_Date"] = current_time
df["Year"] = current_time.year
df["Month"] = current_time.month

# Save curated
output_path = "data_lake/curated/cleaned_data.csv"
df.to_csv(output_path, index=False)

print("📦 Saved to:", output_path)

# -----------------------
# EXTRA: CREATE RUN LOG
# -----------------------
with open("pipeline_log.txt", "a") as f:
    f.write(f"Pipeline ran at: {current_time}\n")

print("✅ Pipeline Completed Successfully!")
# -----------------------
# CREATE HTML OUTPUT
# -----------------------

html_table = df.to_html(index=False)

html_content = f"""
<!DOCTYPE html>
<html>
<head>
    <title>DataFlow Dashboard</title>
    <link rel="stylesheet" href="style.css">
</head>
<body>

<h1>🚀 DataFlow Pipeline Dashboard</h1>
<p>Auto Generated Data</p>

<div class="card">
    <h2>Processed Data</h2>
    {html_table}
</div>

</body>
</html>
"""
with open("index.html", "w", encoding="utf-8") as f:
    f.write(html_content)

print("🌐 HTML Dashboard Generated!")