# Retail-Lakehouse-Analytics-Platform
# Retail Lakehouse Analytics Platform

This project is a simple data engineering pipeline built using Python, Pandas, and PySpark.

It takes raw retail data, cleans it, and transforms it step by step into useful data that can be used for analysis and reporting.


---

# Project Overview (Simple)

Think of this project like a data cleaning factory:

Raw data comes in messy  
We clean and organize it  
We create final data for business use  

---

## Data Flow

Raw → Bronze → Silver → Gold

---

# Layers

## Raw Layer
- Original CSV files
- Stored in: data/raw

## Bronze Layer
- Data stored in parquet format
- No major changes

## Silver Layer
- Cleaned data
- Fixed data types
- Removed duplicates

## Gold Layer
- Final analytics data
- Fact tables, dimension tables, and marts

---

# Project Structure

data/raw/ → raw CSV files

src/
- ingest.py → load data
- bronze_to_silver.py → clean data
- silver_to_gold.py → create analytics tables
- pandas_eda.py → analysis

---

# Technologies Used

- Python
- Pandas
- PySpark
- PyArrow
- Matplotlib

---

# How to Run

pip install -r requirement.txt

python3 src/ingest.py
python3 src/bronze_to_silver.py
python3 src/silver_to_gold.py

python3 src/pandas_eda.pys

---

# What this project answers

- Monthly orders
- Top products
- Seller performances
- Delayed orders
- Customer value

---

# Author

Nirdesh Joshi
ß
