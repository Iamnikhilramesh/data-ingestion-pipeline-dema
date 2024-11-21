# E-commerce Data Ingestion Pipeline

This repository contains a data ingestion pipeline designed to process and combine e-commerce store inventory and orders data for multiple customers. The pipeline creates a customer-specific SQLite database, where the data is stored and updated. After processing, the original CSV files are moved to a "processed" folder with a timestamp for versioning.

## Features:
- Supports multiple customers, each with a separate SQLite database.
- Handles inventory and order CSV files.
- Creates a combined `orders_inventory` table by performing a LEFT JOIN on inventory and orders data.
- Moves processed files to a `processed_files/` folder with a timestamp.

## Requirements:
- Python 3.x
- `pandas` library
- `sqlite3` library (comes built-in with Python)

## Installation:

1. Clone the repository:
    ```bash
    git clone https://github.com/yourusername/data-ingestion-pipeline.git
    cd data-ingestion-pipeline
    ```

2. Install dependencies:
    ```bash
    pip install -r requirements.txt
    ```

## Usage:

### 1. Prepare Customer Folder:

Each customer should have a folder containing `inventory.csv` and `orders.csv` files. The folder name will be used to create a customer-specific database.

Example folder structure:
customer1_dema/ inventory.csv orders.csv


### 2. Run the Pipeline:

Execute the `pipeline.py` script by specifying the customer folder:

```bash
python pipeline.py
You will be prompted to enter the customer folder name (e.g., customer1_dema). The pipeline will process the data, create or update the SQLite database, and move the processed files to the processed_files/ folder.

Example:
Enter the customer folder name (e.g., 'customer1_dema'): customer1_dema
Data ingestion and combination completed successfully for customer: customer1_dema.
                               order_id                 product_id currency  ordered_quantity  shipping_cost  order_amount order_channel order_channel_group order_campaign          ordered_date               product_name product_category product_sub_category
0  efb921c1-6733-3811-b4c2-aa0d80800638  prod1520#prod100011001100      SEK                 1            0.0      7095.930        direct                 sem           None  2023-03-12T17:12:52Z  x Off White hooded jacket         Clothing               Jacket
1  efdd9ffa-0c2d-36fa-a6bb-1d57abe074c8  prod1563#prod106011001110      SEK                 1            0.0     16002.240        direct              direct           None  2023-03-12T16:54:24Z              Suede loafers            Shoes              Loafers
2  ef10181e-51f3-3704-b736-2eac26ae8be9  prod2306#prod102061000115      SEK                 1            0.0     11017.365        google                 sem        kr_pmax     2023-03-12T06:16Z              Tailored coat         Clothing                 Coat
3  ef83f304-102f-3bab-8005-4e2087ac12ad  prod1565#prod106031003080      SEK                 1            0.0     15627.186        others            referral           None  2023-03-12T23:28:50Z     Embellished clutch bag      Accessories                 Bags
4  ef2fc87a-ec83-3533-a1d3-d5d80c692bc2  prod1524#prod100051005115      SEK                 1            0.0      8963.280        google                 sem        kr_pmax  2023-03-12T23:13:38Z               Satin blouse         Clothing               Blouse
Files have been moved to 'processed_files'.


File Structure:
pipeline.py: The main script that processes data.
requirements.txt: A file containing the required Python libraries.
README.md: Documentation for using the pipeline.
