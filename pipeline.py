import sqlite3 as sql
import pandas as pd
import os
import shutil
from datetime import datetime

def data_ingestion_pipeline(customer_folder, processed_folder="processed_files"):
    """
    A pipeline to ingest, combine e-commerce store inventory and orders data into a customer-specific SQLite database.
    
    Arguments/Parameters:
    customer_folder (str): Folder containing the customer-specific CSV files (inventory.csv and orders.csv).
    processed_folder (str): Folder where processed files will be moved. Default is "processed_files".
    """
    # Check if the customer folder exists
    if not os.path.exists(customer_folder):
        print(f"Error: The folder {customer_folder} does not exist.")
        return
    
    # Define file paths for the inventory and orders CSV files
    inventory_file = os.path.join(customer_folder, 'inventory.csv')
    orders_file = os.path.join(customer_folder, 'orders.csv')

    # Check if the files exist
    if not os.path.exists(inventory_file) or not os.path.exists(orders_file):
        print("Error: One or both of the input files do not exist.")
        return

    # Check if the files are empty
    if os.stat(inventory_file).st_size == 0 or os.stat(orders_file).st_size == 0:
        print("Error: One or both of the input files are empty.")
        return

    # Load data from CSV files
    inventory = pd.read_csv(inventory_file)
    orders = pd.read_csv(orders_file)

    # Create a customer-specific database (based on folder name)
    customer_db = f"{customer_folder}.db"
    conn = sql.connect(customer_db)
    cursor = conn.cursor()

    # Ensure the tables exist for the customer database
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS inventory (
            productId TEXT PRIMARY KEY,
            name TEXT,
            category TEXT,
            subCategory TEXT
        );
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            orderId TEXT PRIMARY KEY,
            productId TEXT,
            currency TEXT,
            quantity INTEGER,
            shippingCost REAL,
            amount REAL,
            channel TEXT,
            channelGroup TEXT,
            campaign TEXT,
            dateTime TEXT,
            FOREIGN KEY (productId) REFERENCES inventory (productId)
        );
    """)

    # Insert inventory data and update if there is already a product details exists.
    for _, row in inventory.iterrows():
        cursor.execute("""
            INSERT INTO inventory (productId, name, category, subCategory)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(productId) DO UPDATE SET
                name = excluded.name,
                category = excluded.category,
                subCategory = excluded.subCategory;
        """, (row['productId'], row['name'], row['category'], row['subCategory']))

    # Insert orders data and update if there is already an order details exists.
    for _, row in orders.iterrows():
        cursor.execute("""
            INSERT INTO orders (orderId, productId, currency, quantity, shippingCost, amount, channel, channelGroup, campaign, dateTime)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(orderId) DO UPDATE SET
                productId = excluded.productId,
                currency = excluded.currency,
                quantity = excluded.quantity,
                shippingCost = excluded.shippingCost,
                amount = excluded.amount,
                channel = excluded.channel,
                channelGroup = excluded.channelGroup,
                campaign = excluded.campaign,
                dateTime = excluded.dateTime;
        """, (row['orderId'], row['productId'], row['currency'], row['quantity'], row['shippingCost'], row['amount'], row['channel'], row['channelGroup'], row['campaign'], row['dateTime']))

    # Drop the table if it exists
    cursor.execute("DROP TABLE IF EXISTS orders_inventory;")

    # Perform the LEFT JOIN in SQLite to combine data
    cursor.execute("""
        CREATE TABLE orders_inventory AS
        SELECT 
            DISTINCT orders.orderId as order_id,
            orders.productId as product_id,
            orders.currency as currency,
            orders.quantity as ordered_quantity,
            orders.shippingCost as shipping_cost,
            orders.amount as order_amount,
            orders.channel as order_channel,
            orders.channelGroup as order_channel_group,
            orders.campaign as order_campaign,
            orders.dateTime as ordered_date,
            inventory.name as product_name,
            inventory.category as product_category,
            inventory.subCategory as product_sub_category
        FROM orders
        LEFT JOIN inventory ON orders.productId = inventory.productId;
    """)
    conn.commit()

    print(f"Data ingestion and combination completed successfully for customer: {customer_folder}.")

    # Optionally, you can fetch the merged data to see the result
    merged_data = pd.read_sql("SELECT * FROM orders_inventory", conn)
    print(merged_data.head())

    # Move the processed files to the processed folder
    if not os.path.exists(processed_folder):
        os.makedirs(processed_folder)

    # Add a timestamp to the file name for uniqueness
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    shutil.move(inventory_file, os.path.join(processed_folder, f"inventory_{timestamp}.csv"))
    shutil.move(orders_file, os.path.join(processed_folder, f"orders_{timestamp}.csv"))

    print(f"Files have been moved to '{processed_folder}'.")

    # Close the database connection
    conn.close()

# Run the pipeline for a specific customer
if __name__ == "__main__":
    customer_folder = input("Enter the customer folder name (e.g., 'customer1_dema'): ")
    data_ingestion_pipeline(customer_folder)
