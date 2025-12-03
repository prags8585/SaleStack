import sqlite3
import pandas as pd
import logging
from ingestionn_db import ingest_db

logging.basicConfig(
    filename="logs/get_Sales_Stack_Summary.log", 
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s", 
    filemode="a"  
)

def ingest_db(df, table_name, engine):
    df.to_sql(table_name, con = engine, if_exists = 'replace', index = False)


def create_sales_stack_summary(connection):
    sales_stack_summary = pd.read_sql_query("""WITH FreightSummary AS (
    SELECT 
        VendorNumber AS SaleNumber, 
        SUM(Freight) AS FreightCost 
    FROM sales_invoices 
    GROUP BY VendorNumber
    ), 

    PurchaseSummary AS (
    SELECT 
        p.VendorNumber AS SaleNumber,
        p.VendorName AS SaleName,
        p.Brand,
        p.Description,
        p.PurchasePrice AS CostPrice,
        pp.Price as ActualPrice,
        pp.Volume AS Quantity,
        SUM(p.Quantity) AS TotalQuantityPurchased,
        SUM(p.Dollars) AS TotalDollarsPurchased
    FROM purchases p
    JOIN purchase_cost pp
        ON p.Brand = pp.Brand
    WHERE p.PurchasePrice > 0
    GROUP BY p.VendorNumber, p.VendorName, p.Brand, p.Description, p.PurchasePrice, pp.Price, pp.Volume
   ), 

   SalesSummary AS (
    SELECT 
        VendorNo AS SaleNumber,
        Brand,
        SUM(SalesQuantity) AS TotalQuantitySales,
        SUM(SalesDollars) AS TotalDollarsSales,
        SUM(SalesPrice) AS TotalPriceSales,
        SUM(ExciseTax) AS TotalTax
    FROM sales
    GROUP BY VendorNo, Brand
   ) 

   SELECT 
    ps.SaleNumber,
    ps.SaleName,
    ps.Brand,
    ps.Description,
    ps.CostPrice,
    ps.ActualPrice,
    ps.Quantity,
    ps.TotalQuantityPurchased,
    ps.TotalDollarsPurchased,
    ss.TotalQuantitySales,
    ss.TotalDollarsSales,
    ss.TotalPriceSales,
    ss.TotalTax AS TotalTax,
    fs.FreightCost AS CostFreight
   FROM PurchaseSummary ps
   LEFT JOIN SalesSummary ss 
    ON ps.SaleNumber = ss.SaleNumber 
    AND ps.Brand = ss.Brand
    LEFT JOIN FreightSummary fs 
    ON ps.SaleNumber = fs.SaleNumber
   ORDER BY ps.TotalDollarsPurchased DESC""", connection)

        return sales_stack_summary

def clean_data(df):
    df['Volume'] = df['Volume'].astype('float')
    
    df.fillna(0,inplace = True)
    
    # removing spaces from categorical columns
    df['SaleName'] = df['SaleName'].str.strip()
    df['Description'] = df['Description'].str.strip()

    df['GrossProfit'] = df['TotalDollarsSales'] - df['TotalDollarsPurchased']
    df['ProfitMargin'] = (df['GrossProfit'] / df['TotalDollarsSales'])*100
    df['Stocks'] = df['TotalQuantitySales'] / df['TotalQuantityPurchased']
    df['PurchaseToSalesRatio'] = df['TotalDollarsSales'] / df['TotalDollarsPurchased']

    return df

if __name__ == '__main__':
    # creating database connection
    conn = sqlite3.connect('main.db')
    
    logging.info('Creating sales stack Summary Table.....')
    summary_df = create_sales_stack_summary(connection)
    logging.info(summary_df.head())
    
    logging.info('Cleaning Data.....')
    clean_df = clean_data(summary_df)
    logging.info(clean_df.head())
    
    logging.info('Ingesting data.....')
    ingest_db(clean_df,'sales_stack_summary',connection)
    logging.info('Completed')

