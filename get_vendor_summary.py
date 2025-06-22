import pandas as pd
import sqlite3
import os
from sqlalchemy import create_engine
import logging
from ingestion_db import ingest_db
 
logging.basicConfig(
   filename="VendorData/logs/get_vendor_summary.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s-%(message)s",
    filemode="a"
)

def create_vendor_summary(conn):
    '''this funaction will merge the different tables to get the overall vendor summary and adding new columns in the resultant data'''
    vendor_sales_summary = pd.read_sql_query("""WITH FreightSummary AS(
 SELECT 
  VendorNumber,
  SUM(Freight) AS FreightCost
 From vendor_invoice
 Group by VendorNumber
),

PurchaseSummary AS(
   SELECT
      p.VendorNumber,
      p.VendorName,
      p.Brand,
      p.Description,
      p.PurchasePrice,
      pp.Price AS ActualPrice,
      pp.Volume,
      SUM(p.Quantity) AS TotalPurchaseQuantity,
      SUM(p.Dollars) AS TotalPurchaseDollars
    From purchases p
    JOIN Purchase_prices pp
         ON p.Brand = pp.Brand
    Where p.PurchasePrice > 0
    Group By p.VendorNumber, p.VendorName, p.Brand, p.Description, p.PurchasePrice, pp.Volume
    ),
    
SalesSummary As  (
  SELECT
      VendorNo,
      Brand,
      SUM(SalesQuantity) AS TotalSalesQuantity,
      SUM(SalesDollars) AS TotalSalesDollars,
      SUM(SalesPrice) AS TotalSalesPrice,
      SUM(ExciseTax) AS TotalExciseTax
  FROM sales
  Group By VendorNo, Brand 
) 

SELECT
    ps.VendorNumber,
    ps.VendorName,
    ps.Brand,
    ps.Description,
    ps.PurchasePrice,
    ps.ActualPrice,
    ps.Volume,
    ps.TotalPurchaseQuantity,
    ps.TotalPurchaseDollars,
    ss.TotalSalesQuantity,
    ss.TotalSalesDollars,
    ss.TotalSalesPrice,
    ss.TotalExciseTax,
    fs.FreightCost
FROM PurchaseSummary ps
LEFT Join SalesSummary ss
     ON ps.vendorNumber = ss.VendorNo
     AND ps.Brand = ss.Brand
LEFT JOIN FreightSummary fs
     ON ps.VendorNumber = fs.VendorNumber
ORDER BY ps.TotalPurchaseDollars DESC
""",conn)
    
    return vendor_sales_summary

def clean_data(df):
    '''this function will clean the data'''
    # changing datatype to float
    df['Volume']=df['Volume'].astype('float64')
    
    # filling missing value with 0
    df.fillna(0, inplace = True)
    
    # removing spaces from categorical columns
    df['VendorName'] = df['VendorName'].str.strip()
    df['Description'] = df['Description'].str.strip()
    
    #Creating new Columns for better analysis
    vendor_sales_summary['GrossProfit'] =vendor_sales_summary['TotalSalesDollars']-vendor_sales_summary['TotalPurchaseDollars']
    vendor_sales_summary['ProfitMargin'] =(vendor_sales_summary['GrossProfit'] / vendor_sales_summary['TotalSalesDollars'])*100
    vendor_sales_summary['StockTurnover'] = vendor_sales_summary['TotalSalesQuantity']/vendor_sales_summary['TotalPurchaseQuantity']
    vendor_sales_summary['SalesPurchaseRatio'] = vendor_sales_summary['TotalSalesDollars']/vendor_sales_summary['TotalPurchaseDollars']
    
    return df

if __name__ == '__main__':
    #creating database connection
    try:
        conn = sqlite3.connect('C:/Users/HP/inventory.db')
        logging.info('Connected to database.')

        logging.info('Creating Vendor Summary Table...')
        summary_df = create_vendor_summary(conn)
        logging.info('Vendor Summary Created:\n%s', summary_df.head())

        logging.info('Cleaning Data...')
        clean_df = clean_data(summary_df)  # FIXED: Previously you were calling create_vendor_summary again
        logging.info('Cleaned Data:\n%s', clean_df.head())

        logging.info('Ingesting data...')
        ingest_db(clean_df, 'vendor_sales_summary', conn)
        logging.info('Data ingestion complete.')

    except Exception as e:
        logging.error("An error occurred: %s", str(e))
    finally:
        conn.close()
        logging.info('Database connection closed.')
    