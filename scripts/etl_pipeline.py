# =====================================
# IMPORT LIBRARIES
# =====================================

import pandas as pd
from sqlalchemy import create_engine
import urllib.parse

# =====================================
# EXTRACT STAGE
# =====================================

try:

    df_super_store_sales = pd.read_csv(
        r'C:\Users\Sridhar\Documents\sales_etl_project\data\samplesuperstore.csv'
    )

    print("EXTRACT STAGE COMPLETED")

except Exception as e:

    print("Extract failed :",e)


# =====================================
# TRANSFORM STAGE
# =====================================

df_super_store_sales.columns = (
    df_super_store_sales.columns
    .str.lower()
    .str.replace(' ','_',regex=False)
    .str.replace('-','_',regex=False)
    .str.replace('/','_',regex=False)
)


df_super_store_sales['order_date']=pd.to_datetime(
df_super_store_sales['order_date'],
errors='coerce'
)

df_super_store_sales['ship_date']=pd.to_datetime(
df_super_store_sales['ship_date'],
errors='coerce'
)


df_super_store_sales['sales']=df_super_store_sales['sales'].astype(float)

df_super_store_sales['profit']=df_super_store_sales['profit'].astype(float)

df_super_store_sales['quantity']=df_super_store_sales['quantity'].astype(int)


print("TRANSFORM STAGE COMPLETED")


# =====================================
# DATA QUALITY
# =====================================

duplicate_count=df_super_store_sales.duplicated().sum()

print("Duplicates :",duplicate_count)

if duplicate_count>0:

    df_super_store_sales.drop_duplicates(inplace=True)

    df_super_store_sales.reset_index(drop=True,inplace=True)


null_count=df_super_store_sales.isnull().sum().sum()

print("Null values :",null_count)

df_super_store_sales.fillna({

'postal_code':0,
'discount':0

},inplace=True)


print("DATA QUALITY COMPLETED")


# =====================================
# TABLE SPLIT
# =====================================

df_customers=(

df_super_store_sales[
['customer_id','customer_name','segment',
 'country_region','city','state_province',
 'postal_code','region']
]

.drop_duplicates(subset=['customer_id'])

.reset_index(drop=True)

)


df_products=(

df_super_store_sales[
['product_id','category',
 'sub_category','product_name']
]

.drop_duplicates(subset=['product_id'])

.reset_index(drop=True)

)


df_orders=(

df_super_store_sales[
['order_id','order_date','ship_date',
 'ship_mode','customer_id','product_id',
 'sales','quantity','discount','profit']
]

.drop_duplicates(subset=['order_id','product_id'])

.reset_index(drop=True)

)


print("TABLE SPLIT COMPLETED")


# =====================================
# DATABASE CONNECTION
# =====================================

password=urllib.parse.quote("your_password")

engine=create_engine(

f"mysql+pymysql://root:{password}@127.0.0.1:3306/super_store"

)


# =====================================
# INCREMENTAL LOAD LOGIC
# =====================================

try:

    # Existing customers

    db_customers=pd.read_sql(
    "SELECT customer_id FROM customers",
    engine
    )

    df_customers=df_customers[
    ~df_customers['customer_id'].isin(
    db_customers['customer_id']
    )
    ]


    # Existing products

    db_products=pd.read_sql(
    "SELECT product_id FROM products",
    engine
    )

    df_products=df_products[
    ~df_products['product_id'].isin(
    db_products['product_id']
    )
    ]


    # Existing orders

    db_orders=pd.read_sql(
    "SELECT order_id,product_id FROM orders",
    engine
    )


    df_orders=df_orders.merge(

    db_orders,

    on=['order_id','product_id'],

    how='left',

    indicator=True

    )


    df_orders=df_orders[
    df_orders['_merge']=="left_only"
    ]


    df_orders.drop(
    columns=['_merge'],
    inplace=True
    )


    print("INCREMENTAL FILTER COMPLETED")

    print("New customers :",len(df_customers))

    print("New products :",len(df_products))

    print("New orders :",len(df_orders))


# If first run (tables empty)

except:

    print("First load - full insert")


# =====================================
# LOAD STAGE
# =====================================

try:

    if len(df_customers)>0:

        df_customers.to_sql(

        'customers',

        engine,

        if_exists='append',

        index=False,

        method='multi'

        )


    if len(df_products)>0:

        df_products.to_sql(

        'products',

        engine,

        if_exists='append',

        index=False,

        method='multi'

        )


    if len(df_orders)>0:

        df_orders.to_sql(

        'orders',

        engine,

        if_exists='append',

        index=False,

        method='multi'

        )


    print("LOAD COMPLETED")

except Exception as e:

    print("Load failed :",e)


# =====================================
# FINAL STATUS
# =====================================

print("ETL PIPELINE COMPLETED")

