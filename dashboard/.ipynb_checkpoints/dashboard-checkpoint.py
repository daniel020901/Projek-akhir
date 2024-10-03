import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import streamlit as st
from babel.numbers import format_currency

sns.set(style='dark')

# Helper function yang dibutuhkan untuk menyiapkan berbagai dataframe
def create_daily_orders_df(df):
    daily_orders_df = df.resample(rule='D', on='order_purchase_timestamp').agg({
        "order_id": "nunique"
    })
    daily_orders_df = daily_orders_df.reset_index()
    daily_orders_df.rename(columns={
        "order_id": "order_count"
    }, inplace=True)
    
    return daily_orders_df

def create_sum_order_items_df(df):
    sum_order_items_df = df.groupby("customer_city")["order_id"].nunique().sort_values(ascending=False).reset_index()
    return sum_order_items_df

def create_bystate_df(df):
    bystate_df = df.groupby(by="customer_state")["customer_unique_id"].nunique().reset_index()
    bystate_df.rename(columns={
        "customer_unique_id": "customer_count"
    }, inplace=True)
    
    return bystate_df

def create_rfm_df(df):
    rfm_df = df.groupby(by="customer_unique_id", as_index=False).agg({
        "order_purchase_timestamp": "max", #mengambil tanggal order terakhir
        "order_id": "nunique",
    })
    rfm_df.columns = ["customer_unique_id", "max_order_timestamp", "frequency"]
    
    rfm_df["max_order_timestamp"] = pd.to_datetime(rfm_df["max_order_timestamp"])
    recent_date = df["order_purchase_timestamp"].max()
    rfm_df["recency"] = rfm_df["max_order_timestamp"].apply(lambda x: (recent_date - x).days)
    rfm_df.drop("max_order_timestamp", axis=1, inplace=True)
    
    return rfm_df

# New function: Menghitung keterlambatan pengiriman
def create_delivery_delay_df(df):
    df['delay'] = (df['order_delivered_customer_date'] - df['order_estimated_delivery_date']).dt.days
    delayed_orders_df = df[df['delay'] > 0]  # Pesanan yang mengalami keterlambatan
    return delayed_orders_df

def create_delayed_orders_per_month(df):
    # Membuat kolom 'delay' yang merupakan selisih antara order_delivered_customer_date dan order_estimated_delivery_date
    df['delay'] = (df['order_delivered_customer_date'] - df['order_estimated_delivery_date']).dt.days
    delayed_orders_df = df[df['delay'] > 0]  # Filter pesanan yang mengalami keterlambatan
    
    # Grouping by month of purchase and counting delayed orders
    delayed_orders_df['month'] = delayed_orders_df['order_purchase_timestamp'].dt.to_period('M')
    delayed_orders_per_month = delayed_orders_df.groupby('month')["order_id"].nunique().reset_index()
    
    return delayed_orders_per_month

def create_city_delay_stats(df):
    # Membuat kolom 'delay' yang merupakan selisih antara order_delivered_customer_date dan order_estimated_delivery_date
    df['delay'] = (df['order_delivered_customer_date'] - df['order_estimated_delivery_date']).dt.days
    delayed_orders_df = df[df['delay'] > 0]  # Filter hanya pesanan yang terlambat
    
    # Menghitung rata-rata keterlambatan pengiriman per kota
    city_delay_stats = delayed_orders_df.groupby('customer_city')['delay'].mean().reset_index()
    city_delay_stats.rename(columns={'delay': 'delivery_delay'}, inplace=True)
    
    # Sort berdasarkan keterlambatan pengiriman tertinggi
    city_delay_stats_sorted = city_delay_stats.sort_values(by='delivery_delay', ascending=False)
    
    return city_delay_stats_sorted



# Load cleaned data
all_df = pd.read_csv("main_data.csv")

# Convert columns to datetime
all_df['order_purchase_timestamp'] = pd.to_datetime(all_df['order_purchase_timestamp'])
all_df['order_delivered_customer_date'] = pd.to_datetime(all_df['order_delivered_customer_date'])
all_df['order_estimated_delivery_date'] = pd.to_datetime(all_df['order_estimated_delivery_date'])
all_df.sort_values(by="order_purchase_timestamp", inplace=True)

# Filter data
min_date = all_df["order_purchase_timestamp"].min()
max_date = all_df["order_purchase_timestamp"].max()

with st.sidebar:
    st.image("https://github.com/dicodingacademy/assets/raw/main/logo.png")
    
    start_date, end_date = st.date_input(
        label='Rentang Waktu',min_value=min_date,
        max_value=max_date,
        value=[min_date, max_date]
    )

main_df = all_df[(all_df["order_purchase_timestamp"] >= str(start_date)) & 
                (all_df["order_purchase_timestamp"] <= str(end_date))]

# # Menyiapkan berbagai dataframe
daily_orders_df = create_daily_orders_df(main_df)
sum_order_items_df = create_sum_order_items_df(main_df)
bystate_df = create_bystate_df(main_df)
rfm_df = create_rfm_df(main_df)
delayed_orders_df = create_delivery_delay_df(main_df)  # New dataframe for delayed orders

# plot number of daily orders (2021)
st.header('Dicoding Collection Dashboard :sparkles:')
st.subheader('Daily Orders')

col1, col2 = st.columns(2)

with col1:
    total_orders = daily_orders_df.order_count.sum()
    st.metric("Total orders", value=total_orders)

fig, ax = plt.subplots(figsize=(16, 8))
ax.plot(
    daily_orders_df["order_purchase_timestamp"],
    daily_orders_df["order_count"],
    marker='o', 
    linewidth=2,
    color="#90CAF9"
)
ax.tick_params(axis='y', labelsize=20)
ax.tick_params(axis='x', labelsize=15)

st.pyplot(fig)

# Customer Demographics (by city & state)
st.subheader("Customer Demographics")

fig, ax = plt.subplots(figsize=(20, 10))
sns.barplot(
    x="customer_count", 
    y="customer_state",
    data=bystate_df.sort_values(by="customer_count", ascending=False),
    palette="Blues_d",
    ax=ax
)
ax.set_title("Number of Customer by States", loc="center", fontsize=30)
ax.set_ylabel(None)
ax.set_xlabel(None)
ax.tick_params(axis='y', labelsize=20)
ax.tick_params(axis='x', labelsize=15)
st.pyplot(fig)

# Best Customer Based on RFM Parameters
st.subheader("Best Customer Based on RFM Parameters")

col1, col2 = st.columns(2)

with col1:
    avg_recency = round(rfm_df.recency.mean(), 1)
    st.metric("Average Recency (days)", value=avg_recency)

with col2:
    avg_frequency = round(rfm_df.frequency.mean(), 2)
    st.metric("Average Frequency", value=avg_frequency)

fig, ax = plt.subplots(nrows=1, ncols=2, figsize=(35, 15))
sns.barplot(y="recency", x="customer_unique_id", data=rfm_df.sort_values(by="recency", ascending=True).head(5), ax=ax[0])
ax[0].set_ylabel(None)
ax[0].set_xlabel("customer_unique_id", fontsize=30)
ax[0].set_title("By Recency (days)", loc="center", fontsize=50)
ax[0].tick_params(axis='y', labelsize=30)
ax[0].tick_params(axis='x', labelsize=35)

sns.barplot(y="frequency", x="customer_unique_id", data=rfm_df.sort_values(by="frequency", ascending=False).head(5), ax=ax[1])
ax[1].set_ylabel(None)
ax[1].set_xlabel("customer_unique_id", fontsize=30)
ax[1].set_title("By Frequency", loc="center", fontsize=50)
ax[1].tick_params(axis='y', labelsize=30)
ax[1].tick_params(axis='x', labelsize=35)

st.pyplot(fig)

# Question 1: Perbedaan antara order_estimated_delivery_date dan order_delivered_customer_date
# Fungsi baru untuk menghitung statistik keterlambatan per kota
city_delay_stats_sorted = create_city_delay_stats(main_df)

# Plot top 10 kota dengan rata-rata keterlambatan pengiriman tertinggi
st.subheader("Top 10 Kota dengan Rata-rata Keterlambatan Pengiriman Tertinggi")

fig, ax = plt.subplots(figsize=(12, 6))
ax.bar(
    city_delay_stats_sorted.head(10)['customer_city'],
    city_delay_stats_sorted.head(10)['delivery_delay'],
    color='skyblue'
)
ax.set_xlabel('Kota', fontsize=12)
ax.set_ylabel('Rata-rata Keterlambatan Pengiriman (hari)', fontsize=12)
ax.set_title('Top 10 Kota dengan Rata-rata Keterlambatan Pengiriman Tertinggi', fontsize=16)
ax.tick_params(axis='x', rotation=45)
plt.tight_layout()

st.pyplot(fig)


# Question 2: Jumlah pesanan yang mengalami keterlambatan
# Fungsi baru untuk keterlambatan per bulan
delayed_orders_per_month = create_delayed_orders_per_month(main_df)

# Plot pola keterlambatan pengiriman per bulan
st.subheader("Pola Keterlambatan Pengiriman per Bulan")

fig, ax = plt.subplots(figsize=(12, 6))
ax.plot(
    delayed_orders_per_month['month'].astype(str),
    delayed_orders_per_month['order_id'],
    marker='o', 
    color='b'
)
ax.set_xlabel('Bulan Pembelian', fontsize=12)
ax.set_ylabel('Jumlah Pesanan dengan Keterlambatan', fontsize=12)
ax.set_title('Pola Keterlambatan Pengiriman per Bulan', fontsize=16)
ax.grid(True)
plt.xticks(rotation=45)
plt.tight_layout()

st.pyplot(fig)


st.caption('Copyright © Dicoding 2023')
