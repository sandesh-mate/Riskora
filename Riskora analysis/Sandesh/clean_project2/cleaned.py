import pandas as pd
import numpy as np
import os

raw_data_path = r"C:\Users\Shyam\OneDrive\Documents\project$2"
clean_data_path = r"C:\Users\Shyam\OneDrive\Documents\processed"

os.makedirs(clean_data_path, exist_ok=True)

# Mapping: ticker
file_mapping = {
    'TCS.NS': 'TCS_raw',
    'INFY.NS': 'Infosys_raw',
    'HDFCBANK.NS': 'HDFC_Bank_raw',
    'ICICIBANK.NS': 'ICICI_Bank_raw',
    'RELIANCE.NS': 'Reliance_raw',
    'MARUTI.NS': 'Maruti_raw',          
    'AAPL': 'Apple_raw',
    'AMZN': 'Amazon_raw',
}

# ============================================
# FUNCTION TO READ CSV WITH CUSTOM HEADER HANDLING

def read_stock_csv(filepath):
    
    df = pd.read_csv(filepath, skiprows=1)
    
    first_val = str(df.iloc[0, 0])
    if '-' not in first_val:
        df = pd.read_csv(filepath, header=None, skiprows=2)  
        # Now assign column names
        df.columns = ['Date', 'Open', 'Close', 'Volume']
    else:
        # Rename first column to 'Date'
        df.rename(columns={df.columns[0]: 'Date'}, inplace=True)
    
    # Convert Date column to datetime
    df['Date'] = pd.to_datetime(df['Date'], dayfirst=True, errors='coerce')
    # Drop rows where date conversion failed
    df.dropna(subset=['Date'], inplace=True)
    
    # Convert numeric columns (Open, Close, Volume) to float, coercing errors
    for col in ['Open', 'Close', 'Volume']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Drop any rows with NaN in essential columns
    df.dropna(subset=['Open', 'Close', 'Volume'], inplace=True)
    
    return df

# ============================================
# CLEANING FUNCTION

def clean_stock_data(df, ticker):
    df = df.copy()

    # Set Date as index
    df.set_index('Date', inplace=True)
    df.sort_index(inplace=True)

    # Remove any remaining missing values
    df.dropna(inplace=True)

    # Remove negative prices/volumes
    df = df[df['Close'] > 0]
    df = df[df['Volume'] >= 0]

    # Daily returns (using Close price, since Adj Close is not present)
    df['Daily_Return'] = df['Close'].pct_change() * 100

    # Normalize price (first day = 100)
    df['Normalized'] = (df['Close'] / df['Close'].iloc[0]) * 100

    # Drop first row (NaN return)
    df.dropna(subset=['Daily_Return'], inplace=True)

    return df


for ticker, filename in file_mapping.items():
    input_file = os.path.join(raw_data_path, filename + '.csv')
    print(f"\n--- Processing {ticker} from {filename}.csv ---")

    if not os.path.exists(input_file):
        print(f"  File not found: {input_file}. Skipping.")
        continue

    try:
        df_raw = read_stock_csv(input_file)
        
        # Clean the data
        df_clean = clean_stock_data(df_raw, ticker)

        # Save cleaned file
        output_file = os.path.join(clean_data_path, f"{ticker}_cleaned.csv")
        df_clean.to_csv(output_file)
        print(f" Saved: {output_file}")
    except Exception as e:
        print(f" Error: {e}")
