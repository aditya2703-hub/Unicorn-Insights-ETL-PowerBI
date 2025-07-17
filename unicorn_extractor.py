import pandas as pd
from datetime import datetime
import time
import psycopg2
from sqlalchemy import create_engine, text
import random
import numpy as np # Import numpy for NaN handling

# --- Configuration ---
CSV_FILE_PATH = "Unicorn_Companies.csv" # Make sure this file is in the same directory as your script

# --- Automation Interval ---
MIN_REFRESH_INTERVAL_SECONDS = 3600 * 24 # 24 hours
MAX_REFRESH_INTERVAL_SECONDS = 3600 * 48 # 48 hours

# --- Database Configuration ---
DB_HOST = "localhost"
DB_NAME = "unicorn_db"
DB_USER = "postgres"
DB_PASSWORD = "adi1234" # <<< !!! IMPORTANT: REPLACE THIS !!! >>>
DB_PORT = "5432"

# SQLAlchemy engine for Pandas to_sql
engine_string = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
db_engine = create_engine(engine_string)

# --- Helper function for cleaning currency values ---
def clean_currency_value(val_str):
    """
    Cleans and converts currency strings (e.g., '$1.5B', '200M', 'None') to float (in millions).
    Returns None for unparseable or 'None' values.
    """
    if isinstance(val_str, str):
        val_str = val_str.replace('$', '').replace(',', '').strip()
        if val_str.lower() == 'none' or val_str == '': # Handle 'None' as string or empty string
            return None
        if 'B' in val_str: # Billions
            val_str = val_str.replace('B', '')
            try:
                return float(val_str) * 1000 # Convert to millions for consistency
            except ValueError:
                return None
        elif 'M' in val_str: # Millions
            val_str = val_str.replace('M', '')
            try:
                return float(val_str)
            except ValueError:
                return None
        else:
            try:
                return float(val_str) # Assume it's already a number
            except ValueError:
                return None
    return val_str

# --- Main Automation Loop ---
while True:
    current_wait_time = random.randint(MIN_REFRESH_INTERVAL_SECONDS, MAX_REFRESH_INTERVAL_SECONDS)
    print(f"\n--- Starting data load cycle for Unicorns at {datetime.now()} ---")

    df_transformed = pd.DataFrame() # Initialize empty DataFrame

    try:
        # --- 1. Load data from CSV ---
        print(f"Loading data from {CSV_FILE_PATH}...")
        df_raw = pd.read_csv(CSV_FILE_PATH)
        
        if df_raw.empty:
            print("Error: CSV file is empty or could not be read.")
            time.sleep(current_wait_time)
            continue

        # --- 2. Data Transformation ---
        column_mapping = {
            'Company': 'company_name',
            'Valuation ($B)': 'valuation_billion',
            'Date Joined': 'date_joined_unicorn_club',
            'Country': 'country',
            'City': 'city',
            'Industry': 'industry',
            'Select Inverstors': 'select_investors', # Handles common typo
            'Founded Year': 'founded_year',
            'Total Raised': 'total_raised_million',
            'Financial Stage': 'financial_stage',
            'Investors Count': 'investors_count',
            'Deal Terms': 'deal_terms',
            'Portfolio Exits': 'portfolio_exits'
        }
        
        df_transformed = df_raw.rename(columns=column_mapping)
        df_transformed = df_transformed[[col for col in column_mapping.values() if col in df_transformed.columns]]

        # Numeric columns (for PostgreSQL NUMERIC)
        for col_name in ['valuation_billion', 'total_raised_million']:
            if col_name in df_transformed.columns:
                df_transformed[col_name] = df_transformed[col_name].apply(clean_currency_value)
                # No need to convert pd.isna to None here, universal conversion below will handle it.

        # Date columns (for PostgreSQL DATE)
        for col_name in ['date_joined_unicorn_club']:
            if col_name in df_transformed.columns:
                df_transformed[col_name] = pd.to_datetime(df_transformed[col_name], errors='coerce').dt.date
                # No need to convert pd.isna to None here, universal conversion below will handle it.
        
        # Integer columns (for PostgreSQL INTEGER/SMALLINT)
        for col_name in ['founded_year', 'investors_count']:
            if col_name in df_transformed.columns:
                # Use nullable integer type. pd.NA will be handled by universal conversion.
                df_transformed[col_name] = pd.to_numeric(df_transformed[col_name], errors='coerce').astype('Int64')

        # String columns (for PostgreSQL VARCHAR)
        for col_name in ['company_name', 'industry', 'country', 'city', 'select_investors', 'financial_stage', 'deal_terms', 'portfolio_exits']:
            if col_name in df_transformed.columns:
                df_transformed[col_name] = df_transformed[col_name].astype(str)
                # No need to replace 'nan' or '' here, universal conversion below will handle it.
        
        # --- NEW FIX: Universal conversion of all Pandas missing values (NaN, NaT, pd.NA) to Python None ---
        # This is the most robust way to ensure psycopg2 doesn't encounter unknown types.
        df_transformed = df_transformed.replace({np.nan: None, pd.NA: None, pd.NaT: None, '': None})

        df_transformed['load_date'] = datetime.now().date()

        print("\n--- Transformed Data (first 5 rows) ---")
        print(df_transformed.head())
        print("\n--- Transformed Data Info (check data types) ---")
        df_transformed.info()

        # --- Data Loading into PostgreSQL ---
        print("Loading data into PostgreSQL...")
        try:
            with db_engine.connect() as connection:
                # 1. Load/Update dim_date table
                unique_dates = df_transformed['load_date'].unique()
                
                dim_date_df = pd.DataFrame({
                    'full_date': unique_dates,
                    'year': [d.year for d in unique_dates],
                    'month': [d.month for d in unique_dates],
                    'day': [d.day for d in unique_dates],
                    'day_of_week': [d.weekday() for d in unique_dates],
                    'is_weekday': [d.weekday() < 5 for d in unique_dates]
                })
                
                existing_date_map = {}
                existing_dates_query = connection.execute(text("SELECT full_date, date_key FROM dim_date")).fetchall()
                existing_date_map = {row[0]: row[1] for row in existing_dates_query}
                
                new_dates_to_insert = dim_date_df[~dim_date_df['full_date'].isin(existing_date_map.keys())]
                
                if not new_dates_to_insert.empty:
                    new_dates_to_insert.to_sql('dim_date', connection, if_exists='append', index=False)
                    connection.commit()
                    print(f"Inserted {len(new_dates_to_insert)} new dates into dim_date.")
                    
                    new_inserted_dates_query = connection.execute(text("SELECT full_date, date_key FROM dim_date WHERE full_date IN :dates"), {'dates': tuple(new_dates_to_insert['full_date'].tolist())}).fetchall()
                    for d, dk in new_inserted_dates_query:
                        existing_date_map[d] = dk
                
                df_transformed['load_date_key'] = df_transformed['load_date'].map(existing_date_map)
                
                # 2. Load/Update dim_company table
                dim_company_cols = ['company_name', 'industry', 'country', 'city', 'founded_year', 'date_joined_unicorn_club', 'select_investors']
                dim_company_df = df_transformed[[col for col in dim_company_cols if col in df_transformed.columns]].drop_duplicates(subset=['company_name'])
                
                rows_inserted_company = 0
                rows_updated_company = 0
                
                for index, row_data in dim_company_df.iterrows():
                    try:
                        connection.begin_nested()
                        
                        result = connection.execute(text(f"SELECT company_key FROM dim_company WHERE company_name = :company_name"), {'company_name': row_data['company_name']}).fetchone()
                        
                        insert_update_data = {k: row_data.get(k) for k in dim_company_cols}
                        
                        if result:
                            company_key = result[0]
                            update_sql = text("""
                                UPDATE dim_company SET
                                    industry = :industry,
                                    country = :country,
                                    city = :city,
                                    founded_year = :founded_year,
                                    date_joined_unicorn_club = :date_joined_unicorn_club,
                                    select_investors = :select_investors
                                WHERE company_name = :company_name;
                            """)
                            connection.execute(update_sql, insert_update_data)
                            rows_updated_company += 1
                        else:
                            insert_sql = text("""
                                INSERT INTO dim_company (company_name, industry, country, city, founded_year, date_joined_unicorn_club, select_investors)
                                VALUES (:company_name, :industry, :country, :city, :founded_year, :date_joined_unicorn_club, :select_investors)
                                RETURNING company_key;
                            """)
                            company_key = connection.execute(insert_sql, insert_update_data).scalar_one()
                            rows_inserted_company += 1
                        
                        df_transformed.loc[df_transformed['company_name'] == row_data['company_name'], 'company_key'] = int(company_key)

                        connection.commit()
                        
                    except Exception as e:
                        connection.rollback()
                        print(f"Error managing dim_company for {row_data['company_name']}: {e}")
                        print(f"Problematic dim_company row data: {row_data.to_dict()}")
                        df_transformed.loc[df_transformed['company_name'] == row_data['company_name'], 'company_key'] = None
                connection.commit()
                print(f"Managed dim_company: Inserted {rows_inserted_company}, Updated {rows_updated_company} records.")

                # 3. Load fact_unicorn_snapshot table
                fact_columns = ['load_date_key', 'company_key', 'valuation_billion', 'total_raised_million', 'financial_stage', 'investors_count', 'deal_terms', 'portfolio_exits']
                df_fact = df_transformed[[col for col in fact_columns if col in df_transformed.columns]].copy()
                
                rows_inserted_fact = 0
                rows_skipped_fact = 0
                
                for index, row_data in df_fact.iterrows():
                    if row_data['company_key'] is None or row_data['load_date_key'] is None:
                        print(f"Skipping fact row for company_name: {df_transformed.loc[index, 'company_name']} due to missing company_key or load_date_key.")
                        rows_skipped_fact += 1
                        continue
                        
                    row_data['company_key'] = int(row_data['company_key'])
                    
                    try:
                        connection.begin_nested()
                        
                        insert_fact_sql = text("""
                            INSERT INTO fact_unicorn_snapshot (load_date_key, company_key, valuation_billion, total_raised_million, financial_stage, investors_count, deal_terms, portfolio_exits)
                            VALUES (:load_date_key, :company_key, :valuation_billion, :total_raised_million, :financial_stage, :investors_count, :deal_terms, :portfolio_exits)
                            ON CONFLICT (load_date_key, company_key) DO UPDATE SET
                                valuation_billion = EXCLUDED.valuation_billion,
                                total_raised_million = EXCLUDED.total_raised_million,
                                financial_stage = EXCLUDED.financial_stage,
                                investors_count = EXCLUDED.investors_count,
                                deal_terms = EXCLUDED.deal_terms,
                                portfolio_exits = EXCLUDED.portfolio_exits;
                        """)
                        result = connection.execute(insert_fact_sql, row_data.to_dict())
                        if result.rowcount > 0:
                            rows_inserted_fact += 1
                        else:
                            rows_skipped_fact += 1
                        connection.commit()
                        
                    except Exception as e:
                        connection.rollback()
                        print(f"Error inserting/updating fact row for {row_data.get('company_name')} on {row_data.get('load_date_key')}: {e}")
                        print(f"Problematic fact row data: {row_data.to_dict()}")
                        rows_skipped_fact += 1
                
                print(f"Cycle complete: Loaded {rows_inserted_fact} new/updated fact records. Skipped {rows_skipped_fact} existing records.")

        except psycopg2.Error as db_err:
            print(f"Database error during loading: {db_err}. PostgreSQL error code: {db_err.pgcode}. Retrying in next cycle.")
        except Exception as e:
            print(f"An unexpected error occurred during database loading: {e}. Retrying in next cycle.")

    except Exception as e:
        print(f"An unexpected error occurred in main loop: {e}. Retrying in next cycle.")

    print(f"Waiting for {current_wait_time} seconds before next full fetch cycle...")
    time.sleep(current_wait_time)