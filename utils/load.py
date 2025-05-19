from typing import Optional
from dataclasses import dataclass
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from sqlalchemy import create_engine, types, text
from pathlib import Path
import sys
import time

@dataclass
class LoadConfig:
    """Configuration settings for data loading."""
    # CSV settings
    CSV_FILENAME: str = 'products_cleaned.csv'
    CSV_ENCODING: str = 'utf-8'
    
    # Google Sheets settings
    CREDENTIALS_FILE: str = 'google-sheets-api.json'
    SPREADSHEET_ID: str = '1szvrIQObT_Ln5Ncv21OVeTOgZ2EQ7jut3BBJCKnW0oY'
    WORKSHEET_NAME: str = 'Cleaned Data'
    
    # PostgreSQL settings
    DB_NAME: str = 'fashion_catalog'
    DB_USER: str = 'fatur' 
    DB_PASSWORD: str = 'gataumauisiapa' 
    DB_HOST: str = 'localhost'
    DB_PORT: int = 5432
    TABLE_NAME: str = 'products'

class DataLoader:
    """Handles loading of transformed data to various destinations."""
    
    def __init__(self, config: LoadConfig):
        self.config = config

    def validate_dataframe(self, df: pd.DataFrame) -> bool:
        """Validate DataFrame before loading."""
        required_columns = ['Title', 'Price', 'Rating', 'Colors', 'Size', 'Gender', 'timestamp']

        if df.empty:
            print("Error: Empty DataFrame")
            return False

        if not all(col in df.columns for col in required_columns):
            print("Error: Missing required columns")
            print(f"Required: {required_columns}")
            print(f"Found: {df.columns.tolist()}")
            return False
    
    # Validate data content
        try:
            # Check Title not empty
            if df['Title'].isna().any() or (df['Title'] == '').any():
                print("Error: Empty titles found")
                return False

            # Check Price is positive
            if (df['Price'] <= 0).any():
                print("Error: Invalid prices found")
                return False

            # Check Rating is between 0 and 5
            if ((df['Rating'] < 0) | (df['Rating'] > 5)).any():
                print("Error: Invalid ratings found")
                return False

            # Check Colors is positive
            if (df['Colors'] <= 0).any():
                print("Error: Invalid color counts found")
                return False

            # Check Size is valid
            valid_sizes = ['XS', 'S', 'M', 'L', 'XL', 'XXL']
            if not df['Size'].isin(valid_sizes).all():
                print("Error: Invalid sizes found")
                return False

            # Check Gender is valid
            valid_genders = ['Men', 'Women', 'Unisex']
            if not df['Gender'].isin(valid_genders).all():
                print("Error: Invalid genders found")
                return False

            # Check timestamp format
            try:
                pd.to_datetime(df['timestamp'])
            except ValueError:
                print("Error: Invalid timestamp format")
                return False
            
        except Exception as e:
            print(f"Error validating data: {str(e)}")
            return False
        
        return True

    def load_to_csv(self, df: pd.DataFrame, filepath: Optional[str] = None) -> bool:
        """Save DataFrame to CSV file."""
        try:
            if not self.validate_dataframe(df):
                return False
                
            output_path = filepath or self.config.CSV_FILENAME
            print(f"\nSaving data to CSV file: {output_path}")
            
            df.to_csv(output_path, index=False, encoding=self.config.CSV_ENCODING)
            print(f" Successfully saved {len(df)} rows to CSV")
            return True
            
        except Exception as e:
            print(f" Error saving to CSV: {str(e)}")
            return False

    def load_to_google_sheets(self, df: pd.DataFrame) -> bool:
        """Save DataFrame to Google Sheets using spreadsheet ID."""
        print("\nUploading data to Google Sheets...")
        
        try:
            # Validate credentials file exists
            credentials_path = Path(self.config.CREDENTIALS_FILE)
            if not credentials_path.exists():
                print(f" Credentials file not found at: {credentials_path.absolute()}")
                return False
            
            # Setup Google Sheets connection
            print("Authenticating with Google Sheets...")
            scopes = [
                'https://www.googleapis.com/auth/spreadsheets',
                'https://www.googleapis.com/auth/drive'
            ]
            
            creds = Credentials.from_service_account_file(
                self.config.CREDENTIALS_FILE,
                scopes=scopes
            )
            
            client = gspread.authorize(creds)
            print(" Successfully authenticated")
            
            # Open spreadsheet
            print(f"Opening spreadsheet: {self.config.SPREADSHEET_ID}")
            try:
                spreadsheet = client.open_by_key(self.config.SPREADSHEET_ID)
                print(f" Opened spreadsheet: {spreadsheet.title}")
            except Exception as e:
                print(f" Error opening spreadsheet: {str(e)}")
                return False
            
            # Get or create worksheet
            print(f"Accessing worksheet: {self.config.WORKSHEET_NAME}")
            try:
                worksheet = spreadsheet.worksheet(self.config.WORKSHEET_NAME)
                print(" Found existing worksheet")
            except gspread.WorksheetNotFound:
                print("Creating new worksheet...")
                worksheet = spreadsheet.add_worksheet(
                    self.config.WORKSHEET_NAME, 
                    rows=len(df) + 1,
                    cols=len(df.columns)
                )
                print(" Created new worksheet")
            
            # Update data
            print("Uploading data...")
            worksheet.clear()
            data = [df.columns.values.tolist()] + df.values.tolist()
            worksheet.update(data, value_input_option='RAW')
            
            print(f" Successfully uploaded {len(df)} rows to Google Sheets")
            return True
            
        except Exception as e:
            print(f" Error in Google Sheets upload: {str(e)}")
            return False

    def load_to_postgres(self, df: pd.DataFrame) -> bool:
        """Save DataFrame to PostgreSQL database."""
        print("\nUploading data to PostgreSQL...")
            
        try:
            if not self.validate_dataframe(df):
                return False
                
            # Create connection string
            connection_string = (
                f'postgresql://{self.config.DB_USER}:{self.config.DB_PASSWORD}'
                f'@{self.config.DB_HOST}:{self.config.DB_PORT}/{self.config.DB_NAME}'
            )
            
            # Test database connection
            print("\nConnecting to PostgreSQL...")
            try:
                engine = create_engine(connection_string)
                with engine.connect() as conn:
                    print(" Successfully connected to PostgreSQL")
            except Exception as e:
                print(f" Database connection failed: {str(e)}")
                print("\nPlease check:")
                print("1. PostgreSQL service is running")
                print("2. Database credentials are correct")
                print("3. Database exists")
                return False
            
            # Define column types
            dtype = {
                'Title': types.String(),
                'Price': types.Float(),
                'Rating': types.Float(),
                'Colors': types.Integer(),
                'Size': types.String(),
                'Gender': types.String(),
                'timestamp': types.String()
            }
            
            # Upload data
            print(f"Uploading data to table: {self.config.TABLE_NAME}")
            df.to_sql(
                self.config.TABLE_NAME,
                engine,
                if_exists='replace',
                index=False,
                dtype=dtype
            )
            
            # Verify upload
            with engine.connect() as conn:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {self.config.TABLE_NAME}"))
                count = result.scalar()
                if count == len(df):
                    print(f" Successfully uploaded {count} rows to PostgreSQL")
                    return True
                else:
                    print(f" Upload verification failed. Expected {len(df)} rows, found {count}")
                    return False
                
        except Exception as e:
            print(f" Error in PostgreSQL upload: {str(e)}")
            return False