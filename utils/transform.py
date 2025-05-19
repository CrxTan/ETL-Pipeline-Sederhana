import pandas as pd
from typing import Optional
from dataclasses import dataclass
from datetime import datetime

@dataclass
class TransformConfig:
    """Configuration settings for data transformation."""
    EXCHANGE_RATE: float = 16000.0
    VALID_SIZES: set = None
    VALID_GENDERS: set = None

    def __post_init__(self):
        self.VALID_SIZES = {'XS', 'S', 'M', 'L', 'XL', 'XXL'}
        self.VALID_GENDERS = {'Men', 'Women', 'Unisex'}

class DataTransformer:
    """Handles the transformation of scraped fashion product data."""
    
    def __init__(self, config: TransformConfig):
        self.config = config

    def transform_price(self, price: str) -> Optional[float]:
        """Convert USD price string to IDR float value."""
        try:
            if pd.isna(price):
                return None
            cleaned_price = float(price.replace('$', '').replace(',', '').strip())
            return cleaned_price * self.config.EXCHANGE_RATE
        except (ValueError, AttributeError):
            return None

    def transform_rating(self, rating: str) -> Optional[float]:
        """Convert rating string to float value."""
        try:
            if pd.isna(rating) or 'Invalid' in str(rating) or 'Not Rated' in str(rating):
                return None
            cleaned_rating = rating.split('/')[0].strip()
            return float(cleaned_rating)
        except (ValueError, AttributeError):
            return None

    def transform_colors(self, colors: str) -> Optional[int]:
        """Extract number of colors as integer only."""
        try:
            if pd.isna(colors):
                return None
            num_colors = colors.split()[0]  # Get first part (number)
            return int(num_colors) if num_colors.isdigit() else None
        except (ValueError, AttributeError, IndexError):
            return None

    def transform_size(self, size: str) -> Optional[str]:
        """Extract size value only, without 'Size:' prefix."""
        try:
            if pd.isna(size):
                return None
            cleaned_size = size.replace('Size:', '').strip()
            return cleaned_size if cleaned_size in self.config.VALID_SIZES else None
        except AttributeError:
            return None

    def transform_gender(self, gender: str) -> Optional[str]:
        """Extract gender value only, without 'Gender:' prefix."""
        try:
            if pd.isna(gender):
                return None
            cleaned_gender = gender.replace('Gender:', '').strip()
            return cleaned_gender if cleaned_gender in self.config.VALID_GENDERS else None
        except AttributeError:
            return None

    def transform_timestamp(self, timestamp: str) -> Optional[str]:
        """Convert timestamp to simple UTC+8 format."""
        try:
            if pd.isna(timestamp):
                return None
            dt = pd.to_datetime(timestamp)
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        except (ValueError, AttributeError):
            return None

    def transform_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform the entire dataframe according to requirements."""
        print("\nStarting data transformation...")
        print(f"Initial record count: {len(df)}")
        
        required_columns = ['Title', 'Price', 'Rating', 'Colors', 'Size', 'Gender', 'timestamp']
        # If the DataFrame is empty or there are missing columns, return an empty DataFrame with complete columns.
        if df.empty or not all(col in df.columns for col in required_columns):
            print("Empty DataFrame or missing required columns")
            return pd.DataFrame(columns=required_columns)

        df_cleaned = df.copy()

        # Apply transformations
        df_cleaned['Price'] = df_cleaned['Price'].apply(self.transform_price)
        df_cleaned['Rating'] = df_cleaned['Rating'].apply(self.transform_rating)
        df_cleaned['Colors'] = df_cleaned['Colors'].apply(self.transform_colors)
        df_cleaned['Size'] = df_cleaned['Size'].apply(self.transform_size)
        df_cleaned['Gender'] = df_cleaned['Gender'].apply(self.transform_gender)
        df_cleaned['timestamp'] = df_cleaned['timestamp'].apply(self.transform_timestamp)

        # Remove invalid data
        print("Removing invalid records...")
        initial_count = len(df_cleaned)
        
        df_cleaned = df_cleaned.dropna(subset=['Title', 'Price', 'Rating', 'Colors', 'Size', 'Gender'])
        after_null_count = len(df_cleaned)
        
        df_cleaned = df_cleaned[df_cleaned['Title'] != 'Unknown Product']
        after_unknown_count = len(df_cleaned)
        
        # Remove duplicates
        df_cleaned = df_cleaned.drop_duplicates()
        final_count = len(df_cleaned)

        # Set correct data types
        print("Setting correct data types...")
        df_cleaned = df_cleaned.astype({
            'Title': 'object',  
            'Price': 'float64',
            'Rating': 'float64',
            'Colors': 'int32',
            'Size': 'object',   
            'Gender': 'object', 
            'timestamp': 'string'
        })

        # Print transformation statistics
        print("\nTransformation Statistics:")
        print(f"Initial records: {initial_count}")
        print(f"After removing nulls: {after_null_count} (removed {initial_count - after_null_count})")
        print(f"After removing unknown products: {after_unknown_count} (removed {after_null_count - after_unknown_count})")
        print(f"After removing duplicates: {final_count} (removed {after_unknown_count - final_count})")

        return df_cleaned