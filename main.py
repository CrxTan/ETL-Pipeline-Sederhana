from utils.extract import ScraperConfig, ProductScraper
from utils.transform import TransformConfig, DataTransformer
from utils.load import LoadConfig, DataLoader
import time
import sys
import logging

def setup_logging() -> logging.Logger:
    """Configure and return logger instance."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)

def run_etl_pipeline():
    """Execute the complete ETL pipeline."""
    start_time = time.time()
    logger = setup_logging()
    
    try:
        # Extract
        print("\n=== Starting Extraction Process ===")
        scraper_config = ScraperConfig()
        scraper = ProductScraper(scraper_config, logger)
        
        df_raw = scraper.scrape()
        if df_raw.empty:
            print(" Extraction failed: No data retrieved")
            return False
            
        df_raw.to_csv('products_raw.csv', index=False)
        print(f" Extraction completed: {len(df_raw)} records extracted")

        # Transform
        print("\n=== Starting Transformation Process ===")
        transform_config = TransformConfig()
        transformer = DataTransformer(transform_config)
        
        df_transformed = transformer.transform_dataframe(df_raw)
        if df_transformed.empty:
            print(" Transformation failed: No valid data after cleaning")
            return False
            
        print(f" Transformation completed: {len(df_transformed)} records cleaned")

        # Load
        print("\n=== Starting Loading Process ===")
        load_config = LoadConfig()
        loader = DataLoader(load_config)
        
        # Execute loading operations
        csv_success = loader.load_to_csv(df_transformed)
        sheets_success = loader.load_to_google_sheets(df_transformed)
        postgres_success = loader.load_to_postgres(df_transformed)
        
        # Final summary
        print("\n=== ETL Pipeline Summary ===")
        print(f"Initial records: {len(df_raw)}") 
        print(f"Cleaned records: {len(df_transformed)}")
        print("\nLoading Results:")
        print(f" CSV: {' Success' if csv_success else ' Failed'}")
        print(f" Google Sheets: {' Success' if sheets_success else ' Failed'}")
        print(f" PostgreSQL: {' Success' if postgres_success else ' Failed'}")
        
        execution_time = time.time() - start_time
        print(f"\nTotal execution time: {execution_time:.2f} seconds")
        
        # Return True if all loading operations succeeded
        return all([csv_success, sheets_success, postgres_success])

    except Exception as e:
        print(f"\n Pipeline failed: {str(e)}")
        return False

if __name__ == "__main__":
    print("=== Fashion Product ETL Pipeline ===")
    success = run_etl_pipeline()
    sys.exit(0 if success else 1)