import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import time
import random
import logging
import concurrent.futures
from typing import Dict, List, Optional
from dataclasses import dataclass

@dataclass
class ScraperConfig:
    """Configuration settings for the web scraper."""
    BASE_URL: str = "https://fashion-studio.dicoding.dev"
    NUM_PAGES: int = 50
    MAX_WORKERS: int = 10
    MIN_DELAY: float = 0.05
    MAX_DELAY: float = 0.1
    HEADERS: Dict[str, str] = None

    def __post_init__(self):
        self.HEADERS = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 \
                (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        self.CHUNK_SIZE = self.NUM_PAGES // self.MAX_WORKERS + (self.NUM_PAGES % self.MAX_WORKERS > 0)

def setup_logging() -> logging.Logger:
    """Configure and return logger instance."""
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    
    # Create handlers
    c_handler = logging.StreamHandler()
    f_handler = logging.FileHandler('scraper.log')
    
    # Create formatters
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    c_handler.setFormatter(formatter)
    f_handler.setFormatter(formatter)
    
    # Add handlers to logger
    logger.addHandler(c_handler)
    logger.addHandler(f_handler)
    
    return logger

class ProductScraper:
    """Handles the scraping of product data from Fashion Studio website."""
    
    def __init__(self, config: ScraperConfig, logger: logging.Logger):
        self.config = config
        self.logger = logger

    def extract_product_details(self, soup) -> Optional[Dict[str, str]]:
        """Extract product details from a product card."""
        try:
            product_details = soup.find('div', class_='product-details')
            if not product_details:
                return None

            # Extract basic product information
            product_info = self._get_basic_info(product_details)
            
            # Extract additional details from p tags
            additional_info = self._get_additional_info(product_details)
            
            # Combine all information
            product_info.update(additional_info)
            product_info['timestamp'] = datetime.now().isoformat()
            
            return product_info

        except Exception as e:
            self.logger.error(f"Error extracting product details: {str(e)}")
            return None

    def _get_basic_info(self, product_details) -> Dict:
        """Extract basic product information."""
        title = product_details.find('h3', class_='product-title')
        price_container = product_details.find('div', class_='price-container')
        price = price_container.find('span', class_='price') if price_container else None

        return {
            'Title': title.text.strip() if title else None,
            'Price': price.text.strip() if price else None
        }

    def _get_additional_info(self, product_details) -> Dict:
        """Extract additional product information from p tags."""
        info = {
            'Rating': None,
            'Colors': None,
            'Size': None,
            'Gender': None
        }
        
        p_tags = product_details.find_all('p', style='font-size: 14px; color: #777;')
        for p in p_tags:
            text = p.get_text().strip()
            if 'Rating:' in text:
                rating = text.replace('⭐', '').replace('Rating:', '').split('/')[0].strip()
                info['Rating'] = rating
            elif 'Colors' in text:
                info['Colors'] = text
            elif 'Size:' in text:
                info['Size'] = text
            elif 'Gender:' in text:
                info['Gender'] = text
                
        return info

    def scrape_chunk(self, pages: List[int]) -> List[Dict]:
        """Scrape a chunk of pages using a single session."""
        products = []
        with requests.Session() as session:
            for page in pages:
                products.extend(self._scrape_single_page(session, page))
        return products

    def _scrape_single_page(self, session: requests.Session, page: int) -> List[Dict]:
        """Scrape products from a single page."""
        try:
            url = self.config.BASE_URL if page == 1 else f"{self.config.BASE_URL}/page{page}"
            self.logger.info(f"Scraping page {page}: {url}")
            
            response = session.get(url, headers=self.config.HEADERS)
            response.raise_for_status()
            
            products = self._process_page_content(response.content, page)
            time.sleep(random.uniform(self.config.MIN_DELAY, self.config.MAX_DELAY))
            
            return products

        except Exception as e:
            self.logger.error(f"Error on page {page}: {str(e)}")
            return []

    def _process_page_content(self, content: bytes, page: int) -> List[Dict]:
        """Process the HTML content of a page and extract products."""
        soup = BeautifulSoup(content, 'html.parser')
        collection_grid = soup.find('div', {'id': 'collectionList'})
        
        if not collection_grid:
            self.logger.error(f"Collection grid not found on page {page}")
            return []
            
        product_cards = collection_grid.find_all('div', class_='collection-card')
        self.logger.info(f"Found {len(product_cards)} products on page {page}")
        
        return [
            product_data for card in product_cards
            if (product_data := self.extract_product_details(card))
        ]

    def scrape(self) -> pd.DataFrame:
        """Main scraping function using parallel processing."""
        all_products = []
        
        try:
            pages = list(range(1, self.config.NUM_PAGES + 1))
            chunks = [pages[i:i + self.config.CHUNK_SIZE] 
                     for i in range(0, len(pages), self.config.CHUNK_SIZE)]
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.config.MAX_WORKERS) as executor:
                futures = {
                    executor.submit(self.scrape_chunk, chunk): i 
                    for i, chunk in enumerate(chunks)
                }
                
                for future in concurrent.futures.as_completed(futures):
                    chunk_id = futures[future]
                    try:
                        products = future.result()
                        all_products.extend(products)
                        self.logger.info(f"Completed chunk {chunk_id + 1}/{len(chunks)}")
                    except Exception as e:
                        self.logger.error(f"Error processing chunk {chunk_id}: {str(e)}")
                        continue

        except Exception as e:
            self.logger.error(f"Error in scraping process: {str(e)}")
            return pd.DataFrame()

        return pd.DataFrame(all_products) if all_products else pd.DataFrame()