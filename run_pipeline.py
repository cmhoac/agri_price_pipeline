import os
import sys
import logging
from datetime import datetime

from src.extract.scraper import fetch_durian_prices, fetch_pepper_prices, fetch_cashew_prices
from src.transform.cleaner import clean_durian_data, clean_pepper_data, clean_cashew_data
from src.transform.gold_maker import create_gold_layer

logging.basicConfig(level=logging.INFO, format='%(asctime)s - [%(levelname)s] - %(message)s', handlers=[logging.StreamHandler(sys.stdout)])
logger = logging.getLogger("PipelineRunner")

def run():
    run_date = datetime.now().strftime('%Y-%m-%d')
    logger.info(f"🚀 BẮT ĐẦU CHẠY SERVERLESS PIPELINE CHO NGÀY {run_date} 🚀")
    
    # 1. EXTRACT
    logger.info("--- BƯỚC 1: EXTRACT (BRONZE) ---")
    fetch_durian_prices()
    fetch_pepper_prices()
    fetch_cashew_prices()
    
    # 2. TRANSFORM
    logger.info("--- BƯỚC 2: CLEAN (SILVER) ---")
    clean_durian_data(run_date)
    clean_pepper_data(run_date)
    clean_cashew_data(run_date)
    
    # 3. LOAD
    logger.info("--- BƯỚC 3: WAREHOUSE (GOLD) ---")
    create_gold_layer(run_date)
    
    logger.info("🎉 TẤT CẢ DỮ LIỆU ĐÃ ĐƯỢC ĐẨY LÊN CLOUD THÀNH CÔNG! 🎉")

if __name__ == "__main__":
    run()
