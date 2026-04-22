import os
import sys
import logging
import subprocess
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - [%(levelname)s] - %(message)s', handlers=[logging.StreamHandler(sys.stdout)])
logger = logging.getLogger("PipelineRunner")

def run_step(script_path, run_date, step_name):
    try:
        subprocess.run([sys.executable, script_path, run_date], check=True)
        logger.info(f"✅ {step_name} thành công.")
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ Lỗi khi chạy {script_path}: {e}")
        sys.exit(1)

def run():
    run_date = datetime.now().strftime('%Y-%m-%d')
    logger.info(f"🚀 BẮT ĐẦU CHẠY SERVERLESS PIPELINE CHO NGÀY {run_date} 🚀")
    
    # 1. EXTRACT
    logger.info("--- BƯỚC 1: EXTRACT (BRONZE) ---")
    run_step("src/extract/scraper.py", run_date, "Cào dữ liệu Bronze")
    
    # 2. TRANSFORM
    logger.info("--- BƯỚC 2: CLEAN (SILVER) ---")
    run_step("src/transform/cleaner.py", run_date, "Làm sạch dữ liệu Silver")
    
    # 3. LOAD
    logger.info("--- BƯỚC 3: WAREHOUSE (GOLD) ---")
    run_step("src/transform/gold_maker.py", run_date, "Tạo Gold Layer & Lưu Database")
    
    logger.info("🎉 TẤT CẢ DỮ LIỆU ĐÃ ĐƯỢC ĐẨY LÊN CLOUD THÀNH CÔNG! 🎉")

if __name__ == "__main__":
    run()
