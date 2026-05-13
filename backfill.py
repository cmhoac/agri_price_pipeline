import os
import sys
import glob
import logging
import subprocess

logging.basicConfig(level=logging.INFO, format='%(asctime)s - [%(levelname)s] - %(message)s')
logger = logging.getLogger("BackfillRunner")

def run_step(script_path, run_date):
    try:
        subprocess.run([sys.executable, script_path, run_date], check=True)
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ Lỗi khi chạy {script_path} cho ngày {run_date}: {e}")

def run_backfill():
    # Tìm tất cả các file trong thư mục bronze để lấy danh sách các ngày
    bronze_files = glob.glob("data/bronze/*_raw_*.csv")
    dates = set()
    for file in bronze_files:
        filename = os.path.basename(file)
        # Tên file dạng: hat_dieu_raw_YYYY-MM-DD.csv
        parts = filename.split('_raw_')
        if len(parts) == 2:
            date_part = parts[1].replace('.csv', '')
            dates.add(date_part)
            
    dates = sorted(list(dates))
    logger.info(f"Đã tìm thấy {len(dates)} ngày cần backfill: {dates}")
    
    for date in dates:
        logger.info(f"--- ĐANG BACKFILL CHO NGÀY {date} ---")
        run_step("src/transform/cleaner.py", date)
        run_step("src/transform/gold_maker.py", date)
        
    logger.info("🎉 BACKFILL HOÀN TẤT! 🎉")

if __name__ == "__main__":
    run_backfill()