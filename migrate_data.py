import os
import sys
import logging
import pandas as pd
from sqlalchemy import create_engine

logging.basicConfig(level=logging.INFO, format='%(asctime)s - [%(levelname)s] - %(message)s', handlers=[logging.StreamHandler(sys.stdout)])
logger = logging.getLogger("DataMigrator")

def migrate():
    local_db = 'postgresql+psycopg2://airflow:airflow@127.0.0.1:5432/airflow'
    cloud_db = 'postgresql+psycopg2://neondb_owner:npg_PS3BvUyViL6a@ep-wandering-rice-ao72uw5q-pooler.c-2.ap-southeast-1.aws.neon.tech/neondb?sslmode=require'
    
    logger.info("📡 Đang kết nối tới Local AlloyDB...")
    try:
        local_engine = create_engine(local_db)
        df = pd.read_sql("SELECT * FROM fact_agri_prices", local_engine)
        logger.info(f"✅ Đã tải {len(df)} dòng dữ liệu lịch sử từ Local DB.")
    except Exception as e:
        logger.error(f"❌ Lỗi đọc Local DB (Đảm bảo Docker đang chạy): {e}")
        return

    logger.info("☁️ Đang kết nối tới Neon Cloud DB...")
    try:
        cloud_engine = create_engine(cloud_db)
        
        # Đẩy dữ liệu lên Neon
        logger.info("⏳ Đang upload dữ liệu... vui lòng chờ...")
        df.to_sql('fact_agri_prices', cloud_engine, if_exists='replace', index=False)
        logger.info("✅ Đã đẩy toàn bộ dữ liệu lịch sử lên Neon thành công!")
        
        # Kiểm tra kết quả
        df_cloud = pd.read_sql("SELECT COUNT(*) FROM fact_agri_prices", cloud_engine)
        logger.info(f"📊 Tổng số dòng hiện tại trên Cloud DB: {df_cloud.iloc[0, 0]}")
    except Exception as e:
        logger.error(f"❌ Lỗi ghi lên Cloud DB: {e}")

if __name__ == "__main__":
    migrate()
