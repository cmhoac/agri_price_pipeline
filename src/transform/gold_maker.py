import polars as pl
import os
import sys
import logging
from datetime import datetime
from sqlalchemy import create_engine, text
from dotenv import load_dotenv # <-- Thêm thư viện đọc file .env

# Kích hoạt việc đọc file .env (nếu file tồn tại ở Local)
load_dotenv()

# ==========================================
# 1. Cấu hình Logging
# ==========================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(levelname)s] - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("GoldMaker")

def get_db_engine():
    """Lấy kết nối SQLAlchemy tới PostgreSQL (Neon Cloud DB) qua Két sắt bảo mật."""
    
    # 1. Lấy chìa khóa từ file .env (nếu chạy trên máy bạn) hoặc từ Secrets (nếu chạy trên GitHub Actions)
    db_url = os.getenv("NEON_DB_URL")
    
    if not db_url:
        logger.error("❌ LỖI BẢO MẬT: Không tìm thấy biến môi trường NEON_DB_URL!")
        raise ValueError("Chưa cấu hình NEON_DB_URL trong file .env hoặc GitHub Secrets.")
    
    try:
        # 2. Tạo động cơ kết nối
        # Chú ý: pool_pre_ping=True cực kỳ quan trọng khi dùng Serverless DB như Neon 
        # để máy chủ tự động kiểm tra xem kết nối mạng có bị rớt không trước khi gửi dữ liệu.
        engine = create_engine(db_url, pool_pre_ping=True)
        logger.info("☁️ Đã nạp chìa khóa thành công. Sẵn sàng kết nối tới Neon Cloud Database...")
        return engine
    except Exception as e:
        logger.error(f"❌ Không thể khởi tạo kết nối tới Database: {str(e)}")
        raise

def process_silver_file(file_path, phan_hang_default=None):
    if not os.path.exists(file_path):
        return None
        
    df = pl.read_csv(file_path, infer_schema_length=0)
    
    if phan_hang_default:
        df = df.with_columns(pl.col('Phân hạng').fill_null(pl.lit(phan_hang_default)))
        
    df = df.with_columns([
        pl.col('Loại nông sản').cast(pl.String),
        pl.col('Phân hạng').cast(pl.String),
        pl.col('Khu vực').cast(pl.String),
        pl.col('Ngày thu thập').cast(pl.String),
        pl.col('Giá thấp nhất').cast(pl.Int64, strict=False),
        pl.col('Giá cao nhất').cast(pl.Int64, strict=False),
    ])
    
    df = df.with_columns([
        ((pl.col('Giá thấp nhất') + pl.col('Giá cao nhất')) / 2).cast(pl.Int64).alias('Giá trung bình')
    ])
    
    common_columns = ['Loại nông sản', 'Phân hạng', 'Khu vực', 'Ngày thu thập', 'Giá thấp nhất', 'Giá cao nhất', 'Giá trung bình']
    return df.select(common_columns)

def create_gold_fact_table(date_str):
    silver_dir = "data/silver"
    gold_dir = "data/gold"
    os.makedirs(gold_dir, exist_ok=True)
    
    df_list = []
    
    # Sầu Riêng
    df_sr = process_silver_file(os.path.join(silver_dir, f"sau_rieng_clean_{date_str}.csv"))
    if df_sr is not None: df_list.append(df_sr)
        
    # Hồ Tiêu
    df_tieu = process_silver_file(os.path.join(silver_dir, f"tieu_clean_{date_str}.csv"), phan_hang_default="Tiêu đen")
    if df_tieu is not None: df_list.append(df_tieu)
        
    # Hạt Điều
    df_dieu = process_silver_file(os.path.join(silver_dir, f"hat_dieu_clean_{date_str}.csv"))
    if df_dieu is not None: df_list.append(df_dieu)

    if not df_list:
        logger.warning(f"Không có dữ liệu Silver nào cho ngày {date_str}.")
        return

    df_gold = pl.concat(df_list, how="vertical")
    
    df_gold = df_gold.with_columns([
        pl.col('Ngày thu thập').str.to_date("%Y-%m-%d", strict=False)
    ])
    
    # ==========================================
    # 2. LƯU THÀNH PARQUET (Fallback / Backup)
    # ==========================================
    gold_path_daily = os.path.join(gold_dir, f"fact_agri_prices_{date_str}.parquet")
    gold_path_latest = os.path.join(gold_dir, "fact_agri_prices_latest.parquet")
    
    df_gold.write_parquet(gold_path_daily)
    df_gold.write_parquet(gold_path_latest)
    logger.info(f"🌟 LƯU PARQUET THÀNH CÔNG: {gold_path_latest}")

    # ==========================================
    # 3. LƯU VÀO CƠ SỞ DỮ LIỆU POSTGRESQL
    # ==========================================
    try:
        engine = get_db_engine()
        # Để insert từ Polars vào DB dễ nhất, convert qua Pandas,
        # vì polars.write_database cần thư viện ADBC, trong khi Pandas hỗ trợ SQLAlchemy.
        pdf_gold = df_gold.to_pandas()
        
        # Xóa dữ liệu cũ của ngày này để tránh trùng lặp khi chạy lại (Idempotent)
        with engine.begin() as conn:
            # Kiểm tra xem bảng đã tồn tại chưa để tránh lỗi xóa trên bảng chưa có
            result = conn.execute(text("SELECT to_regclass('public.fact_agri_prices')")).scalar()
            if result:
                conn.execute(text(f"DELETE FROM fact_agri_prices WHERE \"Ngày thu thập\" = '{date_str}'"))
        
        # Chèn dữ liệu mới
        pdf_gold.to_sql('fact_agri_prices', con=engine, if_exists='append', index=False)
        logger.info(f"🗄️ LƯU DATABASE THÀNH CÔNG: Đã thêm {len(pdf_gold)} bản ghi vào bảng fact_agri_prices!")
    except Exception as e:
        logger.error(f"❌ Lỗi khi lưu vào PostgreSQL: {str(e)}")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        run_date = sys.argv[1]
    else:
        run_date = datetime.now().strftime('%Y-%m-%d')
        
    logger.info(f"BẮT ĐẦU CHẠY PIPELINE TẦNG GOLD & WAREHOUSE CHO NGÀY {run_date}...")
    create_gold_fact_table(run_date)