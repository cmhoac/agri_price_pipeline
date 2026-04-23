import polars as pl
import requests
import xml.etree.ElementTree as ET
import os
import sys
import logging
import pandera.polars as pa
from pandera.typing.polars import DataFrame
from datetime import datetime

# ==========================================
# 1. Cấu hình Logging
# ==========================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(levelname)s] - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("Cleaner")

# ==========================================
# 2. Data Validation Schema (Pandera) & Maps
# ==========================================
REGION_MAP = {
    "Bà Rịa - Vũng Tàu": "Vũng Tàu",
    "Bà Rịa Vũng Tàu": "Vũng Tàu",
    "Gia lai": "Gia Lai",
    "Đăk Lăk": "Đắk Lắk",
    "Đắc Lắc": "Đắk Lắk",
    "Đăk Nông": "Đắk Nông",
    "Đắc Nông": "Đắk Nông"
}

# Khai báo Schema sử dụng chuẩn Type của Polars
silver_schema = pa.DataFrameSchema({
    "Loại nông sản": pa.Column(pl.String, nullable=False),
    "Phân hạng": pa.Column(pl.String, nullable=True),
    "Khu vực": pa.Column(pl.String, nullable=False),
    "Ngày thu thập": pa.Column(pl.String, nullable=False),
    "Giá thấp nhất": pa.Column(pl.Int64, nullable=True),
    "Giá cao nhất": pa.Column(pl.Int64, nullable=True),
})

def validate_data(df: pl.DataFrame, name: str) -> pl.DataFrame:
    """Xác thực dữ liệu trực tiếp bằng chuẩn Polars"""
    try:
        # SỬA TẠI ĐÂY: Xóa bỏ việc chuyển qua pandas, validate trực tiếp df của polars
        silver_schema.validate(df)
        logger.info(f"✅ Data validation passed for {name}")
        return df
    except Exception as e:
        logger.error(f"❌ Data validation failed for {name}: {e}")
        # Dù fail vẫn return để không làm sập toàn bộ pipeline, nhưng có log báo động
        return df

def clean_durian_data(date_str):
    bronze_path = os.path.join("data", "bronze", f"sau_rieng_raw_{date_str}.csv")
    if not os.path.exists(bronze_path): return
        
    logger.info(f"Đang xử lý làm sạch file: {bronze_path}...")
    df_raw = pl.read_csv(bronze_path, infer_schema_length=0)
    
    if 'Giá hôm nay' in df_raw.columns:
        df_raw = df_raw.rename({'Giá hôm nay': 'Giá mới'})
    
    durian_name_map = {
        "Sầu riêng Thái": "Thái", "Sầu riêng Ri6": "Ri6", "Sầu riêng Musang King": "Musang King",
        "Sầu riêng Chuồng bò": "Chuồng Bò", "Chuồng bò": "Chuồng Bò", "Sáp Hữu": "Sáu Hữu", "Thái Lan": "Thái"
    }

    durian_grade_map = {
        "Loại A": "A", "Mẫu đẹp A": "A", "A": "A", "Loại B": "B", "Mẫu đẹp B": "B", "B": "B",
        "Loại C": "C", "C": "C", "C-D": "C-D", "VIP A": "VIP A", "VIP B": "VIP B",
        "Dạt": "Dạt", "Dạt nặng": "Dạt nặng", "Lỗi": "Lỗi", "Kem": "Kem"
    }

    grade_regex = r'(?i)\s*\(?\s*(VIP A|VIP B|Mẫu đẹp A|Mẫu đẹp B|Loại A|Loại B|Loại C|C-D|A|B|C|D|Dạt nặng|Dạt|Lỗi|Kem)\s*\)?$'

    df_silver = (
        df_raw
        .with_columns(pl.col('Nông sản').str.replace(r'\s*\(đ/kg\)', '').alias('Nông_sản_sạch'))
        .with_columns([
            pl.col('Nông_sản_sạch').str.extract(grade_regex, 1).alias('Phân hạng_Raw'),
            pl.col('Nông_sản_sạch').str.replace(grade_regex, '').str.strip_chars().alias('Raw_Name'),
            pl.lit('Toàn thị trường').alias('Khu vực')
        ])
        .with_columns([
            pl.col('Raw_Name').str.replace(r"(?i)^Sầu riêng\s+", "").str.strip_chars().replace(durian_name_map).alias('Loại nông sản'),
            pl.col('Phân hạng_Raw').str.strip_chars().replace(durian_grade_map).alias('Phân hạng')
        ])
        .with_columns(
            pl.col('Giá mới').str.replace_all(r'\.', '').str.extract_all(r'\d+').cast(pl.List(pl.Int64)).alias('Mảng_số')
        )
        .with_columns([
            pl.col('Mảng_số').list.min().alias('Giá thấp nhất'),
            pl.col('Mảng_số').list.max().alias('Giá cao nhất'),
            pl.col('Khu vực').str.strip_chars().replace(REGION_MAP).alias('Khu vực')
        ])
        .select(['Loại nông sản', 'Phân hạng', 'Khu vực', 'Ngày thu thập', 'Giá thấp nhất', 'Giá cao nhất'])
    )
    
    df_silver = validate_data(df_silver, "Durian")
    
    output_dir = os.path.join("data", "silver")
    os.makedirs(output_dir, exist_ok=True)
    silver_path = os.path.join(output_dir, f"sau_rieng_clean_{date_str}.csv")
    df_silver.write_csv(silver_path, include_bom=True)

def clean_pepper_data(date_str):
    bronze_path = os.path.join("data", "bronze", f"tieu_raw_{date_str}.csv")
    if not os.path.exists(bronze_path): return
        
    logger.info(f"Đang xử lý làm sạch file: {bronze_path}...")
    df_raw = pl.read_csv(bronze_path, infer_schema_length=0)
    
    if 'Khu vực' in df_raw.columns:
        if 'Giá hôm nay' in df_raw.columns:
            df_raw = df_raw.rename({'Giá hôm nay': 'Giá mới'})
        df = df_raw.with_columns([pl.lit('Hồ tiêu').alias('Loại nông sản'), pl.lit(None).alias('Phân hạng')])
    else:
        df = (df_raw
              .with_columns([
                  pl.lit('Hồ tiêu').alias('Loại nông sản'),
                  pl.lit(None).alias('Phân hạng'),
                  pl.col('Nông sản').str.extract(r'Hồ tiêu\s+(.*?)(?:\s*\(|$)').alias('Khu vực')
              ]))

    df = (
        df.with_columns(
            pl.col('Giá mới').str.replace_all(r'\.', '').str.extract_all(r'\d+').cast(pl.List(pl.Int64)).alias('Mảng_số')
        )
        .with_columns([
            pl.col('Mảng_số').list.min().alias('Giá thấp nhất'),
            pl.col('Mảng_số').list.max().alias('Giá cao nhất'),
            pl.col('Khu vực').str.strip_chars().replace(REGION_MAP).alias('Khu vực')
        ])
        .select(['Loại nông sản', 'Phân hạng', 'Khu vực', 'Ngày thu thập', 'Giá thấp nhất', 'Giá cao nhất'])
    )
    
    df = validate_data(df, "Pepper")
    
    silver_path = os.path.join("data", "silver", f"tieu_clean_{date_str}.csv")
    df.write_csv(silver_path, include_bom=True)

def get_vcb_usd_rate():
    try:
        url = "https://portal.vietcombank.com.vn/Usercontrols/TVPortal.TyGia/pXML.aspx"
        response = requests.get(url, timeout=10)
        tree = ET.fromstring(response.content)
        for exrate in tree.findall('Exrate'):
            if exrate.attrib.get('CurrencyCode') == 'USD':
                return float(exrate.attrib.get('Sell').replace(',', ''))
    except Exception as e:
        logger.warning(f"Không lấy được tỷ giá VCB ({e}). Dùng mặc định 25400.0")
    return 25400.0

def clean_cashew_data(date_str):
    bronze_path = os.path.join("data", "bronze", f"hat_dieu_raw_{date_str}.csv")
    if not os.path.exists(bronze_path): return
        
    logger.info(f"Đang xử lý làm sạch file: {bronze_path}...")
    usd_rate = get_vcb_usd_rate()
    df_raw = pl.read_csv(bronze_path, infer_schema_length=0)
    
    if 'Phân loại / Khu vực' in df_raw.columns:
        df_raw = df_raw.rename({'Phân loại / Khu vực': 'Cột_hỗn_hợp'})
    if 'Thay đổi (+/-)' in df_raw.columns:
        df_raw = df_raw.drop(['Thay đổi (+/-)'])
        
    df = (
        df_raw
        .with_columns([
            pl.lit('Hạt điều').alias('Loại nông sản'),
            pl.when(pl.col('Cột_hỗn_hợp').str.contains('(?i)W|Nhân trắng'))
              .then(pl.lit('Toàn thị trường')).otherwise(pl.col('Cột_hỗn_hợp')).alias('Khu vực'),
            pl.when(pl.col('Cột_hỗn_hợp').str.contains('(?i)W|Nhân trắng'))
              .then(pl.col('Cột_hỗn_hợp')).otherwise(pl.lit('Hạt tươi')).alias('Phân hạng'),
        ])
        .with_columns([
            pl.when(pl.col('Giá').str.contains('USD'))
              .then(pl.col('Giá').str.extract_all(r'[0-9]*\.[0-9]+|[0-9]+').list.eval(pl.element().cast(pl.Float64) * usd_rate).cast(pl.List(pl.Int64)))
              .otherwise(pl.col('Giá').str.replace_all(r'\.', '').str.extract_all(r'\d+').cast(pl.List(pl.Int64)))
              .alias('Mảng_số')
        ])
        .with_columns([
            pl.col('Mảng_số').list.min().alias('Giá thấp nhất'),
            pl.col('Mảng_số').list.max().alias('Giá cao nhất'),
            pl.col('Khu vực').str.strip_chars().replace(REGION_MAP).alias('Khu vực')
        ])
        .select(['Loại nông sản', 'Phân hạng', 'Khu vực', 'Ngày thu thập', 'Giá thấp nhất', 'Giá cao nhất'])
    )
    
    df = validate_data(df, "Cashew")
    
    silver_path = os.path.join("data", "silver", f"hat_dieu_clean_{date_str}.csv")
    df.write_csv(silver_path, include_bom=True)

if __name__ == "__main__":
    if len(sys.argv) > 1:
        run_date = sys.argv[1]
    else:
        run_date = datetime.now().strftime('%Y-%m-%d')
        
    logger.info(f"BẮT ĐẦU CHẠY PIPELINE LÀM SẠCH (SILVER) CHO NGÀY {run_date}...")
    clean_durian_data(run_date)
    clean_pepper_data(run_date) 
    clean_cashew_data(run_date)