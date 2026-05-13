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
    "Giá thấp nhất": pa.Column(pl.Float64, nullable=True),
    "Giá cao nhất": pa.Column(pl.Float64, nullable=True),
    "Đơn vị": pa.Column(pl.String, nullable=False),
})

def validate_data(df: pl.DataFrame, name: str) -> pl.DataFrame:
    """Xác thực dữ liệu trực tiếp bằng chuẩn Polars"""
    try:
        silver_schema.validate(df)
        logger.info(f"✅ Data validation passed for {name}")
        return df
    except Exception as e:
        logger.error(f"❌ Data validation failed for {name}: {e}")
        # Dù fail vẫn return để không làm sập toàn bộ pipeline, nhưng có log báo động
        return df

# ==========================================
# 3. Durian Cleaner
# ==========================================
def clean_durian_data(date_str):
    bronze_path = os.path.join("data", "bronze", f"sau_rieng_raw_{date_str}.csv")
    if not os.path.exists(bronze_path): return
        
    logger.info(f"Đang xử lý làm sạch file: {bronze_path}...")
    df_raw = pl.read_csv(bronze_path, infer_schema_length=0)
    
    if 'Giá hôm nay' in df_raw.columns:
        df_raw = df_raw.rename({'Giá hôm nay': 'Giá mới'})
    
    durian_name_map = {
        "Sầu riêng Thái": "Thái", "Sầu riêng Ri6": "Ri6", "Sầu riêng Musang King": "Musang King",
        "Sầu riêng Chuồng bò": "Chuồng Bò", "Chuồng bò": "Chuồng Bò", "Sáp Hữu": "Sáu Hữu",
        "Thái Lan": "Thái",
    }

    durian_grade_map = {
        "Loại A": "A", "Mẫu đẹp A": "A", "A": "A", "Loại B": "B", "Mẫu đẹp B": "B", "B": "B",
        "Loại C": "C", "C": "C", "C-D": "C-D",
        "Dạt": "Dạt", "Dạt nặng": "Dạt nặng", "Lỗi": "Lỗi", "Kem": "Kem"
    }

    # Regex cải tiến: xóa mọi dạng đơn vị trong ngoặc cuối cùng (đ/kg), (đồng/kg), (vnđ/kg)
    unit_strip_regex = r'\s*\([^)]*\)\s*$'
    # Lưu ý: KHÔNG capture "VIP A/B/C" — VIP là phần tên sản phẩm (Thái VIP), chỉ capture grade cuối (A/B/C)
    grade_regex = r'(?i)\s*\(?\s*(Mẫu đẹp A|Mẫu đẹp B|Loại A|Loại B|Loại C|C-D|A|B|C|D|Dạt nặng|Dạt|Lỗi|Kem)\s*\)?$'

    df_silver = (
        df_raw
        .with_columns(pl.col('Nông sản').str.replace(unit_strip_regex, '').str.strip_chars().alias('Nông_sản_sạch'))
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
            pl.col('Mảng_số').list.min().cast(pl.Float64).alias('Giá thấp nhất'),
            pl.col('Mảng_số').list.max().cast(pl.Float64).alias('Giá cao nhất'),
            pl.col('Khu vực').str.strip_chars().replace(REGION_MAP).alias('Khu vực'),
            pl.lit('đ/kg').alias('Đơn vị')
        ])
        .filter(~pl.col('Giá mới').str.contains('(?i)thương lượng'))
        .select(['Loại nông sản', 'Phân hạng', 'Khu vực', 'Ngày thu thập', 'Giá thấp nhất', 'Giá cao nhất', 'Đơn vị'])
    )
    
    df_silver = validate_data(df_silver, "Durian")
    
    output_dir = os.path.join("data", "silver")
    os.makedirs(output_dir, exist_ok=True)
    silver_path = os.path.join(output_dir, f"sau_rieng_clean_{date_str}.csv")
    df_silver.write_csv(silver_path, include_bom=True)

# ==========================================
# 4. Pepper Cleaner
# ==========================================
def clean_pepper_data(date_str):
    bronze_path = os.path.join("data", "bronze", f"tieu_raw_{date_str}.csv")
    if not os.path.exists(bronze_path): return
        
    logger.info(f"Đang xử lý làm sạch file: {bronze_path}...")
    df_raw = pl.read_csv(bronze_path, infer_schema_length=0)
    
    if 'Giá hôm nay' in df_raw.columns:
        df_raw = df_raw.rename({'Giá hôm nay': 'Giá mới'})

    # Bước 1: Trích xuất đơn vị, phân hạng, khu vực từ cột "Nông sản"
    # Ví dụ: "Hồ Tiêu Đắk Lắk (đ/kg)" → Khu vực: Đắk Lắk, Phân hạng: Tiêu đen, Đơn vị: đ/kg
    # Ví dụ: "Tiêu Đen Indonesia (USD/tấn)" → Khu vực: Indonesia, Phân hạng: Tiêu đen, Đơn vị: USD/tấn
    # Ví dụ: "Tiêu Trắng Malaysia ASTA (USD/tấn)" → Khu vực: Malaysia, Phân hạng: Tiêu trắng, Đơn vị: USD/tấn
    df = (
        df_raw
        .with_columns([
            pl.lit('Hồ tiêu').alias('Loại nông sản'),
            # Trích xuất đơn vị từ ngoặc
            pl.col('Nông sản').str.extract(r'\(([^)]+)\)').alias('Đơn vị'),
            # Xóa đơn vị để lấy tên sạch
            pl.col('Nông sản').str.replace(r'\s*\([^)]*\)\s*$', '').str.strip_chars().alias('Tên_sạch'),
        ])
        .with_columns([
            # Phân hạng: Tiêu trắng hay Tiêu đen
            pl.when(pl.col('Tên_sạch').str.contains(r'(?i)Tiêu Trắng'))
              .then(pl.lit('Tiêu trắng'))
              .otherwise(pl.lit('Tiêu đen'))
              .alias('Phân hạng'),
            # Khu vực: phần còn lại sau khi xóa prefix loại tiêu và suffix ASTA
            pl.col('Tên_sạch')
              .str.replace(r'(?i)^(?:Hồ Tiêu|Tiêu Đen|Tiêu Trắng)\s+', '')
              .str.replace(r'(?i)\s*ASTA\s*\d*$', '')
              .str.strip_chars()
              .alias('Khu vực'),
        ])
        .with_columns(
            pl.col('Giá mới').str.replace_all(r'\.', '').str.extract_all(r'\d+').cast(pl.List(pl.Int64)).alias('Mảng_số')
        )
        .with_columns([
            pl.col('Mảng_số').list.min().cast(pl.Float64).alias('Giá thấp nhất'),
            pl.col('Mảng_số').list.max().cast(pl.Float64).alias('Giá cao nhất'),
            pl.col('Khu vực').str.strip_chars().replace(REGION_MAP).alias('Khu vực')
        ])
        .select(['Loại nông sản', 'Phân hạng', 'Khu vực', 'Ngày thu thập', 'Giá thấp nhất', 'Giá cao nhất', 'Đơn vị'])
    )
    
    df = validate_data(df, "Pepper")
    
    output_dir = os.path.join("data", "silver")
    os.makedirs(output_dir, exist_ok=True)
    silver_path = os.path.join(output_dir, f"tieu_clean_{date_str}.csv")
    df.write_csv(silver_path, include_bom=True)

# ==========================================
# 5. Cashew Cleaner
# ==========================================
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
    df_raw = pl.read_csv(bronze_path, infer_schema_length=0)

    if 'Giá hôm nay' in df_raw.columns:
        df_raw = df_raw.rename({'Giá hôm nay': 'Giá mới'})
        
    # Tương thích ngược với file cũ (3 cột)
    if 'Phân loại / Khu vực' in df_raw.columns:
        df_raw = df_raw.rename({'Phân loại / Khu vực': 'Nông sản'})
    if 'Giá' in df_raw.columns:
        df_raw = df_raw.rename({'Giá': 'Giá mới'})

    # Cấu trúc mới: 4 cột: Nông sản, Giá cũ, Giá mới, Thay đổi
    df = (
        df_raw
        .with_columns([
            pl.lit('Hạt điều').alias('Loại nông sản'),
            # Trích xuất đơn vị từ ngoặc
            pl.col('Nông sản').str.extract(r'\(([^)]+)\)').alias('Đơn_vị_raw'),
            # Xóa đơn vị để lấy tên sạch
            pl.col('Nông sản').str.replace(r'\s*\([^)]*\)\s*$', '').str.strip_chars().alias('Tên_sạch'),
        ])
        # Lọc bỏ các dòng có đơn vị là USD/kg
        .filter(~pl.col('Đơn_vị_raw').str.contains('(?i)USD'))
        .with_columns([
            # Đơn vị chuẩn hóa (chỉ còn VNĐ)
            pl.lit('vnđ/kg').alias('Đơn vị'),
            # Phân hạng: trích xuất W-grade hoặc mặc định "Điều tươi"
            pl.when(pl.col('Tên_sạch').str.contains(r'(?i)W\d+'))
              .then(pl.col('Tên_sạch').str.extract(r'(W\d+)'))
              .otherwise(pl.lit('Điều tươi'))
              .alias('Phân hạng'),
            # Khu vực: cho điều tươi → tên tỉnh; cho nhân → Toàn thị trường
            pl.when(pl.col('Tên_sạch').str.contains(r'(?i)^Điều tươi'))
              .then(pl.col('Tên_sạch').str.replace(r'(?i)^Điều tươi\s+', '').str.strip_chars())
              .otherwise(pl.lit('Toàn thị trường'))
              .alias('Khu vực'),
        ])
        .with_columns(
            pl.col('Giá mới')
              .str.replace_all(r'\.', '')
              .str.extract_all(r'\d+')
              .cast(pl.List(pl.Float64))
              .alias('Mảng_số')
        )
        .with_columns([
            pl.col('Mảng_số').list.min().alias('Giá thấp nhất'),
            pl.col('Mảng_số').list.max().alias('Giá cao nhất'),
            pl.col('Khu vực').str.strip_chars().replace(REGION_MAP).alias('Khu vực'),
        ])
        .select(['Loại nông sản', 'Phân hạng', 'Khu vực', 'Ngày thu thập', 'Giá thấp nhất', 'Giá cao nhất', 'Đơn vị'])
    )
    
    df = validate_data(df, "Cashew")
    
    output_dir = os.path.join("data", "silver")
    os.makedirs(output_dir, exist_ok=True)
    silver_path = os.path.join(output_dir, f"hat_dieu_clean_{date_str}.csv")
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