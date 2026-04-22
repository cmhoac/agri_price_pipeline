import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
import sys
import logging
from datetime import datetime

# ==========================================
# 1. Cấu hình Logging chuyên nghiệp
# ==========================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(levelname)s] - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("Scraper")

# ==========================================
# 2. Base Scraper (OOP)
# ==========================================
class AgriScraper:
    def __init__(self, run_date):
        self.run_date = run_date
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }

    def get_soup(self, url):
        """Hàm tải HTML từ web chung cho mọi lớp"""
        logger.info(f"Đang truy cập: {url}")
        try:
            response = requests.get(url, headers=self.headers, timeout=15)
            if response.status_code == 200:
                return BeautifulSoup(response.text, 'html.parser')
            else:
                logger.error(f"Lỗi truy cập {url}! Mã lỗi: {response.status_code}")
                return None
        except Exception as e:
            logger.error(f"Lỗi kết nối tới {url}: {str(e)}")
            return None

    def save_to_bronze(self, data, columns, prefix_name):
        """Hàm lưu dữ liệu ra file CSV ở tầng Bronze"""
        if not data:
            logger.warning(f"Không có dữ liệu để lưu cho {prefix_name}!")
            return

        df = pd.DataFrame(data, columns=columns)
        df['Ngày thu thập'] = self.run_date
        
        output_dir = os.path.join("data", "bronze")
        os.makedirs(output_dir, exist_ok=True)
        
        file_path = os.path.join(output_dir, f"{prefix_name}_raw_{self.run_date}.csv")
        df.to_csv(file_path, index=False, encoding='utf-8-sig')
        
        logger.info(f"🎉 THÀNH CÔNG! Đã lưu {len(df)} dòng dữ liệu cho {prefix_name}.")
        logger.info(f"📁 Đường dẫn: {file_path}")

    def run_generic_scraper(self, url, prefix_name, expected_cols, columns_names, is_cashew=False):
        """Logic scrape chung cho các bảng HTML dạng tr > td"""
        soup = self.get_soup(url)
        if not soup: return

        table = soup.find('table')
        if not table:
            logger.error(f"Không tìm thấy bảng dữ liệu trên {url}!")
            return
            
        tbody = table.find('tbody')
        rows = tbody.find_all('tr') if tbody else table.find_all('tr')[1:] 
        
        data = []
        for row in rows:
            cols = row.find_all('td')
            if len(cols) == expected_cols:
                # Bỏ qua các dòng tiêu đề nhóm (có colspan) trong Hạt Điều
                if is_cashew and cols[0].has_attr('colspan'):
                    continue
                
                row_data = [col.text.strip() for col in cols]
                data.append(row_data)
                
        self.save_to_bronze(data, columns_names, prefix_name)

# ==========================================
# 3. Main Execution
# ==========================================
if __name__ == "__main__":
    if len(sys.argv) > 1:
        run_date = sys.argv[1]
    else:
        run_date = datetime.now().strftime('%Y-%m-%d')

    logger.info(f"BẮT ĐẦU CHẠY PIPELINE TẦNG BRONZE CHO NGÀY: {run_date}...")
    
    scraper = AgriScraper(run_date)
    
    # Sầu Riêng
    scraper.run_generic_scraper(
        url='https://banggianongsan.com/bang-gia-sau-rieng/',
        prefix_name='sau_rieng',
        expected_cols=4,
        columns_names=["Nông sản", "Giá cũ", "Giá mới", "Thay đổi"]
    )
    
    # Tiêu
    scraper.run_generic_scraper(
        url='https://banggianongsan.com/bang-gia-tieu/',
        prefix_name='tieu',
        expected_cols=4,
        columns_names=["Nông sản", "Giá cũ", "Giá mới", "Thay đổi"]
    )
    
    # Hạt Điều
    scraper.run_generic_scraper(
        url='https://banggianongsan.com/bang-gia-hat-dieu/',
        prefix_name='hat_dieu',
        expected_cols=3,
        columns_names=["Phân loại / Khu vực", "Giá", "Thay đổi (+/-)"],
        is_cashew=True
    )