import pandas as pd
from sqlalchemy import create_engine

# Kết nối tới database
engine = create_engine('postgresql+psycopg2://airflow:airflow@127.0.0.1:5432/airflow')

# Viết câu lệnh SQL của bạn ở đây
query = """
    SELECT 
        "Loại nông sản", 
        "Phân hạng", 
        "Khu vực", 
        "Giá trung bình", 
        "Ngày thu thập"
    FROM fact_agri_prices
    ORDER BY "Ngày thu thập" DESC, "Giá trung bình" DESC
    LIMIT 10;
"""

print(f"🔄 Đang truy vấn CSDL PostgreSQL...")
# Đọc dữ liệu từ SQL vào Pandas DataFrame
df = pd.read_sql(query, engine)

# In kết quả ra màn hình
print("\n✅ KẾT QUẢ TRUY VẤN (10 dòng gần nhất):")
print("-" * 80)
print(df.to_string(index=False))
print("-" * 80)
