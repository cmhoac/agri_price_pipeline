import duckdb

# Câu lệnh SQL thần thánh trỏ thẳng vào thư mục chứa nhiều file CSV
query = """
    SELECT 
        *
    FROM read_parquet('data/gold/fact_agri_prices_*.parquet') 
"""
# Thực thi và in kết quả ra màn hình cực đẹp
print("Đang quét toàn bộ dữ liệu lịch sử...")
result = duckdb.query(query).show()