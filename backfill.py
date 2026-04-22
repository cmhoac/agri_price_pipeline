import os
import re
from src.transform.cleaner import clean_durian_data, clean_pepper_data, clean_cashew_data
from src.transform.gold_maker import create_gold_fact_table

print("BẮT ĐẦU SỬA LỖI TỪ NGÀY 20/03 TRỞ ĐI...")

# 1. Quét tìm tất cả các file
bronze_files = os.listdir("data/bronze")
dates = set()
for f in bronze_files:
    match = re.search(r'\d{4}-\d{2}-\d{2}', f)
    if match:
        date_str = match.group()
        # CHỈ ĐỊNH RÕ: Chỉ lấy các ngày từ 20/03/2026 trở về hiện tại
        if date_str >= '2026-03-20':
            dates.add(date_str)

# 2. Vòng lặp cỗ máy thời gian (Đã giới hạn ngày)
for d in sorted(list(dates)):
    print(f"\n{'='*20} ĐANG XỬ LÝ LẠI NGÀY {d} {'='*20}")
    
    # Dùng try-except để nếu một loại nông sản bị lỗi (do thiếu file), các loại khác vẫn chạy tiếp
    try: clean_durian_data(d)
    except Exception as e: print(f"Bỏ qua Sầu riêng ngày {d}: {e}")
        
    try: clean_pepper_data(d)
    except Exception as e: print(f"Bỏ qua Tiêu ngày {d}: {e}")
        
    try: clean_cashew_data(d)
    except Exception as e: print(f"Bỏ qua Hạt điều ngày {d}: {e}")
        
    # Chạy tầng Gold
    create_gold_fact_table(d)

print("\n🎉 BACKFILL HOÀN TẤT! DỮ LIỆU TỪ 20/03 ĐÃ ĐƯỢC CHUẨN HÓA.")