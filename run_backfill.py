import os
import shutil
import sys

print("🔄 BẮT ĐẦU CHIẾN DỊCH HỒI TỐ DỮ LIỆU (BACKFILL)...")

# 1. CHIẾN LƯỢC WIPE: Dọn sạch "Bóng ma dữ liệu" cũ ở Silver và Gold
print("🧹 Đang dọn dẹp các file rác cũ trong kho...")
for folder in ["data/silver", "data/gold"]:
    if os.path.exists(folder):
        shutil.rmtree(folder) # Xóa toàn bộ thư mục và file bên trong
    os.makedirs(folder)       # Tạo lại thư mục trống mới tinh

# 2. Quét vào thư mục bronze để đếm xem có bao nhiêu ngày đã cào
dates = set()
for file in os.listdir("data/bronze"):
    if file.endswith(".csv"):
        # Cắt lấy chuỗi ngày từ tên file (vd: 2026-03-26)
        date_str = file.split("_")[-1].replace(".csv", "")
        dates.add(date_str)

print(f"Phát hiện {len(dates)} ngày cần tái tạo...")

# 3. CHIẾN LƯỢC REBUILD: Cho máy chạy tự động lại toàn bộ pipeline
for d in sorted(dates):
    print(f"\n--- Đang tái tạo dữ liệu cho ngày {d} ---")
    os.system(f"{sys.executable} src/transform/cleaner.py {d}")
    os.system(f"{sys.executable} src/transform/gold_maker.py {d}")

print("\n✅ HOÀN TẤT BACKFILL TOÀN BỘ LỊCH SỬ! KHO DỮ LIỆU ĐÃ SẠCH SẼ 100%.")