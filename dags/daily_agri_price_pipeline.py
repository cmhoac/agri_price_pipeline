from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'congm',                   
    'depends_on_past': False,           
    'email': ['congminhhoac@gmail.com'], 
    'email_on_failure': True, 
    'email_on_retry': False,
    'retries': 2,                       
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'daily_agri_price_pipeline',
    default_args=default_args,
    description='Pipeline tự động thu thập và làm sạch giá Nông sản',
    schedule='0 2 * * *',               
    start_date=datetime(2026, 3, 1),    
    catchup=False,                      
    tags=['agri_project', 'scraping'],
) as dag:

    # SỬ DỤNG GIỜ VẬT LÝ THỰC TẾ (REAL-TIME) THAY VÌ GIỜ LOGIC CỦA AIRFLOW
    real_time_date = "{{ (macros.datetime.utcnow() + macros.timedelta(hours=7)).strftime('%Y-%m-%d') }}"

    extract_bronze_data = BashOperator(
        task_id='extract_bronze_data',
        bash_command=f"cd /opt/airflow && python src/extract/scraper.py {real_time_date}",
        dag=dag,
    )

    transform_silver_data = BashOperator(
        task_id='transform_silver_data',
        bash_command=f"cd /opt/airflow && python src/transform/cleaner.py {real_time_date}",
        dag=dag,
    )

    create_gold_reporting_data = BashOperator(
        task_id='create_gold_reporting_data',
        bash_command=f"cd /opt/airflow && python src/transform/gold_maker.py {real_time_date}",
        dag=dag,
    )

    send_success_email = EmailOperator(
        task_id='send_success_email',
        to='congminhhoac@gmail.com',
        subject=f"✅ [Agri-Pipeline] Cập nhật dữ liệu ngày {real_time_date} thành công!",
        html_content=f"""
        <h3 style="color: green;">Báo cáo Hệ thống Data Pipeline Nông Sản</h3>
        <p>Hệ thống Airflow đã chạy thành công luồng dữ liệu cho ngày: <b>{real_time_date}</b>.</p>
        <ul>
            <li>Bước 1: Cào dữ liệu thô (Bronze) - Hoàn tất.</li>
            <li>Bước 2: Xử lý Polars đa luồng (Silver) - Hoàn tất.</li>
            <li>Bước 3: Hợp nhất Fact Table (Gold) - Hoàn tất.</li>
        </ul>
        <p>Vui lòng mở file Power BI Desktop và bấm Refresh để xem biểu đồ biến động giá mới nhất.</p>
        <hr>
        <p><i>Hệ thống tự động gửi lúc {{{{ (macros.datetime.utcnow() + macros.timedelta(hours=7)).strftime('%d/%m/%Y %H:%M:%S') }}}} (Giờ VN)</i></p>
        """
    )
    
    extract_bronze_data >> transform_silver_data >> create_gold_reporting_data >> send_success_email