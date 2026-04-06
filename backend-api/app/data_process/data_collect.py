import sys
import os

# Ensure 'backend-api/' is on path so app.* imports work when run as script
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

import pandas as pd
import schedule
import time
from vnstock import Vnstock
import logging
import argparse

# Thiết lập logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)


def save_to_db(df: pd.DataFrame) -> None:
    """Upsert company info DataFrame vào database."""
    from app.database import SessionLocal
    from app.models.company_info import CompanyInfo

    db = SessionLocal()
    try:
        # Xóa toàn bộ dữ liệu cũ rồi insert mới (full refresh)
        db.query(CompanyInfo).delete()
        records = df.rename(columns=str.lower).to_dict(orient='records')
        db.bulk_insert_mappings(CompanyInfo, records)
        db.commit()
        logging.info(f"Đã lưu {len(records)} bản ghi vào database!")
    except Exception as e:
        db.rollback()
        logging.error(f"Lỗi khi lưu dữ liệu vào database: {e}")
        raise
    finally:
        db.close()


# Hàm chính để thu thập dữ liệu và lưu vào database
def run_task():
    logging.info("Bắt đầu thu thập dữ liệu...")
    try:
        stock = Vnstock().stock(symbol='VN30F1M', source='VCI')
        logging.info("Đã khởi tạo đối tượng Vnstock.")
        stock_icb = stock.listing.symbols_by_industries()[['symbol', 'organ_name', 'icb_code1']]
        logging.info(f"Đã lấy danh sách ngành và mã chứng khoán theo ICB: {len(stock_icb)} dòng.")
        icb_all = stock.listing.industries_icb()[['icb_name', 'icb_code']]
        logging.info(f"Đã lấy dữ liệu các ngành ICB: {len(icb_all)} dòng.")
        result2 = pd.merge(stock_icb, icb_all, left_on='icb_code1', right_on='icb_code', how='inner')[['symbol', 'organ_name', 'icb_name']]
        logging.info(f"Đã tạo bảng kết quả ngành: {len(result2)} dòng.")

        def get_stocks_by_exchange(stock, exchange):
            symbols = stock.listing.symbols_by_group(exchange).tolist()
            logging.info(f"Đã lấy {len(symbols)} mã từ sàn {exchange}.")
            return pd.DataFrame({"symbol": symbols, "exchange": exchange})

        def get_all_stocks(stock):
            exchanges = ['HOSE', 'HNX', 'UPCOM']
            stock_dfs = [get_stocks_by_exchange(stock, exchange) for exchange in exchanges]
            return pd.concat(stock_dfs, ignore_index=True)

        all_stocks_df = get_all_stocks(stock)
        logging.info(f"Tổng số mã chứng khoán từ các sàn: {len(all_stocks_df)}.")
        result3 = pd.merge(result2, all_stocks_df, on='symbol', how='inner')
        logging.info(f"Kết quả cuối cùng sau khi ghép: {len(result3)} dòng.")
        save_to_db(result3)
        logging.info("Công việc đã hoàn thành và dữ liệu được cập nhật!")
    except Exception as e:
        logging.error(f"Lỗi trong quá trình thu thập dữ liệu: {e}")


# Chế độ chạy: ngay hoặc schedule
def main():
    parser = argparse.ArgumentParser(description="Thu thập dữ liệu chứng khoán.")
    parser.add_argument('--mode', choices=['now', 'schedule'], default='now', help='Chọn chế độ chạy: now (ngay) hoặc schedule (lập lịch)')
    args = parser.parse_args()
    if args.mode == 'now':
        run_task()
    else:
        logging.info("Thiết lập lịch chạy hàng ngày lúc 08:00...")
        schedule.every().day.at("08:00").do(run_task)
        while True:
            schedule.run_pending()
            time.sleep(1)


if __name__ == "__main__":
    main()
