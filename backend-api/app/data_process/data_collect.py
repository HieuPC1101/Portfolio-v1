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

# Rate limit community: 60 req/min → 1 req/sec, đặt 1.1s để an toàn
_REQUEST_INTERVAL = 1.1


def register_api_key() -> None:
    """Đăng ký API key vnstock từ biến môi trường."""
    api_key = os.getenv('VNSTOCK_API_KEY')
    if not api_key:
        logging.warning("VNSTOCK_API_KEY không được đặt. Chạy với giới hạn Guest (20 req/min).")
        return
    try:
        from vnstock.core.utils.auth import register_user
        success = register_user(api_key=api_key)
        if success:
            logging.info("Đã đăng ký API key vnstock thành công (60 req/min).")
        else:
            logging.warning("Đăng ký API key thất bại, tiếp tục với Guest tier.")
    except Exception as e:
        logging.warning(f"Không thể đăng ký API key: {e}")


def save_to_db(df: pd.DataFrame) -> None:
    """Upsert company info DataFrame vào database."""
    from app.database import SessionLocal
    from app.models.company_info import CompanyInfo

    db = SessionLocal()
    try:
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


def fill_missing_icb(result: pd.DataFrame) -> pd.DataFrame:
    """Fetch icb_name từ company.overview() cho các mã còn thiếu (sequential + rate limit)."""
    missing_symbols = result[result['icb_name'].isna()]['symbol'].tolist()
    if not missing_symbols:
        return result

    logging.info(f"Đang fetch ngành cho {len(missing_symbols)} mã thiếu (ước ~{len(missing_symbols) * _REQUEST_INTERVAL / 60:.1f} phút)...")
    icb_map: dict = {}

    for i, symbol in enumerate(missing_symbols, 1):
        try:
            s = Vnstock().stock(symbol=symbol, source='VCI')
            overview = s.company.overview()
            if overview is not None and not overview.empty and 'icb_name3' in overview.columns:
                val = overview['icb_name3'].iloc[0]
                if pd.notna(val) and str(val).strip():
                    icb_map[symbol] = str(val).strip()
        except Exception:
            pass

        if i % 50 == 0:
            logging.info(f"  [{i}/{len(missing_symbols)}] Tìm được {len(icb_map)} ngành.")

        time.sleep(_REQUEST_INTERVAL)

    logging.info(f"Bổ sung được {len(icb_map)}/{len(missing_symbols)} ngành từ company.overview().")
    result = result.copy()
    result.loc[result['symbol'].isin(icb_map), 'icb_name'] = (
        result.loc[result['symbol'].isin(icb_map), 'symbol'].map(icb_map)
    )
    return result


# Hàm chính để thu thập dữ liệu và lưu vào database
def run_task(fill_missing: bool = False) -> None:
    logging.info("Bắt đầu thu thập dữ liệu...")
    try:
        stock = Vnstock().stock(symbol='VN30F1M', source='VCI')
        logging.info("Đã khởi tạo đối tượng Vnstock.")

        # Lấy danh sách mã theo sàn, chỉ lấy cổ phiếu (type='stock')
        exchange_df = stock.listing.symbols_by_exchange()
        exchange_df = exchange_df[
            (exchange_df['exchange'].isin(['HOSE', 'HNX', 'UPCOM'])) &
            (exchange_df['type'] == 'stock')
        ][['symbol', 'organ_name', 'exchange']].reset_index(drop=True)
        logging.info(f"Đã lấy {len(exchange_df)} mã cổ phiếu từ HOSE/HNX/UPCOM.")

        # Lấy danh sách ngành: symbols_by_industries() chỉ cover ~698 mã HOSE
        industry_df = stock.listing.symbols_by_industries()[['symbol', 'industry_name']]
        logging.info(f"symbols_by_industries() trả về {len(industry_df)} mã có ngành.")

        result = pd.merge(exchange_df, industry_df, on='symbol', how='left')
        result = result.rename(columns={'industry_name': 'icb_name'})
        result = result[['symbol', 'organ_name', 'icb_name', 'exchange']]

        nan_count = result['icb_name'].isna().sum()
        logging.info(f"Sau merge: {len(result) - nan_count} có ngành, {nan_count} chưa có ngành.")

        if fill_missing and nan_count > 0:
            result = fill_missing_icb(result)
            remaining = result['icb_name'].isna().sum()
            logging.info(f"Sau fill: {remaining} mã vẫn không có ngành (để NULL).")

        save_to_db(result)
        logging.info("Công việc đã hoàn thành và dữ liệu được cập nhật!")
    except Exception as e:
        logging.error(f"Lỗi trong quá trình thu thập dữ liệu: {e}")


# Chế độ chạy: ngay hoặc schedule
def main():
    parser = argparse.ArgumentParser(description="Thu thập dữ liệu chứng khoán.")
    parser.add_argument('--mode', choices=['now', 'schedule'], default='now',
                        help='Chọn chế độ chạy: now (ngay) hoặc schedule (lập lịch)')
    parser.add_argument('--fill-missing', action='store_true',
                        help='Fetch ngành từ company.overview() cho mã còn thiếu (chậm hơn)')
    args = parser.parse_args()

    register_api_key()

    if args.mode == 'now':
        run_task(fill_missing=args.fill_missing)
    else:
        logging.info("Thiết lập lịch chạy hàng ngày lúc 08:00...")
        schedule.every().day.at("08:00").do(run_task, fill_missing=args.fill_missing)
        while True:
            schedule.run_pending()
            time.sleep(1)


if __name__ == "__main__":
    main()
