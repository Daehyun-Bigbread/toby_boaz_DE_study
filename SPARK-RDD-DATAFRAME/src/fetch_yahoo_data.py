# 하루 치 data ingestion 코드

from datetime import datetime, timedelta
import os
import pandas as pd
import yfinance as yf

TICKERS = ["AAPL", "MSFT", "GOOG", "TSLA", "NVDA"]


def fetch_raw_for_date(target_date_str: str):
    target_date = datetime.strptime(target_date_str, "%Y-%m-%d").date()
    next_date = target_date + timedelta(days=1)

    all_rows = []

    for ticker in TICKERS:
        df = yf.download(
            ticker,
            start=target_date.isoformat(),
            end=next_date.isoformat(),
            interval="1d",
            progress=False,
            auto_adjust=False
        )

        if df.empty:
            print(f"[WARN] No data for {ticker} on {target_date_str}")
            continue

        # MultiIndex 컬럼이면 1단계 평탄화
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        df = df.reset_index()

        # 필요한 컬럼만 선택
        df = df[["Date", "Open", "High", "Low", "Close", "Volume"]].copy()

        # Ticker 컬럼 추가
        df["Ticker"] = ticker

        # 컬럼 순서 정리
        df = df[["Date", "Ticker", "Open", "High", "Low", "Close", "Volume"]]

        all_rows.append(df)

    if not all_rows:
        print(f"[SKIP] No data downloaded for {target_date_str}")
        return

    result = pd.concat(all_rows, ignore_index=True)

    os.makedirs("data/raw", exist_ok=True)

    output_path = f"data/raw/{target_date_str}.csv"
    result.to_csv(output_path, index=False)

    print(f"Saved raw data to {output_path}")
    print(result)



if __name__ == "__main__":
# 함수 호출 : fetch_raw_for_date("수집할 날짜 str")
    fetch_raw_for_date("2026-03-10") # 화

# 이전에 수집한 데이터 "2026-03-03"~"2026-03-08"
    # 수 fetch_raw_for_date("2026-03-04")
    # 목 fetch_raw_for_date("2026-03-05") 
    # 금 fetch_raw_for_date("2026-03-06") 
    # ❎ fetch_raw_for_date("2026-03-07")
    # ❎ fetch_raw_for_date("2026-03-08")
    # 월 fetch_raw_for_date("2026-03-09")