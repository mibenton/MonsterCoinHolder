import os
import requests
import logging
import gspread
import schedule
import time
import pandas as pd
from google.oauth2.service_account import Credentials
from datetime import datetime
from gspread_formatting import *

# ----------------------------
# 日誌設定
# ----------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ----------------------------
# Google Sheet 認證
# ----------------------------
SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]

# ⚠️ 改成你自己的檔案路徑 & Sheet ID
TOKEN_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "Token.json")
SHEET_ID = "1QYiJwT3wNh5xk5UWWlS8ha-YGmeB9mURyIUmKay1FLs"

if not os.path.exists(TOKEN_PATH):
    logging.error(f"找不到 Google 憑證檔案 {TOKEN_PATH}，請確認檔案存在！")
    exit(1)

creds = Credentials.from_service_account_file(TOKEN_PATH, scopes=SCOPES)
gc = gspread.authorize(creds)
spreadsheet = gc.open_by_key(SHEET_ID)

# ----------------------------
# 計算 Chaikin Money Flow (CMF)
# ----------------------------
def calculate_cmf(df, period=20):
    try:
        # 檢查數據量是否足夠
        if len(df) < period:
            logging.warning(f"CMF 計算失敗：數據量 {len(df)} 小於所需 {period} 根 K 線")
            return df
        # 處理 High == Low 的情況，避免除以零
        df['MFM'] = 0.0  # 預設 MFM 為 0
        valid_mask = df['High'] != df['Low']
        df.loc[valid_mask, 'MFM'] = ((df['Close'] - df['Low']) - (df['High'] - df['Close'])) / (df['High'] - df['Low'])
        df['MFV'] = df['MFM'] * df['Volume']
        df['CMF'] = df['MFV'].rolling(window=period).sum() / df['Volume'].rolling(window=period).sum()
        if df['CMF'].isna().all():
            logging.warning("CMF 計算結果全為 NaN，可能由於數據無效")
        return df
    except Exception as e:
        logging.warning(f"CMF 計算失敗: {e}")
        return df

# ----------------------------
# 抓 Binance Top30 成交量 + 淨流入 + CMF + 止損價
# ----------------------------
def scan_binance_netflow_top30():
    try:
        url = "https://fapi.binance.com/fapi/v1/ticker/24hr"
        resp = requests.get(url, timeout=10).json()
        usdt_perp = [item for item in resp if item["symbol"].endswith("USDT")]

        # 過濾成交額 >= 3億 USDT
        usdt_perp = [item for item in usdt_perp if float(item.get("quoteVolume", 0)) >= 300_000_000]

        # 排序取前 30
        top30 = sorted(usdt_perp, key=lambda x: float(x.get("quoteVolume", 0)), reverse=True)[:30]

        results = []
        now_time = datetime.now().strftime("%m-%d %H:%M:%S")

        for item in top30:
            sym = item["symbol"]
            # 抓取最近 20 根 1 小時 K 線數據以計算 CMF 和止損價（止損需 15 根）
            klines_url_cmf = f"https://fapi.binance.com/fapi/v1/klines?symbol={sym}&interval=1h&limit=20"
            # 抓取最近 24 根 1 小時 K 線數據以計算買入量、賣出量、淨流入
            klines_url_flow = f"https://fapi.binance.com/fapi/v1/klines?symbol={sym}&interval=1h&limit=24"
            try:
                # 獲取 CMF 和止損價 K 線數據
                klines_cmf = requests.get(klines_url_cmf, timeout=10).json()
                if not klines_cmf or len(klines_cmf) < 15:
                    logging.warning(f"Binance {sym} CMF/止損 K線數據不足（需至少15根）")
                    cmf_value = None
                    stop_loss = None
                else:
                    df_cmf = pd.DataFrame(klines_cmf, columns=[
                        "open_time", "Open", "High", "Low", "Close", "Volume",
                        "close_time", "quote_volume", "trades", "taker_buy_volume",
                        "taker_buy_quote_volume", "ignore"
                    ])
                    df_cmf = df_cmf.astype({
                        "Open": float, "High": float, "Low": float,
                        "Close": float, "Volume": float
                    }, errors='ignore')

                    # 計算 CMF
                    df_cmf = calculate_cmf(df_cmf, period=20)
                    cmf_value = df_cmf['CMF'].iloc[-1] if 'CMF' in df_cmf.columns and not df_cmf['CMF'].isna().iloc[-1] else None

                    # 計算止損價：掃描最近 15 根 K 棒，找第一根成交量 > 前一根 ×5 的 K 棒，低點 -5%
                    df_stop = df_cmf.tail(15)  # 取最近 15 根 K 棒
                    vol_spike = df_stop[df_stop['Volume'] > df_stop['Volume'].shift(1) * 5]
                    if not vol_spike.empty:
                        stop_loss = vol_spike['Low'].iloc[0] * 0.95  # 第一根成交量激增 K 棒的低點 -5%
                    else:
                        stop_loss = df_stop['Low'].min() * 0.95  # 15 根 K 棒的最低價 -5%
                    stop_loss = round(stop_loss, 2) if not pd.isna(stop_loss) else None

                    # 記錄 K 棒時間範圍和止損價
                    kline_time = datetime.fromtimestamp(df_cmf['open_time'].iloc[-1] / 1000).strftime("%m-%d %H:%M:%S")
                    logging.info(f"{sym} 最新 K 棒 ({kline_time}) 止損價: {stop_loss}")

                # 獲取 24 小時 K 線數據計算買入量、賣出量
                klines_flow = requests.get(klines_url_flow, timeout=10).json()
                if not klines_flow:
                    logging.warning(f"Binance {sym} 買賣量 K線數據為空")
                    continue
                df_flow = pd.DataFrame(klines_flow, columns=[
                    "open_time", "Open", "High", "Low", "Close", "Volume",
                    "close_time", "quote_volume", "trades", "taker_buy_volume",
                    "taker_buy_quote_volume", "ignore"
                ])
                df_flow = df_flow.astype({
                    "quote_volume": float,
                    "taker_buy_quote_volume": float
                }, errors='ignore')

                # 計算 24 小時的買入量、賣出量
                buy_vol = df_flow['taker_buy_quote_volume'].sum()  # 24 小時買入金額
                total_vol = df_flow['quote_volume'].sum()  # 24 小時總成交金額
                sell_vol = total_vol - buy_vol  # 賣出金額 = 總成交 - 買入
                if buy_vol > sell_vol:  # 只加入淨流入 > 0 的幣種
                    results.append({
                        "time": now_time,
                        "symbol": sym,
                        "price": float(item["lastPrice"]),
                        "amount": round(float(item["quoteVolume"]), 1),
                        "buy_vol": round(buy_vol, 1),
                        "sell_vol": round(sell_vol, 1),
                        "net_inflow": round(buy_vol - sell_vol, 1),
                        "cmf": round(cmf_value, 3) if cmf_value is not None else None,
                        "stop_loss": stop_loss  # 止損價
                    })
            except Exception as e:
                logging.warning(f"Binance {sym} K線數據抓取失敗: {e}")
                continue

        # 依照淨流入排序 (大→小)
        results = sorted(results, key=lambda x: x["net_inflow"], reverse=True)
        return results
    except Exception as e:
        logging.error(f"Binance Netflow 抓取失敗: {e}")
        return []

# ----------------------------
# 更新 Google Sheet - 新增 I 欄止損價
# ----------------------------
def update_google_sheet_binance_netflow(results: list):
    month_title = datetime.now().strftime("%Y-%m-Binance-NetInflow")
    try:
        try:
            ws = spreadsheet.worksheet(month_title)
        except gspread.exceptions.WorksheetNotFound:
            ws = spreadsheet.add_worksheet(title=month_title, rows=2000, cols=10)  # 增加 cols 到 10 以容納 I 欄
            # 初始化第一列和第二列空白
            ws.update('A1:I2', [
                [""] * 9,  # 第一列空白（9 欄）
                [""] * 9   # 第二列空白（9 欄）
            ])

        # 準備插入的資料（標題 + 數據 + 空白列）
        insert_rows = []
        # 插入標題（新增止損價）
        insert_rows.append(["時間", "幣種", "價格", "24h成交額", "買入量", "賣出量", "淨流入", "CMF", "止損價"])

        # 如果 results 為空，插入一筆帶有 N/A 的記錄
        if not results:
            now_time = datetime.now().strftime("%m-%d %H:%M:%S")
            insert_rows.append([now_time, "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A"])
            logging.info("無符合幣種，寫入 N/A 記錄")
        else:
            # 插入數據
            for r in results:
                insert_rows.append([
                    r["time"], r["symbol"], r["price"], r["amount"],
                    r["buy_vol"], r["sell_vol"], r["net_inflow"],
                    r["cmf"] if r["cmf"] is not None else "",
                    r["stop_loss"] if r["stop_loss"] is not None else ""  # 止損價到 I 欄
                ])
                # 根據 CMF 值記錄買賣壓力
                if r["cmf"] is not None and r["cmf"] > 0:
                    logging.info(f"{r['symbol']} CMF = {r['cmf']:.3f} > 0，買方 > 賣方，幣價可能漲！止損價: {r['stop_loss']}")
        
        # 數據後空白列
        insert_rows.append([""] * 9)  # 9 欄空白

        # 插入到第3列，舊數據（包括舊標題）被推到下方
        ws.insert_rows(insert_rows, 3)

        # 設置 D、E、F、G 欄（24h成交額、買入量、賣出量、淨流入）的數字格式為 #0.00,,"M"
        # 新增 I 欄的數字格式（止損價用標準貨幣格式）
        format_range_amount = f"D4:G{len(results) + 4 if results else 4}"
        number_format_amount = CellFormat(
            numberFormat=NumberFormat(type='NUMBER', pattern='#,##0.00,,\"M\"')
        )
        format_cell_range(ws, format_range_amount, number_format_amount)

        format_range_stop = f"I4:I{len(results) + 4 if results else 4}"
        number_format_stop = CellFormat(
            numberFormat=NumberFormat(type='NUMBER', pattern='#,##0.00')
        )
        format_cell_range(ws, format_range_stop, number_format_stop)

        logging.info(f"已寫入 {len(results) if results else 0} 筆 Binance 淨流入資料 到 {month_title}（含止損價）")
    except Exception as e:
        logging.error(f"寫入 Google Sheet (Binance NetInflow) 失敗: {e}")

# ----------------------------
# 主排程
# ----------------------------
def job():
    logging.info("🚀 Binance 淨流入掃描開始...")
    results = scan_binance_netflow_top30()
    update_google_sheet_binance_netflow(results)
    logging.info("✅ Binance 淨流入更新完成")

if __name__ == "__main__":
    # 第一次立即執行
    job()

    # 每小時整點執行
    schedule.every().hour.at(":00").do(job)

    logging.info("⌛ 排程已啟動，每小時整點執行 Binance 淨流入更新")
    while True:
        schedule.run_pending()
        time.sleep(1)
