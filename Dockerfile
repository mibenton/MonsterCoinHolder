# 使用官方 Python 環境
FROM python:3.11-slim

# 設定工作目錄
WORKDIR /app

# 複製並安裝依賴
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 複製應用程式檔案
COPY . .

# 暴露 8080 端口
EXPOSE 8080

# 設定環境變數
ENV PORT=8080

# 預設執行
CMD ["python", "MonsterCoinHolder.py"]
