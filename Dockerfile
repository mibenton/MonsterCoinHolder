# 使用官方 Python 環境
FROM python:3.11-slim

# 設定工作目錄
WORKDIR /app

# 複製檔案
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# 預設執行
CMD ["python", "MonsterCoinHolder.py"]
