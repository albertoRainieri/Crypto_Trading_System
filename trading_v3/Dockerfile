FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY telegram_news.py .

# Run as non-root
RUN useradd -m appuser
USER appuser

CMD ["python", "-u", "telegram_news.py"]
