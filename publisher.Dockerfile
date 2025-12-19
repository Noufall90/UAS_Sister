FROM python:3.11-slim

WORKDIR /app

RUN adduser --disabled-password --gecos '' appuser

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY publisher/ ./publisher/

RUN chown -R appuser:appuser /app

USER appuser
EXPOSE 8081

CMD ["python", "-m", "publisher.main"]
