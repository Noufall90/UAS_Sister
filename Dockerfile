FROM python:3.11-slim

WORKDIR /app

# Buat non-root user
RUN adduser --disabled-password --gecos '' appuser

# Copy requirements dan install dependencies
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY src/ ./src/

# Setup permissions
RUN chown -R appuser:appuser /app && \
    mkdir -p /app/data && \
    chmod -R 755 /app/data

USER appuser
EXPOSE 8080

WORKDIR /app
CMD ["python", "-m", "uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8080"]
