FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    duckdb \
    boto3 \
    pandas \
    pyarrow \
    requests

COPY src/ src/

ENTRYPOINT ["python", "src/main.py"]
