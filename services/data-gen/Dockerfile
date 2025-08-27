FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir psycopg2-binary==2.9.9
COPY generator.py .
ENV PYTHONUNBUFFERED=1
CMD ["python", "generator.py"]
