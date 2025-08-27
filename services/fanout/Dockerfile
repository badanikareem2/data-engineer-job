FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir kafka-python==2.0.2 redis==5.0.4 requests==2.32.3
COPY fanout.py /app/fanout.py
CMD ["python", "-u", "fanout.py"]
