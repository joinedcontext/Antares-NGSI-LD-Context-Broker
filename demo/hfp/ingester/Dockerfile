FROM python:3.15-slim
RUN pip install --no-cache-dir paho-mqtt requests
COPY ingester.py /app/ingester.py
USER 65534:65534
CMD ["python", "-u", "/app/ingester.py"]
