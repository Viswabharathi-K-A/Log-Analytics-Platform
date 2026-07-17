FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install kafka-python-ng==2.2.3

COPY consumer.py .

EXPOSE 8001

CMD ["python", "consumer.py"]