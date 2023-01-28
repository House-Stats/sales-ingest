FROM python:3.9.7

WORKDIR /app

COPY ./ingester .
COPY ./requiremnts.txt .


RUN python3 -m pip install --upgrade pip setuptools wheel
RUN python3 -m pip install confluent_kafka asyncpg

CMD ["python3", "__init__.py"]