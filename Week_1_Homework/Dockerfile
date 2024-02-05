FROM python:3.9

RUN apt-get install wget
RUN pip install pandas psycopg2 sqlalchemy pyarrow fastparquet

WORKDIR /app

COPY ingest_data.py ingest_data.py
COPY ingest.ipynb ingest.ipynb 

ENTRYPOINT [ "python", "ingest_data.py" ]
# ENTRYPOINT [ "bash" ]
