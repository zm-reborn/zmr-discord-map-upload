FROM python:3.11

COPY requirements.txt map-upload.py /app/
WORKDIR /app

RUN pip install --no-cache-dir -r requirements.txt

CMD python ./map-upload.py
