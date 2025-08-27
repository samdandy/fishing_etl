FROM python:3.11-slim

COPY . .

RUN apt-get update

RUN apt-get -y clean

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "main.py"]