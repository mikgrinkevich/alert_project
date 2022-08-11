FROM python:3.10

COPY requirements.txt requirements.txt
COPY main.py main.py
COPY send_mail.py send_mail.py
COPY config.py config.py
COPY data data

RUN pip install -r requirements.txt

CMD ["python", "./main.py"] 