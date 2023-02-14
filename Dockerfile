FROM python:3.11-slim

COPY requirements.txt requirements.txt 

RUN pip install --upgrade pip && \
  pip install --no-cache -r requirements.txt

WORKDIR /project/

COPY ./main.py ./main.py

CMD ["python", "./main.py"]
