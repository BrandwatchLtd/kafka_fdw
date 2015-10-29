FROM ubuntu

RUN apt-get update
RUN apt-get install --assume-yes python-pip
RUN pip install --upgrade pip distribute
RUN pip install kafka-python

COPY load.py /load.py
COPY data.json /data.json

CMD ["python", "-u", "load.py"]
