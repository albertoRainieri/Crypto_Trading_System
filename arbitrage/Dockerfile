FROM python:3.7

WORKDIR /arbitrage
COPY ./ /arbitrage
COPY ./requirements.txt /arbitrage

RUN apt-get update
RUN pip install --upgrade -r ./requirements.txt
RUN apt-get install vim -y
RUN apt install net-tools
RUN yes | apt-get install iputils-ping
RUN pip install "python-jose[cryptography]"


#CMD ["python3", "/arbitrage/binance-ws.py"]