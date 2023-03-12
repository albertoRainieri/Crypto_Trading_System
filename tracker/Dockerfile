FROM python:3.7

WORKDIR /tracker
COPY ./ /tracker
COPY ./requirements.txt /backend

RUN apt-get update
RUN pip install --upgrade -r ./requirements.txt
RUN apt-get install vim -y
RUN apt install net-tools
RUN yes | apt-get install iputils-ping
RUN mkdir logs

CMD ["python3", "/tracker/scripts/main.py"]