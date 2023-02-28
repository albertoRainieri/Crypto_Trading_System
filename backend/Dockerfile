FROM python:3.7

WORKDIR /backend
COPY ./ /backend
COPY ./requirements.txt /backend

RUN apt-get update
RUN pip install --upgrade -r ./requirements.txt
RUN apt-get install vim -y
RUN apt install net-tools
RUN yes | apt-get install iputils-ping
RUN pip install "python-jose[cryptography]"
RUN mkdir logs

#CMD ["uvicorn", "main:app", "--reload", "--host", "0.0.0.0", "--port", "5000"]
CMD ["/bin/bash", "/backend/scripts/docker_wrapper_script.sh"]