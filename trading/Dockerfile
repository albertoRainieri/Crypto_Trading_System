FROM python:3.7

ARG UID
ARG GID

WORKDIR /trading
COPY ./ /trading
COPY ./requirements.txt /trading


RUN groupadd -g ${GID} trading && \
    useradd -u ${UID} -g ${GID} -m -s /bin/bash trading

RUN apt-get update
RUN pip install --upgrade -r ./requirements.txt
RUN apt-get install vim -y
RUN apt install net-tools
RUN yes | apt-get install iputils-ping

USER trading

CMD ["/bin/bash", "/trading/scripts/docker_wrapper_script.sh"]