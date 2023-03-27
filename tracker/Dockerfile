FROM python:3.7

ARG UID
ARG GID

WORKDIR /tracker
COPY ./ /tracker
COPY ./requirements.txt /tracker


RUN groupadd -g ${GID} tracker && \
    useradd -u ${UID} -g ${GID} -m -s /bin/bash tracker

RUN apt-get update
RUN pip install --upgrade -r ./requirements.txt
RUN apt-get install vim -y
RUN apt install net-tools
RUN yes | apt-get install iputils-ping

USER tracker

CMD ["/bin/bash", "/tracker/scripts/docker_wrapper_script.sh"]