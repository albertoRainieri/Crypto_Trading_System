FROM python:3.7

ARG UID
ARG GID

WORKDIR /backend
COPY ./ /backend
COPY ./requirements.txt /backend

RUN groupadd -g ${GID} backend && \
    useradd -u ${UID} -g ${GID} -m -s /bin/bash backend

RUN apt-get update
RUN pip install --upgrade -r ./requirements.txt
RUN apt-get install vim -y
RUN apt install net-tools
RUN yes | apt-get install iputils-ping
RUN pip install "python-jose[cryptography]"

USER backend

CMD ["/bin/bash", "/backend/scripts/docker_wrapper_script.sh"]