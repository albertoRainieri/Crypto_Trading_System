FROM mongo:4.4.6

ARG UID
ARG GID

RUN groupadd -g ${GID} mongo && \
    useradd -u ${UID} -g ${GID} -m -s /bin/bash mongo

RUN cp /etc/mongod.conf.orig /etc/mongod.conf
RUN sed -i 's/#security/security/' /etc/mongod.conf
RUN sed '/^security/a   authorization: "enabled"' /etc/mongod.conf
RUN sed -i 's/127.0.0.1/0.0.0.0/' /etc/mongod.conf
RUN sed -i 's/127.0.0.1/0.0.0.0/' /etc/mongod.conf.orig

USER mongo

CMD ["mongod"]

