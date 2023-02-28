FROM mongo:4.4.6

RUN cp /etc/mongod.conf.orig /etc/mongod.conf
RUN sed -i 's/#security/security/' /etc/mongod.conf
RUN sed '/^security/a   authorization: "enabled"' /etc/mongod.conf
RUN sed -i 's/127.0.0.1/0.0.0.0/' /etc/mongod.conf
RUN sed -i 's/127.0.0.1/0.0.0.0/' /etc/mongod.conf.orig


CMD ["mongod"]

