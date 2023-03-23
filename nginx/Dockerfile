FROM ubuntu/nginx


COPY ./conf/default /etc/nginx/sites-available/default
RUN apt-get update
RUN apt-get -y install vim
RUN yes | apt-get install iputils-ping
RUN mkdir /etc/nginx/ssl