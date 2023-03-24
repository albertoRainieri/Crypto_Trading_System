FROM ubuntu/nginx

WORKDIR /app

RUN groupadd bot 
RUN useradd -g bot bot 

COPY ./conf/default /etc/nginx/sites-available/default
RUN apt-get update
# RUN apt-get -y install vim
# RUN yes | apt-get install iputils-ping
RUN mkdir /etc/nginx/ssl && chown -R bot:bot /etc/nginx/ssl

RUN chown -R bot:bot /app && chmod -R 755 /app && \
        chown -R bot:bot /var/log/nginx && \
        chown -R bot:bot /var/lib/nginx && \
        chown -R bot:bot /etc/nginx/sites-available
RUN touch /var/run/nginx.pid && \
        chown -R bot:bot /var/run/nginx.pid

#USER bot