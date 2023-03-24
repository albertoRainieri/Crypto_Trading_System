FROM ubuntu/nginx


COPY ./conf/default /etc/nginx/sites-available/default
RUN apt-get update
# RUN apt-get -y install vim
# RUN yes | apt-get install iputils-ping
RUN mkdir /etc/nginx/ssl && chown -R nginx:nginx /etc/nginx/ssl

WORKDIR /app

RUN chown -R nginx:nginx /app && chmod -R 755 /app && \
        chown -R nginx:nginx /var/log/nginx && \
        chown -R nginx:nginx /var/lib/nginx && \
        chown -R nginx:nginx /etc/nginx/sites-available
RUN touch /var/run/nginx.pid && \
        chown -R nginx:nginx /var/run/nginx.pid

USER nginx