FROM python:2.7

RUN sed '/st_mysql_options options;/a unsigned int reconnect;' /usr/include/mysql/mysql.h -i.bkp
RUN pip install mysql-python
RUN pip install PyMySQL
RUN pip install sqlalchemy==1.4.32

EXPOSE 8082
ENTRYPOINT [ "luigid" ]

RUN mkdir -p /luigi/state
VOLUME /luigi/state

RUN mkdir /etc/luigi
WORKDIR /app/luigi

COPY . .

RUN pip install -e .
