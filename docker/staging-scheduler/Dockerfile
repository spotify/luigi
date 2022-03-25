FROM python:2.7-alpine

WORKDIR /app
RUN apk add curl

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

RUN mkdir -p /data
RUN curl -Ls https://github.com/foursquare/luigi/archive/5ce6a271df9b42c7da9dd4ffc94b805ba7932d2b.zip > /data/luigi.zip
RUN unzip /data/luigi.zip -d /data
RUN rm /data/luigi.zip
RUN mv /data/luigi-5ce6a271df9b42c7da9dd4ffc94b805ba7932d2b/ /app/luigi/
COPY . .

RUN mkdir /etc/luigi
ADD ./etc/luigi/client.cfg /etc/luigi/
VOLUME /etc/luigi

RUN mkdir -p /luigi/state
VOLUME /luigi/state

ENV PYTHONPATH /app/luigi:PYTHONPATH

EXPOSE 8082
ENTRYPOINT [ "./luigi/bin/luigid" ]