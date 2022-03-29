FROM python:2.7-alpine

EXPOSE 8082
ENTRYPOINT [ "luigid" ]

RUN mkdir -p /luigi/state
VOLUME /luigi/state

RUN mkdir /etc/luigi
ADD /docker/staging-scheduler/etc/luigi/client.cfg /etc/luigi/
VOLUME /etc/luigi

WORKDIR /app/luigi

COPY . .
ENV PYTHONPATH /app/luigi:PYTHONPATH
RUN pip install -e .
