FROM python:2.7-alpine

EXPOSE 8082
ENTRYPOINT [ "luigid" ]

RUN mkdir -p /luigi/state
VOLUME /luigi/state

RUN mkdir /etc/luigi
WORKDIR /app/luigi

COPY . .

RUN pip install -e .
