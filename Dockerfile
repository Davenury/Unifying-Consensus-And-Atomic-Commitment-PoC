FROM alpine:3.17.1

RUN apk add openjdk11 && apk add --no-cache jattach --repository http://dl-cdn.alpinelinux.org/alpine/edge/community/ && apk add redis

WORKDIR /application

ADD build/distributions/PoC-*.tar /application

EXPOSE 8080

ENTRYPOINT ["sh", "-c", "while ! redis-cli ping; do sleep 1 ; done && /application/PoC-*/bin/PoC"]
