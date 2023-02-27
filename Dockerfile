FROM alpine:3.17.1

RUN apk add --no-cache openjdk11 jattach --repository http://dl-cdn.alpinelinux.org/alpine/edge/community/

WORKDIR /application

ADD build/distributions/PoC-*.tar /application

EXPOSE 8080

ENTRYPOINT ["sh", "-c", "/application/PoC-*/bin/PoC"]
