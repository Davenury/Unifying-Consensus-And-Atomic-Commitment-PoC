FROM alpine:3.17.1 AS unpacker

COPY build/distributions/PoC-*.zip /app.zip

RUN unzip /app.zip -d /application

FROM alpine:3.17.1

RUN apk add --no-cache openjdk11 jattach --repository http://dl-cdn.alpinelinux.org/alpine/edge/community/

WORKDIR /application

COPY --from=unpacker /application /application

EXPOSE 8080

ENTRYPOINT ["/application/bin/PoC"]
