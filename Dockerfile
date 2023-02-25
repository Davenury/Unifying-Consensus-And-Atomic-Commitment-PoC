FROM alpine:3.17.1

RUN apk add openjdk11 && apk add --no-cache jattach --repository http://dl-cdn.alpinelinux.org/alpine/edge/community/ && apk add redis

WORKDIR /application

ADD build/distributions/PoC-*.tar /application

EXPOSE 8080

ENTRYPOINT ["sh", "-c", "if [ \"$config_persistence_type\" = \"REDIS\" ] ; then while ! redis-cli -h $config_persistence_redisHost -p $config_persistence_redisPort ping; do sleep 1 ; done ; fi && sh /application/bin/PoC"]
