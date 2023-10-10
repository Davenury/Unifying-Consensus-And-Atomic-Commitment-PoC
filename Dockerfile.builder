### if we change project name we should also change any place where PoC appears

FROM gradle:7.4.2-jdk11-alpine AS builder
COPY --chown=gradle:gradle . /home/gradle/src
WORKDIR /home/gradle/src
RUN gradle build --info --no-daemon -x test

COPY modules ./modules
RUN gradle installDist --no-daemon

###

FROM alpine:3.17.1 as application
RUN apk add openjdk11 && apk add --no-cache jattach --repository http://dl-cdn.alpinelinux.org/alpine/edge/community/ && apk add redis

WORKDIR /application

COPY --from=builder /home/gradle/src/build/install/PoC .

ENTRYPOINT ["sh", "-c", "/application/bin/PoC"]

###

FROM alpine:3.17.1 as tests
RUN apk add openjdk11 && apk add --no-cache jattach --repository http://dl-cdn.alpinelinux.org/alpine/edge/community/

WORKDIR /tests

COPY --from=builder /home/gradle/src/modules/tests/build/install/tests .

ENTRYPOINT ["sh", "-c", "/tests/bin/tests"]

###

FROM alpine:3.17.1 as checker
RUN apk add openjdk11 && apk add --no-cache jattach --repository http://dl-cdn.alpinelinux.org/alpine/edge/community/

WORKDIR /checker

COPY --from=builder /home/gradle/src/modules/checker/build/install/checker .

ENTRYPOINT ["sh", "-c", "/checker/bin/checker"]
