### if we change project name we should also change any place where PoC appears

FROM gradle:7.4.2-jdk11-alpine AS builder
COPY --chown=gradle:gradle . /home/gradle/src
WORKDIR /home/gradle/src
RUN gradle build --info --no-daemon -x test

RUN gradle installDist --no-daemon

###

FROM adoptopenjdk:11-jre-hotspot
WORKDIR /application

COPY --from=builder /home/gradle/src/build/install/PoC .

ENTRYPOINT ["bash", "-c", "/application/bin/PoC"]
