FROM adoptopenjdk:11-jre-hotspot
WORKDIR /application

ADD build/distributions/PoC-*.tar /application

EXPOSE 8080

ENTRYPOINT ["bash", "-c", "/application/PoC-*/bin/PoC"]
