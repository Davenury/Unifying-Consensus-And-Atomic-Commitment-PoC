FROM adoptopenjdk:11-jre-hotspot as application
WORKDIR /application

ADD build/distributions/PoC-*.tar /application

EXPOSE 8080

ENTRYPOINT ["bash", "-c", "/application/PoC-*/bin/PoC"]
