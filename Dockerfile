FROM adoptopenjdk:11-jre-hotspot as application
WORKDIR /application

ADD build/distributions/PoC-*.tar /application

EXPOSE 8080

ENTRYPOINT ["bash", "-c", "/application/PoC-*/bin/PoC"]

###

FROM adoptopenjdk:11-jre-hotspot as tests
WORKDIR /tests

ADD modules/tests/build/distributions/tests*.tar /tests

EXPOSE 8080

ENTRYPOINT ["bash", "-c", "/tests/tests*/bin/tests"]
