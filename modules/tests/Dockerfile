FROM adoptopenjdk:11-jre-hotspot as tests
WORKDIR /tests

ADD build/distributions/tests*.tar /tests

EXPOSE 8080

ENTRYPOINT ["bash", "-c", "/tests/tests*/bin/tests"]