FROM gradle:jdk17 AS build

COPY --chown=gradle:gradle . /cottontail-src
WORKDIR /cottontail-src
RUN gradle --no-daemon distTar
WORKDIR /cottontail-src/cottontaildb-dbms/build/distributions/
RUN tar xf ./cottontaildb-dbms.tar

FROM eclipse-temurin:17-jre

RUN mkdir /cottontaildb-data /cottontaildb-config
COPY config.json /cottontaildb-config/
COPY --from=build /cottontail-src/cottontaildb-dbms/build/distributions/cottontaildb-dbms /cottontaildb-dbms

EXPOSE 1865

ENTRYPOINT /cottontaildb-dbms/bin/cottontaildb /cottontaildb-config/config.json
