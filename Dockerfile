FROM openjdk:17 AS build

COPY . /cottontail-src
WORKDIR /cottontail-src
RUN ./gradlew distTar
WORKDIR /cottontail-src/cottontaildb-dbms/build/distributions/
RUN tar xf ./cottontaildb-dbms.tar

FROM openjdk:17

RUN mkdir /cottontaildb-data /cottontaildb-config
COPY config.json /cottontaildb-config/
COPY --from=build /cottontail-src/cottontaildb-dbms/build/distributions/cottontaildb-dbms /cottontaildb-dbms

EXPOSE 1865

ENTRYPOINT /cottontaildb-dbms/bin/cottontaildb /cottontaildb-config/config.json
