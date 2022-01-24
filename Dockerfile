FROM openjdk:latest AS build

COPY . /cottontail-src
RUN cd /cottontail-src && \
  ./gradlew distTar && \
  mkdir cottontaildb-dbms && \
  cd cottontaildb-dbms && \
  tar xf ../build/cottontaildb-dbms/distributions/cottontaildb-dbms.tar


FROM openjdk:latest

RUN mkdir /cottontaildb-data /cottontaildb-config
COPY config.json /cottontaildb-config/
COPY --from=build /cottontail-src/cottontaildb-bin /

EXPOSE 1865

ENTRYPOINT /cottontaildb-dbms/bin/cottontaildb /cottontaildb-config/config.json
