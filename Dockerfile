FROM openjdk:latest AS build

COPY . /cottontail-src
RUN cd /cottontail-src && \
  ./gradlew distTar && \
  mkdir cottontaildb-bin && \
  cd cottontaildb-bin && \
  tar xf ../build/cottontaildb-dbms/distributions/cottontaildb-dbms.tar


FROM openjdk:latest

RUN mkdir /cottontaildb-data /cottontaildb-config
COPY config.json /cottontaildb-config/
COPY --from=build /cottontail-src/cottontaildb-bin /

EXPOSE 1865

ENTRYPOINT /cottontaildb-bin/bin/cottontaildb /cottontaildb-config/config.json
