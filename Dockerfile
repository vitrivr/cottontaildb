FROM zenika/kotlin:1.4.20-jdk11 AS build

COPY . /cottontail-src
RUN cd /cottontail-src && \
  ./gradlew distTar && \
  mkdir cottontaildb-bin && \
  cd cottontaildb-bin && \
  tar xf ../build/distributions/cottontaildb-bin.tar


FROM zenika/kotlin:1.4.20-jdk11-slim

RUN mkdir /cottontaildb-data /cottontaildb-config
COPY config.json /cottontaildb-config/
COPY --from=build /cottontail-src/cottontaildb-bin /

EXPOSE 1865

ENTRYPOINT /cottontaildb-bin/bin/cottontaildb /cottontaildb-config/config.json
