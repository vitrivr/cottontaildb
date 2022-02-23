FROM openjdk:latest AS build

COPY . /cottontail-src
RUN cd /cottontail-src && \
  ./gradlew distTar && \
  cd ./cottontaildb-dbms/build/distributions/ && \
  tar xf ./cottontaildb-dbms.tar


FROM openjdk:latest

RUN mkdir /cottontaildb-data /cottontaildb-config
COPY config.json /cottontaildb-config/
COPY --from=build /cottontail-src/cottontaildb-dbms/build/distributions/cottontaildb-dbms /cottontaildb-dbms

EXPOSE 1865

ENTRYPOINT /cottontaildb-dbms/bin/cottontaildb /cottontaildb-config/config.json
