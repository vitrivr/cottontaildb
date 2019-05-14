# Cottontail DB
Cottontail DB is a column store aimed at multimedia retrieval. It allows for classical boolean as well as vector-space retrieval (k-nearest-neighbours lookup) used in similarity search.

## Setup
Please clone this repository including submodules or initialize them after cloning using `git submodule update --init --recursive`
Then, generate Proto sources using the gradle task `generateProto` and antlr sources using `generateGrammarSource`


## Credit
Cottontail DB is based on the ideas presented in the following papers:

- Ivan Giangreco and Heiko Schuldt (2016): ADAMpro: Database Support for Big Multimedia Retrieval. Datenbank-Spektrum.
http://link.springer.com/article/10.1007/s13222-015-0209-y
- Ivan Giangreco (2018): Database support for large-scale multimedia retrieval. PhD Thesis. https://edoc.unibas.ch/64751/
