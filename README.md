# Cottontail DB
Cottontail DB is a column store aimed at multimedia retrieval. It allows for classical boolean as well as vector-space retrieval (k-nearest-neighbours lookup) used in similarity search.

## Setup
Please clone this repository including submodules or initialize them after cloning using `git submodule update --init --recursive`

Before executing or building Cottontail DB, you need to generate Proto sources using the Gradle task `generateProto`. An executable distribution of Cottontail DB can then be built using the 
Gradle tasks `distTar` or `distZip`. Distributions will be stored relative to the project root in `build/distributions`.

### Start Cottontail DB Server
Cottontail DB Server can be started using the distribution generated as described before `bin/cottontaildb` or `bin/cottontaildb.bat`. It requires a path to a valid configuration file as a program argument.

### Configuration
All the configuration of Cottontail DB is done by means of a single configuration file. See `config.json` for structure of such a file. Most importantly, the file should contain at least the following
parameters:

* __root__: Path to the root directory used by Cottontail DB. The catalogue and all the data will be stored in this location. Hence, there must be enough space and Cottontail DB must be allowed to read and write it.
* __forceUnmapMappedFiles__: Must be set to __true__ on Windows Systems and __false__ otherwise.

Remaining parameters will be documented in a future version of this file. Check `ch.unibas.dmi.dbis.cottontail.config` package for code documentation of the configuration parameters.

## Credit
Cottontail DB is based on the ideas presented in the following papers:

- Ivan Giangreco and Heiko Schuldt (2016): ADAMpro: Database Support for Big Multimedia Retrieval. Datenbank-Spektrum.
http://link.springer.com/article/10.1007/s13222-015-0209-y
- Ivan Giangreco (2018): Database support for large-scale multimedia retrieval. PhD Thesis. https://edoc.unibas.ch/64751/
