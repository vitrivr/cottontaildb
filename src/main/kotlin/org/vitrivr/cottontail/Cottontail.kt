package org.vitrivr.cottontail

import kotlinx.serialization.json.Json
import org.vitrivr.cottontail.cli.Cli
import org.vitrivr.cottontail.config.Config
import org.vitrivr.cottontail.database.general.DBOVersion
import org.vitrivr.cottontail.legacy.VersionProber
import org.vitrivr.cottontail.model.exceptions.DatabaseException
import org.vitrivr.cottontail.server.grpc.CottontailGrpcServer
import java.io.BufferedReader
import java.io.FileNotFoundException
import java.io.InputStreamReader
import java.nio.file.Files
import java.nio.file.Paths
import kotlin.system.exitProcess
import kotlin.time.ExperimentalTime


/**
 * The environment variable key for the config file
 */
const val COTTONTAIL_CONFIG_FILE_ENV_KEY = "COTTONTAIL_CONFIG"

/**
 * The system property key for the config file path
 */
const val COTTONTAIL_CONFIG_FILE_SYSTEM_PROPERTY_KEY = "org.vitrivr.cottontail.config"


/**
 * Entry point for Cottontail DB demon and CLI.
 *
 * @param args Program arguments.
 */
@ExperimentalTime
fun main(args: Array<String>) {
    /** Check if only CLI should be started. */
    if (args.isNotEmpty() && args[0] in arrayOf("prompt", "cli")) {
        if (args.size < 3) {
            println("Starting CLI on default setup")
            Cli().loop()
            return
        }
        Cli(args[1], args[2].toInt()).loop()
        return
    }

    /* Try to start Cottontail DB */
    try {
        val config: Config = loadConfig(findConfigPathOrdered(args))
        standalone(config)
    } catch (e: Throwable) {
        System.err.println("Failed to start Cottontail DB due to error:")
        System.err.println(e.printStackTrace())
        exitProcess(1)
    }
}

/**
 * Looks in the following places for a config path (ordered, fifo):
 *   1. The first program argument
 *   2. System property with the key [COTTONTAIL_CONFIG_FILE_SYSTEM_PROPERTY_KEY]
 *   3. Environment variable with the name [COTTONTAIL_CONFIG_FILE_ENV_KEY]
 *   4. Literally `./config.json`, i.e., in the CWD
 *
 * @param args The command line arguments array as it was passed to the program
 * @return The path to use, based on the described order
 */
private fun findConfigPathOrdered(args: Array<String>): String {
    return if (args.isNotEmpty() && args[0].isNotBlank()) {
        args[0]
    } else if (System.getProperties()
            .containsKey(COTTONTAIL_CONFIG_FILE_SYSTEM_PROPERTY_KEY) && System.getProperty(
            COTTONTAIL_CONFIG_FILE_SYSTEM_PROPERTY_KEY, ""
        ).isNotBlank()
    ) {
        System.getProperty(COTTONTAIL_CONFIG_FILE_SYSTEM_PROPERTY_KEY)
    } else if (System.getenv().containsKey(COTTONTAIL_CONFIG_FILE_ENV_KEY) && System.getenv(
            COTTONTAIL_CONFIG_FILE_ENV_KEY
        ).isNotBlank()
    ) {
        System.getenv(COTTONTAIL_CONFIG_FILE_ENV_KEY)
    } else {
        System.err.println("No CottontailDB Config file specified. Defaulting to ./config.json")
        "./config.json"
    }
}

/**
 * Tries to load (i.e.n read and parse) the [Config] from the path specified and creates a default [Config] file if none exists.
 *
 * @throws FileNotFoundException In case the specified path is not a regular file
 * @throws RuntimeException In case an error occurs during parsing / reading
 * @return The parsed [Config] ready to be used.
 */
private fun loadConfig(path: String): Config {
    val configPath = Paths.get(path)
    try {
        if (!Files.isRegularFile(configPath)) {
            System.err.println("No CottontailDB config exists under $configPath; trying to create default config!")
            val config = Config()
            Files.newBufferedWriter(configPath).use { it.write(Json.encodeToString(Config.serializer(), config)) }
            return config
        } else {
            return Files.newBufferedReader(configPath).use {
                return@use Json.decodeFromString(Config.serializer(), it.readText())
            }
        }
    } catch (e: Throwable) {
        System.err.println("Could not load Cottontail DB configuration file under $configPath. Cottontail DB will shutdown!")
        e.printStackTrace()
        exitProcess(1)
    }
}

/**
 * Traditional Cottontail DB server startup method (standalone).
 *
 * @param config The [Config] object to start Cottontail DB with.
 */
@ExperimentalTime
fun standalone(config: Config) {
    /* Prepare Log4j logging facilities. */
    if (config.logConfig != null && Files.isRegularFile(config.logConfig)) {
        System.getProperties().setProperty("log4j.configurationFile", config.logConfig.toString())
    }

    /* Start gRPC Server and print message. */
    val server = embedded(config)
    println(
        "Cottontail DB server is up and running at port ${config.server.port}! Hop along... (catalogue: ${server.catalogue.version}, pid: ${
            ProcessHandle.current().pid()
        })"
    )

    /* Start CLI (if configured). */
    if (config.cli) {
        Cli("localhost", config.server.port).loop()
    } else {
        /* Wait for gRPC server to be stopped. */
        val obj = BufferedReader(InputStreamReader(System.`in`))
        loop@ while (server.isRunning) {
            when (obj.readLine()) {
                "exit", "quit", "stop" -> break
            }
            Thread.sleep(100)
        }
    }

    /* Stop server and print shutdown message. */
    server.stop()
    println("Cottontail DB was shut down. Have a binky day!")
}

/**
 * Cottontail DB server startup method for embedded mode.
 *
 * @param config The [Config] object to start Cottontail DB with.
 * @return [CottontailGrpcServer] instance.
 */
@ExperimentalTime
fun embedded(config: Config): CottontailGrpcServer {
    /* Create root-folder, if it doesn't exist yet. */
    if (!Files.exists(config.root)) {
        Files.createDirectories(config.root)
    }

    /* Check catalogue version. */
    val detected = VersionProber(config).probe(config.root)
    if (detected != VersionProber.EXPECTED && detected != DBOVersion.UNDEFINED) {
        throw DatabaseException.VersionMismatchException(VersionProber.EXPECTED, detected)
    }

    /* Start gRPC Server and return it. */
    return CottontailGrpcServer(config)
}
