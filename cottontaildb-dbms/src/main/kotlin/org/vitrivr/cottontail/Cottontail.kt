package org.vitrivr.cottontail

import jdk.incubator.vector.*
import kotlinx.coroutines.Dispatchers
import kotlinx.serialization.json.Json
import org.vitrivr.cottontail.config.Config
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.general.DBOVersion
import org.vitrivr.cottontail.legacy.VersionProber
import org.vitrivr.cottontail.server.CottontailServer
import java.io.FileNotFoundException
import java.nio.file.Files
import java.nio.file.Paths
import java.util.*
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
 * Entry point for Cottontail DB demon.
 *
 * @param args Program arguments.
 */
@ExperimentalTime
fun main(args: Array<String>) {
    /* Try to start Cottontail DB */
    try {
        val config: Config = loadConfig(findConfigPathOrdered(args))

        /* Check for SIMD support, if flag has been set. */
        if (config.execution.simd) {
            try {
                println("Cottontail DB is running with SIMD extensions through the Java Vector API. This feature is experimental!")
                println("(Byte) Preferred: ${ByteVector.SPECIES_MAX}. Maximum: ${ByteVector.SPECIES_MAX}")
                println("(Short) Preferred: ${ShortVector.SPECIES_MAX}. Maximum: ${ShortVector.SPECIES_MAX}")
                println("(Int) Preferred: ${IntVector.SPECIES_MAX}. Maximum: ${IntVector.SPECIES_MAX}")
                println("(Long) Preferred: ${LongVector.SPECIES_MAX}. Maximum: ${LongVector.SPECIES_MAX}")
                println("(Float) Preferred: ${FloatVector.SPECIES_MAX}. Maximum: ${FloatVector.SPECIES_MAX}t")
                println("(Double) Preferred: ${DoubleVector.SPECIES_MAX}. Maximum: ${DoubleVector.SPECIES_MAX}")
            } catch (e: NoClassDefFoundError) {
                System.err.println("Failed to start Cottontail DB due to error: No support for Java Vector API. Please unset 'execution.simd' flag in config.")
                exitProcess(1)
            }
        }

        standalone(config)
    } catch (e: Throwable) {
        System.err.println("Failed to start Cottontail DB due to error:")
        System.err.println(e.printStackTrace())
        exitProcess(1)
    }

    Dispatchers.Default
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
    val server = try {
        embedded(config)
    } catch (e: DatabaseException.VersionMismatchException) {
        if (migrate(config, e.found, e.expected)) {
            println("Cottontail DB upgrade to ${e.expected} was successful. Please restart Cottontail DB now!")
            return
        } else {
            throw e
        }
    }

    println(
        "Cottontail DB server is up and running at port ${config.server.port}! Hop along... (catalogue: ${DBOVersion.current()}, pid: ${
            ProcessHandle.current().pid()
        })"
    )

    /* Start CLI (if configured); wait for gRPC server to be stopped. */
    val scanner = Scanner(System.`in`)
    loop@ while (server.isRunning) {
        if (scanner.hasNext()) {
            when (scanner.nextLine()) {
                "exit", "quit", "stop" -> break
            }
        }
        Thread.sleep(250)
    }

    /* Stop server and print shutdown message. */
    server.shutdownAndWait()
    println("Cottontail DB was shut down. Have a binky day!")
}

/**
 * Cottontail DB server startup method for embedded mode.
 *
 * @param config The [Config] object to start Cottontail DB with.
 * @return [CottontailServer] instance.
 */
@ExperimentalTime
fun embedded(config: Config): CottontailServer {
    /* Create root-folder, if it doesn't exist yet. */
    if (!Files.exists(config.root)) {
        Files.createDirectories(config.root)
    }

    /* Check catalogue version. */
    val detected = VersionProber(config).probe()
    if (detected != VersionProber.EXPECTED && detected != DBOVersion.UNDEFINED) {
        throw DatabaseException.VersionMismatchException(VersionProber.EXPECTED, detected)
    }

    /* Start gRPC Server and return it. */
    return CottontailServer(config)
}

/**
 * Tries to migrate the Cottontail DB version.
 */
@ExperimentalTime
fun migrate(config: Config, from: DBOVersion, to: DBOVersion): Boolean {
    /* Start CLI (if configured); wait for gRPC server to be stopped. */
    val scanner = Scanner(System.`in`)
    println("Cottontail DB detected a version mismatch (expected: $to, found: $from). Would you like to perform a upgrade (y/n)?")
    while (true) {
        when (scanner.nextLine().lowercase()) {
            "yes", "y" -> {
                VersionProber(config).migrate()
                return true
            }
            "no", "n" -> return false
        }
        Thread.sleep(100)
    }
}