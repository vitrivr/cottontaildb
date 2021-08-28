package org.vitrivr.cottontail

import kotlinx.serialization.json.Json
import org.vitrivr.cottontail.cli.Cli
import org.vitrivr.cottontail.config.Config
import org.vitrivr.cottontail.database.catalogue.DefaultCatalogue
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

    /* Order for config: 1) ENV 2) first argument 3) default */
    var config: Config? = null
    var configPath = ""
    var readErr = false
    var readErrCause: Throwable? = null
    /* Load config from ENV specification */
    try {
        configPath = System.getenv(COTTONTAIL_CONFIG_FILE_ENV_KEY)
        config = loadConfig(configPath)
    } catch (e: FileNotFoundException) {
        // Ignore
    } catch (r: RuntimeException) {
        readErr = true
        readErrCause = r.cause
    }
    if (readErr) {
        if (readErrCause != null) {
            System.err.println("Config reading error: ${readErrCause.message}")
        }
        System.err.println("Error on reading Cottontail DB configuration file ($configPath). Cottontail DB will shut down.")
        exitProcess(1)
    }
    /* Load config from argument OR default */
    if (config == null) {
        configPath = if (args.isEmpty()) {
            /* No arguments given, use default */
            System.err.println("No config path specified; using default config at ./config.json.")
            "./config.json"
        } else {
            /* Arguments present, try first argument */
            args.first()
        }
        try {
            config = loadConfig(configPath)
        } catch (e: FileNotFoundException) {
            System.err.println("Specified Cottontail DB configuration file $configPath does not exist. Cottontail DB will shut down.")
            exitProcess(1)
        } catch (r: RuntimeException) {
            if (readErrCause != null) {
                System.err.println("Config reading error: ${readErrCause.message}")
            }
            System.err.println("Error on reading Cottontail DB configuration file ($configPath). Cottontail DB will shut down.")
            exitProcess(1)
        }
    }

    /* Try to start Cottontail DB */
    try {
        standalone(config)
    } catch (e: Throwable) {
        System.err.println("Failed to start Cottontail DB due to error:")
        System.err.println(e.printStackTrace())
        exitProcess(1)
    }
}

/**
 * Loads (i.e. reads and parses) the [Config] from the path specified.
 * @throws FileNotFoundException In case the specified path is not a regular file
 * @throws RuntimeException In case an error occurs during parsing / reading
 * @return The parsed [Config] ready to be used.
 */
private fun loadConfig(path: String): Config {
    val configPath = Paths.get(path)
    /* Check if file is regular and exists */
    if (!Files.isRegularFile(configPath)) {
        throw FileNotFoundException("Cottontail DB configuration file $configPath des not exist.")
    }
    /* Check if loading works */
    try {
        return Files.newBufferedReader(configPath).use {
            return@use Json.decodeFromString(Config.serializer(), it.readText())
        }
    } catch (e: Throwable) {
        throw RuntimeException("Could not read Cottontail DB configuration file", e)
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

    /* Create root-folder, if it doesn't exist yet. */
    if (!Files.exists(config.root)) {
        Files.createDirectories(config.root)
    }

    /* Check catalogue version. */
    val detected = VersionProber(config).probe(config.root)
    if (detected != VersionProber.EXPECTED && detected != DBOVersion.UNDEFINED) {
        System.err.println("Version mismatch: Trying to open an incompatible Cottontail DB catalogue version. Run system migration to upgrade catalogue (detected = $detected, expected = ${VersionProber.EXPECTED}, path = ${config.root}).")
        exitProcess(1)
    }

    /* Instantiate Catalogue, execution engine and gRPC server. */
    val catalogue = DefaultCatalogue(config)

    /* Start gRPC Server and print message. */
    val server = CottontailGrpcServer(config, catalogue)
    server.start()
    println(
        "Cottontail DB server is up and running at port ${config.server.port}! Hop along... (catalogue: ${catalogue.version}, pid: ${
            ProcessHandle.current().pid()
        })"
    )

    /* Start CLI (if configured). */
    if (config.cli) {
        Cli("localhost", config.server.port).loop()
        server.stop()
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

    /* Print shutdown message. */
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

    /* Instantiate Catalogue, execution engine and gRPC server. */
    val catalogue = DefaultCatalogue(config)
    val server = CottontailGrpcServer(config, catalogue)

    /* Start gRPC Server and return object. */
    server.start()
    return server
}
