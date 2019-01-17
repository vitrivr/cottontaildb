package ch.unibas.dmi.dbis.cottontail.cli

import ch.unibas.dmi.dbis.cottontail.config.Config

import com.github.rvesse.airline.annotations.Command
import com.github.rvesse.airline.annotations.Option

import com.google.gson.GsonBuilder

import java.nio.file.Files
import java.nio.file.Paths

@Command(name = "cottontd", description = "Starts the Cottontail DB demon.")
class CottontailDemon : Runnable {
    @Option(name = arrayOf("-c", "--config"), title = "Config", description = "Path to the Cottontail DB configuration file.")
    private val config: String? = null

    override fun run() {
        Files.newBufferedReader(Paths.get(this.config)).use { reader ->
            val gson = GsonBuilder().create()
            val config = gson.fromJson(reader, Config::class.java)
        }
    }
}
