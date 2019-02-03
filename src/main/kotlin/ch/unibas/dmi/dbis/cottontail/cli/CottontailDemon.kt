package ch.unibas.dmi.dbis.cottontail.cli

import ch.unibas.dmi.dbis.cottontail.config.Config


import com.google.gson.GsonBuilder

import java.nio.file.Files
import java.nio.file.Paths

class CottontailDemon : Runnable {
    private val config: String? = null

    override fun run() {
        Files.newBufferedReader(Paths.get(this.config)).use { reader ->
            val gson = GsonBuilder().create()
            val config = gson.fromJson(reader, Config::class.java)
        }
    }
}
