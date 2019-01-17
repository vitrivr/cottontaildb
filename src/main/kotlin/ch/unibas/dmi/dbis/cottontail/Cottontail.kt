package ch.unibas.dmi.dbis.cottontail

import ch.unibas.dmi.dbis.cottontail.cli.CottontailDemon
import com.github.rvesse.airline.annotations.Cli

@Cli(name = "Cottontail", description = "Cottontail DB CLI.", defaultCommand = CottontailDemon::class, commands = arrayOf(CottontailDemon::class))
object Cottontail {
    /**
     *
     * @param args
     */
    @JvmStatic
    fun main(args: Array<String>) {
        val cli = com.github.rvesse.airline.Cli<Runnable>(Cottontail::class.java)
        val cmd = cli.parse(*args)
        cmd.run()
    }
}
