package ch.unibas.dmi.dbis.cottontail.database.schema

import ch.unibas.dmi.dbis.cottontail.config.Config
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger

import java.io.IOException

class Schema
/**
 * Constructor for [Schema].
 *
 * @param config [Config] from which to create [Schema].
 * @throws IOException
 */
@Throws(IOException::class)
constructor(config: Config) {
    companion object {
        /** Filename for the [Schema] catalogue.  */
        private val FILE_CATALOGUE = "catalogue.mapdb"

        /** Filename for the [Schema] catalogue.  */
        private val PROPERTY_ENTITIES = "entities"

        /** Property name for the [Schema] version.  */
        private val PROPERTY_VERSION = "version"

        /** Property name for the last time this [Schema] was opened.  */
        private val PROPERTY_LAST_OPEN = "opened"

        /** Property name for the last time this [Schema] modified.  */
        private val PROPERTY_LAST_MODIFIED = "modified"

        /** Property name for the date this [Schema] created.  */
        private val PROPERTY_CREATED = "created"

        /** The [Logger] instance used to log errors and information.  */
        private val LOGGER = LogManager.getLogger()
    }
}
