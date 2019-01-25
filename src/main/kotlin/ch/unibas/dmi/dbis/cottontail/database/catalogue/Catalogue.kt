package ch.unibas.dmi.dbis.cottontail.database.schema.catalogue

import ch.unibas.dmi.dbis.cottontail.config.Config
import ch.unibas.dmi.dbis.cottontail.database.schema.Schema
import ch.unibas.dmi.dbis.cottontail.model.exceptions.DatabaseException
import org.mapdb.*

import org.mapdb.volume.MappedFileVol

import java.nio.file.Path

internal class Catalogue(config: Config) {

    /** Root to Cottontail DB root folder. */
    val path: Path = config.root

    /** The [StoreWAL] that contains the Cottontail DB catalogue. */
    val store: StoreWAL = try {
        StoreWAL.make(file = this.path.resolve(FILE_CATALOGUE).toString(), volumeFactory = MappedFileVol.FACTORY, fileLockWait = config.lockTimeout)
    } catch (e: DBException) {
        throw DatabaseException("Failed to open Cottontail DB catalogue: ${e.message}'.")
    }

    /** A map of all the [Schema]s contained in this [Catalogue]. */
    val schemas: HashMap<String, Schema> = HashMap()

    /**
     *
     */
    init {

    }



    /**
     * Companion object to [Catalogue]
     */
    companion object {
        /** Filename for the [Entity] catalogue.  */
        internal const val FILE_CATALOGUE = "cottontail.db"


    }
}