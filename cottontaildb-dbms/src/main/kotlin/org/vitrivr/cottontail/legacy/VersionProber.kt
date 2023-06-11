package org.vitrivr.cottontail.legacy

import org.vitrivr.cottontail.config.Config
import org.vitrivr.cottontail.dbms.general.DBOVersion
import java.nio.file.Files
import java.nio.file.Path
import kotlin.time.ExperimentalTime

/**
 * A helper class that can be used to probe a Cottontail DB instance's [DBOVersion].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class VersionProber(val config: Config) {

    companion object {
        /** The current DB version of Cottontail DB. */
        val EXPECTED = DBOVersion.V3_0
    }

    /**
     * Probes the given Cottontail DB instance and returns its [DBOVersion].
     *
     * @return [DBOVersion] or [DBOVersion.UNDEFINED], if the path does not contain a valid Cottontail DB catalogue.
     */
    fun probe(): DBOVersion = when {
        isV3() -> DBOVersion.V3_0
        else -> DBOVersion.UNDEFINED
    }

    /**
     * Probes the given Cottontail DB instance and returns its [DBOVersion].
     *
     * @return [DBOVersion] or [DBOVersion.UNDEFINED], if the path does not contain a valid Cottontail DB catalogue.
     */
    @ExperimentalTime
    fun migrate() {
        when {
            isV3() -> TODO()
            else -> throw IllegalStateException("Cannot migrate Cottontail DB from DB version ${probe()}. Try an older Cottontail DB version.")
        }
    }

    /**
     * Checks if the database under the given [Path] is a [DBOVersion.V3_0] database.
     *
     * TODO: When V3 changes structurally, then we need a more fine-grained check here!
     *
     * @return True if database is [DBOVersion.V3_0], false otherwise.
     */
    private fun isV3(): Boolean = Files.isDirectory(config.root.resolve("xodus"))
}