package org.vitrivr.cottontail.legacy

import org.mapdb.CottontailStoreWAL
import org.mapdb.DB
import org.vitrivr.cottontail.config.Config
import org.vitrivr.cottontail.dbms.general.DBOVersion
import org.vitrivr.cottontail.legacy.v1.MigrationManagerV1
import org.vitrivr.cottontail.legacy.v1.catalogue.CatalogueV1
import org.vitrivr.cottontail.legacy.v1.catalogue.CatalogueV1Header
import org.vitrivr.cottontail.legacy.v2.MigrationManagerV2
import org.vitrivr.cottontail.legacy.v2.catalogue.CatalogueV2
import org.vitrivr.cottontail.legacy.v2.catalogue.CatalogueV2Header
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
        isV2() -> DBOVersion.V2_0
        isV1() -> DBOVersion.V1_0
        else -> DBOVersion.UNDEFINED
    }

    /**
     * Probes the given Cottontail DB instance and returns its [DBOVersion].
     *
     * @return [DBOVersion] or [DBOVersion.UNDEFINED], if the path does not contain a valid Cottontail DB catalogue.
     */
    @ExperimentalTime
    fun migrate(batchSize: Int, log: Path) {
        when {
            isV2() -> MigrationManagerV2(batchSize, log).migrate(this.config)
            isV1() -> MigrationManagerV1(batchSize, log).migrate(this.config)
            else -> throw IllegalStateException("Cannot migrate Cottontail DB from DB version ${probe()}.")
        }
    }


    /**
     * Checks if the database under the given [Path] is a [DBOVersion.V1_0] database.
     *
     * @return True if database is [DBOVersion.V1_0], false otherwise.
     */
    private fun isV1(): Boolean {
        /* Required because old store instances may still be around. This is due to how MapDB handled failure when creating a store. */
        System.gc()

        /* The [StoreWAL] that contains the Cottontail DB catalogue. */
        val store: CottontailStoreWAL? = try {
            config.mapdb.store(config.root.resolve(CatalogueV1.FILE_CATALOGUE))
        } catch (e: Throwable) {
            return false
        }

        val header = try {
            store?.get(CatalogueV1.HEADER_RECORD_ID, CatalogueV1Header.Serializer)
        } catch (e: Throwable) {
            store?.close()
            return false
        }

        /* Close store. */
        store?.close()
        return header != null
    }

    /**
     * Checks if the database under the given [Path] is a [DBOVersion.V2_0] database.
     *
     * @return True if database is [DBOVersion.V2_0], false otherwise.
     */
    private fun isV2(): Boolean {
        /* Required because old store instances may still be around. This is due to how MapDB handled failure when creating a store. */
        System.gc()

        /* The [StoreWAL] that contains the Cottontail DB catalogue. */
        val store: DB? = try {
            config.mapdb.db(config.root.resolve(CatalogueV2.FILE_CATALOGUE))
        } catch (e: Throwable) {
            return false
        }

        val header = try {
            store?.atomicVar(CatalogueV2.CATALOGUE_HEADER_FIELD, CatalogueV2Header.Serializer)?.open()?.get()
        } catch (e: Throwable) {
            store?.close()
            return false
        }

        /* Close store. */
        store?.close()
        return header != null
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