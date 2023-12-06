package org.vitrivr.cottontail.data

import kotlinx.serialization.Serializable
import org.vitrivr.cottontail.core.database.ColumnDef

/**
 * A [Manifest] record as used by [Dumper] and [Restorer].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
@Serializable
data class Manifest(val format: Format, val batchSize: Int, val entites: List<Entity> = mutableListOf(), val created: Long = System.currentTimeMillis()) {
    companion object {
        const val MANIFEST_FILE_NAME = "manifest.json"
    }

    @Serializable
    data class Entity(val name: String, val batches: Long, val size: Long, val columns: List<ColumnDef<*>>)

    @Serializable
    data class Index(val name: String)
}

