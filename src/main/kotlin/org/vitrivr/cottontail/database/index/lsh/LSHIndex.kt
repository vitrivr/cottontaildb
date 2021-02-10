package org.vitrivr.cottontail.database.index.lsh

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.index.Index
import org.vitrivr.cottontail.database.index.IndexType
import org.vitrivr.cottontail.model.values.types.VectorValue
import org.vitrivr.cottontail.utilities.extensions.write
import org.vitrivr.cottontail.utilities.math.KnnUtilities
import java.nio.file.Path

abstract class LSHIndex<T : VectorValue<*>>(path: Path, parent: Entity) : Index(path, parent) {

    /** Index-wide constants. */
    companion object {
        const val LSH_MAP_FIELD = "cdb_lsh_map"
    }

    /** The [LSHIndex] implementation returns exactly the columns that is indexed. */
    final override val produces: Array<ColumnDef<*>> =
        arrayOf(KnnUtilities.queryIndexColumnDef(this.name.entity()))

    /** The type of [Index] */
    override val type: IndexType = IndexType.LSH

    /** Flag indicating if this [LSHIndex] has been closed. */
    @Volatile
    final override var closed: Boolean = false
        private set

    /**
     * Closes this [SuperBitLSHIndex] and the associated data structures.
     */
    final override fun close() = this.closeLock.write {
        if (!this.closed) {
            this.db.close()
            this.closed = true
        }
    }
}