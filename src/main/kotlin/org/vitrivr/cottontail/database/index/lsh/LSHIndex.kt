package org.vitrivr.cottontail.database.index.lsh

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.entity.DefaultEntity
import org.vitrivr.cottontail.database.index.AbstractIndex
import org.vitrivr.cottontail.database.index.IndexType
import org.vitrivr.cottontail.model.values.types.VectorValue
import org.vitrivr.cottontail.utilities.math.KnnUtilities
import java.nio.file.Path

abstract class LSHIndex<T : VectorValue<*>>(path: Path, parent: DefaultEntity) : AbstractIndex(path, parent) {

    /** Index-wide constants. */
    companion object {
        const val LSH_MAP_FIELD = "cdb_lsh_map"
    }

    /** The [LSHIndex] implementation returns exactly the columns that is indexed. */
    final override val produces: Array<ColumnDef<*>> =
        arrayOf(KnnUtilities.queryIndexColumnDef(this.name.entity()))

    /** The type of [AbstractIndex] */
    override val type: IndexType = IndexType.LSH
}