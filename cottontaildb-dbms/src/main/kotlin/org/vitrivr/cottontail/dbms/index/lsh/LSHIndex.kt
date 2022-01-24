package org.vitrivr.cottontail.dbms.index.lsh

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.dbms.entity.DefaultEntity
import org.vitrivr.cottontail.dbms.index.AbstractIndex
import org.vitrivr.cottontail.dbms.index.IndexType
import org.vitrivr.cottontail.core.values.types.VectorValue
import java.nio.file.Path

abstract class LSHIndex<T : VectorValue<*>>(path: Path, parent: DefaultEntity) : AbstractIndex(path, parent) {

    /** Index-wide constants. */
    companion object {
        const val LSH_MAP_FIELD = "cdb_lsh_map"
    }

    /** The [LSHIndex] implementation returns exactly the columns that is indexed. */
    final override val produces: Array<ColumnDef<*>> = emptyArray()

    /** The type of [AbstractIndex] */
    override val type: IndexType = IndexType.LSH
}