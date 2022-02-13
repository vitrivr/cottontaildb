package org.vitrivr.cottontail.dbms.index.lsh

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.values.types.VectorValue
import org.vitrivr.cottontail.dbms.entity.DefaultEntity
import org.vitrivr.cottontail.dbms.index.AbstractHDIndex
import org.vitrivr.cottontail.dbms.index.IndexType


abstract class LSHIndex<T : VectorValue<*>>(name: Name.IndexName, parent: DefaultEntity) : AbstractHDIndex(name, parent) {
    /** The [LSHIndex] implementation usually doesn't return a column. */
    final override val produces: Array<ColumnDef<*>> = emptyArray()

    /** The type of [AbstractIndex] */
    override val type: IndexType = IndexType.LSH
}