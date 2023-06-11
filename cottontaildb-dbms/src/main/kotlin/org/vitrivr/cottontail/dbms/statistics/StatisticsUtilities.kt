package org.vitrivr.cottontail.dbms.statistics

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.dbms.statistics.values.ValueStatistics


/**
 * Estimates the size of a [Tuple] based on the [ColumnDef] and the associated [ValueStatistics].
 *
 * @return The estimated size of the [Tuple] in bytes.
 */
fun Map<ColumnDef<*>, ValueStatistics<*>>.estimateTupleSize() = this.map {
    when (val type = it.key.type) {
        Types.String ->  it.value.avgWidth * Char.SIZE_BYTES
        Types.ByteString -> it.value.avgWidth
        else -> type.physicalSize
    }
}.sum()