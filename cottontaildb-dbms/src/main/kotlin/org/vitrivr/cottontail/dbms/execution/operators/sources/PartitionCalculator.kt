package org.vitrivr.cottontail.dbms.execution.operators.sources

import org.vitrivr.cottontail.dbms.entity.EntityTx
import java.lang.Long.min
import java.lang.Math.floorDiv

/**
 * Obtains a [LongRange] partition for the given [partitionIndex] and the number of [partitions].
 *
 * @param partitionIndex The [partitionIndex].
 * @param partitions Total number of partitions.
 */
fun EntityTx.partitionFor(partitionIndex: Int, partitions: Int): LongRange {
    require(partitionIndex >= 0) { "Partition index must be greater than zero!" }
    require(partitionIndex < partitions) { "Partition index must be smaller than the total number of partitions!" }
    val maxTupleId = this.largestTupleId()
    val minTupleId = this.smallestTupleId()
    val range = maxTupleId - minTupleId
    val partitionSize = floorDiv(range, partitions) + 1L
    val start = minTupleId + partitionIndex * partitionSize
    val end = min(start + partitionSize - 1, maxTupleId)
    return start .. end
}