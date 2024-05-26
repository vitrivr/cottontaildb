package org.vitrivr.cottontail.dbms.statistics.values
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.BooleanVectorValue

/**
 * A [ValueStatistics] implementation for [BooleanVectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.4.0
 */
class BooleanVectorValueStatistics(
    logicalSize: Int,
    override val numberOfNullEntries: Long = 0L,
    override val numberOfNonNullEntries: Long = 0L,
    override val numberOfDistinctEntries: Long = 0L,
    val numberOfTrueEntries: LongArray = LongArray(logicalSize),
) : AbstractVectorStatistics<BooleanVectorValue>(Types.BooleanVector(logicalSize)) {

    /** A histogram capturing the number of false entries per component. */
    val numberOfFalseEntries: LongArray by lazy {
        LongArray(this.type.logicalSize) {
            this.numberOfNonNullEntries - this.numberOfTrueEntries[it]
        }
    }
}