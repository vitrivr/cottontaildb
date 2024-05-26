package org.vitrivr.cottontail.dbms.statistics.values

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.types.VectorValue

/**
 * A [ValueStatistics] implementation for all Vector Values.
 *
 * @author Florian Burkhardt
 * @version 1.0.0
 */
sealed class AbstractVectorStatistics<T: VectorValue<*>>(type: Types<T>): AbstractValueStatistics<T>(type)