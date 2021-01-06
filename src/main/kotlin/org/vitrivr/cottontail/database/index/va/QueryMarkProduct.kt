package org.vitrivr.cottontail.database.index.va

import org.vitrivr.cottontail.database.index.va.signature.Marks
import org.vitrivr.cottontail.model.values.types.RealVectorValue

/**
 * This class is used to cache the component-wise product between a [RealVectorValue] (query) and the [Marks]
 *
 * @author Gabriel Zihlmann & Ralph Gasser
 * @version 1.0.1
 */
class QueryMarkProduct(query: RealVectorValue<*>, marks: Marks) {
    /** */
    val product = Array(marks.marks.size) { dim ->
        DoubleArray(marks.marks[dim].size) { mark ->
            marks.marks[dim][mark] * query[dim].value.toDouble()
        }
    }
}