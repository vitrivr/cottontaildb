package org.vitrivr.cottontail.database.index.va

import org.vitrivr.cottontail.database.index.va.marks.Marks

/**
 * This is to cache data for componentProductSum loops...
 */
inline class QueryMarkProducts(val value: Array<DoubleArray>) {
    constructor(query: DoubleArray, marks: Marks): this(
        Array(marks.marks.size) {dim ->
            DoubleArray(marks.marks[dim].size) {mark ->
                marks.marks[dim][mark] * query[dim]
            }
        })
}