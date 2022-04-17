package org.vitrivr.cottontail.dbms.execution.operators.sort

import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.sort.SortOrder
import kotlin.math.sign

/**
 * A set of [Comparator] implementations to compare two [Record]s.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
sealed interface RecordComparator : Comparator<Record> {

    companion object {
        /**
         * Converts a list of [ColumnDef] to [SortOrder] mappings to a [RecordComparator]
         *
         * @param sortOn [List] of [ColumnDef] to [SortOrder] mappings.
         * @return [RecordComparator].
         */
        fun fromList(sortOn: List<Pair<ColumnDef<*>, SortOrder>>): RecordComparator = when {
            sortOn.size == 1 && sortOn.first().first.nullable -> SingleNullColumnComparator(sortOn.first().first, sortOn.first().second)
            sortOn.size == 1 && !sortOn.first().first.nullable -> SingleNonNullColumnComparator(sortOn.first().first, sortOn.first().second)
            sortOn.size > 1 && sortOn.any { it.first.nullable } -> MultiNullColumnComparator(sortOn)
            else -> MultiNonNullColumnComparator(sortOn)
        }
    }

    /**
     * Compares two [Record]s based on a single [ColumnDef] that are not nullable.
     */
    class SingleNonNullColumnComparator(val sortOn: ColumnDef<*>, val sortOrder: SortOrder) : RecordComparator {
        init {
            require(!this.sortOn.nullable) { "Column cannot be nullable for SingleNonNullColumnComparator but is." }
        }

        override fun compare(o1: Record, o2: Record): Int {
            var sort = o1[this.sortOn]!!.compareTo(o2[this.sortOn]!!).sign
            if (sort == 0) sort = o1.tupleId.compareTo(o2.tupleId).sign
            return this.sortOrder * sort
        }
    }

    /**
     * Compares two [Record]s based on a single [ColumnDef] that can be null.
     */
    class SingleNullColumnComparator(val sortOn: ColumnDef<*>, val sortOrder: SortOrder) : RecordComparator {
        override fun compare(o1: Record, o2: Record): Int {
            val left = o1[this.sortOn]
            val right = o2[this.sortOn]
            return this.sortOrder * when {
                left == null && right == null -> o1.tupleId.compareTo(o2.tupleId).sign
                left != null && right != null -> {
                    var sort = left.compareTo(right).sign
                    if (sort == 0) sort = o1.tupleId.compareTo(o2.tupleId).sign
                    sort
                }
                right != null -> -1
                else -> 1
            }
        }
    }

    /**
     * Compares two [Record]s based on a multiple [ColumnDef] that are not nullable.
     */
    class MultiNonNullColumnComparator(private val sortOn: List<Pair<ColumnDef<*>, SortOrder>>) : RecordComparator {
        init {
            require(!sortOn.any { it.first.nullable }) { "Columns cannot be nullable for SingleNonNullColumnComparator but are." }
        }

        override fun compare(o1: Record, o2: Record): Int {
            for (c in this.sortOn) {
                val comparison = c.second * (o1[c.first]!!.compareTo(o2[c.first]!!).sign)
                if (comparison != 0) return comparison
            }
            return o1.tupleId.compareTo(o2.tupleId).sign
        }
    }

    /**
     * Compares two [Record]s based on a multiple [ColumnDef] that can be nullable.
     */
    class MultiNullColumnComparator(private val sortOn: List<Pair<ColumnDef<*>, SortOrder>>) : RecordComparator {
        override fun compare(o1: Record, o2: Record): Int {
            for (c in this.sortOn) {
                val c1 = o1[c.first]
                val c2 = o2[c.first]
                val comparison = c.second * when {
                    c1 == null && c2 == null -> 0
                    c1 == null && c2 != null -> -1
                    c1 != null && c2 == null -> 1
                    else -> c1!!.compareTo(c2!!).sign
                }
                if (comparison != 0) return comparison
            }
            return o1.tupleId.compareTo(o2.tupleId).sign
        }
    }
}