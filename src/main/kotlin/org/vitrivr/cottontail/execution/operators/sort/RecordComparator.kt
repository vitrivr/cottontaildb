package org.vitrivr.cottontail.execution.operators.sort

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.model.basics.Record
import kotlin.math.sign

/**
 * A set of [Comparator] implementations to compare two [Record]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed class RecordComparator : Comparator<Record> {

    /**
     * Compares two [Record]s based on a single [ColumnDef] that are not nullable.
     */
    class SingleNonNullColumnComparator(val c: ColumnDef<*>) : RecordComparator() {
        init {
            require(!c.nullable) { "Column cannot be nullable for SingleNonNullColumnComparator but is." }
        }

        override fun compare(o1: Record, o2: Record): Int = o1[this.c]!!.compareTo(o2[this.c]!!).sign
    }

    /**
     * Compares two [Record]s based on a single [ColumnDef] that can be null.
     */
    class SingleNullColumnComparator(val c: ColumnDef<*>) : RecordComparator() {
        override fun compare(o1: Record, o2: Record): Int = when {
            o1[this.c] == null && o2[this.c] == null -> 0
            o1[this.c] == null && o2[this.c] != null -> -1
            o1[this.c] != null && o2[this.c] == null -> 1
            else -> o1[this.c]!!.compareTo(o2[this.c]!!).sign
        }
    }

    /**
     * Compares two [Record]s based on a multiple [ColumnDef] that are not nullable.
     */
    class MultiNonNullColumnComparator(val columns: Array<ColumnDef<*>>) : RecordComparator() {
        init {
            require(!columns.any { it.nullable }) { "Columns cannot be nullable for SingleNonNullColumnComparator but are." }
        }

        override fun compare(o1: Record, o2: Record): Int {
            var comparison = 0
            for (c in columns) {
                comparison = o1[c]!!.compareTo(o2[c]!!).sign
                if (comparison != 0) break
            }
            return comparison
        }
    }

    /**
     * Compares two [Record]s based on a multiple [ColumnDef] that can be nullable.
     */
    class MultiNullColumnComparator(val columns: Array<ColumnDef<*>>) : RecordComparator() {
        override fun compare(o1: Record, o2: Record): Int {
            var comparison = 0
            for (c in columns) {
                val c1 = o1[c]
                val c2 = o2[c]
                comparison = when {
                    c1 == null && c2 == null -> 0
                    c1 == null && c2 != null -> -1
                    c1 != null && c2 == null -> 1
                    else -> c1!!.compareTo(c2!!).sign
                }
                if (comparison != 0) break
            }
            return comparison
        }
    }
}