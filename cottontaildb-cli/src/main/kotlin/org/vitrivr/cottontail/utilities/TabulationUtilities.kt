package org.vitrivr.cottontail.utilities

import com.jakewharton.picnic.Table
import com.jakewharton.picnic.TableSectionDsl
import com.jakewharton.picnic.table
import org.vitrivr.cottontail.client.iterators.Tuple
import org.vitrivr.cottontail.client.iterators.TupleIterator
import org.vitrivr.cottontail.core.toDescription
import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * Utility class for tabulated output.
 *
 * @author Ralph Gasser
 * @version 2.0.1
 */
object TabulationUtilities {

    /**
     * Takes a [Iterator] of [TupleIterator], iterates over it and arranges the results in a [Table].
     *
     * @param result The [TupleIterator] to visualize.
     * @return [Table]
     */
    fun tabulate(result: TupleIterator): Table = table {
        cellStyle {
            border = true
            paddingLeft = 1
            paddingRight = 1
        }
        header {
            row {
                result.columnNames.forEach { cell(it) }
            }
        }
        body {
            while (result.hasNext()) {
                tupleToRow(this, result.next())
            }
        }
    }

    /**
     * Takes a [TupleIterator] , iterates over it and arranges the results in a [Table].
     *
     * @param result The [TupleIterator] to go tabulate.
     * @param predicate A predicate evaluating a [Tuple] and returning either true or false.
     * @return [Table]
     */
    fun tabulateIf(result: TupleIterator, predicate: (Tuple) -> Boolean): Table = table {
        cellStyle {
            border = true
            paddingLeft = 1
            paddingRight = 1
        }
        header {
            row {
                result.columnNames.forEach { cell(it) }
            }
        }
        body {
            while (result.hasNext()) {
                val next = result.next()
                if (predicate(next)) {
                    tupleToRow(this, next)
                }
            }
        }
    }

    /**
     * Transforms an individual [CottontailGrpc.Literal] to a row in a table.
     *
     * @param table The [TableSectionDsl] to create the row for.
     * @param tuple The [CottontailGrpc.Literal] to transform.
     */
    private fun tupleToRow(table: TableSectionDsl, tuple: Tuple) = table.row {
        (0 until tuple.size()).forEach {  cell(tuple[it]?.toDescription()) }
    }
}