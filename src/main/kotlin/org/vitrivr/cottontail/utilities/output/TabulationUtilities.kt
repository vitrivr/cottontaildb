package org.vitrivr.cottontail.utilities.output

import com.jakewharton.picnic.Table
import com.jakewharton.picnic.TableSectionDsl
import com.jakewharton.picnic.table
import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * Utility class for tabulated output.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object TabulationUtilities {
    /**
     * Takes a [List] of [CottontailGrpc.Tuple], iterates over it and arranges the results in a [Table].
     *
     * @param tuples The [List] to go over.
     */
    fun tabulate(tuples: List<CottontailGrpc.Tuple>): Table = table {
        cellStyle {
            border = true
            paddingLeft = 1
            paddingRight = 1
        }
        header {
            row {
                tuples.first().dataMap.keys.forEach { this.cell(it) }
            }
        }
        body {
            tuples.forEach { tupleToRow(this, it) }
        }
    }

    /**
     * Takes a [Iterator] of [CottontailGrpc.QueryResponseMessage], iterates over it and arranges
     * the results in a [Table].
     *
     * @param results The [Iterator] to go over.
     */
    fun tabulate(results: Iterator<CottontailGrpc.QueryResponseMessage>): Table {
        val list = mutableListOf<CottontailGrpc.Tuple>()
        results.forEach {
            it.resultsList.forEach {
                list.add(it)
            }
        }
        return this.tabulate(list)
    }

    /**
     * Transforms an individual [CottontailGrpc.Tuple] to a row in a table.
     *
     * @param table The [TableSectionDsl] to create the row for.
     * @param tuple The [CottontailGrpc.Tuple] to transform.
     */
    private fun tupleToRow(table: TableSectionDsl, tuple: CottontailGrpc.Tuple) = table.row {
        tuple.dataMap.values.map {
            when (it.dataCase) {
                CottontailGrpc.Data.DataCase.BOOLEANDATA -> it.booleanData.toString()
                CottontailGrpc.Data.DataCase.INTDATA -> it.intData.toString()
                CottontailGrpc.Data.DataCase.LONGDATA -> it.longData.toString()
                CottontailGrpc.Data.DataCase.FLOATDATA -> it.floatData.toString()
                CottontailGrpc.Data.DataCase.DOUBLEDATA -> it.doubleData.toString()
                CottontailGrpc.Data.DataCase.STRINGDATA -> it.stringData
                CottontailGrpc.Data.DataCase.COMPLEX32DATA -> "${it.complex32Data.real} + i${it.complex32Data.imaginary}"
                CottontailGrpc.Data.DataCase.COMPLEX64DATA -> "${it.complex32Data.real} + i${it.complex32Data.imaginary}"
                CottontailGrpc.Data.DataCase.VECTORDATA -> when (it.vectorData.vectorDataCase) {
                    CottontailGrpc.Vector.VectorDataCase.FLOATVECTOR -> this@TabulationUtilities.vectorToString(it.vectorData.floatVector.vectorList)
                    CottontailGrpc.Vector.VectorDataCase.DOUBLEVECTOR -> this@TabulationUtilities.vectorToString(it.vectorData.doubleVector.vectorList)
                    CottontailGrpc.Vector.VectorDataCase.INTVECTOR -> this@TabulationUtilities.vectorToString(it.vectorData.intVector.vectorList)
                    CottontailGrpc.Vector.VectorDataCase.LONGVECTOR -> this@TabulationUtilities.vectorToString(it.vectorData.longVector.vectorList)
                    CottontailGrpc.Vector.VectorDataCase.BOOLVECTOR -> this@TabulationUtilities.vectorToString(it.vectorData.boolVector.vectorList)
                    CottontailGrpc.Vector.VectorDataCase.COMPLEX32VECTOR -> this@TabulationUtilities.vectorToString(it.vectorData.complex32Vector.vectorList.map { c -> "${c.real} + i${c.imaginary}" })
                    CottontailGrpc.Vector.VectorDataCase.COMPLEX64VECTOR -> this@TabulationUtilities.vectorToString(it.vectorData.complex64Vector.vectorList.map { c -> "${c.real} + i${c.imaginary}" })
                    CottontailGrpc.Vector.VectorDataCase.VECTORDATA_NOT_SET -> "~~NULL~~"
                }
                CottontailGrpc.Data.DataCase.NULLDATA -> "~~NULL~~"
                CottontailGrpc.Data.DataCase.DATA_NOT_SET -> "~~N/A~~"
                else -> ""
            }
        }.forEach { cell(it) }
    }

    /**
     * Concatenates a vector (list) into a [String]
     *
     * @param vector The [List] to concatenate.
     * @param max The maximum number of elements to include.
     */
    private fun vectorToString(vector: List<*>, max: Int = 4) = if (vector.size > max) {
        "[${vector.take(max - 1).joinToString(", ")}.., ${vector.last()}]"
    } else {
        "[${vector.joinToString(", ")}]"
    }
}