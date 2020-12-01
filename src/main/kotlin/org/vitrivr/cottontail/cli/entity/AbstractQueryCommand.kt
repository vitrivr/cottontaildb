package org.vitrivr.cottontail.cli.entity

import com.jakewharton.picnic.table
import org.vitrivr.cottontail.grpc.CottonDQLGrpc
import org.vitrivr.cottontail.grpc.CottontailGrpc
import kotlin.time.ExperimentalTime
import kotlin.time.TimedValue
import kotlin.time.measureTimedValue

/**
 * Base class for commands that issue a query.
 *
 * @author Loris Sauter & Ralph Gasser
 * @version 1.0.1
 */
@ExperimentalTime
abstract class AbstractQueryCommand(private val stub: CottonDQLGrpc.CottonDQLBlockingStub, name: String, help: String) : AbstractEntityCommand(name, help) {

    /**
     * Takes a [CottontailGrpc.QueryMessage], executes and collects all results into a [List]
     * of [CottontailGrpc.Tuple] and measures the time to execute this action.
     *
     * @param query The query to collect.
     * @return [TimedValue] of the query response.
     */
    protected fun execute(query: CottontailGrpc.QueryMessage): TimedValue<List<CottontailGrpc.Tuple>> = measureTimedValue {
        val tuples = mutableListOf<CottontailGrpc.Tuple>()
        val res = this.stub.query(query)
        res.forEach {
            it.resultsList.forEach {
                tuples.add(it)
            }
        }
        tuples
    }

    /**
     * Takes an [List] of [CottontailGrpc.Tuple] , iterates
     * over it and arranges the results in a table.
     *
     * @param tuples The [Iterator] to go over.
     */
    protected fun tabulate(tuples: List<CottontailGrpc.Tuple>) = table {
        cellStyle {
            border = true
            paddingLeft = 1
            paddingRight = 1
        }
        header {
            row {
                tuples.first().dataMap.keys.forEach {
                    this.cell(it)
                }
            }
        }
        body {
            tuples.forEach {
                row {
                    it.dataMap.values.map {
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
                                CottontailGrpc.Vector.VectorDataCase.FLOATVECTOR -> this@AbstractQueryCommand.vectorToString(it.vectorData.floatVector.vectorList)
                                CottontailGrpc.Vector.VectorDataCase.DOUBLEVECTOR -> this@AbstractQueryCommand.vectorToString(it.vectorData.doubleVector.vectorList)
                                CottontailGrpc.Vector.VectorDataCase.INTVECTOR -> this@AbstractQueryCommand.vectorToString(it.vectorData.intVector.vectorList)
                                CottontailGrpc.Vector.VectorDataCase.LONGVECTOR -> this@AbstractQueryCommand.vectorToString(it.vectorData.longVector.vectorList)
                                CottontailGrpc.Vector.VectorDataCase.BOOLVECTOR -> this@AbstractQueryCommand.vectorToString(it.vectorData.boolVector.vectorList)
                                CottontailGrpc.Vector.VectorDataCase.COMPLEX32VECTOR -> this@AbstractQueryCommand.vectorToString(it.vectorData.complex32Vector.vectorList.map { c -> "${c.real} + i${c.imaginary}" })
                                CottontailGrpc.Vector.VectorDataCase.COMPLEX64VECTOR -> this@AbstractQueryCommand.vectorToString(it.vectorData.complex64Vector.vectorList.map { c -> "${c.real} + i${c.imaginary}" })
                                CottontailGrpc.Vector.VectorDataCase.VECTORDATA_NOT_SET -> "~~NULL~~"
                            }
                            CottontailGrpc.Data.DataCase.NULLDATA -> "~~NULL~~"
                            CottontailGrpc.Data.DataCase.DATA_NOT_SET -> "~~N/A~~"
                            else -> ""
                        }
                    }.forEach { cell(it) }
                }
            }
        }
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