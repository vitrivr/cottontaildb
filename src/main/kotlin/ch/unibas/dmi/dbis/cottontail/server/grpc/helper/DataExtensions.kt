package ch.unibas.dmi.dbis.cottontail.server.grpc.helper

import ch.unibas.dmi.dbis.cottontail.grpc.CottontailGrpc
import ch.unibas.dmi.dbis.cottontail.model.exceptions.QueryException
import ch.unibas.dmi.dbis.cottontail.model.values.*

import java.lang.IllegalArgumentException

/**
 * Helper class to convert Kotlin data types to GRPC [Data] objects
 *
 * @author Ralph Gasser
 * @version 1.0
 */
object DataHelper {
    fun toData(value: Any?): CottontailGrpc.Data? = when(value) {
        is StringValue -> CottontailGrpc.Data.newBuilder().setStringData(value.value).build()
        is LongValue -> CottontailGrpc.Data.newBuilder().setLongData(value.value).build()
        is IntValue -> CottontailGrpc.Data.newBuilder().setIntData(value.value).build()
        is ShortValue -> CottontailGrpc.Data.newBuilder().setIntData(value.value.toInt()).build()
        is ByteValue -> CottontailGrpc.Data.newBuilder().setIntData(value.value.toInt()).build()
        is DoubleValue -> CottontailGrpc.Data.newBuilder().setDoubleData(value.value).build()
        is FloatValue -> CottontailGrpc.Data.newBuilder().setFloatData(value.value).build()
        is BooleanValue -> CottontailGrpc.Data.newBuilder().setBooleanData(value.value).build()
        is DoubleArrayValue -> CottontailGrpc.Data.newBuilder().setVectorData(CottontailGrpc.Vector.newBuilder().setDoubleVector(CottontailGrpc.DoubleVector.newBuilder().addAllVector(value.value.asIterable()))).build()
        is FloatArrayValue -> CottontailGrpc.Data.newBuilder().setVectorData(CottontailGrpc.Vector.newBuilder().setFloatVector(CottontailGrpc.FloatVector.newBuilder().addAllVector(value.value.asIterable()))).build()
        is LongArrayValue -> CottontailGrpc.Data.newBuilder().setVectorData(CottontailGrpc.Vector.newBuilder().setLongVector(CottontailGrpc.LongVector.newBuilder().addAllVector(value.value.asIterable()))).build()
        is IntArrayValue -> CottontailGrpc.Data.newBuilder().setVectorData(CottontailGrpc.Vector.newBuilder().setIntVector(CottontailGrpc.IntVector.newBuilder().addAllVector(value.value.asIterable()))).build()
        null -> null
        else -> throw IllegalArgumentException("The specified value cannot be converted to a gRPC Data object.")
    }
}

/**
 * Returns the value of [CottontailGrpc.Data] as String.
 *
 * @return [StringValue]
 * @throws QueryException.UnsupportedCastException If cast is not possible.
 */
fun CottontailGrpc.Data.toStringValue(): Value<String> = StringValue(when (this.dataCase) {
    CottontailGrpc.Data.DataCase.BOOLEANDATA -> this.booleanData.toString()
    CottontailGrpc.Data.DataCase.INTDATA -> this.intData.toString()
    CottontailGrpc.Data.DataCase.LONGDATA -> this.longData.toString()
    CottontailGrpc.Data.DataCase.FLOATDATA -> this.floatData.toString()
    CottontailGrpc.Data.DataCase.DOUBLEDATA -> this.doubleData.toString()
    CottontailGrpc.Data.DataCase.STRINGDATA -> this.stringData
    CottontailGrpc.Data.DataCase.VECTORDATA -> throw QueryException.UnsupportedCastException("A value of type VECTOR cannot be cast to STRING.")
    CottontailGrpc.Data.DataCase.DATA_NOT_SET -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to STRING.")
    null -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to DOUBLE.")
})

/**
 * Returns the value of [CottontailGrpc.Data] as Boolean.
 *
 * @return [BooleanValue]
 * @throws QueryException.UnsupportedCastException If cast is not possible.
 */
fun CottontailGrpc.Data.toBooleanValue(): Value<Boolean> = BooleanValue(when (this.dataCase) {
    CottontailGrpc.Data.DataCase.BOOLEANDATA -> this.booleanData
    CottontailGrpc.Data.DataCase.INTDATA -> throw QueryException.UnsupportedCastException("A value of type INT cannot be cast to BOOLEAN.")
    CottontailGrpc.Data.DataCase.LONGDATA -> throw QueryException.UnsupportedCastException("A value of type LONG cannot be cast to BOOLEAN.")
    CottontailGrpc.Data.DataCase.FLOATDATA -> throw QueryException.UnsupportedCastException("A value of type FLOAT cannot be cast to BOOLEAN.")
    CottontailGrpc.Data.DataCase.DOUBLEDATA -> throw QueryException.UnsupportedCastException("A value of DOUBLE cannot be cast to BOOLEAN.")
    CottontailGrpc.Data.DataCase.STRINGDATA -> throw QueryException.UnsupportedCastException("A value of STRING cannot be cast to BOOLEAN.")
    CottontailGrpc.Data.DataCase.VECTORDATA -> throw QueryException.UnsupportedCastException("A value of VECTOR cannot be cast to BOOLEAN.")
    CottontailGrpc.Data.DataCase.DATA_NOT_SET -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to BOOLEAN.")
    null -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to BOOLEAN.")
})

/**
 * Returns the value of [CottontailGrpc.Data] as Double.
 *
 * @return [DoubleValue]
 * @throws QueryException.UnsupportedCastException If cast is not possible.
 */
fun CottontailGrpc.Data.toDoubleValue(): Value<Double> = DoubleValue(when (this.dataCase) {
    CottontailGrpc.Data.DataCase.BOOLEANDATA -> if (this.booleanData) { 1.0 } else { 0.0 }
    CottontailGrpc.Data.DataCase.INTDATA -> this.intData.toDouble()
    CottontailGrpc.Data.DataCase.LONGDATA -> this.longData.toDouble()
    CottontailGrpc.Data.DataCase.FLOATDATA -> this.floatData.toDouble()
    CottontailGrpc.Data.DataCase.DOUBLEDATA -> this.doubleData
    CottontailGrpc.Data.DataCase.STRINGDATA -> this.stringData.toDoubleOrNull() ?: throw QueryException.UnsupportedCastException("A value of type STRING (v='${this.stringData}') cannot be cast to DOUBLE.")
    CottontailGrpc.Data.DataCase.VECTORDATA -> throw QueryException.UnsupportedCastException("A value of VECTOR cannot be cast to DOUBLE.")
    CottontailGrpc.Data.DataCase.DATA_NOT_SET -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to DOUBLE.")
    null -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to DOUBLE.")
})

/**
 * Returns the value of [CottontailGrpc.Data] as Float.
 *
 * @return [FloatValue]
 * @throws QueryException.UnsupportedCastException If cast is not possible.
 */
fun CottontailGrpc.Data.toFloatValue(): Value<Float> = FloatValue(when (this.dataCase) {
    CottontailGrpc.Data.DataCase.BOOLEANDATA -> if (this.booleanData) { 1.0f } else { 0.0f }
    CottontailGrpc.Data.DataCase.INTDATA -> this.intData.toFloat()
    CottontailGrpc.Data.DataCase.LONGDATA -> this.longData.toFloat()
    CottontailGrpc.Data.DataCase.FLOATDATA -> this.floatData
    CottontailGrpc.Data.DataCase.DOUBLEDATA -> this.doubleData.toFloat()
    CottontailGrpc.Data.DataCase.STRINGDATA -> this.stringData.toFloatOrNull() ?: throw QueryException.UnsupportedCastException("A value of type STRING (v='${this.stringData}') cannot be cast to FLOAT.")
    CottontailGrpc.Data.DataCase.VECTORDATA -> throw QueryException.UnsupportedCastException("A value of VECTOR cannot be cast to FLOAT.")
    CottontailGrpc.Data.DataCase.DATA_NOT_SET -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to FLOAT.")
    null -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to FLOAT.")
})

/**
 * Returns the value of [CottontailGrpc.Data] as Int.
 *
 * @return [ShortValue]
 * @throws QueryException.UnsupportedCastException If cast is not possible.
 */
fun CottontailGrpc.Data.toShortValue(): Value<Short> = ShortValue(when (this.dataCase) {
    CottontailGrpc.Data.DataCase.BOOLEANDATA -> (if (this.booleanData) { 1 } else { 0 }).toShort()
    CottontailGrpc.Data.DataCase.INTDATA -> this.intData.toShort()
    CottontailGrpc.Data.DataCase.LONGDATA -> this.longData.toShort()
    CottontailGrpc.Data.DataCase.FLOATDATA -> this.floatData.toShort()
    CottontailGrpc.Data.DataCase.DOUBLEDATA -> this.doubleData.toShort()
    CottontailGrpc.Data.DataCase.STRINGDATA -> this.stringData.toShortOrNull() ?: throw QueryException.UnsupportedCastException("A value of type STRING (v='${this.stringData}') cannot be cast to SHORT.")
    CottontailGrpc.Data.DataCase.VECTORDATA -> throw QueryException.UnsupportedCastException("A value of VECTOR cannot be cast to SHORT.")
    CottontailGrpc.Data.DataCase.DATA_NOT_SET -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to SHORT.")
    null -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to SHORT.")
})

/**
 * Returns the value of [CottontailGrpc.Data] as Int.
 *
 * @return [ByteValue]
 * @throws QueryException.UnsupportedCastException If cast is not possible.
 */
fun CottontailGrpc.Data.toByteValue(): Value<Byte> = ByteValue(when (this.dataCase) {
    CottontailGrpc.Data.DataCase.BOOLEANDATA -> (if (this.booleanData) { 1 } else { 0 }).toByte()
    CottontailGrpc.Data.DataCase.INTDATA -> this.intData.toByte()
    CottontailGrpc.Data.DataCase.LONGDATA -> this.longData.toByte()
    CottontailGrpc.Data.DataCase.FLOATDATA -> this.floatData.toByte()
    CottontailGrpc.Data.DataCase.DOUBLEDATA -> this.doubleData.toByte()
    CottontailGrpc.Data.DataCase.STRINGDATA -> this.stringData.toByteOrNull() ?: throw QueryException.UnsupportedCastException("A value of type STRING (v='${this.stringData}') cannot be cast to BYTE.")
    CottontailGrpc.Data.DataCase.VECTORDATA -> throw QueryException.UnsupportedCastException("A value of VECTOR cannot be cast to BYTE.")
    CottontailGrpc.Data.DataCase.DATA_NOT_SET -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to BYTE.")
    null -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to BYTE.")
})

/**
 * Returns the value of [CottontailGrpc.Data] as Int.
 *
 * @return [IntValue]
 * @throws QueryException.UnsupportedCastException If cast is not possible.
 */
fun CottontailGrpc.Data.toIntValue(): Value<Int> = IntValue(when (this.dataCase) {
    CottontailGrpc.Data.DataCase.BOOLEANDATA -> if (this.booleanData) { 1 } else { 0 }
    CottontailGrpc.Data.DataCase.INTDATA -> this.intData
    CottontailGrpc.Data.DataCase.LONGDATA -> this.longData.toInt()
    CottontailGrpc.Data.DataCase.FLOATDATA -> this.floatData.toInt()
    CottontailGrpc.Data.DataCase.DOUBLEDATA -> this.doubleData.toInt()
    CottontailGrpc.Data.DataCase.STRINGDATA -> this.stringData.toIntOrNull() ?: throw QueryException.UnsupportedCastException("A value of type STRING (v='${this.stringData}') cannot be cast to INT.")
    CottontailGrpc.Data.DataCase.VECTORDATA -> throw QueryException.UnsupportedCastException("A value of VECTOR cannot be cast to INT.")
    CottontailGrpc.Data.DataCase.DATA_NOT_SET -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to INT.")
    null -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to INT.")
})

/**
 * Returns the value of [CottontailGrpc.Data] as Long.
 *
 * @return [LongValue]
 * @throws QueryException.UnsupportedCastException If cast is not possible.
 */
fun CottontailGrpc.Data.toLongValue(): Value<Long> = LongValue(when (this.dataCase) {
    CottontailGrpc.Data.DataCase.BOOLEANDATA -> if (this.booleanData) { 1L } else { 0L }
    CottontailGrpc.Data.DataCase.INTDATA -> this.intData.toLong()
    CottontailGrpc.Data.DataCase.LONGDATA -> this.longData
    CottontailGrpc.Data.DataCase.FLOATDATA -> this.floatData.toLong()
    CottontailGrpc.Data.DataCase.DOUBLEDATA -> this.doubleData.toLong()
    CottontailGrpc.Data.DataCase.STRINGDATA -> this.stringData.toLongOrNull() ?: throw QueryException.UnsupportedCastException("A value of type STRING (v='${this.stringData}') cannot be cast to LONG.")
    CottontailGrpc.Data.DataCase.VECTORDATA -> throw QueryException.UnsupportedCastException("A value of VECTOR cannot be cast to LONG.")
    CottontailGrpc.Data.DataCase.DATA_NOT_SET -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to LONG.")
    null -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to LONG.")
})

/**
 * Returns the value of [CottontailGrpc.Data] as Long.
 *
 * @return [FloatArrayValue]
 * @throws QueryException.UnsupportedCastException If cast is not possible.
 */
fun CottontailGrpc.Data.toFloatVectorValue(): Value<FloatArray> = FloatArrayValue(when (this.dataCase) {
    CottontailGrpc.Data.DataCase.BOOLEANDATA -> FloatArray(1) { if (this.booleanData) { 1.0f } else { 0.0f } }
    CottontailGrpc.Data.DataCase.INTDATA -> FloatArray(1) { this.intData.toFloat() }
    CottontailGrpc.Data.DataCase.LONGDATA -> FloatArray(1) { this.longData.toFloat() }
    CottontailGrpc.Data.DataCase.FLOATDATA -> FloatArray(1) { this.floatData }
    CottontailGrpc.Data.DataCase.DOUBLEDATA -> FloatArray(1) { this.doubleData.toFloat() }
    CottontailGrpc.Data.DataCase.STRINGDATA -> FloatArray(1) { this.stringData.toFloatOrNull() ?: throw QueryException.UnsupportedCastException("A value of type STRING (v='${this.stringData}') cannot be cast to VECTOR[FLOAT].") }
    CottontailGrpc.Data.DataCase.VECTORDATA -> this.vectorData.toFloatVectorValue()
    CottontailGrpc.Data.DataCase.DATA_NOT_SET -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to VECTOR[FLOAT].")
    null -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to VECTOR[FLOAT].")
})


/**
 * Returns the value of [CottontailGrpc.Data] as Long.
 *
 * @return [DoubleArrayValue] values
 * @throws QueryException.UnsupportedCastException If cast is not possible.
 */
fun CottontailGrpc.Data.toDoubleVectorValue(): Value<DoubleArray> = DoubleArrayValue(when (this.dataCase) {
    CottontailGrpc.Data.DataCase.BOOLEANDATA -> DoubleArray(1) { if (this.booleanData) { 1.0 } else { 0.0 } }
    CottontailGrpc.Data.DataCase.INTDATA -> DoubleArray(1) { this.intData.toDouble() }
    CottontailGrpc.Data.DataCase.LONGDATA -> DoubleArray(1) { this.longData.toDouble() }
    CottontailGrpc.Data.DataCase.FLOATDATA -> DoubleArray(1) { this.floatData.toDouble() }
    CottontailGrpc.Data.DataCase.DOUBLEDATA -> DoubleArray(1) { this.doubleData }
    CottontailGrpc.Data.DataCase.STRINGDATA -> DoubleArray(1) { this.stringData.toDoubleOrNull() ?: throw QueryException.UnsupportedCastException("A value of type STRING (v='${this.stringData}') cannot be cast to VECTOR[DOUBLE].") }
    CottontailGrpc.Data.DataCase.VECTORDATA -> this.vectorData.toDoubleVectorValue()
    CottontailGrpc.Data.DataCase.DATA_NOT_SET -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to VECTOR[DOUBLE].")
    null -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to VECTOR[DOUBLE].")
})

/**
 * Returns the value of [CottontailGrpc.Data] as Long.
 *
 * @return [LongArrayValue] values
 * @throws QueryException.UnsupportedCastException If cast is not possible.
 */
fun CottontailGrpc.Data.toLongVectorValue(): Value<LongArray> = LongArrayValue(when (this.dataCase) {
    CottontailGrpc.Data.DataCase.BOOLEANDATA -> LongArray(1) { if (this.booleanData) { 1L } else { 0L } }
    CottontailGrpc.Data.DataCase.INTDATA -> LongArray(1) { this.intData.toLong() }
    CottontailGrpc.Data.DataCase.LONGDATA -> LongArray(1) { this.longData }
    CottontailGrpc.Data.DataCase.FLOATDATA -> LongArray(1) { this.floatData.toLong() }
    CottontailGrpc.Data.DataCase.DOUBLEDATA -> LongArray(1) { this.doubleData.toLong() }
    CottontailGrpc.Data.DataCase.STRINGDATA -> LongArray(1) { this.stringData.toLongOrNull() ?: throw QueryException.UnsupportedCastException("A value of type STRING (v='${this.stringData}') cannot be cast to VECTOR[LONG].") }
    CottontailGrpc.Data.DataCase.VECTORDATA -> this.vectorData.toLongVectorValue()
    CottontailGrpc.Data.DataCase.DATA_NOT_SET -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to VECTOR[LONG].")
    null -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to VECTOR[LONG].")
})

/**
 * Returns the value of [CottontailGrpc.Data] as Long.
 *
 * @return [IntArrayValue] values
 * @throws QueryException.UnsupportedCastException If cast is not possible.
 */
fun CottontailGrpc.Data.toIntVectorValue(): Value<IntArray> = IntArrayValue(when (this.dataCase) {
    CottontailGrpc.Data.DataCase.BOOLEANDATA -> IntArray(1) { if (this.booleanData) { 1 } else { 0 } }
    CottontailGrpc.Data.DataCase.INTDATA -> IntArray(1) { this.intData }
    CottontailGrpc.Data.DataCase.LONGDATA -> IntArray(1) { this.longData.toInt() }
    CottontailGrpc.Data.DataCase.FLOATDATA -> IntArray(1) { this.floatData.toInt() }
    CottontailGrpc.Data.DataCase.DOUBLEDATA -> IntArray(1) { this.doubleData.toInt() }
    CottontailGrpc.Data.DataCase.STRINGDATA -> IntArray(1) { this.stringData.toIntOrNull() ?: throw QueryException.UnsupportedCastException("A value of type STRING (v='${this.stringData}') cannot be cast to VECTOR[INT].") }
    CottontailGrpc.Data.DataCase.VECTORDATA -> this.vectorData.toIntVectorValue()
    CottontailGrpc.Data.DataCase.DATA_NOT_SET -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to VECTOR[INT].")
    null -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to VECTOR[INT].")
})

/**
 * Returns the value of [CottontailGrpc.Vector] as FloatVector.
 *
 * @return [DoubleArray] values
 * @throws QueryException.UnsupportedCastException If cast is not possible.
 */
fun CottontailGrpc.Vector.toDoubleVectorValue(): DoubleArray = when (this.vectorDataCase) {
    CottontailGrpc.Vector.VectorDataCase.DOUBLEVECTOR -> DoubleArray(this.doubleVector.vectorCount) { this.doubleVector.getVector(it) }
    CottontailGrpc.Vector.VectorDataCase.FLOATVECTOR -> DoubleArray(this.floatVector.vectorCount) { this.floatVector.getVector(it).toDouble()}
    CottontailGrpc.Vector.VectorDataCase.LONGVECTOR -> DoubleArray(this.longVector.vectorCount) { this.longVector.getVector(it).toDouble() }
    CottontailGrpc.Vector.VectorDataCase.INTVECTOR -> DoubleArray(this.intVector.vectorCount) { this.intVector.getVector(it).toDouble() }
    CottontailGrpc.Vector.VectorDataCase.VECTORDATA_NOT_SET -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to VECTOR[DOUBLE].")
    null -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to VECTOR[DOUBLE].")
}

/**
 * Returns the value of [CottontailGrpc.Vector] as FloatVector.
 *
 * @return [FloatArray] values
 * @throws QueryException.UnsupportedCastException If cast is not possible.
 */
fun CottontailGrpc.Vector.toFloatVectorValue(): FloatArray = when (this.vectorDataCase) {
    CottontailGrpc.Vector.VectorDataCase.DOUBLEVECTOR -> FloatArray(this.doubleVector.vectorCount) { this.doubleVector.getVector(it).toFloat() }
    CottontailGrpc.Vector.VectorDataCase.FLOATVECTOR -> FloatArray(this.floatVector.vectorCount) { this.floatVector.getVector(it)}
    CottontailGrpc.Vector.VectorDataCase.LONGVECTOR -> FloatArray(this.longVector.vectorCount) { this.longVector.getVector(it).toFloat() }
    CottontailGrpc.Vector.VectorDataCase.INTVECTOR -> FloatArray(this.intVector.vectorCount) { this.intVector.getVector(it).toFloat() }
    CottontailGrpc.Vector.VectorDataCase.VECTORDATA_NOT_SET -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to VECTOR[FLOAT].")
    null -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to VECTOR[FLOAT].")
}

/**
 * Returns the value of [CottontailGrpc.Vector] as FloatVector.
 *
 * @return [LongArray] values
 * @throws QueryException.UnsupportedCastException If cast is not possible.
 */
fun CottontailGrpc.Vector.toLongVectorValue(): LongArray = when (this.vectorDataCase) {
    CottontailGrpc.Vector.VectorDataCase.DOUBLEVECTOR -> LongArray(this.doubleVector.vectorCount) { this.doubleVector.getVector(it).toLong() }
    CottontailGrpc.Vector.VectorDataCase.FLOATVECTOR -> LongArray(this.floatVector.vectorCount) { this.floatVector.getVector(it).toLong()}
    CottontailGrpc.Vector.VectorDataCase.LONGVECTOR -> LongArray(this.longVector.vectorCount) { this.longVector.getVector(it) }
    CottontailGrpc.Vector.VectorDataCase.INTVECTOR -> LongArray(this.intVector.vectorCount) { this.intVector.getVector(it).toLong() }
    CottontailGrpc.Vector.VectorDataCase.VECTORDATA_NOT_SET -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to VECTOR[LONG].")
    null -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to VECTOR[LONG].")
}

/**
 * Returns the value of [CottontailGrpc.Vector] as FloatVector.
 *
 * @return [IntArray] values
 * @throws QueryException.UnsupportedCastException If cast is not possible.
 */
fun CottontailGrpc.Vector.toIntVectorValue(): IntArray = when (this.vectorDataCase) {
    CottontailGrpc.Vector.VectorDataCase.DOUBLEVECTOR -> IntArray(this.doubleVector.vectorCount) { this.doubleVector.getVector(it).toInt() }
    CottontailGrpc.Vector.VectorDataCase.FLOATVECTOR -> IntArray(this.floatVector.vectorCount) { this.floatVector.getVector(it).toInt()}
    CottontailGrpc.Vector.VectorDataCase.LONGVECTOR -> IntArray(this.longVector.vectorCount) { this.longVector.getVector(it).toInt() }
    CottontailGrpc.Vector.VectorDataCase.INTVECTOR -> IntArray(this.intVector.vectorCount) { this.intVector.getVector(it) }
    CottontailGrpc.Vector.VectorDataCase.VECTORDATA_NOT_SET -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to VECTOR[INT].")
    null -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to VECTOR[INT].")
}