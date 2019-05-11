package ch.unibas.dmi.dbis.cottontail.server.grpc.helper

import ch.unibas.dmi.dbis.cottontail.grpc.CottontailGrpc
import ch.unibas.dmi.dbis.cottontail.model.exceptions.QueryException
import ch.unibas.dmi.dbis.cottontail.model.values.*

import java.lang.IllegalArgumentException

/**
 * Helper class to convert Kotlin data types to gRPC [Data] objects
 *
 * @author Ralph Gasser
 * @version 1.0.1
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
        null -> CottontailGrpc.Data.newBuilder().setNullData(CottontailGrpc.Null.getDefaultInstance()).build()
        else -> throw IllegalArgumentException("The specified value cannot be converted to a gRPC Data object.")
    }
}

/**
 * Returns the value of [CottontailGrpc.Data] as String.
 *
 * @return [StringValue]
 * @throws QueryException.UnsupportedCastException If cast is not possible.
 */
fun CottontailGrpc.Data.toStringValue(): Value<String>? = when (this.dataCase) {
    CottontailGrpc.Data.DataCase.BOOLEANDATA -> StringValue(this.booleanData.toString())
    CottontailGrpc.Data.DataCase.INTDATA -> StringValue(this.intData.toString())
    CottontailGrpc.Data.DataCase.LONGDATA -> StringValue(this.longData.toString())
    CottontailGrpc.Data.DataCase.FLOATDATA -> StringValue(this.floatData.toString())
    CottontailGrpc.Data.DataCase.DOUBLEDATA -> StringValue(this.doubleData.toString())
    CottontailGrpc.Data.DataCase.STRINGDATA -> StringValue(this.stringData)
    CottontailGrpc.Data.DataCase.NULLDATA -> null
    CottontailGrpc.Data.DataCase.VECTORDATA -> throw QueryException.UnsupportedCastException("A value of type VECTOR cannot be cast to STRING.")
    CottontailGrpc.Data.DataCase.DATA_NOT_SET -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to STRING.")
    null -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to DOUBLE.")
}

/**
 * Returns the value of [CottontailGrpc.Data] as Boolean.
 *
 * @return [BooleanValue]
 * @throws QueryException.UnsupportedCastException If cast is not possible.
 */
fun CottontailGrpc.Data.toBooleanValue(): Value<Boolean>? = when (this.dataCase) {
    CottontailGrpc.Data.DataCase.BOOLEANDATA -> BooleanValue(this.booleanData)
    CottontailGrpc.Data.DataCase.NULLDATA -> null
    CottontailGrpc.Data.DataCase.INTDATA -> throw QueryException.UnsupportedCastException("A value of type INT cannot be cast to BOOLEAN.")
    CottontailGrpc.Data.DataCase.LONGDATA -> throw QueryException.UnsupportedCastException("A value of type LONG cannot be cast to BOOLEAN.")
    CottontailGrpc.Data.DataCase.FLOATDATA -> throw QueryException.UnsupportedCastException("A value of type FLOAT cannot be cast to BOOLEAN.")
    CottontailGrpc.Data.DataCase.DOUBLEDATA -> throw QueryException.UnsupportedCastException("A value of DOUBLE cannot be cast to BOOLEAN.")
    CottontailGrpc.Data.DataCase.STRINGDATA -> throw QueryException.UnsupportedCastException("A value of STRING cannot be cast to BOOLEAN.")
    CottontailGrpc.Data.DataCase.VECTORDATA -> throw QueryException.UnsupportedCastException("A value of VECTOR cannot be cast to BOOLEAN.")
    CottontailGrpc.Data.DataCase.DATA_NOT_SET -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to BOOLEAN.")
    null -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to BOOLEAN.")
}

/**
 * Returns the value of [CottontailGrpc.Data] as Double.
 *
 * @return [DoubleValue]
 * @throws QueryException.UnsupportedCastException If cast is not possible.
 */
fun CottontailGrpc.Data.toDoubleValue(): Value<Double>? = when (this.dataCase) {
    CottontailGrpc.Data.DataCase.BOOLEANDATA -> DoubleValue(if (this.booleanData) { 1.0 } else { 0.0 })
    CottontailGrpc.Data.DataCase.INTDATA -> DoubleValue(this.intData.toDouble())
    CottontailGrpc.Data.DataCase.LONGDATA -> DoubleValue(this.longData.toDouble())
    CottontailGrpc.Data.DataCase.FLOATDATA -> DoubleValue(this.floatData.toDouble())
    CottontailGrpc.Data.DataCase.DOUBLEDATA -> DoubleValue(this.doubleData)
    CottontailGrpc.Data.DataCase.STRINGDATA -> DoubleValue(this.stringData.toDoubleOrNull() ?: throw QueryException.UnsupportedCastException("A value of type STRING (v='${this.stringData}') cannot be cast to DOUBLE."))
    CottontailGrpc.Data.DataCase.NULLDATA -> null
    CottontailGrpc.Data.DataCase.VECTORDATA -> throw QueryException.UnsupportedCastException("A value of VECTOR cannot be cast to DOUBLE.")
    CottontailGrpc.Data.DataCase.DATA_NOT_SET -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to DOUBLE.")
    null -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to DOUBLE.")
}

/**
 * Returns the value of [CottontailGrpc.Data] as Float.
 *
 * @return [FloatValue]
 * @throws QueryException.UnsupportedCastException If cast is not possible.
 */
fun CottontailGrpc.Data.toFloatValue(): Value<Float>? = when (this.dataCase) {
    CottontailGrpc.Data.DataCase.BOOLEANDATA -> FloatValue(if (this.booleanData) { 1.0f } else { 0.0f })
    CottontailGrpc.Data.DataCase.INTDATA -> FloatValue(this.intData.toFloat())
    CottontailGrpc.Data.DataCase.LONGDATA -> FloatValue(this.longData.toFloat())
    CottontailGrpc.Data.DataCase.FLOATDATA -> FloatValue(this.floatData)
    CottontailGrpc.Data.DataCase.DOUBLEDATA -> FloatValue(this.doubleData.toFloat())
    CottontailGrpc.Data.DataCase.STRINGDATA -> FloatValue(this.stringData.toFloatOrNull() ?: throw QueryException.UnsupportedCastException("A value of type STRING (v='${this.stringData}') cannot be cast to FLOAT."))
    CottontailGrpc.Data.DataCase.NULLDATA -> null
    CottontailGrpc.Data.DataCase.VECTORDATA -> throw QueryException.UnsupportedCastException("A value of VECTOR cannot be cast to FLOAT.")
    CottontailGrpc.Data.DataCase.DATA_NOT_SET -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to FLOAT.")
    null -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to FLOAT.")
}

/**
 * Returns the value of [CottontailGrpc.Data] as Int.
 *
 * @return [ShortValue]
 * @throws QueryException.UnsupportedCastException If cast is not possible.
 */
fun CottontailGrpc.Data.toShortValue(): Value<Short>? = when (this.dataCase) {
    CottontailGrpc.Data.DataCase.BOOLEANDATA -> ShortValue((if (this.booleanData) { 1 } else { 0 }).toShort())
    CottontailGrpc.Data.DataCase.INTDATA -> ShortValue(this.intData.toShort())
    CottontailGrpc.Data.DataCase.LONGDATA -> ShortValue(this.longData.toShort())
    CottontailGrpc.Data.DataCase.FLOATDATA -> ShortValue(this.floatData.toShort())
    CottontailGrpc.Data.DataCase.DOUBLEDATA -> ShortValue(this.doubleData.toShort())
    CottontailGrpc.Data.DataCase.STRINGDATA -> ShortValue(this.stringData.toShortOrNull() ?: throw QueryException.UnsupportedCastException("A value of type STRING (v='${this.stringData}') cannot be cast to SHORT."))
    CottontailGrpc.Data.DataCase.NULLDATA -> null
    CottontailGrpc.Data.DataCase.VECTORDATA -> throw QueryException.UnsupportedCastException("A value of VECTOR cannot be cast to SHORT.")
    CottontailGrpc.Data.DataCase.DATA_NOT_SET -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to SHORT.")
    null -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to SHORT.")
}

/**
 * Returns the value of [CottontailGrpc.Data] as Int.
 *
 * @return [ByteValue]
 * @throws QueryException.UnsupportedCastException If cast is not possible.
 */
fun CottontailGrpc.Data.toByteValue(): Value<Byte>? = when (this.dataCase) {
    CottontailGrpc.Data.DataCase.BOOLEANDATA -> ByteValue((if (this.booleanData) { 1 } else { 0 }).toByte())
    CottontailGrpc.Data.DataCase.INTDATA -> ByteValue(this.intData.toByte())
    CottontailGrpc.Data.DataCase.LONGDATA -> ByteValue(this.longData.toByte())
    CottontailGrpc.Data.DataCase.FLOATDATA -> ByteValue(this.floatData.toByte())
    CottontailGrpc.Data.DataCase.DOUBLEDATA -> ByteValue(this.doubleData.toByte())
    CottontailGrpc.Data.DataCase.STRINGDATA -> ByteValue(this.stringData.toByteOrNull() ?: throw QueryException.UnsupportedCastException("A value of type STRING (v='${this.stringData}') cannot be cast to BYTE."))
    CottontailGrpc.Data.DataCase.NULLDATA -> null
    CottontailGrpc.Data.DataCase.VECTORDATA -> throw QueryException.UnsupportedCastException("A value of VECTOR cannot be cast to BYTE.")
    CottontailGrpc.Data.DataCase.DATA_NOT_SET -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to BYTE.")
    null -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to BYTE.")
}

/**
 * Returns the value of [CottontailGrpc.Data] as [IntValue].
 *
 * @return [IntValue]
 * @throws QueryException.UnsupportedCastException If cast is not possible.
 */
fun CottontailGrpc.Data.toIntValue(): Value<Int>? = when (this.dataCase) {
    CottontailGrpc.Data.DataCase.BOOLEANDATA -> IntValue(if (this.booleanData) { 1 } else { 0 })
    CottontailGrpc.Data.DataCase.INTDATA -> IntValue(this.intData)
    CottontailGrpc.Data.DataCase.LONGDATA -> IntValue(this.longData.toInt())
    CottontailGrpc.Data.DataCase.FLOATDATA -> IntValue(this.floatData.toInt())
    CottontailGrpc.Data.DataCase.DOUBLEDATA -> IntValue(this.doubleData.toInt())
    CottontailGrpc.Data.DataCase.STRINGDATA -> IntValue(this.stringData.toIntOrNull() ?: throw QueryException.UnsupportedCastException("A value of type STRING (v='${this.stringData}') cannot be cast to INT."))
    CottontailGrpc.Data.DataCase.NULLDATA -> null
    CottontailGrpc.Data.DataCase.VECTORDATA -> throw QueryException.UnsupportedCastException("A value of VECTOR cannot be cast to INT.")
    CottontailGrpc.Data.DataCase.DATA_NOT_SET -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to INT.")
    null -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to INT.")
}

/**
 * Returns the value of [CottontailGrpc.Data] as [LongValue].
 *
 * @return [LongValue]
 * @throws QueryException.UnsupportedCastException If cast is not possible.
 */
fun CottontailGrpc.Data.toLongValue(): Value<Long>? = when (this.dataCase) {
    CottontailGrpc.Data.DataCase.BOOLEANDATA -> LongValue(if (this.booleanData) { 1L } else { 0L })
    CottontailGrpc.Data.DataCase.INTDATA -> LongValue(this.intData.toLong())
    CottontailGrpc.Data.DataCase.LONGDATA -> LongValue(this.longData)
    CottontailGrpc.Data.DataCase.FLOATDATA -> LongValue(this.floatData.toLong())
    CottontailGrpc.Data.DataCase.DOUBLEDATA -> LongValue(this.doubleData.toLong())
    CottontailGrpc.Data.DataCase.STRINGDATA -> LongValue(this.stringData.toLongOrNull() ?: throw QueryException.UnsupportedCastException("A value of type STRING (v='${this.stringData}') cannot be cast to LONG."))
    CottontailGrpc.Data.DataCase.NULLDATA -> null
    CottontailGrpc.Data.DataCase.VECTORDATA -> throw QueryException.UnsupportedCastException("A value of VECTOR cannot be cast to LONG.")
    CottontailGrpc.Data.DataCase.DATA_NOT_SET -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to LONG.")
    null -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to LONG.")
}

/**
 * Returns the value of [CottontailGrpc.Data] as [FloatArrayValue].
 *
 * @return [FloatArrayValue]
 * @throws QueryException.UnsupportedCastException If cast is not possible.
 */
fun CottontailGrpc.Data.toFloatVectorValue(): Value<FloatArray>? = when (this.dataCase) {
    CottontailGrpc.Data.DataCase.BOOLEANDATA -> FloatArrayValue(FloatArray(1) { if (this.booleanData) { 1.0f } else { 0.0f } })
    CottontailGrpc.Data.DataCase.INTDATA -> FloatArrayValue(FloatArray(1) { this.intData.toFloat() })
    CottontailGrpc.Data.DataCase.LONGDATA -> FloatArrayValue(FloatArray(1) { this.longData.toFloat() })
    CottontailGrpc.Data.DataCase.FLOATDATA -> FloatArrayValue(FloatArray(1) { this.floatData })
    CottontailGrpc.Data.DataCase.DOUBLEDATA -> FloatArrayValue(FloatArray(1) { this.doubleData.toFloat() })
    CottontailGrpc.Data.DataCase.STRINGDATA -> FloatArrayValue(FloatArray(1) { this.stringData.toFloatOrNull() ?: throw QueryException.UnsupportedCastException("A value of type STRING (v='${this.stringData}') cannot be cast to VECTOR[FLOAT].") })
    CottontailGrpc.Data.DataCase.VECTORDATA -> FloatArrayValue(this.vectorData.toFloatVectorValue())
    CottontailGrpc.Data.DataCase.NULLDATA -> null
    CottontailGrpc.Data.DataCase.DATA_NOT_SET -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to VECTOR[FLOAT].")
    null -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to VECTOR[FLOAT].")
}


/**
 * Returns the value of [CottontailGrpc.Data] as [DoubleArrayValue].
 *
 * @return [DoubleArrayValue] values
 * @throws QueryException.UnsupportedCastException If cast is not possible.
 */
fun CottontailGrpc.Data.toDoubleVectorValue(): Value<DoubleArray>? = when (this.dataCase) {
    CottontailGrpc.Data.DataCase.BOOLEANDATA -> DoubleArrayValue(DoubleArray(1) { if (this.booleanData) { 1.0 } else { 0.0 } })
    CottontailGrpc.Data.DataCase.INTDATA -> DoubleArrayValue(DoubleArray(1) { this.intData.toDouble() })
    CottontailGrpc.Data.DataCase.LONGDATA -> DoubleArrayValue(DoubleArray(1) { this.longData.toDouble() })
    CottontailGrpc.Data.DataCase.FLOATDATA -> DoubleArrayValue(DoubleArray(1) { this.floatData.toDouble() })
    CottontailGrpc.Data.DataCase.DOUBLEDATA -> DoubleArrayValue(DoubleArray(1) { this.doubleData })
    CottontailGrpc.Data.DataCase.STRINGDATA -> DoubleArrayValue(DoubleArray(1) { this.stringData.toDoubleOrNull() ?: throw QueryException.UnsupportedCastException("A value of type STRING (v='${this.stringData}') cannot be cast to VECTOR[DOUBLE].") })
    CottontailGrpc.Data.DataCase.VECTORDATA -> DoubleArrayValue(this.vectorData.toDoubleVectorValue())
    CottontailGrpc.Data.DataCase.NULLDATA -> null
    CottontailGrpc.Data.DataCase.DATA_NOT_SET -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to VECTOR[DOUBLE].")
    null -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to VECTOR[DOUBLE].")
}

/**
 * Returns the value of [CottontailGrpc.Data] as [LongArrayValue].
 *
 * @return [LongArrayValue] values
 * @throws QueryException.UnsupportedCastException If cast is not possible.
 */
fun CottontailGrpc.Data.toLongVectorValue(): Value<LongArray>? = when (this.dataCase) {
    CottontailGrpc.Data.DataCase.BOOLEANDATA -> LongArrayValue(LongArray(1) { if (this.booleanData) { 1L } else { 0L } })
    CottontailGrpc.Data.DataCase.INTDATA -> LongArrayValue(LongArray(1) { this.intData.toLong() })
    CottontailGrpc.Data.DataCase.LONGDATA -> LongArrayValue(LongArray(1) { this.longData })
    CottontailGrpc.Data.DataCase.FLOATDATA -> LongArrayValue(LongArray(1) { this.floatData.toLong() })
    CottontailGrpc.Data.DataCase.DOUBLEDATA -> LongArrayValue(LongArray(1) { this.doubleData.toLong() })
    CottontailGrpc.Data.DataCase.STRINGDATA -> LongArrayValue(LongArray(1) { this.stringData.toLongOrNull() ?: throw QueryException.UnsupportedCastException("A value of type STRING (v='${this.stringData}') cannot be cast to VECTOR[LONG].") })
    CottontailGrpc.Data.DataCase.VECTORDATA -> LongArrayValue(this.vectorData.toLongVectorValue())
    CottontailGrpc.Data.DataCase.NULLDATA -> null
    CottontailGrpc.Data.DataCase.DATA_NOT_SET -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to VECTOR[LONG].")
    null -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to VECTOR[LONG].")
}

/**
 * Returns the value of [CottontailGrpc.Data] as [IntArrayValue].
 *
 * @return [IntArrayValue] values
 * @throws QueryException.UnsupportedCastException If cast is not possible.
 */
fun CottontailGrpc.Data.toIntVectorValue(): Value<IntArray>? = when (this.dataCase) {
    CottontailGrpc.Data.DataCase.BOOLEANDATA -> IntArrayValue(IntArray(1) { if (this.booleanData) { 1 } else { 0 } })
    CottontailGrpc.Data.DataCase.INTDATA -> IntArrayValue(IntArray(1) { this.intData })
    CottontailGrpc.Data.DataCase.LONGDATA -> IntArrayValue(IntArray(1) { this.longData.toInt() })
    CottontailGrpc.Data.DataCase.FLOATDATA -> IntArrayValue(IntArray(1) { this.floatData.toInt() })
    CottontailGrpc.Data.DataCase.DOUBLEDATA -> IntArrayValue(IntArray(1) { this.doubleData.toInt() })
    CottontailGrpc.Data.DataCase.STRINGDATA -> IntArrayValue(IntArray(1) { this.stringData.toIntOrNull() ?: throw QueryException.UnsupportedCastException("A value of type STRING (v='${this.stringData}') cannot be cast to VECTOR[INT].") })
    CottontailGrpc.Data.DataCase.VECTORDATA -> IntArrayValue(this.vectorData.toIntVectorValue())
    CottontailGrpc.Data.DataCase.NULLDATA -> null
    CottontailGrpc.Data.DataCase.DATA_NOT_SET -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to VECTOR[INT].")
    null -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to VECTOR[INT].")
}
/**
 *
 * Returns the value of [CottontailGrpc.Data] as [BooleanArrayValue].
 *
 * @return [BooleanArrayValue] values
 * @throws QueryException.UnsupportedCastException If cast is not possible.
 */
fun CottontailGrpc.Data.toBooleanVectorValue(): Value<BooleanArray>? = when (this.dataCase) {
    CottontailGrpc.Data.DataCase.BOOLEANDATA ->  BooleanArrayValue(BooleanArray(1) { this.booleanData })
    CottontailGrpc.Data.DataCase.INTDATA ->  BooleanArrayValue(BooleanArray(1) { this.intData > 0 })
    CottontailGrpc.Data.DataCase.LONGDATA ->  BooleanArrayValue(BooleanArray(1) { this.longData > 0 })
    CottontailGrpc.Data.DataCase.FLOATDATA ->  BooleanArrayValue(BooleanArray(1) { this.floatData > 0f })
    CottontailGrpc.Data.DataCase.DOUBLEDATA ->  BooleanArrayValue(BooleanArray(1) { this.doubleData > 0.0 })
    CottontailGrpc.Data.DataCase.STRINGDATA ->  BooleanArrayValue(BooleanArray(1) { this.stringData == "true" })
    CottontailGrpc.Data.DataCase.VECTORDATA ->  BooleanArrayValue(this.vectorData.toIntVectorValue().map { it > 0 }.toBooleanArray()) //TODO this is not a proper solution
    CottontailGrpc.Data.DataCase.NULLDATA -> null
    CottontailGrpc.Data.DataCase.DATA_NOT_SET -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to VECTOR[BOOL].")
    null -> throw QueryException.UnsupportedCastException("A value of NULL cannot be cast to VECTOR[BOOL].")
}

/**
 * Returns the value of [CottontailGrpc.Vector] as [DoubleArray].
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
 * Returns the value of [CottontailGrpc.Vector] as [FloatArray].
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
 * Returns the value of [CottontailGrpc.Vector] as [LongArray].
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
 * Returns the value of [CottontailGrpc.Vector] as [IntArray].
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