package org.vitrivr.cottontail.core

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.tuple.StandaloneTuple
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.core.values.*
import org.vitrivr.cottontail.grpc.CottontailGrpc
import java.util.*


/**
 * Converts a [CottontailGrpc.QueryResponseMessage.Tuple] to a [StandaloneTuple].
 *
 * @return [StandaloneTuple]
 */
fun CottontailGrpc.QueryResponseMessage.Tuple.toTuple(tupleId: TupleId, schema: Array<ColumnDef<*>>): StandaloneTuple
    = StandaloneTuple(tupleId, schema, schema.mapIndexed { index, column -> this.dataList[index].toValue(column.type) }.toTypedArray())

/**
 * Converts a [Tuple] to a [CottontailGrpc.QueryResponseMessage.Tuple]
 *
 * @return [CottontailGrpc.QueryResponseMessage.Tuple]
 */
fun Tuple.toTuple(): CottontailGrpc.QueryResponseMessage.Tuple {
    val tuple = CottontailGrpc.QueryResponseMessage.Tuple.newBuilder()
    for (i in 0 until this.size) {
        tuple.addData((this[i] as? PublicValue?)?.toGrpc() ?: CottontailGrpc.Literal.newBuilder().setNullData(
            CottontailGrpc.Null.newBuilder().setType(this.columns[i].type.proto()).setSize(this.columns[i].type.logicalSize)
        ).build())
    }
    return tuple.build()
}

/**
 * Tries to convert an [Any] to a [PublicValue].
 *
 * This is an internal function since its use is exclusively
 * restricted to the SimpleClient API.
 *
 * @return [PublicValue]
 */
internal fun Any.tryConvertToValue(): PublicValue = when(this) {
    is PublicValue -> this
    is String -> StringValue(this)
    is Boolean -> BooleanValue(this)
    is Byte -> ByteValue(this)
    is Short -> ShortValue(this)
    is Int -> IntValue(this)
    is Long -> LongValue(this)
    is Float -> FloatValue(this)
    is Double -> DoubleValue(this)
    is BooleanArray -> BooleanVectorValue(this)
    is IntArray -> IntVectorValue(this)
    is LongArray -> LongVectorValue(this)
    is FloatArray -> FloatVectorValue(this)
    is DoubleArray -> DoubleVectorValue(this)
    else -> throw IllegalArgumentException("Cannot convert value of type ${this::class.java}.")
}


/**
 * Returns the value of [CottontailGrpc.Literal] as [Value] of the given [Types]. Performs
 * conversion if necessary.
 *
 * @param type The desired [Types].
 * @return [PublicValue] or null
 */
fun CottontailGrpc.Literal.toValue(type: Types<*>): PublicValue? = when (type) {
    is Types.Double -> this.toDoubleValue()
    is Types.Float -> this.toFloatValue()
    is Types.Boolean -> this.toBooleanValue()
    is Types.Byte -> this.toByteValue()
    is Types.Short -> this.toShortValue()
    is Types.Int -> this.toIntValue()
    is Types.Long -> this.toLongValue()
    is Types.Date -> this.toDateValue()
    is Types.String -> this.toStringValue()
    is Types.Uuid -> this.toUuidValue()
    is Types.Complex32 -> this.toComplex32Value()
    is Types.Complex64 -> this.toComplex64Value()
    is Types.IntVector -> this.toIntVectorValue()
    is Types.LongVector -> this.toLongVectorValue()
    is Types.FloatVector -> this.toFloatVectorValue()
    is Types.HalfVector -> this.toHalfVectorValue()
    is Types.DoubleVector -> this.toDoubleVectorValue()
    is Types.BooleanVector -> this.toBooleanVectorValue()
    is Types.Complex32Vector -> this.toComplex32VectorValue()
    is Types.Complex64Vector -> this.toComplex64VectorValue()
    is Types.ShortVector -> this.toShortVectorValue()
    is Types.ByteString -> this.toByteStringValue()
}

/**
 * Returns the value of [CottontailGrpc.Literal] as [Value].
 *
 * @return [PublicValue] or null
 * @throws IllegalArgumentException If cast is not possible.
 */
fun CottontailGrpc.Literal.toValue(): PublicValue? = when(this.dataCase) {
    CottontailGrpc.Literal.DataCase.BOOLEANDATA -> BooleanValue(this.booleanData)
    CottontailGrpc.Literal.DataCase.INTDATA -> IntValue(this.intData)
    CottontailGrpc.Literal.DataCase.LONGDATA -> LongValue(this.longData)
    CottontailGrpc.Literal.DataCase.FLOATDATA -> FloatValue(this.floatData)
    CottontailGrpc.Literal.DataCase.DOUBLEDATA -> DoubleValue(this.doubleData)
    CottontailGrpc.Literal.DataCase.STRINGDATA -> StringValue(this.stringData)
    CottontailGrpc.Literal.DataCase.DATEDATA -> DateValue(this.dateData)
    CottontailGrpc.Literal.DataCase.UUIDDATA -> UuidValue(this.uuidData.mostSignificant, this.uuidData.leastSignificant)
    CottontailGrpc.Literal.DataCase.COMPLEX32DATA -> Complex32Value(this.complex32Data.real, this.complex32Data.imaginary)
    CottontailGrpc.Literal.DataCase.COMPLEX64DATA -> Complex64Value(this.complex64Data.real, this.complex64Data.imaginary)
    CottontailGrpc.Literal.DataCase.BYTESTRINGDATA -> ByteStringValue(this.byteStringData.toByteArray())
    CottontailGrpc.Literal.DataCase.VECTORDATA -> when(this.vectorData.vectorDataCase) {
        CottontailGrpc.Vector.VectorDataCase.DOUBLE -> DoubleVectorValue(this.vectorData.double.vectorList.toTypedArray())
        CottontailGrpc.Vector.VectorDataCase.FLOAT -> FloatVectorValue(this.vectorData.float.vectorList.toTypedArray())
        CottontailGrpc.Vector.VectorDataCase.HALF -> HalfVectorValue(this.vectorData.half.vectorList.toTypedArray())
        CottontailGrpc.Vector.VectorDataCase.LONG -> LongVectorValue(this.vectorData.long.vectorList.toTypedArray())
        CottontailGrpc.Vector.VectorDataCase.INT -> IntVectorValue(this.vectorData.int.vectorList.toTypedArray())
        CottontailGrpc.Vector.VectorDataCase.SHORT -> ShortVectorValue(this.vectorData.short.vectorList.toTypedArray())
        CottontailGrpc.Vector.VectorDataCase.BOOL -> BooleanVectorValue(this.vectorData.bool.vectorList.toTypedArray())
        CottontailGrpc.Vector.VectorDataCase.COMPLEX32-> Complex32VectorValue(Array(this.vectorData.complex32.vectorList.size) {
            Complex32Value(FloatValue(this.vectorData.complex32.vectorList[it].real), FloatValue(this.vectorData.complex32.vectorList[it].imaginary))
        })
        CottontailGrpc.Vector.VectorDataCase.COMPLEX64 -> Complex64VectorValue(Array(this.vectorData.complex64.vectorList.size) {
            Complex32Value(FloatValue(this.vectorData.complex64.vectorList[it].real), FloatValue(this.vectorData.complex64.vectorList[it].imaginary))
        })
        else -> throw IllegalArgumentException("Literal malformed: Cannot convert value of type ${this.vectorData.vectorDataCase}.")
    }
    CottontailGrpc.Literal.DataCase.NULLDATA -> null
    else -> throw IllegalArgumentException("Literal malformed: Cannot convert value of type ${this.dataCase} to value.")
}

/**
 * Returns the [Types] of a [CottontailGrpc.Literal].
 *
 * @return [Types]
 * @throws IllegalArgumentException If cast is not possible.
 */
fun CottontailGrpc.Literal.toType(): Types<*> = when(this.dataCase) {
    CottontailGrpc.Literal.DataCase.BOOLEANDATA -> Types.Boolean
    CottontailGrpc.Literal.DataCase.INTDATA -> Types.Int
    CottontailGrpc.Literal.DataCase.LONGDATA -> Types.Long
    CottontailGrpc.Literal.DataCase.FLOATDATA -> Types.Float
    CottontailGrpc.Literal.DataCase.DOUBLEDATA -> Types.Double
    CottontailGrpc.Literal.DataCase.STRINGDATA -> Types.String
    CottontailGrpc.Literal.DataCase.DATEDATA -> Types.Date
    CottontailGrpc.Literal.DataCase.UUIDDATA -> Types.Uuid
    CottontailGrpc.Literal.DataCase.COMPLEX32DATA -> Types.Complex32
    CottontailGrpc.Literal.DataCase.COMPLEX64DATA -> Types.Complex64
    CottontailGrpc.Literal.DataCase.BYTESTRINGDATA -> Types.ByteString
    CottontailGrpc.Literal.DataCase.VECTORDATA -> when(this.vectorData.vectorDataCase) {
        CottontailGrpc.Vector.VectorDataCase.DOUBLE -> Types.DoubleVector(this.vectorData.double.vectorCount)
        CottontailGrpc.Vector.VectorDataCase.FLOAT -> Types.FloatVector(this.vectorData.float.vectorCount)
        CottontailGrpc.Vector.VectorDataCase.HALF -> Types.HalfVector(this.vectorData.half.vectorCount)
        CottontailGrpc.Vector.VectorDataCase.LONG -> Types.LongVector(this.vectorData.long.vectorCount)
        CottontailGrpc.Vector.VectorDataCase.INT -> Types.IntVector(this.vectorData.int.vectorCount)
        CottontailGrpc.Vector.VectorDataCase.SHORT -> Types.ShortVector(this.vectorData.short.vectorCount)
        CottontailGrpc.Vector.VectorDataCase.BOOL -> Types.BooleanVector(this.vectorData.bool.vectorCount)
        CottontailGrpc.Vector.VectorDataCase.COMPLEX32 -> Types.Complex32Vector(this.vectorData.complex32.vectorCount)
        CottontailGrpc.Vector.VectorDataCase.COMPLEX64 -> Types.Complex64Vector(this.vectorData.complex64.vectorCount)
        CottontailGrpc.Vector.VectorDataCase.VECTORDATA_NOT_SET,
        null -> throw IllegalArgumentException("Type cannot be determined for a value of NULL.")
    }
    CottontailGrpc.Literal.DataCase.NULLDATA -> this.nullData.type.toType(this.nullData.size)
    else -> throw IllegalArgumentException("Type cannot be determined for a value of NULL.")
}

/**
 * Converts a [Types] to the [CottontailGrpc.Type] equivalent.
 *
 * @return [CottontailGrpc.Type]
 */
fun CottontailGrpc.Type.toType(size: Int = 0): Types<*> = when(this) {
    CottontailGrpc.Type.BOOLEAN -> Types.Boolean
    CottontailGrpc.Type.BYTE -> Types.Byte
    CottontailGrpc.Type.SHORT -> Types.Short
    CottontailGrpc.Type.INTEGER -> Types.Int
    CottontailGrpc.Type.LONG -> Types.Long
    CottontailGrpc.Type.FLOAT -> Types.Float
    CottontailGrpc.Type.DOUBLE -> Types.Double
    CottontailGrpc.Type.DATE -> Types.Date
    CottontailGrpc.Type.STRING -> Types.String
    CottontailGrpc.Type.UUID -> Types.Uuid
    CottontailGrpc.Type.COMPLEX32 -> Types.Complex32
    CottontailGrpc.Type.COMPLEX64 -> Types.Complex64
    CottontailGrpc.Type.BYTESTRING -> Types.ByteString
    CottontailGrpc.Type.DOUBLE_VECTOR -> Types.DoubleVector(size)
    CottontailGrpc.Type.FLOAT_VECTOR -> Types.FloatVector(size)
    CottontailGrpc.Type.LONG_VECTOR -> Types.LongVector(size)
    CottontailGrpc.Type.INT_VECTOR -> Types.IntVector(size)
    CottontailGrpc.Type.BOOL_VECTOR -> Types.BooleanVector(size)
    CottontailGrpc.Type.COMPLEX32_VECTOR -> Types.Complex32Vector(size)
    CottontailGrpc.Type.COMPLEX64_VECTOR -> Types.Complex64Vector(size)
    CottontailGrpc.Type.SHORT_VECTOR -> Types.ShortVector(size)
    CottontailGrpc.Type.HALF_VECTOR -> Types.HalfVector(size)
    else -> throw IllegalArgumentException("gRPC type $this is unsupported and cannot be converted to Cottontail DB equivalent.")
}

/**
 * Converts a [Types] to the [CottontailGrpc.Type] equivalent.
 *
 * @return [CottontailGrpc.Type]
 */
fun Types<*>.proto(): CottontailGrpc.Type = when(this) {
    Types.Boolean -> CottontailGrpc.Type.BOOLEAN
    Types.Byte -> CottontailGrpc.Type.BYTE
    Types.Complex32 -> CottontailGrpc.Type.COMPLEX64
    Types.Complex64 -> CottontailGrpc.Type.COMPLEX32
    Types.Date -> CottontailGrpc.Type.DATE
    Types.Double -> CottontailGrpc.Type.DOUBLE
    Types.Float -> CottontailGrpc.Type.FLOAT
    Types.Int -> CottontailGrpc.Type.INTEGER
    Types.Long -> CottontailGrpc.Type.LONG
    Types.Short -> CottontailGrpc.Type.SHORT
    Types.String -> CottontailGrpc.Type.STRING
    Types.Uuid -> CottontailGrpc.Type.UUID
    is Types.BooleanVector -> CottontailGrpc.Type.BOOL_VECTOR
    is Types.IntVector -> CottontailGrpc.Type.INT_VECTOR
    is Types.LongVector -> CottontailGrpc.Type.LONG_VECTOR
    is Types.FloatVector -> CottontailGrpc.Type.FLOAT_VECTOR
    is Types.DoubleVector -> CottontailGrpc.Type.DOUBLE_VECTOR
    is Types.Complex32Vector -> CottontailGrpc.Type.COMPLEX32_VECTOR
    is Types.Complex64Vector -> CottontailGrpc.Type.COMPLEX64_VECTOR
    is Types.ShortVector -> CottontailGrpc.Type.SHORT_VECTOR
    is Types.HalfVector -> CottontailGrpc.Type.HALF_VECTOR
    Types.ByteString -> CottontailGrpc.Type.BYTESTRING
}

/**
 * Returns the value of [CottontailGrpc.Literal] as [StringValue] .
 *
 * @return [StringValue] or nzll
 * @throws IllegalArgumentException If cast is not possible.
 */
fun CottontailGrpc.Literal.toStringValue(): StringValue? = when (this.dataCase) {
    CottontailGrpc.Literal.DataCase.BOOLEANDATA -> StringValue(this.booleanData.toString())
    CottontailGrpc.Literal.DataCase.INTDATA -> StringValue(this.intData.toString())
    CottontailGrpc.Literal.DataCase.LONGDATA -> StringValue(this.longData.toString())
    CottontailGrpc.Literal.DataCase.FLOATDATA -> StringValue(this.floatData.toString())
    CottontailGrpc.Literal.DataCase.DOUBLEDATA -> StringValue(this.doubleData.toString())
    CottontailGrpc.Literal.DataCase.STRINGDATA -> StringValue(this.stringData)
    CottontailGrpc.Literal.DataCase.DATEDATA -> StringValue(Date(this.dateData).toString())
    CottontailGrpc.Literal.DataCase.COMPLEX32DATA -> StringValue("${this.complex32Data.real}+i*${this.complex32Data.imaginary}")
    CottontailGrpc.Literal.DataCase.COMPLEX64DATA -> StringValue("${this.complex64Data.real}+i*${this.complex64Data.imaginary}")
    CottontailGrpc.Literal.DataCase.DATA_NOT_SET,
    CottontailGrpc.Literal.DataCase.NULLDATA -> null
    else -> throw throw IllegalArgumentException("A value of ${this.dataCase} cannot be cast to STRING.")
}

/**
 * Returns the value of [CottontailGrpc.Literal] as [UuidValue] .
 *
 * @return [UuidValue] or nzll
 * @throws IllegalArgumentException If cast is not possible.
 */
fun CottontailGrpc.Literal.toUuidValue(): UuidValue? = when (this.dataCase) {
    CottontailGrpc.Literal.DataCase.STRINGDATA -> UuidValue(UUID.fromString(this.stringData))
    CottontailGrpc.Literal.DataCase.UUIDDATA -> UuidValue(UUID(this.uuidData.leastSignificant, this.uuidData.mostSignificant))
    CottontailGrpc.Literal.DataCase.NULLDATA -> null
    else -> throw throw IllegalArgumentException("A value of ${this.dataCase} cannot be cast to STRING.")
}

/**
 * Returns the value of [CottontailGrpc.Literal] as [ByteStringValue].
 *
 * @return [ByteStringValue] or null
 * @throws IllegalArgumentException If cast is not possible.
 */
fun CottontailGrpc.Literal.toByteStringValue(): ByteStringValue? = when (this.dataCase) {
    CottontailGrpc.Literal.DataCase.BYTESTRINGDATA -> ByteStringValue(this.byteStringData.toByteArray())
    CottontailGrpc.Literal.DataCase.NULLDATA -> null
    else -> throw IllegalArgumentException("A value of NULL cannot be cast to BYTESTRING.")
}

/**
 * Returns the value of [CottontailGrpc.Literal] as [DateValue].
 *
 * @return [DateValue] or null
 * @throws IllegalArgumentException If cast is not possible.
 */
fun CottontailGrpc.Literal.toDateValue(): DateValue? = when (this.dataCase) {
    CottontailGrpc.Literal.DataCase.BOOLEANDATA -> DateValue(this.booleanData.toLong())
    CottontailGrpc.Literal.DataCase.INTDATA -> DateValue(this.intData.toLong())
    CottontailGrpc.Literal.DataCase.LONGDATA -> DateValue(this.intData.toLong())
    CottontailGrpc.Literal.DataCase.FLOATDATA -> DateValue(this.intData.toLong())
    CottontailGrpc.Literal.DataCase.DOUBLEDATA -> DateValue(this.intData.toLong())
    CottontailGrpc.Literal.DataCase.STRINGDATA -> DateValue(this.intData.toLong())
    CottontailGrpc.Literal.DataCase.DATEDATA -> DateValue(this.dateData)
    CottontailGrpc.Literal.DataCase.NULLDATA -> null
    else -> throw throw IllegalArgumentException("A value of ${this.dataCase} cannot be cast to DATE.")
}

/**
 * Returns the value of [CottontailGrpc.Literal] as [BooleanValue].
 *
 * @return [BooleanValue] or null
 * @throws IllegalArgumentException If cast is not possible.
 */
fun CottontailGrpc.Literal.toBooleanValue(): BooleanValue? = when (this.dataCase) {
    CottontailGrpc.Literal.DataCase.BOOLEANDATA -> BooleanValue(this.booleanData)
    CottontailGrpc.Literal.DataCase.NULLDATA -> null
    else -> throw throw IllegalArgumentException("A value of ${this.dataCase} cannot be cast to BOOLEAN.")

}

/**
 * Returns the value of [CottontailGrpc.Literal] as [DoubleValue].
 *
 * @return [DoubleValue] or null
 * @throws IllegalArgumentException If cast is not possible.
 */
fun CottontailGrpc.Literal.toDoubleValue(): DoubleValue? = when (this.dataCase) {
    CottontailGrpc.Literal.DataCase.BOOLEANDATA -> DoubleValue(this.booleanData.toDouble())
    CottontailGrpc.Literal.DataCase.INTDATA -> DoubleValue(this.intData)
    CottontailGrpc.Literal.DataCase.LONGDATA -> DoubleValue(this.longData)
    CottontailGrpc.Literal.DataCase.FLOATDATA -> DoubleValue(this.floatData)
    CottontailGrpc.Literal.DataCase.DOUBLEDATA -> DoubleValue(this.doubleData)
    CottontailGrpc.Literal.DataCase.DATEDATA -> DoubleValue(this.dateData)
    CottontailGrpc.Literal.DataCase.STRINGDATA -> DoubleValue(
        this.stringData.toDoubleOrNull()
            ?: throw IllegalArgumentException("A value of type STRING (v='${this.stringData}') cannot be cast to DOUBLE.")
    )
    CottontailGrpc.Literal.DataCase.BYTESTRINGDATA -> throw IllegalArgumentException("A value of BYTESTRING cannot be cast to DOUBLE.")
    CottontailGrpc.Literal.DataCase.NULLDATA -> null
    else -> throw throw IllegalArgumentException("A value of ${this.dataCase} cannot be cast to DOUBLE.")
}

/**
 * Returns the value of [CottontailGrpc.Literal] as [FloatValue] .
 *
 * @return [FloatValue] or null
 * @throws IllegalArgumentException If cast is not possible.
 */
fun CottontailGrpc.Literal.toFloatValue(): FloatValue? = when (this.dataCase) {
    CottontailGrpc.Literal.DataCase.BOOLEANDATA -> FloatValue(this.booleanData.toFloat())
    CottontailGrpc.Literal.DataCase.INTDATA -> FloatValue(this.intData)
    CottontailGrpc.Literal.DataCase.LONGDATA -> FloatValue(this.longData)
    CottontailGrpc.Literal.DataCase.FLOATDATA -> FloatValue(this.floatData)
    CottontailGrpc.Literal.DataCase.DOUBLEDATA -> FloatValue(this.doubleData)
    CottontailGrpc.Literal.DataCase.DATEDATA -> FloatValue(this.dateData)
    CottontailGrpc.Literal.DataCase.STRINGDATA -> FloatValue(
        this.stringData.toFloatOrNull()
            ?: throw IllegalArgumentException("A value of type STRING (v='${this.stringData}') cannot be cast to FLOAT.")
    )
    CottontailGrpc.Literal.DataCase.NULLDATA -> null
    else -> throw throw IllegalArgumentException("A value of ${this.dataCase} cannot be cast to FLOAT.")
}

/**
 * Returns the value of [CottontailGrpc.Literal] as [ShortValue].
 *
 * @return [ShortValue] or null
 * @throws IllegalArgumentException If cast is not possible.
 */
fun CottontailGrpc.Literal.toShortValue(): ShortValue? = when (this.dataCase) {
    CottontailGrpc.Literal.DataCase.BOOLEANDATA -> ShortValue(this.booleanData.toShort())
    CottontailGrpc.Literal.DataCase.INTDATA -> ShortValue(this.intData)
    CottontailGrpc.Literal.DataCase.LONGDATA -> ShortValue(this.longData)
    CottontailGrpc.Literal.DataCase.FLOATDATA -> ShortValue(this.floatData)
    CottontailGrpc.Literal.DataCase.DOUBLEDATA -> ShortValue(this.doubleData)
    CottontailGrpc.Literal.DataCase.DATEDATA -> ShortValue(this.dateData)
    CottontailGrpc.Literal.DataCase.STRINGDATA -> ShortValue(
        this.stringData.toShortOrNull()
            ?: throw IllegalArgumentException("A value of type STRING (v='${this.stringData}') cannot be cast to SHORT.")
    )
    CottontailGrpc.Literal.DataCase.NULLDATA -> null
    else -> throw throw IllegalArgumentException("A value of ${this.dataCase} cannot be cast to SHORT.")
}

/**
 * Returns the value of [CottontailGrpc.Literal] as [ByteValue].
 *
 * @return [ByteValue] or null
 * @throws IllegalArgumentException If cast is not possible.
 */
fun CottontailGrpc.Literal.toByteValue(): ByteValue? = when (this.dataCase) {
    CottontailGrpc.Literal.DataCase.BOOLEANDATA -> ByteValue(this.booleanData.toByte())
    CottontailGrpc.Literal.DataCase.INTDATA -> ByteValue(this.intData)
    CottontailGrpc.Literal.DataCase.LONGDATA -> ByteValue(this.longData)
    CottontailGrpc.Literal.DataCase.FLOATDATA -> ByteValue(this.floatData)
    CottontailGrpc.Literal.DataCase.DOUBLEDATA -> ByteValue(this.doubleData)
    CottontailGrpc.Literal.DataCase.DATEDATA -> ByteValue(this.dateData.toByte())
    CottontailGrpc.Literal.DataCase.STRINGDATA -> ByteValue(
        this.stringData.toByteOrNull()
            ?: throw IllegalArgumentException("A value of type STRING (v='${this.stringData}') cannot be cast to BYTE.")
    )
    CottontailGrpc.Literal.DataCase.NULLDATA -> null
    else -> throw throw IllegalArgumentException("A value of ${this.dataCase} cannot be cast to BYTE.")
}

/**
 * Returns the value of [CottontailGrpc.Literal] as [IntValue].
 *
 * @return [IntValue] or null
 * @throws IllegalArgumentException If cast is not possible.
 */
fun CottontailGrpc.Literal.toIntValue(): IntValue? = when (this.dataCase) {
    CottontailGrpc.Literal.DataCase.BOOLEANDATA -> IntValue(this.booleanData.toInt())
    CottontailGrpc.Literal.DataCase.INTDATA -> IntValue(this.intData)
    CottontailGrpc.Literal.DataCase.LONGDATA -> IntValue(this.longData)
    CottontailGrpc.Literal.DataCase.FLOATDATA -> IntValue(this.floatData)
    CottontailGrpc.Literal.DataCase.DOUBLEDATA -> IntValue(this.doubleData)
    CottontailGrpc.Literal.DataCase.DATEDATA -> IntValue(this.dateData)
    CottontailGrpc.Literal.DataCase.STRINGDATA -> IntValue(
        this.stringData.toIntOrNull()
            ?: throw IllegalArgumentException("A value of type STRING (v='${this.stringData}') cannot be cast to INT.")
    )
    CottontailGrpc.Literal.DataCase.NULLDATA -> null
    else -> throw throw IllegalArgumentException("A value of ${this.dataCase} cannot be cast to INT.")
}

/**
 * Returns the value of [CottontailGrpc.Literal] as [LongValue].
 *
 * @return [LongValue] or null
 * @throws IllegalArgumentException If cast is not possible.
 */
fun CottontailGrpc.Literal.toLongValue(): LongValue? = when (this.dataCase) {
    CottontailGrpc.Literal.DataCase.BOOLEANDATA -> LongValue(this.booleanData.toLong())
    CottontailGrpc.Literal.DataCase.INTDATA -> LongValue(this.intData)
    CottontailGrpc.Literal.DataCase.LONGDATA -> LongValue(this.longData)
    CottontailGrpc.Literal.DataCase.FLOATDATA -> LongValue(this.floatData)
    CottontailGrpc.Literal.DataCase.DOUBLEDATA -> LongValue(this.doubleData)
    CottontailGrpc.Literal.DataCase.DATEDATA -> LongValue(this.dateData)
    CottontailGrpc.Literal.DataCase.STRINGDATA -> LongValue(
        this.stringData.toLongOrNull() ?: throw IllegalArgumentException("A value of type STRING (v='${this.stringData}') cannot be cast to LONG.")
    )
    CottontailGrpc.Literal.DataCase.DATA_NOT_SET,
    CottontailGrpc.Literal.DataCase.NULLDATA -> null
    else -> throw throw IllegalArgumentException("A value of ${this.dataCase} cannot be cast to LONG.")
}

/**
 * Returns the value of [CottontailGrpc.Literal] as [Complex32Value].
 *
 * @return [Complex32Value] or null
 * @throws IllegalArgumentException If cast is not possible.
 */
fun CottontailGrpc.Literal.toComplex32Value(): Complex32Value? = when (this.dataCase) {
    CottontailGrpc.Literal.DataCase.BOOLEANDATA -> throw IllegalArgumentException("A value of BOOLEAN cannot be cast to COMPLEX32.")
    CottontailGrpc.Literal.DataCase.INTDATA -> Complex32Value(this.intData, 0.0f)
    CottontailGrpc.Literal.DataCase.LONGDATA -> Complex32Value(this.longData, 0.0f)
    CottontailGrpc.Literal.DataCase.FLOATDATA -> Complex32Value(this.floatData, 0.0f)
    CottontailGrpc.Literal.DataCase.DOUBLEDATA -> Complex32Value(this.doubleData, 0.0f)
    CottontailGrpc.Literal.DataCase.COMPLEX32DATA -> Complex32Value(this.complex32Data.real, this.complex32Data.imaginary)
    CottontailGrpc.Literal.DataCase.COMPLEX64DATA -> Complex32Value(this.complex64Data.real, this.complex64Data.imaginary)
    CottontailGrpc.Literal.DataCase.NULLDATA -> null
    else -> throw IllegalArgumentException("Malformed literal: ${this.dataCase} cannot be cast to COMPLEX32.")
}

/**
 * Returns the value of [CottontailGrpc.Literal] as [Complex64Value].
 *
 * @return [Complex64Value] or null
 * @throws IllegalArgumentException If cast is not possible.
 */
fun CottontailGrpc.Literal.toComplex64Value(): Complex64Value? = when (this.dataCase) {
    CottontailGrpc.Literal.DataCase.BOOLEANDATA -> throw IllegalArgumentException("A value of BOOLEAN cannot be cast to COMPLEX64.")
    CottontailGrpc.Literal.DataCase.INTDATA -> Complex64Value(this.intData, 0.0)
    CottontailGrpc.Literal.DataCase.LONGDATA -> Complex64Value(this.longData, 0.0)
    CottontailGrpc.Literal.DataCase.FLOATDATA -> Complex64Value(this.floatData, 0.0)
    CottontailGrpc.Literal.DataCase.DOUBLEDATA -> Complex64Value(this.doubleData, 0.0)
    CottontailGrpc.Literal.DataCase.COMPLEX32DATA -> Complex64Value(doubleArrayOf(this.complex32Data.real.toDouble(), this.complex32Data.imaginary.toDouble()))
    CottontailGrpc.Literal.DataCase.COMPLEX64DATA -> Complex64Value(doubleArrayOf(this.complex64Data.real, this.complex64Data.imaginary))
    CottontailGrpc.Literal.DataCase.NULLDATA -> null
    else -> throw IllegalArgumentException("Malformed literal: ${this.dataCase} cannot be cast to COMPLEX64.")
}

/**
 * Returns the value of [CottontailGrpc.Literal] as [FloatVectorValue].
 *
 * @return [FloatVectorValue] or null.
 * @throws IllegalArgumentException If cast is not possible.
 */
fun CottontailGrpc.Literal.toFloatVectorValue(): FloatVectorValue? = when (this.dataCase) {
    CottontailGrpc.Literal.DataCase.BOOLEANDATA -> FloatVectorValue(FloatArray(1) { this.booleanData.toFloat() })
    CottontailGrpc.Literal.DataCase.INTDATA -> FloatVectorValue(FloatArray(1) { this.intData.toFloat() })
    CottontailGrpc.Literal.DataCase.LONGDATA -> FloatVectorValue(FloatArray(1) { this.longData.toFloat() })
    CottontailGrpc.Literal.DataCase.FLOATDATA -> FloatVectorValue(FloatArray(1) { this.floatData })
    CottontailGrpc.Literal.DataCase.DOUBLEDATA -> FloatVectorValue(FloatArray(1) { this.doubleData.toFloat() })
    CottontailGrpc.Literal.DataCase.STRINGDATA -> FloatVectorValue(FloatArray(1) {
        this.stringData.toFloatOrNull() ?: throw IllegalArgumentException("A value of type STRING (v='${this.stringData}') cannot be cast to VECTOR[FLOAT].")
    })
    CottontailGrpc.Literal.DataCase.DATEDATA -> FloatVectorValue(FloatArray(1) { this.dateData.toFloat() })
    CottontailGrpc.Literal.DataCase.VECTORDATA -> this.vectorData.toFloatVectorValue()
    CottontailGrpc.Literal.DataCase.NULLDATA -> null
    else -> throw IllegalArgumentException("Malformed literal: ${this.dataCase} cannot be cast to VECTOR[DOUBLE].")
}

/**
 * Returns the value of [CottontailGrpc.Literal] as [FloatVectorValue].
 *
 * @return [FloatVectorValue] or null.
 * @throws IllegalArgumentException If cast is not possible.
 */
fun CottontailGrpc.Literal.toHalfVectorValue(): HalfVectorValue? = when (this.dataCase) {
    CottontailGrpc.Literal.DataCase.BOOLEANDATA -> HalfVectorValue(FloatArray(1) { this.booleanData.toFloat() })
    CottontailGrpc.Literal.DataCase.INTDATA -> HalfVectorValue(FloatArray(1) { this.intData.toFloat() })
    CottontailGrpc.Literal.DataCase.LONGDATA -> HalfVectorValue(FloatArray(1) { this.longData.toFloat() })
    CottontailGrpc.Literal.DataCase.FLOATDATA -> HalfVectorValue(FloatArray(1) { this.floatData })
    CottontailGrpc.Literal.DataCase.DOUBLEDATA -> HalfVectorValue(FloatArray(1) { this.doubleData.toFloat() })
    CottontailGrpc.Literal.DataCase.STRINGDATA -> HalfVectorValue(FloatArray(1) {
        this.stringData.toFloatOrNull() ?: throw IllegalArgumentException("A value of type STRING (v='${this.stringData}') cannot be cast to VECTOR[FLOAT].")
    })
    CottontailGrpc.Literal.DataCase.DATEDATA -> HalfVectorValue(FloatArray(1) { this.dateData.toFloat() })
    CottontailGrpc.Literal.DataCase.VECTORDATA -> this.vectorData.toHalfVectorValue()
    CottontailGrpc.Literal.DataCase.NULLDATA -> null
    else -> throw IllegalArgumentException("Malformed literal: ${this.dataCase} cannot be cast to VECTOR[DOUBLE].")
}


/**
 * Returns the value of [CottontailGrpc.Literal] as [DoubleVectorValue].
 *
 * @return [DoubleVectorValue] values or null.
 * @throws IllegalArgumentException If cast is not possible.
 */
fun CottontailGrpc.Literal.toDoubleVectorValue(): DoubleVectorValue? = when (this.dataCase) {
    CottontailGrpc.Literal.DataCase.BOOLEANDATA -> DoubleVectorValue(DoubleArray(1) { this.booleanData.toDouble() })
    CottontailGrpc.Literal.DataCase.INTDATA -> DoubleVectorValue(DoubleArray(1) { this.intData.toDouble() })
    CottontailGrpc.Literal.DataCase.LONGDATA -> DoubleVectorValue(DoubleArray(1) { this.longData.toDouble() })
    CottontailGrpc.Literal.DataCase.FLOATDATA -> DoubleVectorValue(DoubleArray(1) { this.floatData.toDouble() })
    CottontailGrpc.Literal.DataCase.DOUBLEDATA -> DoubleVectorValue(DoubleArray(1) { this.doubleData })
    CottontailGrpc.Literal.DataCase.DATEDATA -> DoubleVectorValue(DoubleArray(1) { this.dateData.toDouble() })
    CottontailGrpc.Literal.DataCase.STRINGDATA -> DoubleVectorValue(DoubleArray(1) {
        this.stringData.toDoubleOrNull()
            ?: throw IllegalArgumentException("A value of type STRING (v='${this.stringData}') cannot be cast to VECTOR[DOUBLE].")
    })
    CottontailGrpc.Literal.DataCase.VECTORDATA -> this.vectorData.toDoubleVectorValue()
    CottontailGrpc.Literal.DataCase.NULLDATA -> null
    else -> throw IllegalArgumentException("Malformed literal: ${this.dataCase} cannot be cast to VECTOR[DOUBLE].")
}

/**
 * Returns the value of [CottontailGrpc.Literal] as [LongVectorValue].
 *
 * @return [LongVectorValue] or null
 * @throws IllegalArgumentException If cast is not possible.
 */
fun CottontailGrpc.Literal.toLongVectorValue(): LongVectorValue? = when (this.dataCase) {
    CottontailGrpc.Literal.DataCase.BOOLEANDATA -> LongVectorValue(LongArray(1) { this.booleanData.toLong() })
    CottontailGrpc.Literal.DataCase.INTDATA -> LongVectorValue(LongArray(1) { this.intData.toLong() })
    CottontailGrpc.Literal.DataCase.LONGDATA -> LongVectorValue(LongArray(1) { this.longData })
    CottontailGrpc.Literal.DataCase.FLOATDATA -> LongVectorValue(LongArray(1) { this.floatData.toLong() })
    CottontailGrpc.Literal.DataCase.DOUBLEDATA -> LongVectorValue(LongArray(1) { this.doubleData.toLong() })
    CottontailGrpc.Literal.DataCase.DATEDATA -> LongVectorValue(LongArray(1) { this.dateData })
    CottontailGrpc.Literal.DataCase.STRINGDATA -> LongVectorValue(LongArray(1) {
        this.stringData.toLongOrNull()
            ?: throw IllegalArgumentException("A value of type STRING (v='${this.stringData}') cannot be cast to VECTOR[LONG].")
    })
    CottontailGrpc.Literal.DataCase.VECTORDATA -> this.vectorData.toLongVectorValue()
    CottontailGrpc.Literal.DataCase.NULLDATA -> null
    else -> throw IllegalArgumentException("Malformed literal: ${this.dataCase} cannot be cast to VECTOR[LONG].")

}

fun CottontailGrpc.Literal.toShortVectorValue(): ShortVectorValue? = when (this.dataCase) {
    CottontailGrpc.Literal.DataCase.BOOLEANDATA -> ShortVectorValue(ShortArray(1) { this.booleanData.toShort() })
    CottontailGrpc.Literal.DataCase.INTDATA -> ShortVectorValue(ShortArray(1) { this.intData.toShort() })
    CottontailGrpc.Literal.DataCase.LONGDATA -> ShortVectorValue(ShortArray(1) { this.longData.toShort() })
    CottontailGrpc.Literal.DataCase.FLOATDATA -> ShortVectorValue(ShortArray(1) { this.floatData.toInt().toShort() })
    CottontailGrpc.Literal.DataCase.DOUBLEDATA -> ShortVectorValue(ShortArray(1) { this.doubleData.toInt().toShort() })
    CottontailGrpc.Literal.DataCase.DATEDATA -> ShortVectorValue(ShortArray(1) { this.dateData.toShort() })
    CottontailGrpc.Literal.DataCase.STRINGDATA -> ShortVectorValue(ShortArray(1) {
        this.stringData.toShortOrNull()
            ?: throw IllegalArgumentException("A value of type STRING (v='${this.stringData}') cannot be cast to VECTOR[SHORT].")
    })
    CottontailGrpc.Literal.DataCase.VECTORDATA -> this.vectorData.toShortVectorValue()
    CottontailGrpc.Literal.DataCase.NULLDATA -> null
    else -> throw IllegalArgumentException("Malformed literal: ${this.dataCase} cannot be cast to VECTOR[LONG].")

}

/**
 * Returns the value of [CottontailGrpc.Literal] as [IntVectorValue].
 *
 * @return [IntVectorValue] values or null
 * @throws IllegalArgumentException If cast is not possible.
 */
fun CottontailGrpc.Literal.toIntVectorValue(): IntVectorValue? = when (this.dataCase) {
    CottontailGrpc.Literal.DataCase.BOOLEANDATA -> IntVectorValue(IntArray(1) { this.booleanData.toInt() })
    CottontailGrpc.Literal.DataCase.INTDATA -> IntVectorValue(IntArray(1) { this.intData })
    CottontailGrpc.Literal.DataCase.LONGDATA -> IntVectorValue(IntArray(1) { this.longData.toInt() })
    CottontailGrpc.Literal.DataCase.FLOATDATA -> IntVectorValue(IntArray(1) { this.floatData.toInt() })
    CottontailGrpc.Literal.DataCase.DOUBLEDATA -> IntVectorValue(IntArray(1) { this.doubleData.toInt() })
    CottontailGrpc.Literal.DataCase.DATEDATA -> IntVectorValue(IntArray(1) { this.dateData.toInt() })
    CottontailGrpc.Literal.DataCase.STRINGDATA -> IntVectorValue(IntArray(1) {
        this.stringData.toIntOrNull()
            ?: throw IllegalArgumentException("A value of type STRING (v='${this.stringData}') cannot be cast to VECTOR[INT].")
    })
    CottontailGrpc.Literal.DataCase.VECTORDATA -> this.vectorData.toIntVectorValue()
    CottontailGrpc.Literal.DataCase.NULLDATA -> null
    else -> throw IllegalArgumentException("Malformed literal: ${this.dataCase} cannot be cast to VECTOR[INT].")
}

/**
 *
 * Returns the value of [CottontailGrpc.Literal] as [BooleanVectorValue].
 *
 * @return [BooleanVectorValue] values or null
 * @throws IllegalArgumentException If cast is not possible.
 */
fun CottontailGrpc.Literal.toBooleanVectorValue(): BooleanVectorValue? = when (this.dataCase) {
    CottontailGrpc.Literal.DataCase.BOOLEANDATA -> BooleanVectorValue(BooleanArray(1) { this.booleanData })
    CottontailGrpc.Literal.DataCase.INTDATA -> BooleanVectorValue(BooleanArray(1) { this.intData > 0 })
    CottontailGrpc.Literal.DataCase.LONGDATA -> BooleanVectorValue(BooleanArray(1) { this.longData > 0 })
    CottontailGrpc.Literal.DataCase.FLOATDATA -> BooleanVectorValue(BooleanArray(1) { this.floatData > 0f })
    CottontailGrpc.Literal.DataCase.DOUBLEDATA -> BooleanVectorValue(BooleanArray(1) { this.doubleData > 0.0 })
    CottontailGrpc.Literal.DataCase.STRINGDATA -> BooleanVectorValue(BooleanArray(1) { this.stringData == "true" })
    CottontailGrpc.Literal.DataCase.DATEDATA -> BooleanVectorValue(BooleanArray(1) { this.dateData > 0 })
    CottontailGrpc.Literal.DataCase.VECTORDATA -> this.vectorData.toBooleanVectorValue()
    CottontailGrpc.Literal.DataCase.NULLDATA -> null
    else -> throw IllegalArgumentException("Malformed literal: ${this.dataCase} cannot be cast to VECTOR[BOOL].")
}

/**
 * Returns the value of [CottontailGrpc.Literal] as [Complex32VectorValue].
 *
 * @return [Complex32VectorValue] values or null
 * @throws IllegalArgumentException If cast is not possible.
 */
fun CottontailGrpc.Literal.toComplex32VectorValue(): Complex32VectorValue? = when (this.dataCase) {
    CottontailGrpc.Literal.DataCase.INTDATA -> Complex32VectorValue(arrayOf(Complex32Value(this.intData)))
    CottontailGrpc.Literal.DataCase.LONGDATA -> Complex32VectorValue(arrayOf(Complex32Value(this.longData)))
    CottontailGrpc.Literal.DataCase.FLOATDATA -> Complex32VectorValue(arrayOf(Complex32Value(this.floatData)))
    CottontailGrpc.Literal.DataCase.DOUBLEDATA -> Complex32VectorValue(arrayOf(Complex32Value(this.doubleData)))
    CottontailGrpc.Literal.DataCase.VECTORDATA -> this.vectorData.toComplex32VectorValue()
    CottontailGrpc.Literal.DataCase.COMPLEX32DATA -> Complex32VectorValue(arrayOf(Complex32Value(FloatValue(this.complex32Data.real), FloatValue(this.complex32Data.imaginary))))
    CottontailGrpc.Literal.DataCase.COMPLEX64DATA -> Complex32VectorValue(arrayOf(Complex32Value(FloatValue(this.complex64Data.real.toFloat()), FloatValue(this.complex64Data.imaginary.toFloat())))) // cave! precision!
    CottontailGrpc.Literal.DataCase.NULLDATA -> null
    else -> throw IllegalArgumentException("Malformed literal: ${this.dataCase} cannot be cast to VECTOR[COMPLEX32].")
}

/**
 * Returns the value of [CottontailGrpc.Literal] as [Complex64VectorValue].
 *
 * @return [Complex64VectorValue] values or null
 * @throws IllegalArgumentException If cast is not possible.
 */
fun CottontailGrpc.Literal.toComplex64VectorValue(): Complex64VectorValue? = when (this.dataCase) {
    CottontailGrpc.Literal.DataCase.INTDATA -> Complex64VectorValue(arrayOf(Complex64Value(this.intData)))
    CottontailGrpc.Literal.DataCase.LONGDATA -> Complex64VectorValue(arrayOf(Complex64Value(this.longData)))
    CottontailGrpc.Literal.DataCase.FLOATDATA -> Complex64VectorValue(arrayOf(Complex64Value(this.floatData)))
    CottontailGrpc.Literal.DataCase.DOUBLEDATA -> Complex64VectorValue(arrayOf(Complex64Value(this.doubleData)))
    CottontailGrpc.Literal.DataCase.VECTORDATA -> this.vectorData.toComplex64VectorValue()
    CottontailGrpc.Literal.DataCase.COMPLEX32DATA -> Complex64VectorValue(arrayOf(Complex64Value(this.complex32Data.real, this.complex32Data.imaginary)))
    CottontailGrpc.Literal.DataCase.COMPLEX64DATA -> Complex64VectorValue(arrayOf(Complex64Value(this.complex64Data.real, this.complex64Data.imaginary)))
    CottontailGrpc.Literal.DataCase.NULLDATA -> null
    else -> throw IllegalArgumentException("Malformed literal: ${this.dataCase} cannot be cast to VECTOR[COMPLEX32].")
}

/**
 * Returns the value of [CottontailGrpc.Vector] as [DoubleArray].
 *
 * @return [DoubleArray] values
 * @throws IllegalArgumentException If cast is not possible.
 */
fun CottontailGrpc.Vector.toDoubleVectorValue(): DoubleVectorValue = when (this.vectorDataCase) {
    CottontailGrpc.Vector.VectorDataCase.DOUBLE -> DoubleVectorValue(this.double.vectorList.toTypedArray())
    CottontailGrpc.Vector.VectorDataCase.FLOAT -> DoubleVectorValue(this.float.vectorList)
    CottontailGrpc.Vector.VectorDataCase.HALF -> DoubleVectorValue(this.half.vectorList)
    CottontailGrpc.Vector.VectorDataCase.LONG -> DoubleVectorValue(this.long.vectorList)
    CottontailGrpc.Vector.VectorDataCase.INT -> DoubleVectorValue(this.int.vectorList)
    CottontailGrpc.Vector.VectorDataCase.SHORT -> DoubleVectorValue(this.short.vectorList)
    CottontailGrpc.Vector.VectorDataCase.BOOL -> DoubleVectorValue(this.bool.vectorList.map { if (it) 1.0 else 0.0 })
    else -> throw IllegalArgumentException("${this.vectorDataCase} cannot be cast to VECTOR[COMPLEX32].")
}

/**
 * Returns the value of [CottontailGrpc.Vector] as [FloatVectorValue].
 *
 * @return [FloatVectorValue] values
 * @throws IllegalArgumentException If cast is not possible.
 */
fun CottontailGrpc.Vector.toFloatVectorValue(): FloatVectorValue = when (this.vectorDataCase) {
    CottontailGrpc.Vector.VectorDataCase.DOUBLE -> FloatVectorValue(this.double.vectorList)
    CottontailGrpc.Vector.VectorDataCase.FLOAT -> FloatVectorValue(this.float.vectorList.toTypedArray())
    CottontailGrpc.Vector.VectorDataCase.HALF -> FloatVectorValue(this.half.vectorList.toTypedArray())
    CottontailGrpc.Vector.VectorDataCase.LONG -> FloatVectorValue(this.long.vectorList)
    CottontailGrpc.Vector.VectorDataCase.INT -> FloatVectorValue(this.int.vectorList)
    CottontailGrpc.Vector.VectorDataCase.SHORT -> FloatVectorValue(this.short.vectorList)
    CottontailGrpc.Vector.VectorDataCase.BOOL -> FloatVectorValue(this.bool.vectorList.map { if (it) 1.0f else 0.0f })
    else -> throw IllegalArgumentException("${this.vectorDataCase} cannot be cast to VECTOR[FLOAT].")
}

/**
 * Returns the value of [CottontailGrpc.Vector] as [HalfVectorValue].
 *
 * @return [HalfVectorValue] values
 * @throws IllegalArgumentException If cast is not possible.
 */
fun CottontailGrpc.Vector.toHalfVectorValue(): HalfVectorValue = when (this.vectorDataCase) {
    CottontailGrpc.Vector.VectorDataCase.DOUBLE -> HalfVectorValue(this.double.vectorList)
    CottontailGrpc.Vector.VectorDataCase.FLOAT -> HalfVectorValue(this.float.vectorList.toTypedArray())
    CottontailGrpc.Vector.VectorDataCase.HALF -> HalfVectorValue(this.half.vectorList.toTypedArray())
    CottontailGrpc.Vector.VectorDataCase.LONG -> HalfVectorValue(this.long.vectorList)
    CottontailGrpc.Vector.VectorDataCase.INT -> HalfVectorValue(this.int.vectorList)
    CottontailGrpc.Vector.VectorDataCase.SHORT -> HalfVectorValue(this.short.vectorList)
    CottontailGrpc.Vector.VectorDataCase.BOOL -> HalfVectorValue(this.bool.vectorList.map { if (it) 1.0f else 0.0f })
    else -> throw IllegalArgumentException("${this.vectorDataCase} cannot be cast to VECTOR[FLOAT].")
}

/**
 * Returns the value of [CottontailGrpc.Vector] as [LongVectorValue].
 *
 * @return [LongVectorValue] values
 * @throws IllegalArgumentException If cast is not possible.
 */
fun CottontailGrpc.Vector.toLongVectorValue(): LongVectorValue = when (this.vectorDataCase) {
    CottontailGrpc.Vector.VectorDataCase.DOUBLE -> LongVectorValue(this.double.vectorList)
    CottontailGrpc.Vector.VectorDataCase.FLOAT -> LongVectorValue(this.float.vectorList)
    CottontailGrpc.Vector.VectorDataCase.HALF -> LongVectorValue(this.half.vectorList)
    CottontailGrpc.Vector.VectorDataCase.LONG -> LongVectorValue(this.long.vectorList.toTypedArray())
    CottontailGrpc.Vector.VectorDataCase.INT-> LongVectorValue(this.int.vectorList)
    CottontailGrpc.Vector.VectorDataCase.SHORT -> LongVectorValue(this.short.vectorList)
    CottontailGrpc.Vector.VectorDataCase.BOOL -> LongVectorValue(this.bool.vectorList.map { if (it) 1L else 0L })
    else -> throw IllegalArgumentException("${this.vectorDataCase} cannot be cast to VECTOR[LONG].")
}

/**
 * Returns the value of [CottontailGrpc.Vector] as [IntArray].
 *
 * @return [IntArray] values
 * @throws IllegalArgumentException If cast is not possible.
 */
fun CottontailGrpc.Vector.toIntVectorValue(): IntVectorValue = when (this.vectorDataCase) {
    CottontailGrpc.Vector.VectorDataCase.DOUBLE -> IntVectorValue(this.double.vectorList)
    CottontailGrpc.Vector.VectorDataCase.FLOAT -> IntVectorValue(this.float.vectorList)
    CottontailGrpc.Vector.VectorDataCase.HALF -> IntVectorValue(this.float.vectorList)
    CottontailGrpc.Vector.VectorDataCase.LONG -> IntVectorValue(this.long.vectorList)
    CottontailGrpc.Vector.VectorDataCase.INT -> IntVectorValue(this.int.vectorList.toTypedArray())
    CottontailGrpc.Vector.VectorDataCase.SHORT -> IntVectorValue(this.short.vectorList)
    CottontailGrpc.Vector.VectorDataCase.BOOL -> IntVectorValue(this.bool.vectorList.map { if (it) 1 else 0 })
    else -> throw IllegalArgumentException("${this.vectorDataCase} cannot be cast to VECTOR[INT].")
}

/**
 * Returns the value of [CottontailGrpc.Vector] as [IntArray].
 *
 * @return [IntArray] values
 * @throws IllegalArgumentException If cast is not possible.
 */
fun CottontailGrpc.Vector.toShortVectorValue(): ShortVectorValue = when (this.vectorDataCase) {
    CottontailGrpc.Vector.VectorDataCase.DOUBLE -> ShortVectorValue(this.double.vectorList)
    CottontailGrpc.Vector.VectorDataCase.FLOAT -> ShortVectorValue(this.float.vectorList)
    CottontailGrpc.Vector.VectorDataCase.HALF -> ShortVectorValue(this.half.vectorList)
    CottontailGrpc.Vector.VectorDataCase.LONG -> ShortVectorValue(this.long.vectorList.toTypedArray())
    CottontailGrpc.Vector.VectorDataCase.INT-> ShortVectorValue(this.int.vectorList)
    CottontailGrpc.Vector.VectorDataCase.SHORT-> ShortVectorValue(this.short.vectorList)
    CottontailGrpc.Vector.VectorDataCase.BOOL -> ShortVectorValue(this.bool.vectorList.map { if (it) 1 else 0 })
    else -> throw IllegalArgumentException("${this.vectorDataCase} cannot be cast to VECTOR[SHORT].")
}

/**
 * Returns the value of [CottontailGrpc.Vector] as [BitSet].
 *
 * @return [BitSet] values
 * @throws IllegalArgumentException If cast is not possible.
 */
fun CottontailGrpc.Vector.toBooleanVectorValue(): BooleanVectorValue = when (this.vectorDataCase) {
    CottontailGrpc.Vector.VectorDataCase.DOUBLE -> BooleanVectorValue(this.double.vectorList)
    CottontailGrpc.Vector.VectorDataCase.FLOAT -> BooleanVectorValue(this.float.vectorList)
    CottontailGrpc.Vector.VectorDataCase.HALF -> BooleanVectorValue(this.half.vectorList)
    CottontailGrpc.Vector.VectorDataCase.LONG -> BooleanVectorValue(this.long.vectorList)
    CottontailGrpc.Vector.VectorDataCase.INT -> BooleanVectorValue(this.int.vectorList)
    CottontailGrpc.Vector.VectorDataCase.SHORT -> BooleanVectorValue(this.short.vectorList)
    CottontailGrpc.Vector.VectorDataCase.BOOL -> BooleanVectorValue(this.bool.vectorList.toTypedArray())
    else -> throw IllegalArgumentException("${this.vectorDataCase} cannot be cast to VECTOR[BOOL].")
}

/**
 * Returns the value of [CottontailGrpc.Vector] as [Complex32VectorValue].
 *
 * @return [Complex32VectorValue] values
 * @throws IllegalArgumentException If cast is not possible.
 */
fun CottontailGrpc.Vector.toComplex32VectorValue(): Complex32VectorValue = when (this.vectorDataCase) {
    CottontailGrpc.Vector.VectorDataCase.DOUBLE -> Complex32VectorValue(Array(this.double.vectorList.size) { Complex32Value(this.double.vectorList[it], 0.0f) })
    CottontailGrpc.Vector.VectorDataCase.FLOAT -> Complex32VectorValue(Array(this.float.vectorList.size) { Complex32Value(this.float.vectorList[it], 0.0f) })
    CottontailGrpc.Vector.VectorDataCase.HALF -> Complex32VectorValue(Array(this.half.vectorList.size) { Complex32Value(this.half.vectorList[it], 0.0f) })
    CottontailGrpc.Vector.VectorDataCase.LONG -> Complex32VectorValue(Array(this.long.vectorList.size) { Complex32Value(this.long.vectorList[it], 0.0f) })
    CottontailGrpc.Vector.VectorDataCase.INT-> Complex32VectorValue(Array(this.int.vectorList.size) { Complex32Value(this.int.vectorList[it], 0.0f) })
    CottontailGrpc.Vector.VectorDataCase.SHORT-> Complex32VectorValue(Array(this.short.vectorList.size) { Complex32Value(this.short.vectorList[it], 0.0f) })
    CottontailGrpc.Vector.VectorDataCase.COMPLEX32 -> Complex32VectorValue(Array(this.complex32.vectorList.size) {
        Complex32Value(this.complex32.vectorList[it].real,this.complex32.vectorList[it].imaginary)
    })
    CottontailGrpc.Vector.VectorDataCase.COMPLEX64 -> Complex32VectorValue(Array(this.complex64.vectorList.size) {
        Complex32Value(
            FloatValue(this.complex64.vectorList[it].real),
            FloatValue(this.complex64.vectorList[it].imaginary)
        )
    })
    else -> throw IllegalArgumentException("${this.vectorDataCase} cannot be cast to VECTOR[COMPLEX32].")
}

/**
 * Returns the value of [CottontailGrpc.Vector] as [Complex64VectorValue].
 *
 * @return [Complex64VectorValue] values
 * @throws IllegalArgumentException If cast is not possible.
 */
fun CottontailGrpc.Vector.toComplex64VectorValue(): Complex64VectorValue = when (this.vectorDataCase) {
    CottontailGrpc.Vector.VectorDataCase.DOUBLE -> Complex64VectorValue(Array(this.double.vectorList.size) { Complex64Value(this.double.vectorList[it], 0.0) })
    CottontailGrpc.Vector.VectorDataCase.FLOAT-> Complex64VectorValue(Array(float.vectorList.size) { Complex64Value(float.vectorList[it], 0.0) })
    CottontailGrpc.Vector.VectorDataCase.HALF -> Complex64VectorValue(Array(this.half.vectorList.size) { Complex64Value(this.half.vectorList[it], 0.0) })
    CottontailGrpc.Vector.VectorDataCase.LONG -> Complex64VectorValue(Array(this.long.vectorList.size) { Complex64Value(this.long.vectorList[it], 0.0) })
    CottontailGrpc.Vector.VectorDataCase.INT -> Complex64VectorValue(Array(this.int.vectorList.size) { Complex64Value(this.int.vectorList[it], 0.0) })
    CottontailGrpc.Vector.VectorDataCase.SHORT-> Complex64VectorValue(Array(this.short.vectorList.size) { Complex32Value(this.short.vectorList[it], 0.0) })
    CottontailGrpc.Vector.VectorDataCase.COMPLEX32 -> Complex64VectorValue(Array(this.complex32.vectorList.size) {
        Complex64Value(this.complex32.vectorList[it].real, this.complex32.vectorList[it].imaginary)
    })
    CottontailGrpc.Vector.VectorDataCase.COMPLEX64 -> Complex64VectorValue(Array(this.complex64.vectorList.size) {
        Complex64Value(this.complex64.vectorList[it].real, this.complex64.vectorList[it].imaginary)
    })
    else -> throw IllegalArgumentException("${this.vectorDataCase} cannot be cast to VECTOR[COMPLEX64].")
}