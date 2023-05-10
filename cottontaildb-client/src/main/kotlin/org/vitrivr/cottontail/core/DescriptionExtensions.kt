package org.vitrivr.cottontail.core

import org.vitrivr.cottontail.core.values.*
import org.vitrivr.cottontail.grpc.CottontailGrpc
import java.util.*


/**
 * Converts this [PublicValue] to a [String] description.
 *
 * @param vectorSeparator The character used to separate vector components.
 * @param max The maximum number of components to visualize.
 */
fun PublicValue.toDescription(vectorSeparator: String = ";", max: Int = 4) = when(this) {
    is BooleanValue,
    is ByteValue,
    is ShortValue,
    is IntValue,
    is LongValue,
    is DoubleValue,
    is FloatValue -> this.toString()
    is StringValue -> "\"${this}\""
    is DateValue -> this.toDate()
    is Complex32Value -> this.toDescription()
    is Complex64Value -> this.toDescription()
    is BooleanVectorValue -> this.toDescription(vectorSeparator, max)
    is DoubleVectorValue -> this.toDescription(vectorSeparator, max)
    is FloatVectorValue -> this.toDescription(vectorSeparator, max)
    is IntVectorValue -> this.toDescription(vectorSeparator, max)
    is LongVectorValue -> this.toDescription(vectorSeparator, max)
    is Complex32VectorValue -> this.toDescription(vectorSeparator, max)
    is Complex64VectorValue -> this.toDescription(vectorSeparator, max)
    is ByteStringValue -> "<BLOB>" /* ByteStrings cannot be exported to CSV. */
}

/**
* Returns a [String] representation of [CottontailGrpc.Literal].
*
* @return [String]
* @throws IllegalArgumentException If cast is not possible.
*/
fun CottontailGrpc.Literal.toDescription() = when (this.dataCase) {
    CottontailGrpc.Literal.DataCase.BOOLEANDATA -> this.booleanData.toString()
    CottontailGrpc.Literal.DataCase.INTDATA -> this.intData.toString()
    CottontailGrpc.Literal.DataCase.LONGDATA -> this.longData.toString()
    CottontailGrpc.Literal.DataCase.FLOATDATA -> this.floatData.toString()
    CottontailGrpc.Literal.DataCase.DOUBLEDATA -> this.doubleData.toString()
    CottontailGrpc.Literal.DataCase.STRINGDATA -> this.stringData
    CottontailGrpc.Literal.DataCase.DATEDATA -> Date(this.dateData).toString()
    CottontailGrpc.Literal.DataCase.COMPLEX32DATA -> "${this.complex32Data.real} + i${this.complex32Data.imaginary}"
    CottontailGrpc.Literal.DataCase.COMPLEX64DATA -> "${this.complex32Data.real} + i${this.complex32Data.imaginary}"
    CottontailGrpc.Literal.DataCase.VECTORDATA -> when (this.vectorData.vectorDataCase) {
        CottontailGrpc.Vector.VectorDataCase.FLOATVECTOR -> toDescription(vectorData.floatVector.vectorList)
        CottontailGrpc.Vector.VectorDataCase.DOUBLEVECTOR -> toDescription(vectorData.doubleVector.vectorList)
        CottontailGrpc.Vector.VectorDataCase.INTVECTOR -> toDescription(vectorData.intVector.vectorList)
        CottontailGrpc.Vector.VectorDataCase.LONGVECTOR -> toDescription(vectorData.longVector.vectorList)
        CottontailGrpc.Vector.VectorDataCase.BOOLVECTOR -> toDescription(vectorData.boolVector.vectorList)
        CottontailGrpc.Vector.VectorDataCase.COMPLEX32VECTOR -> toDescription(vectorData.complex32Vector.vectorList.map { c -> "${c.real} + i${c.imaginary}" })
        CottontailGrpc.Vector.VectorDataCase.COMPLEX64VECTOR -> toDescription(vectorData.complex64Vector.vectorList.map { c -> "${c.real} + i${c.imaginary}" })
        else -> "<NULL>"
    }
    else  -> "<NULL>"
}

/**
 * Converts [Complex32Value] to a [String] description.
 *
 * @return [String]
 */
fun Complex32Value.toDescription() = if (this.imaginary.value < 0.0f) {
    "${this.real} - i${this.imaginary}"
} else {
    "${this.real} + i${this.imaginary}"
}

/**
 * Converts [Complex64Value] to a [String] description.
 *
 * @return [String]
 */
fun Complex64Value.toDescription() = if (this.imaginary.value < 0.0) {
    "${this.real} - i${this.imaginary}"
} else {
    "${this.real} + i${this.imaginary}"
}

/**
 * Converts [BooleanVectorValue] to a [String] description.
 *
 * @param vectorSeparator The character used to separate vector components.
 * @param max The maximum number of components to visualize.
 * @return [String]
 */
fun BooleanVectorValue.toDescription(vectorSeparator: String = ";", max: Int = 4) : String = if (this.logicalSize > max) {
    "[${this.data.take(max - 1).joinToString(separator = vectorSeparator, prefix = "[")}.., ${this.last()}]"
} else {
    this.data.joinToString(separator = vectorSeparator, prefix = "[", postfix = "]")
}

/**
 * Converts [DoubleVectorValue] to a [String] description.
 *
 * @param vectorSeparator The character used to separate vector components.
 * @param max The maximum number of components to visualize.
 * @return [String]
 */
fun DoubleVectorValue.toDescription(vectorSeparator: String = ";", max: Int = 4) : String = if (this.logicalSize > max) {
    "[${this.data.take(max - 1).joinToString(separator = vectorSeparator, prefix = "[")}.., ${this.last()}]"
} else {
    this.data.joinToString(separator = vectorSeparator, prefix = "[", postfix = "]")
}

/**
 * Converts [FloatVectorValue] to a [String] description.
 *
 * @param vectorSeparator The character used to separate vector components.
 * @param max The maximum number of components to visualize.
 * @return [String]
 */
fun FloatVectorValue.toDescription(vectorSeparator: String = ";", max: Int = 4) : String = if (this.logicalSize > max) {
    "[${this.data.take(max - 1).joinToString(separator = vectorSeparator, prefix = "[")}.., ${this.last()}]"
} else {
    this.data.joinToString(separator = vectorSeparator, prefix = "[", postfix = "]")
}

/**
 * Converts [IntVectorValue] to a [String] description.
 *
 * @param vectorSeparator The character used to separate vector components.
 * @param max The maximum number of components to visualize.
 * @return [String]
 */
fun IntVectorValue.toDescription(vectorSeparator: String = ";", max: Int = 4) : String = if (this.logicalSize > max) {
    "[${this.data.take(max - 1).joinToString(separator = vectorSeparator, prefix = "[")}.., ${this.last()}]"
} else {
    this.data.joinToString(separator = vectorSeparator, prefix = "[", postfix = "]")
}

/**
 * Converts [LongVectorValue] to a [String] description.
 *
 * @param vectorSeparator The character used to separate vector components.
 * @param max The maximum number of components to visualize.
 * @return [String]
 */
fun LongVectorValue.toDescription(vectorSeparator: String = ";", max: Int = 4) : String = if (this.logicalSize > max) {
    "[${this.data.take(max - 1).joinToString(separator = vectorSeparator, prefix = "[")}.., ${this.last()}]"
} else {
    this.data.joinToString(separator = vectorSeparator, prefix = "[", postfix = "]")
}

/**
 * Converts [Complex32VectorValue] to a [String] description.
 *
 * @param vectorSeparator The character used to separate vector components.
 * @param max The maximum number of components to visualize.
 * @return [String]
 */
fun Complex32VectorValue.toDescription(vectorSeparator: String = ";", max: Int = 4) : String = if (this.logicalSize / 2 > max) {
    "[${this.take(max - 1).joinToString(separator = vectorSeparator, prefix = "[") { (it as Complex32Value).toDescription() }}.., ${(this.last() as Complex32Value).toDescription()}]"
} else {
    this.joinToString(separator = vectorSeparator, prefix = "[", postfix = "]") { (it as Complex32Value).toDescription() }
}

/**
 * Converts [Complex32VectorValue] to a [String] description.
 *
 * @param vectorSeparator The character used to separate vector components.
 * @param max The maximum number of components to visualize.
 * @return [String]
 */
fun Complex64VectorValue.toDescription(vectorSeparator: String = ";", max: Int = 4) : String = if (this.logicalSize / 2 > max) {
    "[${this.take(max - 1).joinToString(separator = vectorSeparator, prefix = "[") { (it as Complex64Value).toDescription() }}.., ${(this.last() as Complex64Value).toDescription()}]"
} else {
    this.joinToString(separator = vectorSeparator, prefix = "[", postfix = "]") { (it as Complex64Value).toDescription() }
}


/**
 * Concatenates a vector (list) into a [String]
 *
 * @param vector The [List] to concatenate.
 * @param max The maximum number of elements to include.
 */
private fun toDescription(vector: List<*>, max: Int = 4) = if (vector.size > max) {
    "[${vector.take(max - 1).joinToString(", ")}.., ${vector.last()}]"
} else {
    "[${vector.joinToString(", ")}]"
}