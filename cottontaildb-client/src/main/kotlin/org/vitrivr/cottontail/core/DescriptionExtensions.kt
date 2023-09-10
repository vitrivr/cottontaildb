package org.vitrivr.cottontail.core

import org.vitrivr.cottontail.core.values.*

/** [Regex] used to parse complex numbers. */
private val COMPLEXREGEX = Regex("([-+]?\\d+(?:\\.\\d+)?)\\s*([+-])\\s*i\\s*([-+]?\\d+(?:\\.\\d+)?)")

/**
 * Converts this [PublicValue] to a [String] description.
 *
 * @param vectorSeparator The character used to separate vector components.
 * @param max The maximum number of components to visualize.
 */
fun PublicValue.toDescription(vectorSeparator: String = ";", max: Int = 4): String = when(this) {
    is BooleanValue -> this.value.toString()
    is ByteValue -> this.value.toString()
    is ShortValue -> this.value.toString()
    is IntValue -> this.value.toString()
    is LongValue -> this.value.toString()
    is DoubleValue -> this.value.toString()
    is FloatValue -> this.toString()
    is StringValue -> this.value
    is UuidValue -> this.value.toString()
    is DateValue -> this.toDate().toString()
    is ByteStringValue -> this.toBase64()
    is Complex32Value -> this.toDescription()
    is Complex64Value -> this.toDescription()
    is BooleanVectorValue -> this.toDescription(vectorSeparator, max)
    is DoubleVectorValue -> this.toDescription(vectorSeparator, max)
    is FloatVectorValue -> this.toDescription(vectorSeparator, max)
    is IntVectorValue -> this.toDescription(vectorSeparator, max)
    is LongVectorValue -> this.toDescription(vectorSeparator, max)
    is Complex32VectorValue -> this.toDescription(vectorSeparator, max)
    is Complex64VectorValue -> this.toDescription(vectorSeparator, max)
}

/**
 * Converts [Complex32Value] to a [String] description.
 *
 * @return [String]
 */
fun Complex32Value.toDescription() = if (this.imaginary.value < 0.0f) {
    "${this.real}-i${this.imaginary}"
} else {
    "${this.real}+i${this.imaginary}"
}

/**
 * Parses a [String] to a [Complex32Value].
 *
 * @param string The [String] description
 * @return [Complex32Value]
 */
fun parseComplex32Description(string: String): Complex32Value {
    val matchResult = COMPLEXREGEX.matchEntire(string.trim()) ?: throw IllegalArgumentException("Failed to parse complex number.")
    val (real, op, imag) = matchResult.destructured
    return Complex32Value(
        real.toFloatOrNull() ?: throw IllegalArgumentException("Failed to parse real part of complex number."),
        "$op$imag".toFloatOrNull() ?: throw IllegalArgumentException("Failed to parse imaginary part of complex number.")
    )
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
 * Parses a [String] to a [Complex64Value].
 *
 * @param string The [String] description
 * @return [Complex64Value]
 */
fun parseComplex64Description(string: String): Complex64Value {
    val matchResult = COMPLEXREGEX.matchEntire(string.trim()) ?: throw IllegalArgumentException("Failed to parse complex number.")
    val (real, op, imag) = matchResult.destructured
    return Complex64Value(
        real.toDoubleOrNull() ?: throw IllegalArgumentException("Failed to parse real part of complex number."),
        "$op$imag".toDoubleOrNull() ?: throw IllegalArgumentException("Failed to parse imaginary part of complex number.")
    )
}

/**
 * Converts [BooleanVectorValue] to a [String] description.
 *
 * @param vectorSeparator The character used to separate vector components.
 * @param max The maximum number of components to visualize.
 * @return [String]
 */
fun BooleanVectorValue.toDescription(vectorSeparator: String = ";", max: Int = 4) : String = if (this.logicalSize > max) {
    "${this.data.take(max - 1).joinToString(separator = vectorSeparator, prefix = "[")}..$vectorSeparator${this.last().value}]"
} else {
    this.data.joinToString(separator = vectorSeparator, prefix = "[", postfix = "]")
}

/**
 * Parses a [String] to a [BooleanVectorValue].
 *
 * @param string The [String] description
 * @param vectorSeparator The character used to separate vector components.
 * @return [BooleanVectorValue]
 */
fun parseBooleanVectorValue(string: String, vectorSeparator: String = ";") = BooleanVectorValue(
    string.substring(1 until string.length - 1).split(vectorSeparator).map {
        it.toBoolean()
    }.toTypedArray()
)

/**
 * Converts [DoubleVectorValue] to a [String] description.
 *
 * @param vectorSeparator The character used to separate vector components.
 * @param max The maximum number of components to visualize.
 * @return [String]
 */
fun DoubleVectorValue.toDescription(vectorSeparator: String = ";", max: Int = 4) : String = if (this.logicalSize > max) {
    "${this.data.take(max - 1).joinToString(separator = vectorSeparator, prefix = "[")}..$vectorSeparator${this.last().value}]"
} else {
    this.data.joinToString(separator = vectorSeparator, prefix = "[", postfix = "]")
}

/**
 * Parses a [String] to a [DoubleVectorValue].
 *
 * @param string The [String] description
 * @param vectorSeparator The character used to separate vector components.
 * @return [DoubleVectorValue]
 */
fun parseDoubleVectorValue(string: String, vectorSeparator: String = ";") = DoubleVectorValue(
    string.substring(1 until string.length - 1).split(vectorSeparator).map {
        it.toDouble()
    }.toTypedArray()
)

/**
 * Converts [FloatVectorValue] to a [String] description.
 *
 * @param vectorSeparator The character used to separate vector components.
 * @param max The maximum number of components to visualize.
 * @return [String]
 */
fun FloatVectorValue.toDescription(vectorSeparator: String = ";", max: Int = 4) : String = if (this.logicalSize > max) {
    "${this.data.take(max - 1).joinToString(separator = vectorSeparator, prefix = "[")}..$vectorSeparator${this.last().value}]"
} else {
    this.data.joinToString(separator = vectorSeparator, prefix = "[", postfix = "]")
}

/**
 * Parses a [String] to a [FloatVectorValue].
 *
 * @param string The [String] description
 * @param vectorSeparator The character used to separate vector components.
 * @return [FloatVectorValue]
 */
fun parseFloatVectorValue(string: String, vectorSeparator: String = ";") = FloatVectorValue(
    string.substring(1 until string.length - 1).split(vectorSeparator).map {
        it.toFloat()
    }.toTypedArray()
)

/**
 * Converts [IntVectorValue] to a [String] description.
 *
 * @param vectorSeparator The character used to separate vector components.
 * @param max The maximum number of components to visualize.
 * @return [String]
 */
fun IntVectorValue.toDescription(vectorSeparator: String = ";", max: Int = 4) : String = if (this.logicalSize > max) {
    "${this.data.take(max - 1).joinToString(separator = vectorSeparator, prefix = "[")}..$vectorSeparator${this.last().value}]"
} else {
    this.data.joinToString(separator = vectorSeparator, prefix = "[", postfix = "]")
}

/**
 * Parses a [String] to a [IntVectorValue].
 *
 * @param string The [String] description
 * @param vectorSeparator The character used to separate vector components.
 * @return [IntVectorValue]
 */
fun parseIntVectorValue(string: String, vectorSeparator: String = ";") = IntVectorValue(
    string.substring(1 until string.length - 1).split(vectorSeparator).map {
        it.toInt()
    }.toTypedArray()
)

/**
 * Converts [LongVectorValue] to a [String] description.
 *
 * @param vectorSeparator The character used to separate vector components.
 * @param max The maximum number of components to visualize.
 * @return [String]
 */
fun LongVectorValue.toDescription(vectorSeparator: String = ";", max: Int = 4) : String = if (this.logicalSize > max) {
    "${this.data.take(max - 1).joinToString(separator = vectorSeparator, prefix = "[")}..$vectorSeparator${this.last().value}]"
} else {
    this.data.joinToString(separator = vectorSeparator, prefix = "[", postfix = "]")
}

/**
 * Parses a [String] to a [LongVectorValue].
 *
 * @param string The [String] description
 * @param vectorSeparator The character used to separate vector components.
 * @return [LongVectorValue]
 */
fun parseLongVectorValue(string: String, vectorSeparator: String = ";") = LongVectorValue(
    string.substring(1 until string.length - 1).split(vectorSeparator).map {
        it.toLong()
    }.toTypedArray()
)

/**
 * Converts [Complex32VectorValue] to a [String] description.
 *
 * @param vectorSeparator The character used to separate vector components.
 * @param max The maximum number of components to visualize.
 * @return [String]
 */
fun Complex32VectorValue.toDescription(vectorSeparator: String = ";", max: Int = 4) : String = if (this.logicalSize / 2 > max) {
    "${this.take(max - 1).joinToString(separator = vectorSeparator, prefix = "[") { (it as Complex32Value).toDescription() }}..$vectorSeparator${(this.last() as Complex32Value).toDescription()}]"
} else {
    this.joinToString(separator = vectorSeparator, prefix = "[", postfix = "]") { (it as Complex32Value).toDescription() }
}

/**
 * Parses a [String] to a [Complex32VectorValue].
 *
 * @param string The [String] description
 * @param vectorSeparator The character used to separate vector components.
 * @return [Complex32VectorValue]
 */
fun parseComplex32VectorValue(string: String, vectorSeparator: String = ";") = Complex32VectorValue(
    string.substring(1 until string.length - 1).split(vectorSeparator).map {
        parseComplex32Description(it)
    }.toTypedArray()
)

/**
 * Converts [Complex32VectorValue] to a [String] description.
 *
 * @param vectorSeparator The character used to separate vector components.
 * @param max The maximum number of components to visualize.
 * @return [String]
 */
fun Complex64VectorValue.toDescription(vectorSeparator: String = ";", max: Int = 4) : String = if (this.logicalSize / 2 > max) {
    "${this.take(max - 1).joinToString(separator = vectorSeparator, prefix = "[") { (it as Complex64Value).toDescription() }}..$vectorSeparator${(this.last() as Complex64Value).toDescription()}]"
} else {
    this.joinToString(separator = vectorSeparator, prefix = "[", postfix = "]") { (it as Complex64Value).toDescription() }
}

/**
 * Parses a [String] to a [Complex64VectorValue].
 *
 * @param string The [String] description
 * @param vectorSeparator The character used to separate vector components.
 * @return [Complex64VectorValue]
 */
fun parseComplex64VectorValue(string: String, vectorSeparator: String = ";") = Complex64VectorValue(
    string.substring(1 until string.length - 1).split(vectorSeparator).map {
        parseComplex64Description(it)
    }.toTypedArray()
)