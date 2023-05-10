package org.vitrivr.cottontail.core

import org.vitrivr.cottontail.client.iterators.Tuple
import org.vitrivr.cottontail.core.values.*

/**
 * Converts this [Tuple] to a CSV compatible [String] representation.
 *
 * @param separator The column separator to use (defaults to a comma).
 * @param componentSeparator The vector separator to use (default to semicolon)
 * @return [String]
 */
fun Tuple.toCsv(separator: String = ",", componentSeparator: String = ";") = (0..this.size()).map { this[it]?.toCsv() }.joinToString(separator)

/**
 * Converts this [PublicValue] to a CSV compatible [String] representation.
 *
 * @return [String]
 */
fun PublicValue.toCsv(vectorSeparator: String = ";") = when(this) {
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
    is BooleanVectorValue -> this.toCsv()
    is DoubleVectorValue -> this.toCsv()
    is FloatVectorValue -> this.toCsv()
    is IntVectorValue -> this.toCsv()
    is LongVectorValue -> this.toCsv()
    is Complex32VectorValue -> this.toCsv()
    is Complex64VectorValue -> this.toCsv()
    is ByteStringValue -> "<BLOB>" /* ByteStrings cannot be exported to CSV. */
}

/**
 * Converts [BooleanVectorValue] to a [String] representation that can be used in a CSV.
 *
 * @param vectorSeparator The character used to separate vector components.
 *
 * @return [String]
 */
fun BooleanVectorValue.toCsv(vectorSeparator: String = ";") : String = this.data.joinToString(separator = vectorSeparator, prefix = "[", postfix = "]")

/**
 * Converts [DoubleVectorValue] to a [String] representation that can be used in a CSV.
 *
 * @param vectorSeparator The character used to separate vector components.
 *
 * @return [String]
 */
fun DoubleVectorValue.toCsv(vectorSeparator: String = ";") : String = this.data.joinToString(separator = vectorSeparator, prefix = "[", postfix = "]")

/**
 * Converts [FloatVectorValue] to a [String] representation that can be used in a CSV.
 *
 * @param vectorSeparator The character used to separate vector components.
 *
 * @return [String]
 */
fun FloatVectorValue.toCsv(vectorSeparator: String = ";") : String = this.data.joinToString(separator = vectorSeparator, prefix = "[", postfix = "]")

/**
 * Converts [IntVectorValue] to a [String] representation that can be used in a CSV.
 *
 * @param vectorSeparator The character used to separate vector components.
 *
 * @return [String]
 */
fun IntVectorValue.toCsv(vectorSeparator: String = ";") : String = this.data.joinToString(separator = vectorSeparator, prefix = "[", postfix = "]")

/**
 * Converts [LongVectorValue] to a [String] representation that can be used in a CSV.
 *
 * @param vectorSeparator The character used to separate vector components.
 *
 * @return [String]
 */
fun LongVectorValue.toCsv(vectorSeparator: String = ";") : String = this.data.joinToString(separator = vectorSeparator, prefix = "[", postfix = "]")

/**
 * Converts [Complex32VectorValue] to a [String] representation that can be used in a CSV.
 *
 * @param vectorSeparator The character used to separate vector components.
 * @return [String]
 */
fun Complex32VectorValue.toCsv(vectorSeparator: String = ";") : String =
    this.joinToString(separator = vectorSeparator, prefix = "[", postfix = "]") { (it as Complex32Value).toDescription() }

/**
 * Converts [Complex32VectorValue] to a [String] representation that can be used in a CSV.
 *
 * @param vectorSeparator The character used to separate vector components.
 * @return [String]
 */
fun Complex64VectorValue.toCsv(vectorSeparator: String = ";") : String =
    this.joinToString(separator = vectorSeparator, prefix = "[", postfix = "]") { (it as Complex64Value).toDescription() }
