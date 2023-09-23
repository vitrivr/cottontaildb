package org.vitrivr.cottontail.core.values.pattern

import org.vitrivr.cottontail.core.values.StringValue

/**
 * [Pattern]s can be used to check, if the expression they contain matches a given [StringValue].
 * They can be used in LIKE and MATCH queries.
 *
 * @author Ralph Gasser
 * @param 1.2.0
 */
sealed class Pattern(val pattern: String) {
    /**
     * Checks if the given [StringValue] matches this [Pattern].
     *
     * @param value [StringValue] to match.
     * @return True on match, false otherwise.
     */
    abstract fun matches(value: StringValue): Boolean
}
