package org.vitrivr.cottontail.core.values.pattern

import org.vitrivr.cottontail.core.values.StringValue

/**
 * A [Pattern] that corresponds to a Apache Lucene query string. Requires a Apache Lucene
 * based index for filtering, i.e., direct matching is not possible.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class LucenePattern(value: String) : Pattern(value) {
    /**
     * Checks if the given [StringValue] matches this [LucenePattern]. Always throws an [UnsupportedOperationException]
     *
     * @param value [StringValue] to match.
     * @throws [UnsupportedOperationException]
     */
    override fun matches(value: StringValue): Boolean {
        throw UnsupportedOperationException("A LucenePatternValue cannot be used to match values directly; lucene index required.")
    }
}