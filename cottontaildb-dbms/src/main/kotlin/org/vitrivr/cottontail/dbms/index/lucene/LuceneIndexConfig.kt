package org.vitrivr.cottontail.dbms.index.lucene

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.vitrivr.cottontail.dbms.index.IndexConfig

/**
 * A configuration class used with [LuceneIndex] instances.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
data class LuceneIndexConfig(val analyzer: LuceneAnalyzerType) : IndexConfig {
    companion object Serializer : org.mapdb.Serializer<LuceneIndexConfig> {
        const val ANALYZER_TYPE_KEY = "analyzer_type"

        override fun serialize(out: DataOutput2, value: LuceneIndexConfig) {
            out.packInt(value.analyzer.ordinal)
        }

        override fun deserialize(input: DataInput2, available: Int) =
            LuceneIndexConfig(LuceneAnalyzerType.values()[input.unpackInt()])

        /**
         * Constructs a [LuceneIndexConfig] from a parameter map.
         *
         * @param params The parameter map.
         * @return [LuceneIndexConfig]
         */
        fun fromParamMap(params: Map<String, String>) = LuceneIndexConfig(
            try {
                LuceneAnalyzerType.valueOf(params[ANALYZER_TYPE_KEY] ?: "")
            } catch (e: IllegalArgumentException) {
                LuceneAnalyzerType.STANDARD
            }
        )
    }

    /**
     * Converts this [LuceneIndexConfig] to a [Map] representation.
     *
     * @return [Map] representation of this [LuceneIndexConfig].
     */
    override fun toMap(): Map<String, String> = mapOf(
        ANALYZER_TYPE_KEY to this.analyzer.toString()
    )
}