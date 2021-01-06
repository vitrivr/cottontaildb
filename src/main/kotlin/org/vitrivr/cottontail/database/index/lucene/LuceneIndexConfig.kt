package org.vitrivr.cottontail.database.index.lucene

import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.core.SimpleAnalyzer
import org.apache.lucene.analysis.core.WhitespaceAnalyzer
import org.apache.lucene.analysis.en.EnglishAnalyzer
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.mapdb.DataInput2
import org.mapdb.DataOutput2

/**
 * A configuration class used with [LuceneIndex] instances.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
data class LuceneIndexConfig(val type: LuceneAnalyzerType) {
    companion object Serializer : org.mapdb.Serializer<LuceneIndexConfig> {
        override fun serialize(out: DataOutput2, value: LuceneIndexConfig) {
            out.packInt(value.type.ordinal)
        }

        override fun deserialize(input: DataInput2, available: Int) = LuceneIndexConfig(LuceneAnalyzerType.values()[input.unpackInt()])
    }

    /**
     * Returns an [Analyzer] instance for this [LuceneIndexConfig].
     *
     * @return [Analyzer]
     */
    fun getAnalyzer(): Analyzer = when (this.type) {
        LuceneAnalyzerType.STANDARD -> StandardAnalyzer()
        LuceneAnalyzerType.SIMPLE -> SimpleAnalyzer()
        LuceneAnalyzerType.WHITESPACE -> WhitespaceAnalyzer()
        LuceneAnalyzerType.ENGLISH -> EnglishAnalyzer()
        LuceneAnalyzerType.SOUNDEX -> SoundexAnalyzer()
    }
}