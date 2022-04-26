package org.vitrivr.cottontail.dbms.index.lucene

import jetbrains.exodus.bindings.ComparableBinding
import jetbrains.exodus.bindings.IntegerBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.dbms.index.IndexConfig
import java.io.ByteArrayInputStream

/**
 * A configuration class used with [LuceneIndex] instances.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
data class LuceneIndexConfig(val analyzer: LuceneAnalyzerType) : IndexConfig<LuceneIndex> {

    companion object {
        const val KEY_ANALYZER_TYPE_KEY = "analyzer_type"
    }

    /**
     * [ComparableBinding] for [LuceneIndexConfig].
     */
    object Binding: ComparableBinding() {
        override fun readObject(stream: ByteArrayInputStream): Comparable<LuceneIndexConfig> = LuceneIndexConfig(LuceneAnalyzerType.values()[IntegerBinding.readCompressed(stream)])

        override fun writeObject(output: LightOutputStream, `object`: Comparable<LuceneIndexConfig>) {
            require(`object` is LuceneIndexConfig) { "LuceneIndexConfig.Binding can only be used to serialize instances of LuceneIndexConfig." }
            IntegerBinding.writeCompressed(output, `object`.analyzer.ordinal)
        }
    }
}