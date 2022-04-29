package org.vitrivr.cottontail.dbms.index.lucene

import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.core.SimpleAnalyzer
import org.apache.lucene.analysis.core.WhitespaceAnalyzer
import org.apache.lucene.analysis.en.EnglishAnalyzer
import org.apache.lucene.analysis.standard.StandardAnalyzer


/**
 * An enumeration of the different analyzer types that can be used by [LuceneIndex].
 *
 * @author Luca Rossetto & Ralph Gasser
 * @version 1.0.0
 */
enum class LuceneAnalyzerType {
    STANDARD,
    SIMPLE,
    WHITESPACE,
    ENGLISH,
    SOUNDEX;

    /**
     * Creates and returns an [Analyzer] instance for this [LuceneAnalyzerType].
     *
     * @return [Analyzer]
     */
    fun get(): Analyzer = when (this) {
        STANDARD -> StandardAnalyzer()
        SIMPLE -> SimpleAnalyzer()
        WHITESPACE -> WhitespaceAnalyzer()
        ENGLISH -> EnglishAnalyzer()
        SOUNDEX -> SoundexAnalyzer()
    }
}