package org.vitrivr.cottontail.dbms.index.lucene

import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.search.Query
import org.vitrivr.cottontail.core.basics.Cursor
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.tuple.StandaloneTuple
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.storage.lucene.XodusDirectory

/**
 * A [Cursor] implementation for the [LuceneIndex].
 *
 * @author Ralph Gasser
 * @version 1.3.0
 */
class LuceneCursor(index: LuceneIndex.Tx, private val query: Query, private val columns: Array<ColumnDef<*>>): Cursor<Tuple> {

    /** The sub-transaction used by this [LuceneCursor]. */
    private val xodusTx = index.xodusTx.snapshot

    /** The [LuceneIndexDataStore] backing this [LuceneIndex]. */
    private val store = LuceneIndexDataStore(XodusDirectory(index.dbo.name.toString(), this.xodusTx), this.columns[0].name)

    /** Number of [TupleId]s returned by this [Iterator]. */
    @Volatile
    private var returned = 0

    /** [IndexSearcher] instance used for lookup. */
    private val searcher = IndexSearcher(this.store.indexReader)

    /** Execute query and add results. */
    private val results = this.searcher.search(this.query, Integer.MAX_VALUE)

    override fun moveNext(): Boolean = this.returned < this.results.totalHits.value

    override fun key(): TupleId {
        val scores = this.results.scoreDocs[this.returned]
        val doc =  this.searcher.storedFields().document(scores.doc)
        return doc[LuceneIndex.TID_COLUMN].toLong()
    }

    override fun value(): Tuple {
        val scores = this.results.scoreDocs[this.returned++]
        val doc =  this.searcher.storedFields().document(scores.doc)
        return StandaloneTuple(doc[LuceneIndex.TID_COLUMN].toLong(), this.columns, arrayOf(DoubleValue(scores.score)))
    }

    /**
     * Closes this [LuceneCursor] and releases all resources.
     */
    override fun close() {
        this.store.close()
        this.xodusTx.abort()
    }
}