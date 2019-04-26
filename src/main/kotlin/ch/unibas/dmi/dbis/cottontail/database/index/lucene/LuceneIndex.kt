package ch.unibas.dmi.dbis.cottontail.database.index.lucene

import ch.unibas.dmi.dbis.cottontail.database.column.*
import ch.unibas.dmi.dbis.cottontail.database.entity.Entity
import ch.unibas.dmi.dbis.cottontail.database.general.begin
import ch.unibas.dmi.dbis.cottontail.database.index.Index
import ch.unibas.dmi.dbis.cottontail.database.index.IndexType
import ch.unibas.dmi.dbis.cottontail.database.index.hash.UniqueHashIndex
import ch.unibas.dmi.dbis.cottontail.database.queries.*
import ch.unibas.dmi.dbis.cottontail.model.basics.ColumnDef
import ch.unibas.dmi.dbis.cottontail.model.basics.Record
import ch.unibas.dmi.dbis.cottontail.model.exceptions.QueryException
import ch.unibas.dmi.dbis.cottontail.model.recordset.Recordset
import ch.unibas.dmi.dbis.cottontail.model.recordset.StandaloneRecord
import ch.unibas.dmi.dbis.cottontail.model.values.*

import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document.*
import org.apache.lucene.index.*
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.store.Directory
import org.apache.lucene.store.FSDirectory
import org.apache.lucene.store.MMapDirectory
import org.apache.lucene.store.NativeFSLockFactory

import java.nio.file.Path
import kotlin.concurrent.write

/**
 * Represents a Apache Lucene based index in the Cottontail DB data model. The [LuceneIndex] allows for string comparisons using the LIKE operator.
 *
 * @author Luca Rossetto & Ralph Gasser
 * @version 1.0
 */
internal class LuceneIndex(override val name: String, override val parent: Entity, override val columns: Array<ColumnDef<*>>) : Index() {


    companion object {
        const val ATOMIC_COST = 0.01f /** Cost of a single lookup. TODO: Determine real value. */

        /** [ColumnDef] of the _tid column. */
        val TID_COLUMN = ColumnDef("_tid", LongColumnType())

        /** [ColumnDef] of the score column. */
        val SCORE_COLUMN = ColumnDef("score", FloatColumnType())

        /**
         * Maps a [Record] to a [Document] that can be processed by Lucene.
         *
         * @param record The [Record]
         * @return The resulting [Document]
         */
        private fun documentFromRecord(record: Record): Document = Document().apply {
            add(NumericDocValuesField(TID_COLUMN.name, record.tupleId))
            add(StoredField(TID_COLUMN.name, record.tupleId))
            record.columns.forEach {
                val value = record[it]?.value as? String
                if (value != null) {
                    add(TextField(it.name, value, Field.Store.NO))
                }
            }
        }
    }

    /** The [LuceneIndex] implementation produces an additional score column. */
    override val produces: Array<ColumnDef<*>> = arrayOf(SCORE_COLUMN)

    override val path: Path = this.parent.path.resolve("idx_lucene_$name")

    override val type: IndexType = IndexType.LUCENE

    @Volatile
    override var closed: Boolean = false
        private set

    /** The [Directory] containing the data for this [LuceneIndex]. */
    private val directory: Directory = FSDirectory.open(this.path, NativeFSLockFactory.getDefault())

    /** Initial commit in case writer was created. */
    init {
        val writer = IndexWriter(this.directory, IndexWriterConfig(StandardAnalyzer()).setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND).setCommitOnClose(true))
        writer.close()
    }

    /**
     * (Re-)builds the [LuceneIndex].
     */
    override fun rebuild() {
        val writer = IndexWriter(this.directory, IndexWriterConfig(StandardAnalyzer()).setOpenMode(IndexWriterConfig.OpenMode.APPEND).setCommitOnClose(true))
        writer.deleteAll()
        this.parent.Tx(readonly = true, columns = this.columns).begin { tx ->
            tx.forEach (parallelism = 2) {
                writer.addDocument(documentFromRecord(it))
            }
            true
        }
        writer.close()
    }

    /**
     * Closes this [LuceneIndex] and the associated data structures.
     */
    override fun close() = this.globalLock.write {
        if (!this.closed) {
            this.directory.close()
            this.closed = true
        }
    }

    /**
     * Performs a lookup through this [LuceneIndex] and returns [Recordset] containing only the [Record]'s tuple ID. This is an internal method
     * External invocation is only possible through a [Index.Tx] object.
     *
     * @param predicate The [Predicate] to perform the lookup. Must be a LIKE query.
     * @return The resulting [Recordset].
     *
     * @throws DatabaseException.PredicateNotSupportedBxIndexException If predicate is not supported by [Index].
     */
    override fun filter(predicate: Predicate): Recordset = if (predicate is BooleanPredicate) {
        val indexReader = DirectoryReader.open(this.directory)
        val indexSearcher = IndexSearcher(indexReader)
        if (!predicate.columns.all { this.columns.contains(it) })
            throw QueryException.UnsupportedPredicateException("Index '${this.fqn}' (lucene-index) is lacking certain fields the provided predicate requires.")

        if (!predicate.atomics.all { it.operator == ComparisonOperator.LIKE || it.operator == ComparisonOperator.EQUAL})
            throw QueryException.UnsupportedPredicateException("Index '${this.fqn}' (lucene-index) can only process LIKE comparisons.")

        /* Generate query. */
        val query = when (predicate) {
            is AtomicBooleanPredicate<*> -> predicate.toLuceneQuery()
            is CompoundBooleanPredicate -> predicate.toLuceneQuery()
        }

        /* Construct empty Recordset. */
        val resultset = Recordset(columns = arrayOf(*this.produces))

        /* Execute query and add results. */
        val results = indexSearcher.search(query, Integer.MAX_VALUE)
        results.scoreDocs.forEach {sdoc ->
            val doc = indexSearcher.doc(sdoc.doc)
            resultset.addRow(doc[TID_COLUMN.name].toLong(), values = arrayOf(FloatValue(sdoc.score)))
        }
        indexReader.close()
        resultset
    } else {
        throw QueryException.UnsupportedPredicateException("Index '${this.fqn}' (lucene-index) does not support predicates of type '${predicate::class.simpleName}'.")
    }

    /**
     * Applies the given action to all the [LuceneIndex] entries that match the given [Predicate]. This is an internal method!
     * External invocation is only possible through a [Index.Tx] object.
     *
     * @param predicate The [Predicate] to perform the lookup.
     * @param action The action that should be applied.
     *
     * @throws DatabaseException.PredicateNotSupportedBxIndexException If predicate is not supported by [Index].
     */
    override fun forEach(predicate: Predicate, action: (Record) -> Unit) = if (predicate is BooleanPredicate) {
        val indexReader = DirectoryReader.open(this.directory)
        val indexSearcher = IndexSearcher(indexReader)

        if (!predicate.columns.all { this.columns.contains(it) })
            throw QueryException.UnsupportedPredicateException("Index '${this.fqn}' (lucene-index) is lacking certain fields the provided predicate requires.")

        if (!predicate.atomics.all { it.operator == ComparisonOperator.LIKE || it.operator == ComparisonOperator.EQUAL})
            throw QueryException.UnsupportedPredicateException("Index '${this.fqn}' (lucene-index) can only process LIKE comparisons.")

        /* Generate query. */
        val query = when (predicate) {
            is AtomicBooleanPredicate<*> -> predicate.toLuceneQuery()
            is CompoundBooleanPredicate -> predicate.toLuceneQuery()
        }

        /* Execute query and add results. */
        val results = indexSearcher.search(query, Integer.MAX_VALUE)
        results.scoreDocs.forEach {sdoc ->
            val doc = indexSearcher.doc(sdoc.doc)
            action(StandaloneRecord(tupleId = doc[TID_COLUMN.name].toLong(), columns = arrayOf(*this.produces)).assign(arrayOf(FloatValue(sdoc.score))))
        }
        indexReader.close()
    } else {
        throw QueryException.UnsupportedPredicateException("Index '${this.fqn}' (lucene-index) does not support predicates of type '${predicate::class.simpleName}'.")
    }

    /**
     * Applies the given mapping function to all the [LuceneIndex] entries that match the given [Predicate]. This is an internal method!
     * External invocation is only possible through a [Index.Tx] object.
     *
     * @param predicate The [Predicate] to perform the lookup.
     * @param action The action that should be applied.
     *
     * @throws DatabaseException.PredicateNotSupportedBxIndexException If predicate is not supported by [Index].
     */
    override fun <R> map(predicate: Predicate, action: (Record) -> R): Collection<R> = if (predicate is BooleanPredicate) {
        val indexReader = DirectoryReader.open(this.directory)
        val indexSearcher = IndexSearcher(indexReader)

        if (!predicate.columns.all { this.columns.contains(it) })
            throw QueryException.UnsupportedPredicateException("Index '${this.fqn}' (lucene-index) is lacking certain fields the provided predicate requires.")

        if (!predicate.atomics.all { it.operator == ComparisonOperator.LIKE || it.operator == ComparisonOperator.EQUAL})
            throw QueryException.UnsupportedPredicateException("Index '${this.fqn}' (lucene-index) can only process LIKE comparisons.")

        /* Generate query. */
        val query = when (predicate) {
            is AtomicBooleanPredicate<*> -> predicate.toLuceneQuery()
            is CompoundBooleanPredicate -> predicate.toLuceneQuery()
        }

        /* Execute query and add results. */
        val results = indexSearcher.search(query, Integer.MAX_VALUE).scoreDocs.map {sdoc ->
            val doc = indexSearcher.doc(sdoc.doc)
            action(StandaloneRecord(tupleId = doc[TID_COLUMN.name].toLong(), columns = arrayOf(*this.produces)).assign(arrayOf(FloatValue(sdoc.score))))
        }
        indexReader.close()
        results
    } else {
        throw QueryException.UnsupportedPredicateException("Index '${this.fqn}' (lucene-index) does not support predicates of type '${predicate::class.simpleName}'.")
    }

    /**
     * Checks if this [LuceneIndex] can process the given [Predicate].
     *
     * @param predicate [Predicate] to test.
     * @return True if [Predicate] can be processed, false otherwise.
     */
    override fun canProcess(predicate: Predicate): Boolean {
        if (predicate is BooleanPredicate) {
            if (!predicate.columns.all { this.columns.contains(it) }) return false
            if (!predicate.atomics.all { it.operator == ComparisonOperator.LIKE  || it.operator == ComparisonOperator.EQUAL}) return false
            return true
        } else {
            return false
        }
    }

    /**
     * Calculates the cost estimate of this [UniqueHashIndex] processing the provided [Predicate].
     *
     * @param predicate [Predicate] to check.
     * @return Cost estimate for the [Predicate]
     */
    override fun cost(predicate: Predicate): Float = when {
        !canProcess(predicate) -> Float.MAX_VALUE
        else -> predicate.columns.size * ATOMIC_COST
    }
}