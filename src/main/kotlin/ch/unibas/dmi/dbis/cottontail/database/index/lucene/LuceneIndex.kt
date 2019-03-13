package ch.unibas.dmi.dbis.cottontail.database.index.lucene

import ch.unibas.dmi.dbis.cottontail.database.column.ColumnType
import ch.unibas.dmi.dbis.cottontail.database.column.DoubleColumnType
import ch.unibas.dmi.dbis.cottontail.database.column.FloatColumnType
import ch.unibas.dmi.dbis.cottontail.database.entity.Entity
import ch.unibas.dmi.dbis.cottontail.database.general.begin
import ch.unibas.dmi.dbis.cottontail.database.index.Index
import ch.unibas.dmi.dbis.cottontail.database.index.IndexType
import ch.unibas.dmi.dbis.cottontail.database.queries.*
import ch.unibas.dmi.dbis.cottontail.model.basics.ColumnDef
import ch.unibas.dmi.dbis.cottontail.model.basics.Record
import ch.unibas.dmi.dbis.cottontail.model.exceptions.QueryException
import ch.unibas.dmi.dbis.cottontail.model.recordset.Recordset
import ch.unibas.dmi.dbis.cottontail.model.recordset.StandaloneRecord
import ch.unibas.dmi.dbis.cottontail.model.values.FloatValue
import ch.unibas.dmi.dbis.cottontail.model.values.Value

import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document.Document
import org.apache.lucene.document.Field
import org.apache.lucene.document.NumericDocValuesField
import org.apache.lucene.document.TextField
import org.apache.lucene.index.*
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.store.Directory
import org.apache.lucene.store.MMapDirectory

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
        /** Name of the tuple ID field. */
        const val FIELD_NAME_TID = "_tid"

        /** Name of the tuple ID field. */
        val SCORE_COLUMN = ColumnDef("lucene_score", FloatColumnType())

        /**
         * Maps a [Record] to a [Document] that can be processed by Lucene.
         *
         * @param record The [Record]
         * @return The resulting [Document]
         */
        private fun document(record: Record): Document = Document().apply {
            add(NumericDocValuesField(FIELD_NAME_TID, record.tupleId))
            record.columns.forEach {
                val value = record[it]?.value as? String
                if(value != null){
                    add(TextField(it.name, value, Field.Store.NO))
                }
            }
        }
    }

    override val path: Path = this.parent.path.resolve("idx_lucene_$name")

    override val type: IndexType = IndexType.LUCENE

    @Volatile
    override var closed: Boolean = false
        private set

    private val directory: Directory = MMapDirectory.open(this.path)
    private val analyzer: Analyzer = StandardAnalyzer()
    private val indexWriter: IndexWriter = IndexWriter(this.directory, IndexWriterConfig(this.analyzer).setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND))
    private val indexReader: IndexReader = DirectoryReader.open(this.directory)
    private val indexSearcher: IndexSearcher = IndexSearcher(this.indexReader)


    /**
     * (Re-)builds the [LuceneIndex].
     */
    override fun rebuild() {
        indexWriter.deleteAll()
        this.parent.Tx(readonly = true, columns = this.columns).begin { tx ->
            tx.forEach {
                indexWriter.addDocument(document(it))
            }
            indexWriter.flush()
            indexWriter.commit()
            true
        }
        indexWriter.close()
    }

    /**
     * Closes this [LuceneIndex] and the associated data structures.
     */
    override fun close() = this.globalLock.write {
        if (!this.closed) {
            this.indexReader.close()
            this.indexWriter.close()
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
        if (!predicate.columns.all { this.columns.contains(it) })
            throw QueryException.UnsupportedPredicateException("Index '${this.fqn}' (lucene-index) is lacking certain fields the provided predicate requires.")

        if (!predicate.allAtomic().all { it.operator == ComparisonOperator.LIKE || it.operator == ComparisonOperator.EQUAL})
            throw QueryException.UnsupportedPredicateException("Index '${this.fqn}' (lucene-index) can only process LIKE comparisons.")

        /* Generate query. */
        val query = when (predicate) {
            is AtomicBooleanPredicate<*> -> predicate.toLuceneQuery()
            is CompoundBooleanPredicate -> predicate.toLuceneQuery()
        }

        /* Construct empty Recordset. */
        val resultset = Recordset(columns = arrayOf(SCORE_COLUMN))

        /* Execute query and add results. */
        val results = this.indexSearcher.search(query, Integer.MAX_VALUE)
        results.scoreDocs.forEach {
            resultset.addRow(tupleId = this.indexSearcher.doc(it.doc)[FIELD_NAME_TID].toLong(), values = arrayOf(FloatValue(it.score)))
        }
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
        if (!predicate.columns.all { this.columns.contains(it) })
            throw QueryException.UnsupportedPredicateException("Index '${this.fqn}' (lucene-index) is lacking certain fields the provided predicate requires.")

        if (!predicate.allAtomic().all { it.operator == ComparisonOperator.LIKE || it.operator == ComparisonOperator.EQUAL})
            throw QueryException.UnsupportedPredicateException("Index '${this.fqn}' (lucene-index) can only process LIKE comparisons.")

        /* Generate query. */
        val query = when (predicate) {
            is AtomicBooleanPredicate<*> -> predicate.toLuceneQuery()
            is CompoundBooleanPredicate -> predicate.toLuceneQuery()
        }

        /* Execute query and add results. */
        val results = this.indexSearcher.search(query, Integer.MAX_VALUE)
        results.scoreDocs.forEach {
            action(StandaloneRecord(tupleId = this.indexSearcher.doc(it.doc)[FIELD_NAME_TID].toLong(), columns = arrayOf(SCORE_COLUMN)).assign(arrayOf(FloatValue(it.score))))
        }
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
        if (!predicate.columns.all { this.columns.contains(it) })
            throw QueryException.UnsupportedPredicateException("Index '${this.fqn}' (lucene-index) is lacking certain fields the provided predicate requires.")

        if (!predicate.allAtomic().all { it.operator == ComparisonOperator.LIKE || it.operator == ComparisonOperator.EQUAL})
            throw QueryException.UnsupportedPredicateException("Index '${this.fqn}' (lucene-index) can only process LIKE comparisons.")

        /* Generate query. */
        val query = when (predicate) {
            is AtomicBooleanPredicate<*> -> predicate.toLuceneQuery()
            is CompoundBooleanPredicate -> predicate.toLuceneQuery()
        }

        /* Execute query and add results. */
        val results = this.indexSearcher.search(query, Integer.MAX_VALUE)
        results.scoreDocs.map {
            action(StandaloneRecord(tupleId = this.indexSearcher.doc(it.doc)[FIELD_NAME_TID].toLong(), columns = arrayOf(SCORE_COLUMN)).assign(arrayOf(FloatValue(it.score))))
        }
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
            if (!predicate.allAtomic().all { it.operator == ComparisonOperator.LIKE }) return false
            return true
        } else {
            return false
        }
    }
}