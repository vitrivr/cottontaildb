package org.vitrivr.cottontail.dbms.index.lucene

import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document.*
import org.apache.lucene.index.*
import org.apache.lucene.queryparser.flexible.standard.QueryParserUtil
import org.apache.lucene.search.*
import org.apache.lucene.search.similarities.SimilarityBase.log2
import org.apache.lucene.store.Directory
import org.vitrivr.cottontail.core.basics.Cursor
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.core.queries.predicates.BooleanPredicate
import org.vitrivr.cottontail.core.queries.predicates.ComparisonOperator
import org.vitrivr.cottontail.core.queries.predicates.Predicate
import org.vitrivr.cottontail.core.recordset.StandaloneRecord
import org.vitrivr.cottontail.core.values.FloatValue
import org.vitrivr.cottontail.core.values.StringValue
import org.vitrivr.cottontail.core.values.pattern.LikePatternValue
import org.vitrivr.cottontail.core.values.pattern.LucenePatternValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.dbms.catalogue.entries.IndexCatalogueEntry
import org.vitrivr.cottontail.dbms.entity.DefaultEntity
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.exceptions.QueryException
import org.vitrivr.cottontail.dbms.execution.TransactionContext
import org.vitrivr.cottontail.dbms.index.AbstractIndex
import org.vitrivr.cottontail.dbms.index.IndexState
import org.vitrivr.cottontail.dbms.index.IndexTx
import org.vitrivr.cottontail.dbms.index.IndexType
import org.vitrivr.cottontail.dbms.index.hash.UniqueHashIndex
import org.vitrivr.cottontail.dbms.index.lsh.superbit.SuperBitLSHIndex
import org.vitrivr.cottontail.dbms.operations.Operation

/**
 * An Apache Lucene based [AbstractIndex]. The [LuceneIndex] allows for fast search on text using the EQUAL or LIKE operator.
 *
 * @author Luca Rossetto & Ralph Gasser
 * @version 3.0.0
 */
class LuceneIndex(name: Name.IndexName, parent: DefaultEntity) : AbstractIndex(name, parent) {

    companion object {
        /** [ColumnDef] of the _tid column. */
        const val TID_COLUMN = "_tid"
    }

    /** The [LuceneIndex] implementation produces an additional score column. */
    override val produces: Array<ColumnDef<*>> = arrayOf(ColumnDef(this.parent.name.column("score"), Types.Float))

    /** True since [SuperBitLSHIndex] supports incremental updates. */
    override val supportsIncrementalUpdate: Boolean = true

    /** False, since [LuceneIndex] does not support partitioning. */
    override val supportsPartitioning: Boolean = false

    /** The type of this [AbstractIndex]. */
    override val type: IndexType = IndexType.LUCENE

    /** The [LuceneIndexConfig] used by this [LuceneIndex] instance. */
    override val config: LuceneIndexConfig = this.catalogue.environment.computeInTransaction { tx ->
        val entry = IndexCatalogueEntry.read(this.name, this.parent.parent.parent, tx) ?: throw DatabaseException.DataCorruptionException("Failed to read catalogue entry for index ${this.name}.")
        LuceneIndexConfig.fromParamMap(entry.config)
    }

    /** The [Directory] containing the data for this [LuceneIndex]. */
    private val directory: Directory = TODO()

    /**
     * Checks if this [LuceneIndex] can process the given [Predicate].
     *
     * @param predicate [Predicate] to test.
     * @return True if [Predicate] can be processed, false otherwise.
     */
    override fun canProcess(predicate: Predicate): Boolean =
        predicate is BooleanPredicate &&
                predicate.columns.all { it in this.columns } &&
                predicate.atomics.all { it.operator is ComparisonOperator.Binary.Like || it.operator is ComparisonOperator.Binary.Equal || it.operator is ComparisonOperator.Binary.Match }

    /**
     * Calculates the cost estimate of this [UniqueHashIndex] processing the provided [Predicate].
     *
     * @param predicate [Predicate] to check.
     * @return Cost estimate for the [Predicate]
     */
    override fun cost(predicate: Predicate): Cost = when {
        canProcess(predicate) -> {
            val reader = DirectoryReader.open(this.directory)
            var cost = Cost.ZERO
            repeat(predicate.columns.size) {
                cost += (Cost.DISK_ACCESS_READ +  Cost.MEMORY_ACCESS) * log2(reader.numDocs().toDouble()) /* TODO: This is an assumption. */
            }
            cost
        }
        else -> Cost.INVALID
    }

    /**
     * Opens and returns a new [IndexTx] object that can be used to interact with this [AbstractIndex].
     *
     * @param context If the [TransactionContext] to create the [IndexTx] for.
     */
    override fun newTx(context: TransactionContext): IndexTx = Tx(context)

    /**
     * Closes this [LuceneIndex]
     */
    override fun close() {
        /* No op. */
    }

    /**
     * Converts a [Record] to a [Document] that can be processed by Lucene.
     *
     * @param record The [Record]
     * @return The resulting [Document]
     */
    private fun documentFromRecord(record: Record): Document {
        val value = record[this.columns[0]]
        if (value is StringValue) {
            return documentFromValue(value, record.tupleId)
        } else {
            throw IllegalArgumentException("Given record does not contain a StringValue column named ${this.columns[0].name}.")
        }
    }

    /**
     * Converts a [StringValue] and a [TupleId] to [Document] that can be processed by Lucene.
     *
     * @param value: [StringValue] to process
     * @param tupleId The [TupleId] to process
     * @return The resulting [Document]
     */
    private fun documentFromValue(value: StringValue, tupleId: TupleId): Document {
        val doc = Document()
        doc.add(NumericDocValuesField(TID_COLUMN, tupleId))
        doc.add(StoredField(TID_COLUMN, tupleId))
        doc.add(TextField("${this.columns[0].name}_txt", value.value, Field.Store.NO))
        doc.add(StringField("${this.columns[0].name}_str", value.value, Field.Store.NO))
        return doc
    }

    /**
     * Converts a [BooleanPredicate] to a [Query] supported by Apache Lucene.
     *
     * @return [Query]
     */
    private fun BooleanPredicate.toLuceneQuery(): Query = when (this) {
        is BooleanPredicate.Atomic -> this.toLuceneQuery()
        is BooleanPredicate.Compound -> this.toLuceneQuery()
    }

    /**
     * Converts an [BooleanPredicate.Atomic] to a [Query] supported by Apache Lucene.
     * Conversion differs slightly depending on the [ComparisonOperator].
     *
     * @return [Query]
     */
    private fun BooleanPredicate.Atomic.toLuceneQuery(): Query {
        val op = this.operator
        if (op !is ComparisonOperator.Binary) {
            throw QueryException("Conversion to Lucene query failed: Only binary operators are supported.")
        }

        /* Left and right-hand side of boolean predicate */
        val left = op.left
        val right = op.right
        val column = if (right is Binding.Column && right.column == this@LuceneIndex.columns.first()) {
            right.column
        } else if (left is Binding.Column && left.column == this@LuceneIndex.columns.first()) {
            left.column
        } else {
            throw QueryException("Conversion to Lucene query failed: One side of the comparison operator must be a column value!")
        }
        val literal: Value = if (right is Binding.Literal) {
            right.value ?: throw QueryException("Conversion to Lucene query failed: Literal value cannot be null!")
        } else if (left is Binding.Literal) {
            right.value ?: throw QueryException("Conversion to Lucene query failed: Literal value cannot be null!")
        } else {
            throw QueryException("Conversion to Lucene query failed: One side of the comparison operator must be a literal value!")
        }

        return when (op) {
            is ComparisonOperator.Binary.Equal -> {
                if (literal is StringValue) {
                    TermQuery(Term("${column.name}_str", literal.value))
                } else {
                    throw QueryException("Conversion to Lucene query failed: EQUAL queries strictly require a StringValue as second operand!")
                }
            }
            is ComparisonOperator.Binary.Like -> {
                when (literal) {
                    is LucenePatternValue -> QueryParserUtil.parse(
                        arrayOf(literal.value),
                        arrayOf("${column.name}_txt"),
                        StandardAnalyzer()
                    )
                    is LikePatternValue -> QueryParserUtil.parse(
                        arrayOf(literal.toLucene().value),
                        arrayOf("${column.name}_txt"),
                        StandardAnalyzer()
                    )
                    else -> throw throw QueryException("Conversion to Lucene query failed: LIKE queries require a LucenePatternValue OR LikePatternValue as second operand!")
                }
            }
            is ComparisonOperator.Binary.Match -> {
                if (literal is LucenePatternValue) {
                    QueryParserUtil.parse(arrayOf(literal.value), arrayOf("${column.name}_txt"), StandardAnalyzer())
                } else {
                    throw throw QueryException("Conversion to Lucene query failed: MATCH queries strictly require a LucenePatternValue as second operand!")
                }
            }
            else -> throw QueryException("Lucene Query Conversion failed: Only EQUAL, MATCH and LIKE queries can be mapped to a Apache Lucene!")
        }
    }

    /**
     * Converts a [BooleanPredicate.Compound] to a [Query] supported by Apache Lucene.
     *
     * @return [Query]
     */
    private fun BooleanPredicate.Compound.toLuceneQuery(): Query {
        val clause = when (this) {
            is BooleanPredicate.Compound.And -> BooleanClause.Occur.MUST
            is BooleanPredicate.Compound.Or -> BooleanClause.Occur.SHOULD
        }
        val builder = BooleanQuery.Builder()
        builder.add(this.p1.toLuceneQuery(), clause)
        builder.add(this.p2.toLuceneQuery(), clause)
        return builder.build()
    }

    /**
     * An [IndexTx] that affects this [LuceneIndex].
     */
    private inner class Tx(context: TransactionContext) : AbstractIndex.Tx(context) {

        /** The [IndexReader] instance used for accessing the [LuceneIndex]. */
        private val indexReader = DirectoryReader.open(this@LuceneIndex.directory)

        /** The [IndexWriter] instance used for accessing the [LuceneIndex]. */
        private val indexWriter = IndexWriter(this@LuceneIndex.directory, IndexWriterConfig(this.config.analyzer.get()).setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND).setCommitOnClose(true))

        /** The [LuceneIndexConfig] used by this [LuceneIndex] instance. */
        override val config: LuceneIndexConfig
            get() {
                val entry = IndexCatalogueEntry.read(this@LuceneIndex.name, this@LuceneIndex.parent.parent.parent, this.context.xodusTx) ?: throw DatabaseException.DataCorruptionException("Failed to read catalogue entry for index ${this@LuceneIndex.name}.")
                return LuceneIndexConfig.fromParamMap(entry.config)
            }

        /**
         * Returns the number of [Document] in this [LuceneIndex], which should roughly correspond
         * to the number of [TupleId]s it contains.
         *
         * @return Number of [Document]s in this [LuceneIndex]
         */
        override fun count(): Long {
            return this.indexReader.numDocs().toLong()
        }

        /**
         * (Re-)builds the [LuceneIndex].
         */
        override fun rebuild() {
            /* Obtain Tx for parent [Entity. */
            val entityTx = this.context.getTx(this.dbo.parent) as EntityTx

            /* Recreate entries. */
            this.indexWriter.deleteAll()
            entityTx.cursor(this.columns).forEach { record ->
                this.indexWriter.addDocument(documentFromRecord(record))
            }
        }

        /**
         * Updates the [LuceneIndex] with the provided [Operation.DataManagementOperation].
         *
         * @param event [Operation.DataManagementOperation] to process.
         */
        override fun update(event: Operation.DataManagementOperation) {
            when (event) {
                is Operation.DataManagementOperation.InsertOperation -> {
                    val new = event.inserts[this.dbo.columns[0]]
                    if (new is StringValue) {
                        this.indexWriter.addDocument(this@LuceneIndex.documentFromValue(new, event.tupleId))
                    }
                }
                is Operation.DataManagementOperation.UpdateOperation -> {
                    this.indexWriter.deleteDocuments(Term(TID_COLUMN, event.tupleId.toString()))
                    val new = event.updates[this.dbo.columns[0]]?.second
                    if (new is StringValue) {
                        this.indexWriter.addDocument(this@LuceneIndex.documentFromValue(new, event.tupleId))
                    }
                }
                is Operation.DataManagementOperation.DeleteOperation -> {
                    this.indexWriter.deleteDocuments(Term(TID_COLUMN, event.tupleId.toString()))
                }
            }
        }

        /**
         * Clears the [LuceneIndex] underlying this [Tx] and removes all entries it contains.
         */
        override fun clear() {
            this.indexWriter.deleteAll()
            this.updateState(IndexState.STALE)
        }

        /**
         * Performs a lookup through this [LuceneIndex.Tx] and returns a [Cursor] of all [TupleId]s that match the [Predicate].
         * Only supports [BooleanPredicate]s.
         *
         * The [Cursor] is not thread safe!
         *
         * @param predicate The [Predicate] for the lookup*
         * @return The resulting [Iterator]
         */
        override fun filter(predicate: Predicate) = object : Cursor<Record> {

            /** Cast [BooleanPredicate] (if such a cast is possible). */
            private val predicate = if (predicate !is BooleanPredicate) {
                throw QueryException.UnsupportedPredicateException("Index '${this@LuceneIndex.name}' (lucene index) does not support predicates of type '${predicate::class.simpleName}'.")
            } else {
                predicate
            }

            /* Performs some sanity checks. */
            init {
                if (!this@LuceneIndex.canProcess(predicate)) {
                    throw QueryException.UnsupportedPredicateException("Index '${this@LuceneIndex.name}' (lucene-index) cannot process the provided predicate.")
                }
            }

            /** Number of [TupleId]s returned by this [Iterator]. */
            @Volatile
            private var returned = 0

            /** Lucene [Query] representation of [BooleanPredicate] . */
            private val query: Query = this.predicate.toLuceneQuery()

            /** [IndexSearcher] instance used for lookup. */
            private val searcher = IndexSearcher(this@Tx.indexReader)

            /* Execute query and add results. */
            private val results = this.searcher.search(this.query, Integer.MAX_VALUE)

            override fun moveNext(): Boolean = this.returned < this.results.totalHits.value

            override fun key(): TupleId {
                val scores = this.results.scoreDocs[this.returned]
                val doc = this.searcher.doc(scores.doc)
                return doc[TID_COLUMN].toLong()
            }

            override fun value(): Record {
                val scores = this.results.scoreDocs[this.returned++]
                val doc = this.searcher.doc(scores.doc)
                return StandaloneRecord(doc[TID_COLUMN].toLong(), this@LuceneIndex.produces, arrayOf(FloatValue(scores.score)))
            }

            override fun close() {
                /* No op. */
            }
        }

        /**
         * The [LuceneIndex] does not support ranged filtering!
         *
         * @param predicate The [Predicate] for the lookup.
         * @param partitionIndex The [partitionIndex] for this [filterRange] call.
         * @param partitions The total number of partitions for this [filterRange] call.
         * @return The resulting [Cursor].
         */
        override fun filterRange(predicate: Predicate, partitionIndex: Int, partitions: Int): Cursor<Record> {
            throw UnsupportedOperationException("The LuceneIndex does not support ranged filtering!")
        }

        /**
         * Commits changes made through the [IndexWriter]
         */
        override fun beforeCommit() {
            if (this.indexWriter.hasUncommittedChanges()) {
                this.indexWriter.commit()
            }

            /* Close reader and writer. */
            this.indexReader.close()
            this.indexWriter.close()
        }

        /**
         * Rolls back changes made through the [IndexWriter]
         */
        override fun beforeRollback() {
            if (this.indexWriter.hasUncommittedChanges()) {
                this.indexWriter.rollback()
            }

            /* Close reader and writer. */
            this.indexReader.close()
            this.indexWriter.close()
        }
    }
}
