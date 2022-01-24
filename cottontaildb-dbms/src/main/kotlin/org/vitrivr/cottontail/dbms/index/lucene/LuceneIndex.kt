package org.vitrivr.cottontail.dbms.index.lucene

import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document.*
import org.apache.lucene.index.*
import org.apache.lucene.queryparser.flexible.standard.QueryParserUtil
import org.apache.lucene.search.*
import org.apache.lucene.search.similarities.SimilarityBase.log2
import org.apache.lucene.store.Directory
import org.apache.lucene.store.FSDirectory
import org.apache.lucene.store.NativeFSLockFactory
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.dbms.entity.DefaultEntity
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.general.TxAction
import org.vitrivr.cottontail.dbms.general.TxSnapshot
import org.vitrivr.cottontail.dbms.index.AbstractIndex
import org.vitrivr.cottontail.dbms.index.IndexTx
import org.vitrivr.cottontail.dbms.index.IndexType
import org.vitrivr.cottontail.dbms.index.hash.UniqueHashIndex
import org.vitrivr.cottontail.dbms.index.lsh.superbit.SuperBitLSHIndex
import org.vitrivr.cottontail.dbms.operations.Operation
import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.core.queries.predicates.Predicate
import org.vitrivr.cottontail.core.queries.predicates.bool.BooleanPredicate
import org.vitrivr.cottontail.core.queries.predicates.bool.ComparisonOperator
import org.vitrivr.cottontail.core.queries.predicates.bool.ConnectionOperator
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.dbms.exceptions.QueryException
import org.vitrivr.cottontail.core.recordset.StandaloneRecord
import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.core.values.StringValue
import org.vitrivr.cottontail.core.values.pattern.LikePatternValue
import java.nio.file.Path

/**
 * An Apache Lucene based [AbstractIndex]. The [LuceneIndex] allows for fast search on text using the EQUAL
 * or LIKE operator.
 *
 * @author Luca Rossetto & Ralph Gasser
 * @version 2.0.1
 */
class LuceneIndex(path: Path, parent: DefaultEntity, config: LuceneIndexConfig? = null) :
    AbstractIndex(path, parent) {

    companion object {
        /** [ColumnDef] of the _tid column. */
        const val TID_COLUMN = "_tid"
    }

    /** The [LuceneIndex] implementation produces an additional score column. */
    override val produces: Array<ColumnDef<*>> = arrayOf(ColumnDef(this.parent.name.column("score"), Types.Double, false))

    /** True since [SuperBitLSHIndex] supports incremental updates. */
    override val supportsIncrementalUpdate: Boolean = true

    /** False, since [LuceneIndex] does not support partitioning. */
    override val supportsPartitioning: Boolean = false

    /** The type of this [AbstractIndex]. */
    override val type: IndexType = IndexType.LUCENE

    /** The [LuceneIndexConfig] used by this [LuceneIndex] instance. */
    override val config: LuceneIndexConfig

    /** The [Directory] containing the data for this [LuceneIndex]. */
    private val directory: Directory = FSDirectory.open(
        this.path.parent.resolve("${this.name.simple}.lucene"),
        NativeFSLockFactory.getDefault()
    )

    init {
        /** Tries to obtain config from disk. */
        val configOnDisk =
            this.store.atomicVar(INDEX_CONFIG_FIELD, LuceneIndexConfig.Serializer).createOrOpen()
        if (configOnDisk.get() == null) {
            if (config != null) {
                this.config = config
            } else {
                this.config = LuceneIndexConfig(LuceneAnalyzerType.STANDARD)
            }
            configOnDisk.set(config)
        } else {
            this.config = configOnDisk.get()
        }

        /** Initial commit of write in case writer was created freshly. */
        val writer = IndexWriter(this.directory, IndexWriterConfig(this.config.getAnalyzer()).setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND).setCommitOnClose(true))
        writer.close()

        /* Initial commit of store. */
        this.store.commit()
    }

    /** The [IndexReader] instance used for accessing the [LuceneIndex]. */
    private var indexReader = DirectoryReader.open(this.directory)

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
            val searcher = IndexSearcher(this.indexReader)
            var cost = Cost.ZERO
            repeat(predicate.columns.size) {
                cost += Cost(Cost.COST_DISK_ACCESS_READ, Cost.COST_MEMORY_ACCESS) * log2(searcher.indexReader.numDocs().toDouble()) /* TODO: This is an assumption. */
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
     * Closes this [LuceneIndex] and the associated data structures.
     */
    override fun close() {
        try {
            super.close()
        } finally {
            this.indexReader.close()
            this.directory.close()
        }
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
        val column = when {
            op.right is Binding.Column -> (op.right as Binding.Column).column
            op.left is Binding.Column -> (op.left as Binding.Column).column
            else -> throw QueryException("Conversion to Lucene query failed: One side of the comparison operator must be a column value!")
        }
        if (column != this@LuceneIndex.columns.first()) {
            throw QueryException("Conversion to Lucene query failed: One side of the comparison operator must be an indexed column!")
        }
        val literal = when {
            op.right is Binding.Literal -> (op.right as Binding.Literal).value
            op.left is Binding.Literal -> (op.left as Binding.Literal).value
            else -> throw QueryException("Conversion to Lucene query failed: One side of the comparison operator must be a literal value!")
        }
        return when (this.operator) {
            is ComparisonOperator.Binary.Equal -> {
                if (literal is StringValue) {
                    TermQuery(Term("${column.name}_str", literal.value))
                } else {
                    throw QueryException("Conversion to Lucene query failed: EQUAL queries strictly require a StringValue as second operand!")
                }
            }
            is ComparisonOperator.Binary.Like -> {
                if (literal is LikePatternValue) {
                    QueryParserUtil.parse(arrayOf(literal.toLucene().value), arrayOf("${column.name}_txt"), StandardAnalyzer())
                } else {
                    throw throw QueryException("Conversion to Lucene query failed: LIKE queries require a LucenePatternValue OR LikePatternValue as second operand!")
                }
            }
            is ComparisonOperator.Binary.Match -> {
                if (literal is StringValue) {
                    QueryParserUtil.parse(arrayOf(literal.value), arrayOf("${column.name}_txt"), StandardAnalyzer())
                } else {
                    throw throw QueryException("Conversion to Lucene query failed: MATCH queries strictly require a StringValue as second operand!")
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
        val clause = when (this.connector) {
            ConnectionOperator.AND -> BooleanClause.Occur.MUST
            ConnectionOperator.OR -> BooleanClause.Occur.SHOULD
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

        /** The [IndexWriter] instance used to access this [LuceneIndex]. */
        private var writer: IndexWriter? = null

        /** The default [TxSnapshot] of this [IndexTx].  */
        override val snapshot = object : TxSnapshot {
            override val actions: List<TxAction> = emptyList()

            /** Commits DB and Lucene writer and updates lucene reader. */
            override fun commit() {
                this@LuceneIndex.store.commit()
                if (this@Tx.writer?.hasUncommittedChanges() == true) {
                    this@Tx.writer?.commit()
                    val oldReader = this@LuceneIndex.indexReader
                    this@LuceneIndex.indexReader = DirectoryReader.open(this@LuceneIndex.directory)
                    oldReader.close()
                }
            }

            /** Rolls back DB and Lucene writer . */
            override fun rollback() {
                this@LuceneIndex.store.rollback()
                if (this@Tx.writer?.hasUncommittedChanges() == true) {
                    this@Tx.writer?.rollback()
                }
            }

            override fun record(action: TxAction): Boolean = false
        }

        /**
         * Returns the number of [Document] in this [LuceneIndex], which should roughly correspond
         * to the number of [TupleId]s it contains.
         *
         * @return Number of [Document]s in this [LuceneIndex]
         */
        override fun count(): Long = this.withReadLock {
            return this@LuceneIndex.indexReader.numDocs().toLong()
        }

        /**
         * (Re-)builds the [LuceneIndex].
         */
        override fun rebuild() = this.withWriteLock {
            this.ensureWriterAvailable()

            /* Obtain Tx for parent [Entity. */
            val entityTx = this.context.getTx(this.dbo.parent) as EntityTx

            /* Recreate entries. */
            this.writer?.deleteAll()
            entityTx.scan(this@LuceneIndex.columns).forEach { record ->
                this.writer?.addDocument(documentFromRecord(record))
            }
        }

        /**
         * Updates the [LuceneIndex] with the provided [Operation.DataManagementOperation].
         *
         * @param event [Operation.DataManagementOperation] to process.
         */
        override fun update(event: Operation.DataManagementOperation) = this.withWriteLock {
            this.ensureWriterAvailable()
            when (event) {
                is Operation.DataManagementOperation.InsertOperation -> {
                    val new = event.inserts[this.dbo.columns[0]]
                    if (new is StringValue) {
                        this.writer?.addDocument(this@LuceneIndex.documentFromValue(new, event.tupleId))
                    }
                }
                is Operation.DataManagementOperation.UpdateOperation -> {
                    this.writer?.deleteDocuments(Term(TID_COLUMN, event.tupleId.toString()))
                    val new = event.updates[this.dbo.columns[0]]?.second
                    if (new is StringValue) {
                        this.writer?.addDocument(this@LuceneIndex.documentFromValue(new, event.tupleId))
                    }
                }
                is Operation.DataManagementOperation.DeleteOperation -> {
                    this.writer?.deleteDocuments(Term(TID_COLUMN, event.tupleId.toString()))
                }
            }
            Unit
        }

        /**
         * Clears the [LuceneIndex] underlying this [Tx] and removes all entries it contains.
         */
        override fun clear() = this.withWriteLock {
            this.ensureWriterAvailable()
            this.writer?.deleteAll()
            this@LuceneIndex.dirtyField.compareAndSet(false, true)
            Unit
        }

        /**
         * Performs a lookup through this [LuceneIndex.Tx] and returns a [Iterator] of
         * all [TupleId]s that match the [Predicate]. Only supports [BooleanPredicate]s.
         *
         * The [Iterator] is not thread safe!
         *
         * @param predicate The [Predicate] for the lookup*
         * @return The resulting [Iterator]
         */
        override fun filter(predicate: Predicate) = object : Iterator<Record> {
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
                this@Tx.withReadLock { }
            }

            /** Number of [TupleId]s returned by this [Iterator]. */
            @Volatile
            private var returned = 0

            /** Lucene [Query] representation of [BooleanPredicate] . */
            private val query: Query = this.predicate.toLuceneQuery()

            /** [IndexSearcher] instance used for lookup. */
            private val searcher = IndexSearcher(this@LuceneIndex.indexReader)

            /* Execute query and add results. */
            private val results = this.searcher.search(this.query, Integer.MAX_VALUE)

            /**
             * Returns `true` if the iteration has more elements.
             */
            override fun hasNext(): Boolean {
                return this.returned < this.results.totalHits.value
            }

            /**
             * Returns the next element in the iteration.
             */
            override fun next(): Record {
                val scores = this.results.scoreDocs[this.returned++]
                val doc = this.searcher.doc(scores.doc)
                return StandaloneRecord(doc[TID_COLUMN].toLong(), this@LuceneIndex.produces, arrayOf(DoubleValue(scores.score)))
            }
        }

        /**
         * The [LuceneIndex] does not support ranged filtering!
         *
         * @param predicate The [Predicate] for the lookup.
         * @param partitionIndex The [partitionIndex] for this [filterRange] call.
         * @param partitions The total number of partitions for this [filterRange] call.
         * @return The resulting [Iterator].
         */
        override fun filterRange(predicate: Predicate, partitionIndex: Int, partitions: Int): Iterator<Record> {
            throw UnsupportedOperationException("The LuceneIndex does not support ranged filtering!")
        }

        /** Makes the necessary cleanup by closing the [IndexWriter]. */
        override fun cleanup() {
            this.writer?.close()
            super.cleanup()
        }

        /** Makes sure that an [IndexWriter] instance is available. */
        private fun ensureWriterAvailable() {
            if (this.writer == null) {
                val config = IndexWriterConfig(this@LuceneIndex.config.getAnalyzer()).setOpenMode(IndexWriterConfig.OpenMode.APPEND).setMaxBufferedDocs(10000).setCommitOnClose(false)
                this.writer = IndexWriter(this@LuceneIndex.directory, config)
            }
        }
    }
}
