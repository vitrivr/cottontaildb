package org.vitrivr.cottontail.dbms.index.lucene

import jetbrains.exodus.bindings.ComparableBinding
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document.*
import org.apache.lucene.index.*
import org.apache.lucene.queryparser.flexible.standard.QueryParserUtil
import org.apache.lucene.search.*
import org.apache.lucene.search.similarities.SimilarityBase.log2
import org.apache.lucene.store.Directory
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.core.basics.Cursor
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.core.queries.nodes.traits.NotPartitionableTrait
import org.vitrivr.cottontail.core.queries.nodes.traits.Trait
import org.vitrivr.cottontail.core.queries.nodes.traits.TraitType
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.core.queries.predicates.BooleanPredicate
import org.vitrivr.cottontail.core.queries.predicates.ComparisonOperator
import org.vitrivr.cottontail.core.queries.predicates.Predicate
import org.vitrivr.cottontail.core.recordset.StandaloneRecord
import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.core.values.StringValue
import org.vitrivr.cottontail.core.values.pattern.LikePatternValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.dbms.catalogue.entries.IndexCatalogueEntry
import org.vitrivr.cottontail.dbms.entity.DefaultEntity
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.exceptions.QueryException
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionContext
import org.vitrivr.cottontail.dbms.index.*
import org.vitrivr.cottontail.dbms.index.hash.BTreeIndex
import org.vitrivr.cottontail.dbms.operations.Operation
import org.vitrivr.cottontail.storage.lucene.XodusDirectory
import kotlin.concurrent.withLock

/**
 * An Apache Lucene based [AbstractIndex]. The [LuceneIndex] allows for fast search on text using the EQUAL or LIKE operator.
 *
 * @author Luca Rossetto & Ralph Gasser
 * @version 3.0.0
 */
class LuceneIndex(name: Name.IndexName, parent: DefaultEntity) : AbstractIndex(name, parent) {

    /**
     * The [IndexDescriptor] for the [LuceneIndex].
     */
    companion object: IndexDescriptor<LuceneIndex> {
        /** [Logger] instance used by [LuceneIndex]. */
        private val LOGGER: Logger = LoggerFactory.getLogger(LuceneIndex::class.java)

        /** [ColumnDef] of the _tid column. */
        const val TID_COLUMN = "_tid"

        /**
         * Opens a [LuceneIndex] for the given [Name.IndexName] in the given [DefaultEntity].
         *
         * @param name The [Name.IndexName] of the [LuceneIndex].
         * @param entity The [DefaultEntity.Tx] that executes the operation.
         * @return The opened [LuceneIndex]
         */
        override fun open(name: Name.IndexName, entity: DefaultEntity): LuceneIndex = LuceneIndex(name, entity)

        /**
         * Initialize the [XodusDirectory] for a [LuceneIndex].
         *
         * @param name The [Name.IndexName] of the [LuceneIndex].
         * @param entity The [DefaultEntity.Tx] that executes the operation.
         * @return True on success, false otherwise.
         */
        override fun initialize(name: Name.IndexName, entity: DefaultEntity.Tx): Boolean {
            return try {
                val directory = XodusDirectory(entity.dbo.catalogue.vfs, name.toString(), entity.context.xodusTx)
                val config = IndexWriterConfig()
                    .setOpenMode(IndexWriterConfig.OpenMode.CREATE)
                    .setCommitOnClose(true)
                val writer = IndexWriter(directory, config)
                writer.close()
                directory.close()
                true
            } catch (e: Throwable) {
                LOGGER.error("Failed to initialize Lucene Index $name due to an exception: ${e.message}.")
                false
            }
        }

        /**
         * De-initializes the [XodusDirectory] for a [LuceneIndex].
         *
         * @param name The [Name.IndexName] of the [LuceneIndex].
         * @param entity The [DefaultEntity] that holds the [LuceneIndex].
         * @return True on success, false otherwise.
         */
        override fun deinitialize(name: Name.IndexName, entity: DefaultEntity.Tx): Boolean = try {
            val directory = XodusDirectory(entity.dbo.catalogue.vfs, name.toString(), entity.context.xodusTx)
            for (file in directory.listAll()) {
                directory.deleteFile(file)
            }
            directory.close()
            true
        } catch (e: Throwable) {
            LOGGER.error("Failed to de-initialize Lucene Index $name due to an exception: ${e.message}.")
            false
        }

        /**
         * Generates and returns a [LuceneIndexConfig] for the given [parameters] (or default values, if [parameters] are not set).
         *
         * @param parameters The parameters to initialize the default [LuceneIndexConfig] with.
         */
        override fun buildConfig(parameters: Map<String, String>): IndexConfig<LuceneIndex> = LuceneIndexConfig(
            try {
                LuceneAnalyzerType.valueOf(parameters[LuceneIndexConfig.KEY_ANALYZER_TYPE_KEY] ?: "")
            } catch (e: IllegalArgumentException) {
                LuceneAnalyzerType.STANDARD
            }
        )

        /**
         * Returns the [LuceneIndexConfig.Binding]
         *
         * @return [LuceneIndexConfig.Binding]
         */
        override fun configBinding(): ComparableBinding = LuceneIndexConfig.Binding

    }

    /** True since [LuceneIndex] supports incremental updates. */
    override val supportsIncrementalUpdate: Boolean = true

    /** False, since [LuceneIndex] does not support partitioning. */
    override val supportsPartitioning: Boolean = false

    /** The type of this [AbstractIndex]. */
    override val type: IndexType = IndexType.LUCENE

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
     * An [IndexTx] that affects this [LuceneIndex].
     */
    private inner class Tx(context: TransactionContext) : AbstractIndex.Tx(context) {

        /** The [Directory] containing the data for this [LuceneIndex]. */
        private val directory: Directory = XodusDirectory(this@LuceneIndex.catalogue.vfs, this@LuceneIndex.name.toString(), this.context.xodusTx)

        /** The [IndexReader] instance used for accessing the [LuceneIndex]. */
        private val indexReader = DirectoryReader.open(this.directory)

        /** The [IndexWriter] instance used for accessing the [LuceneIndex]. */
        private val indexWriter: IndexWriter by lazy {
            val config = IndexWriterConfig().setOpenMode(IndexWriterConfig.OpenMode.APPEND).setCommitOnClose(true)
            IndexWriter(this.directory, config)
        }

        /** The [LuceneIndexConfig] used by this [LuceneIndex] instance. */
        override val config: LuceneIndexConfig
            get() {
                val entry = IndexCatalogueEntry.read(this@LuceneIndex.name, this@LuceneIndex.parent.parent.parent, this.context.xodusTx) ?: throw DatabaseException.DataCorruptionException("Failed to read catalogue entry for index ${this@LuceneIndex.name}.")
                return entry.config as LuceneIndexConfig
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
            val column = if (right is Binding.Column && right.column == this@Tx.columns[0]) {
                right.column
            } else if (left is Binding.Column && left.column ==  this@Tx.columns[0]) {
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
                        is StringValue -> QueryParserUtil.parse(
                            arrayOf(literal.value),
                            arrayOf("${column.name}_txt"),
                            StandardAnalyzer()
                        )
                        is LikePatternValue -> QueryParserUtil.parse(
                            arrayOf(literal.toLucene().value),
                            arrayOf("${column.name}_txt"),
                            StandardAnalyzer()
                        )
                        else -> throw throw QueryException("Conversion to Lucene query failed: LIKE queries require a StringValue OR LikePatternValue as second operand!")
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
         * Checks if this [LuceneIndex] can process the given [Predicate].
         *
         * @param predicate [Predicate] to test.
         * @return True if [Predicate] can be processed, false otherwise.
         */
        override fun canProcess(predicate: Predicate): Boolean = predicate is BooleanPredicate &&
            predicate.columns.all { it in this.columns } &&
            predicate.atomics.all { it.operator is ComparisonOperator.Binary.Like || it.operator is ComparisonOperator.Binary.Equal || it.operator is ComparisonOperator.Binary.Match }

        /**
         * Returns a [List] of the [ColumnDef] produced by this [LuceneIndex].
         *
         * @return [List] of [ColumnDef].
         */
        override fun columnsFor(predicate: Predicate): List<ColumnDef<*>> = this.txLatch.withLock {
            require(predicate is BooleanPredicate) { "Lucene can only process boolean predicates." }
            return listOf(ColumnDef(this@LuceneIndex.parent.name.column("score"), Types.Double))
        }

        /**
         * The [LuceneIndex] does not return results in a particular order.
         *
         * @param predicate [Predicate] to check.
         * @return List that describes the sort order of the values returned by the [BTreeIndex]
         */
        override fun traitsFor(predicate: Predicate): Map<TraitType<*>, Trait> = this.txLatch.withLock {
            require(predicate is BooleanPredicate) { "Lucene index can only process boolean predicates." }
            mapOf(NotPartitionableTrait to NotPartitionableTrait)
        }

        /**
         * Calculates the cost estimate of this [LuceneIndex.Tx] processing the provided [Predicate].
         *
         * @param predicate [Predicate] to check.
         * @return Cost estimate for the [Predicate]
         */
        override fun costFor(predicate: Predicate): Cost = when {
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
         * Returns the number of [Document] in this [LuceneIndex], which should roughly correspond
         * to the number of [TupleId]s it contains.
         *
         * @return Number of [Document]s in this [LuceneIndex]
         */
        override fun count(): Long = this.txLatch.withLock {
            return this.indexReader.numDocs().toLong()
        }

        /**
         * (Re-)builds the [LuceneIndex].
         */
        override fun rebuild() = this.txLatch.withLock {
            LOGGER.debug("Rebuilding Lucene index {}", this@LuceneIndex.name)

            /* Obtain Tx for parent entity. */
            val entityTx = this.context.getTx(this.dbo.parent) as EntityTx

            /* Recreate entries. */
            this.indexWriter.deleteAll()

            /* Iterate over entity and update index with entries. */
            val cursor = entityTx.cursor(this.columns)
            cursor.forEach { record ->
                this.indexWriter.addDocument(documentFromRecord(record))
            }

            /* Close cursor. */
            cursor.close()

            /* Update index state for index. */
            this.updateState(IndexState.CLEAN)
            LOGGER.debug("Rebuilding Lucene index {} completed!", this@LuceneIndex.name)
        }

        /**
         * Updates the [LuceneIndex] with the provided [Operation.DataManagementOperation.InsertOperation].
         *
         * @param operation [Operation.DataManagementOperation.InsertOperation] to apply.
         */
        override fun insert(operation: Operation.DataManagementOperation.InsertOperation) = this.txLatch.withLock {
            val new = operation.inserts[this.columns[0]]
            if (new is StringValue) {
                this.indexWriter.addDocument(this@Tx.documentFromValue(new, operation.tupleId))
            }
        }

        /**
         * Updates the [LuceneIndex] with the provided [Operation.DataManagementOperation.UpdateOperation].
         *
         * @param operation [Operation.DataManagementOperation.UpdateOperation] to apply.
         */
        override fun update(operation: Operation.DataManagementOperation.UpdateOperation) = this.txLatch.withLock {
            this.indexWriter.deleteDocuments(Term(TID_COLUMN, operation.tupleId.toString()))
            val new = operation.updates[this.columns[0]]?.second
            if (new is StringValue) {
                this.indexWriter.addDocument(this@Tx.documentFromValue(new, operation.tupleId))
            }
        }

        /**
         * Updates the [LuceneIndex] with the provided [Operation.DataManagementOperation.DeleteOperation].
         *
         * @param operation [Operation.DataManagementOperation.DeleteOperation] to apply.
         */
        override fun delete(operation: Operation.DataManagementOperation.DeleteOperation) = this.txLatch.withLock {
            this.indexWriter.deleteDocuments(Term(TID_COLUMN, operation.tupleId.toString()))
            Unit
        }

        /**
         * Clears the [LuceneIndex] underlying this [Tx] and removes all entries it contains.
         */
        override fun clear() = this.txLatch.withLock {
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
        override fun filter(predicate: Predicate) = this.txLatch.withLock {
            object : Cursor<Record> {

                /** Cast [BooleanPredicate] (if such a cast is possible). */
                private val predicate = if (predicate !is BooleanPredicate) {
                    throw QueryException.UnsupportedPredicateException("Index '${this@LuceneIndex.name}' (lucene index) does not support predicates of type '${predicate::class.simpleName}'.")
                } else {
                    predicate
                }

                /** The [ColumnDef] generated by this [Cursor]. */
                private val columns = this@Tx.columnsFor(predicate).toTypedArray()

                /** Number of [TupleId]s returned by this [Iterator]. */
                @Volatile
                private var returned = 0

                /** Lucene [Query] representation of [BooleanPredicate] . */
                private val query: Query = this.predicate.toLuceneQuery()

                /** [IndexSearcher] instance used for lookup. */
                private val searcher = IndexSearcher(this@Tx.indexReader)

                /* Execute query and add results. */
                private val results = this.searcher.search(this.query, Integer.MAX_VALUE)

                override fun moveNext(): Boolean {
                    return this.returned < this.results.totalHits.value
                }

                override fun key(): TupleId {
                    val scores = this.results.scoreDocs[this.returned]
                    val doc = this.searcher.doc(scores.doc)
                    return doc[TID_COLUMN].toLong()
                }

                override fun value(): Record {
                    val scores = this.results.scoreDocs[this.returned++]
                    val doc = this.searcher.doc(scores.doc)
                    return StandaloneRecord(doc[TID_COLUMN].toLong(), this.columns, arrayOf(DoubleValue(scores.score)))
                }

                override fun close() {}
            }
        }

        /**
         * The [LuceneIndex] does not support ranged filtering!
         *
         * @param predicate The [Predicate] for the lookup.
         * @param partition The [LongRange] specifying the [TupleId]s that should be considered.
         * @return The resulting [Cursor].
         */
        override fun filter(predicate: Predicate, partition: LongRange): Cursor<Record> {
            throw UnsupportedOperationException("The LuceneIndex does not support ranged filtering!")
        }

        /**
         * Commits changes made through the [IndexWriter]
         */
        override fun beforeCommit() {
            /* Call super. */
            super.beforeCommit()

            /* Commit and close writer. */
            if (this.indexWriter.hasUncommittedChanges()) {
                this.indexWriter.commit()
            }

            /* Close reader, writer and directory. */
            this.indexReader.close()
            this.indexWriter.close()
            this.directory.close()
        }

        /**
         * Rolls back changes made through the [IndexWriter]
         */
        override fun beforeRollback() {
            /* Call super. */
            super.beforeCommit()


            /* Rollback and close writer. */
            if (this.indexWriter.hasUncommittedChanges()) {
                this.indexWriter.rollback()
            }

            /* Close reader and writer. */
            this.indexReader.close()
            this.indexWriter.close()
            this.directory.close()
        }
    }
}
