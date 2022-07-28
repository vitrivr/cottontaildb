package org.vitrivr.cottontail.dbms.index.lucene

import jetbrains.exodus.bindings.ComparableBinding
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document.Document
import org.apache.lucene.index.IndexWriter
import org.apache.lucene.index.IndexWriterConfig
import org.apache.lucene.index.SerialMergeScheduler
import org.apache.lucene.index.Term
import org.apache.lucene.queryparser.flexible.standard.QueryParserUtil
import org.apache.lucene.search.*
import org.apache.lucene.search.similarities.SimilarityBase.log2
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.core.basics.Cursor
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.core.queries.nodes.traits.NotPartitionableTrait
import org.vitrivr.cottontail.core.queries.nodes.traits.OrderTrait
import org.vitrivr.cottontail.core.queries.nodes.traits.Trait
import org.vitrivr.cottontail.core.queries.nodes.traits.TraitType
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.core.queries.predicates.BooleanPredicate
import org.vitrivr.cottontail.core.queries.predicates.ComparisonOperator
import org.vitrivr.cottontail.core.queries.predicates.Predicate
import org.vitrivr.cottontail.core.queries.sort.SortOrder
import org.vitrivr.cottontail.core.recordset.PlaceholderRecord
import org.vitrivr.cottontail.core.recordset.StandaloneRecord
import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.core.values.StringValue
import org.vitrivr.cottontail.core.values.pattern.LikePatternValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.dbms.catalogue.Catalogue
import org.vitrivr.cottontail.dbms.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.dbms.entity.DefaultEntity
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.events.DataEvent
import org.vitrivr.cottontail.dbms.exceptions.QueryException
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionContext
import org.vitrivr.cottontail.dbms.index.basic.*
import org.vitrivr.cottontail.dbms.index.basic.rebuilder.AbstractIndexRebuilder
import org.vitrivr.cottontail.dbms.index.basic.rebuilder.AsyncIndexRebuilder
import org.vitrivr.cottontail.dbms.index.hash.BTreeIndex
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.storage.lucene.XodusDirectory
import kotlin.concurrent.withLock

/**
 * An Apache Lucene based [AbstractIndex]. The [LuceneIndex] allows for fast search on text using the EQUAL or LIKE operator.
 *
 * @author Luca Rossetto & Ralph Gasser
 * @version 3.3.0
 */
class LuceneIndex(name: Name.IndexName, parent: DefaultEntity) : AbstractIndex(name, parent) {

    /**
     * The [IndexDescriptor] for the [LuceneIndex].
     */
    companion object: IndexDescriptor<LuceneIndex> {
        /** [Logger] instance used by [LuceneIndex]. */
        private val LOGGER: Logger = LoggerFactory.getLogger(LuceneIndex::class.java)

        /** True since [LuceneIndex] supports incremental updates. */
        override val supportsIncrementalUpdate: Boolean = true

        /** False since [LuceneIndex] doesn't support asynchronous rebuilds. */
        override val supportsAsyncRebuild: Boolean = false

        /** False, since [LuceneIndex] does not support partitioning. */
        override val supportsPartitioning: Boolean = false

        /** [ColumnDef] of the _tid column. */
        const val TID_COLUMN = "_tid"

        /**
         * Opens a [LuceneIndex] for the given [Name.IndexName] in the given [DefaultEntity].
         *
         * @param name The [Name.IndexName] of the [LuceneIndex].
         * @param entity The [Entity] to open the [Index] for.
         * @return The opened [LuceneIndex]
         */
        override fun open(name: Name.IndexName, entity: Entity): LuceneIndex = LuceneIndex(name, entity as DefaultEntity)

        /**
         * Initialize the [XodusDirectory] for a [LuceneIndex].
         *
         * @param name The [Name.IndexName] of the [LuceneIndex].
         * @param catalogue [Catalogue] reference.
         * @param context The [TransactionContext] to perform the transaction with.
         * @return True on success, false otherwise.
         */
        override fun initialize(name: Name.IndexName, catalogue: Catalogue, context: TransactionContext): Boolean {
            return try {
                val directory = XodusDirectory((catalogue as DefaultCatalogue).vfs, name.toString(), context.xodusTx)
                val config = IndexWriterConfig().setOpenMode(IndexWriterConfig.OpenMode.CREATE).setMergeScheduler(SerialMergeScheduler())
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
         * @param catalogue [Catalogue] reference.
         * @param context The [TransactionContext] to perform the transaction with.
         * @return True on success, false otherwise.
         */
        override fun deinitialize(name: Name.IndexName, catalogue: Catalogue, context: TransactionContext): Boolean = try {
            val directory = XodusDirectory((catalogue as DefaultCatalogue).vfs, name.toString(), context.xodusTx)
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

    /** The type of this [AbstractIndex]. */
    override val type: IndexType = IndexType.LUCENE

    /**
     * Opens and returns a new [IndexTx] object that can be used to interact with this [AbstractIndex].
     *
     * @param context If the [QueryContext] to create the [IndexTx] for.
     * @return [IndexTx]
     */
    override fun newTx(context: QueryContext): IndexTx
        = context.txn.getCachedTxForDBO(this) ?: this.Tx(context)

    /**
     * Returns a new [LuceneIndexRebuilder] instance.
     *
     * @param context If the [QueryContext] to create the [LuceneIndexRebuilder] for.
     * @return [LuceneIndexRebuilder]
     */
    override fun newRebuilder(context: QueryContext): AbstractIndexRebuilder<*>
        = LuceneIndexRebuilder(this, context)

    override fun newAsyncRebuilder(): AsyncIndexRebuilder<LuceneIndex>
        = throw UnsupportedOperationException("LuceneIndex does not support asynchronous index rebuilding.")

    /**
     * An [IndexTx] that affects this [LuceneIndex].
     */
    inner class Tx(context: QueryContext) : AbstractIndex.Tx(context), org.vitrivr.cottontail.dbms.general.Tx.WithCommitFinalization, org.vitrivr.cottontail.dbms.general.Tx.WithRollbackFinalization  {

        /** The [LuceneIndexDataStore] backing this [LuceneIndex]. */
        private val store = LuceneIndexDataStore(XodusDirectory(this@LuceneIndex.catalogue.vfs, this@LuceneIndex.name.toString(), this.context.txn.xodusTx), this.columns[0].name)

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
            require(predicate is BooleanPredicate) { "Lucene Index can only process Boolean predicates." }
            mapOf(
                NotPartitionableTrait to NotPartitionableTrait,
                OrderTrait to OrderTrait(listOf(ColumnDef(this@LuceneIndex.parent.name.column("score"), Types.Double) to SortOrder.DESCENDING))
            )
        }

        /**
         * Calculates the cost estimate of this [LuceneIndex.Tx] processing the provided [Predicate].
         *
         * @param predicate [Predicate] to check.
         * @return Cost estimate for the [Predicate]
         */
        override fun costFor(predicate: Predicate): Cost = when {
            canProcess(predicate) -> {
                var cost = Cost.ZERO
                repeat(predicate.columns.size) {
                    cost += (Cost.DISK_ACCESS_READ +  Cost.MEMORY_ACCESS) * log2(this.store.indexReader.numDocs().toDouble()) /* TODO: This is an assumption. */
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
            return this.store.indexReader.numDocs().toLong()
        }

        /**
         * Updates the [LuceneIndex] with the provided [DataEvent.Insert].
         *
         * @param event [DataEvent.Insert] to apply.
         */
        override fun tryApply(event: DataEvent.Insert): Boolean {
            val newValue = event.data[this.columns[0]] ?: return true
            this.store.addDocument(event.tupleId, newValue as StringValue)
            return true
        }

        /**
         * Updates the [LuceneIndex] with the provided [DataEvent.Update].
         *
         * @param event [DataEvent.Update] to apply.
         */
        override fun tryApply(event: DataEvent.Update): Boolean {
            val newValue = event.data[this.columns[0]]?.second
            if (newValue == null) {
                this.store.deleteDocument(event.tupleId) /* Null values are not indexed. */
            } else {
                this.store.updateDocument(event.tupleId, newValue as StringValue)
            }
            return true
        }

        /**
         * Updates the [LuceneIndex] with the provided [DataEvent.Delete].
         *
         * @param event [DataEvent.Delete] to apply.
         */
        override fun tryApply(event: DataEvent.Delete): Boolean {
            this.store.deleteDocument(event.tupleId)
            return true
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
                private val searcher = IndexSearcher(this@Tx.store.indexReader)

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
            this.store.close()
        }

        /**
         * Rolls back changes made through the [IndexWriter]
         */
        override fun beforeRollback() {
            this.store.close()
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
            with (PlaceholderRecord) {
                with (this@Tx.context.bindings) {
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
                        right.getValue() ?: throw QueryException("Conversion to Lucene query failed: Literal value cannot be null!")
                    } else if (left is Binding.Literal) {
                        right.getValue() ?: throw QueryException("Conversion to Lucene query failed: Literal value cannot be null!")
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
            }
        }
    }
}
