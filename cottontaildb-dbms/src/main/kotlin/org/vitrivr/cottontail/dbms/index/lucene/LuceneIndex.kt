package org.vitrivr.cottontail.dbms.index.lucene

import jetbrains.exodus.bindings.ComparableBinding
import jetbrains.exodus.vfs.VirtualFileSystem
import org.apache.lucene.document.Document
import org.apache.lucene.index.IndexWriter
import org.apache.lucene.search.similarities.SimilarityBase.log2
import org.vitrivr.cottontail.core.basics.Cursor
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.queries.binding.BindingContext
import org.vitrivr.cottontail.core.queries.nodes.traits.NotPartitionableTrait
import org.vitrivr.cottontail.core.queries.nodes.traits.OrderTrait
import org.vitrivr.cottontail.core.queries.nodes.traits.Trait
import org.vitrivr.cottontail.core.queries.nodes.traits.TraitType
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.core.queries.predicates.BooleanPredicate
import org.vitrivr.cottontail.core.queries.predicates.ComparisonOperator
import org.vitrivr.cottontail.core.queries.predicates.Predicate
import org.vitrivr.cottontail.core.queries.sort.SortOrder
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.StringValue
import org.vitrivr.cottontail.dbms.entity.DefaultEntity
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.events.DataEvent
import org.vitrivr.cottontail.dbms.index.basic.*
import org.vitrivr.cottontail.dbms.index.basic.rebuilder.AbstractIndexRebuilder
import org.vitrivr.cottontail.dbms.index.basic.rebuilder.AsyncIndexRebuilder
import org.vitrivr.cottontail.dbms.index.hash.BTreeIndex
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.storage.lucene.XodusDirectory
import kotlin.concurrent.withLock

/**
 * An Apache Lucene based [DefaultIndex]. The [LuceneIndex] allows for fast search on text using the EQUAL or LIKE operator.
 *
 * @author Luca Rossetto & Ralph Gasser
 * @version 3.4.0
 */
class LuceneIndex(name: Name.IndexName, parent: DefaultEntity) : DefaultIndex(name, parent) {

    /**
     * The [IndexDescriptor] for the [LuceneIndex].
     */
    companion object: IndexDescriptor<LuceneIndex> {
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

    /** The type of this [DefaultIndex]. */
    override val type: IndexType = IndexType.LUCENE

    /**
     * Opens and returns a new [IndexTx] object that can be used to interact with this [LuceneIndex].
     *
     * @param parent The [EntityTx] to create the [IndexTx] for.
     * @return [Tx]
     */
    override fun newTx(parent: EntityTx): IndexTx {
        require(parent is DefaultEntity.Tx) { "VAFIndex can only be used with DefaultEntity.Tx" }
        return this.Tx(parent)
    }

    /**
     * Returns a new [LuceneIndexRebuilder] instance.
     *
     * @param context If the [QueryContext] to create the [LuceneIndexRebuilder] for.
     * @return [LuceneIndexRebuilder]
     */
    override fun newRebuilder(context: QueryContext): AbstractIndexRebuilder<*>
        = LuceneIndexRebuilder(this, context)

    override fun newAsyncRebuilder(context: QueryContext): AsyncIndexRebuilder<LuceneIndex>
        = throw UnsupportedOperationException("LuceneIndex does not support asynchronous index rebuilding.")

    /**
     * An [IndexTx] that affects this [LuceneIndex].
     */
    inner class Tx(parent: DefaultEntity.Tx) : DefaultIndex.Tx(parent), org.vitrivr.cottontail.dbms.general.Tx.BeforeCommit, org.vitrivr.cottontail.dbms.general.Tx.BeforeRollback  {
        /** The [VirtualFileSystem] used by this [LuceneIndex]. */
        private val vfs = VirtualFileSystem(this.xodusTx.environment)

        /** The [LuceneIndexDataStore] backing this [LuceneIndex]. */
        internal var store = LuceneIndexDataStore(XodusDirectory(this.vfs, this@LuceneIndex.name.toString(), this.xodusTx), this.columns[0].name)

        /**
         * Checks if this [LuceneIndex] can process the given [Predicate].
         *
         * @param predicate [Predicate] to test.
         * @return True if [Predicate] can be processed, false otherwise.
         */
        override fun canProcess(predicate: Predicate): Boolean = predicate is BooleanPredicate &&
            predicate.columns.all { it in this.columns } &&
            predicate.atomics.all {
                it is BooleanPredicate.Comparison &&
                (it.operator is ComparisonOperator.Like || it.operator is ComparisonOperator.Equal || it.operator is ComparisonOperator.Match)
            }

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
         * Truncates the [LuceneIndex] backed by this [LuceneIndex.Tx].
         */
        override fun truncate() {
            /* Close current store and delete files related to index. */
            this.store.indexWriter.deleteAll()
        }

        /**
         * Drops [LuceneIndex] backed by this [LuceneIndex.Tx].
         */
        override fun drop() {
            /* Close current store and delete files related to index. */
            this.store.close()
            this.vfs.getFiles(this.xodusTx).filter { it.path.startsWith("lucene/${this@LuceneIndex.name}") }.forEach {
                this.vfs.deleteFile(this.xodusTx, it.path)
            }
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
                    cost += (Cost.DISK_ACCESS_READ_SEQUENTIAL +  Cost.MEMORY_ACCESS) * log2(this.store.indexReader.numDocs().toDouble()) /* TODO: This is an assumption. */
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
        context(BindingContext)
        override fun filter(predicate: Predicate) = this.txLatch.withLock {
            require(predicate is BooleanPredicate) { "LuceneIndex can only be used with a BooleanPredicate. This is a programmer's error!" }
            LuceneCursor(this, this@BindingContext, predicate)
        }

        /**
         * The [LuceneIndex] does not support ranged filtering!
         *
         * @param predicate The [Predicate] for the lookup.
         * @param partition The [LongRange] specifying the [TupleId]s that should be considered.
         * @return The resulting [Cursor].
         */
        override fun filter(predicate: Predicate, partition: LongRange): Cursor<Tuple> {
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
    }
}
