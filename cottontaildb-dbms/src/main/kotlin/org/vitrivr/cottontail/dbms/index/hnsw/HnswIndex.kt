package org.vitrivr.cottontail.dbms.index.hnsw

import jetbrains.exodus.bindings.ComparableBinding
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.dbms.catalogue.Catalogue
import org.vitrivr.cottontail.dbms.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.dbms.catalogue.entries.IndexCatalogueEntry
import org.vitrivr.cottontail.dbms.entity.DefaultEntity
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionContext
import org.vitrivr.cottontail.dbms.index.basic.*
import org.vitrivr.cottontail.dbms.index.basic.rebuilder.AsyncIndexRebuilder
import org.vitrivr.cottontail.dbms.index.basic.rebuilder.IndexRebuilder
import org.vitrivr.cottontail.dbms.queries.context.QueryContext

class HnswIndex(name: Name.IndexName, parent: DefaultEntity) : AbstractIndex(name, parent) {

    companion object: IndexDescriptor<HnswIndex> {

        override val supportsIncrementalUpdate: Boolean = false //TODO true
        override val supportsAsyncRebuild: Boolean = false
        override val supportsPartitioning: Boolean = false //TODO true

        private val inMemoryIndexReferences = mutableMapOf<Name.IndexName, HnswIndex>()

        override fun open(name: Name.IndexName, entity: Entity): HnswIndex {

            val existing = inMemoryIndexReferences[name]

            if (existing != null) {
                return existing
            }

            val index = HnswIndex(name, entity as DefaultEntity)

            inMemoryIndexReferences[name] = index

            return index

        }

        override fun initialize(name: Name.IndexName, catalogue: Catalogue, context: TransactionContext): Boolean {

            val entry = IndexCatalogueEntry.read(name, catalogue as DefaultCatalogue, context.xodusTx) ?: throw IllegalArgumentException("IndexCatalogueEntry not found")

            val config = entry.config as HnswIndexConfig

            val index = inMemoryIndexReferences[name] ?: throw IllegalArgumentException("index with name '${name}' not found")

            index.inMemoryIndex = InMemoryHnswIndex(config.maxItemCount, config.distance, m = config.m, ef = config.ef)

            return true
        }

        override fun deinitialize(name: Name.IndexName, catalogue: Catalogue, context: TransactionContext): Boolean {
            TODO("Not yet implemented")
        }

        override fun buildConfig(parameters: Map<String, String>): IndexConfig<HnswIndex> {
            TODO("Not yet implemented")
        }

        override fun configBinding(): ComparableBinding {
            TODO("Not yet implemented")
        }

    }

    override val type: IndexType = IndexType.HNSW

    var inMemoryIndex: InMemoryHnswIndex? = null

    override fun newTx(context: QueryContext): IndexTx {
        TODO("Not yet implemented")
    }

    override fun newRebuilder(context: QueryContext): IndexRebuilder<*> = HnswIndexRebuilder(this, context)

    override fun newAsyncRebuilder(context: QueryContext): AsyncIndexRebuilder<*> = throw UnsupportedOperationException("HnswIndex does not support asynchronous index rebuilding.")


}