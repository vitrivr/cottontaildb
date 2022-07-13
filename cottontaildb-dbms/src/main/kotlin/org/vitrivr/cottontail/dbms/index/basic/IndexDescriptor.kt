package org.vitrivr.cottontail.dbms.index.basic

import jetbrains.exodus.bindings.ComparableBinding
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.dbms.catalogue.Catalogue
import org.vitrivr.cottontail.dbms.entity.DefaultEntity
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionContext

/**
 * An abstract description of an [Index].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
interface IndexDescriptor<T: Index> {

    /** True, if the [Index] described by this [IndexDescriptor] supports incremental updates, i.e., can be updated tuple by tuple. */
    val supportsIncrementalUpdate: Boolean

    /** True, if the [Index] described by this [IndexDescriptor] backing this [IndexTx] supports asynchronous rebuilds. */
    val supportsAsyncRebuild: Boolean

    /** True, if the [Index] described by this [IndexDescriptor] supports filtering an index-able range of the data. */
    val supportsPartitioning: Boolean

    /**
     * Tries to open an [Index] with the given [Name.IndexName] for the given [Entity].
     *
     * @param name The [Name.IndexName] of the [Index].
     * @param entity The [Entity] to open the [Index] for.
     * @return The opened [Index]
     */
    fun open(name: Name.IndexName, entity: Entity): T

    /**
     * Initializes the necessary data structures for an [Index] with the given [Name.IndexName] and the given [DefaultEntity.Tx].
     *
     * @param name The [Name.IndexName] of the [Index].
     * @param catalogue: [Catalogue] reference.
     * @param context The [TransactionContext] to perform the transaction with.
     * @return true on success, false otherwise.
     */
    fun initialize(name: Name.IndexName, catalogue: Catalogue, context: TransactionContext): Boolean

    /**
     * De-initializes the data structures associated with an [Index] with the given [Name.IndexName] and the given [DefaultEntity.Tx].
     *
     * @param name The [Name.IndexName] of the [Index].
     * @param catalogue: [Catalogue] reference.
     * @param context The [TransactionContext] to perform the transaction with.
     * @return true on success, false otherwise.
     */
    fun deinitialize(name: Name.IndexName, catalogue: Catalogue, context: TransactionContext): Boolean

    /**
     * Creates and returns a default [IndexConfig], optionally, initialized with the provided [parameters].
     *
     * @param parameters A map of named parameters. Their definition is specific to the [IndexConfig].
     */
    fun buildConfig(parameters: Map<String,String> = emptyMap()): IndexConfig<T>

    /**
     * The [ComparableBinding] used to serialize and deserialize [IndexConfig] instances.
     *
     * @return [ComparableBinding]
     */
    fun configBinding(): ComparableBinding
}