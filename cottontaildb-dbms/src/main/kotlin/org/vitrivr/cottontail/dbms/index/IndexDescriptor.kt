package org.vitrivr.cottontail.dbms.index

import jetbrains.exodus.bindings.ComparableBinding
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.dbms.entity.DefaultEntity

/**
 * An abstract description of an [Index].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
interface IndexDescriptor<T: Index> {
    /**
     * Tries to open an [Index] with the given [Name.IndexName] for the given [DefaultEntity].
     *
     * @param name The [Name.IndexName] of the [Index].
     * @param entity The [DefaultEntity] that holds the [Index].
     * @return The opened [Index]
     */
    fun open(name: Name.IndexName, entity: DefaultEntity): T

    /**
     * Initializes the necessary data structures for an [Index] with the given [Name.IndexName] and the given [DefaultEntity].
     *
     * @param name The [Name.IndexName] of the [Index].
     * @param entity The [DefaultEntity.Tx] to perform the transaction with.
     * @return true on success, false otherwise.
     */
    fun initialize(name: Name.IndexName, entity: DefaultEntity.Tx): Boolean

    /**
     * Deinitializes the data structures associated with an [Index] with the given [Name.IndexName] and the given [DefaultEntity].
     *
     * @param name The [Name.IndexName] of the [Index].
     * @param entity The [DefaultEntity.Tx] to perform the transaction with.
     * @return true on success, false otherwise.
     */
    fun deinitialize(name: Name.IndexName, entity: DefaultEntity.Tx): Boolean

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