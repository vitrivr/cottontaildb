package org.vitrivr.cottontail.database.catalogue

import org.vitrivr.cottontail.database.general.Tx
import org.vitrivr.cottontail.database.schema.Schema
import org.vitrivr.cottontail.model.basics.Name
import java.nio.file.Path

/**
 * A [Tx] that operates on a single [Catalogue]. [Tx]s are a unit of isolation for data operations (read/write).
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
interface CatalogueTx : Tx {
    /** Reference to the [Catalogue] this [CatalogueTx] belongs to. */
    override val dbo: Catalogue

    /**
     * Returns a list of [Name.SchemaName] held by this [Catalogue].
     *
     * @return [List] of all [Name.SchemaName].
     */
    fun listSchemas(): List<Name.SchemaName>

    /**
     * Returns the [Schema] for the given [Name.SchemaName].
     *
     * @param name [Name.SchemaName] of the [Schema].
     * @return [Schema]
     */
    fun schemaForName(name: Name.SchemaName): Schema

    /**
     * Creates a new, empty [Schema] with the given [Name.SchemaName] and [Path]
     *
     * @param name The [Name.SchemaName] of the new [Schema].
     */
    fun createSchema(name: Name.SchemaName)

    /**
     * Drops an existing [Schema] with the given [Name.SchemaName].
     *
     * @param name The [Name.SchemaName] of the [Schema] to be dropped.
     */
    fun dropSchema(name: Name.SchemaName)
}