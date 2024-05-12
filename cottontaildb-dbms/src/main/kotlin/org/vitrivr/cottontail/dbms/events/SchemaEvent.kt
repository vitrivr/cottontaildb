package org.vitrivr.cottontail.dbms.events

import org.vitrivr.cottontail.dbms.schema.Schema

/**
 *
 * @author Ralph Gasser
 * @version 1.0
 */
sealed interface SchemaEvent: Event {
    /** The name of the [Schema] affected by this [CatalogueEvent]. */
    val schema: Schema

    /**
     * A [SchemaEvent] that signals a CREATION of a new [Schema]
     */
    data class Create(override val schema: Schema) : SchemaEvent

    /**
     * A [SchemaEvent] that signals a DROP of a [Schema]
     */
    data class Drop(override val schema: Schema) : SchemaEvent
}