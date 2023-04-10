package org.vitrivr.cottontail.dbms.events

import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.dbms.catalogue.entries.SchemaCatalogueEntry

/**
 * An [Event] that signals change to the schema in Cottontail DB.
 *
 * @author Florian Burkhardt
 * @version 1.0.0
 */
interface SchemaEvent : Event {
    /** The name of the [SchemaCatalogueEntry] affected by this [SchemaEvent]. */
    val name: Name.SchemaName

    /**
     * A [SchemaEvent] that signals a CREATION of a new [SchemaCatalogueEntry]
     */
    data class Create(override val name: Name.SchemaName) : SchemaEvent

    /**
     * A [SchemaEvent] that signals a DROP of a new [SchemaCatalogueEntry]
     */
    data class Drop(override val name: Name.SchemaName) : SchemaEvent
}