package org.vitrivr.cottontail.dbms.events

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
/**
 * An [Event] that signals change to an [Name.EntityName] in Cottontail DB.
 *
 * @author Florian Burkhardt
 * @version 1.0.0
 */
interface EntityEvent : Event {
    /** The name of the Entity affected by this [EntityEvent]. */
    val name: Name.EntityName

    /* Array of [ColumnDef] of the created or dropped entity */
    val columns: Array<ColumnDef<*>>

    /**
     * A [EntityEvent] that signals a CREATION of a new Entity
     */
    data class Create(override val name: Name.EntityName, override val columns: Array<ColumnDef<*>>) : EntityEvent

    /**
     * A [EntityEvent] that signals a DROP of a Entity
     */
    data class Drop(override val name: Name.EntityName, override val columns: Array<ColumnDef<*>>) : EntityEvent

    /**
     * A [EntityEvent] that signals TRUNCATE of a Entity
     */
    data class Truncate(override val name: Name.EntityName, override val columns: Array<ColumnDef<*>>) : EntityEvent
}