package org.vitrivr.cottontail.dbms.events

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.dbms.entity.Entity

/**
 * An [Event] that signals change to an [Name.EntityName] in Cottontail DB.
 *
 * @author Florian Burkhardt
 * @version 1.0.0
 */
sealed interface EntityEvent : Event {
    /** The name of the Entity affected by this [EntityEvent]. */
    val entity: Entity

    /** The [List] of [ColumnDef] in the [Entity]. */
    val columns: List<ColumnDef<*>>

    /**
     * A [EntityEvent] that signals a CREATE of a new [Entity]
     */
    data class Create(override val entity: Entity, override val columns: List<ColumnDef<*>>) : EntityEvent

    /**
     * A [EntityEvent] that signals a DROP of an [Entity]
     */
    data class Drop(override val entity: Entity, override val columns: List<ColumnDef<*>>) : EntityEvent

    /**
     * A [EntityEvent] that signals TRUNCATE of an [Entity]
     */
    data class Truncate(override val entity: Entity, override val columns: List<ColumnDef<*>>) : EntityEvent
}