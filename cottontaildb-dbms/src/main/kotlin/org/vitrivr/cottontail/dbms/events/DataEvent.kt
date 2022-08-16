package org.vitrivr.cottontail.dbms.events

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.dbms.entity.Entity

/**
 * An [Event] that signals change to the main data held in Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
sealed interface DataEvent: Event {
    /** The name of the [Entity] affected by this [DataEvent]. */
    val entity: Name.EntityName

    /** The [TupleId] affected by this [DataEvent]. */
    val tupleId: TupleId

    /** The data contained in this [DataEvent]. */
    val data: Map<ColumnDef<*>,*>

    /**
     * A [DataEvent] that signals an INSERT into an [Entity]
     */
    data class Insert(override val entity: Name.EntityName, override val tupleId: TupleId, override val data: Map<ColumnDef<*>, Value?>): DataEvent

    /**
     * A [DataEvent] that signals an UPDATE in an [Entity]
     */
    data class Update(override val entity: Name.EntityName, override val tupleId: TupleId, override val data: Map<ColumnDef<*>, Pair<Value?, Value?>>): DataEvent

    /**
     * A [DataEvent] that signals a DELETE from an [Entity]
     */
    data class Delete(override val entity: Name.EntityName, override val tupleId: TupleId, override val data: Map<ColumnDef<*>, Value?>): DataEvent
}