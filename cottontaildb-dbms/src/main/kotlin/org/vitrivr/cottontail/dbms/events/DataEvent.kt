package org.vitrivr.cottontail.dbms.events

import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.dbms.entity.Entity

/**
 * An [Event] that signals change to the main data held in Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
sealed interface DataEvent: Event {
    /** The name of the [Entity] affected by this [DataEvent]. */
    val entity: Name.EntityName

    /**
     * A [DataEvent] that signals an INSERT into an [Entity]
     */
    data class Insert(override val entity: Name.EntityName, val tuple: Tuple): DataEvent

    /**
     * A [DataEvent] that signals an UPDATE in an [Entity]
     */
    data class Update(override val entity: Name.EntityName, val oldTuple: Tuple, val newTuple: Tuple): DataEvent {
        init {
            require(oldTuple.tupleId == newTuple.tupleId) { "TupleId of old and new tuple must be the same!" }
        }
    }

    /**
     * A [DataEvent] that signals a DELETE from an [Entity]
     */
    data class Delete(override val entity: Name.EntityName, val oldTuple: Tuple): DataEvent
}