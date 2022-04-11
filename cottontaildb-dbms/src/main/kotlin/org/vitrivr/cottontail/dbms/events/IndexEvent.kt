package org.vitrivr.cottontail.dbms.events

import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.dbms.index.Index
import org.vitrivr.cottontail.dbms.index.IndexState

/**
 * An [Event] that signals changes with respect to [Index] structures.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
interface IndexEvent: Event {
    /** The name of the [Index] affected by this [IndexEvent]. */
    val index: Name.IndexName

    /**
     * An [IndexEvent] that signals creation of an [Index].
     */
    data class Created(override val index: Name.IndexName): IndexEvent

    /**
     * An [IndexEvent] that signals dropping of an [Index].
     */
    data class Dropped(override val index: Name.IndexName): IndexEvent

    /**
     * An [IndexEvent] that signals [IndexState] change of an [Index].
     */
    data class State(override val index: Name.IndexName, val state: IndexState): IndexEvent
}