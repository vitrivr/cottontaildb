package org.vitrivr.cottontail.dbms.events

import org.vitrivr.cottontail.dbms.index.basic.Index
import org.vitrivr.cottontail.dbms.index.basic.IndexState

/**
 * An [Event] that signals changes with respect to [Index] structures.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed interface IndexEvent: Event {
    /** The [Index] affected by this [IndexEvent]. */
    val index: Index

    /**
     * An [IndexEvent] that signals creation of an [Index].
     */
    data class Created(override val index: Index): IndexEvent

    /**
     * An [IndexEvent] that signals dropping of an [Index].
     */
    data class Dropped(override val index: Index): IndexEvent

    /**
     * An [IndexEvent] that signals [IndexState] change of an [Index].
     */
    data class State(override val index: Index, val state: IndexState): IndexEvent
}