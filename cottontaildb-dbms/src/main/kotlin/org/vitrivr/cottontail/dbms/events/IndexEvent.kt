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
     *
     */
    data class State(override val index: Name.IndexName, val state: IndexState): IndexEvent
}