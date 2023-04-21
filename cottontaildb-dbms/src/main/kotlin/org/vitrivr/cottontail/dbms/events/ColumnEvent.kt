package org.vitrivr.cottontail.dbms.events

import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.dbms.column.Column

/**
 * An [Event] that signals changes with respect to [Column] structures.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
interface ColumnEvent: Event {
    /** The name of the [C] affected by this [ColumnEvent]. */
    val column: Name.ColumnName

    /**
     * An [ColumnEvent] that signals that column statistics of [Column] have turned stale.
     */
    data class Stale(override val column: Name.ColumnName): ColumnEvent
}