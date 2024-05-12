package org.vitrivr.cottontail.dbms.events

import org.vitrivr.cottontail.dbms.sequence.Sequence

/**
 * An [Event] that signals changes with respect to [Sequence] structures.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed interface SequenceEvent: Event {
    /** The [Sequence] affected by this [SequenceEvent]. */
    val sequence: Sequence

    /**
     * A [SequenceEvent] that signals a CREATION of a new [Sequence]
     */
    data class Create(override val sequence: Sequence) : SequenceEvent

    /**
     * A [SequenceEvent] that signals a DROP of a [Sequence]
     */
    data class Drop(override val sequence: Sequence) : SequenceEvent

    /**
     * A [SequenceEvent] that signals a RESET of a [Sequence]
     */
    data class Reset(override val sequence: Sequence) : SequenceEvent
}