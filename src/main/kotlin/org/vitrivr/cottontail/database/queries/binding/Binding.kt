package org.vitrivr.cottontail.database.queries.binding

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.values.types.Value

/**
 * This class acts as a level of indirection for [Value]'s used during query planning, optimization and execution.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed interface Binding {

    /** The [Value]. */
    val value: Value?

    /** The [Type] held by this [Binding]. */
    val type: Type<*>

    /** The [BindingContext] associated with this [Binding]. */
    val context: BindingContext

    /** A literal [Value] without any indirection other than the [Binding]. */
    class Literal(val index: Int, override val type: Type<*>, override val context: BindingContext): Binding {
        /** The value [T] associated with this [Binding]. */
        override var value: Value? = null

        /** Alternate constructor using a concrete [Value]. */
        constructor(index: Int, value: Value, context: BindingContext): this(index, value.type, context) {
            this.value = value
        }
    }

    /** A value referred to by a [ColumnDef]. Can only be accessed during query execution. */
    class Column(val index: Int, val column: ColumnDef<*>, override val context: BindingContext): Binding {
        override val type: Type<*>
            get() = this.column.type

        override val value: Value?
            get() = (this.context.boundRecord ?: throw IllegalStateException("No record bound for column binding ${this.column}."))[this.column]
    }
}