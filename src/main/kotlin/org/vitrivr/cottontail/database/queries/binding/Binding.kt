package org.vitrivr.cottontail.database.queries.binding

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.values.types.Value

/**
 * This class acts as a level of indirection for [Value]'s used during query planning, optimization and execution.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
sealed interface Binding {

    /** The [Value]. */
    val value: Value?
        get() = this.context[this]

    /** The [Type] held by this [Binding]. */
    val type: Type<*>

    /** The [BindingContext] associated with this [Binding]. */
    var context: BindingContext

    /** A [Binding] for a literal [Value] without any indirection other than the [Binding] itself. */
    data class Literal(val bindingIndex: Int, override val type: Type<*>, override var context: BindingContext): Binding {
        override var value: Value?
            get() = this.context[this.bindingIndex]
            set(v) {
                this.context.update(this, v)
            }
    }

    /** A [Binding] for a value referred to by a [ColumnDef]. Can only be accessed during query execution. */
    data class Column(val column: ColumnDef<*>, override var context: BindingContext): Binding {

        override val type: Type<*>
            get() = this.column.type
    }
}