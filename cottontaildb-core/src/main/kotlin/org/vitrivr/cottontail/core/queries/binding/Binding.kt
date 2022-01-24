package org.vitrivr.cottontail.core.queries.binding

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value

/**
 * This class acts as a level of indirection for [Value]'s used during query planning, optimization and execution.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
sealed interface Binding {

    /** The [Value]. */
    val value: Value?
        get() = this.context[this]

    /** The [Types] held by this [Binding]. */
    val type: Types<*>

    /** Flag indicating whether [Binding] remains static in the context of a query. */
    val static: Boolean

    /** The [BindingContext] associated with this [Binding]. */
    var context: BindingContext

    /** A [Binding] for a literal [Value] without any indirection other than the [Binding] itself. */
    data class Literal(val bindingIndex: Int, override val type: Types<*>, override var context: BindingContext, override val static: Boolean = true):
        Binding {
        override var value: Value?
            get() = this.context[this.bindingIndex]
            set(v) {
                this.context.update(this, v)
            }

        override fun toString(): String = ":$bindingIndex"
    }

    /** A [Binding] for a value referred to by a [ColumnDef]. Can only be accessed during query execution. */
    data class Column(val column: ColumnDef<*>, override var context: BindingContext): Binding {

        override val type: Types<*>
            get() = this.column.type

        override val static: Boolean
            get() = false

        override fun toString(): String = "${this.column.name}"
    }
}