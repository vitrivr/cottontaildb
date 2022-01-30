package org.vitrivr.cottontail.core.queries.binding

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.Digest
import org.vitrivr.cottontail.core.queries.Node
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value

/**
 * This class acts as a level of indirection for [Value]'s used during query planning, optimization and execution.
 *
 * @author Ralph Gasser
 * @version 1.3.0
 */
sealed interface Binding: Node {

    /** The [Value]. */
    val value: Value?

    /** The [Types] held by this [Binding]. */
    val type: Types<*>

    /** Flag indicating whether [Binding] remains static in the context of a query. */
    val static: Boolean

    /** The [BindingContext] associated with this [Binding]. */
    var context: BindingContext

    /**
     * Updates this [Binding] with a new [Value].
     *
     * @param value The new [Value] to update.
     */
    fun update(value: Value?)

    /**
     * Copies this [Binding], creating a new [Binding] that is initially bound to the same [BindingContext].
     *
     * @return Copy of this [Binding]
     */
    override fun copy(): Binding

    /** A [Binding] for a literal [Value] without any indirection other than the [Binding] itself. */
    data class Literal(val bindingIndex: Int, override val type: Types<*>, override var context: BindingContext, override val static: Boolean = true): Binding {
        override val value: Value?
            get() = this.context[this]
        override fun copy() = Literal(this.bindingIndex, this.type, this.context, this.static)
        override fun digest(): Digest= this.hashCode().toLong()
        override fun bind(context: BindingContext) {
            this.context = context
        }
        override fun update(value: Value?) = this.context.update(this, value)
        override fun toString(): String = ":$bindingIndex"
    }

    /** A [Binding] for a value referred to by a [ColumnDef]. Can only be accessed during query execution. */
    data class Column(val column: ColumnDef<*>, override var context: BindingContext): Binding {
        override val value: Value?
            get() = this.context[this]
        override val type: Types<*>
            get() = this.column.type
        override val static: Boolean
            get() = false
        override fun copy() = Column(this.column, this.context)
        override fun digest(): Digest = this.hashCode().toLong()
        override fun bind(context: BindingContext) {
            this.context = context
        }
        override fun update(value: Value?) = this.context.update(this, value)
        override fun toString(): String = "${this.column.name}"
    }
}