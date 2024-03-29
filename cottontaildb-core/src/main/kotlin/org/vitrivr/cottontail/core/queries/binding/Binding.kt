package org.vitrivr.cottontail.core.queries.binding

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.Digest
import org.vitrivr.cottontail.core.queries.GroupId
import org.vitrivr.cottontail.core.queries.nodes.NodeWithCost
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.types.Value

/**
 * This class acts as a level of indirection for [Value]'s used during query planning, optimization and execution.
 *
 * @author Ralph Gasser
 * @version 3.0.0
 */
sealed interface Binding: NodeWithCost {

    /** The [Types] held by this [Binding]. */
    val type: Types<*>

    /** Flag indicating whether [Binding] can return a NULL value. */
    val canBeNull: Boolean

    /** Flag indicating whether [Binding] remains static in the context of a query. */
    val static: Boolean

    /** Returns the (estimated) size of this [Binding]. */
    fun size(): Long = 1L

    /**
     * Obtains the current [Value] for this [Binding].
     *
     * @return A bound [Value]
     */
    context(BindingContext, Tuple)
    fun getValue(): Value?

    /**
     * Obtains the current [List] of [Value]s for this [Binding].
     *
     * @return A [List] of bound [Value]
     */
    context(BindingContext, Tuple)
    fun getValues(): List<Value?> = listOf(this.getValue())

    /**
     * Caclulates and returns the [Digest] for this [Binding]
     */
    override fun digest(): Digest = this.hashCode().toLong()

    /** A [Binding] for a literal [Value] without any indirection other than the [Binding] itself. */
    data class Literal(val bindingIndex: Int, override val static: Boolean, override val canBeNull: Boolean, override val type: Types<*>): Binding {
        override val cost: Cost = Cost.MEMORY_ACCESS
        context(BindingContext, Tuple)
        override fun getValue(): Value? = this@BindingContext[this]
        context(BindingContext, Tuple)
        fun update(value: Value?) = this@BindingContext.update(this, value)
        override fun toString(): String = ":$bindingIndex"
    }

    /** A [Binding] for a literal [Value] without any indirection other than the [Binding] itself. */
    data class LiteralList(val bindingIndexStart: Int, val bindingIndexEnd: Int, override val canBeNull: Boolean, override val type: Types<*>): Binding {
        override val cost: Cost = Cost.MEMORY_ACCESS
        override val static: Boolean = true
        override fun size(): Long = this.bindingIndexEnd - this.bindingIndexStart + 1L
        context(BindingContext,Tuple)
        override fun getValue(): Value? = this@BindingContext[this].first()
        context(BindingContext, Tuple)
        override fun getValues(): List<Value?> = this@BindingContext[this]
        override fun toString(): String = ":$bindingIndexStart..$bindingIndexEnd"
    }

    /**
     * A [Binding] for a value generated by invocation of a [org.vitrivr.cottontail.core.queries.functions.Function].
     *
     * Can only be accessed during query execution.
     */
    data class Function(val bindingIndex: Int, val function: org.vitrivr.cottontail.core.queries.functions.Function<*>, val arguments: List<Binding>): Binding {
        init {
            this.function.signature.arguments.forEachIndexed {  i, arg ->
                check(arg.type == this.arguments[i].type) { "Type ${this.arguments[i].type} of argument $i is incompatible with function ${function.signature}." }
            }
        }
        override val type: Types<*> = this.function.signature.returnType
        override val canBeNull: Boolean = this.arguments.any { it.canBeNull }
        override val cost: Cost = this.function.cost + this.arguments.map { it.cost }.reduce { c1, c2 -> c1 + c2}
        override val static: Boolean = false
        val executable: Boolean = this.function.executable
        context(BindingContext, Tuple)
        override fun getValue(): Value? = this@BindingContext[this]
        override fun toString(): String = "${this.function.signature}"

        /**
         * Tries to resolve all [ColumnDef] that are required by this [Binding.Function] and possible sub-functions.
         *
         * @return List of required [ColumnDef].
         */
        fun requiredColumns(): List<Column> = this.arguments.flatMap {
            when (it) {
                is Column -> listOf(it)
                is Function -> it.requiredColumns()
                else -> emptyList()
            }
        }
    }

    /**
     * A [Binding] for a [Value] referred to by a [ColumnDef].
     *
     * Can only be accessed during query execution.
     */
    data class Column(val column: ColumnDef<*>, val physical: ColumnDef<*>?): Binding  {
        override val type: Types<*> = this.column.type
        override val canBeNull: Boolean = this.column.nullable
        override val static: Boolean = false
        override val cost: Cost = Cost.MEMORY_ACCESS
        context(BindingContext, Tuple)
        override fun getValue(): Value? = this@Tuple[this.column]
        override fun toString(): String = "${this.column.name}"
    }

    /**
     * A [Binding] for one or multiple [Value]s generated by invocation of a sub-query.
     *
     * Can only be accessed during query execution.
     */
    data class Subquery(val dependsOn: GroupId, val column: Column): Binding {
        override val type: Types<*> = this.column.type
        override val canBeNull: Boolean = this.column.canBeNull
        override val static: Boolean = false
        override val cost: Cost = Cost.ZERO

        context(BindingContext, Tuple)
        override fun getValue(): Value? = this@BindingContext[this].firstOrNull()

        /**
         * Returns all values held for this [Subquery] in the context of the provided [BindingContext].
         *
         * @return List of [Value]s.
         */
        context(BindingContext, Tuple)
        override fun getValues() = this@BindingContext[this]

        /**
         * Appends a [Value] to this [Subquery] in the provided [BindingContext].
         *
         * @param value The [Value] to append.
         */
        context(BindingContext, Tuple)
        fun append(value: Value) = this@BindingContext.append(this, value)

        /**
         * Clears all [Value]s bound to this [Subquery] within the provided [BindingContext].
         */
        context(BindingContext, Tuple)
        fun clear() = this@BindingContext.clear(this)
    }
}