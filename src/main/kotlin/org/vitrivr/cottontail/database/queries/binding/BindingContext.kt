package org.vitrivr.cottontail.database.queries.binding

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.values.types.Value
import java.util.*
import kotlin.collections.ArrayList

/**
 * A context for late binding of values. Late binding is used during query planning or execution,
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class BindingContext(startSize: Int = 100) {

    /** List of bound [Value]s for this [QueryContext]. */
    private var bound = ArrayList<Binding>(startSize)

    /** The size of this [BindingContext], i.e., the number of values bound. */
    val size: Int
        get() = this.bound.size

    /**
     * Returns the [Value] for the given [Binding].
     *
     * @param bindingIndex The [Binding] to lookup.
     * @param type The [Binding] to lookup.
     * @return The bound [Value].
     */
    operator fun get(bindingIndex: Int, type: Type<*>? = null): Value? {
        require(bindingIndex < this.bound.size) { "Binding $bindingIndex is not known to this binding context." }
        return this.bound[bindingIndex].value
    }

    /**
     * Returns a list of all [Binding]s held by this [BindingContext].
     */
    fun bindings(): List<Binding> = Collections.unmodifiableList(this.bound)

    /**
     * Creates and returns a [Binding] for the given [Value].
     *
     * @param value The [Value] to bind.
     * @return A value [Binding]
     */
    fun bind(value: Value): Binding.Literal {
        val binding = Binding.Literal(this.bound.size, value, this)
        this.bound.add(binding)
        return binding
    }

    /**
     * Creates and returns a [Binding] for the given [Value].
     *
     * @param type The [Type] to bind.
     * @return A value [Binding]
     */
    fun bindNull(type: Type<*>): Binding.Literal {
        val binding = Binding.Literal(this.bound.size, type, this)
        this.bound.add(binding)
        return binding
    }

    /**
     * Creates and returns a [Binding] for the given [ColumnDef].
     *
     * @param column The [ColumnDef] to bind.
     * @return [Binding.Column]
     */
    fun bind(column: ColumnDef<*>): Binding.Column {
        val binding = Binding.Column(this.bound.size, column, this)
        this.bound.add(binding)
        return binding
    }
}