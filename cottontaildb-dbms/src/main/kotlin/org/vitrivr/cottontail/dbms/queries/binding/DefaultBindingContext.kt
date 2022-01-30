package org.vitrivr.cottontail.dbms.queries.binding

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.core.queries.binding.BindingContext
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value

/**
 * A context for late binding of values. Late binding is used during query planning or execution, e.g., when
 * literal values are replaced upon re-use of a query plan or when values used in query execution are dynamically loaded.
 *
 * The [BindingContext] class is NOT thread-safe. When concurrently executing part of a query plan, different copies of
 * the [BindingContext] should be created and used.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
class DefaultBindingContext(startSize: Int = 100) : BindingContext {

    /** List of bound [Value]s used to resolve [Binding.Literal] in this [BindingContext]. */
    private val boundLiterals = ArrayList<Value?>(startSize)

    /** List of bound [Value]s used to resolve [Binding.Column] in this [BindingContext]. */
    private val boundColumns = Object2ObjectOpenHashMap<ColumnDef<*>, Value?>()

    /**
     * Returns the [Value] for the given [Binding].
     *
     * @param binding The [Binding] to lookup.
     * @return The bound [Value].
     */
    override operator fun get(binding: Binding.Literal): Value? {
        require(binding.context == this) { "The given binding $binding has not been registered with this binding context." }
        return this.boundLiterals[binding.bindingIndex]
    }

    /**
     * Returns the [Value] for the given [Binding].
     *
     * @param binding The [Binding] to lookup.
     * @return The bound [Value].
     */
    override operator fun get(binding: Binding.Column): Value? {
        require(binding.context == this) { "The given binding $binding has not been registered with this binding context." }
        return this.boundColumns[binding.column]
    }

    /**
     * Creates and returns a [Binding] for the given [Value].
     *
     * @param value The [Value] to bind.
     * @param static True, if [Binding] is expected to change during query execution.
     * @return A value [Binding]
     */
    override fun bind(value: Value, static: Boolean): Binding.Literal {
        val bindingIndex = this.boundLiterals.size
        check(this.boundLiterals.add(value)) { "Failed to add $value to list of bound values for index $bindingIndex." }
        return Binding.Literal(bindingIndex, value.type, this, static)
    }

    /**
     * Creates and returns a [Binding] for the given [Value].
     *
     * @param type The [Types] to bind.
     * @param static True, if [Binding] is expected to change during query execution.
     * @return A value [Binding]
     */
    override fun bindNull(type: Types<*>, static: Boolean): Binding.Literal {
        val bindingIndex = this.boundLiterals.size
        check(this.boundLiterals.add(null)) { "Failed to add null to list of bound values for index $bindingIndex." }
        return Binding.Literal(bindingIndex, type, this, static)
    }

    /**
     * Updates the [Value] for a [Binding.Literal].
     *
     * @param binding The [Binding.Literal] to update.
     * @param value The [Value] to bind.
     * @return A value [Binding]
     */
    override fun update(binding: Binding.Literal, value: Value?) {
        require(binding.context == this) { "The given binding $binding has not been registered with this binding context." }
        require(value == null || binding.type.compatible(value)) { "Value $value cannot be bound to $binding because of type mismatch (${binding.type})."}
        this.boundLiterals[binding.bindingIndex] = value
    }

    /**
     * Updates the [Value] for a [Binding.Column].
     *
     * @param binding The [Binding.Column] to update.
     * @param value The [Value] to bind.
     * @return A value [Binding]
     */
    override fun update(binding: Binding.Column, value: Value?) {
        require(binding.context == this) { "The given binding $binding has not been registered with this binding context." }
        require((value == null && binding.column.nullable) || (value != null && binding.type.compatible(value))) { "Value $value cannot be bound to $binding because of type mismatch (${binding.column}."}
        this.boundColumns[binding.column] = value
    }

    /**
     * Creates and returns a [Binding] for the given [ColumnDef].
     *
     * @param column The [ColumnDef] to bind.
     * @return [Binding.Column]
     */
    override fun bind(column: ColumnDef<*>): Binding.Column =  Binding.Column(column, this)

    /**
     * Creates a copy of this [DefaultBindingContext].
     *
     * @return Copy of this [DefaultBindingContext].
     */
    override fun copy(): BindingContext {
        val copy = DefaultBindingContext(this.boundLiterals.size)
        for ((i,v) in this.boundLiterals.withIndex()) {
            copy.boundLiterals[i] = v
        }
        for ((k,v) in this.boundColumns) {
            copy.boundColumns[k] = v
        }
        return copy
    }
}