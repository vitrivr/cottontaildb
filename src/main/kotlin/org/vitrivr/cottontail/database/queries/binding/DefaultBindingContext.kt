package org.vitrivr.cottontail.database.queries.binding

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.values.types.Value
import kotlin.collections.ArrayList

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

    /** List of bound [Value]s for this [BindingContext]. */
    private val boundValues = ArrayList<Value?>(startSize)

    /** A [Record] currently bound to this [BindingContext]. Used to resolve [Binding.Column]. */
    private var boundRecord: Record? = null

    /**
     * Returns the [Value] for the given [Binding].
     *
     * @param binding The [Binding] to lookup.
     * @return The bound [Value].
     */
    override operator fun get(binding: Binding): Value? {
        require(binding.context == this) { "The given binding $binding has not been registered with this binding context." }
        return when (binding) {
            is Binding.Column -> (this.boundRecord ?: throw IllegalStateException("No record bound for column binding ${binding.column}."))[binding.column]
            is Binding.Literal -> this[binding.bindingIndex]
        }
    }

    /**
     * Returns the [Value] for the given [bindingIndex].
     *
     * @param bindingIndex The [Binding] to lookup.
     * @return The bound [Value].
     */
    override operator fun get(bindingIndex: Int): Value? {
        require(bindingIndex < this.boundValues.size) { "Binding $bindingIndex is not known to this binding context." }
        return this.boundValues[bindingIndex]
    }

    /**
     * Creates and returns a [Binding] for the given [Value].
     *
     * @param value The [Value] to bind.
     * @return A value [Binding]
     */
    override fun bind(value: Value): Binding.Literal {
        val bindingIndex = this.boundValues.size
        check(this.boundValues.add(value)) { "Failed to add $value to list of bound values for index $bindingIndex." }
        return Binding.Literal(bindingIndex, value.type, this)
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
        this.boundValues[binding.bindingIndex] = value
    }

    /**
     * Creates and returns a [Binding] for the given [Value].
     *
     * @param type The [Type] to bind.
     * @return A value [Binding]
     */
    override fun bindNull(type: Type<*>): Binding.Literal {
        val bindingIndex = this.boundValues.size
        check(this.boundValues.add(null)) { "Failed to add null to list of bound values for index $bindingIndex." }
        return Binding.Literal(bindingIndex, type, this)
    }

    /**
     * Creates and returns a [Binding] for the given [ColumnDef].
     *
     * @param column The [ColumnDef] to bind.
     * @return [Binding.Column]
     */
    override fun bind(column: ColumnDef<*>): Binding.Column =  Binding.Column(column, this)

    /**
     * Updates the [Binding.Column]s based on the given [Record].
     *
     * @param record The [Record] to update the [Binding.Column] with.
     */
    override fun bindRecord(record: Record) {
        this.boundRecord = record
    }
}