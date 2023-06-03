package org.vitrivr.cottontail.dbms.queries.binding

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.GroupId
import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.core.queries.binding.BindingContext
import org.vitrivr.cottontail.core.queries.functions.Function
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.types.Value
import java.util.*

/**
 * A context that manages different types of [Binding]s for binding of [Value]. [Binding]s are used during query planning
 * and execution as proxies for values (either literal or computed).
 *
 * The [BindingContext] class is NOT thread-safe. When concurrently executing part of a query plan, different copies of
 * the [BindingContext] should be created and used.
 *
 * @author Ralph Gasser
 * @version 3.0.0
 */
class DefaultBindingContext: BindingContext {

    /** List of bound [Value]s used to resolve [Binding.Literal] in this [BindingContext]. */
    private val boundLiterals = ArrayList<Value?>(100)

    /** List of bound [Function]s used to resolve [Binding.Function] in this [BindingContext]. */
    private val boundFunctions = ArrayList<Array<Value?>>(10)

    /** Map of bound [Value]s used to resolve [Binding.Subquery] in this [BindingContext]. */
    private val boundSubqueries = Int2ObjectOpenHashMap<MutableList<Value>>()

    /**
     * Returns the [Value] for the given [Binding.Literal].
     *
     * @param binding The [Binding.Literal] to lookup.
     * @return The bound [Value].
     */
    override operator fun get(binding: Binding.Literal): Value?
        = this.boundLiterals[binding.bindingIndex]

    /**
     * Returns the [List] of [Value]s for the given [Binding.LiteralList].
     *
     * @param binding The [Binding.LiteralList] to lookup.
     * @return The bound [Value].
     */
    override fun get(binding: Binding.LiteralList): List<Value?>
        = (binding.bindingIndexStart .. binding.bindingIndexEnd).map { this.boundLiterals[it] }

    /**
     * Returns the [Value] for the given [Binding.Function].
     *
     * @param binding The [Binding] to lookup.
     * @return The bound [Value].
     */
    context(Tuple)
    override operator fun get(binding: Binding.Function): Value? {
        val arguments = this.boundFunctions[binding.bindingIndex]
        for ((i,a) in binding.arguments.withIndex()) {
            arguments[i] = a.getValue()
        }
        return binding.function(*arguments)
    }

    /**
     * Returns the [Value]s for the given [Binding.Subquery].
     *
     * @param binding The [Binding.Subquery] to lookup.
     * @return A [Collection] of the bound [Value]s.
     */
    override fun get(binding: Binding.Subquery): List<Value?> {
        return this.boundSubqueries[binding.dependsOn]
            ?: throw IllegalStateException("Could not find data collection for sub-query ${binding.dependsOn}. This is a programmer's error!")
    }

    /**
     * Creates and returns a [Binding] for the given [Value].
     *
     * @param value The [Value] to bind.
     * @return A [Binding.Literal]
     */
    override fun bind(value: Value, static: Boolean): Binding.Literal {
        val bindingIndex = this.boundLiterals.size
        check(this.boundLiterals.add(value)) { "Failed to add $value to list of bound values for index $bindingIndex." }
        return Binding.Literal(bindingIndex, static, false, value.type)
    }

    /**
     * Creates and returns a [Binding] for the given [Value].
     *
     * @param type The [Types] to bind.
     * @return A [Binding.Literal]
     */
    override fun bindNull(type: Types<*>, static: Boolean): Binding.Literal {
        val bindingIndex = this.boundLiterals.size
        check(this.boundLiterals.add(null)) { "Failed to add null to list of bound values for index $bindingIndex." }
        return Binding.Literal(bindingIndex, static, true, type)
    }

    /**
     * Creates and returns a [Binding] for the given [List] of [Value]s.
     *
     * @param values The [List] of [Value] to bind.
     * @return A [Binding.LiteralList]
     */
    override fun bind(values: List<Value>): Binding.LiteralList {
        require(values.isNotEmpty()) { "Failed to bind empty values list." }
        val bindingIndexStart = this.boundLiterals.size
        for ((i, v) in values.withIndex()) {
            check(this.boundLiterals.add(v)) { "Failed to add $v to list of bound values for index ${bindingIndexStart + i}." }
        }
        return Binding.LiteralList(bindingIndexStart, bindingIndexStart + values.size - 1, false, values.first().type)
    }

    /**
     * Creates and returns a [Binding.Column] for the given [ColumnDef].
     *
     * @param column The [ColumnDef] to bind.
     * @return [Binding.Column]
     */
    override fun bind(column: ColumnDef<*>) = Binding.Column(column)


    /**
     * Creates and returns a [Binding.Function] for the given [Function] invocation
     *
     * @param function The [Function] to bind.
     * @param arguments The list of argument [Binding]s for the [Function] invocation
     * @return [Binding.Function]
     */
    override fun bind(function: Function<*>, arguments: List<Binding>): Binding.Function {
        val bindingIndex = this.boundFunctions.size
        check(this.boundFunctions.add(arrayOfNulls(arguments.size))) { "Failed to add $function to list of bound functions for index $bindingIndex." }
        return Binding.Function(bindingIndex, function, arguments)
    }

    /**
     * Creates and returns a [Binding.Subquery] for the given [GroupId] invocation
     *
     * @param dependsOn The [GroupId] of the query plan that generates the values.
     * @param column The[ColumnDef] that should be extracted from the results.
     * @return [Binding.Subquery]
     */
    override fun bind(dependsOn: GroupId, column: ColumnDef<*>): Binding.Subquery {
        val binding = Binding.Subquery(dependsOn, column)
        this.boundSubqueries[binding.dependsOn] = LinkedList()
        return binding
    }

    /**
     * Updates the [Value] for a [Binding.Literal].
     *
     * @param binding The [Binding.Literal] to update.
     * @param value The [Value] to bind.
     * @return A value [Binding]
     */
    override fun update(binding: Binding.Literal, value: Value?) {
        require(value == null || binding.type == value.type) { "Value $value cannot be bound to $binding because of type mismatch (${binding.type})."}
        this.boundLiterals[binding.bindingIndex] = value
    }

    /**
     * Appends a [Value] for a [Binding.Subquery].
     *
     * @param binding The [Binding.Subquery] to append value to.
     * @param value The new [Value] to append.
     */
    override fun append(binding: Binding.Subquery, value: Value) {
        val list = this.boundSubqueries[binding.dependsOn] ?: throw IllegalStateException("Could not find data collection for sub-query ${binding.dependsOn}. This is a programmer's error!")
        list.add(value)
    }

    /**
     * Clears all [Value]s for a [Binding.Subquery].
     *
     * @param binding The [Binding.Subquery] to clear.
     */
    override fun clear(binding: Binding.Subquery) {
        val list = this.boundSubqueries[binding.dependsOn] ?: throw IllegalStateException("Could not find data collection for sub-query ${binding.dependsOn}. This is a programmer's error!")
        list.clear()
    }

    /**
     * Creates a copy of this [DefaultBindingContext].
     *
     * @return Copy of this [DefaultBindingContext].
     */
    override fun copy(): BindingContext {
        val copy = DefaultBindingContext()
        this.boundLiterals.forEach { copy.boundLiterals.add(it) }
        this.boundFunctions.forEach { copy.boundFunctions.add(it.copyOf()) }
        this.boundSubqueries.forEach { (k, v) -> copy.boundSubqueries[k] = LinkedList(v) }
        return copy
    }
}