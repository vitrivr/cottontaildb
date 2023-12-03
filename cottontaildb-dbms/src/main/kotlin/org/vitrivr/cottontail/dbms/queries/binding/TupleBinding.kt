package org.vitrivr.cottontail.dbms.queries.binding

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.core.queries.binding.BindingContext
import org.vitrivr.cottontail.core.tuple.MutableTuple
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.core.types.Value

/**
 * A [Tuple] implementation that depends on the existence of [Binding]s for the [Value]s it contains. Used for inserts.
 *
 * @author Ralph Gasser
 * @version 1.3.0
 */
class TupleBinding(override var tupleId: TupleId, override val columns: Array<ColumnDef<*>>, private val values: Array<Binding.Literal>, private val context: BindingContext) : MutableTuple {

    init {
        require(this.columns.size == this.values.size) { "Number of values and number of columns must be the same." }
        for ((c, b) in this.columns.zip(this.values)) {
            require((c.nullable || c.autoIncrement) || (!(c.nullable || c.autoIncrement) && !b.canBeNull)) { "Value binding $b is incompatible with column $c." }
            require(c.type == b.type) { "Value binding $b is incompatible with column $c." }
        }
    }

    /**
     * Creates a copy of this [TupleBinding]
     */
    override fun copy(): Tuple = TupleBinding(this.tupleId, this.columns.copyOf(), this.values.copyOf(), this.context)

    /**
     * Returns a list of all [Value]s contained in this [TupleBinding].
     *
     * @return [List] of [Value]
     */
    override fun values(): List<Value?> = with(this.context) {
        return this@TupleBinding.values.map { it.getValue() }
    }

    /**
     * Retrieves the value for the specified column index from this [TupleBinding].
     *
     * @param index The index for which to retrieve the value.
     * @return The value for the column index.
     */
    override fun get(index: Int): Value? = with(this.context) {
        require(index in (0 until this@TupleBinding.size)) { "The specified column $index is out of bounds." }
        return this@TupleBinding.values[index].getValue()
    }

    /**
     * Sets the [Value]  for the specified [ColumnDef] in this [Tuple].
     *
     * @param column The [ColumnDef] for which to set the value.
     * @param value The new [Value]
     */
    override fun set(column: ColumnDef<*>, value: Value?) = this.set(this.columns.indexOf(column), value)

    /**
     * Sets the [Value]  for the specified column index in this [Tuple].
     *
     * @param index The column index for which to set the value.
     * @param value The new [Value]
     */
    override fun set(index: Int, value: Value?) = with(this.context){
        require(index in (0 until this@TupleBinding.size)) { "The specified column $index is out of bounds." }
        this@TupleBinding.values[index].update(value)
    }
}