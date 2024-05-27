package org.vitrivr.cottontail.dbms.index.deg

import org.vitrivr.cottontail.core.basics.Cursor
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.core.types.VectorValue
import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.dbms.entity.values.StoredTuple
import org.vitrivr.cottontail.dbms.entity.values.StoredValue

class DEGIndexCursor(query: VectorValue<*>, k: Int, private val columns: Array<ColumnDef<*>>, private val index: DEGIndex.Tx): Cursor<Tuple> {

    /** List of results; this is obtained lazily. */
    private val list by lazy {
        this.index.graph.search(query, k, 0.8f, this.index.graph.randomNodes(4))
    }

    /** The current position of this [DEGIndexCursor]. */
    private var position = -1

    override fun hasNext(): Boolean = this.position < (this.list.size - 1)
    override fun moveNext(): Boolean = (++this.position) < this.list.size
    override fun key(): TupleId = this.list[this.position].label
    override fun value(): Tuple {
        val entry = this.list[this.position]
        val tupleId = entry.label
        val tuple = this.index.parent.read(tupleId) as StoredTuple
        return StoredTuple(tupleId, this.columns, arrayOf(*tuple.values, StoredValue.Inline(DoubleValue(entry.distance))))
    }
    override fun next(): Tuple {
        if (!this.moveNext()) throw NoSuchElementException("No more elements in cursor.")
        return this.value()
    }

    /**
     * Closes this [DEGIndexCursor].
     */
    override fun close() {
        /* Nothing to do here. */
    }
}