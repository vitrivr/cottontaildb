package org.vitrivr.cottontail.database.statistics.entity

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap
import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.vitrivr.cottontail.database.events.DataChangeEvent
import org.vitrivr.cottontail.database.statistics.columns.ValueStatistics
import org.vitrivr.cottontail.model.basics.TupleId
import org.vitrivr.cottontail.model.values.types.Value
import kotlin.math.max

/**
 * A data object that collects statistics for an entity.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
data class EntityStatistics(var count: Long = -1L, var maximumTupleId: TupleId = -1L, val columns: Map<String, ValueStatistics<Value>>) {

    /**
     * Serializer for [EntityStatistics] object.
     */
    companion object Serializer : org.mapdb.Serializer<EntityStatistics> {
        override fun serialize(out: DataOutput2, value: EntityStatistics) {
            out.packLong(value.count)
            out.packLong(value.maximumTupleId)
            out.packInt(value.columns.size)
            value.columns.forEach { (t, u) ->
                out.writeUTF(t)
                ValueStatistics.serialize(out, u)
            }
        }

        override fun deserialize(input: DataInput2, available: Int): EntityStatistics {
            val count = input.unpackLong()
            val maximumTupleId = input.unpackLong()
            val map = Object2ObjectOpenHashMap<String, ValueStatistics<Value>>()
            repeat(input.unpackInt()) {
                map.putIfAbsent(input.readUTF(), ValueStatistics.deserialize(input, available) as ValueStatistics<Value>)
            }
            return EntityStatistics(count, maximumTupleId, map)
        }
    }

    /**
     * Consumes a [DataChangeEvent] and updates the [ValueStatistics] in this [EntityStatistic].
     *
     * @param event The [DataChangeEvent] to process
     */
    fun consume(event: DataChangeEvent) = when (event) {
        is DataChangeEvent.DeleteDataChangeEvent -> {
            this.count -= 1
            event.deleted.forEach { (t, u) -> this.columns[t.name.simple]?.delete(u) }
        }
        is DataChangeEvent.InsertDataChangeEvent -> {
            this.count += 1
            this.maximumTupleId = max(this.maximumTupleId, event.tupleId)
            event.inserts.forEach { (t, u) -> this.columns[t.name.simple]?.insert(u) }
        }
        is DataChangeEvent.UpdateDataChangeEvent -> {
            event.updates.forEach { (t, u) -> this.columns[t.name.simple]?.update(u.first, u.second) }
        }
    }
}