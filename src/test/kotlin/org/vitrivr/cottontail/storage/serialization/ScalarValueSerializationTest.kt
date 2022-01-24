package org.vitrivr.cottontail.storage.serialization

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.column.ColumnEngine
import org.vitrivr.cottontail.model.values.types.Types
import org.vitrivr.cottontail.model.recordset.StandaloneRecord
import org.vitrivr.cottontail.model.values.*
import org.vitrivr.cottontail.model.values.types.Value

/**
 * Test case that tests for correctness of [LongVectorValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
class ScalarValueSerializationTest : AbstractSerializationTest() {

    /** Columns tested by this [DoubleVectorValueSerializationTest]. */
    override val columns: Array<Pair<ColumnDef<*>, ColumnEngine>> = arrayOf(
        ColumnDef(this.entityName.column("id"), Types.Int) to ColumnEngine.MAPDB,
        ColumnDef(this.entityName.column("int"), Types.Int) to ColumnEngine.MAPDB,
        ColumnDef(this.entityName.column("long"), Types.Long) to ColumnEngine.MAPDB,
        ColumnDef(this.entityName.column("double"), Types.Double) to ColumnEngine.MAPDB,
        ColumnDef(this.entityName.column("float"), Types.Float) to ColumnEngine.MAPDB,
        ColumnDef(this.entityName.column("byte"), Types.Byte) to ColumnEngine.MAPDB,
        ColumnDef(this.entityName.column("short"), Types.Short) to ColumnEngine.MAPDB,
    )

    /** Name of this [LongVectorValueSerializationTest]. */
    override val name: String = "ScalarValueSerialization"

    /**
     * Generates the next [StandaloneRecord] and returns it.
     */
    override fun nextRecord(i: Int): StandaloneRecord {
        val values: Array<Value?> = arrayOf(
            IntValue(i),
            IntValue(this.random.nextInt()),
            LongValue(this.random.nextLong()),
            DoubleValue(this.random.nextDouble()),
            FloatValue(this.random.nextDouble()),
            ByteValue(this.random.nextInt()),
            ShortValue(this.random.nextInt())
        )
        return StandaloneRecord(0L, columns = this.columns.map { it.first }.toTypedArray(), values = values)
    }
}