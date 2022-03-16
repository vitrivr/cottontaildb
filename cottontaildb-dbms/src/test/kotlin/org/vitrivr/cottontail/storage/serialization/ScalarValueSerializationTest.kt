package org.vitrivr.cottontail.storage.serialization

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.recordset.StandaloneRecord
import org.vitrivr.cottontail.core.values.*
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value

/**
 * Test case that tests for correctness of [LongVectorValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.3.0
 */
class ScalarValueSerializationTest : AbstractSerializationTest() {

    /** Columns tested by this [DoubleVectorValueSerializationTest]. */
    override val columns: Array<ColumnDef<*>> = arrayOf(
        ColumnDef(this.entityName.column("id"), Types.Int),
        ColumnDef(this.entityName.column("int"), Types.Int),
        ColumnDef(this.entityName.column("long"), Types.Long),
        ColumnDef(this.entityName.column("double"), Types.Double),
        ColumnDef(this.entityName.column("float"), Types.Float),
        ColumnDef(this.entityName.column("byte"), Types.Byte),
        ColumnDef(this.entityName.column("short"), Types.Short),
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
        return StandaloneRecord(0L, columns = this.columns, values = values)
    }
}