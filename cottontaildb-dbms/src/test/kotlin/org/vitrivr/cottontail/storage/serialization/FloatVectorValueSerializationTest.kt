package org.vitrivr.cottontail.storage.serialization

import org.vitrivr.cottontail.TestConstants
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.recordset.StandaloneRecord
import org.vitrivr.cottontail.core.values.FloatVectorValue
import org.vitrivr.cottontail.core.values.IntValue
import org.vitrivr.cottontail.core.values.generators.FloatVectorValueGenerator
import org.vitrivr.cottontail.core.values.types.Types
import java.util.*

/**
 * Test case that tests for correctness of [FloatVectorValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.3.0
 */
class FloatVectorValueSerializationTest : AbstractSerializationTest() {
    /** Create a random vector between 2 and 2048 dimensions. */
    private val d = SplittableRandom().nextInt(2, TestConstants.largeVectorMaxDimension)

    /** Columns tested by this [FloatVectorValueSerializationTest]. */
    override val columns: Array<ColumnDef<*>> = arrayOf(
        ColumnDef(this.entityName.column("id"), Types.Int),
        ColumnDef(this.entityName.column("vector"), Types.FloatVector(this.d))
    )

    /** Name of this [FloatVectorValueSerializationTest]. */
    override val name: String = "FloatVectorSerialization($d)"

    /**
     * Generates the next [StandaloneRecord] and returns it.
     */
    override fun nextRecord(i: Int): StandaloneRecord {
        val id = IntValue(i)
        val vector = FloatVectorValueGenerator.random(this.d, this.random)
        return StandaloneRecord(0L, columns = this.columns, values = arrayOf(id, vector))
    }
}