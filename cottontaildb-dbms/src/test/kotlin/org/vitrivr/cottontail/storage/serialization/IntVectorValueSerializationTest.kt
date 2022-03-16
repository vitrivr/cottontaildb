package org.vitrivr.cottontail.storage.serialization

import org.vitrivr.cottontail.TestConstants
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.recordset.StandaloneRecord
import org.vitrivr.cottontail.core.values.IntValue
import org.vitrivr.cottontail.core.values.IntVectorValue
import org.vitrivr.cottontail.core.values.generators.IntVectorValueGenerator
import org.vitrivr.cottontail.core.values.types.Types
import java.util.*

/**
 * Test case that tests for correctness of [IntVectorValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.3.0
 */
class IntVectorValueSerializationTest : AbstractSerializationTest() {
    /** Create a random vector between 2 and 2048 dimensions. */
    private val d = SplittableRandom().nextInt(2, TestConstants.largeVectorMaxDimension)

    /** Columns tested by this [IntVectorValueSerializationTest]. */
    override val columns: Array<ColumnDef<*>> = arrayOf(
        ColumnDef(this.entityName.column("id"), Types.Int),
        ColumnDef(this.entityName.column("vector"), Types.IntVector(this.d))
    )

    /** Name of this [IntVectorValueSerializationTest]. */
    override val name: String = "IntVectorSerialization($d)"

    /**
     * Generates the next [StandaloneRecord] and returns it.
     */
    override fun nextRecord(i: Int): StandaloneRecord {
        val id = IntValue(i)
        val vector = IntVectorValueGenerator.random(this.d, this.random)
        return StandaloneRecord(0L, columns = this.columns, values = arrayOf(id, vector))
    }
}