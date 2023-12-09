package org.vitrivr.cottontail.dbms.entity.serialization

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.tuple.StandaloneTuple
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.BooleanVectorValue
import org.vitrivr.cottontail.core.values.IntValue
import org.vitrivr.cottontail.core.values.generators.BooleanVectorValueGenerator
import org.vitrivr.cottontail.test.TestConstants
import java.util.*

/**
 * Test case that tests for correctness of [BooleanVectorValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.4.0
 */
class BooleanVectorValueSerializationTest : AbstractSerializationTest() {

    /** Create a random vector between 2 and 2048 dimensions. */
    private val d = SplittableRandom().nextInt(2, TestConstants.LARGE_VECTOR_MAX_DIMENSION)

    /** Columns tested by this [BooleanVectorValueSerializationTest]. */
    override val columns: Array<ColumnDef<*>> = arrayOf(
        ColumnDef(this.entityName.column("id"), Types.Int),
        ColumnDef(this.entityName.column("vector"), Types.BooleanVector(this.d))
    )

    /** Name of this [BooleanVectorValueSerializationTest]. */
    override val name: String = "BooleanVectorSerialization($d)"

    /**
     * Generates the next [StandaloneTuple] and returns it.
     */
    override fun nextRecord(i: Int): StandaloneTuple {
        val id = IntValue(i)
        val vector = BooleanVectorValueGenerator.random(this.d, this.random)
        return StandaloneTuple(0L, columns = this.columns, values = arrayOf(id, vector))
    }
}