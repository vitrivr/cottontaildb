package org.vitrivr.cottontail.dbms.entity.sequence

import org.junit.jupiter.api.Assertions
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.core.values.LongValue
import org.vitrivr.cottontail.test.TestConstants

class LongSequenceTest: AbstractSequenceTest() {
    /** The test entity. */
    override val entities: List<Pair<Name.EntityName, List<ColumnDef<*>>>> = listOf(
        TestConstants.TEST_ENTITY_NAME to listOf(
            ColumnDef(TestConstants.TEST_ENTITY_NAME.column(TestConstants.ID_COLUMN_NAME), Types.Long, nullable = false, primary = true, autoIncrement = true),
            ColumnDef(TestConstants.TEST_ENTITY_NAME.column(TestConstants.STRING_COLUMN_NAME), Types.String, nullable = false, primary = false, autoIncrement = false),
        )
    )

    override fun test(value: Value?, index: Int) = Assertions.assertEquals(index+1L, (value as? LongValue)!!.value)
}