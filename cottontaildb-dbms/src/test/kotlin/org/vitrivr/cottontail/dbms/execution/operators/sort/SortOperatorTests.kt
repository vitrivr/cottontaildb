package org.vitrivr.cottontail.dbms.execution.operators.sort

import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.sort.SortOrder
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.dbms.execution.operators.DummyQueryContext
import org.vitrivr.cottontail.dbms.execution.operators.RandomTupleSourceOperator
import org.vitrivr.cottontail.test.TestConstants
import java.lang.Math.floorDiv
import java.util.*

class SortOperatorTests {

    /** The schema used for the [SortOperatorTests]. */
    private val schema = arrayOf(
        ColumnDef(TestConstants.TEST_ENTITY_NAME.column(TestConstants.ID_COLUMN_NAME), Types.Int, false, true, true),
        ColumnDef(TestConstants.TEST_ENTITY_NAME.column(TestConstants.STRING_COLUMN_NAME), Types.String, false, false, false),
        ColumnDef(TestConstants.TEST_ENTITY_NAME.column(TestConstants.DOUBLE_COLUMN_NAME), Types.Double, false, false, false)
    )

    /** The random seed used for testing.  */
    private val seed = System.currentTimeMillis()

    /**
     * Test case for in-memory sorting using [HeapSortOperator] (ascending).
     */
    @ParameterizedTest
    @ValueSource(ints = [0, 1, 2])
    fun testInMemorySortAsc(columnIndex : Int) {
        val random = SplittableRandom(this.seed)
        val source = RandomTupleSourceOperator(0, TestConstants.TEST_COLLECTION_SIZE, random, this.schema.toList(), DummyQueryContext)
        val sort = HeapSortOperator(source, listOf(this.schema[columnIndex] to SortOrder.ASCENDING), TestConstants.TEST_COLLECTION_SIZE, DummyQueryContext)
        val sorted = ArrayList<Tuple>(TestConstants.TEST_COLLECTION_SIZE)

        /* Execute the two operators. */
        runBlocking {
            sort.toFlow().collect {
                sorted.add(it)
            }
        }

        /* Now create manually sorted versions of the list. */
        for (i in 0 .. 2) {
            val mSorted = sorted.sortedBy { it[i] }
            if (i != columnIndex) {
                Assertions.assertNotEquals(sorted, mSorted)
            } else {
                Assertions.assertEquals(sorted, mSorted)
            }
        }
    }

    /**
     * Test case for in-memory sorting using [HeapSortOperator] (descending).
     */
    @ParameterizedTest
    @ValueSource(ints = [0, 1, 2])
    fun testInMemorySortDesc(columnIndex : Int) {
        val random = SplittableRandom(this.seed)
        val source = RandomTupleSourceOperator(0, TestConstants.TEST_COLLECTION_SIZE, random, this.schema.toList(), DummyQueryContext)
        val sort = HeapSortOperator(source, listOf(this.schema[columnIndex] to SortOrder.DESCENDING), TestConstants.TEST_COLLECTION_SIZE, DummyQueryContext)
        val sorted = ArrayList<Tuple>(TestConstants.TEST_COLLECTION_SIZE)

        /* Execute the two operators. */
        runBlocking {
            sort.toFlow().collect {
                sorted.add(it)
            }
        }

        /* Now create manually sorted versions of the list. */
        for (i in 0 .. 2) {
            val mSorted = sorted.sortedByDescending { it[i] }
            if (i != columnIndex) {
                Assertions.assertNotEquals(sorted, mSorted)
            } else {
                Assertions.assertEquals(sorted, mSorted)
            }
        }
    }

    /**
     * Test case for external sorting using [ExternalMergeSortOperator].
     */
    @ParameterizedTest
    @ValueSource(ints = [0, 1, 2])
    fun testExternalSortAsc(columnIndex: Int) {
        val random = SplittableRandom(this.seed)
        val source = RandomTupleSourceOperator(0, TestConstants.TEST_COLLECTION_SIZE, random, this.schema.toList(), DummyQueryContext)
        val sort = ExternalMergeSortOperator(source, listOf(this.schema[columnIndex] to SortOrder.ASCENDING), floorDiv(TestConstants.TEST_COLLECTION_SIZE, 1000), DummyQueryContext)
        val sorted = ArrayList<Tuple>(TestConstants.TEST_COLLECTION_SIZE)

        /* Execute the two operators. */
        runBlocking {
            sort.toFlow().collect {
                sorted.add(it)
            }
        }

        /* Now create manually sorted versions of the list. */
        for (i in 0 .. 2) {
            val mSorted = sorted.sortedBy { it[i] }
            if (i != columnIndex) {
                Assertions.assertNotEquals(sorted, mSorted)
            } else {
                Assertions.assertEquals(sorted, mSorted)
            }
        }
    }
}