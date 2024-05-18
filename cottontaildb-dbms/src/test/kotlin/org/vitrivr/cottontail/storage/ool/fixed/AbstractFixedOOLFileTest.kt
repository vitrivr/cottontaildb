package org.vitrivr.cottontail.storage.ool.fixed

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.core.types.VectorValue
import org.vitrivr.cottontail.core.values.generators.ValueGenerator.Companion.random
import org.vitrivr.cottontail.core.values.generators.VectorValueGenerator
import org.vitrivr.cottontail.dbms.AbstractDatabaseTest
import org.vitrivr.cottontail.dbms.entity.values.StoredValueRef
import org.vitrivr.cottontail.storage.ool.FixedOOLFile
import org.vitrivr.cottontail.storage.ool.interfaces.AccessPattern
import org.vitrivr.cottontail.test.TestConstants
import org.vitrivr.cottontail.utilities.io.TxFileUtilities
import java.util.*

/**
 * A unit test for the [FixedOOLFile] class.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
abstract class AbstractFixedOOLFileTest<V: VectorValue<*>> {

    /** The [Types] of the [Value]s stored in the [FixedOOLFile]. */
    protected abstract val type: Types<V>

    /** The [FixedOOLFile] to use for the test. */
    private lateinit var file: FixedOOLFile<V>

    /**
     * Tests the basic read-write functionality of a [FixedOOLFile].
     */
    @Test
    fun test() {
        val generator = this.type.random() as VectorValueGenerator<V>
        val writer = this.file.writer()
        val readerS = this.file.reader(AccessPattern.SEQUENTIAL)
        val readerR = this.file.reader(AccessPattern.RANDOM)

        /* Data structures for tests. */
        val values = mutableListOf<V>()
        val references = mutableListOf<StoredValueRef.OutOfLine.Fixed>()

        /* Generate data. */
        repeat(TestConstants.TEST_COLLECTION_SIZE) {
            val value = generator.random(type.logicalSize)
            values.add(value)
            references.add(writer.append(value))
        }
        writer.flush()

        /* Check generated data. */
        repeat(TestConstants.TEST_COLLECTION_SIZE) {
            val value = values[it]

            /* Compare ground truth with read values. */
            Assertions.assertTrue(value.isEqual(readerS.read(references[it])), "Sequential read failed for value $it.")
            Assertions.assertTrue(value.isEqual(readerR.read(references[it])), "Random read failed for value $it.")
        }
    }

    /**
     * Tests the basic read-write functionality of a [FixedOOLFile] with in-between flushes.
     */
    @Test
    fun testWithFlush() {
        val random = SplittableRandom()
        val generator = this.type.random() as VectorValueGenerator<V>
        val writer = this.file.writer()
        val readerS = this.file.reader(AccessPattern.SEQUENTIAL)
        val readerR = this.file.reader(AccessPattern.RANDOM)

        /* Data structures for tests. */
        val values = mutableListOf<V>()
        val references = mutableListOf<StoredValueRef.OutOfLine.Fixed>()

        /* Generate data. */
        val mod = random.nextInt(256, 1024)
        repeat(TestConstants.TEST_COLLECTION_SIZE) {
            val value = generator.random(type.logicalSize)
            values.add(value)
            references.add(writer.append(value))
            if (it % mod == 0) {
                writer.flush()
            }
        }
        writer.flush()

        /* Check generated data. */
        repeat(TestConstants.TEST_COLLECTION_SIZE) {
            val value = values[it]

            /* Compare ground truth with read values. */
            Assertions.assertTrue(value.isEqual(readerS.read(references[it])), "Sequential read failed for value $it.")
            Assertions.assertTrue(value.isEqual(readerR.read(references[it])), "Random read failed for value $it.")
        }
    }

    /**
     * Initializes this [AbstractDatabaseTest].
     */
    @BeforeEach
    fun initialize() {
        this.file = FixedOOLFile(TestConstants.testConfig().dataFolder(UUID.randomUUID()), this.type)
    }

    /**
     * Tears down this [AbstractDatabaseTest].
     */
    @AfterEach
    fun teardown() {
        TxFileUtilities.delete(this.file.path)
    }
}