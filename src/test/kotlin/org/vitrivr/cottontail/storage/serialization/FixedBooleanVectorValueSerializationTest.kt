package org.vitrivr.cottontail.storage.serialization

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.vitrivr.cottontail.TestConstants
import org.vitrivr.cottontail.database.catalogue.Catalogue
import org.vitrivr.cottontail.database.column.ColumnType
import org.vitrivr.cottontail.database.general.begin
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.recordset.StandaloneRecord
import org.vitrivr.cottontail.model.values.BooleanVectorValue
import org.vitrivr.cottontail.model.values.IntValue
import org.vitrivr.cottontail.utilities.VectorUtility
import java.nio.file.Files
import java.util.*
import java.util.stream.Collectors

/**
 * Test cases that test for correctness of [BooleanVectorValue] serialization
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class FixedBooleanVectorValueSerializationTest {
    /** */
    private val random = SplittableRandom()

    /** */
    private var catalogue: Catalogue = Catalogue(TestConstants.config)

    /** */
    private var schema = catalogue.let {
        val name = Name.SchemaName("schema-test")
        it.createSchema(name)
        it.schemaForName(name)
    }

    @AfterEach
    fun teardown() {
        this.catalogue.close()
        val pathsToDelete = Files.walk(TestConstants.config.root).sorted(Comparator.reverseOrder()).collect(Collectors.toList())
        pathsToDelete.forEach { Files.delete(it) }
    }

    /**
     * Executes the test.
     */
    @ParameterizedTest
    @ValueSource(ints = [32, 33, 64, 65, 128, 130, 256, 260, 512, 1024, 1023, 2048])
    fun test(dimension: Int) {
        val nameEntity = this.schema.name.entity("boolean-test")
        val idCol = ColumnDef(nameEntity.column("id"), ColumnType.forName("INTEGER"), -1, false)
        val vectorCol = ColumnDef(nameEntity.column("vector"), ColumnType.forName("BOOL_VEC"), dimension, false)
        val columns = arrayOf(idCol, vectorCol)

        /* Prepare entity. */
        this.schema.createEntity(nameEntity, idCol, vectorCol)
        val schema = this.schema.entityForName(nameEntity)

        /* Prepare random number generator. */
        val seed = System.currentTimeMillis()
        val r1 = SplittableRandom(seed)

        /* Insert data into column. */
        schema.Tx(false).begin { tx1 ->
            var i1 = 1L
            VectorUtility.randomBoolVectorSequence(dimension, 100_000, r1).forEach {
                tx1.insert(StandaloneRecord(columns = arrayOf(idCol, vectorCol), values = arrayOf(IntValue(++i1), it)))
            }
            true
        }

        /* Read data from column. */
        val r2 = SplittableRandom(seed)
        val tx2 = schema.Tx(true).begin { tx2 ->
            var i2 = 1L
            VectorUtility.randomBoolVectorSequence(dimension, 100_000, r2).forEach {
                val rec2 = tx2.read(++i2, columns)
                assertArrayEquals(it.data, (rec2[vectorCol] as BooleanVectorValue).data)
            }
            true
        }
    }
}