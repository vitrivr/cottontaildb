package org.vitrivr.cottontail.storage.serialization

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.RepeatedTest
import org.vitrivr.cottontail.TestConstants
import org.vitrivr.cottontail.database.catalogue.Catalogue
import org.vitrivr.cottontail.database.column.ColumnType
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.recordset.StandaloneRecord
import org.vitrivr.cottontail.model.values.Complex64VectorValue
import org.vitrivr.cottontail.model.values.IntValue
import org.vitrivr.cottontail.utilities.VectorUtility
import org.vitrivr.cottontail.utilities.name.Name
import org.junit.jupiter.api.Assertions.*
import org.vitrivr.cottontail.model.values.BooleanVectorValue
import java.nio.file.Files
import java.util.*
import java.util.stream.Collectors

/**
 * Test cases that test for correctness of some basic distance calculations with [Complex64VectorValue].
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
        val name = Name("schema-test")
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
     *
     */
    @RepeatedTest(100)
    fun test() {
        val size = this.random.nextInt(2048)
        val nameEntity = Name("boolean-test")
        val idCol =  ColumnDef(Name("id"), ColumnType.forName("INTEGER"), -1, false)
        val vectorCol = ColumnDef(Name("vector"), ColumnType.forName("BOOL_VEC"), size, false)


        /* Prepare entity. */
        this.schema.createEntity(nameEntity, idCol, vectorCol)
        val schema = this.schema.entityForName(nameEntity)

        /* Prepare random number generator. */
        val seed = System.currentTimeMillis()
        val r1 = SplittableRandom(seed)

        /* Insert data into column. */
        val tx1 = schema.Tx(false)
        val rec1 = StandaloneRecord(columns = arrayOf(idCol, vectorCol))
        var i1 = 1L
        VectorUtility.randomBoolVectorSequence(size, 1_000_000, r1).forEach {
            rec1.assign(arrayOf(IntValue(++i1), it))
            tx1.insert(rec1)
        }
        tx1.commit()
        tx1.close()

        /* Read data from column. */
        val r2 = SplittableRandom(seed)
        val tx2 = schema.Tx(true)
        var i2 = 1L
        try {
            VectorUtility.randomBoolVectorSequence(size, 1_000_000, r2).forEach {
                val rec2 = tx2.read(++i2)
                assertArrayEquals(it.data, (rec2[vectorCol] as BooleanVectorValue).data)
            }
        } finally {
            tx2.close()
        }
    }
}