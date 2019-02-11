package ch.unibas.dmi.dbis.cottontail.database

import ch.unibas.dmi.dbis.cottontail.TestConstants
import ch.unibas.dmi.dbis.cottontail.database.catalogue.Catalogue
import ch.unibas.dmi.dbis.cottontail.database.column.ColumnDef
import ch.unibas.dmi.dbis.cottontail.database.general.begin
import ch.unibas.dmi.dbis.cottontail.database.schema.Schema
import ch.unibas.dmi.dbis.cottontail.model.basics.StandaloneRecord

import ch.unibas.dmi.dbis.cottontail.utilities.VectorUtility
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.RepeatedTest

import java.util.*
import kotlin.collections.HashMap
import kotlin.math.absoluteValue


import org.junit.jupiter.api.Assertions.*


class DataTest {
    private val schemaName = "data-test"

    private val random = Random()

    /** */
    private val catalogue = Catalogue(TestConstants.config)

    private var schema: Schema? = null


    @BeforeEach
    fun initialize() {
        catalogue.createSchema(schemaName)
        schema = catalogue.getSchema(schemaName)
    }

    @AfterEach
    fun teardown() {
        catalogue.dropSchema(schemaName)
    }

    /**
     * Tries to persist some random float vectors + Int field and tests, if the persisted version equals the original version.
     */
    @RepeatedTest(25)
    fun insertFloatVectorTest() {
        val size = random.nextInt(1024).absoluteValue
        val count = 10000
        System.out.println("Running float vector test with d=$size.")

        val intField = ColumnDef.withAttributes("counter", "INTEGER", size)
        val vectorField = ColumnDef.withAttributes("vector", "FLOAT_VEC", size)

        schema?.createEntity("vector-test", intField, vectorField)
        val entity = schema?.getEntity("vector-test")

        /* Insert the float vectors. */
        val tx = entity?.Tx(false)
        val iterator = VectorUtility.randomFloatVectorSequence(size, count)
        val vectorMap = HashMap<Long,FloatArray>()
        val counterMap = HashMap<Long,Int>()
        var counter = 0
        tx?.begin {
            iterator.forEach {
                val record = StandaloneRecord(null, intField, vectorField)
                record[intField] = counter
                record[vectorField] = it
                val tid = tx.insert(record)
                assertNotNull(tid)
                vectorMap[tid!!] = it
                counterMap[tid] = counter
                counter += 1
            }
            true
        }

        /* Fetch and compare the float vectors. */
        val tx2 = entity?.Tx(true)
        assertEquals(count.toLong(), tx2?.count())
        tx2?.begin {
            vectorMap.forEach { t, u ->
                val tuple = tx2.read(t, intField, vectorField)
                assertArrayEquals(u, tuple[vectorField] as FloatArray)
                assertEquals(counterMap[t], tuple[intField] as Int)
            }
            true
        }
    }

}