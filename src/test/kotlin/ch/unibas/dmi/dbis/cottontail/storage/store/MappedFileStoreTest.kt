package ch.unibas.dmi.dbis.cottontail.storage.store

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.RepeatedTest
import java.nio.file.Paths
import java.util.*
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import java.nio.file.Files

class MappedFileStoreTest {

    private val path = Paths.get("./test.db")
    private val random = Random(System.currentTimeMillis())
    private var store: MappedFileStore? = null

    @BeforeEach
    fun initialize() {
        this.store = MappedFileStore(this.path, false)

    }

    @AfterEach
    fun teardown() {
        this.store?.close()
        Files.delete(this.path)
    }

    @RepeatedTest(3)
    fun writeDoubleTest() {
        val size = this.random.nextInt(1_000_000_000).toLong()

        assertTrue(store!!.size == 0L)

        this.store!!.grow(size)

        assertTrue(this.store!!.size == size)
        val indexes = LongArray(1000)
        val values = DoubleArray(1000)
        for (i in values.indices) {
            values[i] = this.random.nextDouble()
            indexes[i] = this.random.nextInt(size.toInt()).toLong()
            this.store!!.putDouble(indexes[i], values[i])
        }

        for (i in values.indices) {
            assertEquals(this.store!!.getDouble(indexes[i]), values[i])
        }
    }

    @RepeatedTest(3)
    fun writeFloatTest() {
        val size = this.random.nextInt(1_000_000_000).toLong()

        assertTrue(store!!.size == 0L)

        this.store!!.grow(size)

        assertTrue(this.store!!.size == size)
        val indexes = LongArray(1000)
        val values = FloatArray(1000)
        for (i in values.indices) {
            values[i] = this.random.nextFloat()
            indexes[i] = this.random.nextInt(size.toInt()).toLong()

            this.store!!.putFloat(indexes[i], values[i])
        }

        for (i in values.indices) {
            assertEquals(this.store!!.getFloat(indexes[i]), values[i])
        }
    }

    @RepeatedTest(3)
    fun writeIntTest() {
        val size = this.random.nextInt(1_000_000_000).toLong()

        assertTrue(store!!.size == 0L)

        this.store!!.grow(size)

        assertTrue(this.store!!.size == size)
        val indexes = LongArray(1000)
        val values = IntArray(1000)
        for (i in values.indices) {
            values[i] = this.random.nextInt()
            indexes[i] = this.random.nextInt(size.toInt()).toLong()
            this.store!!.putInt(indexes[i], values[i])
        }

        for (i in values.indices) {
            assertEquals(this.store!!.getInt(indexes[i]), values[i])
        }
    }

    @RepeatedTest(3)
    fun writeLongTest() {
        val size = this.random.nextInt(1_000_000_000).toLong()

        assertTrue(store!!.size == 0L)

        this.store!!.grow(size)

        assertTrue(this.store!!.size == size)
        val indexes = LongArray(1000)
        val values = LongArray(1000)
        for (i in values.indices) {
            values[i] = this.random.nextLong()
            indexes[i] = this.random.nextInt(size.toInt()).toLong()
            this.store!!.putLong(indexes[i], values[i])
        }

        for (i in values.indices) {
            assertEquals(this.store!!.getLong(indexes[i]), values[i])
        }
    }

    @RepeatedTest(3)
    fun writeShortTest() {
        val size = this.random.nextInt(1_000_000_000).toLong()

        assertTrue(store!!.size == 0L)

        this.store!!.grow(size)

        assertTrue(this.store!!.size == size)
        val indexes = LongArray(1000)
        val values = ShortArray(1000)
        for (i in values.indices) {
            values[i] = this.random.nextInt(Short.MAX_VALUE.toInt()).toShort()
            indexes[i] = this.random.nextInt(size.toInt()).toLong()
            this.store!!.putShort(indexes[i], values[i])
        }

        for (i in values.indices) {
            assertEquals(this.store!!.getShort(indexes[i]), values[i])
        }
    }

    @RepeatedTest(3)
    fun writeByteTest() {
        val size = this.random.nextInt(1_000_000_000).toLong()

        assertTrue(store!!.size == 0L)

        this.store!!.grow(size)

        assertTrue(this.store!!.size == size)
        val indexes = LongArray(1000)
        val values = ByteArray(1000)
        for (i in values.indices) {
            values[i] = this.random.nextInt(Byte.MAX_VALUE.toInt()).toByte()
            indexes[i] = this.random.nextInt(size.toInt()).toLong()
            this.store!!.putByte(indexes[i], values[i])
        }

        for (i in values.indices) {
            assertEquals(this.store!!.getByte(indexes[i]), values[i])
        }
    }

}