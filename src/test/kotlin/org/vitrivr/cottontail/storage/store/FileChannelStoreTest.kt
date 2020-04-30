package org.vitrivr.cottontail.storage.store

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.RepeatedTest
import org.junit.jupiter.api.Test
import java.nio.ByteBuffer
import java.nio.file.Files
import java.nio.file.Paths
import java.util.*

class FileChannelStoreTest {

    private val path = Paths.get("./test.db")
    private val random = Random(System.currentTimeMillis())
    private var store: FileChannelStore? = null

    @BeforeEach
    fun initialize() {
        this.store = FileChannelStore(this.path, false)

    }

    @AfterEach
    fun teardown() {
        this.store?.close()
        Files.delete(this.path)
    }

    @Test
    fun sizeTestAllFail() {
        assertThrows(IllegalArgumentException::class.java) { this.store!!.putByte(0L, this.random.nextInt(Byte.MAX_VALUE.toInt()).toByte()) }
        assertThrows(IllegalArgumentException::class.java) { this.store!!.putShort(0L, this.random.nextInt(Short.MAX_VALUE.toInt()).toShort()) }
        assertThrows(IllegalArgumentException::class.java) { this.store!!.putDouble(0L, this.random.nextDouble()) }
        assertThrows(IllegalArgumentException::class.java) { this.store!!.putFloat(0L, this.random.nextFloat()) }
        assertThrows(IllegalArgumentException::class.java) { this.store!!.putInt(0L, this.random.nextInt()) }
        assertThrows(IllegalArgumentException::class.java) { this.store!!.putLong(0L, this.random.nextLong()) }
    }

    @Test
    fun sizeTestWithTwoBytes() {
        this.store!!.grow(2)
        assertDoesNotThrow { this.store!!.putByte(0L, this.random.nextInt(Byte.MAX_VALUE.toInt()).toByte()) }
        assertDoesNotThrow { this.store!!.putShort(0L, this.random.nextInt(Short.MAX_VALUE.toInt()).toShort()) }
        assertThrows(IllegalArgumentException::class.java) { this.store!!.putDouble(0L, this.random.nextDouble()) }
        assertThrows(IllegalArgumentException::class.java) { this.store!!.putFloat(0L, this.random.nextFloat()) }
        assertThrows(IllegalArgumentException::class.java) { this.store!!.putInt(0L, this.random.nextInt()) }
        assertThrows(IllegalArgumentException::class.java) { this.store!!.putLong(0L, this.random.nextLong()) }
    }

    @Test
    fun sizeTestWithFourBytes() {
        this.store!!.grow(4)
        assertDoesNotThrow { this.store!!.putByte(0L, this.random.nextInt(Byte.MAX_VALUE.toInt()).toByte()) }
        assertDoesNotThrow { this.store!!.putShort(0L, this.random.nextInt(Short.MAX_VALUE.toInt()).toShort()) }
        assertDoesNotThrow { this.store!!.putInt(0L, this.random.nextInt()) }
        assertDoesNotThrow { this.store!!.putFloat(0L, this.random.nextFloat()) }

        assertThrows(IllegalArgumentException::class.java) { this.store!!.putDouble(0L, this.random.nextDouble()) }
        assertThrows(IllegalArgumentException::class.java) { this.store!!.putLong(0L, this.random.nextLong()) }
    }

    @Test
    fun sizeTestWithEightBytes() {
        this.store!!.grow(8)
        assertDoesNotThrow { this.store!!.putByte(0L, this.random.nextInt(Byte.MAX_VALUE.toInt()).toByte()) }
        assertDoesNotThrow { this.store!!.putByte(0L, this.random.nextInt(Byte.MAX_VALUE.toInt()).toByte()) }
        assertDoesNotThrow { this.store!!.putByte(0L, this.random.nextInt(Byte.MAX_VALUE.toInt()).toByte()) }
        assertDoesNotThrow { this.store!!.putByte(0L, this.random.nextInt(Byte.MAX_VALUE.toInt()).toByte()) }
        assertDoesNotThrow { this.store!!.putDouble(0L, this.random.nextDouble()) }
        assertDoesNotThrow { this.store!!.putLong(0L, this.random.nextLong()) }
    }

    @Test
    @RepeatedTest(3)
    fun sizeTestWithRandomSize() {
        val size = this.random.nextInt(1_000_000_000)
        val data = ByteArray(size + 1)
        this.random.nextBytes(data)

        this.store!!.grow(size.toLong())
        assertThrows(IllegalArgumentException::class.java) { this.store!!.putData(0L, data) }
        assertDoesNotThrow { this.store!!.putData(0L, ByteBuffer.wrap(data, 1, data.size - 1)) }
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

    @RepeatedTest(3)
    fun writeDataTest() {
        val size = this.random.nextInt(1_000_000_000).toLong()

        assertTrue(store!!.size == 0L)

        this.store!!.grow(size)

        assertTrue(this.store!!.size == size)
        val data = ByteArray(size.toInt())
        this.random.nextBytes(data)

        this.store!!.putData(0L, data)

        val data1 = this.store!!.getData(0L, ByteArray(size.toInt()))
        assertTrue(data.contentEquals(data1))
    }

}