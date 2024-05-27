package org.vitrivr.cottontail.storage.lucene

import jetbrains.exodus.env.Environment
import jetbrains.exodus.env.Environments
import org.apache.commons.math3.random.JDKRandomGenerator
import org.apache.lucene.store.ByteBuffersDirectory
import org.apache.lucene.store.IOContext
import org.apache.lucene.store.IndexInput
import org.apache.lucene.store.IndexOutput
import org.junit.jupiter.api.*
import org.vitrivr.cottontail.dbms.AbstractDatabaseTest
import org.vitrivr.cottontail.test.TestConstants
import org.vitrivr.cottontail.utilities.io.TxFileUtilities

/**
 * A series of unit tests for the [XodusDirectory].
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class XodusDirectoryTest {
    /** The random number generator. */
    val random = JDKRandomGenerator()

    /** Temporary directory for tests. */
    private val path = TestConstants.testConfig().root

    /** The Xodus [Environment] used by this [XodusDirectoryTest]. */
    private val environment = Environments.newInstance(this.path.toFile())

    /**
     * Tests ordinary reading and writing from/to [IndexInput] and [IndexOutput]
     */
    @Test
    fun testSingleTxRequestAndRenameFile() {
        val txn = this.environment.beginTransaction()
        val directory = XodusDirectory("test", txn)
        try {
            /* Prepare data to write. */
            val output = directory.createOutput("test", IOContext())
            val bytes = ByteArray(4096)
            this.random.nextBytes(bytes)

            /* Write to output . */
            for (b in bytes) {
                output.writeByte(b)
            }

            /* Close output. */
            output.close()

            /* Test file's existence. */
            Assertions.assertTrue(directory.listAll().contains("test"))

            /* Rename file and test again. */
            directory.rename("test", "test1")
            Assertions.assertFalse(directory.listAll().contains("test"))
            Assertions.assertTrue(directory.listAll().contains("test1"))
        } finally { /* Commit. */
            directory.close()
            txn.commit()
        }
    }

    /**
     * Tests ordinary reading and writing from/to [IndexInput] and [IndexOutput]
     */
    @Test
    fun testSingleTxWriteAndRead() {
        /* Prepare directories. */
        val txn = this.environment.beginTransaction()
        val directory = XodusDirectory("test", txn)

        try {
            val referenceDirectory = ByteBuffersDirectory()

            /* Prepare data to write. */
            val output = directory.createOutput("test", IOContext())
            val outputRef = referenceDirectory.createOutput("test", IOContext())
            val bytes = ByteArray(4096)
            this.random.nextBytes(bytes)

            /* Write to output . */
            for (b in bytes) {
                output.writeByte(b)
                outputRef.writeByte(b)
            }

            /* Compare the checksums. */
            Assertions.assertEquals(output.checksum, outputRef.checksum)

            /* Close outputs. */
            output.close()
            outputRef.close()

            /* Read data. */
            val input = directory.openInput("test", IOContext())
            val inputRef = referenceDirectory.openInput("test", IOContext())

            Assertions.assertEquals(input.length(), inputRef.length())
            for (i in 0 until input.length()) {
                Assertions.assertEquals(input.readByte(), inputRef.readByte())
            }

            /* Close inputs. */
            input.close()
            inputRef.close()
        } finally { /* Commit txn. */
            directory.close()
            txn.commit()
        }
    }

    /**
     * Tests ordinary reading and writing from/to [IndexInput] and [IndexOutput]
     */
    @Test
    fun testSingleTxBatchedWriteAndRead() {
        /* Prepare directories. */
        val txn = this.environment.beginTransaction()
        val directory = XodusDirectory("test", txn)

        try {
            val referenceDirectory = ByteBuffersDirectory()

            /* Prepare data to write. */
            val output = directory.createOutput("test", IOContext())
            val outputRef = referenceDirectory.createOutput("test", IOContext())
            val bytes = ByteArray(4096)
            this.random.nextBytes(bytes)

            /* Write to output . */
            output.writeBytes(bytes, 0, 1024)
            output.writeBytes(bytes, 1027, 1)
            output.writeBytes(bytes, 2048, 1024)

            /* Write to reference output. */
            outputRef.writeBytes(bytes, 0, 1024)
            outputRef.writeBytes(bytes, 1027, 1)
            outputRef.writeBytes(bytes, 2048, 1024)

            /* Compare the checksums. */
            Assertions.assertEquals(output.checksum, outputRef.checksum)

            /* Close outputs. */
            output.close()
            outputRef.close()

            /* Read data. */
            val input = directory.openInput("test", IOContext())
            val inputRef = referenceDirectory.openInput("test", IOContext())

            Assertions.assertEquals(input.length(), inputRef.length())
            for (i in 0 until input.length()) {
                Assertions.assertEquals(input.readByte(), inputRef.readByte())
                Assertions.assertEquals(input.filePointer, inputRef.filePointer)
            }

            /* Close inputs. */
            input.close()
            inputRef.close()
        } finally {
            /* Commit txn. */
            directory.close()
            txn.commit()
        }
    }

    /**
     * Tests ordinary reading and writing from/to [IndexInput] and [IndexOutput]
     */
    @Test
    fun testSingleTxBatchedWriteAndSlicedRead() {
        /* Prepare directories. */
        val txn = this.environment.beginTransaction()
        val directory = XodusDirectory("test", txn)

        try {
            val referenceDirectory = ByteBuffersDirectory()

            /* Prepare data to write. */
            val output = directory.createOutput("test", IOContext())
            val outputRef = referenceDirectory.createOutput("test", IOContext())
            val bytes = ByteArray(4096)
            this.random.nextBytes(bytes)

            /** Write to output . */
            output.writeBytes(bytes, 0, 1024)
            output.writeBytes(bytes, 1027, 1)
            output.writeBytes(bytes, 2048, 1024)

            /** Write to reference output. */
            outputRef.writeBytes(bytes, 0, 1024)
            outputRef.writeBytes(bytes, 1027, 1)
            outputRef.writeBytes(bytes, 2048, 1024)

            /** Compare the checksums. */
            Assertions.assertEquals(output.checksum, outputRef.checksum)

            /** Close outputs. */
            output.close()
            outputRef.close()

            /** Read data. */
            val input = directory.openInput("test", IOContext())
            val inputRef = referenceDirectory.openInput("test", IOContext())

            /* Create and compare slice. */
            val sliced = input.slice("test", 1024, 1024)
            val slicedRef = input.slice("test", 1024, 1024)

            Assertions.assertEquals(sliced.length(), slicedRef.length())
            for (i in 0 until sliced.length()) {
                Assertions.assertEquals(sliced.readByte(), slicedRef.readByte())
                Assertions.assertEquals(sliced.filePointer, slicedRef.filePointer)
            }

            /* Create and compare slice of slice. */
            val sliced2 = sliced.slice("test2", 128, 512)
            val slicedRef2 = sliced.slice("test2", 128, 512)
            Assertions.assertEquals(sliced2.length(), slicedRef2.length())
            for (i in 0 until sliced2.length()) {
                Assertions.assertEquals(sliced2.readByte(), slicedRef2.readByte())
                Assertions.assertEquals(sliced2.filePointer, slicedRef2.filePointer)
            }

            /* Close inputs. */
            input.close()
            inputRef.close()
        } finally {
            /* Commit txn. */
            directory.close()
            txn.commit()
        }
    }

    /**
     * Tests ordinary reading and writing from/to [IndexInput] and [IndexOutput]
     */
    @Test
    fun testSingleTxBatchedWriteAndSeekRead() {
        /* Prepare directories. */
        val txn = this.environment.beginTransaction()
        val directory = XodusDirectory("test", txn)

        try {
            val referenceDirectory = ByteBuffersDirectory()

            /* Prepare data to write. */
            val output = directory.createOutput("test", IOContext())
            val outputRef = referenceDirectory.createOutput("test", IOContext())
            val bytes = ByteArray(4096)
            this.random.nextBytes(bytes)

            /** Write to output . */
            output.writeBytes(bytes, 0, 1024)
            output.writeBytes(bytes, 1027, 1)
            output.writeBytes(bytes, 2048, 1024)

            /** Write to reference output. */
            outputRef.writeBytes(bytes, 0, 1024)
            outputRef.writeBytes(bytes, 1027, 1)
            outputRef.writeBytes(bytes, 2048, 1024)

            /** Compare the checksums. */
            Assertions.assertEquals(output.checksum, outputRef.checksum)

            /** Close outputs. */
            output.close()
            outputRef.close()

            /** Read data. */
            val input = directory.openInput("test", IOContext())
            val inputRef = referenceDirectory.openInput("test", IOContext())

            /** Seek position. */
            val seek = this.random.nextLong(1024L)
            input.seek(seek)
            inputRef.seek(seek)

            /* Compare bytes. */
            Assertions.assertEquals(input.length(), inputRef.length())
            for (i in 0 until (input.length() - seek)) {
                Assertions.assertEquals(input.readByte(), inputRef.readByte())
                Assertions.assertEquals(input.filePointer, inputRef.filePointer)
            }

            /* Close inputs. */
            input.close()
            inputRef.close()
            directory.close()
        } finally {
            /* Commit txn. */
            directory.close()
            txn.commit()
        }
    }

    /**
     * Tests ordinary reading and writing from/to [IndexInput] and [IndexOutput]
     */
    @Test
    fun testMultiTxBatchedWriteAndRead() {
        /* Prepare directories. */
        val txn1 = this.environment.beginTransaction()
        val directory = XodusDirectory("test", txn1)
        val referenceDirectory = ByteBuffersDirectory()

        /* Prepare data to write. */
        val output = directory.createOutput("test", IOContext())
        val outputRef = referenceDirectory.createOutput("test", IOContext())
        val bytes = ByteArray(4096)
        this.random.nextBytes(bytes)

        /** Write to output . */
        output.writeBytes(bytes, 0, 1024)
        output.writeBytes(bytes, 1027, 1)
        output.writeBytes(bytes, 2048, 1024)

        /** Write to reference output. */
        outputRef.writeBytes(bytes, 0, 1024)
        outputRef.writeBytes(bytes, 1027, 1)
        outputRef.writeBytes(bytes, 2048, 1024)

        /* Compare the checksums. */
        Assertions.assertEquals(output.checksum, outputRef.checksum)

        /* Close outputs. */
        output.close()
        outputRef.close()

        /* Commit transaction. */
        directory.close()
        txn1.commit()

        /* Open new Txn and XodusDirectory. */
        val txn2 = this.environment.beginTransaction()
        val directory2 = XodusDirectory("test", txn2)

        /** Read data in txn2. */
        val input = directory2.openInput("test", IOContext())
        val inputRef = referenceDirectory.openInput("test", IOContext())
        Assertions.assertEquals(input.length(), inputRef.length())
        for (i in 0 until input.length()) {
            Assertions.assertEquals(input.readByte(), inputRef.readByte())
        }

        /**Close inputs. */
        input.close()
        inputRef.close()

        /* Abort second transaction. */
        directory2.close()
        txn2.abort()
    }

    /**
     * Initializes this [AbstractDatabaseTest].
     */
    @BeforeEach
    fun initialize() {
    }

    /**
     * Tears down this [AbstractDatabaseTest].
     */
    @AfterEach
    fun teardown() {
        this.environment.clear()
    }

    @AfterAll
    fun cleanup() {
        this.environment.close()
        TxFileUtilities.delete(this.path)
    }
}