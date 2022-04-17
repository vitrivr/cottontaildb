package org.vitrivr.cottontail.cli

import io.grpc.ManagedChannel
import io.grpc.netty.NettyChannelBuilder
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.vitrivr.cottontail.cli.entity.DumpEntityCommand
import org.vitrivr.cottontail.cli.entity.ImportDataCommand
import org.vitrivr.cottontail.cli.entity.TruncateEntityCommand
import org.vitrivr.cottontail.client.SimpleClient
import org.vitrivr.cottontail.client.language.dql.Query
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.data.Format
import org.vitrivr.cottontail.test.EmbeddedCottontailGrpcServer
import org.vitrivr.cottontail.test.GrpcTestUtils
import org.vitrivr.cottontail.test.TestConstants
import java.nio.file.Path
import java.util.concurrent.TimeUnit
import kotlin.io.path.Path
import kotlin.io.path.deleteIfExists
import kotlin.time.ExperimentalTime

@ExperimentalTime
class ExportImportCommandTest {

    private val entityName: Name.EntityName = Name.EntityName(TestConstants.TEST_SCHEMA, TestConstants.TEST_ENTITY)

    /** The [EmbeddedCottontailGrpcServer] used for this unit test. */
    private val embedded = EmbeddedCottontailGrpcServer(TestConstants.testConfig())

    /** The [ManagedChannel] used for this unit test. */
    private val channel: ManagedChannel = NettyChannelBuilder.forAddress("localhost", 1865).usePlaintext().build()

    /** The [SimpleClient] used for this unit test. */
    private var client: SimpleClient = SimpleClient(this.channel)


    private val formats = listOf(Format.JSON)

    @BeforeEach
    fun startCottontail() {
        assert(client.ping())
        GrpcTestUtils.dropTestSchema(client)
        GrpcTestUtils.createTestSchema(client)
        GrpcTestUtils.createTestVectorEntity(client)
        GrpcTestUtils.createTestEntity(client)
        GrpcTestUtils.populateTestEntity(client)
        GrpcTestUtils.populateVectorEntity(client)
        assert(client.ping())
    }

    @AfterEach
    fun cleanup() {
        GrpcTestUtils.dropTestSchema(this.client)

        /* Shutdown ManagedChannel. */
        this.channel.shutdown()
        this.channel.awaitTermination(5000, TimeUnit.MILLISECONDS)

        /* Stop embedded server. */
        this.embedded.shutdownAndWait()
    }

    fun exportFolder(): Path {
        return Path("data", "test", "export")
    }

    @Test
    fun exportCreatesFile() {
        formats.forEach { format ->
            val path = exportFolder()
            path.toFile().mkdirs()
            val exported = exportFile(format)
            exported.deleteIfExists()
            DumpEntityCommand.dumpEntity(this.entityName, path, format, client)
            assert(exported.toFile().exists())
            assert(exported.toFile().totalSpace > 1)
        }
    }

    private fun exportFile(format: Format): Path {
        return exportFolder().resolve("warren.${TestConstants.TEST_SCHEMA}.${TestConstants.TEST_ENTITY}.${format.suffix}")
    }

    @Test
    fun exportImport() {
        exportCreatesFile()
        formats.forEach { format ->
            val exportFile = exportFile(format)
            val count = countElements()
            TruncateEntityCommand.truncate(this.entityName, this.client, true)
            ImportDataCommand.importData(this.entityName, exportFile, format, this.client, true)
            assert(count == countElements())
        }
    }

    fun countElements(): Long? {
        val query = Query(this.entityName.toString()).count()
        val res = this.client.query(query)
        return res.next().asLong(0)
    }
}