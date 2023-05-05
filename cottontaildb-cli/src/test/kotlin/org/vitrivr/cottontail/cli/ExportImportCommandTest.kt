package org.vitrivr.cottontail.cli

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.vitrivr.cottontail.cli.entity.DumpEntityCommand
import org.vitrivr.cottontail.cli.entity.ImportDataCommand
import org.vitrivr.cottontail.cli.entity.TruncateEntityCommand
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.data.Format
import org.vitrivr.cottontail.test.AbstractClientTest
import org.vitrivr.cottontail.test.GrpcTestUtils
import org.vitrivr.cottontail.test.GrpcTestUtils.countElements
import org.vitrivr.cottontail.test.TestConstants
import org.vitrivr.cottontail.test.TestConstants.TEST_ENTITY_NAME
import java.nio.file.Path
import kotlin.io.path.Path
import kotlin.io.path.deleteIfExists
import kotlin.time.ExperimentalTime

@ExperimentalTime
class ExportImportCommandTest : AbstractClientTest() {

    private val formats = listOf(Format.JSON)

    @BeforeEach
    fun beforeEach() {
        this.startAndPopulateCottontail()
    }

    @AfterEach
    fun afterEach() {
        this.cleanup()
    }

    fun exportFolder(): Path {
        return Path("data", "test", "export")
    }

    @Test
    fun exportCreatesFile() {
        formats.forEach { format ->
            TestConstants.ALL_ENTITY_NAMES.forEach { name ->
                exportEntity(format, name)
            }
        }
    }

    private fun exportFile(format: Format, entityName: Name.EntityName): Path {
        return exportFolder().resolve("${entityName.fqn}.${format.suffix}")
    }

    @Test
    fun exportImport() {
        exportCreatesFile()
        formats.forEach { format ->
            TestConstants.ALL_ENTITY_NAMES.forEach { entityName ->
                val exportFile = exportFile(format, entityName)
                val count = countElements(this.client, entityName)
                TruncateEntityCommand.truncate(entityName, this.client)
                ImportDataCommand.importData(entityName, exportFile, format, this.client, true)
                assert(count == countElements(this.client, entityName))
            }
        }
    }

    @Test
    fun exportNan() {
        GrpcTestUtils.insertIntoTestEntity(client, double = Double.NaN)
        GrpcTestUtils.insertIntoTestEntity(client)
        formats.forEach { format ->
            exportEntity(format, TEST_ENTITY_NAME)
            val exportFile = exportFile(format, TEST_ENTITY_NAME)
            val count = countElements(this.client, TEST_ENTITY_NAME)
            TruncateEntityCommand.truncate(TEST_ENTITY_NAME, this.client)
            ImportDataCommand.importData(TEST_ENTITY_NAME, exportFile, format, this.client, true)
            assert(count == countElements(this.client, TEST_ENTITY_NAME))
        }
    }

    private fun exportEntity(format: Format, fqn: Name.EntityName) {
        val path = exportFolder()
        path.toFile().mkdirs()
        val exported = exportFile(format, fqn)
        exported.deleteIfExists()
        DumpEntityCommand.dumpEntity(fqn, path, format, this.client)
        assert(exported.toFile().exists())
        assert(exported.toFile().totalSpace > 1)
    }
}