package org.vitrivr.cottontail.cli

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.vitrivr.cottontail.cli.entity.DumpEntityCommand
import org.vitrivr.cottontail.cli.entity.ImportDataCommand
import org.vitrivr.cottontail.cli.entity.TruncateEntityCommand
import org.vitrivr.cottontail.client.language.dql.Query
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.data.Format
import org.vitrivr.cottontail.test.AbstractClientTest
import org.vitrivr.cottontail.test.GrpcTestUtils.countElements
import org.vitrivr.cottontail.test.GrpcTestUtils.toEn
import org.vitrivr.cottontail.test.TestConstants
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
            TestConstants.entityNames.forEach { entityName ->
                val path = exportFolder()
                path.toFile().mkdirs()
                val exported = exportFile(format, entityName)
                exported.deleteIfExists()
                DumpEntityCommand.dumpEntity(toEn(entityName), path, format, this.client)
                assert(exported.toFile().exists())
                assert(exported.toFile().totalSpace > 1)
            }
        }
    }

    private fun exportFile(format: Format, entityName: String): Path {
        return exportFolder().resolve("warren.${TestConstants.TEST_SCHEMA}.${entityName}.${format.suffix}")
    }

    @Test
    fun exportImport() {
        exportCreatesFile()
        formats.forEach { format ->
            TestConstants.entityNames.forEach { entityName ->
                val exportFile = exportFile(format, entityName)
                val count = countElements(this.client, entityName)
                TruncateEntityCommand.truncate(toEn(entityName), this.client, true)
                ImportDataCommand.importData(toEn(entityName), exportFile, format, this.client, true)
                assert(count == countElements(this.client, entityName))
            }
        }
    }
}