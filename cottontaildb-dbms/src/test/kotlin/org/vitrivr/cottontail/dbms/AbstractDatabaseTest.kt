package org.vitrivr.cottontail.dbms

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.config.Config
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.dbms.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.dbms.execution.ExecutionManager
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionManager
import org.vitrivr.cottontail.test.TestConstants
import org.vitrivr.cottontail.utilities.io.TxFileUtilities
import java.nio.file.Files

/**
 * Abstract class for unit tests that require a Cottontail DB database.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
abstract class AbstractDatabaseTest {
    companion object {
        protected val LOGGER: Logger = LoggerFactory.getLogger(AbstractDatabaseTest::class.java)
    }
    
    /** [Config] used for this [AbstractDatabaseTest]. */
    private val config: Config = TestConstants.testConfig()

    init {
        /* Assure that root folder is empty! */
        if (Files.exists(this.config.root)) {
            TxFileUtilities.delete(this.config.root)
        }
        Files.createDirectories(this.config.root)
    }

    /** [Name.SchemaName] of the test schema. */
    protected val schemaName = Name.SchemaName.create("test")

    /** The [ExecutionManager] used for tests. */
    private val executor = ExecutionManager(this.config)

    /** Catalogue used for testing. */
    protected var catalogue: DefaultCatalogue = DefaultCatalogue(this.config, this.executor)

    /** Pointer to the underlying [TransactionManager] for convenience sake. */
    protected val manager: TransactionManager
        get() = this.catalogue.transactionManager

    /** The [Logger] instance used by this [AbstractDatabaseTest]. */
    protected val logger = LOGGER

    /**
     * Initializes this [AbstractDatabaseTest].
     */
    @BeforeEach
    protected open fun initialize() {

    }

    /**
     * Tears down this [AbstractDatabaseTest].
     */
    @AfterEach
    protected open fun teardown() {
        /* Shutdown thread pool executor. */
        this.executor.shutdownAndWait()

        /* Close catalogue. */
        this.catalogue.close()

        /* Delete unnecessary files. */
        TxFileUtilities.delete(this.config.root)
    }
}