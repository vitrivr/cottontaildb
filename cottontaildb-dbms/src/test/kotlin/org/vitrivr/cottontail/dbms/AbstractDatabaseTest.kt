package org.vitrivr.cottontail.dbms

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.config.Config
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.dbms.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionManager
import org.vitrivr.cottontail.server.Instance
import org.vitrivr.cottontail.test.TestConstants
import org.vitrivr.cottontail.utilities.io.TxFileUtilities
import java.nio.file.Files

/**
 * Abstract class for unit tests that require a Cottontail DB database.
 *
 * @author Ralph Gasser
 * @version 1.2.0
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

    /** The [Instance] used by this [AbstractDatabaseTest]. */
    protected val instance = Instance(this.config)

    /** Pointer to the underlying [DefaultCatalogue], for convenience. */
    protected val catalogue: DefaultCatalogue
        get() = this.instance.catalogue

    /** Pointer to the underlying [TransactionManager], for convenience. */
    protected val manager: TransactionManager
        get() = this.instance.transactions

    /** The [Logger] instance used by this [AbstractDatabaseTest]. */
    protected val logger = LOGGER

    /**
     * Initializes this [AbstractDatabaseTest].
     */
    @BeforeEach
    protected open fun initialize() {}

    /**
     * Tears down this [AbstractDatabaseTest].
     */
    @AfterEach
    protected open fun teardown() {
        /* Shutdown thread pool executor. */
        this.instance.close()

        /* Delete unnecessary files. */
        TxFileUtilities.delete(this.config.root)
    }
}