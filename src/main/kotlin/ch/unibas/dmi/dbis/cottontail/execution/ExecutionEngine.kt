package ch.unibas.dmi.dbis.cottontail.execution

import ch.unibas.dmi.dbis.cottontail.config.Config
import ch.unibas.dmi.dbis.cottontail.sql.antlr.CottonSQLLexer
import ch.unibas.dmi.dbis.cottontail.sql.antlr.CottonSQLParser
import ch.unibas.dmi.dbis.cottontail.sql.Context
import ch.unibas.dmi.dbis.cottontail.sql.metamodel.StatementList
import ch.unibas.dmi.dbis.cottontail.sql.toAst
import org.antlr.v4.runtime.CharStreams
import org.antlr.v4.runtime.CommonTokenStream

import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit



class ExecutionEngine(config: Config) {

    /** The [ThreadPoolExecutor] used for executing queries. */
    private val executor = ThreadPoolExecutor(config.executionConfig.coreThreads, config.executionConfig.maxThreads, config.executionConfig.keepAliveMs, TimeUnit.MILLISECONDS, ArrayBlockingQueue(config.executionConfig.queueSize))

    /**
     *
     */
    internal fun parse(sql: String, context: Context): StatementList {
        val lexer = CottonSQLLexer(CharStreams.fromString(sql))
        val tokens = CommonTokenStream(lexer)
        return CottonSQLParser(tokens).root().sql_stmt_list().toAst(context)
    }

    /**
     * Creates and returns a new [ExecutionPlan].
     *
     * @return [ExecutionPlan]
     */
    fun newExecutionPlan() = ExecutionPlan(this.executor)


}