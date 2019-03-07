package ch.unibas.dmi.dbis.cottontail.database.index.lucene

import ch.unibas.dmi.dbis.cottontail.database.entity.Entity
import ch.unibas.dmi.dbis.cottontail.database.general.begin
import ch.unibas.dmi.dbis.cottontail.database.index.Index
import ch.unibas.dmi.dbis.cottontail.database.index.IndexType
import ch.unibas.dmi.dbis.cottontail.database.queries.Predicate
import ch.unibas.dmi.dbis.cottontail.model.basics.ColumnDef
import ch.unibas.dmi.dbis.cottontail.model.exceptions.ValidationException
import ch.unibas.dmi.dbis.cottontail.model.recordset.Recordset
import ch.unibas.dmi.dbis.cottontail.model.values.StringValue
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document.Document
import org.apache.lucene.document.Field
import org.apache.lucene.document.NumericDocValuesField
import org.apache.lucene.document.TextField
import org.apache.lucene.index.*
import org.apache.lucene.search.FuzzyQuery
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.store.Directory
import org.apache.lucene.store.MMapDirectory
import java.nio.file.Path
import kotlin.concurrent.read
import kotlin.concurrent.write

internal class LuceneIndex(override val name: String, override val parent: Entity, override val columns: Array<ColumnDef<*>>) : Index() {


    override val path: Path = this.parent.path.resolve("idx_$name")

    override val type: IndexType = IndexType.LUCENE

    @Volatile
    override var closed: Boolean = false
        private set

    private val directory: Directory
    private val analyzer: Analyzer
    private val indexWriter: IndexWriter
    private val indexWriterConf: IndexWriterConfig
    private val indexReader: IndexReader
    private val indexSearcher: IndexSearcher

    init {
        directory = MMapDirectory.open(this.path)
        analyzer = StandardAnalyzer(); //TODO depending on settings
        indexWriterConf = IndexWriterConfig(analyzer)
        indexWriter = IndexWriter(directory, indexWriterConf)
        indexReader = DirectoryReader.open(directory)
        indexSearcher = IndexSearcher(indexReader)
    }

    private fun document(text: String, tid: Long): Document = Document().apply {
            add(TextField("text", text, Field.Store.NO))
            add(NumericDocValuesField("tid", tid))
    }


    override fun rebuild()  = txLock.write {
        this.indexWriter.deleteAll()
        this.parent.Tx(readonly = true, columns = this.columns).begin { tx ->
            tx.forEach {
                val value = it.values.joinToString(separator = ", ")
                val document = document(value, it.tupleId)

                this.indexWriter.addDocument(document)

            }
            this.indexWriter.flush()
            this.indexWriter.commit()
            true
        }
    }

    override fun filter(predicate: Predicate): Recordset = this.txLock.read {

        val queryString = "" //TODO get from predicate

        val queryTerm = Term("text", queryString)

        val query = FuzzyQuery(queryTerm) //TODO query type based on predicate

        val results = this.indexSearcher.search(query, 100) //TODO specify n


        TODO("not implemented")
    }

    override fun close()  = this.globalLock.write {
        if (!this.closed) {
            this.indexReader.close()
            this.indexWriter.close()
            this.directory.close()
            this.closed = true
        }
    }

    override fun canProcess(predicate: Predicate): Boolean {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }


}