package org.vitrivr.cottontail.dbms.index.lucene

import org.apache.lucene.document.*
import org.apache.lucene.index.*
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.values.StringValue
import org.vitrivr.cottontail.storage.lucene.XodusDirectory
import java.io.Closeable

/**
 * This is an abstraction over a [XodusDirectory] that provides certain primitives required by the [LuceneIndex].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
internal class LuceneIndexDataStore(val directory: XodusDirectory, private val indexedColumn: Name.ColumnName): Closeable {
    /** Flag indicating, that [IndexReader] was initialized. */
    @Volatile
    private var readerInitialized = false

    /** Flag indicating, that [IndexWriter] was initialized. */
    @Volatile
    private var writerInitialized = false

    @Volatile
    /** Flag indicating whether [LuceneIndexDataStore] was closed. */
    private var closed = false

    /** The [IndexWriter] instance used for accessing the [LuceneIndex]. */
    val indexWriter: IndexWriter by lazy {
        val config = IndexWriterConfig().setOpenMode(IndexWriterConfig.OpenMode.APPEND).setMergeScheduler(
            SerialMergeScheduler()
        )
        this.writerInitialized = true
        IndexWriter(this.directory, config)
    }

    /** The [IndexReader] instance used for accessing the [LuceneIndex]. */
    val indexReader: IndexReader by lazy {
        this.readerInitialized = true
        DirectoryReader.open(this.directory)
    }

    /**
     * Adds a document with the given [TupleId] and [StringValue] from this [LuceneIndexDataStore].
     *
     * @param tupleId The [TupleId] to add.
     * @param value The [StringValue] to add.
     */
    fun addDocument(tupleId: TupleId, value: StringValue) {
        require(!this.closed) { "Cannot add document to a closed LuceneIndexDataStore. This is a programmer's error!" }
        this.indexWriter.addDocument(documentFromValue(value, tupleId))
        this.checkAndFlush()
    }

    /**
     * Updates a document with the given [TupleId] and [StringValue] from this [LuceneIndexDataStore].
     *
     * @param tupleId The [TupleId] to add.
     * @param value The new (updated) [StringValue].
     */
    fun updateDocument(tupleId: TupleId, value: StringValue) {
        require(!this.closed) { "Cannot add document to a closed LuceneIndexDataStore. This is a programmer's error!" }
        this.indexWriter.updateDocument(Term(LuceneIndex.TID_COLUMN, tupleId.toString()), documentFromValue(value, tupleId))
        this.checkAndFlush()
    }

    /**
     * Deletes the document with the given [TupleId] from this [LuceneIndexDataStore].
     *
     * @param tupleId The [TupleId] to delete.
     */
    fun deleteDocument(tupleId: TupleId) {
        require(!this.closed) { "Cannot add document to a closed LuceneIndexDataStore. This is a programmer's error!" }
        this.indexWriter.deleteDocuments(Term(LuceneIndex.TID_COLUMN, tupleId.toString()))
        this.checkAndFlush()
    }

    /**
     * Closes this [LuceneIndexDataStore]
     */
    override fun close() {
        if (!this.closed) {
            /* Commit and close writer. */
            if (this.writerInitialized) {
                if (this.indexWriter.hasUncommittedChanges()) {
                    this.indexWriter.commit()
                }
                this.indexWriter.close()
            }

            /* Close reader. */
            if (this.readerInitialized) {
                this.indexReader.close()
            }

            /* Close directory. */
            this.directory.close()
            this.closed = true
        }
    }

    /**
     * Internal function that flushes changes if too many pending documents are registered.
     */
    private fun checkAndFlush() {
        if (this.indexWriter.pendingNumDocs % 1_000_000L == 0L) {
            this.indexWriter.flush()
        }
    }

    /**
     * Converts a [StringValue] and a [TupleId] to [Document] that can be processed by Lucene.
     *
     * @param value: [StringValue] to process
     * @param tupleId The [TupleId] to process
     * @return The resulting [Document]
     */
    private fun documentFromValue(value: StringValue, tupleId: TupleId): Document {
        val doc = Document()
        doc.add(NumericDocValuesField(LuceneIndex.TID_COLUMN, tupleId))
        doc.add(StoredField(LuceneIndex.TID_COLUMN, tupleId))
        doc.add(TextField("${this.indexedColumn.column}_txt", value.value, Field.Store.NO))
        doc.add(StringField("${this.indexedColumn.column}_str", value.value, Field.Store.NO))
        return doc
    }
}