package query

// Javalin Context
import io.javalin.http.Context

// vitrivr SimpleClient
import org.vitrivr.cottontail.client.SimpleClient
import org.vitrivr.cottontail.client.language.dql.Query

// Caches defined in Main.kt
import channelCache
import com.google.gson.Gson
import pageCache
import pagedCache
import queryCache
private val gson = Gson()

data class QueryFunctionCall (val name: String, val parameters : List<String>)

object QueryController {

    data class QueryPagesKey (val sessionID: String,
                             val queryRequest : Array<QueryFunctionCall>,
                             val pageSize: Int,
                             val port: Int) {
        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (javaClass != other?.javaClass) return false

            other as QueryPagesKey

            if (sessionID != other.sessionID) return false
            if (!queryRequest.contentEquals(other.queryRequest)) return false
            if (pageSize != other.pageSize) return false
            if (port != other.port) return false

            return true
        }

        override fun hashCode(): Int {
            var result = sessionID.hashCode()
            result = 31 * result + queryRequest.contentHashCode()
            result = 31 * result + pageSize
            result = 31 * result + port
            return result
        }
    }

    data class QueryPageKey (val sessionID: String,
                             val queryRequest : Array<QueryFunctionCall>,
                             val pageSize: Int,
                             val page: Int,
                             val port: Int) {
        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (javaClass != other?.javaClass) return false

            other as QueryPageKey

            if (sessionID != other.sessionID) return false
            if (!queryRequest.contentEquals(other.queryRequest)) return false
            if (pageSize != other.pageSize) return false
            if (page != other.page) return false
            if (port != other.port) return false

            return true
        }

        override fun hashCode(): Int {
            var result = sessionID.hashCode()
            result = 31 * result + queryRequest.contentHashCode()
            result = 31 * result + pageSize
            result = 31 * result + page
            result = 31 * result + port
            return result
        }
    }

    data class QueryKey(val sessionID: String,
                        val queryRequest: Array<QueryFunctionCall>,
                        val port: Int) {
        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (javaClass != other?.javaClass) return false

            other as QueryKey

            if (sessionID != other.sessionID) return false
            if (!queryRequest.contentEquals(other.queryRequest)) return false
            if (port != other.port) return false

            return true
        }

        override fun hashCode(): Int {
            var result = sessionID.hashCode()
            result = 31 * result + queryRequest.contentHashCode()
            result = 31 * result + port
            return result
        }
    }

    class QueryData(private val columnNames: List<String>, private val rows: MutableList<List<String>>){

        fun paginate(size: Int) : List<Page>{
            println("PAGINATING")
            val chunks = rows.chunked(size)
            val pages = mutableListOf<Page>()
            repeat(chunks.size){
                val page = Page(columnNames, chunks[it], size, it, rows.size)
                pages.add(page)
            }
            return pages
        }
    }

    class Page(val columnNames: List<String>, val rows:List<List<String>>, val pageSize: Int, val page: Int, val numberOfRows: Int)


    fun query(context: Context)  {

        println("QUERY CALL")

        val sessionId = context.req().session.id
        println("SESSION_ID: $sessionId")

        val queryRequest = gson.fromJson(context.body(), Array<QueryFunctionCall>::class.java)

        val pageSize = context.queryParam("pageSize")
        val page = context.queryParam("page")
        val port = context.pathParam("port").toInt()


        if ( page != null && pageSize != null) {
            val key = QueryPageKey(sessionId, queryRequest, pageSize.toInt(), page.toInt(), port)
            println(key)
            context.json(pageCache.get(key))
        } else {
            context.status(400).result("Bad Request: page and/or pageSize not provided as query parameters.")
        }
    }

    fun executeQuery(queryMap: Array<QueryFunctionCall>, port: Int): QueryData {

        val channel = channelCache.get(Pair(port,"localhost"))
        val client = SimpleClient(channel)
        var query = Query()

        queryMap.forEach {
            when(it.name){
                "SELECT" -> query = query.select(it.parameters[0])
                "FROM" -> query = query.from(it.parameters[0])
            }
        }

        val result =  client.query(query)

        val tuples : MutableList<List<String>> = mutableListOf()
        result.forEach {tuple ->
            val list = mutableListOf<String>()
            repeat(tuple.size()){
                list.add(tuple[it].toString())
            }
            tuples.add(list)
        }

        client.close()
        return QueryData(result.columnNames, tuples)
    }


    /**
     * get or load the Query Response from the cache,
     * paginate it and return all pages as [List] of type [Page].
     */
    fun computePages(sessionID: String, queryRequest: Array<QueryFunctionCall>, port: Int, pageSize: Int) : List<Page> {

        return queryCache.get(QueryKey(sessionID,queryRequest,port)).paginate(pageSize)
    }

    fun getPage(sessionID: String, queryRequest: Array<QueryFunctionCall>, port: Int, pageSize: Int, page: Int) : Page{

        return pagedCache.get(QueryPagesKey(sessionID,queryRequest,pageSize, port))[page]

    }


}