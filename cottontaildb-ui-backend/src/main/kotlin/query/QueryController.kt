package query

import ClientConfig
import cache
import io.javalin.http.Context
import org.vitrivr.cottontail.client.language.dql.Query
import pagedCache

object QueryController {

    class QueryPageKey(val sessionID: String,
                       val queryMessage : QueryMessage,
                       val pageSize: Int,
                       val page: Int,
                       val port: Int){

    }

    class QueryKey(val sessionID: String, val queryMessage: QueryMessage, val port: Int)


    class QueryMessage (val queryMap:  Map<String, List<String>>){
    }

    class QueryData(private val columnNames: List<String>, private val rows: MutableList<List<String>>, size: Int){

        val pages = paginate(size)

        fun paginate(size: Int) : List<Page>{
            val chunks = rows.chunked(size)
            val pages = mutableListOf<Page>()
            repeat(chunks.size){
                val page = Page(columnNames, chunks[it])
                pages.add(page)
            }
            return pages
        }
    }

    class Page(val columnNames: List<String>, val rows:List<List<String>>)


    fun query(context: Context)  {

        val sessionId = context.req().getSession(true).id

        val pageSize = 10
        val page = 0

        val port = context.pathParam("port").toInt()
        val queryMap = context.queryParamMap().toMutableMap()
        // Remove pagination parameters -> queryMap.remove()
        val key = QueryPageKey(sessionId, QueryMessage((queryMap)),pageSize,page,port)

        println("key: ")
        print(key)

        context.json(pagedCache.get(key))


    }

    fun executeQuery(queryMessage: QueryMessage, port: Int): QueryData {

        println("EXECUTE")

        val queryMap = queryMessage.queryMap

        val client = ClientConfig(port).client
        var query = Query(queryMap["FROM"]?.elementAt(0))

        val select = queryMap["SELECT"]
        if (select != null) {
            for (column in select) {
                query = query.select(column)
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
        return QueryData(result.columnNames, tuples, 10)

    }

    fun computePages(sessionID: String, queryMessage: QueryMessage, port: Int) : List<Page> {
        return cache.get(QueryKey(sessionID,queryMessage,port)).pages
    }


}