package query

import cache
import channelCache
import io.javalin.http.Context
import org.vitrivr.cottontail.client.SimpleClient
import org.vitrivr.cottontail.client.language.dql.Query
import pagedCache
import java.io.Serializable

object QueryController {

    class QueryPageKey (val sessionID: String,
                       val queryMessage : QueryMessage,
                       val pageSize: Int,
                       val page: Int,
                       val port: Int) : Serializable {
    }

    class QueryKey(val sessionID: String, val queryMessage: QueryMessage, val port: Int)


    class QueryMessage (val queryMap:  Map<String, List<String>>){
    }

    class QueryData(private val columnNames: List<String>, private val rows: MutableList<List<String>>){

        fun paginate(size: Int) : List<Page>{
            println("PAGINATING")
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

        println("QUERY CALL")
        val session = context.req().session
        println(session)

        val pageSize = 10
        val page = 0

        val port = context.pathParam("port").toInt()
        val queryMap = context.queryParamMap().toMutableMap()
        // Remove pagination parameters -> queryMap.remove()
        val key = QueryPageKey(session.id, QueryMessage((queryMap)),pageSize,page,port)

        context.json(pagedCache.get(key))


    }

    fun executeQuery(queryMessage: QueryMessage, port: Int): QueryData {

        println("EXECUTE")

        val queryMap = queryMessage.queryMap

        val channel = channelCache.get(Pair(port,"localhost"))
        val client = SimpleClient(channel)

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
        return QueryData(result.columnNames, tuples)

    }

    fun computePages(sessionID: String, queryMessage: QueryMessage, port: Int, pageSize: Int) : List<Page> {
        println("COMPUTE PAGES")
        return cache.get(QueryKey(sessionID,queryMessage,port)).paginate(pageSize)
    }


}