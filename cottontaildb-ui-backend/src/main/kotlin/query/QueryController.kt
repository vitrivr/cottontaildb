package query

// Javalin Context
import io.javalin.http.Context

// vitrivr SimpleClient
import org.vitrivr.cottontail.client.SimpleClient
import org.vitrivr.cottontail.client.language.dql.Query

// Caches defined in Main.kt
import channelCache
import com.google.gson.Gson
import org.vitrivr.cottontail.client.language.basics.Direction
import org.vitrivr.cottontail.client.language.basics.Distances
import org.vitrivr.cottontail.client.language.basics.predicate.*
import org.vitrivr.cottontail.client.language.extensions.parseOperator
import org.vitrivr.cottontail.core.values.types.Types


import pageCache
import pagedCache
import queryCache
import java.time.LocalDate


private val gson = Gson()



object QueryController {

    data class QueryFunctionCall (val name: String, val parameters : List<String>)

    data class QueryPagesKey (val sessionID: String,
                              val queryRequest : Array<QueryFunctionCall>,
                              val pageSize: Int,
                              val port: Int,
                              val address: String) {
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
                             val port: Int,
                             val address: String) {
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
                        val port: Int,
                        val address: String) {
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

            val chunks = rows.chunked(size)
            val pages = mutableListOf<Page>()
            repeat(chunks.size){
                val page = Page(columnNames, chunks[it], size, it, rows.size)
                pages.add(page)
            }
            return pages
        }
    }
    @Suppress("unused")
    class Page(val columnNames: List<String>, val rows:List<List<String>>, val pageSize: Int, val page: Int, val numberOfRows: Int)

    fun query(context: Context)  {

        val sessionId = context.req().session.id
        val queryRequest = gson.fromJson(context.body(), Array<QueryFunctionCall>::class.java)
        val pageSize = context.queryParam("pageSize")
        val page = context.queryParam("page")
        val port = context.queryParam("port")
        val address = context.queryParam("address")

        require(page != null && pageSize != null && port != null && address != null)
        { context.status(400).result("Bad Request: page and/or pageSize not provided as query parameters.") }

        val key = QueryPageKey(sessionId, queryRequest, pageSize.toInt(), page.toInt(), port.toInt(), address)
        try {
            context.json(pageCache.get(key))
        } catch (e: Exception) {
            context.status(500)
        }

    }

    fun executeQuery(queryFunctionCalls: Array<QueryFunctionCall>, port: Int, address: String): QueryData {

        val channel = channelCache.get(Pair(port,address))
        val client = SimpleClient(channel)
        var query = Query()

        queryFunctionCalls.forEach {
            when(it.name){
                "SELECT" -> query = query.select(it.parameters[0])
                "FROM" -> query = query.from(it.parameters[0])
                "ORDER" -> query = query.order(it.parameters[0], Direction.valueOf(it.parameters[1]))
                "LIMIT" -> query = query.limit(it.parameters[0].toLong())
                "COUNT" -> query = query.count()
                "DISTANCE" -> {
                    val arrType : Class<*> = when(it.parameters[2]){
                        "FLOAT_VEC" -> Array<Float>::class.java
                        "DOUBLE_VEC" -> Array<Double>::class.java
                        "INT_VEC" -> Array<Int>::class.java
                        "BOOL_VEC" -> Array<Boolean>::class.java
                        "LONG_VEC" -> Array<Long>::class.java
                        else -> Array::class.java
                    }
                    query = query.distance(it.parameters[0], gson.fromJson(it.parameters[1], arrType), Distances.valueOf(it.parameters[3]), it.parameters[4])
                }
                "WHERE" -> {
                    println(it.parameters[2])
                    val value = it.parameters[2]
                    val typedValue : Any
                    println(it.parameters[3])
                    when(it.parameters[3]){
                            "SHORT" -> typedValue = value.toShort()
                            "LONG" -> typedValue = value.toLong()
                            "INTEGER" -> typedValue = value.toInt()
                            "DOUBLE" ->  typedValue = value.toDouble()
                            "BOOLEAN" -> typedValue = value.toBoolean()
                            "BYTE" -> typedValue = value.toByte()
                            "FLOAT" -> typedValue = value.toFloat()
                            "DATE" -> typedValue = LocalDate.parse(value)
                            "FLOAT_VEC" -> typedValue = gson.fromJson(value, Array<Float>::class.java)
                            "LONG_VEC" -> typedValue = gson.fromJson(value, Array<Long>::class.java)
                            "INT_VEC" -> typedValue = gson.fromJson(value, Array<Int>::class.java)
                            "BOOL_VEC" -> typedValue = gson.fromJson(value, Array<Boolean>::class.java)
                            "COMPLEX_32" -> typedValue = gson.fromJson(value, Types.Complex32Vector::class.java)
                            "COMPLEX_64" -> typedValue = gson.fromJson(value, Types.Complex64Vector::class.java)
                            "BYTESTRING" -> typedValue = gson.fromJson(value, Types.ByteString::class.java)
                            else -> typedValue = value
                    }
                    println(it.parameters[1].parseOperator())
                    println(Expression(it.parameters[0], it.parameters[1], typedValue).operator)
                    println(it.parameters[1].javaClass)
                    query = query.where(Expression(it.parameters[0], it.parameters[1], typedValue))
                }
            }
        }

        val result =  client.query(query)
        val tuples : MutableList<List<String>> = mutableListOf()

        result.forEach {tuple ->
            val list = mutableListOf<String>()
            repeat(tuple.size()){
                when(val value = tuple[it]){
                    is FloatArray -> list.add(value.map{ num -> num.toString() }.toString())
                    else -> list.add(value.toString())
                }
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
    fun computePages(sessionID: String, queryRequest: Array<QueryFunctionCall>, port: Int, address: String, pageSize: Int) : List<Page> {
        return queryCache.get(QueryKey(sessionID,queryRequest,port, address)).paginate(pageSize)
    }

    fun getPage(sessionID: String, queryRequest: Array<QueryFunctionCall>, port: Int, address: String, pageSize: Int, page: Int) : Page{
        return pagedCache.get(QueryPagesKey(sessionID,queryRequest,pageSize, port, address))[page]
    }


}