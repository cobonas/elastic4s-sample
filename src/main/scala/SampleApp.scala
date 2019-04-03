import com.sksamuel.elastic4s.RefreshPolicy
import com.sksamuel.elastic4s.http.{ElasticClient, ElasticProperties}
import com.sksamuel.elastic4s.http.Response
import com.sksamuel.elastic4s.http.search.SearchResponse
import com.sksamuel.elastic4s.http.ElasticDsl._

object SampleApp extends App {

  val client = ElasticClient(ElasticProperties("http://localhost:9200"))

  client.execute { createIndex("myindex") }

  client.execute {
    bulk(
      indexInto("myindex" / "mytype").fields("country" -> "Mongolia", "capital" -> "Ulaanbaatar"),
      indexInto("myindex" / "mytype").fields("country" -> "Namibia", "capital" -> "Windhoek")
    ).refresh(RefreshPolicy.WaitFor)
  }.await

  val response: Response[SearchResponse] = client.execute {
    search("myindex").matchQuery("capital", "ulaanbaatar")
  }.await

  println(response.result.hits.hits.head.sourceAsString)

  client.close()

}
