import com.sksamuel.elastic4s.http.{ElasticClient, ElasticProperties}
import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.mappings.Child
import com.sksamuel.elastic4s.searches.queries.InnerHit

object JoinTypeApp extends App {

  // client 生成
  val client = ElasticClient(ElasticProperties("http://localhost:9200"))

  val indexName = "jointypeindex"
  // delete index
  client.execute(deleteIndex(indexName))

  // indexのマッピング定義
  // join datatypeの定義
  // joinフィールド名を"myjoinfield"、 親を"parent"、子を"child"
  client.execute {
    createIndex(indexName).mappings {
      mapping("_doc").fields(
        keywordField("name"),
        textField("kind"),
        joinField("myjoinfield").relation("parent", "child")
      )
    }
  }.await

  // データの投入
  client.execute {
    bulk(
      // 親1を親としてid=1で登録
      indexInto(indexName / "_doc").fields(Map("name" -> "親1", "kind" -> "p", "myjoinfield" -> "parent")).id("1").routing("1"),
      // 親2を親としてid=2で登録
      indexInto(indexName / "_doc").fields(Map("name" -> "親2", "kind" -> "p", "myjoinfield" -> "parent")).id("2").routing("1"),
      // 子供1を子としてid=3で登録 親はid=1の親1
      indexInto(indexName / "_doc").fields(Map("name" -> "子供1", "kind" -> "c", "myjoinfield" -> Child("child", "1"), "age" -> 10)).id("3").routing("1"),
      // 子供2を子としてid=4で登録 親はid=1の親1
      indexInto(indexName / "_doc").fields(Map("name" -> "子供2", "kind" -> "c", "myjoinfield" -> Child("child", "1"), "age" -> 3)).id("4").routing("1"),
      // 子供3を子としてid=5で登録 親はid=2の親2
      indexInto(indexName / "_doc").fields(Map("name" -> "子供3", "kind" -> "c", "myjoinfield" -> Child("child", "2"), "age" -> 20)).id("5").routing("1"),
      // 親3を親としてid=6で登録
      indexInto(indexName / "_doc").fields(Map("name" -> "親3", "kind" -> "p", "myjoinfield" -> "parent")).id("6").routing("1")
    ).refreshImmediately
  }.await

  // 全てのデータを取得
  val response1 = client.execute {
    search(indexName).matchAllQuery()
  }.await.result
  println("---全てのデータ---")
  println(s"件数：${response1.totalHits}")
  response1.hits.hits.foreach(h => println(h.sourceAsString))

  // kind=pのデータ(全ての親)を取得
  val response2 = client.execute {
    search(indexName).matchQuery("kind", "p")
  }.await.result
  println("---kind=pのデータ---")
  println(s"件数：${response2.totalHits}")
  response2.hits.hits.foreach(h => println(h.sourceAsString))

  // kind=cのデータ(全ての子)を取得
  val response3 = client.execute {
    search(indexName).matchQuery("kind", "c")
  }.await.result
  println("---kind=cのデータ---")
  println(s"件数：${response3.totalHits}")
  response3.hits.hits.foreach(h => println(h.sourceAsString))

  // 子を持っている親を取得
  val response4 = client.execute {
    search(indexName).query {
      hasChildQuery("child", matchAllQuery()).innerHit(InnerHit("myinner"))
    }
  }.await.result
  println("---子を持っている親---")
  println(s"件数：${response4.totalHits}")
  response4.hits.hits.foreach(h => println(h.sourceAsString))

  // 20歳以上の子を持っている親を取得
  val response5 = client.execute {
    search(indexName).query {
      hasChildQuery("child", rangeQuery("age").gte(20)).innerHit(InnerHit("myinner"))
    }
  }.await.result
  println("---20歳以上の子を持っている親---")
  println(s"件数：${response5.totalHits}")
  response5.hits.hits.foreach(h => println(h.sourceAsString))
  response5.hits.hits.foreach(h => println(h.innerHits.head._2.hits.head.source))

  client.close()

}
