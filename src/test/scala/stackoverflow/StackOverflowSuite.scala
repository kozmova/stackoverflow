package stackoverflow

import org.scalatest.{FunSuite, BeforeAndAfterAll}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import java.io.File

@RunWith(classOf[JUnitRunner])
class StackOverflowSuite extends FunSuite with BeforeAndAfterAll {

  @transient lazy val conf: SparkConf = new SparkConf().setMaster("local").setAppName("StackOverflowSuite")
  @transient lazy val sc: SparkContext = new SparkContext(conf)

  lazy val testObject = new StackOverflow {
    override val langs =
      List(
        "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
        "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")
    override def langSpread = 50000
    override def kmeansKernels = 45
    override def kmeansEta: Double = 20.0D
    override def kmeansMaxIterations = 120
  }

  val question1 = Posting(1, 100, Some(101), None, 1, Some("Scala")) //question id 100, accepted answer id 101
  val answer1 =  Posting(2, 101, None, Some(100), 20, Some("Scala")) //answer id 101 to question 100
  val answer2 =  Posting(2, 102, None, Some(100), 3, Some("Scala")) //answer id 102 to question 100
  val question2 = Posting(1, 103, None, None, 4, Some("Scala")) //question id 103, no answer

  test("testObject can be instantiated") {
    val instantiatable = try {
      testObject
      true
    } catch {
      case _: Throwable => false
    }
    assert(instantiatable, "Can't instantiate a StackOverflow object")
  }

  test("groupedPosting works correctly") {
    val postings = sc.parallelize(List(
      question1,
      answer1,
      answer2,
      question2
    ))

    val grouped = StackOverflow.groupedPostings(postings).collect()
    assert(grouped.length == 1)
    val head = grouped.head
    assert(head._1 == 100)

    val joinedList = head._2.toList
    assert(joinedList.size == 2)
    assert(joinedList.map(_._2).toSet == Set(answer1, answer2))
  }

  test("scored posting works correctly") {
    val rdd = sc.parallelize(List(
      (question1.id, Iterable((question1, answer1), (question1, answer2)))
    ))

    val scored = StackOverflow.scoredPostings(rdd).collect()
    assert(scored.length == 1)
    assert(scored.head._2 == answer1.score)
  }

  test("vectorPosting works correctly") {
    val rdd = sc.parallelize(List(
      (question1, 100)
    ))

    val vectorized = StackOverflow.vectorPostings(rdd).collect()

    assert(vectorized sameElements Array((500000, 100)))
  }

}
