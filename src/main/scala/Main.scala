import akka.actor.ActorSystem
import akka.stream.{Supervision, ActorAttributes, RestartSettings}
import akka.stream.scaladsl._
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.TimeoutException
import spray.json._
import DefaultJsonProtocol._
import scala.collection.immutable.ListMap
import scala.util.control.NonFatal
import scalafx.application.JFXApp
import scalafx.application.JFXApp.PrimaryStage
import scalafx.scene.Scene
import javafx.application.Platform
import scalafx.beans.property.{IntegerProperty, StringProperty, ObjectProperty}
import scalafx.collections.ObservableBuffer
import scalafx.scene.control.TableColumn._
import scalafx.scene.control.{TableCell, TableColumn, TableView}
import javafx.stage.WindowEvent
import javafx.event.EventHandler

case class Hashtag(tag: String)
case class Message(id: String, text: String)
case class Tweet(data: Message) {
  // filters the hashstags and creates a list of hashtags removing # and #_+
  def hashtags = data.text.split(" ")
                          .collect {
                            case t if t.startsWith("#") => Hashtag(t.replaceAll("[^#\\w]", ""))
                          }
                          .toList
                          .map(_.tag)
                          .mkString(" ")
                          .replace("#", " #")
                          .trim.replaceAll(" +", " ")
                          .toLowerCase()
                            
}
// class for the observable buffer to build the list.
class Tag(tag_ : String, 
          frequency_ : String) {
  val tag = new StringProperty(this, "Tags", tag_)
  val frequency = new StringProperty(this, "Frequency", frequency_)
}

object Main extends JFXApp {
  implicit val system = ActorSystem("twitter")
  val token = "Bearer AAAAAAAAAAAAAAAAAAAAAGrdKQEAAAAAtH4h%2BhR1AedlmYlxGDqDtfHTskI%3DXbX9JR1fvHGeoACsxYReXGXINeIWKCukOWqL5QWJa7wCKlinSJ"
  implicit val messageFormat = jsonFormat2(Message)
  implicit val tweetFormat = jsonFormat1(Tweet)

  val characters = ObservableBuffer[Tag]()

  stage = new PrimaryStage {
    title = "Twitter Popular Tags"
    scene = new Scene {
      content = new TableView[Tag](characters) {
        columns ++= List(
          new TableColumn[Tag, String] {
            text = "Tags"
            cellValueFactory = { _.value.tag }
            prefWidth = 300
          },
          new TableColumn[Tag, String]() {
            text = "Frequency"
            cellValueFactory = { _.value.frequency }
            prefWidth = 100
          }
        )
      }
    }
  }

  //supervision strategy to restart source once it consumes the data
  val decider: Supervision.Decider = {
    case _: TimeoutException => Supervision.Restart
    case NonFatal(error) => Supervision.Resume
  }

  //restart source settings for backoff if it fails
  val settings = RestartSettings(
    minBackoff = .3.seconds,
    maxBackoff = 30.seconds,
    randomFactor = 0.2
  )

  //source to the twitter tweet stream. if it failes it will attempt to reconnect based on the parameters set in the restart settings
  val source = RestartSource.withBackoff(settings){() =>
    Source.futureSource {
      // http request to twitter api with approriate headers
      Http().singleRequest(HttpRequest(uri = "https://api.twitter.com/2/tweets/sample/stream", 
      headers = List(HttpHeader.parse("Authorization", token) match {
        case HttpHeader.ParsingResult.Ok(header, _) => header
        case HttpHeader.ParsingResult.Error(error)  => throw new Exception(s"Unable to convert to HttpHeader: ${error.summary}")
      }))).map(response => {
            response.status match {
              // return Source if server sends back a json otherwise fail and restart and source
              case StatusCodes.OK => response.entity.dataBytes
              case code => Source.failed(new RuntimeException(s"$code"))
            }
          })
    }
  }
  // series of transformations to get tag count and then output the top 10 tags
  source.map(_.utf8String)
        .map(_.parseJson.convertTo[Tweet])
        .map(_.hashtags)
        .filter(_.contains("#"))
        .scan(Map.empty[String, Int]){
          (sum, tags) => {
            val tweetCount = {
              tags.split(" ")
                  .foldLeft(Map.empty[String, Int]) {
                    (count, tag) => count + (tag -> (count.getOrElse(tag, 0)+1))
                  }
            }
            
            ListMap(((sum.keySet ++ tweetCount.keySet).map {tag => (tag, sum.getOrElse(tag, 0) + tweetCount.getOrElse(tag, 0))}.toMap).filter(x => x._1 matches("[#][a-zA-Z0-9]+")).toSeq.sortWith(_._2 > _._2).take(10): _*)
          }
        }
        .withAttributes(ActorAttributes.supervisionStrategy(decider))
        .runWith(Sink.foreach( a =>
          Platform.runLater{new Runnable { def run() = 
            a.map(a => characters.append(new Tag(a._1, a._2.toString())))
            characters.clear()
          }}
        ))
  stage.setOnHiding(new EventHandler[WindowEvent]() {
    override def handle(event: WindowEvent) {
      system.terminate()
    }
  })
}