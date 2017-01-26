import org.mongodb.scala._
import org.bson._
import java.io.File
import com.github.tototoshi.csv._
import scala.collection.Iterator

object LoadGameDB {
  def main(args: Array[String]): Unit = {
    val ticker: Ticker = new Ticker(true)
    val csvFile: File = new File("./source/games.csv")
    val mongoClient: MongoClient = MongoClient("mongodb://127.0.0.1")
    val database: MongoDatabase = mongoClient.getDatabase("games")
    Developers.load(CSVReader.open(csvFile), database, true, true)
    Publishers.load(CSVReader.open(csvFile), database, true, true)
    Genres.load(CSVReader.open(csvFile), database, true, true)
    Games.load(CSVReader.open(csvFile), database, true, false)
    mongoClient.close
    ticker.echoMemory("Memory used: ")
    ticker.echoTick("Time passed: ")
    println("Database loaded!")
  }
}

