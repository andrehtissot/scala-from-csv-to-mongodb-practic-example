import org.mongodb.scala.bson.collection.mutable.Document
import org.mongodb.scala.bson._
import java.text.SimpleDateFormat
import scala.collection.mutable.Stack

object Games extends DBRecord {
  override def getCollectionName(): String = {
    "games"
  }
  override def addRowAsDocumentToIserted(row: Seq[String]): Unit = {
    val title: String = row(header.indexOf("Title")).replaceAll("\\s\\s+", " ").trim()
    val steamId: String = row(header.indexOf("ID")).trim()
    if(!title.isEmpty && !steamId.isEmpty){
      insertedRecords(steamId) = Document(
        "title" -> title,
        "type" -> row(header.indexOf("Type")).trim(),
        "controllerSupported" -> BsonBoolean.apply(
          row(header.indexOf("Controller Support")).trim() == "1"),
        "os" -> org.mongodb.scala.bson.collection.mutable.Document(
          "windows" -> BsonBoolean.apply(row(header.indexOf("Windows")).trim() == "1"),
          "mac" -> BsonBoolean.apply(row(header.indexOf("Mac")).trim() == "1"),
          "linux" -> BsonBoolean.apply(row(header.indexOf("Linux")).trim() == "1")
        ),
        "multiplayer" -> BsonBoolean.apply(row(header.indexOf("Multiplayer")).trim() == "1"),
        "hoursPlayed" -> BsonDouble.apply(row(header.indexOf("hrsHours Played")).trim().toFloat),
        "fullPrice" -> BsonDouble.apply(row(header.indexOf("Default Price (USD)")).trim().toFloat)
      )
      try {
        insertedRecords(steamId)("metascore")
          = BsonInt32.apply(row(header.indexOf("Metascore")).trim().toInt)
      } catch { case e: Exception => {} }
      val format: SimpleDateFormat = new SimpleDateFormat("MMM dd, yyyy");
      try {
        insertedRecords(steamId)("releaseDate")
          = BsonDateTime.apply(format.parse(row(header.indexOf("Release Date")).trim()))
      } catch { case e: Exception => {} }
      val developersNames: String = row(header.indexOf("Developers"))
        .replaceAll("\\s\\s+", " ").trim()
      if(!developersNames.isEmpty)
        insertedRecords(steamId)("developer") = Developers.getInsertedRecordId(developersNames)
      val publishersNames: String = row(header.indexOf("Publishers"))
        .replaceAll("\\s\\s+", " ").trim()
      if(!publishersNames.isEmpty)
        insertedRecords(steamId)("publisher") = Publishers.getInsertedRecordId(publishersNames)
      val genreNames: String = row(header.indexOf("Genres")).replaceAll("\\s\\s+", " ").trim()
      if(!genreNames.isEmpty){
        val genreNamesArray = genreNames.split(",")
        val genreIds: Stack[BsonValue] = new Stack[BsonValue]
        for (name <- genreNamesArray)
          genreIds.push(Genres.getInsertedRecordId(name))
        insertedRecords(steamId)("genres") = BsonArray.apply(genreIds)
      }
    }
  }
}