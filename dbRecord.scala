import org.mongodb.scala._
import com.github.tototoshi.csv._
import collection.mutable.HashMap
import collection.mutable.ArraySeq
import org.mongodb.scala.bson.collection.mutable.Document
import org.bson.BsonValue

abstract class DBRecord() {
  val insertedRecords: HashMap[String, Document]=HashMap()
  val insertedRecordsIds: HashMap[String, BsonValue]=HashMap()
  var header: Array[String] = _

  def getCollectionName(): String
  def addRowAsDocumentToIserted(row: Seq[String]): Unit

  def load(csvReader: CSVReader, database: MongoDatabase, firstRowAsHeader: Boolean,
    keepInsertedRecordsIds: Boolean): Unit = {
    val collectionName: String = getCollectionName
    val collection: MongoCollection[Document]
      = database.getCollection(collectionName)
    collection.drop().subscribe(new Observer[Completed] {
      override def onNext(result: Completed): Unit = {}
      override def onError(e: Throwable): Unit = {
        println("Drop of " + collectionName + " failed:")
        println(e)
      }
      override def onComplete(): Unit = println(collectionName+" collection cleaned!")
    })
    val csvIt: Iterator[Seq[String]] = csvReader.iterator
    if(firstRowAsHeader && csvIt.hasNext) {
      val row = csvIt.next()
      var currentIndex = 0
      header = new Array[String](row.length)
      for (columnName <- row) {
        header(currentIndex) = columnName.replaceAll("\\s\\s+", " ").trim()
        currentIndex+=1
      }
    }
    while (csvIt.hasNext) addRowAsDocumentToIserted(csvIt.next())
    if(!insertedRecords.isEmpty) {
      collection.insertMany(insertedRecords.values.toList).subscribe(new Observer[Completed] {
        override def onNext(result: Completed): Unit = {}
        override def onError(e: Throwable): Unit = {
          println("Insert in " + collectionName + " failed:")
          println(e)
        }
        override def onComplete(): Unit = {
          println(collectionName+" saved!")
          if(keepInsertedRecordsIds){
            val insertedKeysIterator = insertedRecords.keys.iterator
            while(insertedKeysIterator.hasNext) {
              val key = insertedKeysIterator.next()
              insertedRecordsIds(key) = insertedRecords(key)("_id")
            }
            insertedRecords.clear
          }
        }
      })
    }
  }

  def getInsertedRecordId(key: String): BsonValue = {
    insertedRecordsIds(key)
  }
}
