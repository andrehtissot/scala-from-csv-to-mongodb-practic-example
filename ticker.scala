class Ticker(start: Boolean) {
  def this() = this(false)

  var lastTick: Long = 0
  var lastCurrentMemory: Long = 0

  def startTick(): Unit = {
    lastTick = System.nanoTime
  }

  def startMemory(){
    val runtime = Runtime.getRuntime
    lastCurrentMemory = (runtime.totalMemory - runtime.freeMemory);
  }

  def echoTick(messageBefore: String,messageAfter: String,decimals: Int,decPoint: String): Unit = {
    print(messageBefore + diffTickVerbose(decimals, decPoint) + messageAfter)
    startTick()
  }

  def echoTick(messageBefore: String, messageAfter: String, decimals: Int): Unit = {
    echoTick(messageBefore, messageAfter, decimals, ".")
  }

  def echoTick(messageBefore: String, messageAfter: String): Unit = {
    echoTick(messageBefore, messageAfter, 2, ".")
  }

  def echoTick(messageBefore: String): Unit = {
    echoTick(messageBefore, "\n", 2, ".")
  }

  def echoMemory(messageBefore: String,messageAfter: String,decimals: Int,decPoint: String): Unit ={
    print(messageBefore + diffMemoryVerbose(decimals, decPoint) + messageAfter)
    startMemory()
  }

  def echoMemory(messageBefore: String, messageAfter: String, decimals: Int): Unit = {
    echoMemory(messageBefore, messageAfter, decimals, ".")
  }

  def echoMemory(messageBefore: String, messageAfter: String): Unit = {
    echoMemory(messageBefore, messageAfter, 2, ".")
  }

  def echoMemory(messageBefore: String): Unit = {
    echoMemory(messageBefore, "\n", 2, ".")
  }

  def diffTick(): Long = {
    System.nanoTime - lastTick
  }

  def diffTickVerbose(decimals: Int, decPoint: String): String = {
    ("%."+decimals+"f" format (diffTick().toFloat/1000000000)).replace(".", decPoint)+" sec."
  }

  def diffTickVerbose(decimals: Int): String = {
    ("%."+decimals+"f" format (diffTick().toFloat/1000000000))+" sec."
  }

  def diffTickVerbose(): String = {
    ("%.2f" format (diffTick().toFloat/1000000000))+" sec."
  }

  def diffMemory(): Long = {
    val runtime = Runtime.getRuntime
    (runtime.totalMemory - runtime.freeMemory) - lastCurrentMemory
  }

  def diffMemoryVerbose(decimals: Int, decPoint: String): String = {
    ("%."+decimals+"f" format (diffMemory().toFloat/1048576)).replace(".", decPoint)+" MB."
  }

  def diffMemoryVerbose(decimals: Int): String = {
    ("%."+decimals+"f" format (diffMemory().toFloat/1048576))+" MB."
  }

  def diffMemoryVerbose(): String = {
    ("%.2f" format (diffMemory().toFloat/1048576))+" MB."
  }

  if(start){
    startMemory
    startTick
  }
}
