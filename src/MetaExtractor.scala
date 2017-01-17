package mmdb

/**
  * Created by joe on 14/01/2017.
  */


import scala.util.{Try, Success, Failure}

case class MetaData(recordSize: Int, nodeCount: Int) {
  def getSearchTreeSectionSize: Int = {
    ( ( recordSize * 2 ) / 8 ) * nodeCount
  }
}

class MetaExtractor {

  def getMetaData(bytes: Array[Byte]): Try[MetaData] = {
    for {
      rs <- getRecordSize(bytes)
      nc <- getNodeCount(bytes)
    } yield MetaData(rs, nc)
  }


  def getRecordSize(bytes: Array[Byte]) : Try[Int] = {
    val rsOffset = findLastStr(bytes, "record_size") + "record_size".length
    rsOffset match {
      case y if y < 0 => Failure(new Exception("record size detected was a negative value"))
      case x => Success(bytes(rsOffset+1) & 0xFF)
    }
  }

  def getNodeCount(bytes: Array[Byte]) : Try[Int] = {
    val controlByteOffset = findLastStr(bytes, "node_count") + "node_count".length
    val cinfo = readControlByte(controlByteOffset, bytes)
    cinfo match {
      case ControlInfo(UINT16(), payload) => {
        logger.debug("getting uint16")
        Success(getIntFromBytes(controlByteOffset+1, payload, bytes))
      }
      case ControlInfo(UINT32(), payload) => {
        logger.debug("parsing uint32")
        Success(getIntFromBytes(controlByteOffset+1, payload, bytes))
      }
    }
  }

  def getIntFromBytes(ptr: Int, bytesNum: Int, bytes: Array[Byte]): Int = {
    // get 4 bytes and convert to unsigned int
    val b1 = bytes(ptr) & 0xFF
    val b2 = bytes(ptr+1) & 0xFF
    val b3 = bytes(ptr+2) & 0xFF
    val b4 = bytes(ptr+3) & 0xFF
    val res = bytesNum match {
      case 1 => b1
      case 2 => (b1 << 8) + b2
      case 3 => (b1 << 16) + (b2 << 8) + b3
      case 4 => (b1 << 24) + (b2 << 16) + (b3 << 8) + b4
    }
    //logger.debug("getintfrombytes res:"+res)
    res
  }

  def readControlByte(ptr: Int, bytes: Array[Byte]): ControlInfo = {
    val cbyte = bytes(ptr) & 0xFF
    val ctype = cbyte >> 5
    val payload = bytes(ptr) & 0x1f
    logger.debug("field type (first 3 bits) to int:" + ctype + " size in bytes:" + payload)
    val fieldtype = ctype match {
      case 1 => PTR()
      case 2 => STR()
      case 3 => DBL()
      case 4 => BYT()
      case 5 => UINT16()
      case 6 => UINT32()
      case 7 => MAP()
    }
    ControlInfo(fieldtype, payload)
  }

  def findLastStr(bytes: Array[Byte], str: String): Int= {
    var count = bytes.length -1

    while (!testStrLocation(bytes, str, count)) {
      count -= 1
      if (count == 0) {
        return -1
      }
    }
    count
  }

  def testStrLocation(bytes: Array[Byte], str: String, index: Int): Boolean = {

    for (i <- 0 until str.length) {
      if (index + i >= bytes.length) {
        return false
      }
      if (bytes(index+i) != str(i)) {
        return false
      }
    }
    true
  }
}
