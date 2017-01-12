/**
  * Created by joe on 09/01/2017.
  */
import java.nio.file.{Files, Paths}

case class IP(ip1:Int, ip2:Int, ip3:Int, ip4:Int) {

  /**
    * shift the 4 ip bytes into place and return as one Int
    * @return - the integer representation of the given ip numbers
    */
  def getInt: Int = {
    ((ip1 << 24) + (ip2 << 16) + (ip3 << 8) + ip4)
  }

  def getString(length:Int): String = {
    val bs = getInt.toBinaryString
    val zerosNeeded = length - bs.length
    val zeros = List.fill(zerosNeeded)("0").mkString
    zeros + bs
  }

  def getv6Mapped: String = {
    val zeros = List.fill(80)("0").mkString
    val ones = List.fill(16)("0").mkString
    zeros + ones + getString(32)
  }


  def display = {
    val b = getInt
    println(f"IP display, hex value: $b%x")
    println("bit length: " + getString(32).length)
    println("binary: " + getString(32))
    println("ipv6: " + getv6Mapped)
  }

}

/*case class Node(index: Int, bytes: Array[Byte]) {
  def getLeft = {

  }
}*/

abstract class Direction
case class Right() extends Direction
case class Left() extends Direction

abstract class FieldType
case class PTR() extends FieldType
case class STR() extends FieldType
case class DBL() extends FieldType
case class BYT() extends FieldType
case class UINT16() extends FieldType
case class UINT32() extends FieldType
case class MAP() extends FieldType

case class ControlInfo(ctype: FieldType, payload: Int)

object MMDBReader extends App {

  // read db file

  val byteArray = Files.readAllBytes(Paths.get("GeoLite2-Country.mmdb"))
  //(0 to 100) foreach { x => println(byteArray(x)) }

  //println(byteArray(0))
  println("maxmind index:"+findLastStr(byteArray, "MaxMind.com"))
  findMetaSection(byteArray)
  val recordSize = getRecordSize(byteArray)
  println("record size:"+getRecordSize(byteArray))
  val nodeCount = getNodeCount(byteArray)
  println(f"node count: 0x$nodeCount%x - "+ nodeCount)
  val searchTreeSize = getSearchTreeSectionSize(recordSize, nodeCount)
  println(f"search tree section size: 0x$searchTreeSize%x")

  // create ip address
  //val ip = IP(62,254,119,11)
  val ip = IP(178,20,86,35)

  ip.display

  val dataPtr = getDataPtr(0, ip, 0, getNodeCount(byteArray), 128)
  dataPtr match {
    case Some(ptr) => println(f"data location: 0x$ptr%x")
    case None => println("data for ip not found")
  }


  def getDataPtr(nodeAddress: Int, ip: IP, ipBit: Int, nodeCount: Int, ipLength: Int): Option[Int] = {

    def getNodeAddress(nodeNumber: Int): Int = {
      //println("getting node address:"+nodeNumber * ((recordSize * 2) / 8))
      (nodeNumber * 6)
    }

    if (ipBit == ipLength) return None

    val result = getNodeVal(nodeAddress, byteArray, getDirection(ipBit, ip, ipLength))
    println(f"getting node val - address: $nodeAddress bit: $ipBit val " + getDirection(ipBit, ip, ipLength) + " got val: " + (result))
    result match {
      case x if x == nodeCount => println("address data does not exist for this ip"); return None
      case dataPtr if dataPtr > nodeCount => {
        println(s"address pointer found: $dataPtr")
        return Some(getDataOffset(result, nodeCount))
      }
      case nodeNum if nodeNum < nodeCount => {

        getDataPtr(getNodeAddress(nodeNum), ip, ipBit + 1, nodeCount, ipLength)
      }
    }
  }

  def getDataOffset(dataPtr: Int, nodeCount: Int): Int = {
    (( dataPtr - nodeCount ) - 16)
  }

  def readControlByte(ptr: Int, bytes: Array[Byte]): ControlInfo = {
    val cbyte = bytes(ptr) & 0xFF
    val ctype = cbyte >> 5
    val payload = bytes(ptr) & 0x1f
    println("field type (first 3 bits) to int:" + ctype + " size in bytes:" + payload)
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

  def padTo(length: Int, str: String) : String = {
    val zeros = length - str.length
    val zerosStr = List.fill(zeros)("0")
    zerosStr.mkString + str
  }

  def getDirection(index: Int, ip: IP, ipLength: Int): Direction = {
    ip.getv6Mapped(index) match {
      case '0' => Left()
      case '1' => Right()
    }
  }

  def getNodeVal(index: Int, bytes: Array[Byte], dir: Direction): Int = {

    dir match {
      case Left() => {
        val b1 = bytes(index) & 0xFF
        val b2 = bytes(index + 1) & 0xFF
        val b3 = bytes(index + 2) & 0xFF
        ((b1 << 16) + (b2 << 8) + b3)
      }
      case Right() => {
        val b1 = bytes(index + 3) & 0xFF
        val b2 = bytes(index + 4) & 0xFF
        val b3 = bytes(index + 5) & 0xFF
        ((b1 << 16) + (b2 << 8) + b3)
      }
    }

  }

  def getSearchTreeSectionSize(recordSize:Int, nodeCount:Int): Int = {
    ( ( recordSize * 2 ) / 8 ) * nodeCount
  }

  def findMetaSection(bytes: Array[Byte]) : Int = {
    findLastStr(byteArray, "MaxMind")
  }

  def getRecordSize(bytes: Array[Byte]) : Int = {
    val rsOffset = findLastStr(bytes, "record_size") + "record_size".length
    //Integer.parseInt(f"$rs%x$rs2%x",16)
    bytes(rsOffset+1)
  }

  def getNodeCount(bytes: Array[Byte]) : Int = {
    val controlByteOffset = findLastStr(bytes, "node_count") + "node_count".length
    val cinfo = readControlByte(controlByteOffset, bytes)
    cinfo match {
      case ControlInfo(UINT16(), y) => {
        println("getting uint16")
        getIntFromBytes(controlByteOffset+1, cinfo.payload, bytes)
      }
      case ControlInfo(UINT32(), y) => {
        println("parsing uint32")
        getIntFromBytes(controlByteOffset+1, cinfo.payload, bytes)
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
        case 1 => b1 & 0xFF
        case 2 => (b1 << 8) + b2
        case 3 => (b1 << 16) + (b2 << 8) + b3
        case 4 => (b1 << 24) + (b2 << 16) + (b3 << 8) + b4
      }
    //println("getintfrombytes res:"+res)
    res
  }

  def getDataSectionStart = {}

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
