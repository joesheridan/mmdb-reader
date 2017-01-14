package mmdb

/**
  * Created by joe on 14/01/2017.
  */
class BinarySearch(byteArray: Array[Byte]) {

  def getDataPtr(nodeAddress: Int, ip: IP, ipBit: Int, nodeCount: Int, ipLength: Int): Option[Int] = {

    def getNodeAddress(nodeNumber: Int): Int = {
      //println("getting node address:"+nodeNumber * ((recordSize * 2) / 8))
      (nodeNumber * 6)
    }

    if (ipBit == ipLength) return None

    val result = getNodeVal(nodeAddress, byteArray, getDirection(ipBit, ip, ipLength))
    println(f"getting node val - address: $nodeAddress bit: $ipBit val " +
              getDirection(ipBit, ip, ipLength) + " got val: " + (result))
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

  def getDataOffset(dataPtr: Int, nodeCount: Int): Int = {
    (( dataPtr - nodeCount ) - 16)
  }

}
