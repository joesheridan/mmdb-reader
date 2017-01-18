package mmdb

/**
  * Created by joe on 14/01/2017.
  */

import scala.util.{Try, Success, Failure}

abstract class Direction
case object Right extends Direction
case object Left extends Direction

class BinarySearch(byteArray: Array[Byte]) {

  /**
    * Navigate through the binary search tree using the binary representation of the
    * ip address and find the corresponding pointer to the geo data record
    * @param nodeAddress - current node number
    * @param ip - ip address we are looking up
    * @param ipBit - bit number of the ip address
    * @param nodeCount - total number of nodes
    * @param ipLength - binary length of ip address (128 or 32)
    * @return - a pointer to a geo data record in the data section
    */
  def getGeoDataRecord(nodeAddress: Int, ip: IPAddress, ipBit: Int, nodeCount: Int, ipLength: Int): Try[Int] = {

    def getNodeAddress(nodeNumber: Int): Int = nodeNumber * 6

    if (ipBit == ipLength) {
      // we've come too far
      return Failure(new Exception("Data record location not found"))
    }

    val result = getNodeVal(nodeAddress, byteArray, getDirection(ipBit, ip, ipLength))
    logger.debug(f"getting node val - address: $nodeAddress bit: $ipBit val " +
              getDirection(ipBit, ip, ipLength) + " got val: " + (result))
    result match {
      case x if x == nodeCount => {
        Failure(new Exception("address data does not exist for this ip"))
      }
      case dataPtr if dataPtr > nodeCount => {
        logger.debug(s"address pointer found: $dataPtr")
        Success(getDataOffset(result, nodeCount))
      }
      case nodeNum if nodeNum < nodeCount => {
        getGeoDataRecord(getNodeAddress(nodeNum), ip, ipBit + 1, nodeCount, ipLength)
      }
    }
  }

  /**
    * For the given bit number, determine whether we should navigate
    * left or right through the search tree
    * @param index - the bit number to interrogate
    * @param ip - the ip address we are inspecting
    * @param ipLength - the total length of the ip address in bits
    * @return - Direction case class either Left or Right
    */
  def getDirection(index: Int, ip: IPAddress, ipLength: Int): Direction = {
    ip.getv6Mapped(index) match {
      case '0' => Left
      case '1' => Right
    }
  }

  /**
    * Depending on the direction choice, read the value of the node
    * @param index - the pointer to the position in the byte array
    * @param bytes - the mmdb file byte array
    * @param dir - the Direction case class indicating the left or right record
    * @return - the value of the chosen node
    */
  def getNodeVal(index: Int, bytes: Array[Byte], dir: Direction): Int = {

    dir match {
      case Left => {
        val b1 = bytes(index) & 0xFF
        val b2 = bytes(index + 1) & 0xFF
        val b3 = bytes(index + 2) & 0xFF
        (b1 << 16) + (b2 << 8) + b3
      }
      case Right => {
        val b1 = bytes(index + 3) & 0xFF
        val b2 = bytes(index + 4) & 0xFF
        val b3 = bytes(index + 5) & 0xFF
        (b1 << 16) + (b2 << 8) + b3
      }
    }
  }

  /**
    * Get the pointer to the data section record from the node value
    * @param dataPtr - the node pointer val
    * @param nodeCount - the number of nodes
    * @return - a pointer to a data section geo record
    */
  def getDataOffset(dataPtr: Int, nodeCount: Int): Int = dataPtr - nodeCount - 16

}
