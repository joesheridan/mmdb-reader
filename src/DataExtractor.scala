
package mmdb

import scala.util.{Try, Success, Failure}

abstract class FieldType
case object PTR extends FieldType
case object STR extends FieldType
case object DBL extends FieldType
case object BYT extends FieldType
case object UINT16 extends FieldType
case object UINT32 extends FieldType
case object MAP extends FieldType

case class ControlInfo(fieldType: FieldType, payload: Int)
case class Pointer(address: Int, next: Int, item: Any)
case class ReadResult(item: Any, next: Int)

/**
  * Provides functionality to extract geo data from the mmdb database file
  * @param bytes - the mmdb byte array representation
  * @param dataSection - the pointer to the geo data section
  */
class DataExtractor(bytes: Array[Byte], dataSection: Int) {

  /**
    * Retrieves the country iso code from a given geo data record
    * @param recordPtr - pointer to a geo data record
    * @return - the string of the iso code
    */
  def getIsoCode(recordPtr: Int): Try[String] = {

    val result = readItem(recordPtr)
    result match {
      case Success(ReadResult(map: Map[String, Any], _)) => {
        Try {
          val isoCode = map.get("country").flatMap {
            case innerMap: Map[String, String] => innerMap.get("iso_code")
          }
          isoCode getOrElse (throw new Exception("iso_code entry not found in data record"))
        }
      }
      case Success(_) => Failure(new Exception("data map not found"))
      case Failure(e) => Failure(e)
    }

  }

  /**
    * Recursive function to read an item in the geo data section
    * If the item is a map, then recursively read all the map items
    * @param ptr - pointer to a geo data record
    * @return - a ReadResult containing all the data read
    */
  def readItem(ptr: Int): Try[ReadResult] = {
    val res = readControlByte(ptr)
    val nextPtr = ptr + 1

    logger.debug("ci2:" + res)

    res match {
      case Success(ControlInfo(PTR, payload)) => {
        val pointer = getPointer(ptr, payload)
        pointer.item match {
          case Success(ReadResult(item, next)) => {
            // swap the next pointer to be the byte after the pointer
            // not the one after the field the pointer has just read
            Success(ReadResult(item, pointer.next))
          }
          case Failure(e) => Failure(e)
        }
      }
      case Success(ControlInfo(STR, size)) => {
        Success(ReadResult(readStr(ptr + 1, size), ptr + 1 + size))
      }
      case Success(ControlInfo(DBL, size)) => {
        //logger.debug("reading double")
        Success(ReadResult(0, ptr + 1 + size))
      }
      case Success(ControlInfo(BYT, size)) => {
        Failure(new Exception("MMDB read error - Byte type has not been implemented yet"))
      }
      case Success(ControlInfo(UINT16, size)) => {
        logger.debug("read uint16:" + getIntFromBytes(ptr, size))
        Success(ReadResult(getIntFromBytes(ptr, size), ptr + 1 + size))
      }
      case Success(ControlInfo(UINT32, size)) => {
        logger.debug("read uint32:" + getIntFromBytes(ptr, size))
        Success(ReadResult(getIntFromBytes(ptr, size), ptr + 1 + size))
      }
      case Success(ControlInfo(MAP, size)) => {
        logger.debug("map found with items:" + size)
        readMap(ptr, size);
      }

      case Failure(e) => { logger.debug("failure:"+ e); Failure(e) }
    }

  }

  /**
    * Read a map type. Each map has a number of child elements which
    * need to be recursively read
    * @param ptr - pointer to the map in the data section
    * @param size - number of map items to read
    * @return - the map and all its children as a Scala Map type wrapped in a ReadResult
    */
  def readMap(ptr: Int, size: Int): Try[ReadResult] = {
    var nextPtr = ptr + 1
    var map: Map[String, Any] = Map()

    // read key should be a string or a pointer to a string
    (1 to size).foreach { i =>
      val pair = for {
        key <- readItem(nextPtr)
        value <- readItem(key.next)
      } yield (key, value)

      logger.debug("map pair found:" + pair.toString)
      pair match {
        case Success((ReadResult(key: String, _), value)) => {
          nextPtr = value.next
          // add the key value pair to the map
          map += key -> value.item
        }
        case Success(_) => { new Exception("map key not in string format:" + pair) }
        case Failure(e) => { logger.debug("map match failed:" + e) }
      }

    }
    logger.debug("_________________________map:" + map.mkString(" , "))
    Try(ReadResult(map, nextPtr))
  }

  /**
    * Reads the first byte in a data field to determine the type of the field
    * @param ptr - pointer to the control byte
    * @return - the type of the field as a ControlInfo case object
    */
  def readControlByte(ptr: Int): Try[ControlInfo] = {
    Try {
      val cbyte = bytes(ptr) & 0xFF
      val ctype = cbyte >> 5
      val payload = bytes(ptr) & 0x1f
      //logger.debug(f"control byte: $cbyte%x - int:" + ctype + " size in bytes:" + payload)
      val fieldtype = ctype match {
        case 0 => {
          throw new Exception("control byte was zero")
        }
        case 1 => PTR
        case 2 => STR
        case 3 => DBL
        case 4 => BYT
        case 5 => UINT16
        case 6 => UINT32
        case 7 => MAP
      }
      ControlInfo(fieldtype, payload)
    }
  }

  /**
    * Read an integer from a given pointer and number of bytes
    * @param ptr - pointer to a int structure in the data section
    * @param bytesNum - number of bytes to read
    * @return - the resulting Int
    */
  def getIntFromBytes(ptr: Int, bytesNum: Int): Int = {
    // get 4 bytes and convert to unsigned int
    val b1 = bytes(ptr) & 0xFF
    val b2 = bytes(ptr+1) & 0xFF
    val b3 = bytes(ptr+2) & 0xFF
    val b4 = bytes(ptr+3) & 0xFF
    bytesNum match {
      case 1 => b1
      case 2 => concat2Bytes(b1, b2)
      case 3 => concat3Bytes(b1, b2, b3)
      case 4 => concat4Bytes(b1, b2, b3, b4)
    }
  }

  /**
    * Add two bytes together and return an Int
    * @param b1 - the first byte
    * @param b2 - the second byte
    * @return - an Int
    */
  def concat2Bytes(b1: Int, b2: Int): Int = {
    ((b1 & 0xFF) << 8) + (b2 & 0xFF)
  }

  def concat3Bytes(b1: Int, b2: Int, b3: Int): Int = {
    ((b1 & 0xFF) << 16) + ((b2 & 0xFF) << 8) + (b3 & 0xFF)
  }

  def concat4Bytes(b1: Int, b2: Int, b3: Int, b4: Int): Int = {
    ((b1 & 0xFF) << 24) + ((b2 & 0xFF) << 16) + ((b3 & 0xFF) << 8) + (b4 & 0xFF)
  }

  /**
    * Read a value that the pointer is pointing to and wrap it in a Pointer case object
    * @param ptr - the pointer location
    * @param payload - the size of the pointer
    * @return - a Pointer case object containing the read pointer value
    */
  def getPointer(ptr: Int, payload: Int): Pointer = {
    // get the size bits (4 and 5)
    val size = (payload & 0x18) >> 3
    // get first 3 bits of pointer value
    val firstBits = payload & 0x7
    size match {
      case 0 => {
        val ptrval = concat2Bytes(firstBits, bytes(ptr + 1))
        getPointerVal(ptrval, ptr + 2)
      }
      case 1 => {
        val ptrval = concat3Bytes(firstBits, bytes(ptr + 1), bytes(ptr + 2)) + 2048
        getPointerVal(ptrval, ptr + 3)
      }
      case 2 => {
        val ptrval = concat4Bytes(firstBits, bytes(ptr + 1), bytes(ptr + 2), bytes(ptr + 3)) + 526336
        getPointerVal(ptrval, ptr + 4)
      }
      case 3 => {
        val ptrval = concat4Bytes(bytes(ptr + 1), bytes(ptr + 2), bytes(ptr + 3), bytes(ptr + 4))
        getPointerVal(ptrval, ptr + 5)
      }
    }
  }

  /**
    * Read a pointer value from an address
    * @param ptr - the pointer location
    * @param next - a pointer to the next field after the pointer
    * @return - a Pointer case object containing the new item read
    */
  def getPointerVal(ptr: Int, next: Int): Pointer = {
    val address = ptr + dataSection
    Pointer(ptr, next, readItem(address))
  }

  /**
    * Read a string field from the given location
    * @param ptr - the location to read
    * @param size - the number of bytes to read
    * @return - the resulting string
    */
  def readStr(ptr: Int, size: Int): String = {
    val chunk = bytes.slice(ptr, ptr + size).toList
    chunk.map(_.toChar).mkString
  }

  /**
    * Get the data location of a given pointer
    * @param ptr
    * @return
    */
  def getGeoDataOffset(ptr: Int): Int = ptr + dataSection

}
