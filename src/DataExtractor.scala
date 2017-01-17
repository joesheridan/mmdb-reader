/**
  * Created by joe on 14/01/2017.
  */

package mmdb

import scala.util.{Try, Success, Failure}

abstract class FieldType
case class PTR() extends FieldType
case class STR() extends FieldType
case class DBL() extends FieldType
case class BYT() extends FieldType
case class UINT16() extends FieldType
case class UINT32() extends FieldType
case class MAP() extends FieldType

case class ControlInfo(fieldType: FieldType, payload: Int)
case class Pointer(address: Int, next: Int, item: Any)
case class ReadResult(item: Any, next: Int)

class DataExtractor(bytes: Array[Byte], dataSection: Int) {

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

  def readItem(ptr: Int): Try[ReadResult] = {
    val res = readControlByte(ptr)
    val nextPtr = ptr + 1

    logger.debug("ci2:" + res)

   res match {
      case Success(ControlInfo(PTR(), payload)) => {
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
      case Success(ControlInfo(STR(), size)) => {
        Success(ReadResult(readStr(ptr + 1, size), ptr + 1 + size))
      }
      case Success(ControlInfo(DBL(), size)) => {
        //logger.debug("reading double")
        Success(ReadResult(0, ptr + 1 + size))
      }
      case Success(ControlInfo(BYT(), size)) => {
        Failure(new Exception("MMDB read error - Byte type has not been implemented yet"))
      }
      case Success(ControlInfo(UINT16(), size)) => {
        logger.debug("read uint16:" + readInt(ptr, size))
        Success(ReadResult(readInt(ptr, size), ptr + 1 + size))
      }
      case Success(ControlInfo(UINT32(), size)) => {
        logger.debug("read uint32:" + readInt(ptr, size))
        Success(ReadResult(readInt(ptr, size), ptr + 1 + size))
      }
      case Success(ControlInfo(MAP(), size)) => {
        logger.debug("map found with items:" + size)
        readMap(ptr, size);
      }

      case Failure(e) => { logger.debug("failyre:"+ e); Failure(e) }
    }

  }

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
  }

  def readInt(ptr: Int, size: Int): Int = getIntFromBytes(ptr, size)

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

  def concat2Bytes(b1: Int, b2: Int): Int = {
    ((b1 & 0xFF) << 8) + (b2 & 0xFF)
  }

  def concat3Bytes(b1: Int, b2: Int, b3: Int): Int = {
    ((b1 & 0xFF) << 16) + ((b2 & 0xFF) << 8) + (b3 & 0xFF)
  }

  def concat4Bytes(b1: Int, b2: Int, b3: Int, b4: Int): Int = {
    ((b1 & 0xFF) << 24) + ((b2 & 0xFF) << 16) + ((b3 & 0xFF) << 8) + (b4 & 0xFF)
  }

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

  def getPointerVal(ptr: Int, next: Int): Pointer = {
    val address = ptr + dataSection
    Pointer(ptr, next, readItem(address))
  }

  def readStr(ptr: Int, size: Int): String = {
    val chunk = bytes.slice(ptr, ptr + size).toList
    chunk.map(_.toChar).mkString
  }

  def getGeoDataOffset(ptr: Int): Int = ptr + dataSection

}
