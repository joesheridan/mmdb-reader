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


  def getGeoData(ptr: Int) = {

    val x = readItem(ptr)
    println("first item read:"+x)

  }

  def readItem(ptr: Int): Try[ReadResult] = {
    val res = readControlByte(ptr)

    println("ci2:" + res)

   res match {
      case Success(ControlInfo(PTR(), payload)) => {

          //println("reading ptr")
          val pointer = getPointer(ptr, payload)
          //prinltn("pointer value:" + getPointerVal(pointer))
        pointer.item match {
          case Success(ReadResult(item, next)) => {
            Success(ReadResult(item, pointer.next))
          }
          case Failure(e) => Failure(e)
        }


      }
      case Success(ControlInfo(STR(), size)) => {
        println("reading str:" + readStr(ptr+1, size))
        Success(ReadResult(readStr(ptr + 1, size), ptr + 1 + size))
      }
      case Success(ControlInfo(DBL(), size)) => {
        //println("reading double")
        Success(ReadResult(0, ptr + 1 + size))
      }
      case Success(ControlInfo(BYT(), size)) => {
        //println("reading byte")
        Success(ReadResult(0, ptr + size))
      }
      case Success(ControlInfo(UINT16(), size)) => {
        println("read uint16:" + readInt(ptr, size))
        Success(ReadResult(readInt(ptr, size), ptr + 1 + size))
      }
      case Success(ControlInfo(UINT32(), size)) => {
        println("read uint32:" + readInt(ptr, size))
        Success(ReadResult(readInt(ptr, size), ptr + 1 + size))
      }
      case Success(ControlInfo(MAP(), size)) => {
        println("map found with items:" + size)
        var nextPtr = Try(ptr + 1)
        var map: Map[Any, Any] = Map()

        // read key should be a string or a pointer to a string
        for (i <- 1 to size) {
          println("map iteration - i:" + i)
          val pair = for {
            next <- nextPtr
            key <- readItem(next)
            value <- readItem(key.next)
          } yield (key, value)

          println("map pair found:" + pair.toString)
          pair match {
            case Success((key, value)) => {
              nextPtr = Try(value.next)
              map += key.item -> value.item
            }
            case Failure(e) => { println("map match failed:" + e) }
          }

        }
        println("_________________________map:" + map.mkString(" , "))
        Try(ReadResult(map, nextPtr.getOrElse(0)))
      }

      case Failure(e) => { println("failyre:"+ e); Failure(e) }
    }

  }

  def readControlByte(ptr: Int): Try[ControlInfo] = {
    Try {
      val cbyte = bytes(ptr) & 0xFF
      val ctype = cbyte >> 5
      val payload = bytes(ptr) & 0x1f
      //println(f"control byte: $cbyte%x - int:" + ctype + " size in bytes:" + payload)
      val fieldtype = ctype match {
        case 0 => {
          //println("control byte was zero..")
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
      val next = ptr + 1
      ControlInfo(fieldtype, payload)
    }
  }

  def readInt(ptr: Int, size: Int): Int = {
    getIntFromBytes(ptr, size)
  }

  def getIntFromBytes(ptr: Int, bytesNum: Int): Int = {
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
    //println("getintfrombytes res:"+res)
    res
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
    //println("pointer size:"+size)
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
    //println(f"pointer address: 0x$address%x")
    //println("pointer data val:" + (bytes(address) & 0xFF).toChar)

    Pointer(ptr, next, readItem(address))
  }

  def getStr(ptr: Int): Try[String] = {
    val ci = readControlByte(ptr)
    ci match {
      case Success(ControlInfo(STR(), size)) => {
        Success(readStr(ptr+1, size))
      }
      case Success(_) => {
        throw new Exception("getstr expecting string - parsing failed")
      }
      case Failure(e) => Failure(e)
    }
  }

  def readStr(ptr: Int, size: Int): String = {
    val r = bytes.slice(ptr, ptr+size).toList
    //println(r)
    r.map(_.toChar).mkString
  }

  def getGeoDataOffset(ptr: Int): Int = {
    ptr + dataSection
  }
}
