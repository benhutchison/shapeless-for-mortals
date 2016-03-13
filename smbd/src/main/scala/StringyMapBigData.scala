// Copyright (C) 2015 Sam Halliday
// License: http://www.apache.org/licenses/LICENSE-2.0
/**
 * TypeClass (api/impl/syntax) for marshalling objects into
 * `java.util.HashMap<String,Object>` (yay, big data!).
 */
package s4m.smbd

import shapeless._, shapeless.labelled._

/**
 * This exercise involves writing tests, only a skeleton is provided.
 *
 * - Exercise 1.1: derive =BigDataFormat= for sealed traits.
 * - Exercise 1.2: define identity constraints using singleton types.
 */
package object api {
  type StringyMap = java.util.HashMap[String, AnyRef]
  type BigResult[T] = Either[String, T] // aggregating errors doesn't add much

  def bigResult[T](t: T): BigResult[T] = Right[String, T](t)
}

package api {
  trait BigDataFormat[T] {
    def label: String
    def toProperties(t: T): StringyMap
    def fromProperties(m: StringyMap): BigResult[T]
  }

  trait SPrimitive[V] {
    def toValue(v: V): AnyRef
    def fromValue(v: AnyRef): V
  }

  // EXERCISE 1.2
  trait BigDataFormatId[T, V] {
    def key: String
    def value(t: T): V
  }
}

package object impl {
  import api._

  // EXERCISE 1.1 goes here
  implicit def hNilBigDataFormat = new BigDataFormat[HNil] {
    override def label: String = ???

    override def toProperties(t: HNil): StringyMap = new java.util.HashMap()

    override def fromProperties(m: StringyMap): BigResult[HNil] = Right(HNil)
  }

  implicit def hListBigDataFormat[Name <: Symbol, Head, Tail <: HList](
    implicit
    key: Witness.Aux[Name],
    pf: SPrimitive[Head],
    bft: Lazy[BigDataFormat[Tail]]
  ): BigDataFormat[FieldType[Name, Head] :: Tail] = new BigDataFormat[FieldType[Name, Head] :: Tail] {
    override def label: String = "hListBigDataFormat"

    override def toProperties(t: FieldType[Name, Head] :: Tail): StringyMap = {
      val m = bft.value.toProperties(t.tail)
      m.put(key.value.name, pf.toValue(t.head))
      m
    }

    override def fromProperties(m: StringyMap): BigResult[FieldType[Name, Head] :: Tail] = {
      val headR = m.get(key.value.name) match {
        case null =>
          Left("some sensible error here")
        case found =>
          Right(pf.fromValue(found))
      }
      val tailR = bft.value.fromProperties(m)
      for {
        h <- headR.right
        t <- tailR.right
      } yield (field[Name](h) :: t)
    }
  }

  implicit object StringSPrimitive extends SPrimitive[String] {
    def toValue(v: String): AnyRef = v
    def fromValue(v: AnyRef): String = v.asInstanceOf[String]
  }

  implicit object IntSPrimitive extends SPrimitive[Int] {
    def toValue(v: Int): AnyRef = Integer.valueOf(v)
    def fromValue(v: AnyRef): Int = v.asInstanceOf[Integer].toInt
  }

  implicit object DoubleSPrimitive extends SPrimitive[Double] {
    def toValue(v: Double): AnyRef = java.lang.Double.valueOf(v)
    def fromValue(v: AnyRef): Double = v.asInstanceOf[java.lang.Double].toDouble
  }

  implicit object BooleanSPrimitive extends SPrimitive[Boolean] {
    def toValue(v: Boolean): AnyRef = java.lang.Boolean.valueOf(v)
    def fromValue(v: AnyRef): Boolean = v.asInstanceOf[java.lang.Boolean].booleanValue()
  }

  implicit def cNilBigDataFormat = new BigDataFormat[CNil] {
    override def label: String = "CNil"

    override def toProperties(t: CNil): StringyMap = ???

    override def fromProperties(m: StringyMap): BigResult[CNil] = ???
  }
  implicit def coproductBigDataFormat[Name <: Symbol, Head, Tail <: Coproduct](
    implicit
    key: Witness.Aux[Name],
    ch: Lazy[BigDataFormat[Head]],
    ct: Lazy[BigDataFormat[Tail]]
  ): BigDataFormat[FieldType[Name, Head] :+: Tail] = new BigDataFormat[FieldType[Name, Head] :+: Tail] {
    def label: String = "coproductBigDataFormat"

    def toProperties(t: FieldType[Name, Head] :+: Tail): StringyMap = t match {
      case Inl(found) =>
        val m = ch.value.toProperties(found)
        m.put("type", key.value.name)
        m
      case Inr(tail) => ct.value.toProperties(tail)
    }

    def fromProperties(m: StringyMap): BigResult[FieldType[Name, Head] :+: Tail] = {
      if (m.containsKey("type") && m.get("type") == key.value.name)
        ch.value.fromProperties(m).right.map(v => Inl(field[Name](v)))
      else
        ct.value.fromProperties(m).right.map(Inr(_))
    }
  }

  implicit def familyBigDataFormat[T, Repr](
    implicit
    g: LabelledGeneric.Aux[T, Repr],
    fr: Lazy[BigDataFormat[Repr]]
  ) = new BigDataFormat[T] {
    override def label: String = "foibles"

    override def toProperties(t: T): StringyMap = {
      fr.value.toProperties(g.to(t))
    }

    override def fromProperties(m: StringyMap): BigResult[T] = {
      fr.value.fromProperties(m).right.map(g.from(_))
    }
  }
}

package impl {
  import api._

  // EXERCISE 1.2 goes here
}

package object syntax {
  import api._

  implicit class RichBigResult[R](val e: BigResult[R]) extends AnyVal {
    def getOrThrowError: R = e match {
      case Left(error) => throw new IllegalArgumentException(error.mkString(","))
      case Right(r) => r
    }
  }

  /** Syntactic helper for serialisables. */
  implicit class RichBigDataFormat[T](val t: T) extends AnyVal {
    def label(implicit s: BigDataFormat[T]): String = s.label
    def toProperties(implicit s: BigDataFormat[T]): StringyMap = s.toProperties(t)
    def idKey[P](implicit lens: Lens[T, P]): String = ???
    def idValue[P](implicit lens: Lens[T, P]): P = lens.get(t)
  }

  implicit class RichProperties(val props: StringyMap) extends AnyVal {
    def as[T](implicit s: BigDataFormat[T]): T = s.fromProperties(props).getOrThrowError
  }
}
