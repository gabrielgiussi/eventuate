import com.rbmhtechnology.eventuate.VectorTime

val a = """
          |A = 1,B = 2,C = 3
          |A = 1,B = 1,C = 0
        """.stripMargin.trim
val lines =  a.split(System.lineSeparator()).toSeq.map(_.trim)
val lines2 = lines.map(_.split(",").toSeq)
val lines3 = lines2.map(_.map(_.split("=").toSeq.map(_.trim).toList match {
  case name::number::Nil => (name -> number.toLong)
  case other => throw new Exception(other.toString)
}))
val lines4 = lines3.toList.map(x => VectorTime(x: _*))
