import com.rbmhtechnology.eventuate.VectorTime

val v1 = VectorTime("A" -> 1l)
val v2 = VectorTime("A" -> 1l,"B" -> 1l)

v1 < v2
v1 > v2
v1 equiv v2
v1 <-> v2

val v = com.rbmhtechnology.eventuate.StabilityChecker2.stableVectorTime(Seq(v1,v2))