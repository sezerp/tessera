
import com.pawelzabczynski.tessera.tree.TimestampTree
import java.time.Instant

object SimpleExample extends App {


  val baseTime = Instant.parse("2025-12-26T15:45:00.000Z")

  val tree = TimestampTree("a", baseTime)

  val paths = List(
    List("a", "b", "c", "d",  "e") -> 60,
    List("a", "b", "c", "f",  "g") -> 3000,
    List("a", "b", "h", "i",  "j") -> 180,
    List("a", "b", "k") -> 240,
    List("a", "b", "k", "l") -> 300,
    List("a", "b", "k", "l", "m") -> 300,
    List("a", "b", "k", "l", "n") -> 56000,
    List("a", "b", "k", "l", "o") -> 2100,
    List("a", "b", "k", "l", "o1") -> 2100,
    List("a", "b", "k", "l", "o2") -> 2100,
    List("a", "b", "k", "l", "o3") -> 2100,
    List("a", "b", "k", "l", "o4") -> 2100,
    List("a", "b", "k", "l", "o5") -> 2100,
    List("a", "b", "k", "l", "o6") -> 2100,
    List("a", "b", "k", "l", "o7") -> 2100,
    List("a", "b", "k", "l", "o8") -> 2100,
    List("a", "b", "k", "l", "o9") -> 2100,
    List("a", "b", "k", "l", "o10") -> 2100,
    List("a", "b", "k", "l", "o11") -> 2100,
    List("a", "b", "k", "l", "o12") -> 2100,
    List("a", "b", "k", "l", "o13") -> 2100,
    List("a", "b", "k", "l", "o14") -> 2100,
    List("a", "b", "k", "l", "o15") -> 2100,
    List("a", "b", "k", "l", "o16") -> 2100,
    List("a", "b", "k", "l", "o17") -> 2100,
    List("a", "b", "k", "l", "o18") -> 2100,
    List("a", "b", "k", "l", "o19") -> 2100,
    List("a", "b", "k", "l", "o20") -> 2100,
    List("a", "b", "k", "l", "o22") -> 2100,
    List("a", "b", "k", "l", "o23") -> 2100,
    List("a", "b", "k", "l", "o24") -> 2100,
    List("a", "b", "k", "l", "o25") -> 2100,
    List("a", "b", "k", "l", "o26") -> 2100,
    List("a", "b", "k", "l", "o27") -> 2100,
    List("a", "b", "k", "l", "o28") -> 2100,
    List("a", "b", "k", "l", "o29") -> 990,
    List("a", "b", "k", "l", "o30") -> 2100,
    List("a", "b", "k", "l", "o31") -> 29999,
    List("a", "b", "k", "l", "o32") -> 32423,
    List("a", "b", "k", "l", "o33") -> 2100,
    List("a", "b", "k", "l", "o34") -> 225535,
    List("a", "b", "k", "l", "o35") -> 2100,
    List("a", "b", "k", "l", "o36") -> 2100,
    List("a", "b", "k", "l", "o37") -> 2100,
    List("a", "b", "k", "l", "o38") -> 2100,
    List("a", "b", "k", "l", "o39") -> 5634656,
    List("a", "b", "k", "l", "o40") -> 2100,
    List("a", "b", "k", "l", "o41") -> 2100,
    List("a", "b", "k", "l", "o42") -> 2100,
    List("a", "b", "k", "l", "o43") -> 2100,
    List("a", "b", "k", "l", "o44") -> 2100,
    List("a", "b", "k", "l", "o45") -> 2100,
    List("a", "b", "k", "l", "o46") -> 2100,
    List("a", "b", "k", "l", "o47") -> 2100,
    List("a", "b", "k", "l", "o48") -> 2100,
    List("a", "b", "k", "l", "o49") -> 2100,
    List("a", "b", "k", "l", "o50") -> 2100,
    List("a", "b", "k", "l", "o51") -> 2100,
    List("a", "b", "k", "l", "o52") -> 53453,
    List("a", "b", "k", "l", "o53") -> 2100,
    List("a", "b", "k", "l", "o54") -> 2100,
    List("a", "b", "k", "l", "o55") -> 2100,
    List("a", "b", "k", "l", "o56") -> 2100,
    List("a", "b", "k", "l", "o57") -> 2100,
    List("a", "b", "k", "l", "o58") -> 2100,
    List("a", "b", "k", "l", "o59") -> 2100,
    List("a", "b", "k", "l", "o60") -> 2100,
    List("a", "b", "k", "l", "o61") -> 2100,
    List("a", "b", "k", "l", "o62") -> 2100,
    List("a", "b", "k", "l", "o63") -> 2100,
    List("a", "b", "k", "l", "o64") -> 33333,
    List("a", "b", "k", "l", "o65") -> 2100,
    List("a", "b", "k", "l", "o66") -> 2100,
    List("a", "b", "k", "l", "o67") -> 2100,
    List("a", "b", "k", "l", "o68") -> 2100,
    List("a", "b", "k", "l", "o69") -> 2100,
    List("a", "b", "k", "l", "o70") -> 2100,
    List("a", "b", "k", "l", "o71") -> 57657,
    List("a", "b", "k", "l", "o72") -> 2100,
    List("a", "b", "k", "l", "o73") -> 2100,
    List("a", "b", "k", "l", "o74") -> 2100,
    List("a", "b", "k", "l", "o75") -> 2100,
    List("a", "b", "k", "l", "o76") -> 2100,
    List("a", "b", "k", "l", "o77") -> 2100,
    List("a", "b", "k", "l", "o78") -> 2100,
    List("a", "b", "k", "l", "o79") -> 2100,
    List("a", "b", "k", "l", "o80") -> 2100,
    List("a", "b", "k", "l", "o81") -> 2100,
    List("a", "b", "k", "l", "o82") -> 2100,
    List("a", "b", "k", "l", "o83") -> 2100,
    List("a", "b", "k", "l", "o84") -> 2100,
    List("a", "b", "k", "l", "o85") -> 2100,
    List("a", "b", "k", "l", "o86") -> 2100,
    List("a", "b", "k", "l", "o87") -> 2100,
    List("a", "b", "k", "l", "o88") -> 2100,
    List("a", "b", "k", "l", "o89") -> 2100,
    List("a", "b", "k", "l", "o90") -> 2100,
    List("a", "b", "k", "l", "o91") -> 2100,
    List("a", "b", "k", "l", "o92") -> 2100,
    List("a", "b", "k", "l", "o93") -> 2100,
    List("a", "b", "k", "l", "o94") -> 2100,
    List("a", "b", "k", "l", "o95") -> 2100,
    List("a", "b", "k", "l", "o96") -> 2100,
    List("a", "b", "k", "l", "o97") -> 2100,
    List("a", "b", "k", "l", "o98") -> 2100,
    List("a", "b", "k", "l", "o99") -> 2100,
    List("a", "b", "k", "l", "o100") -> 2100,
    List("a", "b", "k", "l", "o101") -> 2100,
    List("a", "b", "k", "l", "o102") -> 2100,
    List("a", "b", "k", "l", "o103") -> 2100,
    List("a", "b", "k", "l", "o104") -> 2100,
    List("a", "b", "k", "l", "o105") -> 2100,
    List("a", "b", "k", "l", "o106") -> 2100,
    List("a", "b", "k", "l", "o107") -> 2100,
    List("a", "b", "k", "l", "o108") -> 2100,
    List("a", "b", "k", "l", "o109") -> 2100,
    List("a", "b", "k", "l", "o110") -> 2100,
    List("a", "b", "k", "l", "o111") -> 2100,
    List("a", "b", "k", "l", "o112") -> 2100,
    List("a", "b", "k", "l", "o113") -> 2100,
  )

  paths.foreach { case (path, ts) =>
    tree.put(path, baseTime.plusSeconds(ts))
  }

  println("=== Before compression ===")
  paths.foreach { case (path, ts) =>
    println(s"""  ${path.mkString(".")} -> modifyAt=${tree.modifyAt(path)} (expected ${ts.toLong * 1000})""")
  }

  val compressed = tree.serialize()

  println(s"\nCompressed size: ${compressed.length} bytes")

  val restored = TimestampTree.deserialize(compressed)

  println("\n=== After decompression ===")
  paths.foreach { case (path, ts) =>
    println(s"""  ${path.mkString(".")} -> modifyAt=${restored.modifyAt(path)} (expected ${ts.toLong * 1000})""")
  }

  val allMatch = paths.forall { case (path, _) =>
    tree.modifyAt(path) == restored.modifyAt(path)
  }
  println(s"\nRound-trip match: $allMatch")

}
