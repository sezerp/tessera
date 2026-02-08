import com.pawelzabczynski.tessera.tree.TimestampTree
import java.time.Instant

object SimpleExample extends App {

  val baseTime = Instant.parse("2025-12-26T15:45:00.000Z")

  val tree = TimestampTree("a", baseTime)

  val paths = List(
    List("a", "b", "c", "d", "e") -> (60, 60000),
    List("a", "b", "c", "f", "g") -> (3000, 3000000),
    List("a", "b", "h", "i", "j") -> (180, 180000),
    // this part must be done in certain order, from older to newer, bigger delta time to lower as is done in real life
    // otherwise the data will be corrupted by overriding the newest nodes by oldest
    List("a", "b", "k") -> (305, 230000),
    List("a", "b", "k", "l") -> (240, 230000),
    List("a", "b", "k", "l", "m") -> (230, 230000),
  )

  paths.foreach { case (path, (ts, _)) =>
    tree.put(path, baseTime.plusSeconds(ts))
  }

  println("=== Before compression ===")
  paths.foreach { case (path, (_, expected)) =>
    println(s"""  ${path.mkString(".")} -> modifyAt=${tree.modifyAt(path)} (expected ${expected.toLong})""")
  }

  val compressed = tree.serialize()

  println(s"\nCompressed size: ${compressed.length} bytes")

  val restored = TimestampTree.deserialize(compressed)

  println("\n=== After decompression ===")
  paths.foreach { case (path, (_, expected)) =>
    println(s"""  ${path.mkString(".")} -> modifyAt=${restored.modifyAt(path)} (expected ${expected.toLong})""")
  }

  val allMatch = paths.forall { case (path, (_, expected)) =>
    val originalDuration = tree.modifyAt(path)
    val restoredDuration = restored.modifyAt(path)
    originalDuration == expected && originalDuration == restoredDuration
  }
  println(s"\nRound-trip match: $allMatch")

}
