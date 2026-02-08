package com.pawelzabczynski.tessera.tree

import com.pawelzabczynski.tessera.tree.TimestampNode.{DataNode, LeafNode}
import com.pawelzabczynski.tessera.tree.TimestampTree.{
  LeafCountBlockLength,
  NodeIdBlockSize,
  NumberNodesLength,
  RootNameLength,
  TimeBaseBlockLength,
  VersionIdLength,
  pathToId,
}
import net.jpountz.xxhash.XXHashFactory

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.time.Instant
import scala.annotation.tailrec
import scala.collection.mutable.{ArrayBuffer, Map => MMap, Set => MSet, Stack => MStack}

/** @param version
  *   compression version of compression, important when uncompress data
  * @param root
  *   the root node name, must be shorter than 256 characters, for objects it will be usually `.`
  * @param nodes
  *   graph representation as adjacency list
  * @param timeBase
  *   the time to which is delta calculated, allow shrink data size in compressed data
  */
class TimestampTree private (version: Byte, root: String, nodes: MMap[Int, TimestampNode], timeBase: Option[Instant]) {

  /** mark path and node in graph as updated at given timestamp
    *
    * @param path
    *   the exact path from root to entry in object
    * @param ts
    *   timestamp on which entry was updated
    */
  def put(path: List[String], ts: Instant): TimestampTree = {
    @tailrec
    def loop(p: List[String], parent: DataNode, accPath: List[String]): Unit =
      p match {
        case Nil         => ()
        case head :: Nil =>
          val currentPath = head :: accPath
          val currentId = TimestampTree.pathToId(currentPath.reverse)
          nodes.get(currentId) match {
            case Some(n: LeafNode) =>
              val modifiedAt = nodeTS(ts)
              val updatedNode = n.withModifiedAt(modifiedAt)
              parent.addChild(updatedNode)
              nodes += updatedNode.id -> updatedNode
            case Some(dn: DataNode) =>
              // override existing node, make it a leaf.
              // Whole subtree is replaced and all nodes in subtree have the same timestamp
              val modifiedAt = nodeTS(ts)
              val leafNode = dn.asLeaf(modifiedAt)
              parent.addChild(leafNode)
              nodes += dn.id -> leafNode
            case None =>
              val modifiedAt = nodeTS(ts)
              val node = LeafNode(currentId, modifiedAt)
              parent.addChild(node)
              nodes += node.id -> node
          }
        case head :: tail =>
          val currentPath = head :: accPath
          val currentId = TimestampTree.pathToId(currentPath.reverse)
          nodes.get(currentId) match {
            case Some(ln: LeafNode) =>
              val currentNode = ln.asData
              nodes += currentNode.id -> currentNode
              parent.addChild(currentNode)
              loop(tail, currentNode, currentPath)
            case Some(dn: DataNode) =>
              parent.addChild(dn)
              loop(tail, dn, currentPath)
            case None =>
              val currentNode = DataNode(currentId, MSet.empty)
              parent.addChild(currentNode)
              nodes += currentNode.id -> currentNode
              loop(tail, currentNode, currentPath)
          }
      }

    path match {
      case head :: tail if head == root =>
        val rootId = TimestampTree.pathToId(head)

        nodes.get(rootId) match {
          case Some(dn: DataNode) => loop(tail, dn, head :: Nil)
          case Some(_: LeafNode)  =>
            val currentRoot = DataNode(rootId, MSet.empty)
            nodes += currentRoot.id -> currentRoot
            loop(tail, currentRoot, head :: Nil)
          case None => throw new IllegalArgumentException(s"Missing root node")
        }
      case _ :: _ => throw new IllegalArgumentException(s"path must start at root.")
      case Nil    => ()
    }

    this
  }

  /** @param path
    *   the path to object entry to check when last time was updated
    * @return
    *   the duration in milliseconds thad has been elapsed since [[timeBase]]
    */
  def modifyAt(path: List[String]): Long = {
    @tailrec
    def loop(p: List[String], accPath: List[String]): Long =
      p match {
        case Nil         => -1L
        case head :: Nil =>
          val currentPath = head :: accPath
          val currentId = TimestampTree.pathToId(currentPath.reverse)
          nodes.get(currentId) match {
            case Some(ln: LeafNode) => ln.modifiedAt
            case Some(dn: DataNode) => calculateDataNodeTs(dn.id)
            case None               => -1
          }
        case head :: tail =>
          val currentPath = head :: accPath
          val currentId = TimestampTree.pathToId(currentPath.reverse)
          nodes.get(currentId) match {
            case Some(ln: LeafNode) =>
              // even if path is longer, whole subtree has been modified at time when whole subtree has been updated
              ln.modifiedAt
            case Some(_: DataNode) => loop(tail, currentPath)
            case None              => -1L
          }
      }

    path match {
      case Nil                       => -1L
      case head :: _ if head == root => loop(path, Nil)
      case _                         => throw new IllegalArgumentException(s"Path must starts at root node.")
    }
  }

  /** allow remove no longer existing data from tree, if not performed than graph potentially can grow infinitely
    * @note
    *   allPaths must contain all path combination otherwise the resulting graph can be corrupted and fail on
    *   serialization
    * @param allPaths
    *   all paths that exists in object
    */
  def shrink(allPaths: List[List[String]]): TimestampTree = {
    val pathToIds = allPaths.map(p => TimestampTree.pathToId(p) -> p).toMap
    val pathsInTree = nodes.keys
    val toRemove = MSet.empty[Int]

    for (tp <- pathsInTree)
      pathToIds.get(tp) match {
        case Some(p) => ()
        case None    => toRemove += tp
      }

    // removing from relations, adjacent lists
    nodes.foreach {
      case (_, _: LeafNode)         => ()
      case (nodeId, node: DataNode) =>
        // convert into leaf node as no longer has child
        val ts = calculateDataNodeTs(nodeId)
        toRemove.foreach(node.children.remove)
        if (node.children.isEmpty) {

          nodes += (nodeId -> node.asLeaf(ts))
        }
    }
    // remove from graph
    for (nId <- toRemove)
      nodes.remove(nId)

    this
  }

  /** Serialize graph as binary data, the format preserve whole tree structure. memory layout:
    * {{{
    *   +---------+---------+------+---------+---------+----------+
    *   | version | rootLen | root | nodeCnt | leafCnt | timeBase |
    *   | 1b      | 1b      | Nb   | 2b      | 2b      | 8b       |
    *   +---------+---------+------+---------+---------+----------+
    *   | topology (balanced parenthesis, 2 bits/node)             |
    *   +----------------------------------------------------------+
    *   | node IDs (XXHash32, 4b each, DFS pre-order)              |
    *   +----------------------------------------------------------+
    *   | timestamps (leaf-only, delta+zigzag+varint, DFS order)   |
    *   +----------------------------------------------------------+
    * }}}
    */
  def serialize(): Array[Byte] = {
    val topologyBits = new ArrayBuffer[Boolean]()
    val ids = new ArrayBuffer[Int]()
    val rawTimestamps = new ArrayBuffer[Long]()

    // create balanced parentheses and collect all timestamps
    def dsf(nodeId: Int): Unit = {
      topologyBits += true
      ids += nodeId
      nodes(nodeId) match {
        case ln: LeafNode =>
          rawTimestamps += ln.modifiedAt
        case dn: DataNode =>
          // preserve always same order to avoid problems between scala versions if set implementation change
          // for traversing and searching decompressed graph order doesn't matter, but during serialization/deserialization is crucial
          dn.children.toList.sorted.foreach(dsf)
      }
      topologyBits += false
    }

    dsf(TimestampTree.pathToId(root))

    val rootBytes = root.getBytes(StandardCharsets.UTF_8)
    val rootNameSize = rootBytes.length
    require(rootNameSize < 256, "The root name cannot be string longer than 256 characters")
    val topologyBytes = TimestampTree.packBits(topologyBits)

    /* TODO: improve with RLE (run-length encoding) on the delta stream.
     * When many leaves share the same timestamp, deltas become runs of zeros.
     * Add a heuristic to decide whether RLE is beneficial (e.g. ratio of unique deltas to total count)
     * and encode as (value, count) pairs instead of individual varints.
     */
    val tsStream = new ByteArrayOutputStream()
    var prevTs = 0L
    rawTimestamps.foreach { ts =>
      // zigzag -> make numbers smaller, usually, before variable length encoding, resulting better compression.
      // Lower numbers require fewer bytes when save
      TimestampTree.writeVariant(tsStream, TimestampTree.zigzagEncode(ts - prevTs))
      prevTs = ts
    }
    val compressedTimestamps = tsStream.toByteArray

    val nodeCount = ids.size
    val leafCount = rawTimestamps.size
    val idsSize = NodeIdBlockSize * nodeCount
    val totalSize = VersionIdLength +
      RootNameLength +
      rootBytes.length +
      NumberNodesLength +
      LeafCountBlockLength +
      TimeBaseBlockLength +
      topologyBytes.length +
      idsSize +
      compressedTimestamps.length

    val compressed = ByteBuffer.allocate(totalSize)
    // compression metadata
    compressed.put(version)
    compressed.put(
      rootBytes.length.toByte
    ) // important: conversion int -> byte, above 127 bytes change to negative number be aware during deserialization
    compressed.put(rootBytes)
    compressed.putShort(nodeCount.toShort)
    compressed.putShort(leafCount.toShort)
    compressed.putLong(timeBase.map(_.toEpochMilli).getOrElse(0L))
    // graph serialized as balanced parentheses
    compressed.put(topologyBytes)
    ids.foreach(h => compressed.putInt(h))
    // timestamps
    compressed.put(compressedTimestamps)

    compressed.array()
  }

  private def collectLeafs(nodeId: Int): List[LeafNode] = {
    @tailrec
    def loop(stack: List[Int], acc: List[LeafNode]): List[LeafNode] =
      stack match {
        case Nil          => acc
        case head :: tail =>
          nodes.get(head) match {
            case Some(ln: LeafNode) => loop(tail, ln :: acc)
            case Some(dn: DataNode) => loop(dn.children.toList ++ tail, acc)
            case None               => loop(tail, acc)
          }
      }

    nodes.get(nodeId) match {
      case Some(node) => loop(node.id :: Nil, List.empty)
      case None       => List.empty[LeafNode]
    }
  }

  private def calculateDataNodeTs(nodeId: Int): Long = {
    val all = collectLeafs(nodeId)
    val timeDelta = all.foldLeft(Long.MaxValue) { case (acc, n) =>
      Math.min(acc, n.modifiedAt)
    }

    if (timeDelta == Long.MaxValue) -1 else timeDelta
  }

  private def nodeTS(nodeModifyTs: Instant): Long =
    timeBase match {
      case Some(base) => nodeModifyTs.toEpochMilli - base.toEpochMilli
      case None       => nodeModifyTs.toEpochMilli
    }

}

object TimestampTree {
  private val VersionIdLength = 1
  private val RootNameLength = 1
  private val NumberNodesLength = 2
  private val LeafCountBlockLength = 2
  private val TimeBaseBlockLength = 8 // long value
  private val NodeIdBlockSize = 4 // int value
  private val hasher = XXHashFactory.fastestJavaInstance().hash32()

  private def pathToId(p: String): Int = {
    val data = p.getBytes(StandardCharsets.UTF_8)
    hasher.hash(data, 0, data.length, 0)
  }

  private def pathToId(path: List[String]): Int = {
    val p = path.mkString(".")
    pathToId(p)
  }

  def apply(root: String, base: Instant): TimestampTree = {
    val nodes = MMap.empty[Int, TimestampNode]
    val rootId = pathToId(root)
    nodes += rootId -> LeafNode(rootId, 0)

    new TimestampTree(1, root, nodes, Some(base))
  }

  def deserialize(data: Array[Byte]): TimestampTree = {
    val buf = ByteBuffer.wrap(data)

    val version = buf.get()
    val rootLen = buf.get() & 0xff // int -> byte and on decompression is byte -> int require mask
    val rootBytes = new Array[Byte](rootLen)
    buf.get(rootBytes)
    val rootStr = new String(rootBytes, StandardCharsets.UTF_8)

    val nodeCount = buf.getShort() & 0xffff
    val leafCount = buf.getShort() & 0xffff
    val timeBaseMillis = buf.getLong()

    val topologyByteCount = (2 * nodeCount + 7) / 8
    val topologyBytes = new Array[Byte](topologyByteCount)
    buf.get(topologyBytes)
    val topologyBits = unpackBits(topologyBytes, 2 * nodeCount)

    val hashArray = (0 until nodeCount).map(_ => buf.getInt()).toArray

    var prevTs = 0L
    val tsArray = (0 until leafCount).map { _ =>
      val delta = zigzagDecode(readVarint(buf))
      prevTs += delta
      prevTs
    }.toArray

    var bitIdx = 0
    var idIdx = 0
    var tsIdx = 0
    val nodes = MMap.empty[Int, TimestampNode]

    def decode(): Int = {
      assert(topologyBits(bitIdx), s"Expected open paren at bit $bitIdx")
      bitIdx += 1

      val id = hashArray(idIdx)
      idIdx += 1

      val children = MSet.empty[Int]
      while (bitIdx < topologyBits.length && topologyBits(bitIdx))
        children += decode()

      bitIdx += 1 // consume close paren

      if (children.isEmpty) {
        val ts = tsArray(tsIdx)
        tsIdx += 1
        nodes += id -> LeafNode(id, ts)
      } else {
        nodes += id -> DataNode(id, children)
      }

      id
    }

    decode()

    new TimestampTree(version, rootStr, nodes, Some(Instant.ofEpochMilli(timeBaseMillis)))
  }

  private[tessera] def zigzagEncode(n: Long): Long =
    (n << 1) ^ (n >> 63)

  private[tessera] def zigzagDecode(n: Long): Long =
    (n >>> 1) ^ -(n & 1)

  /** Variable length encoding using 8 bit blocks, 128 based variant as it's done in protobuf
    *
    * @see
    *   [[https://protobuf.dev/programming-guides/encoding/]]
    */
  private[tessera] def writeVariant(out: ByteArrayOutputStream, value: Long): Unit = {
    var v = value
    while ((v & ~0x7fL) != 0) {
      out.write(((v & 0x7f) | 0x80).toInt)
      v >>>= 7
    }
    out.write((v & 0x7f).toInt)
  }

  /** Read variable length encoded value using 8 bit blocks, 128 based variant as it's done in protobuf
    *
    * @see
    *   [[https://protobuf.dev/programming-guides/encoding/]]
    */
  private[tessera] def readVarint(buf: ByteBuffer): Long = {
    var result = 0L
    var shift = 0
    var b = 0
    do {
      b = buf.get() & 0xff
      result |= (b & 0x7fL) << shift
      shift += 7
    } while ((b & 0x80) != 0)
    result
  }

  /** Pack bits into 1 byte blocks
    */
  private[tessera] def packBits(bits: ArrayBuffer[Boolean]): Array[Byte] = {
    val byteCount = (bits.size + 7) / 8
    val bytes = new Array[Byte](byteCount)
    bits.indices.foreach { i =>
      if (bits(i)) bytes(i / 8) = (bytes(i / 8) | (1 << (7 - i % 8))).toByte
    }
    bytes
  }

  /** Unpack bits into 1 byte blocks
    *
    * @param bytes
    *   blob containing boolean data
    * @param count
    *   the number of bytes to unpack
    */
  private[tessera] def unpackBits(bytes: Array[Byte], count: Int): Array[Boolean] = {
    val bits = new Array[Boolean](count)
    (0 until count).foreach { i =>
      bits(i) = (bytes(i / 8) & (1 << (7 - i % 8))) != 0
    }
    bits
  }
}
