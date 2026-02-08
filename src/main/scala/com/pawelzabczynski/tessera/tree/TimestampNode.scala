package com.pawelzabczynski.tessera.tree

import scala.collection.mutable.{Set => MSet}

sealed trait TimestampNode extends Node[Int]

object TimestampNode {
  case class LeafNode(id: Int, modifiedAt: Long) extends TimestampNode {
    def withModifiedAt(modifiedAt: Long): LeafNode = {
      copy(modifiedAt = modifiedAt)
    }
    def asData: DataNode = {
      DataNode(id, MSet.empty)
    }
  }
  case class DataNode(id: Int, children: MSet[Int]) extends TimestampNode {
    def asLeaf(modifiedAt: Long): LeafNode = {
      LeafNode(id, modifiedAt)
    }

    def addChild(n: TimestampNode): Unit = {
      children.add(n.id)
    }
  }
}
