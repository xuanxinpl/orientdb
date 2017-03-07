package com.orientechnologies.common.directmemory;

public class OTwoSizeQueueList {
  private final OByteBufferHolder head = new OByteBufferHolder(null, -1, -1);
  private final OByteBufferHolder tail = new OByteBufferHolder(null, -1, -1);

  OTwoSizeQueueList() {
    head.prev = null;
    head.next = tail;

    tail.next = null;
    tail.prev = head;
  }

  public void push(OByteBufferHolder holder) {
    final OByteBufferHolder prev = tail.prev;
    final OByteBufferHolder next = tail;

    holder.prev = prev;
    holder.next = next;

    prev.next = holder;
    next.prev = holder;
  }

  OByteBufferHolder pull() {
    OByteBufferHolder holder = head.next;

    if (holder == tail)
      return null;

    head.next = holder.next;
    holder.next.prev = head;

    holder.prev = null;
    holder.next = null;

    return holder;
  }

  public void remove(OByteBufferHolder holder) {
    OByteBufferHolder prev = holder.prev;
    OByteBufferHolder next = holder.next;

    holder.prev = null;
    holder.next = null;

    prev.next = next;
    next.prev = prev;
  }
}
