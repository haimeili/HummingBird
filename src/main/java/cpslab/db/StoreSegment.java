package cpslab.db;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.HashSet;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.locks.Lock;

import static cpslab.db.DataIO.*;

public class StoreSegment extends StoreDirect {

  private long startingRecId = RECID_LAST_RESERVED;

  Serializer serializer = null;

  public StoreSegment(String fileName, Volume.VolumeFactory volumeFactory,
                      Cache cache, int lockScale, int lockingStrategy, boolean checksum,
                      boolean compress, byte[] password, boolean readonly, boolean snapshotEnable,
                      ScheduledExecutorService executor) {
    super(fileName, volumeFactory, cache, lockScale, lockingStrategy, checksum, compress,
            password, readonly, snapshotEnable, 0, false,
            0, executor);
  }

  public StoreSegment(String fileName) {
    super(fileName);
  }

  @Override
  public void initCreate() {
    super.initCreate();
    vol.putLong(MAX_RECID_OFFSET, parity1Set(startingRecId * indexValSize));
  }

  protected void freeDataPut(long[] linkedOffsets) {
    if (CC.ASSERT && !structuralLock.isHeldByCurrentThread())
      throw new AssertionError();
    for (long v : linkedOffsets) {
      int size = round16Up((int) (v >>> 48));
      v &= MOFFSET;
      freeDataPut(v, size);
    }
  }

  protected void freeDataPut(long offset, int size) {
    if (CC.ASSERT && !structuralLock.isHeldByCurrentThread())
      throw new AssertionError();
    if (CC.ASSERT && size % 16 != 0)
      throw new DBException.DataCorruption("unalligned size");
    if (CC.ASSERT && offset % 16 != 0)
      throw new DBException.DataCorruption("wrong offset");

    vol.clear(offset, offset + size);

    //shrink store if this is last record
    if (offset + size == lastAllocatedData) {
      lastAllocatedData -= size;
      return;
    }

    long masterPointerOffset = size / 2 + FREE_RECID_STACK; // really is size*8/16
    longStackPut(masterPointerOffset, offset >>> 4);
  }

  protected void longStackPut(final long masterLinkOffset, final long value){
    if (CC.ASSERT && !structuralLock.isHeldByCurrentThread())
      throw new AssertionError();
    if (CC.ASSERT && (masterLinkOffset <= 0 ||
            masterLinkOffset > PAGE_SIZE ||
            masterLinkOffset % 8 != 0)) //TODO perhaps remove the last check
      throw new DBException.DataCorruption("wrong master link");

    long masterLinkVal = parity4Get(headVol.getLong(masterLinkOffset));
    long pageOffset = masterLinkVal & MOFFSET;

    if (masterLinkVal == 0L) {
      longStackNewPage(masterLinkOffset, 0L, value);
      return;
    }

    long currSize = masterLinkVal >>> 48;

    long prevLinkVal = parity4Get(vol.getLong(pageOffset));
    long pageSize = prevLinkVal >>> 48;
    //is there enough space in current page?
    if (currSize + 8 >= pageSize) {
      // +8 is just to make sure and is worse case scenario, perhaps make better check based
      // on actual packed size, no there is not enough space
      //first zero out rest of the page
      vol.clear(pageOffset + currSize, pageOffset + pageSize);
      //allocate new page
      longStackNewPage(masterLinkOffset, pageOffset, value);
      return;
    }

    //there is enough space, so just write new value
    currSize += vol.putLongPackBidi(pageOffset + currSize, longParitySet(value));
    //and update master pointer
    headVol.putLong(masterLinkOffset, parity4Set(currSize << 48 | pageOffset));
  }



  @Override
  public <A> long put(A value, Serializer<A> serializer) {
    long recid;
    long[] offsets;
    DataIO.DataOutputByteArray out = serialize(value, serializer);
    boolean notalloc = out == null || out.pos == 0;
    try {
      structuralLock.lock();
      //Nan return new record id
      recid = freeRecidTake();
      offsets = notalloc ? null : freeDataTake(out.pos);
    } finally {
      structuralLock.unlock();
    }
    if (CC.ASSERT && offsets != null && (offsets[0] & MOFFSET) < PAGE_SIZE)
      throw new DBException.DataCorruption();

    int pos = lockPos(recid);
    Lock lock = locks[pos].writeLock();
    lock.lock();
    try {
      if (CC.ASSERT && vol.getLong(recidToOffset(recid)) != 0) {
        throw new AssertionError("Recid not empty: " + recid);
      }

      if (caches != null) {
        caches[pos].put(recid, value);
      }
      if (snapshotEnable) {
        for (Snapshot snap : snapshots) {
          snap.oldRecids[pos].putIfAbsent(recid, 0);
        }
      }
      putData(recid, offsets, out == null ? null : out.buf, out == null ? 0 : out.pos);
    } finally {
      lock.unlock();
    }

    return recid;
  }

  protected void putData(long recid, long[] offsets, byte[] src, int srcLen) {
    if (CC.ASSERT) {
      assertWriteLocked(lockPos(recid));
    }
    if (CC.ASSERT && offsetsTotalSize(offsets) != (src == null ? 0 : srcLen)) {
      throw new DBException.DataCorruption("size mismatch");
    }

    if (offsets != null) {
      int outPos = 0;
      for (int i = 0; i < offsets.length; i++) {
        final boolean last = (i == offsets.length - 1);
        if (CC.ASSERT && ((offsets[i] & MLINKED) == 0) != last)
          throw new DBException.DataCorruption("linked bit set wrong way");

        long offset = (offsets[i] & MOFFSET);
        if (CC.ASSERT && offset % 16 != 0)
          throw new DBException.DataCorruption("not aligned to 16");

        int plus = (last ? 0 : 8);
        int size = (int) ((offsets[i] >>> 48) - plus);
        if (CC.ASSERT && ((size & 0xFFFF) != size || size == 0))
          throw new DBException.DataCorruption("size mismatch");
        //write offset to next page
        if (!last) {
          putDataSingleWithLink(offset, parity3Set(offsets[i + 1]), src, outPos, size);
        } else {
          //System.out.println("put without link");
          putDataSingleWithoutLink(offset, src, outPos, size);
        }
        outPos += size;

      }
      if (CC.ASSERT && outPos != srcLen)
        throw new DBException.DataCorruption("size mismatch");
    }
    //update index val
    boolean firstLinked =
            (offsets != null && offsets.length > 1) || //too large record
                    (src == null); //null records
    boolean empty = offsets == null || offsets.length == 0;
    int firstSize = (int) (empty ? 0L : offsets[0] >>> 48);
    long firstOffset = empty ? 0L : offsets[0] & MOFFSET;
    indexValPut(recid, firstSize, firstOffset, firstLinked, false);
  }

  private void putDataSingleWithoutLink(long offset, byte[] buf, int bufPos, int size) {
    vol.putData(offset, buf, bufPos, size);
  }

  private void putDataSingleWithLink(long offset, long link, byte[] buf, int bufPos, int size) {
    vol.putLong(offset,link);
    vol.putData(offset + 8, buf, bufPos, size);
  }

  protected long freeRecidTake() {
    if (CC.ASSERT && !structuralLock.isHeldByCurrentThread()) {
      throw new AssertionError();
    }

    //try to reuse recid from free list
    long currentRecid = longStackTake(FREE_RECID_STACK);
    if (currentRecid != 0) {
      return currentRecid;
    }

    long maxRecIdOffset = parity1Get(headVol.getLong(MAX_RECID_OFFSET));
    maxRecIdOffset += indexValSize;
    headVol.putLong(MAX_RECID_OFFSET, parity1Set(maxRecIdOffset));

    long expectedRecId = maxRecIdOffset / indexValSize;
    //check if new index page has to be allocated
    if (recidTooLarge(expectedRecId)) {
      extendIndexPage();
    }
    return expectedRecId;
  }

  /**
   * take {@code size} bytes in the memory space assigned to partition {@code partitionId}
   *
   * @param size the size of the memory
   * @return the offsets of the memory
   */
  protected long[] freeDataTake(int size) {
    //TODO: take data from the volume
    if (CC.ASSERT && !structuralLock.isHeldByCurrentThread())
      throw new AssertionError();
    if (CC.ASSERT && size <= 0)
      throw new DBException.DataCorruption("size too small");
    long[] ret = EMPTY_LONGS;
    int requestBytes = size;
    while (requestBytes > MAX_REC_SIZE) {
      ret = Arrays.copyOf(ret, ret.length + 1);
      ret[ret.length - 1] = (((long) MAX_REC_SIZE) << 48) |
              freeDataTakeSingle(round16Up(MAX_REC_SIZE)) | MLINKED;
      requestBytes = requestBytes - MAX_REC_SIZE + 8;
    }
    //allocate last section
    ret = Arrays.copyOf(ret, ret.length + 1);
    ret[ret.length - 1] = (((long) requestBytes) << 48) | freeDataTakeSingle(round16Up(requestBytes));
    return ret;
  }

  /**
   * take bytes from the volume, to be times of 16 bytes
   *
   * @param size the number of bytes to take
   * @return offset of the data within the last page
   */
  protected long freeDataTakeSingle(int size) {
    if (CC.ASSERT && !structuralLock.isHeldByCurrentThread())
      throw new AssertionError();
    if (CC.ASSERT && size % 16 != 0)
      throw new DBException.DataCorruption("unalligned size");
    if(CC.ASSERT && size>round16Up(MAX_REC_SIZE))
      throw new DBException.DataCorruption("size too big");

    // really is size*8/16
    long masterPointerOffset = size / 2 + FREE_RECID_STACK;
    //System.out.println("masterPointerOffset:" + masterPointerOffset);
    // try to use pre-allocated but now released memory chunk
    // offset is multiple of 16, save some space
    long ret = longStackTake(masterPointerOffset) << 4;
    if (ret != 0) {
      if (CC.ASSERT && ret < PAGE_SIZE)
        throw new DBException.DataCorruption("wrong ret");
      if (CC.ASSERT && ret % 16 != 0)
        throw new DBException.DataCorruption("unalligned ret");
      return ret;
    }
    // need to allocate new memory chunk
    if (lastAllocatedData == 0) {
      //allocate new data page
      long page = pageAllocate(PAGE_SIZE);
      lastAllocatedData = page + size;
      if (CC.ASSERT && page < PAGE_SIZE)
        throw new DBException.DataCorruption("wrong page");
      if (CC.ASSERT && page % 16 != 0)
        throw new DBException.DataCorruption("unalligned page");
      return page;
    }

    //does record fit into rest of the page?
    if ((lastAllocatedData % PAGE_SIZE + size) / PAGE_SIZE != 0) {
      // does not fit in the current page
      // throw away rest of the page and allocate new
      lastAllocatedData = 0;
      return freeDataTakeSingle(size);
      //TODO i thing return! should be here, but not sure.

      //TODO it could be possible to recycle data here.
      // save pointers and put them into free list after new page was allocated.
    }
    //yes it fits here, increase pointer
    ret = lastAllocatedData;
    lastAllocatedData += size;

    if (CC.ASSERT && ret % 16 != 0)
      throw new DBException.DataCorruption();
    if (CC.ASSERT && lastAllocatedData % 16 != 0)
      throw new DBException.DataCorruption();
    if (CC.ASSERT && ret < PAGE_SIZE)
      throw new DBException.DataCorruption();

    return ret;
  }

  /**
   * get a memory chunk
   * @param masterLinkOffset the longstack top pointer
   * @return the offset of memory chunk, or 0 when there is not free memory chunk
   */
  private long longStackTake(long masterLinkOffset) {
    //TODO: return the next free RecID
    if (CC.ASSERT && !structuralLock.isHeldByCurrentThread()) {
      throw new AssertionError();
    }
    if (CC.ASSERT && (masterLinkOffset < FREE_RECID_STACK ||
            masterLinkOffset > FREE_RECID_STACK + round16Up(MAX_REC_SIZE) / 2 ||
            masterLinkOffset % 8 != 0)) {
      throw new DBException.DataCorruption("wrong master link");
    }
    long masterLinkVal = parity4Get(headVol.getLong(masterLinkOffset));
    if (masterLinkVal == 0) {
      return 0;
    }
    long totalSize = masterLinkVal >>> 48;
    final long pageOffsetOfChunk = masterLinkVal & MOFFSET;
    long lastPackedLongValue;
    try {
      lastPackedLongValue = vol.getLongPackBidiReverse(pageOffsetOfChunk + totalSize);
    } catch (DBException.DataCorruption e) {
      throw e;
    }
    long lastValueOffset = totalSize - (lastPackedLongValue >>> 56);
    if (CC.ASSERT && lastValueOffset < 8) {
      throw new DBException.DataCorruption();
    }
    vol.clear(pageOffsetOfChunk + lastValueOffset, pageOffsetOfChunk + totalSize);
    long lastUnPackedValue = longParityGet(lastPackedLongValue & DataIO.PACK_LONG_RESULT_MASK);
    //is there space left on current page?
    if (lastValueOffset > 8) {
      //yes, just update master link
      headVol.putLong(masterLinkOffset, parity4Set(lastValueOffset << 48 | pageOffsetOfChunk));
      return lastUnPackedValue;
    }

    //there is no space at current page, so delete the value in current page and
    // update master pointer
    /**
     * Structure of Long Stack Chunk is as follow:
     * byte 1-2 total size of this chunk.
     * byte 3-8 pointer to previous chunk in this long stack.
     * Parity 4, parity is shared with total size at byte 1-2.
     * rest of chunk is filled with bidi-packed longs with parity 1
     */
    long currentChunkMetadata = parity4Get(vol.getLong(pageOffsetOfChunk));
    final int currentChunkSize = (int) (currentChunkMetadata >>> 48);
    long prevPageOffset = currentChunkMetadata & MOFFSET;

    //does previous page exists?
    if (prevPageOffset != 0) {
      //yes previous page exists
      //find pointer to end of previous page
      // (data are packed with var size, traverse from end of page, until zeros
      //first read size of current page
      long totalChunkSize = parity4Get(vol.getLong(prevPageOffset)) >>> 48;
      //now read bytes from end of page, until they are zeros
      while (vol.getUnsignedByte(prevPageOffset + totalChunkSize - 1) == 0) {
        totalChunkSize--;
      }
      lastValueOffset = totalChunkSize;
      if (CC.ASSERT && totalSize < 10) {
        throw new DBException.DataCorruption();
      }
    } else {
      //no prev page does not exist
      lastValueOffset = 0;
    }

    //update master link with curr page size and offset
    headVol.putLong(masterLinkOffset, parity4Set(lastValueOffset << 48 | prevPageOffset));

    //release old page, size is stored as part of prev page value
    freeDataPut(pageOffsetOfChunk, currentChunkSize);

    return lastUnPackedValue;
  }

  private void extendIndexPage() {
    if (CC.ASSERT && !structuralLock.isHeldByCurrentThread()) {
      throw new AssertionError();
    }
    long indexPage = pageAllocate(PAGE_SIZE);
    //add link to previous page
    long nextPagePointerOffset = indexPages[indexPages.length - 1];
    //if zero page, put offset to end of page
    nextPagePointerOffset = Math.max(nextPagePointerOffset, HEAD_END);
    //first 8 bytes pointing to the next index page
    vol.putLong(nextPagePointerOffset, parity16Set(indexPage));
    //set zero link on next page
    vol.putLong(indexPage, parity16Set(0));
    //put into index page array
    long[] indexPages2 = Arrays.copyOf(indexPages, indexPages.length + 1);
    indexPages2[indexPages.length] = indexPage;
    indexPages = indexPages2;
  }

  private long pageAllocate(long pageSize) {
    if (CC.ASSERT && !structuralLock.isHeldByCurrentThread()) {
      throw new AssertionError();
    }

    long storeSize = parity16Get(headVol.getLong(STORE_SIZE));
    vol.ensureAvailable(storeSize + pageSize);
    vol.clear(storeSize, storeSize + pageSize);
    headVol.putLong(STORE_SIZE, parity16Set(storeSize + pageSize));

    if (CC.ASSERT && storeSize % pageSize != 0)
      throw new DBException.DataCorruption();

    return storeSize;
  }

  private long recIdToOffsetInFile(long recId, File file) throws IOException {
    RandomAccessFile raf = null;
    try {
      if (CC.ASSERT && recId <= 0)
        throw new DBException.DataCorruption("negative recid: " + recId);

      raf = new RandomAccessFile(file, "r");

      //4194304, recid: 126966, 4194320
      //convert recid to offset
      //Nan Zhu: 8 bytes are the pointer to the next page, HEAD_END is the header offset,
      //(recid - 1) counts the existing recids,
      long totalOffset = (recId - 1) * indexValSize + HEAD_END + 8;

      //Nan Zhu: convert the
      // Math.min(1, totalOffset/DATA_PAGE_SIZE) is to avoid using if condition
      // if recid is in the first index page, then Math.min(1, totalOffset/ DATA_PAGE_SIZE) is zero
      // otherwise, (totalOffset - DATA_PAGE_SIZE) / (DATA_PAGE_SIZE - 8) counts how many additional
      // (not including the first page) 8 bytes are needed for indicating to the next index page
      totalOffset += Math.min(1, totalOffset / PAGE_SIZE) *
              (8 + ((totalOffset - PAGE_SIZE) / (PAGE_SIZE - 8)) * 8);

      int indexPageNum = (int) (totalOffset / PAGE_SIZE);
      int indexPagePointer = 0;
      long indexPageOffset = 0;
      // find the right index Page
      while (indexPagePointer != indexPageNum) {
        raf.seek(indexPageOffset);
        indexPageOffset = raf.readLong();
        indexPagePointer++;
      }
      long objectOffset = 0;
      long indexValOffset = indexPageOffset + totalOffset % PAGE_SIZE;
      raf.seek(indexValOffset);
      long indexVal = DataIO.parity1Get(raf.readLong());
      //TODO: get objectOffset
      return objectOffset;
    } catch (FileNotFoundException e) {
      e.printStackTrace();
      LOG.warning("canot find file " + file.getAbsolutePath());
      return -1;
    } finally {
      if (raf != null) {
        raf.close();
      }
    }
  }

  private void fetchOutputByteArrayWithRecordID(
          long recId,
          DataOutputByteArray output) {
    long[] offsets = offsetsGet(indexValGet(recId));
    fillDataOutputByteArray(offsets, output);
  }

  @Override
  protected Store persist(String persistPath) {
    try {
      StoreAppend store = new StoreAppend(
              persistPath,
              Volume.RandomAccessFileVol.FACTORY,
              null,
              this.lockScale,
              0,
              false,
              false,
              null,
              false,
              false,
              true,
              null);
      store.init();
      //skip those freeId
      //try to reuse recid from free list
      HashSet<Long> freedIds = new HashSet<>();
      try {
        structuralLock.lock();
        long currentRecid = longStackTake(FREE_RECID_STACK);
        while (currentRecid != 0) {
          freedIds.add(currentRecid);
          currentRecid = longStackTake(FREE_RECID_STACK);
        }
      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        structuralLock.unlock();
      }

      long maxRecId = parity1Get(vol.getLong(MAX_RECID_OFFSET)) / indexValSize;
      store.extendSpace(maxRecId);
      System.out.println("starting persistence");
      DataOutputByteArray output = new DataOutputByteArray();
      for (long recId = startingRecId + 1; recId <= maxRecId; recId++) {
        if (freedIds.contains(recId)) {
          continue;
        }
        int pos = lockPos(recId);
        try {
          store.locks[pos].writeLock().lock();
          fetchOutputByteArrayWithRecordID(recId, output);
          store.insertOrUpdate(recId, output, true);
          output.resetByteArray();
        } finally {
          store.locks[pos].writeLock().unlock();
        }
      }
      store.sync();
      System.out.println("finished persistence");
      return store;
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }
  }
}
