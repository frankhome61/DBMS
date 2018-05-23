package edu.berkeley.cs186.database.index;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Random;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;

import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.databox.IntDataBox;
import edu.berkeley.cs186.database.databox.Type;
import edu.berkeley.cs186.database.io.Page;
import edu.berkeley.cs186.database.io.PageAllocator;
import edu.berkeley.cs186.database.table.RecordId;

public class TestLeafNode {
    public static final String testFile = "TestLeafNode";

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    // 1 second max per method tested.
    @Rule
    public TestRule globalTimeout = new DisableOnDebug(Timeout.seconds(1));

    private static DataBox d0 = new IntDataBox(0);
    private static DataBox d1 = new IntDataBox(1);
    private static DataBox d2 = new IntDataBox(2);
    private static DataBox d3 = new IntDataBox(3);
    private static DataBox d4 = new IntDataBox(4);

    private static RecordId r0 = new RecordId(0, (short) 0);
    private static RecordId r1 = new RecordId(1, (short) 1);
    private static RecordId r2 = new RecordId(2, (short) 2);
    private static RecordId r3 = new RecordId(3, (short) 3);
    private static RecordId r4 = new RecordId(4, (short) 4);

    // Helpers /////////////////////////////////////////////////////////////////
    private BPlusTreeMetadata getBPlusTreeMetadata(Type keySchema, int order)
        throws IOException {
      File file = tempFolder.newFile(testFile);
      String path = file.getAbsolutePath();
      PageAllocator allocator = new PageAllocator(path, false);
      return new BPlusTreeMetadata(allocator, keySchema, order);
    }

    private LeafNode getEmptyLeaf(BPlusTreeMetadata meta,
                                  Optional<Integer> rightSibling) {
      List<DataBox> keys = new ArrayList<>();
      List<RecordId> rids = new ArrayList<>();
      return new LeafNode(meta, keys, rids, rightSibling);
    }

    // Tests ///////////////////////////////////////////////////////////////////
    @Test
    public void testGetL() throws IOException {
      BPlusTreeMetadata meta = getBPlusTreeMetadata(Type.intType(), 5);
      LeafNode leaf = getEmptyLeaf(meta, Optional.empty());
      for (int i = 0; i < 10; ++i) {
        assertEquals(leaf, leaf.get(new IntDataBox(i)));
      }
    }

    @Test
    public void testGetLeftmostLeafL() throws IOException {
      BPlusTreeMetadata meta = getBPlusTreeMetadata(Type.intType(), 5);
      LeafNode leaf = getEmptyLeaf(meta, Optional.empty());
      assertEquals(leaf, leaf.getLeftmostLeaf());
    }

    @Test
    public void testSmallBulkLoad() throws BPlusTreeException, IOException {
      // Bulk loads with 60% of a leaf's worth, then checks that the
      // leaf didn't split.
      int d = 5;
      float fillFactor = 0.8f;
      BPlusTreeMetadata meta = getBPlusTreeMetadata(Type.intType(), d);
      LeafNode leaf = getEmptyLeaf(meta, Optional.empty());

      List<Pair<DataBox, RecordId>> data = new ArrayList<>();
      for (int i = 0; i < (int) Math.ceil(1.5 * d * fillFactor); ++i) {
        DataBox key = new IntDataBox(i);
        RecordId rid = new RecordId(i, (short) i);
        data.add(i, new Pair<>(key, rid));
      }

      assertFalse(leaf.bulkLoad(data.iterator(), fillFactor).isPresent());

      Iterator<RecordId> iter = leaf.scanAll();
      Iterator<Pair<DataBox, RecordId>> expected = data.iterator();
      while (iter.hasNext() && expected.hasNext()) {
        assertEquals(expected.next().getSecond(), iter.next());
      }
      assertFalse(iter.hasNext());
      assertFalse(expected.hasNext());
    }

    @Test
    public void testLargeBulkLoad() throws BPlusTreeException, IOException {
        // Bulk loads with 60% of a leaf's worth, then checks that the
        // leaf didn't split.
        int d = 5;
        float fillFactor = 0.8f;
        BPlusTreeMetadata meta = getBPlusTreeMetadata(Type.intType(), d);
        LeafNode leaf = getEmptyLeaf(meta, Optional.empty());

        List<Pair<DataBox, RecordId>> data = new ArrayList<>();
        for (int i = 0; i < (int) Math.ceil(3 * d * fillFactor); ++i) {
            DataBox key = new IntDataBox(i);
            RecordId rid = new RecordId(i, (short) i);
            data.add(i, new Pair<>(key, rid));
        }


        Iterator<RecordId> iter = leaf.scanAll();
        Iterator<Pair<DataBox, RecordId>> expected = data.iterator();
        while (iter.hasNext() && expected.hasNext()) {
            assertEquals(expected.next().getSecond(), iter.next());
        }
        assertFalse(iter.hasNext());
        assertTrue(expected.hasNext()); // Leaf overflowed
        int size = leaf.getKeys().size();
        assertTrue(size / (2 * d + 1) < fillFactor);
    }

    @Test
    public void testNoOverflowPuts() throws BPlusTreeException, IOException {
      int d = 5;
      BPlusTreeMetadata meta = getBPlusTreeMetadata(Type.intType(), d);
      LeafNode leaf = getEmptyLeaf(meta, Optional.empty());

      for (int i = 0; i < 2 * d; ++i) {
        DataBox key = new IntDataBox(i);
        RecordId rid = new RecordId(i, (short) i);
        assertEquals(Optional.empty(), leaf.put(key, rid));

        for (int j = 0; j <= i; ++j) {
          key = new IntDataBox(j);
          rid = new RecordId(j, (short) j);
          assertEquals(Optional.of(rid), leaf.getKey(key));
        }
      }
    }

    @Test
    public void testNoOverflowPutsFromDisk()
        throws BPlusTreeException, IOException {
      int d = 5;
      BPlusTreeMetadata meta = getBPlusTreeMetadata(Type.intType(), d);
      LeafNode leaf = getEmptyLeaf(meta, Optional.empty());

      // Populate the leaf.
      for (int i = 0; i < 2 * d; ++i) {
        leaf.put(new IntDataBox(i), new RecordId(i, (short) i));
      }

      // Then read the leaf from disk.
      int pageNum = leaf.getPage().getPageNum();
      LeafNode fromDisk = LeafNode.fromBytes(meta, pageNum);

      // Check to see that we can read from disk.
      for (int i = 0; i < 2 * d; ++i) {
        IntDataBox key = new IntDataBox(i);
        RecordId rid = new RecordId(i, (short) i);
        assertEquals(Optional.of(rid), fromDisk.getKey(key));
      }
    }

    @Test(expected = BPlusTreeException.class)
    public void testDuplicatePut() throws BPlusTreeException, IOException {
      BPlusTreeMetadata meta = getBPlusTreeMetadata(Type.intType(), 4);
      LeafNode leaf = getEmptyLeaf(meta, Optional.empty());

      // The initial insert is fine.
      leaf.put(new IntDataBox(0), new RecordId(0, (short) 0));

      // The duplicate insert should raise an exception.
      leaf.put(new IntDataBox(0), new RecordId(0, (short) 0));
    }

    @Test
    public void testSimpleRemoves() throws BPlusTreeException, IOException {
      int d = 5;
      BPlusTreeMetadata meta = getBPlusTreeMetadata(Type.intType(), d);
      LeafNode leaf = getEmptyLeaf(meta, Optional.empty());

      // Insert entries.
      for (int i = 0; i < 2 * d; ++i) {
        IntDataBox key = new IntDataBox(i);
        RecordId rid = new RecordId(i, (short) i);
        leaf.put(key, rid);
        assertEquals(Optional.of(rid), leaf.getKey(key));
      }

      // Remove entries.
      for (int i = 0; i < 2 * d; ++i) {
        IntDataBox key = new IntDataBox(i);
        leaf.remove(key);
        assertEquals(Optional.empty(), leaf.getKey(key));
      }
    }

    @Test
    public void testScanAll() throws BPlusTreeException, IOException {
      int d = 5;
      BPlusTreeMetadata meta = getBPlusTreeMetadata(Type.intType(), d);
      LeafNode leaf = getEmptyLeaf(meta, Optional.empty());

      // Insert tuples in reverse order to make sure that scanAll is returning
      // things in sorted order.
      for (int i = 2 * d - 1; i >= 0; --i) {
        leaf.put(new IntDataBox(i), new RecordId(i, (short) i));
      }

      Iterator<RecordId> iter = leaf.scanAll();
      for (int i = 0; i < 2 * d; ++i) {
        assertTrue(iter.hasNext());
        assertEquals(new RecordId(i, (short) i), iter.next());
      }
      assertFalse(iter.hasNext());
    }

    @Test
    public void testScanGreaterEqual() throws BPlusTreeException, IOException {
      int d = 5;
      BPlusTreeMetadata meta = getBPlusTreeMetadata(Type.intType(), d);
      LeafNode leaf = getEmptyLeaf(meta, Optional.empty());

      // Insert tuples in reverse order to make sure that scanAll is returning
      // things in sorted order.
      for (int i = 2 * d - 1; i >= 0; --i) {
        leaf.put(new IntDataBox(i), new RecordId(i, (short) i));
      }

      Iterator<RecordId> iter = leaf.scanGreaterEqual(new IntDataBox(5));
      for (int i = 5; i < 2 * d; ++i) {
        assertTrue(iter.hasNext());
        assertEquals(new RecordId(i, (short) i), iter.next());
      }
      assertFalse(iter.hasNext());
    }

    @Test
    public void testMaxOrder() {
      // Note that this white box test depend critically on the implementation
      // of toBytes and includes a lot of magic numbers that won't make sense
      // unless you read toBytes.
      assertEquals(4, Type.intType().getSizeInBytes());
      assertEquals(6, RecordId.getSizeInBytes());
      for (int d = 0; d < 10; ++d) {
        int dd = d + 1;
        for (int i = 9 + (2*d) * (4+6); i < 9 + (2*dd) * (4+6); ++i) {
          assertEquals(d, LeafNode.maxOrder(i, Type.intType()));
        }
      }
    }

    @Test
    public void testToSexp() throws BPlusTreeException, IOException {
      int d = 2;
      BPlusTreeMetadata meta = getBPlusTreeMetadata(Type.intType(), d);
      LeafNode leaf = getEmptyLeaf(meta, Optional.empty());

      assertEquals("()", leaf.toSexp());
      leaf.put(new IntDataBox(4), new RecordId(4, (short) 4));
      assertEquals("((4 (4 4)))", leaf.toSexp());
      leaf.put(new IntDataBox(1), new RecordId(1, (short) 1));
      assertEquals("((1 (1 1)) (4 (4 4)))", leaf.toSexp());
      leaf.put(new IntDataBox(2), new RecordId(2, (short) 2));
      assertEquals("((1 (1 1)) (2 (2 2)) (4 (4 4)))", leaf.toSexp());
      leaf.put(new IntDataBox(3), new RecordId(3, (short) 3));
      assertEquals("((1 (1 1)) (2 (2 2)) (3 (3 3)) (4 (4 4)))", leaf.toSexp());
    }

    @Test
    public void testToAndFromBytes() throws BPlusTreeException, IOException {
      int d = 5;
      BPlusTreeMetadata meta = getBPlusTreeMetadata(Type.intType(), d);

      List<DataBox> keys = new ArrayList<>();
      List<RecordId> rids = new ArrayList<>();
      LeafNode leaf = new LeafNode(meta, keys, rids, Optional.of(42));
      int pageNum = leaf.getPage().getPageNum();

      assertEquals(leaf, LeafNode.fromBytes(meta, pageNum));

      for (int i = 0; i < 2 * d; ++i) {
        leaf.put(new IntDataBox(i), new RecordId(i, (short) i));
        assertEquals(leaf, LeafNode.fromBytes(meta, pageNum));
      }
    }

    @Test
    public void testPutDuplicate() throws BPlusTreeException, IOException {
        int d = 5;
        BPlusTreeMetadata meta = getBPlusTreeMetadata(Type.intType(), d);
        LeafNode leaf = getEmptyLeaf(meta, Optional.empty());

        DataBox key = new IntDataBox(1);
        RecordId rid = new RecordId(1, (short) 1);
        assertEquals(Optional.empty(), leaf.put(key, rid));
        boolean thrown = false;
        try {
            DataBox key2 = new IntDataBox(1);
            RecordId rid2 = new RecordId(1, (short) 1);
            leaf.put(key2, rid2);
        } catch (BPlusTreeException e) {
            thrown = true;
        }
        assertTrue(thrown);
    }
}
