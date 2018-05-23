package edu.berkeley.cs186.database.index;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.*;

import org.junit.Before;
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
import edu.berkeley.cs186.database.table.RecordId;

import javax.xml.crypto.Data;

public class TestBPlusTree {
  public static final String filename = "TestBPlusTree";
  private File file;

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  // 10 seconds max per method tested.
  @Rule
  public TestRule globalTimeout = new DisableOnDebug(Timeout.seconds(10));

  // Helpers /////////////////////////////////////////////////////////////////
  @Before
  public void initFile() throws IOException {
    this.file = tempFolder.newFile(filename);
  }

  private BPlusTree getBPlusTree(Type keySchema, int order)
          throws BPlusTreeException, IOException {
    return new BPlusTree(file.getAbsolutePath(), keySchema, order);
  }

  private static <T> List<T> iteratorToList(Iterator<T> iter) {
    List<T> xs = new ArrayList<>();
    while (iter.hasNext()) {
      xs.add(iter.next());
    }
    return xs;
  }

  // Tests ///////////////////////////////////////////////////////////////////
  @Test
  public void testSimpleBulkLoad() throws BPlusTreeException, IOException {
    BPlusTree tree = getBPlusTree(Type.intType(), 2);
    float fillFactor = 0.75f;
    assertEquals("()", tree.toSexp());

    List<Pair<DataBox, RecordId>> data = new ArrayList<>();
    for (int i = 1; i <= 11; ++i) {
      data.add(new Pair<>(new IntDataBox(i), new RecordId(i, (short) i)));
    }

    tree.bulkLoad(data.iterator(), fillFactor);
    //      (    4        7         10        _   )
    //       /       |         |         \
    // (1 2 3 _) (4 5 6 _) (7 8 9 _) (10 11 _ _)
    String leaf0 = "((1 (1 1)) (2 (2 2)) (3 (3 3)))";
    String leaf1 = "((4 (4 4)) (5 (5 5)) (6 (6 6)))";
    String leaf2 = "((7 (7 7)) (8 (8 8)) (9 (9 9)))";
    String leaf3 = "((10 (10 10)) (11 (11 11)))";
    String sexp = String.format("(%s 4 %s 7 %s 10 %s)", leaf0, leaf1, leaf2, leaf3);
    assertEquals(sexp, tree.toSexp());
  }

  @Test
  public void midtermBulkLoad() throws BPlusTreeException, IOException {
    BPlusTree tree = getBPlusTree(Type.intType(), 1);
    float fillFactor = 0.5f;
    assertEquals("()", tree.toSexp());

    List<Pair<DataBox, RecordId>> data = new ArrayList<>();
    data.add(new Pair<>(new IntDataBox(3), new RecordId(3, (short) 3)));
    data.add(new Pair<>(new IntDataBox(5), new RecordId(5, (short) 5)));
    data.add(new Pair<>(new IntDataBox(7), new RecordId(7, (short) 7)));
    data.add(new Pair<>(new IntDataBox(8), new RecordId(8, (short) 8)));
    data.add(new Pair<>(new IntDataBox(11), new RecordId(11, (short) 11)));
    data.add(new Pair<>(new IntDataBox(19), new RecordId(19, (short) 19)));
    data.add(new Pair<>(new IntDataBox(20), new RecordId(20, (short) 20)));
    tree.bulkLoad(data.iterator(), fillFactor);
    tree.toDotPDFFile("midterm.pdf");
  }

  @Test (expected = BPlusTreeException.class)
  public void testNonEmptyBulkLoad() throws BPlusTreeException, IOException {
    BPlusTree tree = getBPlusTree(Type.intType(), 2);
    float fillFactor = 0.75f;
    assertEquals("()", tree.toSexp());

    List<Pair<DataBox, RecordId>> data = new ArrayList<>();
    List<Pair<DataBox, RecordId>> data2 = new ArrayList<>();
    for (int i = 1; i <= 11; ++i) {
      data.add(new Pair<>(new IntDataBox(i), new RecordId(i, (short) i)));
      data.add(new Pair<>(new IntDataBox(i + 10), new RecordId(i + 10, (short) (i + 10))));
    }
    tree.bulkLoad(data.iterator(), fillFactor);
    tree.bulkLoad(data2.iterator(), fillFactor);
  }

  // HIDDEN
  @Test
  public void testPartiallyEmptyScans()
          throws BPlusTreeException, IOException {
    // Create and then empty part of the tree.
    BPlusTree tree = getBPlusTree(Type.intType(), 2);
    for (int i = 0; i < 100; ++i) {
      tree.put(new IntDataBox(i), new RecordId(i, (short) i));
    }
    for (int i = 25; i < 75; ++i) {
      tree.remove(new IntDataBox(i));
    }

    // Scan over the tree.
    Iterator<RecordId> actual = tree.scanAll();
    List<RecordId> expected = new ArrayList<>();
    for (int i = 0; i < 25; ++i) {
      expected.add(new RecordId(i, (short) i));
    }
    for (int i = 75; i < 100; ++i) {
      expected.add(new RecordId(i, (short) i));
    }
    assertEquals(expected, iteratorToList(actual));

    actual = tree.scanGreaterEqual(new IntDataBox(42));
    expected = new ArrayList<>();
    for (int i = 75; i < 100; ++i) {
      expected.add(new RecordId(i, (short) i));
    }
    assertEquals(expected, iteratorToList(actual));

    actual = tree.scanGreaterEqual(new IntDataBox(99));
    expected = new ArrayList<>();
    expected.add(new RecordId(99, (short) 99));
    assertEquals(expected, iteratorToList(actual));
  }

    @Test (expected = BPlusTreeException.class)
    public void testNonemptyRemoveBulkLoad() throws BPlusTreeException, IOException {
        BPlusTree tree = getBPlusTree(Type.intType(), 2);
        float fillFactor = 0.75f;
        assertEquals("()", tree.toSexp());

        List<Pair<DataBox, RecordId>> data = new ArrayList<>();
        List<Pair<DataBox, RecordId>> data2 = new ArrayList<>();
        for (int i = 1; i <= 11; ++i) {
            data.add(new Pair<>(new IntDataBox(i), new RecordId(i, (short) i)));
            data2.add(new Pair<>(new IntDataBox(i + 10), new RecordId(i + 10, (short) (i + 10))));
        }
        tree.bulkLoad(data.iterator(), fillFactor);
        for (int i = 0; i <= 10; i ++) {
            tree.remove(data.get(i).getFirst());
        }
        Iterator<RecordId> iter = tree.scanAll();
        assertFalse(iter.hasNext());
        tree.bulkLoad(data2.iterator(), fillFactor);
    }

  @Test
  public void testWhiteBoxTest() throws BPlusTreeException, IOException {
    BPlusTree tree = getBPlusTree(Type.intType(), 1);
    assertEquals("()", tree.toSexp());

    // (4)
    tree.put(new IntDataBox(4), new RecordId(4, (short) 4));
    assertEquals("((4 (4 4)))", tree.toSexp());

    // (4 9)
    tree.put(new IntDataBox(9), new RecordId(9, (short) 9));
    assertEquals("((4 (4 4)) (9 (9 9)))", tree.toSexp());

    //   (6)
    //  /   \
    // (4) (6 9)
    tree.put(new IntDataBox(6), new RecordId(6, (short) 6));
    String l = "((4 (4 4)))";
    String r = "((6 (6 6)) (9 (9 9)))";
    assertEquals(String.format("(%s 6 %s)", l, r), tree.toSexp());

    //     (6)
    //    /   \
    // (2 4) (6 9)
    tree.put(new IntDataBox(2), new RecordId(2, (short) 2));
    l = "((2 (2 2)) (4 (4 4)))";
    r = "((6 (6 6)) (9 (9 9)))";
    assertEquals(String.format("(%s 6 %s)", l, r), tree.toSexp());

    //      (6 7)
    //     /  |  \
    // (2 4) (6) (7 9)
    tree.put(new IntDataBox(7), new RecordId(7, (short) 7));
    l = "((2 (2 2)) (4 (4 4)))";
    String m = "((6 (6 6)))";
    r = "((7 (7 7)) (9 (9 9)))";
    assertEquals(String.format("(%s 6 %s 7 %s)", l, m, r), tree.toSexp());

    //         (7)
    //        /   \
    //     (6)     (8)
    //    /   \   /   \
    // (2 4) (6) (7) (8 9)
    tree.put(new IntDataBox(8), new RecordId(8, (short) 8));
    String ll = "((2 (2 2)) (4 (4 4)))";
    String lr = "((6 (6 6)))";
    String rl = "((7 (7 7)))";
    String rr = "((8 (8 8)) (9 (9 9)))";
    l = String.format("(%s 6 %s)", ll, lr);
    r = String.format("(%s 8 %s)", rl, rr);
    assertEquals(String.format("(%s 7 %s)", l, r), tree.toSexp());

    //            (7)
    //           /   \
    //     (3 6)       (8)
    //   /   |   \    /   \
    // (2) (3 4) (6) (7) (8 9)
    tree.put(new IntDataBox(3), new RecordId(3, (short) 3));
    ll = "((2 (2 2)))";
    String lm = "((3 (3 3)) (4 (4 4)))";
    lr = "((6 (6 6)))";
    rl = "((7 (7 7)))";
    rr = "((8 (8 8)) (9 (9 9)))";
    l = String.format("(%s 3 %s 6 %s)", ll, lm, lr);
    r = String.format("(%s 8 %s)", rl, rr);
    assertEquals(String.format("(%s 7 %s)", l, r), tree.toSexp());

    //            (4 7)
    //           /  |  \
    //   (3)      (6)       (8)
    //  /   \    /   \    /   \
    // (2) (3) (4 5) (6) (7) (8 9)
    tree.put(new IntDataBox(5), new RecordId(5, (short) 5));
    ll = "((2 (2 2)))";
    lr = "((3 (3 3)))";
    String ml = "((4 (4 4)) (5 (5 5)))";
    String mr = "((6 (6 6)))";
    rl = "((7 (7 7)))";
    rr = "((8 (8 8)) (9 (9 9)))";
    l = String.format("(%s 3 %s)", ll, lr);
    m = String.format("(%s 6 %s)", ml, mr);
    r = String.format("(%s 8 %s)", rl, rr);
    assertEquals(String.format("(%s 4 %s 7 %s)", l, m, r), tree.toSexp());

    //            (4 7)
    //           /  |  \
    //    (3)      (6)       (8)
    //   /   \    /   \    /   \
    // (1 2) (3) (4 5) (6) (7) (8 9)
    tree.put(new IntDataBox(1), new RecordId(1, (short) 1));
    ll = "((1 (1 1)) (2 (2 2)))";
    lr = "((3 (3 3)))";
    ml = "((4 (4 4)) (5 (5 5)))";
    mr = "((6 (6 6)))";
    rl = "((7 (7 7)))";
    rr = "((8 (8 8)) (9 (9 9)))";
    l = String.format("(%s 3 %s)", ll, lr);
    m = String.format("(%s 6 %s)", ml, mr);
    r = String.format("(%s 8 %s)", rl, rr);
    assertEquals(String.format("(%s 4 %s 7 %s)", l, m, r), tree.toSexp());

    //            (4 7)
    //           /  |  \
    //    (3)      (6)       (8)
    //   /   \    /   \    /   \
    // (  2) (3) (4 5) (6) (7) (8 9)
    tree.remove(new IntDataBox(1));
    ll = "((2 (2 2)))";
    lr = "((3 (3 3)))";
    ml = "((4 (4 4)) (5 (5 5)))";
    mr = "((6 (6 6)))";
    rl = "((7 (7 7)))";
    rr = "((8 (8 8)) (9 (9 9)))";
    l = String.format("(%s 3 %s)", ll, lr);
    m = String.format("(%s 6 %s)", ml, mr);
    r = String.format("(%s 8 %s)", rl, rr);
    assertEquals(String.format("(%s 4 %s 7 %s)", l, m, r), tree.toSexp());

    //            (4 7)
    //           /  |  \
    //    (3)      (6)       (8)
    //   /   \    /   \    /   \
    // (  2) (3) (4 5) (6) (7) (8  )
    tree.remove(new IntDataBox(9));
    ll = "((2 (2 2)))";
    lr = "((3 (3 3)))";
    ml = "((4 (4 4)) (5 (5 5)))";
    mr = "((6 (6 6)))";
    rl = "((7 (7 7)))";
    rr = "((8 (8 8)))";
    l = String.format("(%s 3 %s)", ll, lr);
    m = String.format("(%s 6 %s)", ml, mr);
    r = String.format("(%s 8 %s)", rl, rr);
    assertEquals(String.format("(%s 4 %s 7 %s)", l, m, r), tree.toSexp());

    //            (4 7)
    //           /  |  \
    //    (3)      (6)       (8)
    //   /   \    /   \    /   \
    // (  2) (3) (4 5) ( ) (7) (8  )
    tree.remove(new IntDataBox(6));
    ll = "((2 (2 2)))";
    lr = "((3 (3 3)))";
    ml = "((4 (4 4)) (5 (5 5)))";
    mr = "()";
    rl = "((7 (7 7)))";
    rr = "((8 (8 8)))";
    l = String.format("(%s 3 %s)", ll, lr);
    m = String.format("(%s 6 %s)", ml, mr);
    r = String.format("(%s 8 %s)", rl, rr);
    assertEquals(String.format("(%s 4 %s 7 %s)", l, m, r), tree.toSexp());

    //            (4 7)
    //           /  |  \
    //    (3)      (6)       (8)
    //   /   \    /   \    /   \
    // (  2) (3) (  5) ( ) (7) (8  )
    tree.remove(new IntDataBox(4));
    ll = "((2 (2 2)))";
    lr = "((3 (3 3)))";
    ml = "((5 (5 5)))";
    mr = "()";
    rl = "((7 (7 7)))";
    rr = "((8 (8 8)))";
    l = String.format("(%s 3 %s)", ll, lr);
    m = String.format("(%s 6 %s)", ml, mr);
    r = String.format("(%s 8 %s)", rl, rr);
    assertEquals(String.format("(%s 4 %s 7 %s)", l, m, r), tree.toSexp());

    //            (4 7)
    //           /  |  \
    //    (3)      (6)       (8)
    //   /   \    /   \    /   \
    // (   ) (3) (  5) ( ) (7) (8  )
    tree.remove(new IntDataBox(2));
    ll = "()";
    lr = "((3 (3 3)))";
    ml = "((5 (5 5)))";
    mr = "()";
    rl = "((7 (7 7)))";
    rr = "((8 (8 8)))";
    l = String.format("(%s 3 %s)", ll, lr);
    m = String.format("(%s 6 %s)", ml, mr);
    r = String.format("(%s 8 %s)", rl, rr);
    assertEquals(String.format("(%s 4 %s 7 %s)", l, m, r), tree.toSexp());

    //            (4 7)
    //           /  |  \
    //    (3)      (6)       (8)
    //   /   \    /   \    /   \
    // (   ) (3) (   ) ( ) (7) (8  )
    tree.remove(new IntDataBox(5));
    ll = "()";
    lr = "((3 (3 3)))";
    ml = "()";
    mr = "()";
    rl = "((7 (7 7)))";
    rr = "((8 (8 8)))";
    l = String.format("(%s 3 %s)", ll, lr);
    m = String.format("(%s 6 %s)", ml, mr);
    r = String.format("(%s 8 %s)", rl, rr);
    assertEquals(String.format("(%s 4 %s 7 %s)", l, m, r), tree.toSexp());

    //            (4 7)
    //           /  |  \
    //    (3)      (6)       (8)
    //   /   \    /   \    /   \
    // (   ) (3) (   ) ( ) ( ) (8  )
    tree.remove(new IntDataBox(7));
    ll = "()";
    lr = "((3 (3 3)))";
    ml = "()";
    mr = "()";
    rl = "()";
    rr = "((8 (8 8)))";
    l = String.format("(%s 3 %s)", ll, lr);
    m = String.format("(%s 6 %s)", ml, mr);
    r = String.format("(%s 8 %s)", rl, rr);
    assertEquals(String.format("(%s 4 %s 7 %s)", l, m, r), tree.toSexp());

    //            (4 7)
    //           /  |  \
    //    (3)      (6)       (8)
    //   /   \    /   \    /   \
    // (   ) ( ) (   ) ( ) ( ) (8  )
    tree.remove(new IntDataBox(3));
    ll = "()";
    lr = "()";
    ml = "()";
    mr = "()";
    rl = "()";
    rr = "((8 (8 8)))";
    l = String.format("(%s 3 %s)", ll, lr);
    m = String.format("(%s 6 %s)", ml, mr);
    r = String.format("(%s 8 %s)", rl, rr);
    assertEquals(String.format("(%s 4 %s 7 %s)", l, m, r), tree.toSexp());

    //            (4 7)
    //           /  |  \
    //    (3)      (6)       (8)
    //   /   \    /   \    /   \
    // (   ) ( ) (   ) ( ) ( ) (   )
    tree.remove(new IntDataBox(8));
    ll = "()";
    lr = "()";
    ml = "()";
    mr = "()";
    rl = "()";
    rr = "()";
    l = String.format("(%s 3 %s)", ll, lr);
    m = String.format("(%s 6 %s)", ml, mr);
    r = String.format("(%s 8 %s)", rl, rr);
    assertEquals(String.format("(%s 4 %s 7 %s)", l, m, r), tree.toSexp());
  }

  @Test
  public void testRandomPuts() throws BPlusTreeException, IOException {
    List<DataBox> keys = new ArrayList<>();
    List<RecordId> rids = new ArrayList<>();
    List<RecordId> sortedRids = new ArrayList<>();
    for (int i = 0; i < 1000; ++i) {
      keys.add(new IntDataBox(i));
      rids.add(new RecordId(i, (short) i));
      sortedRids.add(new RecordId(i, (short) i));
    }

    // Try trees with different orders.
    for (int d = 2; d < 5; ++d) {
      // Try trees with different insertion orders.
      for (int n = 0; n < 2; ++n) {
        Collections.shuffle(keys, new Random(42));
        Collections.shuffle(rids, new Random(42));

        // Insert all the keys.
        BPlusTree tree = getBPlusTree(Type.intType(), d);
        for (int i = 0; i < keys.size(); ++i) {
          tree.put(keys.get(i), rids.get(i));
        }

        // Test get.
        for (int i = 0; i < keys.size(); ++i) {
          assertEquals(Optional.of(rids.get(i)), tree.get(keys.get(i)));
        }

        // Test scanAll.
        assertEquals(sortedRids, iteratorToList(tree.scanAll()));

        // Test scanGreaterEqual.
        for (int i = 0; i < keys.size(); i += 100) {
          Iterator<RecordId> actual = tree.scanGreaterEqual(new IntDataBox(i));
          List<RecordId> expected = sortedRids.subList(i, sortedRids.size());
          assertEquals(expected, iteratorToList(actual));
        }

        // Load the tree from disk.
        BPlusTree fromDisk = new BPlusTree(file.getAbsolutePath());
        assertEquals(sortedRids, iteratorToList(fromDisk.scanAll()));

        // Test remove.
        Collections.shuffle(keys, new Random(42));
        Collections.shuffle(rids, new Random(42));
        for (DataBox key : keys) {
          fromDisk.remove(key);
          assertEquals(Optional.empty(), fromDisk.get(key));
        }
      }
    }
  }

  @Test
  public void testMaxOrder() {
    // Note that this white box test depend critically on the implementation
    // of toBytes and includes a lot of magic numbers that won't make sense
    // unless you read toBytes.
    assertEquals(4, Type.intType().getSizeInBytes());
    assertEquals(6, RecordId.getSizeInBytes());
    int pageSizeInBytes = 100;
    Type keySchema = Type.intType();
    assertEquals(4, LeafNode.maxOrder(pageSizeInBytes, keySchema));
    assertEquals(5, InnerNode.maxOrder(pageSizeInBytes, keySchema));
    assertEquals(4, BPlusTree.maxOrder(pageSizeInBytes, keySchema));
  }

  @Test
  public void testTreeIterator() throws BPlusTreeException, IOException {
    BPlusTree tree = getBPlusTree(Type.intType(), 2);
    float fillFactor = 0.75f;
    assertEquals("()", tree.toSexp());

    List<Pair<DataBox, RecordId>> data = new ArrayList<>();
    for (int i = 1; i <= 11; ++i) {
      data.add(new Pair<>(new IntDataBox(i), new RecordId(i, (short) i)));
    }

    tree.bulkLoad(data.iterator(), fillFactor);
    //      (    4        7         10        _   )
    //       /       |         |         \
    // (1 2 3 _) (4 5 6 _) (7 8 9 _) (10 11 _ _)
    Iterator<RecordId> iter = tree.scanAll();
    RecordId next = iter.next();
    assertEquals(new RecordId(1, (short) 1), next);
    next = iter.next();
    assertEquals(new RecordId(2, (short) 2), next);
    next = iter.next();
    assertEquals(new RecordId(3, (short) 3), next);
    next = iter.next();
    assertEquals(new RecordId(4, (short) 4), next);
    next = iter.next();
    assertEquals(new RecordId(5, (short) 5), next);
    next = iter.next();
    assertEquals(new RecordId(6, (short) 6), next);
    next = iter.next();
    assertEquals(new RecordId(7, (short) 7), next);
    next = iter.next();
    assertEquals(new RecordId(8, (short) 8), next);
    next = iter.next();
    assertEquals(new RecordId(9, (short) 9), next);
    next = iter.next();
    assertEquals(new RecordId(10, (short) 10), next);
    next = iter.next();
    assertEquals(new RecordId(11, (short) 11), next);
    assertFalse(iter.hasNext());
    boolean thrown = false;
    try {
      iter.next();
    } catch (NoSuchElementException e) {
      thrown = true;
    }
    assertTrue(thrown);
  }

  @Test
  public void testTreeIteratorFromNode() throws BPlusTreeException, IOException {
    BPlusTree tree = getBPlusTree(Type.intType(), 2);
    float fillFactor = 0.75f;
    assertEquals("()", tree.toSexp());

    List<Pair<DataBox, RecordId>> data = new ArrayList<>();
    for (int i = 1; i <= 11; ++i) {
      data.add(new Pair<>(new IntDataBox(i), new RecordId(i, (short) i)));
    }

    tree.bulkLoad(data.iterator(), fillFactor);
    DataBox key = data.get(2).getFirst();
    Iterator<RecordId> iter = tree.scanGreaterEqual(key);
    RecordId next = iter.next();
    assertEquals(new RecordId(3, (short) 3), next);
  }

  @Test
  public void testScanAll() throws BPlusTreeException, IOException{
    BPlusTree tree = getBPlusTree(Type.intType(), 2);
    float fillFactor = 0.75f;
    assertEquals("()", tree.toSexp());

    List<Pair<DataBox, RecordId>> data = new ArrayList<>();
    for (int i = 1; i <= 20; ++i) {
      data.add(new Pair<>(new IntDataBox(i), new RecordId(i, (short) i)));
    }

    tree.bulkLoad(data.iterator(), fillFactor);
    Iterator<RecordId> iter = tree.scanAll();
    for (int i = 1; iter.hasNext(); i++) {
      RecordId next = iter.next();
      assertEquals(new RecordId(i, (short) i), next);
    }
    boolean thrown = false;
    try {
      iter.next();
    } catch (NoSuchElementException e) {
      thrown = true;
    }
    assertTrue(thrown);
  }

  @Test
  public void testScanGreaterEqual() throws BPlusTreeException, IOException{
    BPlusTree tree = getBPlusTree(Type.intType(), 2);
    float fillFactor = 0.75f;
    assertEquals("()", tree.toSexp());

    List<Pair<DataBox, RecordId>> data = new ArrayList<>();
    for (int i = 1; i <= 20; ++i) {
      data.add(new Pair<>(new IntDataBox(i), new RecordId(i, (short) i)));
    }

    tree.bulkLoad(data.iterator(), fillFactor);
    DataBox key = new IntDataBox(3);
    Iterator<RecordId> iter = tree.scanGreaterEqual(key);

    for (int i = 3; iter.hasNext(); i++) {
      RecordId next = iter.next();
      assertEquals(new RecordId(i, (short) i), next);
    }
    boolean thrown = false;
    try {
      iter.next();
    } catch (NoSuchElementException e) {
      thrown = true;
    }
    assertTrue(thrown);
  }


}