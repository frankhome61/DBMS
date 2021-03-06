package edu.berkeley.cs186.database.query;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.common.BacktrackingIterator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.io.Page;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.RecordIterator;
import edu.berkeley.cs186.database.table.Schema;

public class SortMergeOperator extends JoinOperator {

  public SortMergeOperator(QueryOperator leftSource,
           QueryOperator rightSource,
           String leftColumnName,
           String rightColumnName,
           Database.Transaction transaction) throws QueryPlanException, DatabaseException {
    super(leftSource, rightSource, leftColumnName, rightColumnName, transaction, JoinType.SORTMERGE);

  }

  public Iterator<Record> iterator() throws QueryPlanException, DatabaseException {
    return new SortMergeOperator.SortMergeIterator();
  }

  /**
   * An implementation of Iterator that provides an iterator interface for this operator.
   *
   * Before proceeding, you should read and understand SNLJOperator.java
   *    You can find it in the same directory as this file.
   *
   * Word of advice: try to decompose the problem into distinguishable sub-problems.
   *    This means you'll probably want to add more methods than those given (Once again,
   *    SNLJOperator.java might be a useful reference).
   * 
   * Also, see discussion slides for week 7. 
   */
  private class SortMergeIterator extends JoinIterator {
    /** 
    * Some member variables are provided for guidance, but there are many possible solutions.
    * You should implement the solution that's best for you, using any member variables you need.
    * You're free to use these member variables, but you're not obligated to.
    */

    private BacktrackingIterator<Record> leftRecordIterator = null;
    private BacktrackingIterator<Record> rightRecordIterator = null;
    private Record leftRecord;
    private Record nextRecord;
    private Record rightRecord;
    private Record lastCheckedRecord;
    private Comparator<Record> leftRightCom;

    public SortMergeIterator() throws QueryPlanException, DatabaseException {
        super();
        SortOperator leftSort = new SortOperator(getTransaction(), getLeftTableName(), new LeftRecordComparator());
        SortOperator rightSort = new SortOperator(getTransaction(), getRightTableName(), new RightRecordComparator());
        this.leftRightCom = new LR_RecordComparator();
        this.leftRecordIterator = getRecordIterator(leftSort.sort());
        this.rightRecordIterator = getRecordIterator(rightSort.sort());

        this.nextRecord = null;

        this.leftRecord = leftRecordIterator.hasNext() ? leftRecordIterator.next() : null;
        this.rightRecord = rightRecordIterator.hasNext() ? rightRecordIterator.next() : null;

        if (rightRecord != null) {
            this.lastCheckedRecord = rightRecord;
            rightRecordIterator.mark();
        }
        else return;

        try {
            fetchNextRecord();
        } catch (DatabaseException e) {
            this.nextRecord = null;
        }
    }

      private void nextLeftRecord() throws DatabaseException {
          if (!leftRecordIterator.hasNext()) throw new DatabaseException("All Done!");
          leftRecord = leftRecordIterator.next();
      }

      private void resetRightRecord(){
          this.rightRecordIterator.reset();
          assert(rightRecordIterator.hasNext());
          rightRecord = rightRecordIterator.next();
          lastCheckedRecord = rightRecord;
          rightRecordIterator.mark();
      }

    private void fetchNextRecord() throws DatabaseException{
        if (this.leftRecord == null) throw new DatabaseException("No new record to fetch");
        this.nextRecord = null;
        do {
            if (rightRecord != null) {
                if (!rightRecord.equals(lastCheckedRecord)) {
                    nextLeftRecord();
                    resetRightRecord();
                }
                int compareRes = leftRightCom.compare(leftRecord, rightRecord);
                while (compareRes < 0) {
                    if (leftRecordIterator.hasNext()) {
                        leftRecord = leftRecordIterator.next();
                        compareRes = leftRightCom.compare(leftRecord, rightRecord);
                    } else {
                        throw new DatabaseException("All Done");
                    }
                }
                while (compareRes > 0) {
                    if (rightRecordIterator.hasNext()) {
                        rightRecord = rightRecordIterator.next();
                        lastCheckedRecord = rightRecord;
                        rightRecordIterator.mark();
                        compareRes = leftRightCom.compare(leftRecord, rightRecord);
                    }
                }
                if (compareRes == 0) {
                    List<DataBox> leftValues = new ArrayList<>(leftRecord.getValues());
                    List<DataBox> rightValues = new ArrayList<>(rightRecord.getValues());
                    leftValues.addAll(rightValues);
                    this.nextRecord = new Record(leftValues);
                }
                this.rightRecord = rightRecordIterator.hasNext() ? rightRecordIterator.next() : null;
            } else {
                nextLeftRecord();
                resetRightRecord();
            }
        } while (!hasNext());
    }

    /**
     * Checks if there are more record(s) to yield
     *
     * @return true if this iterator has another record to yield, otherwise false
     */
    public boolean hasNext() {
        return nextRecord != null;
    }

    /**
     * Yields the next record of this iterator.
     *
     * @return the next Record
     * @throws NoSuchElementException if there are no more Records to yield
     */
    public Record next() {
        if (!this.hasNext()) {
            throw new NoSuchElementException();
        }

        Record next = this.nextRecord;
        try {
            this.fetchNextRecord();
        } catch (DatabaseException e) {
            this.nextRecord = null;
        }
        return next;
    }

    public void remove() {
      throw new UnsupportedOperationException();
    }


    private class LeftRecordComparator implements Comparator<Record> {
      public int compare(Record o1, Record o2) {
        return o1.getValues().get(SortMergeOperator.this.getLeftColumnIndex()).compareTo(
                o2.getValues().get(SortMergeOperator.this.getLeftColumnIndex()));
      }
    }

    private class RightRecordComparator implements Comparator<Record> {
      public int compare(Record o1, Record o2) {
        return o1.getValues().get(SortMergeOperator.this.getRightColumnIndex()).compareTo(
                o2.getValues().get(SortMergeOperator.this.getRightColumnIndex()));
      }
    }

    /**
    * Left-Right Record comparator
    * o1 : leftRecord
    * o2: rightRecord
    */
    private class LR_RecordComparator implements Comparator<Record> {
      public int compare(Record o1, Record o2) {
        return o1.getValues().get(SortMergeOperator.this.getLeftColumnIndex()).compareTo(
                o2.getValues().get(SortMergeOperator.this.getRightColumnIndex()));
      }
    }
  }
}
