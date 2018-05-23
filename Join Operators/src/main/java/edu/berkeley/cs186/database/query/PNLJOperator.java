package edu.berkeley.cs186.database.query;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.common.BacktrackingIterator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.io.Page;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;

public class PNLJOperator extends JoinOperator {

  public PNLJOperator(QueryOperator leftSource,
                      QueryOperator rightSource,
                      String leftColumnName,
                      String rightColumnName,
                      Database.Transaction transaction) throws QueryPlanException, DatabaseException {
    super(leftSource,
          rightSource,
          leftColumnName,
          rightColumnName,
          transaction,
          JoinType.PNLJ);

  }

  public Iterator<Record> iterator() throws QueryPlanException, DatabaseException {
    return new PNLJIterator();
  }


  /**
   * PNLJ: Page Nested Loop Join
   *  See lecture slides.
   *
   * An implementation of Iterator that provides an iterator interface for this operator.
   *
   * Before proceeding, you should read and understand SNLJOperator.java
   *    You can find it in the same directory as this file.
   *
   * Word of advice: try to decompose the problem into distinguishable sub-problems.
   *    This means you'll probably want to add more methods than those given (Once again,
   *    SNLJOperator.java might prove to be a useful reference).
   */
  private class PNLJIterator extends JoinIterator {
      /**
       * Some member variables are provided for guidance, but there are many possible solutions.
       * You should implement the solution that's best for you, using any member variables you need.
       * You're free to use these member variables, but you're not obligated to.
       */

      private BacktrackingIterator<Page> leftPageIterator = null;
      private BacktrackingIterator<Page> rightPageIterator = null;
      private BacktrackingIterator<Record> leftRecordIterator = null;
      private BacktrackingIterator<Record> rightRecordIterator = null;
      private Record leftRecord = null;
      private Record rightRecord = null;
      private Record nextRecord = null;

      public PNLJIterator() throws QueryPlanException, DatabaseException {
          super();
          this.rightPageIterator = PNLJOperator.this.getPageIterator(this.getRightTableName());
          this.leftPageIterator = PNLJOperator.this.getPageIterator(this.getLeftTableName());
          if (this.leftPageIterator.hasNext() && rightPageIterator.hasNext()) {
              Page leftHeader = this.leftPageIterator.next();
              Page rightHeader = this.rightPageIterator.next();
              this.leftRecordIterator = getBlockIterator(getLeftTableName(), leftPageIterator, 1);
              this.rightRecordIterator = getBlockIterator(getRightTableName(), rightPageIterator, 1);
              this.rightPageIterator.mark();// In order to reset right page
              this.leftRecord = leftRecordIterator.hasNext() ? leftRecordIterator.next() : null;
              this.rightRecord = rightRecordIterator.hasNext() ? rightRecordIterator.next() : null;
          }
          if (rightRecord != null) {
              rightRecordIterator.mark();
          } else {
              return;
          }

          if (leftRecord != null) {
              leftRecordIterator.mark();
          } else {
              return;
          }

          try {
              fetchNextRecord();
          } catch (DatabaseException e) {
              this.nextRecord = null;
          }
      }

      private void resetRightRecord(){
          this.rightRecordIterator.reset();
          assert(rightRecordIterator.hasNext());
          rightRecord = rightRecordIterator.next();
          rightRecordIterator.mark();
      }

      private void resetLeftRecord() {
          this.leftRecordIterator.reset();
          assert (leftRecordIterator.hasNext());
          leftRecord = leftRecordIterator.next();
          leftRecordIterator.mark();
      }

      private void resetRightPage() throws DatabaseException{

          this.rightPageIterator = PNLJOperator.this.getPageIterator(this.getRightTableName());
          this.rightPageIterator.next();
          if (rightPageIterator.hasNext()) {
              this.rightRecordIterator = getBlockIterator(getRightTableName(), rightPageIterator, 1);
              this.rightRecord = rightRecordIterator.hasNext() ? rightRecordIterator.next() : null;
              this.rightRecordIterator.mark();
          }
          if (rightRecord != null) {
              rightRecordIterator.mark();
          } else {
              return;
          }
      }

      private void nextLeftRecord() throws DatabaseException {
          if (!leftRecordIterator.hasNext()) throw new DatabaseException("All Done!");
          leftRecord = leftRecordIterator.next();
      }

      private void nextRightRecord() throws DatabaseException {
          if (!rightRecordIterator.hasNext()) throw new DatabaseException("All Done!");
          rightRecord = rightRecordIterator.next();
      }

      private void fetchNextRecord() throws DatabaseException {
          if (this.leftRecord == null) throw new DatabaseException("No new record to fetch");
          this.nextRecord = null;
          do {
              if (this.rightRecord != null) {
                  DataBox leftJoinValue = this.leftRecord.getValues().get(PNLJOperator.this.getLeftColumnIndex());
                  DataBox rightJoinValue = this.rightRecord.getValues().get(PNLJOperator.this.getRightColumnIndex());
                  if (leftJoinValue.equals(rightJoinValue)) {
                      List<DataBox> leftValues = new ArrayList<>(this.leftRecord.getValues());
                      List<DataBox> rightValues = new ArrayList<>(this.rightRecord.getValues());
                      leftValues.addAll(rightValues);
                      this.nextRecord = new Record(leftValues);
                  }
                  this.rightRecord = rightRecordIterator.hasNext() ? rightRecordIterator.next() : null;
              } else {
                  if (leftRecordIterator.hasNext()) {
                      nextLeftRecord();
                      resetRightRecord();
                  } else {
                      // leftRecordIterator doesn't have next
                      if (rightPageIterator.hasNext()) {
                          // We still have more right page
                          this.rightRecordIterator = getBlockIterator(getRightTableName(), rightPageIterator, 1);
                          nextRightRecord();
                          this.rightRecordIterator.mark();
                          resetLeftRecord();
                      } else {
                          // Right page run out
                          if (leftPageIterator.hasNext()) {
                              this.leftRecordIterator = getBlockIterator(getLeftTableName(), leftPageIterator, 1);
                              nextLeftRecord();
                              leftRecordIterator.mark();
                              resetRightPage();
                          } else return;
                      }
                  }
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
  }
}
