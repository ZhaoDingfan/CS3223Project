package qp.operators;

import qp.optimizer.BufferManager;
import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Block;
import qp.utils.Tuple;

import java.io.*;
import java.util.ArrayList;
import java.util.Vector;

public class SortMergeJoin extends Join {
    int batchsize;  //Number of tuples per out batch

    int leftindex;     // Index of the join attribute in left table
    int rightindex;    // Index of the join attribute in right table
    Attribute leftattr;
    Attribute rightattr;

    int leftBatchSize;
    int rightBatchSize;

    Batch outbatch;   // Output buffer
    Batch leftbatch;  // Buffer for left input stream
    Batch rightbatch;  // Buffer for right input stream
    int numBuff;

    SortMerge sortedLeft;
    SortMerge sortedRight;

    Tuple leftCurr;
    Tuple rightCurr;
    Vector<Tuple> leftVector=new Vector();
    Vector<Tuple> rightVector=new Vector();

    public SortMergeJoin(Join jn) {
        super(jn.getLeft(), jn.getRight(), jn.getCondition(), jn.getOpType());
        schema = jn.getSchema();
        jointype = jn.getJoinType();
        numBuff = jn.getNumBuff();
    }


    /** During open finds the index of the join attributes
     **  Materializes the right hand side into a file
     **  Opens the connections
     **/

    @Override
    public boolean open() {

        // select number of tuples per batch and number of batches per block
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;

        leftattr = con.getLhs();
        Vector<Attribute> leftSet = new Vector<>();
        leftSet.add(leftattr);

        rightattr = (Attribute) con.getRhs();
        Vector<Attribute> rightSet = new Vector<>();
        rightSet.add(rightattr);

        leftindex = left.getSchema().indexOf(leftattr);
        int leftTupleSize = left.getSchema().getTupleSize();
        leftBatchSize = Batch.getPageSize() / leftTupleSize;
        System.out.println("left tuple size: " + leftTupleSize);

        rightindex = right.getSchema().indexOf(rightattr);
        int rightTupleSize = right.getSchema().getTupleSize();
        rightBatchSize = Batch.getPageSize() / rightTupleSize;

        System.out.println("=====left schema====");
        Debug.PPrint(left.getSchema());
        System.out.println();
        System.out.println("index: " + leftindex);
        System.out.println("attr: " + leftattr.getTabName() + " " + leftattr.getColName());

        System.out.println("=====right schema====");
        Debug.PPrint(right.getSchema());
        System.out.println();
        System.out.println("index: " + rightindex);
        System.out.println("attr: " + rightattr.getTabName() + " " + rightattr.getColName());

        sortedLeft = new SortMerge(left, leftSet, BufferManager.getBuffersPerJoin(), "left");
//        System.out.println(left.getSchema().getAttList());

        sortedRight = new SortMerge(right, rightSet,BufferManager.getBuffersPerJoin(), "right");
//        System.out.println(right.getSchema().getAttList());


        if(!sortedLeft.open() || !sortedRight.open()) {
            return false;
        }
        leftbatch=sortedLeft.next();
        rightbatch=sortedRight.next();
        outbatch=new Batch(batchsize);

        if (leftbatch!=null&&rightbatch!=null){
            leftCurr=leftbatch.remove(0);
            rightCurr=rightbatch.remove(0);
            rightVector.add(rightCurr);
        }
        return true;
    }

    public Batch next(){
        outbatch=new Batch(batchsize);

        while(sortedLeft!=null&sortedRight!=null){
            while(Tuple.compareTuples(leftCurr,rightCurr,leftindex,rightindex)<=0){
                for (Tuple i:rightVector){
                    if (Tuple.compareTuples(leftCurr,i,leftindex,rightindex)==0){
                        outbatch.add(leftCurr.joinWith(i));
                        if(outbatch.isFull()) return outbatch;
                    }
                }

                leftVector.add(leftCurr);
                if (leftbatch.isEmpty()) leftbatch=sortedLeft.next();
                if (leftbatch==null) break;
                leftCurr=leftbatch.remove(0);
            }

            if (leftbatch==null) break;
            leftVector.add(leftCurr);
            rightVector.clear();
            if (rightbatch.isEmpty()) rightbatch=sortedRight.next();
            if (!rightbatch.isFull())
            if(rightbatch==null) break;
            rightCurr=rightbatch.remove(0);

            while(Tuple.compareTuples(leftCurr,rightCurr,leftindex,rightindex)>=0){
                for (Tuple j:leftVector){
                    if (Tuple.compareTuples(j,rightCurr,leftindex,rightindex)==0){
                        outbatch.add(j.joinWith(rightCurr));
                        if (outbatch.isFull()) return outbatch;
                }

                rightVector.add(rightCurr);
                if (rightbatch.isEmpty()) rightbatch=sortedRight.next();
                if (rightbatch==null) break;
                rightCurr=rightbatch.remove(0);
            }
            if (rightbatch==null) break;
            rightVector.add(rightCurr);
            leftVector.clear();
            if (leftbatch.isEmpty()) leftbatch=sortedLeft.next();
            if (leftbatch==null) break;
            leftCurr=leftbatch.remove(0);
        }

        }
        if (outbatch.isEmpty()){
            close();
            return null;
        }
        return outbatch;
        }
//    public Batch next() {
//        if (eos) {
//            close();
//            return null;
//        }
//        outbatch = new Batch(batchsize);
//        while (true) {
//            if (lcurs == 0) {
//                leftbatch = sortedLeft.next();
//                if (leftbatch == null) {
//                    eos = true;
//                    return outbatch;
//                }
//            }
//
//            if (rcurs == 0) {
//                rightbatch = sortedRight.next();
//                if (rightbatch == null) {
//                    eos = true;
//                    return outbatch;
//                }
//            }
//
//
//            int diff = Tuple.compareTuples(leftTuple, rightTuple, leftindex, rightindex);
//            if (diff == 0) {
//                tempBlock.addTuple(rightTuple);
//                rcurs++;
//            } else if (diff < 0) {
//                rcurs--;
//                for (int i = tempRcurs; i < tempBlock.getTupleSize(); i++) {
//                    outbatch.add(leftTuple.joinWith(rightTuple));
//                    if (outbatch.isFull()) {
//                        tempRcurs = i + 1;
//                        return outbatch;
//                    }
//                }
//                lcurs++;
//            } else if (diff > 0) {
//                tempBlock.clear();
//                rcurs++;
//            }
//        }
//
//
//
//
//
//        lcurs++;
//        for(int i = tempRcurs; i < rcurs; i++) {
//            outbatch.add(leftTuple.joinWith(rightTuple));
//        }
//    } else {
//        rcurs++;
//    }
//
//
//            for (i = lcurs; i < leftbatch.size(); i++) {
//        for (j = rcurs; j < rightbatch.size(); j++) {
//            Tuple lefttuple = leftbatch.elementAt(i);
//            Tuple righttuple = rightbatch.elementAt(j);
//
//            int diff = Tuple.compareTuples(lefttuple, righttuple, leftindex, rightindex);
//
//
//
//            if (lefttuple.checkJoin(righttuple, leftindex, rightindex)) {
//                Tuple outtuple = lefttuple.joinWith(righttuple);
//
//                //Debug.PPrint(outtuple);
//                //System.out.println();
//                outbatch.add(outtuple);
//                if (outbatch.isFull()) {
//                    if (i == leftbatch.size() - 1 && j == rightbatch.size() - 1) {//case 1
//                        lcurs = 0;
//                        rcurs = 0;
//                    } else if (i != leftbatch.size() - 1 && j == rightbatch.size() - 1) {//case 2
//                        lcurs = i + 1;
//                        rcurs = 0;
//                    } else if (i == leftbatch.size() - 1 && j != rightbatch.size() - 1) {//case 3
//                        // to keep the last state, next time calling next() will go to this location directly
//                        lcurs = i;
//                        rcurs = j + 1;
//                    } else {
//                        lcurs = i;
//                        rcurs = j + 1;
//                    }
//                    return outbatch;
//                }
//            }
//        }
//        rcurs = 0;
//    }
//    lcurs = 0;
//} catch (EOFException e) {
//        try {
//        in.close();
//        } catch (IOException io) {
//        System.out.println("NestedJoin:Error in temporary file reading");
//        }
//        eosr = true;
//        } catch (ClassNotFoundException c) {
//        System.out.println("NestedJoin:Some error in deserialization ");
//        System.exit(1);
//        } catch (IOException io) {
//        System.out.println("NestedJoin:temporary file reading error");
//        System.exit(1);
//        }
//        }
//
//
//
///** from input buffers selects the tuples satisfying join condition
// ** And returns a page of output tuples
// **/
//
/** Close the operator */
public boolean close() {
        sortedLeft.close();
        sortedRight.close();
        return true;
        }
        }