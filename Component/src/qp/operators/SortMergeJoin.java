package qp.operators;

import org.w3c.dom.Attr;
import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Block;
import qp.utils.Tuple;

import java.io.*;
import java.util.ArrayList;
import java.util.Vector;

public class SortMergeJoin extends Join {
    private int batchsize;  //Number of tuples per out batch
    private int blocksize;  // number of batches per block

    private int leftindex;     // Index of the join attribute in left table
    private int rightindex;    // Index of the join attribute in right table

    private String rfname;    // The file name where the right table is materialize

    private static int filenum = 0;   // To get unique filenum for this operation

//    Batch outbatch;   // Output buffer
//    Batch leftbatch;  // Buffer for left input stream
//    Block leftblock;

    private Batch rightbatch;  // Buffer for right input stream
    private ObjectInputStream in; // File pointer to the right hand materialized file

    private int lcurs;    // Cursor for left side buffer
    private int rcurs;    // Cursor for right side buffer
    private boolean eosl;  // Whether end of stream (left table) is reached
    private boolean eosr;  // End of stream (right table)

    private File fileLeft;
    private File fileRight;
    private ArrayList<File> filesRight;

    private SortMerge sortedLeft;
    private SortMerge sortedRight;

    int numOfRightTuples;

    // next()
    private Batch leftBatch;
    private Batch rightBatch;

    private int rightIndex;
    private int startOfPartition;
    private boolean tupleEqual;

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
        blocksize = numBuff - 1;

        Attribute leftattr = con.getLhs();
        Vector<Attribute> leftSet = new Vector<>();
        leftSet.add(leftattr);

        Attribute rightattr = (Attribute) con.getRhs();
        Vector<Attribute> rightSet = new Vector<>();
        rightSet.add(rightattr);

        leftindex = left.getSchema().indexOf(leftattr);
        rightindex = right.getSchema().indexOf(rightattr);

        sortedLeft = new SortMerge(left, leftSet, OpType.SORT, numBuff);
//        System.out.println(left.getSchema().getAttList());

        sortedRight = new SortMerge(right, rightSet, OpType.SORT, numBuff);
//        System.out.println(right.getSchema().getAttList());


        if(!sortedLeft.open() || !sortedRight.open()) {
            return false;
        }

        System.out.println();
        Debug.PPrint(sortedLeft.next());

        System.out.println();
        Debug.PPrint(sortedRight.next());

        System.out.println("=========Left==========");
        try {
            in = new ObjectInputStream(new FileInputStream(sortedLeft.sortedFiles.get(0)));
            while (true) {
                Batch batch = sortedLeft.getNextBatch(in);
                if (batch == null)
                    break;
                for (int j = 0; j < batch.size(); j++) {
                    Tuple present = batch.elementAt(j);
                    System.out.println("tuple: " + present.dataAt(0) + " " + present.dataAt(1)
                            + " " + present.dataAt(2) + " " + present.dataAt(3));
                }
            }
            System.out.println();
        } catch (Exception e) {
            System.err.println(" Error reading " + sortedLeft.sortedFiles.get(0));
        }
        System.out.println();

        System.out.println("=========Right==========");
        try {
            in = new ObjectInputStream(new FileInputStream(sortedRight.sortedFiles.get(0)));
            while (true) {
                Batch batch = sortedRight.getNextBatch(in);
                if (batch == null)
                    break;
                for (int j = 0; j < batch.size(); j++) {
                    Tuple present = batch.elementAt(j);
                    System.out.println("tuple: " + present.dataAt(0) + " " + present.dataAt(1)
                            + " " + present.dataAt(2) + " " + present.dataAt(3));
                }
            }
            System.out.println();
        } catch (Exception e) {
            System.err.println(" Error reading " + sortedLeft.sortedFiles.get(0));
        }
        System.out.println();

        // Left and right operators are not base table, materialize them to files.
//        fileLeft = materialize(sortedLeft, "left");
        numOfRightTuples = 0;
        filesRight = materialize(sortedRight, "right");
        System.out.println("The number of right tuples " +numOfRightTuples);
//        sortedLeft.close();
//        sortedRight.close();

        eosl = false;
        eosr = false;

        lcurs = 0;
        rcurs = 0;

        rightIndex = 0;
        startOfPartition = 0;
        tupleEqual = false;

        return true;
    }

    public Batch next() {
//        Batch left = sortedLeft.next();
        Batch result = new Batch(batchsize);
        try {
            boolean flag = true;
            while(flag) {
                if(lcurs == 0) {
                    leftBatch = sortedLeft.next();

//                    if(leftBatch != null) {
//                        for(int i = 0;i < leftBatch.size();i++) {
//                            System.out.println(leftBatch.elementAt(i)._data);
//                        }
//                    }
//
                    if(leftBatch == null) {
                        return null;
                    }
                }

                int rightFileIndex = rightIndex / batchsize;
                int rightTupleIndex = rightIndex % batchsize;
                File rightFile = filesRight.get(rightFileIndex);
                Batch rightBatch = (Batch) (new ObjectInputStream(new FileInputStream(rightFile))).readObject();
                Tuple rightTuple = rightBatch.elementAt(rightTupleIndex);
                Tuple leftTuple = leftBatch.elementAt(lcurs);

                System.out.println("Left tuple: " + leftTuple._data);
                System.out.println("Right tuple: " + rightTuple._data);

                int leftOrRight = Tuple.compareTuples(leftTuple, rightTuple, leftindex, rightindex);

                if(leftOrRight < 0) {
                    lcurs = (lcurs+1) % batchsize;
                    if(tupleEqual) {
                        rightIndex = startOfPartition;
                    }
                    tupleEqual = false;
                } else if(leftOrRight > 0) {
                    rightIndex++;
                    tupleEqual = false;
                } else // leftOrRight == 1, which means leftTuple == rightTuple
                {
                    if(!tupleEqual) {
                        startOfPartition = rightIndex; // new startOfPartition
                    }
                    tupleEqual = true;
                    Tuple resultTuple = leftTuple.joinWith(rightTuple);
                    result.add(resultTuple);
                    rightIndex++;
                }

                flag = !result.isFull() && ((leftBatch = sortedLeft.next()) != null) && (rightIndex < numOfRightTuples);
            }
        } catch (IOException|ClassNotFoundException e) {
            e.printStackTrace();
        }

        if(result.isEmpty()) {
            return null;
        }
        return result;
    }


    /** from input buffers selects the tuples satisfying join condition
     ** And returns a page of output tuples
     **/

    /** Close the operator */
    public boolean close() {
//        for(File file : filesRight) {
//            file.delete();
//        }
//
//        sortedLeft.close();
//        sortedRight.close();
        return true;
    }

    public ArrayList<File> materialize(SortMerge op, String base) {
        int num = 0;
        ArrayList<File> results = new ArrayList<>();
        try {
            Batch batch = op.next();
            while(batch != null) {
                File result = new File("base-" + base + "-" + num + ".txt");
                ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(result));
                out.writeObject(batch);
                out.close();
                results.add(result);
                numOfRightTuples += batch.size();
                batch = op.next();
                num++;
            }
            return results;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

}
