package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Tuple;

import java.io.*;

public class HashJoin extends Join {
    int batchsize;
    int blocksize;

    int leftindex;
    int rightindex;

    static int filenum = 0;
    String rfname;
    Tuple hashTuple;

    ObjectInputStream in;
    boolean[] judge;

    Batch outputBatch;
    Batch leftBatch;
    Batch rightBatch;

    public HashJoin(Join jn) {
        super(jn.getLeft(), jn.getRight(), jn.getCondition(), jn.getOpType());
        schema = jn.getSchema();
        jointype = jn.getJoinType();
        numBuff = jn.getNumBuff();
        judge = new boolean[numBuff];
    }

    public boolean open() {

        /** select number of tuples per batch **/
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;
        blocksize = numBuff - 2;

        Attribute leftattr = con.getLhs();
        Attribute rightattr = (Attribute) con.getRhs();
        leftindex = left.getSchema().indexOf(leftattr);
        rightindex = right.getSchema().indexOf(rightattr);
        Batch rightpage;
        /** initialize the cursors of input buffers **/


        /** Right hand side table is to be materialized
         ** for the hash join to perform
         **/

        if (!right.open() || left.open()) {
            return false;
        } else {
            /** If the right operator is not a base table then
             ** Materialize the intermediate result from right
             ** into a file
             **/

            //if(right.getOpType() != OpType.SCAN){
            filenum++;
            ObjectOutputStream outright = null;
            while ((rightpage = right.next()) != null) {
                for (int i = 0; i < rightpage.size(); i++) {
                    hashTuple = rightpage.elementAt(i);
                    judge[hashTuple.dataAt(rightindex).hashCode() % (numBuff - 1)] = true;
                    rfname = "righttemp" + String.valueOf(filenum) + String.valueOf(hashTuple.dataAt(rightindex).hashCode() % (numBuff - 1));
                    try {
                        outright = new ObjectOutputStream(new FileOutputStream(rfname));
                        outright.writeObject(hashTuple);
                        outright.close();
                    } catch (IOException io) {
                        System.out.println("BlockNestedJoin:writing the temporary file error");
                        return false;
                    }
                }
            }

            //}
            if (!right.close())
                return false;
            if (!left.close())
                return false;
        }
        return true;
    }


    public Batch next() {
        outputBatch=new Batch(batchsize);

    }
}