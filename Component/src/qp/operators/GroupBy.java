package qp.operators;

import java.util.Vector;

public class GroupBy extends SortMerge {

    public GroupBy(Operator base, Vector as, int type,int numBuff) {
        super(base, as, type);
    }

}