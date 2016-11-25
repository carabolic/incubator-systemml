package org.apache.sysml.runtime.instructions.flink.functions;

/**
 * Created by carabolic on 15/02/17.
 */

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.spark.api.java.function.Function;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;
import org.apache.sysml.runtime.matrix.data.SparseBlock;

/**
 * General purpose copy function for binary block values. This function can be used in
 * mapValues (copy matrix blocks) to change the internal sparse block representation.
 * See CopyBlockFunction if no change of SparseBlock.Type required.
 *
 */
public class CreateSparseBlockFunction
        implements MapFunction<Tuple2<MatrixIndexes, MatrixBlock>, Tuple2<MatrixIndexes, MatrixBlock>> {

    private SparseBlock.Type _stype = null;

    public CreateSparseBlockFunction( SparseBlock.Type stype ) {
        _stype = stype;
    }

    @Override
    public Tuple2<MatrixIndexes, MatrixBlock> map(Tuple2<MatrixIndexes, MatrixBlock> value) throws Exception {
        //convert given block to CSR representation if in sparse format
        //but allow shallow pass-through if already in CSR representation.
        MatrixBlock block = value.f1;
        if(block.isInSparseFormat()) {
            value.f1 = new MatrixBlock(block, _stype, false);
        }
        return value;
    }
}
