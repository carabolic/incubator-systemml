package org.apache.sysml.runtime.instructions.flink;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.sysml.hops.OptimizerUtils;
import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysml.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysml.runtime.controlprogram.context.FlinkExecutionContext;
import org.apache.sysml.runtime.instructions.InstructionUtils;
import org.apache.sysml.runtime.instructions.cp.CPOperand;
import org.apache.sysml.runtime.instructions.flink.data.DataSetObject;
import org.apache.sysml.runtime.instructions.flink.functions.CopyBlockPairFunction;
import org.apache.sysml.runtime.instructions.flink.functions.CreateSparseBlockFunction;
import org.apache.sysml.runtime.matrix.MatrixCharacteristics;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;
import org.apache.sysml.runtime.matrix.data.SparseBlock;
import org.apache.sysml.runtime.matrix.operators.Operator;

/**
 * Created by carabolic on 23.11.16.
 */
public class CheckpointFLInstruction extends UnaryFLInstruction {

    public CheckpointFLInstruction(Operator op, CPOperand in, CPOperand out, String opcode, String instr) {
        super(op, in, out, opcode, instr);
    }

    @Override
    public void processInstruction(ExecutionContext ec) throws DMLRuntimeException {
        FlinkExecutionContext fec = (FlinkExecutionContext) ec;

        DataSet<Tuple2<MatrixIndexes, MatrixBlock>> in = fec.getBinaryBlockDataSetHandleForVariable(input1.getName());
        //since persist is an in-place marker for a storage level, we
        //apply a narrow shallow copy to allow for short-circuit collects
        DataSet<Tuple2<MatrixIndexes, MatrixBlock>> out = in.map(new CopyBlockPairFunction(false));

        MatrixCharacteristics mcIn = fec.getMatrixCharacteristics( input1.getName() );

        //convert mcsr into memory-efficient csr if potentially sparse
        if( OptimizerUtils.checkSparseBlockCSRConversion(mcIn) ) {
            out = out.map(new CreateSparseBlockFunction(SparseBlock.Type.CSR));
        }

        //actual checkpoint into given storage level
        out = out.persist();

        MatrixObject mo = fec.getMatrixObject( input1.getName() );
        DataSetObject inro =  mo.getDataSetHandle();  //guaranteed to exist (see above)
        DataSetObject outro = new DataSetObject(out, output.getName()); //create new rdd object
        outro.setCheckpointed(true);         //mark as checkpointed
        outro.addLineageChild(inro);          //keep lineage to prevent cycles on cleanup
        mo.setDataSetHandle(outro);

        fec.setVariable( output.getName(), mo);
    }

    public static FLInstruction parseInstruction(String str) throws DMLRuntimeException {
        String[] parts = InstructionUtils.getInstructionPartsWithValueType(str);
        InstructionUtils.checkNumFields(parts, 3);

        String opcode = parts[0];
        CPOperand in = new CPOperand(parts[1]);
        CPOperand out = new CPOperand(parts[2]);

        return new CheckpointFLInstruction(null, in, out, opcode, str);
    }
}
