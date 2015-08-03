/**
 * IBM Confidential
 * OCO Source Materials
 * (C) Copyright IBM Corp. 2010, 2015
 * The source code for this program is not published or otherwise divested of its trade secrets, irrespective of what has been deposited with the U.S. Copyright Office.
 */

package com.ibm.bi.dml.runtime.instructions.spark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.broadcast.Broadcast;

import com.ibm.bi.dml.runtime.DMLRuntimeException;
import com.ibm.bi.dml.runtime.DMLUnsupportedOperationException;
import com.ibm.bi.dml.runtime.controlprogram.context.ExecutionContext;
import com.ibm.bi.dml.runtime.controlprogram.context.SparkExecutionContext;
import com.ibm.bi.dml.runtime.instructions.cp.CPOperand;
import com.ibm.bi.dml.runtime.instructions.spark.data.PartitionedMatrixBlock;
import com.ibm.bi.dml.runtime.instructions.spark.functions.MatrixVectorBinaryOpFunction;
import com.ibm.bi.dml.runtime.matrix.MatrixCharacteristics;
import com.ibm.bi.dml.runtime.matrix.data.MatrixBlock;
import com.ibm.bi.dml.runtime.matrix.data.MatrixIndexes;
import com.ibm.bi.dml.runtime.matrix.operators.BinaryOperator;
import com.ibm.bi.dml.runtime.matrix.operators.Operator;

public class MatrixBVectorBuiltinSPInstruction extends BuiltinBinarySPInstruction 
{
	@SuppressWarnings("unused")
	private static final String _COPYRIGHT = "Licensed Materials - Property of IBM\n(C) Copyright IBM Corp. 2010, 2015\n" +
                                             "US Government Users Restricted Rights - Use, duplication  disclosure restricted by GSA ADP Schedule Contract with IBM Corp.";
	
	public MatrixBVectorBuiltinSPInstruction(Operator op, CPOperand in1, CPOperand in2, CPOperand out, String opcode, String istr) 
		throws DMLRuntimeException 
	{
		super(op, in1, in2, out, opcode, istr);
		
		//sanity check opcodes
		if(!( opcode.equalsIgnoreCase("mapmax") || opcode.equalsIgnoreCase("mapmin")) ) 
		{
			throw new DMLRuntimeException("Unknown opcode in MatrixBVectorBuiltinSPInstruction: " + toString());
		}
	}

	@Override
	public void processInstruction(ExecutionContext ec)
			throws DMLRuntimeException, DMLUnsupportedOperationException 
	{
		SparkExecutionContext sec = (SparkExecutionContext)ec;
			
		//sanity check dimensions
		checkMatrixMatrixBinaryCharacteristics(sec);

		//get input RDDs
		String rddVar = input1.getName(); 
		String bcastVar = input2.getName();
		JavaPairRDD<MatrixIndexes,MatrixBlock> in1 = sec.getBinaryBlockRDDHandleForVariable( rddVar );
		Broadcast<PartitionedMatrixBlock> in2 = sec.getBroadcastForVariable( bcastVar );
		MatrixCharacteristics mc1 = sec.getMatrixCharacteristics(rddVar);
		MatrixCharacteristics mc2 = sec.getMatrixCharacteristics(bcastVar);
		
		BinaryOperator bop = (BinaryOperator) _optr;
		boolean isColVector = (mc2.getCols() == 1);
		boolean isOuter = (mc1.getCols() == 1 && mc2.getRows() == 1);
		
		//execute map binary operation
		JavaPairRDD<MatrixIndexes,MatrixBlock> out = in1
				.flatMapToPair(new MatrixVectorBinaryOpFunction(true, isColVector, in2, bop, isOuter));
		
		//set output RDD
		updateBinaryOutputMatrixCharacteristics(sec);
		sec.setRDDHandleForVariable(output.getName(), out);
		sec.addLineageRDD(output.getName(), rddVar);
		sec.addLineageBroadcast(output.getName(), bcastVar);
	}
}