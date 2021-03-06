/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.sysml.runtime.io;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

import org.apache.sysml.conf.DMLConfig;
import org.apache.sysml.hops.OptimizerUtils;
import org.apache.sysml.runtime.controlprogram.parfor.stat.InfrastructureAnalyzer;
import org.apache.sysml.runtime.matrix.data.IJV;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.OutputInfo;
import org.apache.sysml.runtime.matrix.data.SparseRowsIterator;
import org.apache.sysml.runtime.util.MapReduceTool;

/**
 * 
 */
public class WriterMatrixMarketParallel extends WriterMatrixMarket
{
	/**
	 * 
	 * @param fileName
	 * @param src
	 * @param rlen
	 * @param clen
	 * @param nnz
	 * @throws IOException
	 */
	@Override
	protected void writeMatrixMarketMatrixToHDFS( Path path, JobConf job, MatrixBlock src, long rlen, long clen, long nnz )
		throws IOException
	{
		//estimate output size and number of output blocks (min 1)
		int numPartFiles = (int)(OptimizerUtils.estimateSizeTextOutput(src.getNumRows(), src.getNumColumns(), src.getNonZeros(), 
				              OutputInfo.MatrixMarketOutputInfo)  / InfrastructureAnalyzer.getHDFSBlockSize());
		numPartFiles = Math.max(numPartFiles, 1);
		
		//determine degree of parallelism
		int numThreads = OptimizerUtils.getParallelTextWriteParallelism();
		numThreads = Math.min(numThreads, numPartFiles);
		
		//fall back to sequential write if dop is 1 (e.g., <128MB) in order to create single file
		if( numThreads <= 1 ) {
			super.writeMatrixMarketMatrixToHDFS(path, job, src, rlen, clen, nnz);
			return;
		}
		
		//create directory for concurrent tasks
		MapReduceTool.createDirIfNotExistOnHDFS(path.toString(), DMLConfig.DEFAULT_SHARED_DIR_PERMISSION);

		//create and execute tasks
		try 
		{
			ExecutorService pool = Executors.newFixedThreadPool(numThreads);
			ArrayList<WriteMMTask> tasks = new ArrayList<WriteMMTask>();
			int blklen = (int)Math.ceil((double)rlen / numThreads);
			for(int i=0; i<numThreads & i*blklen<rlen; i++) {
				Path newPath = new Path(path, String.format("0-m-%05d",i));
				tasks.add(new WriteMMTask(newPath, job, src, i*blklen, (int)Math.min((i+1)*blklen, rlen)));
			}

			//wait until all tasks have been executed
			List<Future<Object>> rt = pool.invokeAll(tasks);	
			pool.shutdown();
			
			//check for exceptions 
			for( Future<Object> task : rt )
				task.get();
		} 
		catch (Exception e) {
			throw new IOException("Failed parallel write of text output.", e);
		}
	}
	
	/**
	 * 
	 * 
	 */
	private static class WriteMMTask implements Callable<Object> 
	{
		private JobConf _job = null;
		private MatrixBlock _src = null;
		private Path _path =null;
		private int _rl = -1;
		private int _ru = -1;

		public WriteMMTask(Path path, JobConf job, MatrixBlock src, int rl, int ru)
		{
			_path = path;
			_job = job;
			_src = src;
			_rl = rl;
			_ru = ru;
		}

		@Override
		public Object call() throws Exception 
		{
			boolean entriesWritten = false;
			FileSystem fs = FileSystem.get(_job);
			BufferedWriter bw = null;
			
			int rows = _src.getNumRows();
	    	int cols = _src.getNumColumns();
	    	long nnz = _src.getNonZeros();
	    	
			try
			{
				//for obj reuse and preventing repeated buffer re-allocations
				StringBuilder sb = new StringBuilder();
		        bw = new BufferedWriter(new OutputStreamWriter(fs.create(_path,true)));
				
		        if( _rl == 0 ) {
					// First output MM header
					sb.append ("%%MatrixMarket matrix coordinate real general\n");
				
					// output number of rows, number of columns and number of nnz
					sb.append (rows + " " + cols + " " + nnz + "\n");
		            bw.write( sb.toString());
		            sb.setLength(0);		            
		        }
		        
				if( _src.isInSparseFormat() ) //SPARSE
				{			   
					SparseRowsIterator iter = _src.getSparseRowsIterator(_rl, _ru);

					while( iter.hasNext() )
					{
						IJV cell = iter.next();

						sb.append(cell.i+1);
						sb.append(' ');
						sb.append(cell.j+1);
						sb.append(' ');
						sb.append(cell.v);
						sb.append('\n');
						bw.write( sb.toString() );
						sb.setLength(0); 
						entriesWritten = true;
					}
				}
				else //DENSE
				{
					for( int i=_rl; i<_ru; i++ )
					{
						String rowIndex = Integer.toString(i+1);					
						for( int j=0; j<cols; j++ )
						{
							double lvalue = _src.getValueDenseUnsafe(i, j);
							if( lvalue != 0 ) //for nnz
							{
								sb.append(rowIndex);
								sb.append(' ');
								sb.append( j+1 );
								sb.append(' ');
								sb.append( lvalue );
								sb.append('\n');
								bw.write( sb.toString() );
								sb.setLength(0); 
								entriesWritten = true;
							}
						}
					}
				}
				
				//handle empty result
				if ( !entriesWritten ) {
			        bw.write("1 1 0\n");
				}
			}
			finally
			{
				IOUtilFunctions.closeSilently(bw);
			}
			
			return null;
		}
	}
}
