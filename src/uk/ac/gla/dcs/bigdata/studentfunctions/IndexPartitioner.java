package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.Partitioner;

public class IndexPartitioner extends Partitioner{
	
	/**
	 * A Partitioner which takes the index of each article, divides the index with the number of partitions and uses
	 * the remainder as the assigned partition number/ID.
	 * 
	 * Due to this partitioner using index in assignments, it is effectively like range partitioning.
	 */
	private static final long serialVersionUID = 7578092514730601618L;
	
	final int numberOfPartitions;
	
	public IndexPartitioner(int numberOfPartitions) {
		this.numberOfPartitions = numberOfPartitions;
	}

	@Override
	public int getPartition(Object key) {
		
		// Set the key object to be an integer.
		int item = (Integer)key;
		
		// Partitioning based on the index, using modulo to get the
		// remainder when divided by the number of partitions
		int partition = item % numberOfPartitions;
		
		return partition;
	}

	@Override
	public int numPartitions() {
		return numberOfPartitions;
	}
	
}
