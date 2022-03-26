package uk.ac.gla.dcs.bigdata.studentfunctions;
import java.util.ArrayList;
import java.util.List;
import uk.ac.gla.dcs.bigdata.studentstructures.TermFrequencies;


import org.apache.spark.api.java.function.ReduceFunction;

public class QueryTermFrequencyReducer implements ReduceFunction<TermFrequencies>{
	/**
	 * A pairwise frequency reducer which when applied to the set of frequencies for all documents
	 * will return the aggregated frequencies for the entire corpus.
	 */
	private static final long serialVersionUID = 1599295298686179466L;

	@Override
	public TermFrequencies call(TermFrequencies v1, TermFrequencies v2) throws Exception {
		TermFrequencies termFrequencies = new TermFrequencies();
		for (int row=0; row < v1.size(); row++)
		{
			List<Integer> singleQueryFreqList = new ArrayList<Integer>();
		    for (int col=0; col < v1.getRow(row).size(); col++)
		    {
		        singleQueryFreqList.add(Integer.valueOf(v1.getElement(row,col)+ v2.getElement(row,col)));
		    }
			termFrequencies.add(singleQueryFreqList);	
		}
		return termFrequencies;
	}

}

	
	