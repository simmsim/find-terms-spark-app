package uk.ac.gla.dcs.bigdata.studentfunctions;
import uk.ac.gla.dcs.bigdata.studentstructures.TermFrequencies;

import org.apache.spark.api.java.function.MapFunction;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticleStats;
import uk.ac.gla.dcs.bigdata.studentstructures.TermFrequencies;


public class NewsQueryTermFrequencyMapper implements MapFunction<NewsArticleStats,TermFrequencies> {
	
	/**
	 * A simple mapper for extracting the query term frequencies for a document from the
	 * NewsArticleStats structure
	 */
	private static final long serialVersionUID = 318358972922006206L;
	
	@Override
	public TermFrequencies call(NewsArticleStats stats) throws Exception {
		
		return stats.getQueryTermFrequency();
	}
}
