package uk.ac.gla.dcs.bigdata.studentfunctions;

import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticleStats;
import uk.ac.gla.dcs.bigdata.studentstructures.TermFrequencies;
import uk.ac.gla.dcs.bigdata.studentstructures.DPH;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;

import static uk.ac.gla.dcs.bigdata.providedutilities.DPHScorer.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;

/**
 * A flatmap that calculated DPH score between a document and each query in the query list.
 *
 */
public class DPHCalculatorFlatMap implements FlatMapFunction<NewsArticleStats, DPH>{

	private static final long serialVersionUID = -8700327078549422249L;
	
	// Statistics needed for calculating the DPH.
	Broadcast<List<Query>> broadcastQueries;
	Broadcast<Long> broadcastCorpusDocumentCount;
	Broadcast<Double> broadcastAverageDocumentLength;
	Broadcast<TermFrequencies> broadcastTotalTermFrequencyInCorpus;
	
	public DPHCalculatorFlatMap(Broadcast<List<Query>> broadcastQueries, Broadcast<Long> broadcastCorpusDocumentCount,
								Broadcast<Double> broadcastAverageDocumentLength, Broadcast<TermFrequencies> broadcastTotalTermFrequencyInCorpus) {
		this.broadcastQueries = broadcastQueries;
		this.broadcastCorpusDocumentCount = broadcastCorpusDocumentCount;
		this.broadcastAverageDocumentLength = broadcastAverageDocumentLength;
		this.broadcastTotalTermFrequencyInCorpus = broadcastTotalTermFrequencyInCorpus;
	}
	
	@Override
	public Iterator<DPH> call(NewsArticleStats newsArticle) throws Exception {
		List<DPH> docQueryPairDPHList = new ArrayList<DPH>();
		List<Query> queryTermNameList = broadcastQueries.value();
		
		// Get statistics needed for calculating the DPH from the broadcast form.
		int currentDocumentLength = newsArticle.getLength();
		long totalDocsInCorpus = broadcastCorpusDocumentCount.value();
		double averageDocumentLengthInCorpus = broadcastAverageDocumentLength.value();
		TermFrequencies totalTermFrequencyInCorpus = broadcastTotalTermFrequencyInCorpus.value();
				
		// Iterate through queries in the query list and calculate DPH between the current newsArticle and each query
		List<List<Integer>> queryTermFrequency = newsArticle.getQueryTermFrequency().getFrequencies();
		for (int i = 0; i < queryTermFrequency.size(); i++) {
			List<Integer> queryTermInDocument = queryTermFrequency.get(i);
			List<Integer> queryTermInCorpus = totalTermFrequencyInCorpus.getFrequencies().get(i);
			
			double queryTermDPHScore = 0.0;
			int termCount = queryTermInDocument.size();
			// Iterate through terms in a single query and accumulate the score for each term.
			for (int j = 0; j < termCount; j++) {
				short queryTermDocCount = queryTermInDocument.get(j).shortValue();
				// Skip calling dphScore method if query term count in document in zero - this would produce a 0 result anyways
				if (queryTermDocCount == 0) continue;
				
				double termDphScore = getDPHScore(queryTermDocCount, (int)queryTermInCorpus.get(j),
						 currentDocumentLength, averageDocumentLengthInCorpus, totalDocsInCorpus);
				if (Double.isNaN(termDphScore) || Double.isInfinite(termDphScore)) termDphScore = 0.0;
				queryTermDPHScore += termDphScore;
			}
			
			// If the DPH result is zero, it means that term did not appear in document, thus the document is not at all relevant; dismiss it.
			// Thus, only add non zero scores.
			if (queryTermDPHScore != 0.0) {
				// DHP score of a query for a document is averaged over query terms.
				queryTermDPHScore = queryTermDPHScore/(double)termCount;
				docQueryPairDPHList.add(new DPH(newsArticle.getId(), newsArticle.getTitle(), 
						queryTermNameList.get(i).getOriginalQuery(), queryTermDPHScore));
			}
		}
		
		return docQueryPairDPHList.iterator();
	}

}
