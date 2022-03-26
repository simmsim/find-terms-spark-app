package uk.ac.gla.dcs.bigdata.studentfunctions;
import org.apache.spark.broadcast.Broadcast;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Collections;
import java.util.Iterator;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.util.LongAccumulator;

import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedutilities.TextPreProcessor;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticleStats;
import uk.ac.gla.dcs.bigdata.studentstructures.TermFrequencies;


public class NewsArticleStatsFormatter implements FlatMapFunction<NewsArticle,NewsArticleStats> {
	
	/**
	 * This function maps from a NewsArticle to the NewsArticleStats structure. We take the document ID, title and first 5 paragraph's text
	 * and process it (tokenize, stem, remove stopwords) to get the terms. Then we count the terms to get the individual document length,
	 * and add this length to the total combined document length accumulator. Lastly, for each query, we get the frequency of each term in
	 * the document.
	 * 
	 * We return the NewsArticleStats structure, containing:
	 * news article ID
	 * news article title
	 * news article length - counted in the processed terms (tokenized, stemmed, stopwords removed)
	 * queryTermFrequency (List<List<Interger>) - a list of lists of integers containing
	 *                    the frequency of each query term in the document (the internal List<Integer) per each query (thus external List)
	 */
	private static final long serialVersionUID = 318358972922006206L;
	
	// Counter/accumulator to get the total combined document length.
	private LongAccumulator documentLengthAccumulator;
	
	// Global data containing the list of queries
	Broadcast<List<Query>> broadcastQueries;
	
	public NewsArticleStatsFormatter(Broadcast<List<Query>> broadcastQueries, LongAccumulator documentLengthAccumulator) {
		this.broadcastQueries = broadcastQueries;
		this.documentLengthAccumulator = documentLengthAccumulator;
	}

	@Override
	public Iterator<NewsArticleStats> call(NewsArticle article) throws Exception {
		
		// Get the document title string.
		String title = article.getTitle();
		if (title == null || title.isEmpty()) {
			return new ArrayList<NewsArticleStats>(0).iterator();
		}
		
		// Add the title to string for processing (with first 5 paragraph's text).
		StringBuilder builder = new StringBuilder();
		builder.append(title);
		
		// For ContentITem in contents, if ContentItem subtype is paragraph, get text string.
		// Only get the text of the first 5 paragraphs - break after getting 5 paragraphs, or go through all ContentItems, if less than 5.
		int paragraphCounter = 0;
		for (ContentItem content: article.getContents()) {
			// If we got 5 paragraph's text, we don't need more, thus break out of the loop (this saves time and memory as well.
			if (paragraphCounter == 5) {
				break;
			}
			if (Objects.equals(content.getSubtype(), "paragraph")) {
				// Make sure to separate the paragraph texts with a space - otherwise might accidentally create combined random words.
				builder.append(" ");
				builder.append(content.getContent());
				paragraphCounter++;
			}
		}
		
		// Gather the string of the title and first 5 (or all, if less) paragraphs.
		String allText = builder.toString();
		
		// Get the tokenized, stemmed and stopword-removed processed text.
		TextPreProcessor processor = new TextPreProcessor();
		List<String> tokenizedText = processor.process(allText);
		
		// Get the length of the news article in terms (using the processed text format).
		int articleLength = tokenizedText.size();
		// Add the document length to the total combined document length accumulator.
		documentLengthAccumulator.add(articleLength);
		
		// A list of lists of term frequencies for all queries.
		TermFrequencies termFrequencies = new TermFrequencies();
			
		// Get the list of all queries.
		List<Query> queries = broadcastQueries.value();
		
		// Go over each query, each of its terms and find the frequency of the term in the tokenized text.
		// The frequency for each term of each query can be accessed by appropriately indexing the 
		// list of lists - the first index being the index of the query, the second one being the index 
		// of the term in the list of terms of the query.
		for (Query query: queries) {
			List<Integer> singleQueryFreqList = new ArrayList<Integer>();
			for (String term: query.getQueryTerms()) {
				
				singleQueryFreqList.add(Collections.frequency(tokenizedText, term));
			}
			termFrequencies.add(singleQueryFreqList);	
		}
		
		// Add the News Article information to the NewsArticleStats structure.
		NewsArticleStats processedNewsArticle = new NewsArticleStats(
				article.getId(),
				title,
				articleLength,
				termFrequencies
				);
		
		List<NewsArticleStats> processedNewsArticleList = new ArrayList<NewsArticleStats>(1);
		processedNewsArticleList.add(processedNewsArticle);
		return processedNewsArticleList.iterator();
	}
}
