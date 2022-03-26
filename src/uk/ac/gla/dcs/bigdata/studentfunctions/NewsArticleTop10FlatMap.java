package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;

import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;

/**
 * A flatmap that filters out non-top10 NewsArticle entries based on top10 scored article ids.
 *
 */
public class NewsArticleTop10FlatMap implements FlatMapFunction<NewsArticle, NewsArticle>{
	
	private static final long serialVersionUID = -7764160443724946894L;
	
	Broadcast<List<String>> broadcastTop10ScoresIds;
	
	public NewsArticleTop10FlatMap(Broadcast<List<String>> broadcastTop10ScoresIds) {
		this.broadcastTop10ScoresIds = broadcastTop10ScoresIds;
	}

	@Override
	public Iterator<NewsArticle> call(NewsArticle newsArticle) throws Exception {
		List<String> top10ScoresIds = broadcastTop10ScoresIds.value();
		String newsArticleId = newsArticle.getId();
		
		// If the article has one of the top10 article ids, return it,
		if (top10ScoresIds.contains(newsArticleId)) {
			List<NewsArticle> articleList = new ArrayList<NewsArticle>(1);
			articleList.add(newsArticle);
			return articleList.iterator();
		} else {
			List<NewsArticle> articleList = new ArrayList<NewsArticle>(0);
			return articleList.iterator();
		}
	}

}
