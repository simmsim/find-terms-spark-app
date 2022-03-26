package uk.ac.gla.dcs.bigdata.studentstructures;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;

public class NewsArticlePairing implements PairFunction<NewsArticle, Integer, NewsArticle>{
	
	/**
	 * A <key, value> pair, where the key is an integer index and the value is the NewsArticle structure itself.
	 * We use a private index variable here to assign the index keys, as this operation is done when we still
	 * have only one partition, thus there is no risk of the private index variable becoming corrupted. After
	 * assigning an index as a key, we increase/iterate the private index variable by 1 (++).
	 */
	private static final long serialVersionUID = -2057288433033564871L;
	
	private int index = 0; // private index variable to use as key assignment

	@Override
	public Tuple2<Integer, NewsArticle> call(NewsArticle t) throws Exception {
		
		// Create the <key, value> tuple containing the index as key and NewsArticle as value.
		Tuple2<Integer, NewsArticle> pairing = new Tuple2<Integer, NewsArticle>(this.index, t);
		// Increase the private index variable by 1.
		this.index++;
		
		return pairing;
	}
	
}
