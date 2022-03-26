package uk.ac.gla.dcs.bigdata.studentstructures;

import java.io.Serializable;
import uk.ac.gla.dcs.bigdata.studentstructures.TermFrequencies;


public class NewsArticleStats implements Serializable {
	
	/**
	 * This structure holds the information of the processed News Articles, which is then used to calculate DPH.
	 * 
	 * The data is:
	 * id (String) - the ID of the News Article document
	 * title (String) - the title of the News Article document
	 * length (int) - the length of the News Article individual document, acquired by counting the tokenized, stemmed and stopword-removed 'words'
	 * queryTermFrequency (List<List<Interger>) - a list of lists of integers containing
	 *                    the frequency of each query term in the document (the internal List<Integer) per each query (thus external List)
	 */
	private static final long serialVersionUID = -8955789612533648071L;
	
	String id; // unique article identifier
	String title; // article title
	int length; // length of the document - counted tokens
	TermFrequencies queryTermFrequency; // frequency of each query term in the news article (per each query itself)

	public NewsArticleStats() {}

	public NewsArticleStats(String id, String title, int length, TermFrequencies queryTermFrequency) {
		super();
		this.id = id;
		this.title = title;
		this.length = length;
		this.queryTermFrequency = queryTermFrequency;
	}
	

	// Getters and setters
	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public int getLength() {
		return length;
	}

	public void setLength(int length) {
		this.length = length;
	}

	public TermFrequencies getQueryTermFrequency() {
		return queryTermFrequency;
	}

	public void setQueryTermFrequency(TermFrequencies queryTermFrequency) {
		this.queryTermFrequency = queryTermFrequency;
	}

	
}