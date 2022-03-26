package uk.ac.gla.dcs.bigdata.studentstructures;

import java.io.Serializable;

/**
 * Represents a unique query-article pair with the title of the article
 * and the DPH score the pair has.
 */
public class DPH implements Serializable, Comparable<DPH> {

	private static final long serialVersionUID = 5810396985925088828L;
	
	String articleId;
	String title;
	String originalQuery;
	double dphScore;
	
	public DPH(String articleId, String title, String originalQuery, double dphScore) {
		super();
		this.articleId = articleId;
		this.title = title;
		this.originalQuery = originalQuery;
		this.dphScore = dphScore;
	}
	
	public DPH() {
		
	}

	// Getters and setters
	public String getArticleId() {
		return articleId;
	}
	
	public void setArticleId(String articleId) {
		this.articleId = articleId;
	}
	
	public String getTitle() {
		return title;
	}
	
	public void setTitle(String title) {
		this.title = title;
	}
	
	public String getOriginalQuery() {
		return originalQuery;
	}
	
	public void setOriginalQuery(String originalQuery) {
		this.originalQuery = originalQuery;
	}
	
	public double getDPHScore() {
		return dphScore;
	}
	
	public void setDPHScore(double dphScore) {
		this.dphScore = dphScore;
	}

	@Override
	public int compareTo(DPH o) {
		return Double.compare(this.dphScore, o.dphScore);
	}

}
