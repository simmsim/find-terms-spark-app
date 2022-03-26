package uk.ac.gla.dcs.bigdata.studentstructures;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class TermFrequencies implements Serializable {
	    /**
	 * This structure holds the information about the frequencies of each query term in a document.
	 * For each query there is a corresponding list of integers, where each integer in turn corresponds
	 * to the frequency of each term in the tokenized article.
	 */
	private static final long serialVersionUID = 5311238583079838684L;
	
		List<List<Integer>> frequencies;
		
		public TermFrequencies() {
			super();
			this.frequencies = new ArrayList<List<Integer>>();
		}

	    public List<List<Integer>> getFrequencies() {
	        return frequencies;
	    }
	    
	    public void setList(List<List<Integer>> frequencies) {
	        this.frequencies = frequencies;
	    }
	    
	    public List<Integer> getRow(int row) {
	        return frequencies.get(row);
	    }
	    
	    public int getElement(int row, int col) {
	        return frequencies.get(row).get(col);
	    }
	    
	    public int size() {
	    	return frequencies.size();
	    }
	    
	    public void add(List<Integer> list) {
	    	frequencies.add(list);
	    }
	}