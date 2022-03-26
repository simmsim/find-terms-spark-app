package uk.ac.gla.dcs.bigdata.apps;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import uk.ac.gla.dcs.bigdata.providedfunctions.NewsFormaterMap;
import uk.ac.gla.dcs.bigdata.providedfunctions.QueryFormaterMap;
import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;
import uk.ac.gla.dcs.bigdata.studentfunctions.NewsArticleStatsFormatter;
import uk.ac.gla.dcs.bigdata.studentfunctions.NewsArticleTop10FlatMap;
import uk.ac.gla.dcs.bigdata.studentfunctions.QueryTermFrequencyReducer;
import uk.ac.gla.dcs.bigdata.studentfunctions.NewsQueryTermFrequencyMapper;
import uk.ac.gla.dcs.bigdata.studentfunctions.DPHCalculatorFlatMap;
import uk.ac.gla.dcs.bigdata.studentfunctions.IndexPartitioner;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticleStats;
import uk.ac.gla.dcs.bigdata.studentstructures.TermFrequencies;
import uk.ac.gla.dcs.bigdata.util.RealizationEngineClient;
import uk.ac.gla.dcs.bigdata.studentstructures.DPH;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticlePairing;

import static uk.ac.gla.dcs.bigdata.providedutilities.TextDistanceCalculator.*;


/**
 * This is the main class where your Spark topology should be specified.
 * 
 * By default, running this class will execute the topology defined in the
 * rankDocuments() method in local mode, although this may be overriden by
 * the spark.master environment variable.
 * @author Richard
 *
 */
public class AssessedExercise {
	
	public static void main(String[] args) {
		
		// Remote deployment code.
		System.out.println("#----------------------------------");
		System.out.println("# Big Data AE Remote Deployer");
		System.out.println("#----------------------------------");

		System.out.println("Arguments:");
		System.out.println("  1) TeamID: 'humongous-data'");
		System.out.println("  1) Project: 'bigdata-ae'");
		
		
		args = new String[] {
				"humongous-data",
				"bigdata-ae"
			};
		
		if (args.length!=2 || args[0].equalsIgnoreCase("TODO")) {
			System.out.println("TeamID or Project not set, aborting...");
			System.exit(0);
		}
		
		System.out.println("# Stage 1: Register Your GitLab Repo with the Realization Engine");
		System.out.print("Sending Registration Request...");
		boolean registerOk = RealizationEngineClient.registerApplication(args[0], args[1], "bdaefull");
		if (registerOk) {
			System.out.println("OK");
		} else {
			System.out.println("Failed, Aborting");
			System.exit(1);
		}
		
		System.out.println("# Stage 2: Trigger the Build and Deployment Sequence");
		System.out.print("Sending Application Start Request...");
		boolean startOk = RealizationEngineClient.startBuildOperationSequenceForTeam(args[0]);
		if (startOk) {
			System.out.println("OK");
		} else {
			System.out.println("Failed, Aborting");
			System.exit(1);
		}
		
		// Original template code starts here.
		File hadoopDIR = new File("resources/hadoop/"); // represent the hadoop directory as a Java file so we can get an absolute path for it
		System.setProperty("hadoop.home.dir", hadoopDIR.getAbsolutePath()); // set the JVM system property so that Spark finds it
		
		// The code submitted for the assessed exerise may be run in either local or remote modes
		// Configuration of this will be performed based on an environment variable
		String sparkMasterDef = System.getenv("spark.master");
		if (sparkMasterDef==null) sparkMasterDef = "local[2]"; // default is local mode with two executors
		
		String sparkSessionName = "BigDataAE"; // give the session a name
		
		// Create the Spark Configuration 
		SparkConf conf = new SparkConf()
				.setMaster(sparkMasterDef)
				.setAppName(sparkSessionName);
		
		// Create the spark session
		SparkSession spark = SparkSession
				  .builder()
				  .config(conf)
				  .getOrCreate();
	
		
		// Get the location of the input queries
		String queryFile = System.getenv("bigdata.queries");
		if (queryFile==null) queryFile = "data/queries.list"; // default is a sample with 3 queries
		
		// Get the location of the input news articles
		String newsFile = System.getenv("bigdata.news");
		if (newsFile==null) newsFile = "data/TREC_Washington_Post_collection.v3.example.json"; // default is a sample of 5000 news articles
		
		// Call the student's code
		long startTime = System.nanoTime();
		List<DocumentRanking> results = rankDocuments(spark, queryFile, newsFile);
		long elapsedTime = System.nanoTime() - startTime;
		System.out.println("Execution time of rankDocuments: " + elapsedTime/1000000 + " ms");
		// Close the spark session
		spark.close();
		
		// Check if the code returned any results
		if (results==null) System.err.println("Topology return no rankings, student code may not be implemented, skiping final write.");
		else {
			
			// We have set of output rankings, lets write to disk
			
			// Create a new folder 
			File outDirectory = new File("results/"+System.currentTimeMillis());
			if (!outDirectory.exists()) outDirectory.mkdir();
			
			// Write the ranking for each query as a new file
			for (DocumentRanking rankingForQuery : results) {
				rankingForQuery.write(outDirectory.getAbsolutePath());
			}
		}
		
		
	}
	
	public static List<DocumentRanking> rankDocuments(SparkSession spark, String queryFile, String newsFile) {
		
		// Load queries and news articles
		Dataset<Row> queriesjson = spark.read().text(queryFile);
		Dataset<Row> newsjson = spark.read().text(newsFile); // read in files as string rows, one row per article
		
		// Perform an initial conversion from Dataset<Row> to Query and NewsArticle Java objects
		Dataset<Query> queries = queriesjson.map(new QueryFormaterMap(), Encoders.bean(Query.class)); // this converts each row into a Query
		Dataset<NewsArticle> news = newsjson.map(new NewsFormaterMap(), Encoders.bean(NewsArticle.class)); // this converts each row into a NewsArticle
		
		//----------------------------------------------------------------
				// Start of our Spark Topology
				//----------------------------------------------------------------
				
				// Convert to RDD.
				JavaRDD<NewsArticle> newsArticlesAsRDD = news.toJavaRDD();
				
				// Partitioners need <key, value> pairs, so we need to extract the title as key.
				NewsArticlePairing indexKeyExtractor = new NewsArticlePairing();
				JavaPairRDD<Integer, NewsArticle> newsArticlesWithKeysAsRDDPair = newsArticlesAsRDD.mapToPair(indexKeyExtractor);
				
				// Repartition the RDD.
				JavaPairRDD<Integer, NewsArticle> newsArticlesRepartitionedAsRDDPair = newsArticlesWithKeysAsRDDPair.partitionBy(new IndexPartitioner(12));
				
				// Drop the keys now that we don't need them anymore.
				JavaRDD<NewsArticle> newsArticlesRepartitionedAsRDD = newsArticlesRepartitionedAsRDDPair.map(x -> x._2);
				
				// Convert back to dataset.
				Dataset<NewsArticle> newsRepartitioned = spark.createDataset(newsArticlesRepartitionedAsRDD.rdd(), Encoders.bean(NewsArticle.class));
				
				// See number of partitions
				System.out.println("Partition number: " + newsRepartitioned.toJavaRDD().partitions().size());
				
				// Create accumulator for the total length of all the documents combined. Later use this to find the average document length.
				LongAccumulator documentLengthAccumulator = spark.sparkContext().longAccumulator();

				//Broadcats the list of queries, so we do not have multiple copies.
				List<Query> listOfQueries = queries.collectAsList();
				Broadcast<List<Query>> broadcastQueries = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(listOfQueries);
				
				// Map the news article into a structure which contains the query term frequencies, title, id and document length for each individual document.
				Encoder<NewsArticleStats> newsArticleStatsEncoder = Encoders.bean(NewsArticleStats.class);
				Dataset<NewsArticleStats> newsArticleStats = newsRepartitioned.flatMap(new NewsArticleStatsFormatter(broadcastQueries, documentLengthAccumulator), newsArticleStatsEncoder);
				
				// To get the accumulator value, we need to call an action first.
				// Thus, we can call count to get the count of all documents in the dataset and run the mapping.
				long documentCount = newsArticleStats.count();
				// Now we can get the total combined document length and calculate the average document length.
				long totalDocumentsLength = documentLengthAccumulator.value();
				double averageDocumentLength = (1.0*totalDocumentsLength)/documentCount;
				
				//Define a pairwise reducer of query term frequencies and apply it to the dataset  of the term frequencies in all documents
				//The result is aggregated term frquencies for the entire corpus 
				Encoder<TermFrequencies> termFrequenciesEncoder = Encoders.bean(TermFrequencies.class);
				Dataset<TermFrequencies> termFrequencies = newsArticleStats.map(new NewsQueryTermFrequencyMapper(), termFrequenciesEncoder);
				TermFrequencies summarizedTermFrequencies = termFrequencies.reduce(new QueryTermFrequencyReducer());
				
				// Broadcast statistics needed for the DPH calculator.
				Broadcast<Long> broadcastCorpusDocumentCount = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(documentCount);
				Broadcast<Double> broadcastAverageDocumentLength = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(averageDocumentLength);
				Broadcast<TermFrequencies> broadcastTotalTermFrequencyInCorpus = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(summarizedTermFrequencies);
				
				// Calculating DHP Score and placing results in a new DPH structure.
				Encoder<DPH> dphEncoder = Encoders.bean(DPH.class);
				DPHCalculatorFlatMap dphCalculatorFlatMap = new DPHCalculatorFlatMap(broadcastQueries, broadcastCorpusDocumentCount, 
																					 broadcastAverageDocumentLength, broadcastTotalTermFrequencyInCorpus);
				Dataset<DPH> dphScores = newsArticleStats.flatMap(dphCalculatorFlatMap, dphEncoder);
				
				// Convert DPH dataset to a java list that we can analyse it to find top 10 non-similar documents per query.
				List<DPH> dphScoresList = dphScores.collectAsList();
				Collections.sort(dphScoresList); // default ordering of DPH is by dphScore
				Collections.reverse(dphScoresList); // Collections.sort() sorts in ascending order, reverse to get descending order of dphScore
				
				// Auxiliary intermediate structures needed for the construction of final top 10 results.
				Map<String, Query> queryNameToQueryObjectMap = new HashMap<>();
				Map<String, List<DPH>> queryNameToTop10DocMap = new HashMap<>();
				List<String> queryNamesToBeProcessed = new ArrayList<>();
				for (Query query: listOfQueries) {
					queryNameToQueryObjectMap.put(query.getOriginalQuery(), query);
					queryNameToTop10DocMap.put(query.getOriginalQuery(), new ArrayList<DPH>());
					queryNamesToBeProcessed.add(query.getOriginalQuery());
				}
				
				// The filtering phase based on similarity score.
				for (DPH currentDphEntry: dphScoresList) {
					
					String queryName = currentDphEntry.getOriginalQuery();
					// If the currentDphEntry is for query that already collected top 10, just skip it.
					if (!queryNamesToBeProcessed.contains(queryName)) continue;
					
					List<DPH> topDPHValuesPerQuery = queryNameToTop10DocMap.get(queryName);
					// If no documents have been yet added to the top10 list for the query, add the first one found in the dphScoresList
					// and since this list is sorted, the first entry will have the highest dphScore; else check for similarity.
					if (topDPHValuesPerQuery.isEmpty()) {
						topDPHValuesPerQuery.add(currentDphEntry);
					} else {
						boolean addToTop10 = false;
						for (DPH topDphEntry: topDPHValuesPerQuery) {
							String titleCurrent = currentDphEntry.getTitle();
							String titleToCompare = topDphEntry.getTitle();
							double similarity = similarity(titleCurrent, titleToCompare);
							// Since dphScoresList is already sorted by dphScore, if the two titles are very similar,
							// we won't check for DPH since it will be smaller than the one already in top 10 list.
							if (similarity < 0.5) {
								addToTop10 = false;
								// Since the title is too similar, we won't be comparing it to other titles in the top10 list for query and thus we break from the loop.
								break;
							} else {
								// currentDphEntry is a candidate to be added to top10.
								addToTop10 = true;
							}
						}
						
						// currentDphEntry was not similar to any of the titles in the top10 list, so we add it.
						if (addToTop10) topDPHValuesPerQuery.add(currentDphEntry);
						
						if (topDPHValuesPerQuery.size() == 10) {
							// We have top10 for this query, so we don't need to do further filtering for it.
							queryNamesToBeProcessed.remove(queryName);
						}
						
						// All queries have now collected top10 dph entries and thus we can stop searching.
						if (queryNamesToBeProcessed.isEmpty()) break;
					}
				}
					
				// However, what we have to far is filtered down DPH structure entries, and we need NewsArticle objects to construct DocumentRanking.
				// Get a list of all articleIds that are in the top10 list for each query.
				List<String> top10ArticleIds = new ArrayList<>();
				for (List<DPH> dphEntries: queryNameToTop10DocMap.values()) {
					for (DPH dphEntry: dphEntries) {
						String articleId = dphEntry.getArticleId();
						// There might be articles that are applicable to several queries, so we don't want 
						// to add duplicates.
						int index = top10ArticleIds.indexOf(articleId);
						if (index == -1) {
							top10ArticleIds.add(articleId);
						}
					}
				}
				
				// Use spark flatMap function to return the top10 NewsArticle objects. 
				// We're basically filtering the NewsArticles based on the articleIds that are in top10 list for each query and which are broadcasted.
				// Since the NewsArticle dataset is already sitting on nodes (from being used before) this operation shouldn't be too expensive?
				// Also, we have a lot of newsarticle documents, so perhaps it would be too slow to do the artcileId based filtering on the host side.
				Broadcast<List<String>> broadcastTop10ScoresIds = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(top10ArticleIds);
				Dataset<NewsArticle> top10Articles = newsRepartitioned.flatMap(new NewsArticleTop10FlatMap(broadcastTop10ScoresIds), Encoders.bean(NewsArticle.class));
				
				// This contains only the NewsArticle objects that have ids which appeared in one of the top10 lists.
				List<NewsArticle> newsArticleTop10List = top10Articles.collectAsList();
				Map<String, NewsArticle> articleIdToObjectMap = (HashMap<String, NewsArticle>) newsArticleTop10List.stream() 
																						.collect(Collectors.toMap(NewsArticle::getId, Function.identity()));
				
				// We know which docId and query pairs are in top10, need to build the final DocumentRanking list.
				List<DocumentRanking> documentRankingList = new ArrayList<>();
				for (Map.Entry<String,Query> entry : queryNameToQueryObjectMap.entrySet()) {
					String queryName = entry.getKey();
					Query query = entry.getValue();
					
					List<RankedResult> top10ArticleResults = new ArrayList<RankedResult>();
					List<DPH> dphScoresForQuery = queryNameToTop10DocMap.get(queryName);
					for (DPH dphScore: dphScoresForQuery) {
						String articleId = dphScore.getArticleId();
						double dphScoreValue = dphScore.getDPHScore();
						NewsArticle newsArticle = articleIdToObjectMap.get(articleId);
						
						top10ArticleResults.add(new RankedResult(articleId, newsArticle, dphScoreValue));
					}
					
					documentRankingList.add(new DocumentRanking(query, top10ArticleResults));
				}
				
				//----------------------------------------------------------------
				// End of our Spark Topology
				//----------------------------------------------------------------
		return documentRankingList; // replace this with the the list of DocumentRanking output by your topology
	}
}
