package com.dbsearchutility.ija;

import org.apache.directmemory.cache.*;
import org.apache.directmemory.DirectMemory;
import org.apache.directmemory.memory.Pointer;

import java.sql.*;
import java.util.*;
import java.util.regex.*;



/*
 * This class implements the thread for each search_keyword command entered by user on console
 */
public class SearchKeywordService implements Runnable{
	
	private static final String dbURL = "jdbc:mysql://localhost";  
	private static final String dbUser = "root";  
	private static final String dbPassword = "ija-tp";   
	private static final String DbTABLE = "ija_tp.JsonData"; 	
	
	private enum SearchResultFunction {FUNCTION_1, FUNCTION_2, FUNCTION_3, NONE};	
	private static final String CACHED = "Cached Search";	
	private static final String UNCACHED = "Database Query Search";
	private static final int MAX_DISPLAY_LENGTH_NAME = 60;
	
	private static CacheService<String, Map<String, Integer>> cacheServ;
	
	private  Set<String> keyStringSet = new HashSet<String>(); //use Set for keyStrings to ensure uniqueness
	private Map<String, Map<String, Integer>> keyStringToResultsMap = new HashMap<String, Map<String, Integer>>();	
	private Map<String, Integer> consolidatedResultsMap = new HashMap<String, Integer>();
	private Map<String, String> function1SearchStatsMap = new HashMap<String, String>();
	
	private volatile SearchResultFunction functionChosen = SearchResultFunction.NONE;
	private volatile String categoryRestriction = null;
	
	private volatile String userInputStr;
	
	public void setUserInputStr(String userInputStr)	{
		this.userInputStr = userInputStr;
	}
	
	public String getUserInputStr() {
		return this.userInputStr;
	}
	
	
	public void run() {
		try {
			String locUserInputStr = userInputStr.trim();
			List<String> userInputList = createUserInputList(locUserInputStr);
			if (((!userInputList.isEmpty()) && userInputList.get(0) != null && userInputList.get(0).equals("search_keyword"))) {
				doWork(userInputList, locUserInputStr);
			}
		}catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/*
	 * Set up Cache Service variable (cacheServ) which enables off-heap storage of cache using
	 * Apache DirectMemory (called once-only)
	 */
	public void setupCacheService() {
		cacheServ = new DirectMemory<String, Map<String, Integer>>()
			    .setNumberOfBuffers( 100 )
			    .setSize( 100000 )
			    .setInitialCapacity( 1000000 )
			    .setConcurrencyLevel( 4 )
			    .newCacheService();
	}
	
	/*
	 *  Establish MySql driver so we can make subsequent connections on the fly (called once-only)
	 */
	public void establishMySQL() {
		try {
			Class.forName("com.mysql.jdbc.Driver");
		}catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	/*
	 *  Synchronized method to invoke 'workhorse' functionality
	 */
	private synchronized void doWork(List<String> inUserInputList, String inUserInputStr) {
		if (validateAndPopulateQueryVars(inUserInputList,inUserInputStr)) {
			performSearch();
			consolidateAndPrintResults();
		}
		else {
			printInvalidUsageMsg();
		}
		
	}
	
	/*
	 * From the blank-delimited userInputStr we create an array list of strings which are unquoted single
	 * words (no blanks) and extracted double-quoted string.
	 * e.g. "search_keyword video \"the best\" \"here and there\" after"
	 * will produce an ArrayList<String> with list.get(0)="search_keyword", list.get(1)="video",
	 * list.get(2)="the best", list.get(3)="here and there", list.get(4)="after"
	 */
	private  List<String> createUserInputList(String inUserInputStr) {
		List<String> userInputList = new ArrayList<String>();
		Pattern regex = Pattern.compile("[^\\s\"]+|\"([^\"]*)\"");
		Matcher regexMatcher = regex.matcher(inUserInputStr);
		while (regexMatcher.find()) {
		    if (regexMatcher.group(1) != null) {
		        // Add double-quoted string without the quotes
		        userInputList.add(regexMatcher.group(1));
		    } else {
		        // Add unquoted word
		        userInputList.add(regexMatcher.group());
		    }
		} 
		return userInputList;
	}
	
	/*
	 * remove hyphens enclosed in double quotes and return result string.   The purpose of this is so that we
	 * can test to see if there are switches (which begin with "-") in the unquoted portions of the original string.
	 */	
	private String removeDoubleQuotedHyphens(String inStr) {
		boolean deleteHyphens = false;
		String outStr = inStr.trim();
		int i = 0;
		while (true) {
			if (i >= outStr.length()) {	
				break;
			}				
		    if(outStr.charAt(i)=='\"'){
		    	//outStr = outStr.substring(0, i) + outStr.substring(i+1, outStr.length());
		    	deleteHyphens = !deleteHyphens;
		    	i++;
		    }
		    else if(outStr.charAt(i)=='-'&& deleteHyphens){
		    	outStr = outStr.substring(0, i) + outStr.substring(i+1, outStr.length());
		    }
		    else {
		    	i++;
		    }
		}
		return outStr;
	}
		
	/*
	 *  Algorithmically determines if there is proper usage of the search_keyword facility outlined in the 
	 *  comments to this method.*  
	 *  
	 *  If usage was correct then populate keyStringSet (for one or more keywords to be searched), functionChosen 
	 *  and categoryRestriction object variables are populated for use in query logic and true is returned.  
	 *  For bad usage return false.
	 */	
	private boolean validateAndPopulateQueryVars(List<String> inUserInputList, String inUserInputStr) {
		
		// Bad usage if nothing after search_keyword
		if (inUserInputList.size() < 2) {
			return false;
		}
		functionChosen = SearchResultFunction.NONE;
		categoryRestriction = "";
		keyStringSet.clear();
		
		// check the number of switches (hyphens) in noHyphensInQuotesUIStr (# of hyphens in unquoted portions of
		// inUserInputStr.  If there is more than one then invalid.  If there are none then we have function 1.   If
		// its one switch then it must be VALID -sum (function 2) or -category (function 3)
		String noHyphensInQuotesUIStr = removeDoubleQuotedHyphens(inUserInputStr);
		
		String findStr = "-";
		int cnt = 0;
		int lastIndex = 0;
		
		while(lastIndex != -1){
		    lastIndex = noHyphensInQuotesUIStr.indexOf(findStr,lastIndex);
 	       	if( lastIndex != -1){
		        cnt++;
		       lastIndex+=findStr.length();
		    }
		}
		// not supposed to have more than one switch
		if (cnt > 1) {
			return false;
		}
		
		// do we have valid function 2 or function 3?
		else if (cnt == 1) {
			//test for function 2 (-sum switch)
			if (noHyphensInQuotesUIStr.lastIndexOf("-sum") > 0) {
				lastIndex = inUserInputStr.lastIndexOf("-sum");
				if (inUserInputStr.substring(lastIndex -1, lastIndex).equals(" ") && 
						(lastIndex + "-sum".length()) >= inUserInputStr.length()) 
				{
					functionChosen = SearchResultFunction.FUNCTION_2;
				}
				else {
					return false;
				}
			}
			// test for function 3 (-category switch)
			// for valid function 3 populate the categoryRestriction string for the query logic.
			else if (noHyphensInQuotesUIStr.lastIndexOf("-category") > 0) {
				lastIndex = inUserInputStr.lastIndexOf("-category");
				if (inUserInputStr.substring(lastIndex -1, lastIndex).equals(" ") && 
						((lastIndex + "-category".length() + 2) < inUserInputStr.length()) &&
						inUserInputStr.substring(lastIndex + "-category".length(), lastIndex + "-category".length() + 1).equals(" ") && 
						!inUserInputStr.substring(lastIndex + "-category".length() ).trim().equals(" "))
				{
					functionChosen = SearchResultFunction.FUNCTION_3;
					categoryRestriction = inUserInputStr.substring(lastIndex + "-category".length() + 1);

				}
				else {
					return false;
				}		
			}
			//switches other than -category / -sum are invalid
			else {
				return false;
			}
		}
		else { //no switches --> function 1
			functionChosen = SearchResultFunction.FUNCTION_1;
		}
		
		
		// based on function 1/ function 2 / function 3, parse the inUserInputList and populate the keyStringSet
		// for the query logic.
		if (functionChosen == SearchResultFunction.FUNCTION_1) {
			for (int i = 1; i < inUserInputList.size() ; i++) {
				keyStringSet.add(inUserInputList.get(i));
			}
		}
		else if (functionChosen == SearchResultFunction.FUNCTION_2) {
			for (int i = 1; i < inUserInputList.size() - 1; i++) {
				keyStringSet.add(inUserInputList.get(i));
			}
		}
		else if (functionChosen == SearchResultFunction.FUNCTION_3) {
			for (int i = inUserInputList.size() - 1; i > 0; i--) {
				if (inUserInputList.get(i).equals("-category")) {
					lastIndex = i;
				}
			}
			for (int i = 1; i < lastIndex ; i++) {
				keyStringSet.add(inUserInputList.get(i));
			}
		}
		return true;
	}
	
	/*
	 *  Print to console the invalid usage message with the string entered and the valid usage forms
	 */
	public  void printInvalidUsageMsg() {
		System.out.println("Invalid usage of search_keyword utility: ");
		System.out.println(userInputStr);
		System.out.println("");
		System.out.println("Proper Usages:");
		System.out.println("(1) search_keyword [Key1 Key2...KeyN]" );
		System.out.println("(2) search_keyword [Key1 Key2...KeyN] -sum" );
		System.out.println("(3) search_keyword [Key1 Key2...KeyN] -category [CatRestriction]" );
		System.out.println("WHERE KeyI is a string enclosed in double quotes OR an unquoted word without blanks");
		System.out.println("AND CatRestriction is a string of one or more words (e.g. Digital Camera) where quotes");
		System.out.println("are not necessary");
	}
	
	
	/*
	 * For each element in keyStringSet, do a cached search (if the KeyString is in DirectMemory cache) for function1 otherwise
	 * do a database search (where function1 keyString map not in cache AND for function2/function3)  based on the preparedStatemet 
	 * formed on query string which is determined by func1/func2/func3 where preparedStatement parameters are fed in from the
	 *  keyStringSet and in the func3 case also the categoryRestriction variable.
	 * Note that since we are in a threaded app this method forms a new database connection (conn variable) and closes it
	 * at the end because we don't want different threads sharing a connection.
	 * Also note that due to consolidation, ordering is not done in SQL query but deferred using Comparator once 
	 * the SEPARATE results for each keyString are consolidated (consolidateAndPrintResults() method)
	 */
	private void performSearch() {	
		StringBuilder querySB = new StringBuilder();
		// the query used for the PreparedStatement depends upon the function used (func1/func2/func3)
		if (functionChosen == SearchResultFunction.FUNCTION_1) {
			querySB.append("SELECT Z.TName,  Z.TotNumKeyWordOccur ");
			querySB.append("FROM ");
			querySB.append("(");
			querySB.append("SELECT A.name AS TName, ");
			querySB.append("A.NumDuplicates * ((LENGTH(A.description) - LENGTH(REPLACE(A.description, ?, ''))) / LENGTH(?)) AS TotNumKeyWordOccur ");
			querySB.append("FROM ");
			querySB.append("(SELECT name, description, COUNT(1) AS NumDuplicates ");
			querySB.append("FROM " + DbTABLE + " " );
			querySB.append("GROUP BY name, description) A ");
			querySB.append(") Z ");
			querySB.append("WHERE Z.TotNumKeyWordOccur > 0 ");					
		}
		else if (functionChosen == SearchResultFunction.FUNCTION_2) { 
			querySB.append("SELECT name, description, SUM(IFNULL(offers_total,0)) AS SumOfOffers ");	
			querySB.append("FROM " + DbTABLE + " ");	
			querySB.append("WHERE description LIKE  ?  ");		
			querySB.append("GROUP BY name, description ");	
		}
		else if (functionChosen == SearchResultFunction.FUNCTION_3) {
			querySB.append("SELECT Z.TName,  Z.TotNumKeyWordOccur ");	
			querySB.append("FROM ");	
			querySB.append("( ");	
			querySB.append("SELECT A.name AS TName, ");	
			querySB.append("A.NumDuplicates * ((LENGTH(A.description) - LENGTH(REPLACE(A.description, ?, ''))) / LENGTH(?)) AS TotNumKeyWordOccur ");	
			querySB.append("FROM ");	
			querySB.append("(SELECT name, description, category, COUNT(1) AS NumDuplicates ");	
			querySB.append("FROM " + DbTABLE + " ");	
			querySB.append("WHERE category = ? ");	
			querySB.append("GROUP BY name, description) A ");	
			querySB.append(") Z ");	
			querySB.append("WHERE Z.TotNumKeyWordOccur > 0 ");	
		}
		Connection conn = null;
		try {
			conn = DriverManager.getConnection(dbURL,dbUser,dbPassword);
			PreparedStatement pStmt = conn.prepareStatement(querySB.toString());	
			//function 1 - database query takes a long time so do cached search if results for keyString is in cache.
			// If results not in cache then do database query and put the results in cache for next search on keyString
			if (functionChosen == SearchResultFunction.FUNCTION_1) {
				for (String tKeyStr : keyStringSet) {
					StringBuilder srchStatSB = new StringBuilder();
					long startTime = System.currentTimeMillis();
					Map<String, Integer> resultMap = null;
					if ((resultMap = cacheServ.retrieve(tKeyStr)) == null) {
						resultMap = new HashMap<String, Integer>();
						pStmt.clearParameters();
						pStmt.setString(1, tKeyStr);
						pStmt.setString(2, tKeyStr);
						ResultSet rs = pStmt.executeQuery();
						if (rs != null) {						
							while (rs.next()) {
								resultMap.put(rs.getString(1), new Integer(rs.getInt(2)));
							}
							rs.close();
							//cacheServ.allocate(tKeyStr, resultMap.getClass(), 10000);
							Pointer ptr = cacheServ.put(tKeyStr, resultMap);
							if (ptr == null) {
								System.out.println("");
								System.out.println("Warning: not able to store current function1 database query result to cache memory. ");
								System.out.println("");
							}
						}
						srchStatSB.append(UNCACHED);
					}	
					else {
						srchStatSB.append(CACHED);
					}
					keyStringToResultsMap.put(tKeyStr, resultMap);
					srchStatSB.append(" Duration= " + (System.currentTimeMillis() - startTime) + "ms");		
					function1SearchStatsMap.put("\"" + tKeyStr + "\"", srchStatSB.toString());
				}
			}
			//function 2 - search is quick so we stick to database query
			else if (functionChosen == SearchResultFunction.FUNCTION_2) {
				for (String tKeyStr : keyStringSet) {
					pStmt.clearParameters();
					pStmt.setString(1, "%" + tKeyStr + "%");
					Map <String, Integer> resultMap = new HashMap<String, Integer>();
					ResultSet rs = pStmt.executeQuery();
					if (rs != null) {						
						while (rs.next()) {
							resultMap.put(rs.getString(1), new Integer(rs.getInt(3)));
						}
						rs.close();
					}
					keyStringToResultsMap.put(tKeyStr, resultMap);					
				}
			}
			//function 3 - search is quick so we stick to database query
			else if (functionChosen == SearchResultFunction.FUNCTION_3) {
				for (String tKeyStr : keyStringSet) {
					pStmt.clearParameters();
					pStmt.setString(1, tKeyStr);
					pStmt.setString(2, tKeyStr);
					pStmt.setString(3, categoryRestriction);
					Map <String, Integer> resultMap = new HashMap<String, Integer>();
					ResultSet rs = pStmt.executeQuery();
					if (rs != null) {						
						while (rs.next()) {
							resultMap.put(rs.getString(1), new Integer(rs.getInt(2)));
						}
						rs.close();
					}
					keyStringToResultsMap.put(tKeyStr, resultMap);
				}
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		finally {
			// close connection since we don't want different threads to share an open connection
			try {
				if (conn != null) {
					conn.close();
				}
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}
	}
	
	
	/*
	 * (1)Consolidate the results from the keyString queries.  
	 * (2)Order results in descending order of amount (sum of offers/keyword# total) using Comparator
	 * (3) Print the results
	 */
	private  void consolidateAndPrintResults() {		
		StringBuilder resultDescSB = new StringBuilder();
		boolean isFirstSearchStr = true;
		for(Map.Entry<String, Map<String, Integer>> mapEntry : keyStringToResultsMap.entrySet()) { 
			// Print header results for search
			if (isFirstSearchStr) {
				if (functionChosen == SearchResultFunction.FUNCTION_2) {
					resultDescSB.append("Sum of offers for at least one occurence in description of any of ");
					resultDescSB.append("\"" + mapEntry.getKey() + "\"");
				}
				else {
					if (functionChosen == SearchResultFunction.FUNCTION_3) {
						resultDescSB.append("For Category = ");
						resultDescSB.append("\"" + categoryRestriction + "\", ");
					}
					resultDescSB.append("Sum of number of occurences in description of each of  ");
					resultDescSB.append("\"" + mapEntry.getKey() + "\"");
				}
				isFirstSearchStr = false;
			}
			else {
				resultDescSB.append(", ");
				resultDescSB.append("\"" + mapEntry.getKey() + "\"");
			}
			
			// populate the consolidated results map
			Map<String, Integer> locResultMap  = mapEntry.getValue();
			for(Map.Entry<String, Integer> tEntry : locResultMap.entrySet()) { 
				int amount = tEntry.getValue().intValue();
				// for function1, function3 we sum up the occurrences over each keyword checked
				// in the description
				if (functionChosen != SearchResultFunction.FUNCTION_2 && 
						consolidatedResultsMap.containsKey(tEntry.getKey())) 
				{
					amount += consolidatedResultsMap.get(tEntry.getKey()).intValue();
				}
				consolidatedResultsMap.put(tEntry.getKey(), new Integer(amount));
			}
		}
		if (isFirstSearchStr) {  // empty search
			System.out.println("");
			System.out.println("No results found for search");
			System.out.println("");
		}
		else {
			// order the consolidated results (based on values (amount) ) using Comparator logic
			NavigableMap <String, Integer> sortedConsolidatedResultMap = new TreeMap <String, Integer>();
			Map sortedMap = sortMapByValue(consolidatedResultsMap);
			if (sortedMap instanceof TreeMap<?, ?>) {
				TreeMap <String, Integer >sortedConsolidatedResultMapASCVal = (TreeMap<String, Integer>) sortedMap;			
				sortedConsolidatedResultMap = sortedConsolidatedResultMapASCVal.descendingMap();
			}
			// print the ordered (by value) consolidated results map
			// Name/Amount headings first
			System.out.println("");
			System.out.println(resultDescSB.toString());
			System.out.println("");
			StringBuilder nameTitleSB = new StringBuilder();
			nameTitleSB.append("Name");
			for (int i = 0; i < MAX_DISPLAY_LENGTH_NAME - 4; i++) {
				nameTitleSB.append(" ");
			}
			StringBuilder nameUnderlineSB = new StringBuilder();
			for (int i = 0; i < MAX_DISPLAY_LENGTH_NAME; i++) {
				nameUnderlineSB.append("-");
			}
			String amountTitle = ((functionChosen == SearchResultFunction.FUNCTION_2) ? "Sum of Offers" : "Keywords#");
			StringBuilder amountUnderlineSB = new StringBuilder();
			for (int i = 0; i < amountTitle.length(); i++) {
				amountUnderlineSB.append("-");
			}
			System.out.println("");
			System.out.println(nameTitleSB.toString() + "  " + amountTitle);
			System.out.println(nameUnderlineSB.toString() + "  " + amountUnderlineSB.toString());
			
			// now iteratively print out sorted results
			for(Map.Entry<String, Integer> mapEntry : sortedConsolidatedResultMap.entrySet()) { 
				StringBuilder nameSB = new StringBuilder();
				if (mapEntry.getKey().length() > MAX_DISPLAY_LENGTH_NAME) {
					nameSB.append(mapEntry.getKey().substring(0, MAX_DISPLAY_LENGTH_NAME));
				}
				else if (mapEntry.getKey().length() < MAX_DISPLAY_LENGTH_NAME) {
					nameSB.append(mapEntry.getKey());
					while (nameSB.length() < MAX_DISPLAY_LENGTH_NAME) {
						nameSB.append(" ");
					}
				}
				else if (mapEntry.getKey().length() == MAX_DISPLAY_LENGTH_NAME) {
					nameSB.append(mapEntry.getKey());
				}
				System.out.println(nameSB.toString() + "  " + mapEntry.getValue().toString());
				
			}
			
			// for function1, print out cached/database search stats for each keyWord
			if (functionChosen == SearchResultFunction.FUNCTION_1) {
				System.out.println("");
				System.out.println("Function1 stats by keyword:");
				for(Map.Entry<String, String> mapEntry : function1SearchStatsMap.entrySet()) { 
					System.out.println(mapEntry.getKey() + ": " + mapEntry.getValue());
				}
			}
			System.out.println("");
		}
		// this method is called by synchronized method (doWork()) so clear the maps for the next thread
		function1SearchStatsMap.clear();
		consolidatedResultsMap.clear();
		keyStringToResultsMap.clear();
	}
	
	/*
     * This method
     * 1. Constructs a sorted map object providing a Comparator class
     * 2. Populates the sorted map by calling putAll with the unsorted 
     *     data. and when putAll is called, the comparator is used to do the
     *     ordering (ascending values)
     * 3. The method returns the sorted map.
     */
    static private <K, V extends Comparable<V>> Map<K, V> sortMapByValue(Map<K, V> unsortedMap) {
        SortedMap<K, V> sortedMap = new TreeMap<K, V>(new ValueComparer<K, V>(unsortedMap) );
        sortedMap.putAll(unsortedMap);
        return sortedMap;
    }

	
	/*
     * An inner class that implements a Comparator to compare the VALUES inside the map where 
     * the constructor takes the values of the unsorted map
     */
    private static class ValueComparer<K, V extends Comparable<V>> implements Comparator<K> {

        private final Map<K, V> map;

        public ValueComparer(Map<K, V> map) {
            super();
            this.map = map;
        }

        public int compare(K key1, K key2) {
            V value1 = this.map.get(key1);
            V value2 = this.map.get(key2);
            int c = value1.compareTo(value2);
            if (c != 0) {
                return c;
            }
            Integer hashCode1 = key1.hashCode();
            Integer hashCode2 = key2.hashCode();
            return hashCode1.compareTo(hashCode2);
        }
    }

}
