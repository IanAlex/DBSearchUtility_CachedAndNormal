package com.dbsearchutility.ija;


import java.util.*;

public class SearchKeyWordDriver {
	
	
	/*
	 *  utility reads from console and starts a SearchKeyWordService thread if user enters a command beginning with
	 *  "search_keyword".   The application is stopped when user enters in "stop search_keyword"
	 */
	public static void main(String[] args) {
		try {
			System.out.println("search_keyword utility is started");
			Scanner input = new Scanner(System.in);
			long threadCnt = 1;
			SearchKeywordService srchKWService = new SearchKeywordService();
			srchKWService.establishMySQL();
			srchKWService.setupCacheService();
			while (true) {
				String userInputStr = input.nextLine();
				// condition to fork a new thread
				if (userInputStr.startsWith("search_keyword")) {
					srchKWService.setUserInputStr(userInputStr);
					(new Thread(srchKWService, "Thread" + threadCnt)).start();
					Thread.sleep(1000);
					threadCnt++;
				}
				// condition where user exits the app.
				else if (userInputStr.trim().equals("stop search_keyword")) {
					System.out.println("search_keyword utility is stopped");
					break;
				}
			}
			System.exit(0);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	

}
