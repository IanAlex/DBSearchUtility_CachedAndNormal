# DBSearchUtility_CachedAndNormal

This a multi-threaded app which runs form the console.  It utilizes the executable JAR created by Maven (IanDBSearchUtility.jar).

Note: the database table used is ija_tp.JsonData (set up in the Ian_JSONToRDBMS repository) and is setup as: ija_tp.JsonData with colums:
(name VARCHAR(10500),description VARCHAR(10500),category VARCHAR(300), offers_total INT)

Caching is done via the Apache Direct Memory library (which was retired but still available at time this application was written; referebced in pom.xml).

Usages:
-------

(1) search_keyword [Key1 Key2...KeyN] 

The utility accepts a multiple keywords as input argument and returns list of records from database table (ija_tp.JsonData) which have these keywords in their description (description colum).  The search results are consolidated by keyword groupings.  Keywords are iterated upon and the output result (per keyword) is  grouped by & sorted by frequency of given keyword in description field (i.e. the more times a keyword is found in description field the higher position of this record in return result)

For this particular usage, the search utility makes use of caching techniques so that searches for the same keyword don't always have to be rerun against the database.   Thus,  there will be stats at the bottom of search results giving the duration stats along with search type (database search vs. cached search) for each of the keystrings searched in a thread.

(2) search_keyword [Key1 Key2...KeyN] -category [CatRestriction]

Again multiple keywords are used for input arguments but limits the search results (per keyword iteration) to records that have the provided category (e.g. search_keyword video --category Digital Cameras).  For each keyword it will produce the number of  Search execution of this function is faster against the database than doing an unrestricted keyword search, and thus caching is not used here.

(3) search_keyword [Key1 Key2...KeyN] -sum

For each keyword, this will return the sum of offers (using the offers_total field).

(4) Stopping the application:
  keyin:  stop search_keyword 
OR use Ctl-C


Notes:
------

- For first 3 usages above: KeyI (I=1,...N) is a string enclosed in double quotes OR an unquoted word without blanks

- In the results, the names are truncated to 60 characters in order to be able to fit the screen.

-  when the application is first started, you'll see 3 lines beginning with 
"SLF4J: Failed to load class "org.slf4j.impl.StaticLoggerBinder"
--> this is a bug when initializing Apache DirectMemory caching and is not of concern (ie. please ignore).



