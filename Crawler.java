import java.io.*;
import java.lang.management.MemoryNotificationInfo;
import java.net.*;
import java.util.regex.*;
import java.sql.*;
import java.sql.Connection;
import java.util.*;
import java.util.concurrent.*;

import javax.management.Notification;
import javax.management.NotificationListener;

import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.html.HtmlParser;
import org.jsoup.*;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.xml.sax.SAXException;

public class Crawler
{
	//final static int MAX_THREADS = 8;
	final int QUEUE_UPPER_BOUND = 100;
	Connection connection;
	int urlID;
	volatile int urlCounter;
	int maxUrls;
	public int maxThreads;
	String restrictedDomain;
	public Properties props;
	//public ConcurrentLinkedDeque<UrlIdPair> queue;
	public LinkedList<UrlIdPair> queue;
	public HashMap<String, LinkedHashSet<Integer>> wordsCache;
	private Object cacheLock = new Object();
	//private Object queueLock = new Object();
	//private Object databaseLock = new Object();
	boolean doneCrawling = false;

	Crawler() {
		urlID = 0;
		queue = new LinkedList<UrlIdPair>();
		wordsCache = new HashMap<>();
	}

	public boolean isQueueEmpty(){
		synchronized(this){
			if(urlCounter > maxUrls) return true;
			else return queue.isEmpty();
		}
	}

    public boolean isDoneCrawling() {
		synchronized(this){
			return doneCrawling;
		}
	}
	
	public void refreshQueue(){
		String sql = "SELECT * FROM urls WHERE urlid BETWEEN ? AND ?";
		PreparedStatement query;
		try {
			query = connection.prepareStatement(sql);
			query.setInt(1, urlCounter);
			query.setInt(2, urlCounter+QUEUE_UPPER_BOUND);
			ResultSet result = query.executeQuery();
			while(result.next()){
				int id = result.getInt("urlid");
				String url = result.getString("url");
				queue.addLast(new UrlIdPair(url, id));
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public UrlIdPair getNextUrl(){
		synchronized(this){
			if(urlCounter > maxUrls){
				doneCrawling = true;
				return null;
			}
			else{
				if(queue.size() < 2*maxThreads){
					refreshQueue();
					if(queue.isEmpty()){
						//doneCrawling = true;
						return null;
					}
				}

				System.out.println("Thread "+Thread.currentThread().getId()+": "+urlCounter);
				urlCounter++;
				return queue.pollFirst();
			}
		}
	}

	public void readProperties() throws IOException {
      		props = new Properties();
      		FileInputStream in = new FileInputStream("database.properties");
      		props.load(in);
      		in.close();

      		maxUrls = Integer.parseInt(props.getProperty("crawler.maxurls"));
      		restrictedDomain = props.getProperty("crawler.domain");
	}

	public void openConnection() throws SQLException, IOException
	{
		String drivers = props.getProperty("jdbc.drivers");
  		if (drivers != null) System.setProperty("jdbc.drivers", drivers);

  		String url = props.getProperty("jdbc.url");
  		String username = props.getProperty("jdbc.username");
  		String password = props.getProperty("jdbc.password");

		connection = DriverManager.getConnection( url, username, password);
   	}

	public void createDB() throws SQLException, IOException {
		openConnection();

        Statement stat = connection.createStatement();
		
		// Delete the table first if any
		try {
			stat.executeUpdate("DROP TABLE URLS");
			stat.executeUpdate("DROP TABLE words");
		}
		catch (Exception e) {
		}
			
		// Create the table
        stat.executeUpdate("CREATE TABLE URLS (urlid INT, url VARCHAR(512), description VARCHAR(200))");
        stat.executeUpdate("CREATE TABLE words (word VARCHAR(100), urllist TEXT)");
	}

	public boolean urlInDB(String urlFound) throws SQLException, IOException {
        //Statement stat = connection.createStatement();
		//ResultSet result = stat.executeQuery( "SELECT * FROM urls WHERE url LIKE '"+urlFound+"'");
		
		if(urlFound.endsWith("/")){
			urlFound = urlFound.substring(0, urlFound.length()-1);
		}
		String sql = "SELECT * FROM urls WHERE url LIKE ?";
		PreparedStatement query = connection.prepareStatement(sql);
		query.setString(1, urlFound);
		ResultSet result = query.executeQuery();

		if (result.next()) {
	        //System.out.println("URL "+urlFound+" already in DB");
			return true;
		}
	       // System.out.println("URL "+urlFound+" not yet in DB");
		return false;
	}

	public int insertURLInDB( String url) throws SQLException, IOException {
        /*Statement stat = connection.createStatement();
		String query = "INSERT INTO urls VALUES ('"+urlID+"','"+url+"','')";
		//System.out.println("Executing "+query);
		stat.executeUpdate( query );*/
		
		if(url.endsWith("/")){
			url = url.substring(0, url.length()-1);
		}
		
		String sql = "INSERT INTO urls VALUES (?,?,'')";
		PreparedStatement query = connection.prepareStatement(sql);
		query.setInt(1, urlID);
		query.setString(2, url);
		//query.setString(3, content.substring(0, 100));
		query.executeUpdate();
		urlID++;
		return urlID-1;
	}
	
	public void updateUrlDescription(String url, String description) throws SQLException{
		//System.out.println(url+": "+description.substring(0, 15));
		if(url.endsWith("/")){
			url = url.substring(0, url.length()-1);
		}
		String sql = "UPDATE urls SET description=? WHERE url LIKE ?";
		PreparedStatement query = connection.prepareStatement(sql);
		query.setString(1, description);
		query.setString(2, url);
		query.executeUpdate();
	}
	
	public void updateUrlDescription(int id, String description) throws SQLException{
		String sql = "UPDATE urls SET description=? WHERE urlid=?";
		PreparedStatement query = connection.prepareStatement(sql);
		query.setString(1, description);
		query.setInt(2, id);
		query.executeUpdate();
	}
	
	public void insertWordsIntoCache(int id, String content) {
		String patternString = "[a-zA-Z0-9\\-']+";
		Pattern pattern = Pattern.compile(patternString, Pattern.CASE_INSENSITIVE);
		Matcher matcher = pattern.matcher(content);
		while(matcher.find()){
			synchronized(cacheLock){
				String word = matcher.group().toLowerCase(Locale.ENGLISH);
				if(word.length() > 50) continue;
				LinkedHashSet<Integer> list = wordsCache.get(word);
				if(list==null){
					list = new LinkedHashSet<>();
					wordsCache.put(word, list);
				}
				list.add(id);
			}
		}
	}
	
	public void writeCacheToTxt(){
		PrintWriter out = null;
		try {
			out = new PrintWriter(new BufferedWriter(new FileWriter("words.cache")));
			for(Map.Entry<String, LinkedHashSet<Integer>> entry : wordsCache.entrySet()){
				LinkedHashSet<Integer> set = entry.getValue();
				StringBuilder urlList = new StringBuilder();
				for(Integer i : set){
					urlList.append(i);
					urlList.append(",");
				}
				out.println(entry.getKey()+"\t"+urlList.toString());
			}
			out.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}
		finally{
			if(out!=null){
				out.close();
			}
		}
		
		String sql = "LOAD DATA LOCAL INFILE 'words.cache' INTO TABLE words LINES TERMINATED BY '"+System.lineSeparator()+"';";
		try {
			Statement stat = connection.createStatement();
			stat.execute(sql);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void writeCacheToDB(){
		String sql = "INSERT INTO words VALUES (?,?)";
		try {
			PreparedStatement query = connection.prepareStatement(sql);
			int count = 0, batchSize = 50;
			for(Map.Entry<String, LinkedHashSet<Integer>> entry : wordsCache.entrySet()){
				LinkedHashSet<Integer> set = entry.getValue();
				StringBuilder urlList = new StringBuilder();
				for(Integer i : set){
					urlList.append(i);
					urlList.append(",");
				}
				query.setString(1, entry.getKey());
				query.setString(2, urlList.toString());
				query.addBatch();
				
				if(++count % batchSize == 0) {
					synchronized(this){
						query.executeBatch();
					}
			    }
			}
			query.executeBatch();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void insertWordsInDB(int id, String content) throws SQLException{
		long startTime;
		String patternString = "[a-zA-Z\\-']+";
		Pattern pattern = Pattern.compile(patternString, Pattern.CASE_INSENSITIVE);
		Matcher matcher = pattern.matcher(content);
		
		int batchSize = 1000;
		int count = 0;
		String sql = "INSERT INTO words VALUES (?,?)";
		PreparedStatement query = connection.prepareStatement(sql);
		while(matcher.find()){
			query.setString(1, matcher.group());
			query.setInt(2, id);
			query.addBatch();
			
			if(++count % batchSize == 0) {
				synchronized(this){
					startTime = System.currentTimeMillis();
					query.executeBatch();
					System.out.println(id+" inserting words took "+(System.currentTimeMillis()-startTime)/1000+" seconds. Content length: "+content.length());
					//System.out.println("Thread "+Thread.currentThread().getId()+" just executed a batch insert.");
				}
		    }
		}
		synchronized(this){
			startTime = System.currentTimeMillis();
			query.executeBatch();
			System.out.println(id+" inserting words took "+(System.currentTimeMillis()-startTime)/1000+" seconds. Content length: "+content.length());
			//System.out.println("Thread "+Thread.currentThread().getId()+" just executed a batch insert.");
		}
		
	}

	public String makeAbsoluteUrl(String url, String parentUrl) {
		if (url.indexOf(":")>0) {
			// the protocol part is already there.
			return url;
		}

		if (url.length() > 0 && url.charAt(0) == '/') {
			// It starts with '/'. Add only host part.
			int posHost = parentUrl.indexOf("://");
			if (posHost < 0) {
				return url;
			}
			int posAfterHost = parentUrl.indexOf("/", posHost+3);
			if (posAfterHost < 0) {
				posAfterHost = parentUrl.length();
			}
			String hostPart = parentUrl.substring(0, posAfterHost);
			return hostPart + url;
		} 

		// URL start with a char different than "/"
		int pos = parentUrl.lastIndexOf("/");
		int posHost = parentUrl.indexOf("://");
		if (posHost <0) {
			return url;
		}
		if(pos == parentUrl.length()-1){
			return parentUrl + url;
		}
		else{
			return parentUrl + "/" + url;
		}
	}
	
	public void fetchURL(String urlScanned, int urlScannedId) {
		Document doc = null;
		try {
			URL url = new URL(urlScanned);
			//System.out.println("Thread "+Thread.currentThread().getId()+": urlscanned="+urlScanned+" url.path="+url.getPath());
			
			doc = Jsoup.connect(urlScanned).get();
			//System.out.println("\n\n"+doc.body().text()+"\n\n");
			String content = doc.title() + " " + doc.body().text();
			synchronized(this){
				if(content.length() > 100){
					updateUrlDescription(urlScannedId, content.substring(0, 100));
				}
				else{
					updateUrlDescription(urlScannedId, content);
				}
				//insertWordsInDB(urlScannedId, content);
			}
			//insertWordsInDB(urlScannedId, content);
			
			Elements links = doc.select("a");
			for(Element anchor : links){
				String urlFound = anchor.attr("href");
				urlFound = makeAbsoluteUrl(urlFound, urlScanned);
				//System.out.println("urlscannedId "+urlScannedId+": "+urlFound);
				if(!urlFound.contains(restrictedDomain)){
					continue;
				}
				synchronized(this){
					if (!urlInDB(urlFound)) {
						int currentId = insertURLInDB(urlFound);
						/*UrlIdPair pair = new UrlIdPair(urlFound, currentId);
						queue.addLast(pair);*/
					}		
				}
			}
		}
		catch (MalformedURLException e){
			System.out.println("Bad url: "+urlScanned);
		}
		catch (UnsupportedMimeTypeException e){
			System.out.println("Not text/html: "+urlScanned);
		}
		catch (SocketTimeoutException e){
			System.out.println("Url timed out: "+urlScanned);
		}
		catch (UnknownServiceException e){
			
		}
		catch (NullPointerException e){
			System.out.println("NullPointerException: \n\tUrl: "+urlScanned);
			System.out.println("\tdoc==null? "+doc.body()==null);
		}
		catch (java.nio.charset.IllegalCharsetNameException e){
			System.out.println("Url is using an unsupported charset: "+urlScanned);
		}
		catch (HttpStatusException e){
			System.out.println("HTTP error fetching "+e.getUrl()+". Status="+e.getStatusCode());
		}
  		catch (Exception e)
  		{
   			e.printStackTrace();
  		}
	}
	
	public void insertListOfUrls(ArrayList<String> links){
		for(String anchor : links){
			if(!anchor.contains(restrictedDomain)||anchor.startsWith("mailto:")){
				continue;
			}
			synchronized(this){
				try {
					if (!urlInDB(anchor)) {
						insertURLInDB(anchor);
						/*UrlIdPair pair = new UrlIdPair(urlFound, currentId);
						queue.addLast(pair);*/
					}
				} catch (SQLException | IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}		
			}
		}
	}
	
	public void crawl(){
		try {
			readProperties();
			createDB();

			maxThreads = Integer.parseInt(props.getProperty("crawler.numthreads"));
			CrawlerRunnable[] crawlerArray = new CrawlerRunnable[maxThreads];
			String root = props.getProperty("crawler.root");
			insertURLInDB(root);
			for(int i=0;i<maxThreads;i++){
				crawlerArray[i] = new CrawlerRunnable(this);
				new Thread(crawlerArray[i]).start();
			}
			
			while(!doneCrawling){
				try {
					Thread.sleep(3000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			for(int i=0;i<maxThreads;i++){
				while(!crawlerArray[i].isDoneRunning()){
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
			
			System.out.println("Writing cache to the database...");
			//writeCacheToDB();
			writeCacheToTxt();
			
			System.out.println("Done crawling.");
			System.out.println("Cache contains "+wordsCache.size()+" mappings.");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

   	/*public static void main(String[] args)
   	{
		Crawler crawler = new Crawler();
	
		long startTime = System.currentTimeMillis();
		crawler.crawl();
		long totalTime = System.currentTimeMillis()-startTime;
		System.out.println("Finished in "+(totalTime/1000f)+" seconds");
		System.out.println("Each url averaged "+(totalTime/1000f/crawler.maxUrls)+" seconds");
	}*/

	public class CrawlerRunnable implements Runnable{
		private String urlAddr;
		private int id;
		private Crawler crawler;
		private HtmlParser parser;
		private boolean doneRunning;
		
		public CrawlerRunnable(Crawler c){
			this.crawler = c;
			parser = new HtmlParser();
			doneRunning = false;
		}
		
		public boolean isDoneRunning(){
			return doneRunning;
		}

		public void run(){
			while(true){
				UrlIdPair pair = crawler.getNextUrl();
				if(pair==null){ //the queue is empty
					if(crawler.isDoneCrawling()){
						doneRunning = true;
						break;
					}
					else{
						try {
							Thread.sleep(1000);
							continue;
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
				urlAddr = pair.getUrl();
				id = pair.getId();
				
				URL url;
				InputStream stream = null;
				try {
					url = new URL(urlAddr);
					HttpContentHandler handler = new HttpContentHandler(url.getProtocol()+"://"+url.getHost());
					Metadata metadata = new Metadata();
					ParseContext context = new ParseContext();
					
					stream = url.openStream();
					parser.parse(stream, handler, metadata, context);
					crawler.insertListOfUrls(handler.getAnchors());
					String description = handler.getBodyText();
					/*if(description.length() > 50){
						System.out.println("Thread "+Thread.currentThread().getId()+": "+urlAddr+" - "+description.replace('\n', ' ').substring(0,  50));
					}
					else{
						System.out.println("Thread "+Thread.currentThread().getId()+": "+urlAddr+" - "+description.replace('\n', ' '));
					}*/
					if(description.length() > 100){
						crawler.updateUrlDescription(id, description.substring(0, 100));
					}
					else{
						crawler.updateUrlDescription(id, description);
					}
					crawler.insertWordsIntoCache(id, description);
				}
				catch (FileNotFoundException e){
					
				}
				catch (ConnectException e){
					
				}
				catch (MalformedURLException e){
					System.out.println("Bad url: "+urlAddr);
				}
				catch (UnsupportedMimeTypeException e){
					System.out.println("Not text/html: "+urlAddr);
				}
				catch (SocketTimeoutException e){
					System.out.println("Url timed out: "+urlAddr);
				}
				catch (UnknownServiceException e){
					
				}
				catch (NullPointerException e){
					System.out.println("NullPointerException: \n\tUrl: "+urlAddr);
				}
				catch (java.nio.charset.IllegalCharsetNameException e){
					System.out.println("Url is using an unsupported charset: "+urlAddr);
				}
				catch (HttpStatusException e){
					System.out.println("HTTP error fetching "+e.getUrl()+". Status="+e.getStatusCode());
				}
		  		catch (SAXException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} 
				catch (TikaException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				catch (IOException e){
					
				}
				catch (Exception e){
		   			e.printStackTrace();
		  		} 
				finally{
					if(stream!=null){
						try {
							stream.close();
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
			}
			System.out.println("---Thread "+Thread.currentThread().getId()+" is done crawling.");
		}
	}
	
	private class UsageListener implements NotificationListener{
		@Override
		public void handleNotification(Notification notif, Object handback) {
			String notifType = notif.getType();
            if (notifType.equals(MemoryNotificationInfo.MEMORY_THRESHOLD_EXCEEDED)) {
                new Thread(new Runnable(){
                	public void run(){
                		synchronized(cacheLock){
                			writeCacheToDB();
                			wordsCache.clear();
                		}
                	}
                }).start();
            }
		}
	}
    
    public class UrlIdPair{
    	private String url;
    	private int id;
    	
    	public UrlIdPair(String url, int id){
    		this.url = url;
    		this.id = id;
    	}
    	
    	public String getUrl(){
    		return this.url;
    	}
    	
    	public int getId(){
    		return this.id;
    	}
    }
}