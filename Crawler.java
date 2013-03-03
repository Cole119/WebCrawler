import java.io.*;
import java.net.*;
import java.util.regex.*;
import java.sql.*;
import java.sql.Connection;
import java.util.*;
import java.util.concurrent.*;
import org.jsoup.*;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class Crawler
{
	final static int MAX_THREADS = 10;
	Connection connection;
	int urlID;
	volatile int urlCounter;
	int maxUrls;
	String restrictedDomain;
	public Properties props;
	public ConcurrentLinkedDeque<UrlIdPair> queue;
	private Object queueLock;

	Crawler() {
		urlID = 0;
		queue = new ConcurrentLinkedDeque<UrlIdPair>();
		queueLock = new Object();
	}

	public boolean isQueueEmpty(){
		synchronized(this){
			if(urlCounter > maxUrls) return true;
			else return queue.isEmpty();
		}
	}

	public UrlIdPair getNextUrl(){
		synchronized(this){
			urlCounter++;
			System.out.println("Thread "+Thread.currentThread().getId()+": "+urlCounter);
			if(urlCounter > maxUrls) return null;
			else return queue.pollFirst();
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
        stat.executeUpdate("CREATE TABLE words (word VARCHAR(100), urlid INT)");
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

	public int insertURLInDB( String url, String content) throws SQLException, IOException {
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
	
	public void insertWordsInDB(int id, String content) throws SQLException{
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
					query.executeBatch();
					System.out.println("Thread "+Thread.currentThread().getId()+" just executed a batch insert.");
				}
		    }
		}
		synchronized(this){
			query.executeBatch();
			System.out.println("Thread "+Thread.currentThread().getId()+" just executed a batch insert.");
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
			insertWordsInDB(urlScannedId, content);
			
			Elements links = doc.select("a");
			for(Element anchor : links){
				String urlFound = anchor.attr("href");
				urlFound = makeAbsoluteUrl(urlFound, urlScanned);
				if(!urlFound.contains(restrictedDomain)){
					continue;
				}
				synchronized(this){
					if (!urlInDB(urlFound)) {
						int currentId = insertURLInDB(urlFound, "");
						UrlIdPair pair = new UrlIdPair(urlFound, currentId);
						queue.addLast(pair);
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

   	public static void main(String[] args)
   	{
		Crawler crawler = new Crawler();
		BlockingQueue<Runnable> blockingQueue = new ArrayBlockingQueue<Runnable>(10);
    	RejectedExecutionHandler rejectedExecutionHandler = 
    				new ThreadPoolExecutor.CallerRunsPolicy();
    	ExecutorService executorService = 
    				new ThreadPoolExecutor(MAX_THREADS, MAX_THREADS, 0L, TimeUnit.MILLISECONDS, blockingQueue, rejectedExecutionHandler);

		try {
			crawler.readProperties();
			String root = crawler.props.getProperty("crawler.root");
			crawler.createDB();
			crawler.insertURLInDB(root, "");
			//queue.addLast(root);
			crawler.fetchURL(root, 0);
			String nextUrl;
			/*while((nextUrl=crawler.getNextUrl())!=null){
				executorService.execute(crawler.new CrawlerRunnable(nextUrl));
				//crawler.fetchURL(root);
			}*/
			while(!crawler.isQueueEmpty()){
				executorService.execute(crawler.new CrawlerRunnable(crawler));
				//crawler.fetchURL(root);
			}
			executorService.shutdown();
			System.out.println("Done.");
		}
		catch( Exception e) {
         		e.printStackTrace();
		}
	}

    public class CrawlerRunnable implements Runnable{
		private String url;
		private int id;
		private Crawler crawler;
		
		public CrawlerRunnable(Crawler c){
			this.crawler = c;
		}

		public void run(){
			UrlIdPair pair = crawler.getNextUrl();
			if(pair==null){ //the queue is empty
				return;
			}
			url = pair.getUrl();
			id = pair.getId();
			//System.out.println("Thread "+Thread.currentThread().getId()+": "+url);
			fetchURL(url, id);
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