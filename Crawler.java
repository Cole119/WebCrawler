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
	public ConcurrentLinkedDeque<String> queue;
	private Object queueLock;

	Crawler() {
		urlID = 0;
		queue = new ConcurrentLinkedDeque<String>();
		queueLock = new Object();
	}

	public boolean isQueueEmpty(){
		synchronized(this){
			if(urlCounter > maxUrls) return true;
			else return queue.isEmpty();
		}
	}

	public String getNextUrl(){
		synchronized(this){
			urlCounter++;
			//System.out.println("Thread "+Thread.currentThread().getId()+": "+urlCounter);
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
			stat.executeUpdate("DROP TABLE word");
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

	public void insertURLInDB( String url, String content) throws SQLException, IOException {
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

   	/*public void fetchURL(String urlScanned) {
		try {
			URL url = new URL(urlScanned);
			System.out.println("Thread "+Thread.currentThread().getId()+": urlscanned="+urlScanned+" url.path="+url.getPath());
			
			Document doc = Jsoup.connect(urlScanned).get();
			Elements links = doc.select("a");
			for(Element e : links){
				System.out.println("**"+e.attr("href"));
			}
			// open reader for URL
			InputStreamReader in = 
   				new InputStreamReader(url.openStream());

			// read contents into string builder
			StringBuilder input = new StringBuilder();
			int ch;
			while ((ch = in.read()) != -1) {
         			input.append((char) ch);
			}

 			// search for all occurrences of pattern
			//String patternString =  "<a\\s+href\\s*=\\s*(\"[^\"]*\"|[^\\s>]*)\\s*>";
			String patternString =  "<a\\s+href\\s*=\\s*(\"([^\"]*)\"|[^\\s>]*)\\s*>";

			Pattern pattern = 			
     			Pattern.compile(patternString, 
     			Pattern.CASE_INSENSITIVE);
			Matcher matcher = pattern.matcher(input);
		
			while (matcher.find()) {
				int start = matcher.start();
				int end = matcher.end();
				String match = input.substring(start, end);
				String urlFound = matcher.group(2);
				if(urlFound == null){
					continue;
				}

				//check for relative url
				urlFound = makeAbsoluteUrl(urlFound, urlScanned);
				if(urlFound.startsWith("/")){
					urlFound = url.getProtocol() + "://" + url.getHost() + urlFound;
				}
				//System.out.println(urlFound);

				// Check if it is already in the database
				synchronized(this){
					if (!urlInDB(urlFound)) {
						insertURLInDB(urlFound);
						queue.addLast(urlFound);
					}		
				}		
	
    				//System.out.println(match);
 			}

		}
		catch (UnknownServiceException e){
			
		}
  		catch (Exception e)
  		{
   			e.printStackTrace();
  		}
	}*/
	
	public void fetchURL(String urlScanned) {
		Document doc = null;
		try {
			URL url = new URL(urlScanned);
			//System.out.println("Thread "+Thread.currentThread().getId()+": urlscanned="+urlScanned+" url.path="+url.getPath());
			
			doc = Jsoup.connect(urlScanned).get();
			//System.out.println("\n\n"+doc.body().text()+"\n\n");
			String content = doc.title() + " " + doc.body().text();
			synchronized(this){
				if(content.length() > 100){
					updateUrlDescription(urlScanned, content.substring(0, 100));
				}
				else{
					updateUrlDescription(urlScanned, content);
				}
			}
			
			Elements links = doc.select("a");
			for(Element anchor : links){
				String urlFound = anchor.attr("href");
				urlFound = makeAbsoluteUrl(urlFound, urlScanned);
				if(!urlFound.contains(restrictedDomain)){
					continue;
				}
				synchronized(this){
					if (!urlInDB(urlFound)) {
						insertURLInDB(urlFound, "");
						queue.addLast(urlFound);
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
			//queue.addLast(root);
			crawler.fetchURL(root);
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
		private Crawler crawler;
		
		public CrawlerRunnable(Crawler c){
			this.crawler = c;
		}
		public CrawlerRunnable(String url){
			this.url = url;
		}

		public void run(){
			if(url==null){
				url = crawler.getNextUrl();
				if(url==null){ //the queue is empty
					return;
				}
				//System.out.println("Thread "+Thread.currentThread().getId()+": "+url);
			}
			fetchURL(url);
		}
	}
}