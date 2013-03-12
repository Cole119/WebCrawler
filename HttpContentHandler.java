import java.util.ArrayList;
import java.util.regex.Pattern;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;


public class HttpContentHandler extends DefaultHandler{
	StringBuilder text;
	String baseUrl;
	ArrayList<String> urlsFound;
	private final static Pattern NONTEXT_EXTENSIONS = Pattern.compile(".*(\\.(css|js|bmp|gif|jpe?g" 
														            + "|png|tiff?|mid|mp2|mp3|mp4"
														            + "|wav|avi|mov|mpeg|ram|m4v|pdf" 
														            + "|ppt|pptx|doc|docx|xls|xlsx|ps" 
														            + "|rm|smil|wmv|swf|wma|zip|rar|gz))$",
														            Pattern.CASE_INSENSITIVE);
	
	public HttpContentHandler(String base){
		text = new StringBuilder();
		urlsFound = new ArrayList<>();
		baseUrl = base;
	}
	
	@Override
    public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
		if(localName.equalsIgnoreCase("a")){
			String anchor = attributes.getValue("href");
			if(anchor!=null){
				if(anchor.contains("#")){
					anchor = anchor.substring(0, anchor.indexOf("#"));
				}
				if(NONTEXT_EXTENSIONS.matcher(anchor).matches()){
					return;
				}
				anchor = makeAbsoluteUrl(anchor, baseUrl);
				urlsFound.add(anchor);
			}
		}
	}
	
	@Override
    public void characters(char ch[], int start, int length) throws SAXException {
		text.append(new String(ch, start, length));
	}
	
	public ArrayList<String> getAnchors(){
		return urlsFound;
	}
	
	public String getBodyText(){
		return text.toString();
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
}
