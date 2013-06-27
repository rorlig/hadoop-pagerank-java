/*
 *  cs292 homework6
 *
 *  Gaurav Gupta
 */

package cs292.hw6;

import java.util.*;
import java.util.regex.*;

/*
 *  WikiPage class represents one page
 *
 *  Set the page in the constructor or with setPage() method
 *  getTitle() returns the page's title
 *  getLinks() returns an HashSet with outgoing links
 *  getCodedLinks() returns a String with "<num-links>||<Link1>||...||<LinkN>"
 *      Links only include the linked page's title.  Sections and
 *      display names are removed.
 *      External links are excluded.
 *      Links within a page are exluded.
 */

public class WikiPage
{
    //-----------------------------------------
    //  static constants
    //
    private static final String TITLE_REGEX = "<title>(.+?)</title>";
    private static final String LINK_REGEX = "\\[\\[(.+?)\\]\\]";


    //------------------------------------------
    //  class helper methods
    //
    private static String normalizeTitle(String title)
    {
	if(title.length()>0){
	    // first character is uppercase (2nd and later are case-sensitive)
	    title = title.substring(0, 1).toUpperCase()
	        + title.substring(1);
	    // convert space to underscore
	    title.replace(" ","_");
	    // remove tabs
	    title.replace("\t", "");
	}
	return title;
    }

    
    //------------------------------------------
    //  private attributes
    //
    private String title = null;
    private HashSet<String> links = null;
    private String codedLinks = null;
    private String page = null;


    //------------------------------------------
    //  constructor
    //
    public WikiPage(String page){
	this.page = page;
    }


    //------------------------------------------
    //  public accessors
    //
    public String getTitle()
    {
	if(title == null){
	    // parse title from page
	    Pattern p = Pattern.compile(TITLE_REGEX);
	    Matcher m = p.matcher(page);
	    if(m.find()) title = m.group(1);
	}
	return title;
    }

    public HashSet<String> getLinks()
    {
	if(links == null){
	    links = new HashSet<String>();
	    // parse the links from page
	    Pattern p = Pattern.compile(LINK_REGEX);
	    Matcher m = p.matcher(page);
	    while(m.find()){
		String rawLink = m.group(1);

		// drop if external link [[ xx:xx ]]
		//if(rawLink.matches(".*:.*")) continue;
		if(rawLink.contains(":")) continue;

		// drop the section part [[ page#secion ]] or [[ #section ]]
		int section = rawLink.indexOf("#");
		if(section == 0) continue;
		if(section > 0) rawLink = rawLink.substring(0, section);

		// drop the display name and pipe character
		int pipe = rawLink.indexOf("|");
		if(pipe >= 0) rawLink = rawLink.substring(0, pipe);

		rawLink = normalizeTitle(rawLink).trim();
		if(rawLink.length()>0) links.add(rawLink);
	    }
	}
	return links;
    }
    
    public String getCodedLinks()
    {
	HashSet<String> links = getLinks();
	String code = String.valueOf(links.size());
	for(String l : links){
	    code += "||" + l;
	}
	return code;
    }

    public boolean hasLinks()
    {
	return (getLinks().size() > 0);
    }

    public void setPage(String page)
    {
	this.page = page;
	title = null;
	links = null;
    }
}
