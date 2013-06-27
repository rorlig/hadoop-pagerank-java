/*
 *  cs292 homework6
 *
 *  Gaurav Gupta
 */

package cs292.hw6;

import cs292.hw6.WikiPage;

import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;


/*
 *  Build a link graph from a collection of wiki pages
 *
 *  Output will be of the form: (<Title>, <rank-and-links>)
 *  where <rank-and-links> = "<page-rank>||<num-links>||<Link1>||...||<LinkN>"
 *  Each page is initialized with page-rank = 1.0
 */

public class LinkGraph
{
    // mapper outputs (<Title>, <rank-and-links>) for each page
    public static class Map extends MapReduceBase
	implements Mapper<LongWritable, Text, Text, Text>
    {
	private Text title = new Text();
	private Text codedLinks = new Text();
	private WikiPage page = new WikiPage(null);
	
	public void map(LongWritable key, Text value, 
			OutputCollector<Text, Text> output, 
			Reporter reporter)
	    throws IOException
	{
	    page.setPage(value.toString());

	    // ignore empty titles and special pages with ':' in title
	    if(page.getTitle() == null) return;
	    if(page.getTitle().contains(":")) return;

	    title.set(page.getTitle());
	    codedLinks.set("1.0||" + page.getCodedLinks());
	    output.collect(title, codedLinks);
	}
    }

    
    // reducer does nothing
    public static class Reduce extends MapReduceBase
	implements Reducer<Text, Text, Text, Text>
    {
	public void reduce(Text key, Iterator<Text> links,
			   OutputCollector<Text, Text> output,
			   Reporter reporter)
	    throws IOException
	{
	    if(links.hasNext())
		output.collect(key, links.next());
	    else
		throw new IOException("No coded links for page "
				      + key.toString());
	}
    }
}
