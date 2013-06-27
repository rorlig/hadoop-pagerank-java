/*
 *  cs292 homework6
 *
 *  Gaurav Gupta
 */

package cs292.hw6;

import cs292.hw6.RunPageRank;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;


/*
 *  page rank algorithm
 *
 *  input file format: (<title>, <rank-and-links>)
 *  where <rank-and-links> = "<page-rank>||<num-links>||<Link1>||...||<LinkN>"
 *
 *  output has two kinds of records,
 *  one link record for each page:
 *     (<title>, <"##links##||<num-links>||<Link1>||...||<LinkN>">
 *  and a rank-contribution record for each link on the page:
 *  (<Link>, <rank-contribution>)
 */

public class PageRank
{
    private static final String linkIndicator = "##links##";

    // mapper will output two kinds of records,
    //    for each link, the rank contribution from page being processed:
    //            (<Link>, <rank-contrib>)
    //    for the page being processed, the coded links:
    //            (<Title>, "##links##||<coded-links>")
    public static class Map extends MapReduceBase
	implements Mapper<Text, Text, Text, Text>
    {
	private String code;
	private Text link = new Text();
	private Text rankContribText = new Text();
	private Text codeText = new Text();
	private float rank, count;

	public void map(Text key, Text value, 
			OutputCollector<Text, Text> output, 
			Reporter reporter)
	    throws IOException
	{
	    code = value.toString();

	    // code = <rank>||<num-links>||<link1>||...||<linkN>

	    int split = code.indexOf("||");
	    rank = new Float(code.substring(0, split));
	    code = code.substring(split+2);

	    // code now = <num-links>||<link1>||...||<linkN>

	    // output the ##links## record
	    codeText.set(linkIndicator + "||" + code);
	    output.collect(key, codeText);

	    split = code.indexOf("||");

	    // if split < 0 then there are no links
	    if(split >= 0){
		count = new Float(code.substring(0, split));
		code = code.substring(split+2);

		// code now = <link1>||...||<linkN>

		// output the rank-contribution for each link
		String[] links = code.split("\\|\\|");
		for(String l : links){
		    link.set(l);
		    rankContribText.set(String.valueOf(rank/count));
		    output.collect(link, rankContribText);
		}
	    }
	}
    }

    public static class MapText extends MapReduceBase
	implements Mapper<LongWritable, Text, Text, Text>
    {
	private String code;
	private Text link = new Text();
	private Text rankContribText = new Text();
	private Text codeText = new Text();
	private float rank, count;

	public void map(LongWritable offset, Text value, 
			OutputCollector<Text, Text> output, 
			Reporter reporter)
	    throws IOException
	{
	    Text key = new Text();
	    String[] val = value.toString().split("\t");
	    key.set(val[0]);
	    value.set(val[1]);

	    code = value.toString();

	    // code = <rank>||<num-links>||<link1>||...||<linkN>

	    int split = code.indexOf("||");
	    rank = new Float(code.substring(0, split));
	    code = code.substring(split+2);

	    // code now = <num-links>||<link1>||...||<linkN>

	    // output the ##links## record
	    codeText.set(linkIndicator + "||" + code);
	    output.collect(key, codeText);

	    split = code.indexOf("||");

	    // if split < 0 then there are no links
	    if(split >= 0){
		count = new Float(code.substring(0, split));
		code = code.substring(split+2);

		// code now = <link1>||...||<linkN>

		// output the rank-contribution for each link
		String[] links = code.split("\\|\\|");
		for(String l : links){
		    link.set(l);
		    rankContribText.set(String.valueOf(rank/count));
		    output.collect(link, rankContribText);
		}
	    }
	}
    }

    
    // reducer outputs (<title>, <rank-and-links>)

    public static class Reduce extends MapReduceBase
	implements Reducer<Text, Text, Text, Text>
    {
	private Text outCodeText = new Text();

	public void reduce(Text key, Iterator<Text> values,
			   OutputCollector<Text, Text> output,
			   Reporter reporter)
	    throws IOException
	{
	    String codedLinks = "0";
	    float rank = 1 - RunPageRank.discount;
	    while(values.hasNext()){
		String inCode = values.next().toString();
		int split = inCode.indexOf("||");
		//String part1 = (split >= 0) ? inCode.substring(0, split)
		//                          : inCode;
		if(split < 0){
		    // this record just has the rank contribution
		    rank += RunPageRank.discount * new Float(inCode);
		}else{
		    // this record has the coded links
		    codedLinks = inCode.substring(split + 2);
		}
	    }

	    // output the rank and coded links
	    outCodeText.set(String.valueOf(rank) + "||" + codedLinks);
	    output.collect(key, outCodeText);
	}
    }
}
