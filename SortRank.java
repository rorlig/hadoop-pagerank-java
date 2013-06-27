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
 *  sort the results of the PageRank algorithm from highest to lowest
 *
 *  input file format: (<title>, <rank>||<num-links>||<link1>||...||<linkN>)
 *  
 */

public class SortRank
{

    public static final int    SORT_MAX = 1000000000;
    public static final float SORT_MULT =     100000.0f;

    // map class will output (<SORT_MAX - Rank*SORT_MULT>, <Title>)
    // for each page
    public static class Map extends MapReduceBase
	implements Mapper<Text, Text, Text, Text>
    {
	private Text rankText = new Text();
	private Text title = new Text();

	public void map(Text key, Text value, 
			OutputCollector<Text, Text> output, 
			Reporter reporter)
	    throws IOException
	{
	    String val0 = key.toString();
	    String val1 = value.toString();

	    title.set(val0);

	    int split = val1.indexOf("||");
	    float r = new Float(val1.substring(0, split));
	    int rInt = SORT_MAX - (int)(SORT_MULT * r);
	    rankText.set(String.valueOf(rInt));
	    output.collect(rankText, title);
	}
    }


    public static class MapText extends MapReduceBase
	implements Mapper<LongWritable, Text, Text, Text>
    {
	private Text rankText = new Text();
	private Text title = new Text();

	public void map(LongWritable offset, Text value, 
			OutputCollector<Text, Text> output, 
			Reporter reporter)
	    throws IOException
	{
	    String[] val = value.toString().split("\t");

	    title.set(val[0]);

	    int split = val[1].indexOf("||");
	    float r = new Float(val[1].substring(0, split));
	    int rInt = SORT_MAX - (int)(SORT_MULT * r);
	    rankText.set(String.valueOf(rInt));
	    output.collect(rankText, title);
	}
    }


    // reducer converts rank back to normal value and formats it
    public static class Reduce extends MapReduceBase
	implements Reducer<Text, Text, Text, Text>
    {
	private Text valText = new Text();

	public void reduce(Text key, Iterator<Text> values,
			   OutputCollector<Text, Text> output,
			   Reporter report)
	    throws IOException
	{
	    while(values.hasNext()){
		float val = (float)(SORT_MAX - new Integer(key.toString()))
		    /SORT_MULT;
		//String valString = String.valueOf(val);
		String valString = String.format("%12.4f", val);
		valText.set(valString);
		output.collect(valText, values.next());
	    }
	}
    }
}
