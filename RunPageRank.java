/*
 *  cs292 homework6
 *
 *  Gaurav Gupta
 */

package cs292.hw6;

import cs292.hw6.LinkGraph;
import cs292.hw6.PageRank;
import cs292.hw6.SortRank;

import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;


/*
 * Run page rank algorithm
 */

public class RunPageRank
{
    public static float discount = 0.85f;
    public static boolean useTextFormat = false;
    // set number of reducers, -1 => let Hadoop chose
    public static int numLinkGraphReducers = -1;
    public static int numPageRankReducers = -1;

    public static void main(String[] args) throws Exception
    {
	if(args.length < 3){
	    System.out.println("Useage: LinkGraph <in-file> <out-file> " +
			       "<page-rank-iterations>");
	    System.exit(-1);
	}

	int rank_runs = new Integer(args[2]);

	JobConf conf = new JobConf(LinkGraph.class);
	conf.setJobName("link_graph");
	FileSystem fs = FileSystem.get(conf);

	// paths for  input, output, and intermediate files
	Path inPath = new Path(args[0]);
	Path outPath = new Path(args[1]);
	Path lgPath = new Path("hw6/linkgraph-out");
	Path prPath = new Path("hw6/pagerank-out");

	// delete output and intermediate files if they exist
	if(fs.exists(outPath))fs.delete(outPath, true);
	if(fs.exists(lgPath))fs.delete(lgPath, true);
	if(fs.exists(prPath))fs.delete(prPath, true);

	// Build the link graph
	System.out.println("\n---------------------------");
	System.out.println(  "      build link graph     ");
	System.out.println("input  = " + inPath.toString());
	System.out.println("output = " + lgPath.toString());
	System.out.println(  "---------------------------");

	conf.setOutputKeyClass(Text.class);
	conf.setOutputValueClass(Text.class);
	conf.setMapperClass(LinkGraph.Map.class);
	conf.setReducerClass(LinkGraph.Reduce.class);

	conf.setInputFormat(TextInputFormat.class);

	if(useTextFormat)
	    conf.setOutputFormat(TextOutputFormat.class);
	else
	    conf.setOutputFormat(SequenceFileOutputFormat.class);

	if(numLinkGraphReducers > 0)
	    conf.setNumReduceTasks(numLinkGraphReducers);

	FileInputFormat.setInputPaths(conf, inPath);
	FileOutputFormat.setOutputPath(conf, lgPath);

	JobClient.runJob(conf);

	System.out.println("\n---------------------------");
	System.out.println(  "       run page rank       ");
	System.out.println(  "    discount = " + 
			     String.format("%4.2f", discount) 
			     + "        ");
	System.out.println(  "  iterations =" + 
			     String.format("%3d", rank_runs) 
			     + "          ");
	System.out.println(  "---------------------------");
	Path prInputPath = null;
	Path prOutputPath = null;
	for(int i=1; i<=rank_runs; i++){

	    // delete previous input path
	    if(prInputPath != null) fs.delete(prInputPath, true);

	    // set up the paths
	    prInputPath = (i == 1) ? lgPath : prOutputPath;
	    prOutputPath = new Path(prPath, String.valueOf(i));

	    // run page rank 
	    System.out.println(  "---------------------------");
	    System.out.println(  "     iteration "+i);
	    System.out.println("input  = " + prInputPath.toString());
	    System.out.println("output = " + prOutputPath.toString());
	    System.out.println(  "---------------------------");
	    
 	    conf = new JobConf(PageRank.class);
	    conf.setJobName("page_rank");
	    fs = FileSystem.get(conf);

	    conf.setOutputKeyClass(Text.class);
	    conf.setOutputValueClass(Text.class);
	    //conf.setMapperClass(PageRank.Map.class);
	    conf.setReducerClass(PageRank.Reduce.class);

	    if(useTextFormat){
		conf.setMapperClass(PageRank.MapText.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
	    }else{
		conf.setMapperClass(PageRank.Map.class);
		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);
	    }
	    
	    if(numPageRankReducers > 0)
		conf.setNumReduceTasks(numPageRankReducers);

	    FileInputFormat.setInputPaths(conf, 
					  new Path(prInputPath, "part-*"));
	    FileOutputFormat.setOutputPath(conf, prOutputPath);
	    JobClient.runJob(conf);
	}

	// Sort the rankings
	System.out.println("\n---------------------------");
	System.out.println(  "       sort rankings       ");
	System.out.println(  "input  = " + prOutputPath.toString());
	System.out.println(  "output = " + outPath.toString());
	System.out.println(  "---------------------------");

	conf = new JobConf(SortRank.class);
	conf.setJobName("sort_rank");
	fs = FileSystem.get(conf);

	conf.setOutputKeyClass(Text.class);
	conf.setOutputValueClass(Text.class);
	//conf.setMapperClass(SortRank.Map.class);
	conf.setReducerClass(SortRank.Reduce.class);

	if(useTextFormat){
	    conf.setMapperClass(SortRank.MapText.class);
	    conf.setInputFormat(TextInputFormat.class);
	}else{
	    conf.setMapperClass(SortRank.Map.class);
	    conf.setInputFormat(SequenceFileInputFormat.class);
	}
	conf.setOutputFormat(TextOutputFormat.class);

 	conf.setNumReduceTasks(1);

	FileInputFormat.setInputPaths(conf, 
				      new Path(prOutputPath, "part-*"));
	FileOutputFormat.setOutputPath(conf, outPath);
	JobClient.runJob(conf);

	// clean-up intermediate results
	if(fs.exists(lgPath))fs.delete(lgPath, true);
	if(fs.exists(prPath))fs.delete(prPath, true);
    }
}
