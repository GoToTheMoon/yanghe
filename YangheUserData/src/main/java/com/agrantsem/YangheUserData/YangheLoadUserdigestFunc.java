package com.agrantsem.YangheUserData;

import com.agrantsem.tracking.tag.Tag;
import com.agrantsem.tracking.tag.TagUtils;
import com.agrantsem.tracking.tag.policy.impl.DefaultCompressPolicyImpl;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.pig.*;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;


public class YangheLoadUserdigestFunc extends LoadFunc implements LoadMetadata{


	protected RecordReader reader = null;
	
	@Override
	public InputFormat<LongWritable, Text> getInputFormat() throws IOException {
		return new TextInputFormat();
	}

	@Override
	public Tuple getNext() throws IOException {
		Text val = null;
		try {
			// Read the next key value pair from the record reader. If it's
			// finished, return null
			if (!reader.nextKeyValue())
				return null;

			// Get the current value. We don't use the key.
			val = (Text) reader.getCurrentValue();
		} catch (InterruptedException ie) {
			throw new IOException(ie);
		}

		// Create a parser specific for this input line. This may not be the
		// most efficient approach.

		//return buildTuple(val.toString());
		return buildDevidTuple(val.toString());
	}

	/**
	 * 前面三列(aguid,tag,agsid)
	 * 本方法1.有些agsid后面有分号，agsid提取 2.解压tag，保留da，di和tt标签
	 * @param line
	 * @return
	 * @throws ExecException
	 */
	private Tuple buildTuple(String line) throws ExecException {
		
		
		String[] parts = line.split("\\t",-1);
        List<Tuple> tuples = new ArrayList<Tuple>();
        DataBag bag = BagFactory.getInstance().newDefaultBag(tuples);
        Tuple result = TupleFactory.getInstance().newTuple(bag);
        
		if(parts==null || parts.length<3){
			return result;
		}
		if(parts[2]==null || parts[2].length()==0){
			return result;
		}
		String[] agsids=parts[2].split(";");
		if(agsids==null || agsids.length==0){
			return result;
		}
		
		List<Tag> tagsCompressed = TagUtils.deserialize2list(parts[1]);
        if(tagsCompressed ==null || tagsCompressed.isEmpty()){
        	return result;
        }
        Set<Tag> tagsDecompressed = new DefaultCompressPolicyImpl().decompress(tagsCompressed);
        if(tagsDecompressed ==null || tagsDecompressed.isEmpty()){
        	return result;
        }
        
        StringBuilder sb=new StringBuilder("");

        for(Tag tag : tagsDecompressed){
        	if(tag.getX().equals("DI")||tag.getX().equals("DA")){
        		sb.append(tag.getOriTagText())
        		.append("|~|");
        	}
        }
        if(sb.toString().length()==0){
        	return result;
        }

        // 因为getNext函数只能返回一个tuple，
        // 而我们希望每个agsid一个单独的tuple，
        // 所以我们将多个tuple放到一个bag里面，
        // 然后返回一个包含一个bag的tuple。
        for (String agsid : agsids) {
        	if(agsid!=null && agsid.length()>0){
        		Tuple tuple= TupleFactory.getInstance().newTuple(2);

                 tuple.set(0,agsid);
                 tuple.set(1, sb.toString());

                 tuples.add(tuple);
        	}
        }
    	return result;
	}

	private Tuple buildDevidTuple(String line) throws ExecException {
		String[] parts = line.split("\\t",-1);
		List<Tuple> tuples = new ArrayList<Tuple>();
		DataBag bag = BagFactory.getInstance().newDefaultBag(tuples);
		Tuple result = TupleFactory.getInstance().newTuple(bag);

		if(parts==null || parts.length<13){
			return result;
		}
		if(parts[12]==null || parts[12].length()==0){
			return result;
		}
		String[] devids=parts[12].split(";");
		if(devids==null || devids.length==0){
			return result;
		}

		List<Tag> tagsCompressed = TagUtils.deserialize2list(parts[1]);
		if(tagsCompressed ==null || tagsCompressed.isEmpty()){
			return result;
		}
		Set<Tag> tagsDecompressed = new DefaultCompressPolicyImpl().decompress(tagsCompressed);
		if(tagsDecompressed ==null || tagsDecompressed.isEmpty()){
			return result;
		}

		StringBuilder sb=new StringBuilder("");

		for(Tag tag : tagsDecompressed){
			if(tag.getX().equals("DI")||tag.getX().equals("DA")){
				sb.append(tag.getOriTagText())
						.append("|~|");
			}
		}
		if(sb.toString().length()==0){
			return result;
		}

		// 因为getNext函数只能返回一个tuple，
		// 而我们希望每个agsid一个单独的tuple，
		// 所以我们将多个tuple放到一个bag里面，
		// 然后返回一个包含一个bag的tuple。
		for (String devid : devids) {
			if(devid!=null && devid.length()>0){
				Tuple tuple= TupleFactory.getInstance().newTuple(2);

				tuple.set(0,devid);
				tuple.set(1, sb.toString());

				tuples.add(tuple);
			}
		}
		return result;
	}

	@Override
	public void prepareToRead(RecordReader read, PigSplit pigsplit)
			throws IOException {
		this.reader =  read;
	}

	@Override
	public void setLocation(String location, Job job) throws IOException {
		FileInputFormat.setInputPaths(job, location);
	}

	@Override
	public String[] getPartitionKeys(String arg0, Job arg1) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ResourceSchema getSchema(String arg0, Job arg1) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ResourceStatistics getStatistics(String arg0, Job arg1)
			throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setPartitionFilter(Expression arg0) throws IOException {
		// TODO Auto-generated method stub
		
	}
}
