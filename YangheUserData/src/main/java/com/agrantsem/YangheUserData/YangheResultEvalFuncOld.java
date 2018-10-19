package com.agrantsem.YangheUserData;

import com.agrantsem.tracking.tag.Tag;
import com.agrantsem.tracking.tag.TagUtils;
import com.agrantsem.tracking.tag.policy.impl.DefaultCompressPolicyImpl;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class YangheResultEvalFuncOld extends  EvalFunc<DataBag>{

	@Override
	public DataBag exec(Tuple tuple) throws IOException {
		if (null == tuple || tuple.size() < 1) {
			return null;
		}
		DataBag userbag = (DataBag) tuple.get(0);
		Set<String> agsidSet=new HashSet<String>();
		//Set<String> DAtagSet=new HashSet<String>();
		Set<String> DItagSet=new HashSet<String>();
		Set<String> midSet=new HashSet<String>();
		Set<String> nameset=new HashSet<String>();
		Set<String> planIDSet=new HashSet<String>();
		Set<String> planNameSet=new HashSet<String>();
		Set<String> qaxSet =new  HashSet<String>();
		Set<String> youkuSet =new  HashSet<String>();
		Set<String> contctgrSet =new  HashSet<String>();
		Set<String> conttitleSet = new  HashSet<String>();
		//Set<Integer> ftSet =new  HashSet<Integer>();
		Set<String> vrdSet=new HashSet<String>();
		//Set<String> uaSet=new HashSet<String>();
		String ua="";
		String DAStr="";
		//------extData
		//剧目类型
		//String contctgr = "";
		//类型偏好，关键词
		//String conttitle = "m";
		String ft = "";
		String province ="";
		String city = "";
		String channel ="";
		String ip="";
		int count=0;
		for (Tuple tp : userbag) {
			count++;
			if(tp == null || tp.size()< 16){
				continue;
			}
			String agsid=(String) tp.get(1);
			String tag=(String) tp.get(2);//拆成两列da、di
			String mid=(String) tp.get(3);
			String name=(String) tp.get(4);
			String planid=(String) tp.get(5);
			String planName=(String) tp.get(6);
			String qaxid = (String) (tp.get(7)==null?"":tp.get(7));
			String youkuid= (String) (tp.get(8)==null?"":tp.get(8));

			//-----extData
			//剧目类型
			String contctgr = (String) tp.get(9);
			//类型偏好，关键词
			String conttitle = (String) tp.get(10);
			ft = (String) (tp.get(11)==null?"":tp.get(11));
			province = (String)tp.get(12);
			city = (String)tp.get(13);
			channel =(String)tp.get(14);
			ip=(String)tp.get(15);
			String vrd=(String) (tp.get(16)==null?"":tp.get(16));
			ua=(String) (tp.get(17)==null?"":tp.get(17));
			//标签处理
			List<Tag> tagsCompressed = TagUtils.deserialize2list(tag);
			if (tagsCompressed != null && !tagsCompressed.isEmpty()) {
				Set<Tag> tagsDecompressed = new DefaultCompressPolicyImpl().decompress(tagsCompressed);
				if (tagsDecompressed != null && !tagsDecompressed.isEmpty()) {
					for (Tag tagItem : tagsDecompressed) {
						if (tagItem.getX().equals("DI")) {
							int index = tagItem.getV().indexOf("_");
							String value = tagItem.getV();
							if (index > 0) {
								value = tagItem.getV().substring(0, index);
							}
							DItagSet.add(value);
						} else if (tagItem.getX().equals("DA")) {
							//DAtagSet.add(tagItem.getV());
							if(tagItem.getV()!=null && tagItem.getV().length()>0){
								DAStr=tagItem.getV().hashCode()>DAStr.hashCode()?tagItem.getV():DAStr;
							}
						}
					}
				}
			}

			if (conttitle != null && conttitle.length() > 0) {
				conttitle = conttitle.replaceAll(" ", ",");
				if (conttitle.length() > 10 && conttitle.indexOf(",", 10) > 0) {
					conttitle = conttitle.substring(0, conttitle.indexOf(",", 10));
				}
			}

			agsidSet.add(agsid);
			midSet.add(mid);
			nameset.add(name);
			planIDSet.add(planid);	
			planNameSet.add(planName);
			qaxSet.add(qaxid);
			youkuSet.add(youkuid);
			vrdSet.add(vrd);
			contctgrSet.add(contctgr);
			conttitleSet.add(conttitle);
			//uaSet.add(ua);
		}
		
		Tuple item = TupleFactory.getInstance().newTuple(17);
		item.set(0, setToString(agsidSet));
		item.set(1, setToString(qaxSet));
		item.set(2, setToString(youkuSet));
		item.set(3, DAStr);
		item.set(4, setToString(DItagSet));
		item.set(5, setToString(midSet));
		item.set(6, setToString(nameset));
		item.set(7, setToString(planIDSet));
		item.set(8, setToString(planNameSet));
		
		//extData
		item.set(9, setToString(contctgrSet));
		item.set(10, setToString(conttitleSet));
		item.set(11, ft);
		item.set(12, (city==null||city.length()==0||city.equals("未知"))?province:province+"."+city);
		item.set(13, ip);
		item.set(14, channel);
		item.set(15,setToString(vrdSet));
		item.set(16,ua);
		
		DataBag retBag =BagFactory.getInstance().newDefaultBag(); 
		for(int i=0;i<count;i++){
			retBag.add(item);
		}
	
		return retBag;
	}

	private String setToString(Set<String> set) {
		if(set==null || set.size()==0)
			return "";
		
		StringBuilder sb = new StringBuilder("");
		for(String str: set){
			if(str!=null&&str.length()>0){
				sb.append(str).append(",");
			}
		}
		String retString=sb.toString();
		if(retString.endsWith(",")){
			retString=retString.substring(0, retString.length()-1);
		}
		return retString;
	}


}
