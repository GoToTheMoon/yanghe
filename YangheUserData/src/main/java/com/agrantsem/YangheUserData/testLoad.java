package com.agrantsem.YangheUserData;

import java.util.List;
import java.util.Set;

import org.apache.pig.backend.executionengine.ExecException;

import com.agrantsem.tracking.tag.Tag;
import com.agrantsem.tracking.tag.TagUtils;
import com.agrantsem.tracking.tag.policy.impl.DefaultCompressPolicyImpl;

public class testLoad {

	public static String[] buildTuple(String line) throws ExecException {

		String[] parts = line.split("\\t", -1);
		String[] t = new String[2];
		if (parts == null || parts.length < 3) {
			return t;
		}
		if (parts[2] == null || parts[2].length() == 0) {
			return t;
		}
		String agsid = parts[2];
		if (agsid.contains(";")) {
			agsid = agsid.substring(0, agsid.indexOf(";"));
		}
		t[0]=agsid;

		List<Tag> tagsCompressed = TagUtils.deserialize2list(parts[1]);
		if (tagsCompressed == null || tagsCompressed.isEmpty()) {
			t[1]= "";
			return t;
		}
		Set<Tag> tagsDecompressed = new DefaultCompressPolicyImpl()
				.decompress(tagsCompressed);
		if (tagsDecompressed == null || tagsDecompressed.isEmpty()) {
			t[1]= "";
			return t;
		}

		StringBuilder sb = new StringBuilder("");
		boolean hasDsptt = false;
		for (Tag tag : tagsDecompressed) {
			if (tag.getX().equals("DI") || tag.getX().equals("DA")) {
				sb.append(tag.getOriTagText()).append("|~|");
			} else if (tag.getX().equals("BE") && tag.getM() != null
					&& tag.getM().equals("DSPTT")) {
				sb.append(tag.getOriTagText()).append("|~|");
				hasDsptt = true;
			}
		}
		// 过滤非点击
		if (!hasDsptt) {
			t[1]= "";
		} else {
			t[1]=sb.toString();
		}
		return t;
	}

	public static void main(String[] args) {

		String[] t = null;
		try {
			t = testLoad
					.buildTuple("89V6Y1TEX5E13u  {X:BE,C:18084,M:DSPTT,V:1,T:1701202157}|~|{X:FI,Y:CL,C:18084,V:3,T:1701122158}|~|{X:CM,H:3,V:1612262022,T:1701122230}|~|{X:CM,H:2,V:1612262029,T:1701122120}|~|{X:FI,Y:CL,C:17522,V:3,T:1701051818}|~|{X:CM,H:9,V:1612262022,T:1701122120}|~|{X:BR,C:15151,O:1,T:1701051818}|~|{X:CM,H:4,V:1612262029,T:1701122120}|~|{X:BE,C:17522,M:TT,V:1,T:1701051818}|~|{X:BR,C:17522,O:1,T:1701051818}|~|{X:SH,V:5.375,N:17,T:1701120315}|~|{X:DI,Y:IN,V:[a3333_2_170113,a10_2_170113,a18_3_170113,a19_3_170113,a14_3_170113,a15_3_170113,a17_3_170113,a02_3_170113],W:3,T:170113}|~|{X:DA,H:3,V:121010,W:2,T:170113}|~||~|{X:DA,H:3,V:122010,W:2,T:170113}       BCJOm2WSeYuFRfnC        WWjW91RDJqZ7gL5C        CAESEDLrcoYUYAJQXb6bMzHRx1U");
		} catch (ExecException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println(t.toString());
	}
}
