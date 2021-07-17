package test2;

import java.util.HashMap;
import java.util.Map;

public class MRDPUtils {
    /* In : <row id="6939296" PostTypeId="2" ParentId="6939137" CreationDate="2011-08-04T09:30:25.043" Score="4" ViewCount="" Body="&lt;p&gt;You should have imported Poll with &lt;code&gt;from polls.models import Poll&lt;/code&ge;&lt;/p&gt;&#xA;" OvnerUserId="634150" LastActivityDate="2011-08-04T09:50:25.043" CommentCount="1" />
     * Out: [{"id":"6939296"},{"PostTypeId","2"},...]
     */
    public static Map<String,String> transformXmlToMap(String xml){
        Map<String,String> map=new HashMap<String, String>();

        try{
            // 去掉头和尾，按照引号分割
            String[] tokens=xml.trim().substring(5,xml.trim().length()-3).split("\"");

            // 遍历数组，每2个组成一个键值对，添加到map中
            for(int i=0;i<tokens.length-1;i+=2){
                String key=tokens[i].trim();
                String val=tokens[i+1];
                map.put(key.substring(0,key.length()-1),val);
            }
        }catch (StringIndexOutOfBoundsException e){
            System.err.println(xml);
        }

        return map;
    }
}
