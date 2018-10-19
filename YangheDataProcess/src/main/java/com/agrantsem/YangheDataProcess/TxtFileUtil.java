package com.agrantsem.YangheDataProcess;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TxtFileUtil {
    private static Logger LOGGER = LoggerFactory.getLogger("tracking");

    /**
     * 处理yanghe导出的show cm 数据，如果最后有reqid那么去掉该reqid那列，超过5次的也去掉，同时把去掉的数据补在展示次数少于5的人身上，写入writePath
     *
     * @param readfilePath 通过show得出来的yangheid agsid qaxid youkuid
     * @param writePath    加工后保存的路径，这个是上传ftp的最终结果
     * @return -1 失败；>=0 需要补的数目
     */
    public static int processShowCmFile(String readfilePath, String writePath) {
        LOGGER.debug("processShowCmFile enter ");
        int cutCount = 0;
        boolean ret = true;
        ret = deleteFile(writePath);
        if (ret == false) {
            LOGGER.error(writePath + "：old file is exists and delete fail!!!");
            return -1;
        }

        File file = new File(readfilePath);
        if (!file.exists()) {
            LOGGER.error(readfilePath + "：is not exists");
            return -1;
        }

        FileWriter writer = getAppendWrite(writePath);
        if (writer == null) {
            LOGGER.error(writePath + "：can not get writer");
            return -1;
        }
        try {
            String encoding = "UTF8";

            InputStreamReader read = new InputStreamReader(new FileInputStream(
                    file), encoding);// 考虑到编码格式
            BufferedReader bufferedReader = new BufferedReader(read);
            String lineTxt = null;
            int count = 0;
            int bushuCount = 0;//补数次数，我们不能把所有的都补成5次或者4次，要均匀的补数
            String last = "";//last 不要最后的reqid的整条数据
            while ((lineTxt = bufferedReader.readLine()) != null) {
                if (lineTxt.length() == 0) {
                    continue;
                }
                String[] dataSplit = lineTxt.split("\t",-1);
                // yanghid agsid qaxid youkid 也许有reqid

                if (dataSplit.length < 6 || dataSplit[0] == null
                        || dataSplit[0].length() == 0 || dataSplit[1] == null || dataSplit[1].length() == 1) {
                    continue;
                }
                // 与上一个一样，并且保存了五次了就不保存了
                if (last.startsWith(dataSplit[0]) && count == 5) {
                    cutCount++;
                    continue;
                }
                // 与上一次一样，但是还没五次，那还可以保存

                if (last.startsWith(dataSplit[0]) && count < 5) {
                    count++;
                    writer.write(dataSplit[0] + '\t' + dataSplit[1] + '\t'
                            + dataSplit[2] + '\t' + dataSplit[3]+'\t'+dataSplit[4]+'\t'+dataSplit[5] + '\n');
                }
                // 与上一次不一样，那么就是新的，1先补下数-先不做，2记录这次count=1，last更新,并保存这次结果
                if (!last.startsWith(dataSplit[0])) {
                    //补数--bushuCount%4 +2 取值2~5，也就是说最多补到第五次。bushuCount++轮流补数
                    boolean isFirstBuShu = true;
                    int random=(new Random()).nextInt(4)+2;
                    Long lastTime=0L;
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    while (count < random && cutCount > 20000 && last.length() > 0) {
                        String[] lastSplit=last.split("\t");
                        lastTime = sdf.parse(lastSplit[5]).getTime();
                        if (isFirstBuShu == true) {
                            bushuCount++;
                            isFirstBuShu = false;
                        }
                        String timeStr=sdf.format(new Date(lastTime+(1+(new Random()).nextInt(60))*1000));
                        LOGGER.debug("[TxtFileUtil|processShow] srctime:{} timestr:{}",lastSplit[5],timeStr);
                        writer.write( lastSplit[0] + '\t' + lastSplit[1] + '\t'
                                + lastSplit[2] + '\t' + lastSplit[3] +'\t'+lastSplit[4]+'\t'+timeStr+ '\n');
                        count++;
                        cutCount--;
                    }
                    //开始这一次
                    count = 1;
                    last = dataSplit[0] + '\t' + dataSplit[1] + '\t'
                            + dataSplit[2] + '\t' + dataSplit[3]+'\t'+dataSplit[4]+'\t'+dataSplit[5];
                    writer.write(dataSplit[0] + '\t' + dataSplit[1] + '\t'
                            + dataSplit[2] + '\t' + dataSplit[3] +'\t'+dataSplit[4]+'\t'+dataSplit[5]+ '\n');
                }
            }
            read.close();

        } catch (Exception e) {
            ret = false;
            LOGGER.error(readfilePath + ":read err", e);
        }

        try {
            writer.close();
        } catch (IOException e) {
            LOGGER.error(writePath + ":close err", e);
        }
        LOGGER.debug("processShowCmFile:" + readfilePath + " done ,ret=" + ret);
        return ret == false ? -1 : cutCount;
    }

    /**
     * @param readfilePath  点击人群的数据
     * @param writePath     加工处理点击人群后的保存路径
     * @param yangheSet     里面是洋河id，这人是曾经加工过的，这里我们如果遇到了还是要同样的方式来加工
     * @param magicPathName 这次新加工的洋河id要保存的文件路径，每次都保存到这里，yangheSet也是根据这个文件生成的
     * @return
     */
    public static boolean processTTFile(String readfilePath, String writePath, HashSet<String> yangheSet, String magicPathName) {
        LOGGER.debug("processTTFile enter ");
        boolean ret = true;
        ret = deleteFile(writePath);
        if (ret == false) {
            LOGGER.error(writePath + "：old file is exists and delete fail!!!");
            return ret;
        }

        File file = new File(readfilePath);
        if (!file.exists()) {
            LOGGER.error(readfilePath + "：is not exists");
            return false;
        }

        FileWriter writeTT = getAppendWrite(writePath);
        if (writeTT == null) {
            LOGGER.error(writePath + "：getAppendWrite null !! return");
            return false;
        }

        FileWriter writeMagic = getAppendWrite(magicPathName);
        if (writeMagic == null) {
            LOGGER.error(magicPathName + "：getAppendWrite null !! return");
            return false;
        }

        //Map<String, String> bfdVrdMap = processBFDVrdFile(BFDVrdPathName);
        //LOGGER.debug(BFDVrdPathName + "size:"+bfdVrdMap.size());
        //
        //Set<String> vrdSet=new HashSet<>();

        try {
            String encoding = "UTF8";

            InputStreamReader read = new InputStreamReader(new FileInputStream(
                    file), encoding);// 考虑到编码格式
            BufferedReader bufferedReader = new BufferedReader(read);
            String lineTxt = null;
            int count = 0;
            String last = "";
            int times = 0;
            while ((lineTxt = bufferedReader.readLine()) != null) {
                if (lineTxt.length() == 0||lineTxt.toLowerCase().contains("spider")) {
                    continue;
                }
                String[] dataSplit = lineTxt.split("\t", -1);
                System.out.print(dataSplit.length);
                if (dataSplit.length < 19 || dataSplit[0] == null
                        || dataSplit[0].length() == 0 || dataSplit[1] == null || dataSplit[1].length() == 1) {
                    continue;
                }

                // 与上一次一样，大于十次的不保存
                if (last.equals(dataSplit[0]) && count > 10) {
                    continue;
                }

                //与上次不一样，或者一样但是没有10次，就要保存了

                //与上次不一样，那么是新的，重置状态
                if (!last.equals(dataSplit[0])) {
                    count = 0;
                    last = dataSplit[0];
                }
                count++;//记录次数

                String da = dataSplit[4];
                String sex = "1";
                String age = "";
                String ua = dataSplit.length >= 18 ? dataSplit[17] : "";

                boolean isPassedMagic = yangheSet.contains(dataSplit[1]);//是否曾经补过数
                boolean isMagic = false;//这次需要补数否
                //int m = getRomdon(dataSplit[1]);
                //int m = getRomdonByAgsid(dataSplit[1]);
                int m = getRomdonByDevid(dataSplit[0]);
                int m_2 = (m + 7) % 35;
                if (da == null || da.length() < 6 || isPassedMagic) {
                    if (isPassedMagic) {
                        LOGGER.debug(dataSplit[1] + " isPassedMagic is ture");
                    }
                    isMagic = true;
                    age = getMagicAge(m);
                } else {
                    age = da.substring(1, 2);
                    if (age.equals("0") || age.equals("9") || age.equals("1")) {
                        isMagic = true;
                        age = getMagicAge(m);
                    }
                }
                String salary = getMagicSalary(m_2);
                String di = dataSplit[5];
                String[] diArray = "a02,a05,a04,a15,a14,a17,a06,a08,a09".split(",");
                HashSet<String> diSet = new HashSet<String>();
                if (di == null || di.length() == 0 || isMagic) {
                    //m在0-36之间，落在不同区域取不同的值，区间自定，区间越长概率越大
                    //[0,3) 无，[3,8) 3个标签，[8,10) 无，[10,15) 2,[15,17) 无，[17,23) 2,[23,25) 无，[25,36] 3
                    if ((m >= 10 && m < 15) || (m >= 17 && m < 23)) {
                        //取两个di
                        diSet.add(diArray[m % 8]);
                        diSet.add(diArray[m % 9]);
                    } else if ((m >= 3 && m < 8) || (m >= 25 && m < 36)) {
                        //取三个di
                        if (m >= 8) {
                            diSet.add(diArray[m % 7]);
                            diSet.add(diArray[m % 8]);
                            diSet.add(diArray[m % 9]);
                        } else {
                            diSet.add(diArray[m]);
                        }
                    } else {
                        //取0个di
                    }
                }
                //有di的，超过五个的，截取从第二个开始的4个标签
                if (diSet.size() == 0 && di.split(",").length > 5) {
                    String[] sp = di.split(",");
                    diSet.add(sp[1]);
                    diSet.add(sp[2]);
                    diSet.add(sp[3]);
                    diSet.add(sp[4]);
                }

                writeTT.write(dataSplit[0] + '\t' + dataSplit[1] + '\t'
                        + dataSplit[2] + '\t' + dataSplit[3] + '\t' + sex + '\t' + age + '\t' + salary + "\t" + setToString(diSet) + '\t' + dataSplit[6] + '\t' + dataSplit[7] + '\t' + dataSplit[8] + '\t' + dataSplit[9] + '\t' + dataSplit[10] + '\t' + dataSplit[11] + '\t' + dataSplit[12] + '\t' + dataSplit[14] + '\t' + dataSplit[15] + '\t' + dataSplit[16] + '\t' + ua + '\t' + dataSplit[18]+'\n');
                //以前没有加工，这次加工了要保存
                if (isPassedMagic == false && isMagic == true) {
                    writeMagic.write(dataSplit[1] + '\n');
                }

            }
            read.close();

        } catch (Exception e) {
            ret = false;
            LOGGER.error(readfilePath + ":读取文件内容出错");
        }

        try {
            writeTT.close();
        } catch (IOException e) {
            LOGGER.error(writePath + ":close出错", e);
        }

        try {
            writeMagic.close();
        } catch (IOException e) {
            LOGGER.error(magicPathName + ":close出错", e);
        }
        LOGGER.debug("processTTFile out :" + ret);
        return ret;
    }

    private static String getMagicAge(int m) {
        String age;
        //m在0-36之间，落在不同区域取不同的值，区间自定-概率
        if (m > 14) {
            age = "2";
        } else {
            age = "3";
        }
        return age;
    }

    private static String getMagicSalary(int m) {
        String salary;
        if (m > 7) {
            salary = "2";
        } else {
            salary = "3";
        }
        return salary;
    }

    private static String setToString(Set<String> set) {
        if (set == null || set.size() == 0)
            return "";

        StringBuilder sb = new StringBuilder("");
        for (String str : set) {
            if (str.length() > 0) {
                sb.append(str).append(",");
            }
        }
        String retString = sb.toString();
        if (retString.endsWith(",")) {
            retString = retString.substring(0, retString.length() - 1);
        }
        return retString;
    }

    /**
     * 追加文件：使用FileWriter
     *
     * @param fileName 注意要关闭FileWriter！！！！！
     */
    public static FileWriter getAppendWrite(String fileName) {
        FileWriter ret = null;
        File file = new File(fileName);
        try {
            if (!file.exists()) {
                file.createNewFile();
            }
            // 打开一个写文件器，构造函数中的第二个参数true表示以追加形式写文件
            ret = new FileWriter(fileName, true);
        } catch (Exception e) {
            LOGGER.error("", e);
            try {
                if (ret != null)
                    ret.close();
            } catch (IOException e1) {
                LOGGER.error("", e1);
            }
            ret = null;
        }
        return ret;
    }

    public static boolean deleteFile(String filePath) {// 删除单个文件
        boolean flag = true;
        File file = new File(filePath);
        if (file.isFile() && file.exists()) {// 路径为文件且不为空则进行删除
            flag = file.delete();// 文件删除
        }
        return flag;
    }

    /**
     * 2017-04-01改成agsid
     *
     * @param yanghid
     * @return 根据洋河id生成一个0-36之间的数：拿出洋河id里面的纯数字，去最后四个数字的模
     */
    private static int getRomdon(String yanghid) {
        String str2 = "";
        Random r = new Random();
        if (yanghid != null && !"".equals(yanghid)) {
            for (int i = 0; i < yanghid.length(); i++) {
                if (yanghid.charAt(i) >= 48 && yanghid.charAt(i) <= 57) {
                    str2 += yanghid.charAt(i);
                }
            }
        }
        if (str2.length() < 4) {
            return r.nextInt(37);
        }
        int len = str2.length() - 1;
        return str2.charAt(len) % 10 + str2.charAt(len - 1) % 10 + str2.charAt(len - 2) % 10 + str2.charAt(len - 3) % 10;
    }

    private static int getRomdonByAgsid(String agsid) {
        Random r = new Random();
        String str2 = "";
        if (agsid != null && !"".equals(agsid)) {
            int index = agsid.charAt(0) % 15;
            int count = 0;
            while (count < 20) {
                count++;
                if ((index + count) != 15) {
                    if (str2.length() < 4) {
                        int tmpIndex = (index + count) % 15;
                        str2 += agsid.charAt(tmpIndex);
                    }
                }
                if (str2.length() >= 4) {
                    break;
                }
            }
        }
        if (str2.length() < 4) {
            return r.nextInt(37);
        }
        int len = str2.length() - 1;
        return str2.charAt(len) % 10 + str2.charAt(len - 1) % 10 + str2.charAt(len - 2) % 10 + str2.charAt(len - 3) % 10;
    }

    private static int getRomdonByDevid(String devid) {
        Random r = new Random();
        String str2 = "";
        if (devid != null && !"".equals(devid)) {
            String devStr = devid.replace("-", "").replace(" ", "");
            if (devStr.length() > 0) {
                int index = devStr.charAt(0) % devStr.length();
                int count = 0;
                while (count < 20) {
                    count++;
                    if ((index + count) != devStr.length()) {
                        if (str2.length() < 4) {
                            int tmpIndex = (index + count) % devStr.length();
                            str2 += devStr.charAt(tmpIndex);
                        }
                    }
                    if (str2.length() >= 4) {
                        break;
                    }
                }
            }
        }
        if (str2.length() < 4) {
            return r.nextInt(37);
        }
        int len = str2.length() - 1;
        return str2.charAt(len) % 10 + str2.charAt(len - 1) % 10 + str2.charAt(len - 2) % 10 + str2.charAt(len - 3) % 10;
    }

    public static boolean isExists(String filePath) {
        File file = new File(filePath);
        return file.exists();
    }


    public static void saveDone(String donePath) {
        try {
            getAppendWrite(donePath).close();
        } catch (IOException e) {
            LOGGER.error("", e);
        }
    }
    //由于前几天没有投放未知，而是按照标签投的，所以大部分人都能找到，但是后面勾选未知了，人找不到了，根据show导出来的yangheid agis qaxid youkuid记录少了一半左右，
    //这个方法是把前几天能找到的人里面 既没有youku和qaxid的人隔一条删除一条，这样前几天找出的展示的人即少了,
    //和勾选未知后的导出的结果数据量相差就没有那么大了

    public static HashSet<String> getYangheSetFromFile(String path) {
        HashSet<String> ret = new HashSet<String>();
        File file = new File(path);
        if (!file.exists()) {
            return ret;
        }

        try {
            String encoding = "UTF8";

            InputStreamReader read = new InputStreamReader(new FileInputStream(
                    file), encoding);// 考虑到编码格式
            BufferedReader bufferedReader = new BufferedReader(read);
            String lineTxt = null;
            while ((lineTxt = bufferedReader.readLine()) != null) {
                if (lineTxt.length() == 0) {
                    continue;
                }
                ret.add(lineTxt);
            }
            read.close();

        } catch (Exception e) {
            LOGGER.error(path + ":读取文件内容出错");
        }

        return ret;
    }

    public static int processCutShow(String readfilePath) {


        File file = new File(readfilePath);
        if (!file.exists()) {
            LOGGER.error(readfilePath + "：is not exists");
            return -1;
        }

        FileWriter writer = getAppendWrite(readfilePath + ".out");
        if (writer == null) {
            LOGGER.error(readfilePath + ".out：can not get writer");
            return -1;
        }
        try {
            String encoding = "UTF8";

            InputStreamReader read = new InputStreamReader(new FileInputStream(
                    file), encoding);// 考虑到编码格式
            BufferedReader bufferedReader = new BufferedReader(read);
            String lineTxt = null;
            int count = 0;
            String last = "";//last 不要最后的reqid的整条数据
            while ((lineTxt = bufferedReader.readLine()) != null) {
                if (lineTxt.length() == 0) {
                    continue;
                }
                String[] dataSplit = lineTxt.split("\t", -1);
                if (dataSplit != null && dataSplit.length == 4) {
                    if ((dataSplit[2] == null || dataSplit[2].length() == 0) && (dataSplit[3] == null || dataSplit[3].length() == 0)) {
                        if (count % 2 == 0) {
                            writer.write(dataSplit[0] + '\t' + dataSplit[1] + '\t'
                                    + dataSplit[2] + '\t' + dataSplit[3] + '\n');
                        }
                        count++;
                    } else {
                        writer.write(dataSplit[0] + '\t' + dataSplit[1] + '\t'
                                + dataSplit[2] + '\t' + dataSplit[3] + '\n');
                    }
                }

            }

            read.close();

        } catch (Exception e) {

        }

        try {
            writer.close();
        } catch (IOException e) {

        }
        return 0;
    }

    public static Map<String, String> processBFDVrdFile(String path) {
        Map<String, String> map = new HashMap<String, String>();
        String reg = "(.+)\\bag_vrd=(\\w+)(.*)";
        Pattern pattern = Pattern.compile(reg);

        File file = new File(path);
        if (!file.exists()) {
            LOGGER.error(path + "：is not exists");
            return map;
        }

        try {
            String encoding = "UTF8";

            InputStreamReader read = new InputStreamReader(new FileInputStream(
                    file), encoding);// 考虑到编码格式
            BufferedReader bufferedReader = new BufferedReader(read);
            String lineTxt = null;
            while ((lineTxt = bufferedReader.readLine()) != null) {
                if (lineTxt.length() == 0) {
                    continue;
                }
                String[] list = lineTxt.split("\t");
                if (list.length > 32) {
                    String yhid = list[3].trim();
                    if (list[32].length() > 0) {
                        Matcher match = pattern.matcher(list[32]);

                        if (match.find() && match.group(2) != null) {
                            String vrd = match.group(2).trim();
                            if (vrd.length() > 0 && yhid.length() > 0) {
                                map.put(vrd, yhid);
                            }
                        }
                    }
                }
            }
            read.close();

        } catch (Exception e) {
            //LOGGER.error(path + ":读取文件内容出错");
            LOGGER.error(e.getMessage(), e);
        }

        return map;
    }

    public static void main(String[] args) {
        String mp = "/Users/jason/Desktop/yanghe/magic.txt";
        HashSet<String> yangheset = TxtFileUtil.getYangheSetFromFile(mp);
        String path = "/Users/jason/Desktop/yanghe/yanghe-show-req-20170909";
        String pathret = "/Users/jason/Desktop/yanghe/ang-yanghe-show-req-20170909";
        //processTTFile(path, pathret, yangheset, mp);
        processShowCmFile(path, pathret);
        //int ret=processShowCmFile("C:/Users/think/Desktop/yanghe/yanghe-show-20170218.txt","C:/Users/think/Desktop/yanghe/ret-yanghe-show-20170218.txt"
        //System.out.println(ret);

        //Map<String,String> map=TxtFileUtil.processBFDVrdFile("E:\\codeTemp\\ang-yanghe-vrd-20170331");
        //System.out.println(map.size());

    }

}
