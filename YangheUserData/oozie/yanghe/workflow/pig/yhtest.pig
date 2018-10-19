--pig -Dpig.additional.jars=lib/*.jar -param inputDirUserDigest=xxx -param inputDirCM=xxx -param channelName=xxx -param dspttReg=xxx -param inputDirDspTT=xxx -param selectCid=xxx -param inputDirAd3=xxx -param outputdir=xxx -f yanhe.pig
--pig -Dpig.additional.jars=lib/*.jar -param inputDirUserDigest=/user/le.xiang/yanghe/data/userdigest.txt -param inputDirCM=/user/le.xiang/yanghe/data/cm.txt -param channelName=QAX -param dspttReg=X:BE,C:18084,M:DSPTT,V:1,T:170112 -param inputDirDspTT=/user/le.xiang/yanghe/data/dsptt.txt -param selectCid=15151 -param inputDirAd3=/user/le.xiang/yanghe/data/ad3.txt -param outputdir=/user/le.xiang/yanghe/output -f testYangheData.pig

--%default inputDirUserDigestTmp '/tmp/mapred/userdigest/export/ucld/';
--%default inputDirDspShowCm 'hdfs://agrant/user/le.xiang/cm-youku-qax-yangheShow/dateline=20170222-8';
--%default inputDirDspTT 'hdfs://agrant/user/dsp/warehouse/logdata.db/dsptt/dateline=20170222/';
--%default inputDirAd3 'hdfs://agrant/user/dsp/warehouse/dsp_report.db/ad3/dateline=2017022223/';
--%default outputdir 'hdfs://agrant/user/le.xiang/tt-youku-qax/dateline=20170222';
--%default selectCid '18210';

--pig -Dpig.additional.jars=lib/*.jar
DEFINE YangheLoadUserdigestFunc com.agrantsem.YangheUserData.YangheLoadUserdigestFunc();--1.有些agsid后面有分号，agsid提取 2.解压tag，保留da，di和tt标签
DEFINE YangheResultEvalFunc com.agrantsem.YangheUserData.YangheResultEvalFunc();--合并 agsid  ，tag合并分离出da，di的值，mid，name，planid ，planname

----------------------------/user/tracking/userdigest/export/userdigest_export_2017-01-13_ucld_inc.txt.gz，标签数据---------
--/tmp/mapred/userdigest/export/ucld
userAllBag = load '$inputDirUserDigestTmp' using YangheLoadUserdigestFunc() as (users:bag{user:(agsid: chararray ,tag: chararray)});--日期是当天执行的日期，数据其实是昨天
userALL = foreach userAllBag generate flatten($0) as (agsid: chararray ,tag: chararray);
user = filter userALL by tag is not null and tag !='';


------------------------------showd的cm结果（洋河id，agsid，qaxid，优酷id,reqid）---------
cmDup = load '$inputDirDspShowCm' USING PigStorage()  as (yangheid: chararray,agsid: chararray,qaxid: chararray,youkuid: chararray,reqid: chararray);
--cmFilter = filter cmDup yangheid is not null and yangheid!='';--20170224
cm= DISTINCT cmDup;--20170224改成cmFilter
userCmDuplicateAgsid = join cm by agsid LEFT OUTER,user by agsid ;--拿agsid去找标签
userCm = foreach userCmDuplicateAgsid GENERATE cm::reqid as reqid, cm::yangheid as yangheid,cm::agsid as agsid,cm::qaxid as qaxid,cm::youkuid as youkuid,user::tag as tag;--一个channelid 可能对应多个agsid，tag

 ----------------------------dsptt日志--------------
dspttAll = load '$inputDirDspTT' USING  parquet.pig.ParquetLoader();

dspttYHAll = filter dspttAll  by cid == $selectCid and  channel in ('YOUKU','QAX','BAIDUAPP','BAIDUFEED','BAIDUBN'); --yangheCid ;--只要洋河的点击数据
dspttYH = foreach dspttYHAll  generate reqid,agsid,mid,contctgr as channelContctgr,conttitle,ft,province,city,channel,ip;

-------------------------userCm与dsptt
userCmTTDuplicateAgsid = join dspttYH by reqid ,userCm by reqid ;--主要是通过reqid拿到agsid
userCmTT = foreach userCmTTDuplicateAgsid GENERATE userCm::yangheid as yangheid,userCm::agsid as agsid,userCm::qaxid as qaxid,userCm::youkuid as youkuid,userCm::tag as tag ,dspttYH::mid as mid,dspttYH::channelContctgr as channelContctgr,dspttYH::conttitle as conttitle,dspttYH::ft as ft,dspttYH::province as province,dspttYH::city as city,dspttYH::channel as channel,dspttYH::ip as ip;

---------------------------ad3-----------
ad3All =  load '$inputDirAd3' USING parquet.pig.ParquetLoader();
ad3 = foreach ad3All GENERATE mid,name,planid,planname;

userCmTTAd3 = join userCmTT by mid LEFT OUTER,ad3 by mid;--链接ad3

simpUserCmTTAd3 = foreach userCmTTAd3 GENERATE userCmTT::yangheid as yangheid,userCmTT::agsid as agsid,userCmTT::qaxid as qaxid,userCmTT::youkuid as youkuid,userCmTT::tag as tag,userCmTT::mid as mid,ad3::name as name,ad3::planid as planid,ad3::planname as planname,userCmTT::channelContctgr as channelContctgr,userCmTT::conttitle as conttitle,userCmTT::ft as ft,userCmTT::province as province,userCmTT::city as city,userCmTT::channel as channel,userCmTT::ip as ip;

-----------------ext--agsid,agContctgr,conttitle,ft,province,city,channel;
contentCategory = load '/user/yukun.huang/contentCategory.txt' USING PigStorage()  as (agContctgr: chararray,channel: chararray,channelContctgr: chararray);-- using parquet.pig.ParquetLoader();

dspttYHExtDataAgCCcode = join simpUserCmTTAd3 by (channel,channelContctgr) LEFT OUTER,contentCategory by (channel,channelContctgr);

extData = foreach dspttYHExtDataAgCCcode generate simpUserCmTTAd3::yangheid as yangheid,simpUserCmTTAd3::agsid as agsid,simpUserCmTTAd3::tag as tag,simpUserCmTTAd3::mid as mid,simpUserCmTTAd3::name as name,simpUserCmTTAd3::planid as planid,simpUserCmTTAd3::planname as planname,simpUserCmTTAd3::qaxid as qaxid,simpUserCmTTAd3::youkuid as youkuid,contentCategory::agContctgr as agContctgr,simpUserCmTTAd3::conttitle as conttitle,simpUserCmTTAd3::ft as ft,simpUserCmTTAd3::province as province,simpUserCmTTAd3::city as city,simpUserCmTTAd3::channel as channel,simpUserCmTTAd3::ip as ip;

----------------------------------------------最后根据洋河id合并------------------
extDataGroup =  Group extData by yangheid;--按照洋河id分组 agsid mid planid 合并了
--格式化agsid da di mid name planid planname
yanghedata = foreach extDataGroup generate group as yangheid,FLATTEN(YangheResultEvalFunc(extData)) as (agsid: chararray,qaxid: chararray,youkuid: chararray,sex: chararray,age: chararray,di: chararray,mid: chararray,name: chararray,planid: chararray,planname: chararray,agContctgr: chararray,conttitle: chararray,ft: chararray,location: chararray, ip: chararray,channel: chararray);

orderyanghedata = order yanghedata by yangheid;
STORE orderyanghedata INTO '$outputdir' USING PigStorage('\t');

--0 16 * * * hadoop fs -get /user/le.xiang/adview/ttimpid-`date  +"%Y%m%d" -d  "-1 days"`/part-00000 ttimpid-`date  +"%Y%m%d" -d  "-1 days"`.txt