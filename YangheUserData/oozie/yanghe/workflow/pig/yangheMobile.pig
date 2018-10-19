--create by jiaxin.zhu
--use vrd link tt->cm
--use reqid link tt->cm
--one script solve all problems

DEFINE YangheLoadUserdigestFunc com.agrantsem.YangheUserData.YangheLoadUserdigestFunc();--1.有些agsid后面有分号，agsid提取 2.解压tag，保留da，di和tt标签
DEFINE YangheResultEvalFunc com.agrantsem.YangheUserData.YangheResultEvalFunc();--合并 agsid  ，tag合并分离出da，di的值，mid，name，planid ，planname

----------------------------/user/tracking/userdigest/export/userdigest_export_2017-01-13_ucld_inc.txt.gz，标签数据---------
--/tmp/mapred/userdigest/export/ucld
userAllBag = load '$inputDirUserDigestTmp' using YangheLoadUserdigestFunc() as (users:bag{user:(agsid: chararray ,tag: chararray)});--日期是当天执行的日期，数据其实是昨天
userALL = foreach userAllBag generate flatten($0) as (agsid: chararray ,tag: chararray);
user = filter userALL by tag is not null and tag !='';
 ----------------------------dsptt日志--------------
dspttAll = load '$inputDirDspTT' USING  parquet.pig.ParquetLoader();
dspttYHAll = filter dspttAll  by cid == $selectCid and  channel in ('ADVIEW','QAX','TANXAPP','XTX','BAIDUAPP') and reqid is not null and reqid !='' and reqid !='{requestid}' and  not ip matches '^(101\\.226|112\\.64|223\\.73|112\\.65|180\\.153|101\\.226).+' and ip != '118.244.154.107' and ip != '125.39.17.84' and refurl is not null and mid is not null and site is not null and refurl !=''; --yangheCid ;--只要洋河的点击数据
dspttYH = foreach dspttYHAll  generate reqid,devid as agsid,mid,contctgr as channelContctgr,conttitle,ft,province,city,channel,ip,vrd,ua,logtime;
---------------------------ad3-----------
ad3All =  load '$inputDirAd3' using parquet.pig.ParquetLoader();
ad3 = foreach ad3All GENERATE mid,name,planid,planname;

---------------------------bidder baiduapp-----------------------
bidderBaiduAppAll = load '$baiduBidder' USING parquet.pig.ParquetLoader();
bidderBaiduApp = FILTER bidderBaiduAppAll BY deviceid is not null and deviceid != '' and deviceid != '0';
bidderBaidu = FOREACH bidderBaiduApp GENERATE req,deviceid;

---------------------------show ----------------------------------------
showAll = load '$inputDirDspShow' using parquet.pig.ParquetLoader();
show = filter showAll by cid == $selectCid;
Showagsid =  foreach show GENERATE channelid,reqid,channel,devid,logtime;

---------取Qax的show--------
qaxShowAll = FILTER Showagsid BY channel=='QAX';
qaxShow = FOREACH qaxShowAll GENERATE logtime,reqid,channelid as qaxid,'' as adviewid,devid,channel;
---------取Adview的show--------
--AdviewShowAll = FILTER Showagsid BY channel=='ADVIEW';
adviewShowAll = FILTER Showagsid BY channel=='ADVIEW';
AdviewShow = FOREACH adviewShowAll GENERATE logtime,reqid,'' as qaxid,channelid as adviewid, devid,channel;

baiduShowAll = FILTER Showagsid BY channel=='BAIDUAPP';
baiduShow = FOREACH baiduShowAll GENERATE logtime,reqid,'' as qaxid,channelid as adviewid, devid,channel;
tanxShowAll =  JOIN baiduShow BY reqid LEFT OUTER, bidderBaidu BY req;
tanxShow = FOREACH tanxShowAll GENERATE baiduShow::logtime as logtime,baiduShow::reqid as reqid,baiduShow::qaxid as qaxid,baiduShow::adviewid as adviewid,bidderBaidu::deviceid as devid,baiduShow::channel as channel;
---------取other的show-----------
otherShowAll = FILTER Showagsid BY channel!='ADVIEW' and channel!='QAX' and channel!='BAIDUAPP';
otherShow = FOREACH otherShowAll GENERATE logtime,reqid,'' as qaxid,channelid as adviewid,devid,channel;

showDataQaxAdview = UNION qaxShow, AdviewShow;
showDataTanxOther = UNION tanxShow,otherShow;
showDataRes = UNION showDataQaxAdview, showDataTanxOther;
showData = DISTINCT showDataRes;

--------tt join show------------
ttWithShowAll = JOIN dspttYH BY reqid LEFT OUTER, showData BY reqid;
ttWithShow = FOREACH ttWithShowAll GENERATE dspttYH::reqid as reqid,dspttYH::agsid as agsid,dspttYH::mid as mid,dspttYH::channelContctgr as channelContctgr,dspttYH::conttitle as conttitle,dspttYH::ft as ft,dspttYH::province as province,dspttYH::city as city,dspttYH::channel as channel,dspttYH::ip as ip,dspttYH::ua as ua,dspttYH::vrd as vrd,showData::qaxid as qaxid,showData::adviewid as adviewid,dspttYH::logtime as logtime;

----------------------------------tt join cm by reqid-------------------------------------------
reqCmTagAll = join ttWithShow by agsid LEFT OUTER, user by agsid;
--reqCmTag = FOREACH reqCmTagAll GENERATE reqCmData::channelid as YHID, reqCmData::agsid as agsid,reqCmData::reqid as reqid, user::tag as tag;
--ttReqCmDuplicateAgsid = JOIN reqCmTag BY reqid ,ttWithShow by reqid; 
ttReqCm = FOREACH reqCmTagAll GENERATE ttWithShow::agsid as YHID, ttWithShow::agsid as agsid, user::tag as tag, ttWithShow::mid as mid, ttWithShow::channelContctgr as channelContctgr, ttWithShow::conttitle as conttitle, ttWithShow::ft as ft, ttWithShow::province as province, ttWithShow::city as city,ttWithShow::channel as channel, ttWithShow::ip as ip,ttWithShow::ua as ua,ttWithShow::qaxid as qaxid,ttWithShow::adviewid as adviewid,ttWithShow::vrd as vrd,ttWithShow::logtime as logtime;

----------------------join ad3---------------------------------
reqYHTTAd3 = join ttReqCm by mid LEFT OUTER, ad3 by mid;
simplePvYHTTAd3 = foreach reqYHTTAd3 GENERATE ttReqCm::YHID as yangheid, ttReqCm::agsid as agsid, ttReqCm::tag as tag, ttReqCm::mid as mid, ad3::name as name, ad3::planid as planid, ad3::planname as planname, ttReqCm::channelContctgr as channelContctgr, ttReqCm::conttitle as conttitle, ttReqCm::ft as ft,ttReqCm::province as province, ttReqCm::city as city, ttReqCm::channel as channel, ttReqCm::ip as ip,ttReqCm::ua as ua,ttReqCm::qaxid as qaxid,ttReqCm::adviewid as adviewid,ttReqCm::vrd as vrd,ttReqCm::logtime as logtime;

---当初为了图简便，下面这部分处理代码是直接粘过来的
-----------------ext--agsid,agContctgr,conttitle,ft,province,city,channel;
contentCategory = load '/user/yukun.huang/contentCategory.txt' USING PigStorage()  as (agContctgr: chararray,channel: chararray,channelContctgr: chararray);-- using parquet.pig.ParquetLoader();

dspttPVYHExtDataAgCCcode = join simplePvYHTTAd3 by (channel,channelContctgr) LEFT OUTER,contentCategory by (channel,channelContctgr);

extData = foreach dspttPVYHExtDataAgCCcode generate simplePvYHTTAd3::yangheid as yangheid,simplePvYHTTAd3::agsid as agsid,simplePvYHTTAd3::tag as tag,simplePvYHTTAd3::mid as mid,simplePvYHTTAd3::name as name,simplePvYHTTAd3::planid as planid,simplePvYHTTAd3::planname as planname,simplePvYHTTAd3::qaxid as qaxid,simplePvYHTTAd3::adviewid as adviewid,contentCategory::agContctgr as agContctgr,simplePvYHTTAd3::conttitle as conttitle,simplePvYHTTAd3::ft as ft,simplePvYHTTAd3::province as province,simplePvYHTTAd3::city as city,simplePvYHTTAd3::channel as channel,simplePvYHTTAd3::ip as ip,simplePvYHTTAd3::vrd as vrd,simplePvYHTTAd3::ua as ua,simplePvYHTTAd3::logtime as logtime;

----------------------------------------------最后根据洋河id合并------------------
extDataGroup =  Group extData by yangheid;--按照洋河id分组 agsid mid planid 合并了
--格式化agsid da di mid name planid planname
yanghedata = foreach extDataGroup generate group as yangheid,FLATTEN(YangheResultEvalFunc(extData)) as (agsid: chararray,qaxid: chararray,adviewid: chararray,age: chararray,di: chararray,mid: chararray,name: chararray,planid: chararray,planname: chararray,agContctgr: chararray,conttitle: chararray,ft: chararray,city:chararray,ip: chararray,channel: chararray,vrd:chararray,ua:chararray,logtime:chararray);
yanghedataRes = FILTER yanghedata BY yangheid is not null and yangheid != '' and yangheid != '0';
orderyanghedata = order yanghedataRes by yangheid;
STORE orderyanghedata INTO '$outputdirTTReq' USING PigStorage('\t');

------------------------出show文件------------------
--showResAll = join showData by reqid, cmAllYHTime by reqid;
--showRes = FOREACH showResAll GENERATE cmAllYHTime::channelid as yangheid, cmAllYHTime::agsid as agsid, showData::qaxid as qaxid,showData::adviewid as adviewid;
showRes = FOREACH showData GENERATE devid as yangheid,devid,qaxid,adviewid,channel,logtime;
showResFinal = FILTER showRes BY devid is not null and devid != '' and devid != '0';
showResGroupSrc = GROUP showResFinal BY yangheid;
showResGroup = FOREACH showResGroupSrc GENERATE group,COUNT(showResFinal.yangheid) as times;
showFinal = join showResFinal by yangheid,showResGroup by group;
showFinalRes = FOREACH showFinal GENERATE showResFinal::yangheid as yangheid,showResFinal::devid as devid,showResFinal::qaxid as qaxid,showResFinal::adviewid as adviewid,showResFinal::channel as channel,showResFinal::logtime as logtime,showResGroup::times as times;
orderShowRes = order showFinalRes by times desc,yangheid;
STORE orderShowRes INTO '$outputdirShow' USING PigStorage('\t');