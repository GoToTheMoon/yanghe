--------------------------------------以agsid找人------------------------

---------------------------cm日志-----------
cmAll = load '$inputDirCM' using parquet.pig.ParquetLoader();

cmAllYH = filter cmAll by channel == '$channelName' and channelid is not null and channelid !='';--YH
cmAllYHTime = FOREACH cmAllYH GENERATE agsid,channelid,logtime;
cmAllYHGroup = Group cmAllYHTime by agsid;

--只留下最新agsid和channlid
cm = FOREACH cmAllYHGroup  {
    yhsorted = ORDER cmAllYHTime BY logtime DESC;
    yhlimited = LIMIT yhsorted 1;
    yhlimited_field = FOREACH yhlimited GENERATE agsid,channelid;
    GENERATE FLATTEN(yhlimited_field) AS (agsid,channelid);
};



--------------------爱奇艺和优酷id---根据agsid分组取最新一条，所以不会有同一个agsid有多行的情况-----
cmQaxAll = filter cmAll by channel == 'QAX' ;
cmQaxTime = FOREACH cmQaxAll GENERATE agsid,channelid,logtime;
cmQaxGroup = Group cmQaxTime by agsid;
--只取最新一条渠道id
cmQax = FOREACH cmQaxGroup  {
    qaxsorted = ORDER cmQaxTime BY logtime DESC;
    qaxlimited = LIMIT qaxsorted 1;
    qaxlimited_field = FOREACH qaxlimited GENERATE agsid,channelid;
    GENERATE FLATTEN(qaxlimited_field) AS (agsid,channelid);
}
--cmQax = FOREACH cmQaxAll GENERATE agsid,channelid;

cmYoukuAll = filter cmAll by channel == 'Youku' ;
--cmYouku = FOREACH cmYoukuAll GENERATE agsid,channelid;
cmYoukuTime = FOREACH cmYoukuAll GENERATE agsid,channelid,logtime;
cmYoukuTimeGroup = Group cmYoukuTime by agsid;
cmYouku = FOREACH cmYoukuTimeGroup {
    yksorted = ORDER cmYoukuTime BY logtime DESC;
    yklimited = LIMIT yksorted 1;
    yklimited_field = FOREACH yklimited GENERATE agsid,channelid;
    GENERATE FLATTEN(yklimited_field) AS (agsid,channelid);
}


---------------------------show-----------------------

showAll = load '$inputDirDspShow' using parquet.pig.ParquetLoader();
show = filter showAll by cid == $selectCid and agsid is not null and agsid !='';
Showagsid =  foreach show GENERATE agsid,reqid;

--加洋河id
showYH = join Showagsid by agsid,cm by agsid;--//20170224 改成左连接left OUTER
simpShowYH = foreach showYH GENERATE cm::channelid as yangheid,cm::agsid as agsid,Showagsid::reqid as reqid;

--加qax
showYHQax = join simpShowYH by agsid left OUTER,cmQax by agsid;
simpShowYHQax = foreach showYHQax GENERATE simpShowYH::yangheid as yangheid,simpShowYH::agsid as agsid,cmQax::channelid as qaxid,simpShowYH::reqid as reqid;

--加youku
showYHQaxYouku = join simpShowYHQax by agsid left OUTER,cmYouku by agsid parallel 1;
simpShowYHQaxYouku = foreach showYHQaxYouku GENERATE simpShowYHQax::yangheid as yangheid,simpShowYHQax::agsid as agsid,simpShowYHQax::qaxid as qaxid,cmYouku::channelid as youkuid,simpShowYHQax::reqid as reqid;
orderSimpShowYHQaxYouku = order simpShowYHQaxYouku by yangheid;

------------------------cm 去重,展示不去重----------
--distinctShowYHQaxYouku = DISTINCT simpShowYHQaxYouku ;--去除重复,主要是cm重复了,就会重复,但是我们只要展示不去重复,不要cm重复
--展示不去重
--showagsidDistinctShowYHQaxYouku = join Showagsid by agsid,distinctShowYHQaxYouku by agsid parallel 1;

--result = foreach showagsidDistinctShowYHQaxYouku GENERATE distinctShowYHQaxYouku::yangheid as yangheid,distinctShowYHQaxYouku::agsid as agsid,distinctShowYHQaxYouku::qaxid as qaxid,distinctShowYHQaxYouku::youkuid as youkuid;
STORE orderSimpShowYHQaxYouku INTO '$outputdirShow' USING PigStorage('\t');