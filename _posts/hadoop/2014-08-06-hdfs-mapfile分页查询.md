---
layout: post
category : hadoop
tagline: ""
tags : [hadoop,hdfs,mapfile]
---
{% include JB/setup %}

日志分析项目中需要用到HDFS存储海量nginx日志，并对外提供分页查询明细的服务<br/>
于是用到了mapile<br/>
mapfile的分页逻辑如下：

`@RequestMapping(value = "/iis/detail")
    public ModelAndView iisdetail(String ip,String host,String time,Integer startLine){
        ModelAndView mv = new ModelAndView("/iis/detail");
        BloomMapFile.Reader reader=null;
        try {
            if(startLine==null){
                startLine=1;
            }
            Integer endLine = startLine+99;
            List<String> logs = new ArrayList<String>();
            Date d = DateUtils.p1(time);
            String datepath = DateUtils.daypath(d);
            String hourstr = DateUtils.hourstr(d);
            String p = "/work/iis_un2/"+host+"/"+ip+"/"+datepath+"/"+hourstr+"_z.map";
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", "hdfs://hadoop-lq-194-129:8020");
            FileSystem fs = FileSystem.get(conf);
            int cnum = 0;//已经去掉的数据条数
            int hnum = 0;//已经取得的数据条数
            reader =new  BloomMapFile.Reader(fs,p,conf);
            Text key = new Text(time);
            Text value = new Text();
            reader.get(key,value);
            if(StringUtils.isNotBlank(value.toString())){
                cnum+=1;
                if(startLine==1){
                    hnum +=1;
                    logs.add(value.toString());
                }
            }
            for(int i = 1; i < startLine-cnum; i++){
                reader.next(key,value);
            }
            for(int i = startLine + hnum; i <= endLine; i++){
                boolean readSomething = reader.next(key, value);
                if(!readSomething) break;
                logs.add(value.toString());
            }
            startLine = startLine+100;

            mv.addObject("logs",logs);
            mv.addObject("ip",ip);
            mv.addObject("host",host);
            mv.addObject("time",time);
            mv.addObject("startLine",startLine);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return mv;
    }`