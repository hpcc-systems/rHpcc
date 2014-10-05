hpcc.mean <-
	function(dataframe,fields){
		
		strim<-function (dataframe)
		{
			gsub("^\\s+|\\s+$", "", dataframe)
		}
		
		out.dataframe <- hpcc.get.name()
		varlst<-strsplit(fields, ",")
		str1<-''
		str2<-''
		for (i in 1:length(varlst[[1]]))
		{
			k<-strim(varlst[[1]][i])
			h<-strsplit(k," ")
			
			charh<-paste("'",h[[1]][1],"'",sep="")
			str1<-strim(paste(str1,charh,sep=","))
			hh<-strim(paste("LEFT.",h[[1]][1],sep=""))
			str2<-strim(paste(str2,hh,sep=","))
		}
		recavg <- hpcc.get.name()
		code<-sprintf("%s :=RECORD\n",recavg)
		code<-sprintf("%sINTEGER3 id;\n",code)
		code<-sprintf("%s%s;\nEND;\n",code,dataframe)
		avgtrans <- hpcc.get.name()
		code<-sprintf("%s%s %s (%s L, INTEGER C) := TRANSFORM\n",code,recavg,avgtrans,dataframe)
		code<-sprintf("%sSELF.id :=C;\n",code)
		code<-sprintf("%sSELF :=L;\nEND;\n",code)
		DSRAVG <- hpcc.get.name()
		code<-sprintf("%s%s:=PROJECT(%s,%s(LEFT,COUNTER));\n",code,DSRAVG,dataframe,avgtrans)
		NumAvgField <- hpcc.get.name()
		code<-sprintf("%s%s:=RECORD\n",code,NumAvgField)
		code<-sprintf("%sUNSIGNED id;\n",code)
		code<-sprintf("%sSTRING field;\n",code)
		code<-sprintf("%sREAL8 value;\nEND;\n",code)
		OutDsavg <- hpcc.get.name()
		code<-sprintf("%s%s:=NORMALIZE(%s,%s,TRANSFORM
							  (%s,SELF.id:=LEFT.id,SELF.field:=CHOOSE
							  (COUNTER%s);SELF.value:=CHOOSE(COUNTER%s)));\n",
					  code,OutDsavg,DSRAVG,length(varlst[[1]]),NumAvgField,str1,str2)
		
		SingleavgField <- hpcc.get.name()
		code<-sprintf("%s%s := RECORD\n",code,SingleavgField)
		code<-sprintf("%s%s.field;\n",code,OutDsavg)
		code<-sprintf("%sMean := AVE(GROUP,%s.value);\nEND;\n",code,OutDsavg)
		code<-sprintf("%s%s:= TABLE(%s,%s,field);\n",code,out.dataframe,OutDsavg,SingleavgField)
		hpcc.submit(code)
		return(out.dataframe)
	}
