hpcc.mode <-
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
		recmax <- hpcc.get.name()
		xyz<-sprintf("%s :=RECORD\n",recmax)
		xyz<-sprintf("%sINTEGER3 id;\n",xyz)
		xyz<-sprintf("%s%s;\nEND;\n",xyz,dataframe)
		maxtrans <- hpcc.get.name()
		xyz<-sprintf("%s%s %s (%s L, INTEGER C) := TRANSFORM\n",xyz,recmax,maxtrans,dataframe)
		xyz<-sprintf("%sSELF.id :=C;\n",xyz)
		xyz<-sprintf("%sSELF :=L;\nEND;\n",xyz)
		DSRMAX <- hpcc.get.name()
		xyz<-sprintf("%s%s:=PROJECT(%s,%s(LEFT,COUNTER));\n",xyz,DSRMAX,dataframe,maxtrans)
		MaxField <- hpcc.get.name()
		xyz<-sprintf("%s%s:=RECORD\n",xyz,MaxField)
		xyz<-sprintf("%sUNSIGNED id;\n",xyz)
		xyz<-sprintf("%sSTRING Field;\n",xyz)
		xyz<-sprintf("%sUNSIGNED4 number;\n",xyz)
		xyz<-sprintf("%sREAL8 value;\nEND;\n",xyz)
		OutDsMode <- hpcc.get.name()
		xyz<-sprintf("%s%s:=NORMALIZE(%s,%s,
					 TRANSFORM(%s,SELF.id:=LEFT.id,SELF.number:=COUNTER;
					 SELF.Field:=CHOOSE(COUNTER%s);
					 SELF.value:=CHOOSE(COUNTER%s)));\n",xyz,
					 OutDsMode,DSRMAX,length(varlst[[1]]),MaxField,str1,str2)
		
		
		
		RankableField <- hpcc.get.name()
		xyz <- sprintf("%s%s := RECORD\n",xyz,RankableField)
		xyz<-sprintf("%s%s;\n",xyz,OutDsMode)
		xyz<-sprintf("%sUNSIGNED Pos := 0;\nEND;\n",xyz)
		Ta <- hpcc.get.name()
		xyz<-sprintf("%s%s := TABLE(SORT(%s,Number,field,Value),%s);\n",xyz,Ta,OutDsMode,RankableField)
		add_rank <- hpcc.get.name()
		xyz<-sprintf("%sTYPEOF(%s) %s(%s le,UNSIGNED c) := TRANSFORM\n",xyz,Ta,add_rank,Ta)
		xyz<-sprintf("%sSELF.Pos := c;\n",xyz)
		xyz<-sprintf("%sSELF := le;\nEND;\n",xyz)
		P <- hpcc.get.name()
		xyz<-sprintf("%s %s:= PROJECT(%s,%s(LEFT,COUNTER));\n",xyz,P,Ta,add_rank) 
		RS <- hpcc.get.name()
		xyz<-sprintf("%s%s := RECORD\n",xyz,RS)
		xyz<-sprintf("%sSeq := MIN(GROUP,%s.pos);\n",xyz,P)
		xyz<-sprintf("%s%s.number;\n",xyz,P)
		xyz<-sprintf("%s%s.field;\nEND;\n",xyz,P)
		Splits <- hpcc.get.name()
		xyz<-sprintf("%s%s := TABLE(%s,%s,number,field,FEW);\n",xyz,Splits,P,RS)
		xyz<-sprintf("%sTYPEOF(%s) to(%s le,%s ri) := TRANSFORM\n",xyz,Ta,P,Splits)
		xyz<-sprintf("%sSELF.pos := 1+le.pos - ri.Seq;\n",xyz)
		xyz<-sprintf("%sSELF := le;\n",xyz)
		xyz<-sprintf("%sEND;\n",xyz)
		outfile <- hpcc.get.name()
		xyz<-sprintf("%s%s := JOIN(%s,%s,LEFT.number=RIGHT.number,to(LEFT,RIGHT),LOOKUP);\n",xyz,outfile,P,Splits)
		xyz<-sprintf("%sn := COUNT(%s);\n",xyz,OutDsMode)
		ModeRec <- hpcc.get.name()
		xyz<-sprintf("%s%s := RECORD\n",xyz,ModeRec)
		xyz<-sprintf("%s%s.number;\n",xyz,outfile)
		xyz<-sprintf("%s%s.value;\n",xyz,outfile)
		xyz<-sprintf("%s%s.field;\n",xyz,outfile)
		xyz<-sprintf("%s%s.pos;\n",xyz,outfile)
		xyz<-sprintf("%svals := COUNT(GROUP);\n",xyz)
		xyz<-sprintf("%sEND;\n",xyz)
		MTable <- hpcc.get.name()
		xyz<-sprintf("%s%s := TABLE(%s,%s,number,field,value);\n",xyz,MTable,outfile,ModeRec)
		newRec <- hpcc.get.name()
		xyz<-sprintf("%s%s := RECORD\n",xyz,newRec)
		xyz<-sprintf("%s%s.number;\n",xyz,MTable)
		xyz<-sprintf("%s%s.value;\n",xyz,MTable)
		xyz<-sprintf("%s%s.field;\n",xyz,MTable)
		xyz<-sprintf("%spo := (%s.pos*%s.vals + ((%s.vals-1)*%s.vals/2))/%s.vals;\n",
					 xyz,MTable,MTable,MTable,MTable,MTable)
		xyz<-sprintf("%sEND;\n",xyz)
		
		newTable <- hpcc.get.name()
		xyz<-sprintf("%s%s := TABLE(%s,%s);\n",xyz,newTable,MTable,newRec)
		modT <- hpcc.get.name()
		xyz<-sprintf("%s%s := TABLE(%s,{number,cnt:=MAX(GROUP,vals)},number);\n",xyz,modT,MTable)
		Modes <- hpcc.get.name()
		xyz<-sprintf("%s%s:=JOIN(%s,%s,LEFT.number=RIGHT.number AND LEFT.vals=RIGHT.cnt);\n",
					 xyz,Modes,MTable,modT)
		ModesRec <- hpcc.get.name()
		xyz<-sprintf("%s%s := RECORD\n",xyz,ModesRec)
		xyz<-sprintf("%sfield := %s.field;\n",xyz,Modes)
		xyz<-sprintf("%smode := %s.value;\n",xyz,Modes)
		xyz<-sprintf("%s%s.cnt;\n",xyz,Modes)
		xyz<-sprintf("%sEND;\n",xyz)
		
		xyz<-sprintf("%s%s:= TABLE(%s,%s);\n",xyz,out.dataframe,Modes,ModesRec)
		hpcc.submit(xyz)
		return(out.dataframe)
	}
