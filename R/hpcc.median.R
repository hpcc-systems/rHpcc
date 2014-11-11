hpcc.median <-
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
		xyz<-sprintf("%sUNSIGNED4 number;\n",xyz)
		xyz<-sprintf("%sSTRING Field;\n",xyz)
		xyz<-sprintf("%sREAL8 value;\nEND;\n",xyz)
		
		OutDsMed <- hpcc.get.name()
		xyz<-sprintf("%s%s:=NORMALIZE(%s,%s,TRANSFORM(%s,SELF.id:=LEFT.id,SELF.number:=COUNTER;
					 SELF.Field:=CHOOSE(COUNTER%s);SELF.value:=CHOOSE(COUNTER%s)));\n",
					 xyz,OutDsMed,DSRMAX,length(varlst[[1]]),MaxField,str1,str2)
		
		
		RankableField <- hpcc.get.name()
		xyz<-sprintf("%s%s := RECORD\n",xyz,RankableField)
		xyz<-sprintf("%s%s;\n",xyz,OutDsMed)
		xyz<-sprintf("%sUNSIGNED Pos := 0;\nEND;\n",xyz)
		
		Ta <- hpcc.get.name()
		xyz<-sprintf("%s%s := TABLE(SORT(%s,Number,field,Value),%s);\n",xyz,Ta,OutDsMed,RankableField)
		
		xyz<-sprintf("%sTYPEOF(%s) add_rank(%s le,UNSIGNED c) := TRANSFORM\n",xyz,Ta,Ta)
		xyz<-sprintf("%sSELF.Pos := c;\n",xyz)
		xyz<-sprintf("%sSELF := le;\nEND;\n",xyz)
		
		P <- hpcc.get.name()
		xyz<-sprintf("%s%s := PROJECT(%s,add_rank(LEFT,COUNTER));\n",xyz,P,Ta)  
		RS <- hpcc.get.name()
		xyz<-sprintf("%s%s := RECORD\n",xyz,RS)
		xyz<-sprintf("%sSeq := MIN(GROUP,%s.pos);\n",xyz,P)
		xyz<-sprintf("%s%s.number;\n",xyz,P)
		xyz<-sprintf("%s%s.field;\n",xyz,P)
		xyz<-sprintf("%sEND;\n",xyz)
		Splits <- hpcc.get.name()
		xyz<-sprintf("%s%s := TABLE(%s,%s,number,field,FEW);\n",xyz,Splits,P,RS)
		xyz<-sprintf("%sTYPEOF(%s) to(%s le,%s ri) := TRANSFORM\n",xyz,Ta,P,Splits)
		xyz<-sprintf("%sSELF.pos := 1+le.pos - ri.Seq;\n",xyz)
		xyz<-sprintf("%sSELF := le;\nEND;\n",xyz)
		outfile <- hpcc.get.name()
		xyz<-sprintf("%s%s := JOIN(%s,%s,LEFT.number=RIGHT.number,to(LEFT,RIGHT),LOOKUP);\n",xyz,outfile,P,Splits)
		n <- hpcc.get.name()
		xyz<-sprintf("%s%s := COUNT(%s);\n",xyz,n,DSRMAX)
		MedRec <- hpcc.get.name()
		xyz<-sprintf("%s%s := RECORD\n",xyz,MedRec)
		xyz<-sprintf("%s%s.number;\n",xyz,outfile)
		xyz<-sprintf("%sSET OF UNSIGNED poso := IF(%s%s2=0,[%s/2,%s/2 + 1],[(%s+1)/2]);\nEND;\n",xyz,n,'%',n,n,n)
		MyT <- hpcc.get.name()
		xyz<-sprintf("%s%s := TABLE(%s,%s,field,number);\n",xyz,MyT,outfile,MedRec)
		MedianValues <- hpcc.get.name()
		xyz<-sprintf("%s%s:=JOIN(%s,%s,LEFT.number=RIGHT.number AND LEFT.pos IN RIGHT.poso);\n",
					 xyz,MedianValues,outfile,MyT)
		medianRec <- hpcc.get.name()
		xyz<-sprintf("%s%s := RECORD\n",xyz,medianRec)
		xyz<-sprintf("%s%s.field;\n",xyz,MedianValues)
		xyz<-sprintf("%sMedian := AVE(GROUP, %s.value);\n",xyz,MedianValues)
		xyz<-sprintf("%sEND;\n",xyz)
		
		xyz<-sprintf("%s%s:= TABLE(%s,%s,field);\n",xyz,out.dataframe,MedianValues,medianRec)
		hpcc.submit(xyz)
		return(out.dataframe)
	}
