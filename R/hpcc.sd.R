hpcc.sd <-
	function(dataframe,fields){
		
		out.dataframe <- hpcc.get.name()
		strim<-function (dataframe)
		{
			gsub("^\\s+|\\s+$", "", dataframe)
		}
		
		varlst<-strsplit(fields, ",")
		str1<-''
		str2<-''
		for (i in 1:length(varlst[[1]]))
		{
			k<-strim(varlst[[1]][i])
			h<-strsplit(k," ")
			if (i > 1)
			{
				charh<-paste("'",h[[1]][1],"'",sep="")
				str1<-strim(paste(str1,charh,sep=","))
				hh<-strim(paste("LEFT.",h[[1]][1],sep=""))
				str2<-strim(paste(str2,hh,sep=","))
			}
			else
			{
				charh<-paste("'",h[[1]][1],"'",sep="")
				str1<-strim(paste(str1,charh))
				hh<-strim(paste("LEFT.",h[[1]][1],sep=""))
				str2<-strim(paste(str2,hh))
			}
			
		}
		recsd <- hpcc.get.name()
		xyz<-sprintf("%s :=RECORD\n",recsd)
		xyz<-sprintf("%sINTEGER3 id;\n",xyz)
		xyz<-sprintf("%s%s;\n",xyz,dataframe)
		xyz<-sprintf("%sEND;\n",xyz)
		sdtrans <- hpcc.get.name()
		xyz<-sprintf("%s%s %s (%s L, INTEGER C) := TRANSFORM\n",
					 xyz,recsd,sdtrans,dataframe)
		xyz<-sprintf("%sSELF.id :=C;\n",xyz)
		xyz<-sprintf("%sSELF :=L;\n",xyz)
		xyz<-sprintf("%sEND;\n",xyz)
		DSRSD <- hpcc.get.name()
		xyz<-sprintf("%s%s:=PROJECT(%s,%s(LEFT,COUNTER));\n",xyz,DSRSD,dataframe,sdtrans)
		NumSdField <- hpcc.get.name()
		xyz<-sprintf("%s%s:=RECORD\n",xyz,NumSdField)
		xyz<-sprintf("%sUNSIGNED id;\n",xyz)
		xyz<-sprintf("%sSTRING field;\n",xyz)
		xyz<-sprintf("%sREAL8 value;\n",xyz)
		xyz<-sprintf("%sEND;\n",xyz)
		OutDsSd <- hpcc.get.name()
		xyz<-sprintf("%s%s:=NORMALIZE(%s,%s,
					TRANSFORM(%s,SELF.id:=LEFT.id,SELF.field:=CHOOSE
					(COUNTER,%s);SELF.value:=CHOOSE(COUNTER,%s)));\n",
					xyz,OutDsSd,DSRSD,length(varlst[[1]]),NumSdField,str1,str2)
		SingleSdField<- hpcc.get.name()
		xyz<-sprintf("%s%s := RECORD\n",xyz,SingleSdField)
		xyz<-sprintf("%s%s.field;\n",xyz,OutDsSd)
		xyz<-sprintf("%sSd := SQRT(VARIANCE(GROUP,%s.value));\n",xyz,OutDsSd)
		xyz<-sprintf("%sEND;\n",xyz)
		xyz<-sprintf("%s%s:= TABLE(%s,%s,field);\n",xyz,out.dataframe,OutDsSd,SingleSdField)
		hpcc.submit(xyz)
		return(out.dataframe)
	}