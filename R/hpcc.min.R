hpcc.min <-
	function(dataframe,fields) {
	
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
		
		charh<-sprintf("'%s'",h[[1]][1])
		str1<-strim(sprintf("%s,%s",str1,charh))
		hh<-strim(sprintf("LEFT.%s",h[[1]][1]))
		str2<-strim(sprintf("%s,%s",str2,hh))		
	}
	recmin <- hpcc.get.name()
	xyz <- recmin
	xyz<-sprintf("%s :=RECORD\n",xyz)
	xyz<-sprintf("%sINTEGER3 id;\n",xyz)
	xyz<-sprintf("%s %s ;\nEND;\n",xyz,dataframe)
	mintrans <- hpcc.get.name()
	xyz<-sprintf("%s%s %s (%s L, INTEGER C) := TRANSFORM\n",xyz,recmin,mintrans,dataframe)
	xyz<-sprintf("%sSELF.id :=C;\n",xyz)
	xyz<-sprintf("%sSELF :=L;\nEND;\n",xyz)
	DSRMIN <- hpcc.get.name()
	xyz<-sprintf("%s%s:=PROJECT(%s,%s(LEFT,COUNTER));\n",xyz,DSRMIN,dataframe,mintrans)
	NumField <-  hpcc.get.name()
	xyz<-sprintf("%s%s:=RECORD\n",xyz,NumField)
	xyz<-sprintf("%sUNSIGNED id;\n",xyz)
	xyz<-sprintf("%sSTRING Field;\nREAL8 value;\nEND;\n",xyz)
	OutDsMin <- hpcc.get.name()
	xyz<-sprintf("%s%s:=NORMALIZE(%s,%s,TRANSFORM(%s,SELF.id:=LEFT.id,SELF.Field:=CHOOSE
				 (COUNTER%s);SELF.value:=CHOOSE(COUNTER%s)));\n",
				 xyz,OutDsMin,DSRMIN,length(varlst[[1]]),NumField,str1,str2)
	SingleField <- hpcc.get.name()
	xyz<-sprintf("%s%s := RECORD\n",xyz,SingleField)
	xyz<-sprintf("%s%s.Field;\n",xyz,OutDsMin)
	xyz<-sprintf("%sMinval := MIN(GROUP,%s.value);\nEND;\n",xyz,OutDsMin)
	xyz<-sprintf("%s%s:= TABLE(%s,%s,Field);\n",xyz,out.dataframe,OutDsMin,SingleField)
	hpcc.submit(xyz)
	return(out.dataframe)
}
