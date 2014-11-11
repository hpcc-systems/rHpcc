hpcc.corr <-
	function(dataframe,fields,method) {
		
		strim<-function (x)
		{
			gsub("^\\s+|\\s+$", "", x)
			gsub("^,+|,+$", "", x)
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
		xyz<-sprintf("%sINTEGER3 id; \n",xyz)
		xyz<-paste(xyz,paste(dataframe,";"),"\n")
		xyz<-sprintf("%sEND;\n",xyz)
		maxtrans <- hpcc.get.name()
		xyz<-sprintf("%s%s %s (%s  L, INTEGER C) := TRANSFORM\n",xyz,recmax,maxtrans,dataframe)
		xyz<-sprintf("%sSELF.id :=C;\n",xyz)
		xyz<-sprintf("%sSELF :=L;\n",xyz)
		xyz<-sprintf("%sEND;\n",xyz)
		DSRMAX <- hpcc.get.name()
		xyz<-sprintf("%s%s:=PROJECT(%s ,%s(LEFT,COUNTER));\n",xyz,DSRMAX,dataframe,maxtrans)
		
		NumericField <- hpcc.get.name()
		xyz<-sprintf("%s%s :=RECORD\n",xyz,NumericField)
		xyz<-sprintf("%sUNSIGNED id;\n",xyz)
		xyz<-sprintf("%sUNSIGNED4 number;\n",xyz)
		xyz<-sprintf("%sREAL8 value;\n",xyz)
		xyz<-sprintf("%sSTRING field;\n",xyz)
		xyz<-sprintf("%sEND;\n",xyz)
		
		OutDs <- hpcc.get.name()
		xyz<-sprintf("%s%s:=NORMALIZE(%s,%s,TRANSFORM(%s,SELF.id:=LEFT.id,
					 SELF.number:=COUNTER;
					 SELF.field:=CHOOSE(COUNTER,%s);
					 SELF.value:=CHOOSE(COUNTER,%s)));\n"
					 ,xyz,OutDs,DSRMAX,length(varlst[[1]]),NumericField,str1,str2)
		
		RankableField <- hpcc.get.name()
		xyz<-sprintf("%s%s :=RECORD\n",xyz,RankableField)
		xyz<-sprintf("%s%s;\n",xyz,OutDs)
		xyz<-sprintf("%sUNSIGNED Pos := 0;\n",xyz)
		xyz<-sprintf("%sEND;\n",xyz)
		Ta <- hpcc.get.name()
		xyz<-sprintf("%s%s :=TABLE(SORT(%s,Number,field,Value),%s);\n"
					 ,xyz,Ta,OutDs,RankableField)
		
		add_rank <- hpcc.get.name()
		xyz<-sprintf("%sTYPEOF(%s) %s(%s le,UNSIGNED c) := TRANSFORM\n",xyz,Ta,add_rank,Ta)
		xyz<-sprintf("%sSELF.Pos := c;\n",xyz)
		xyz<-sprintf("%sSELF := le;\n",xyz)
		xyz<-sprintf("%sEND;\n",xyz)
		P <- hpcc.get.name()
		xyz<-sprintf("%s%s := PROJECT(%s,%s(LEFT,COUNTER));\n",xyz,P,Ta,add_rank)
		RS <- hpcc.get.name()
		xyz<-sprintf("%s%s := RECORD\n",xyz,RS)
		xyz<-sprintf("%sSeq := MIN(GROUP,%s.pos);\n",xyz,P)
		xyz<-sprintf("%s%s.number;\n",xyz,P)
		xyz<-sprintf("%sEND;\n",xyz)
		Splits <- hpcc.get.name()
		xyz<-sprintf("%s%s := TABLE(%s,%s,number,FEW);\n",xyz,Splits,P,RS)
		
		to <- hpcc.get.name()
		xyz<-sprintf("%sTYPEOF(%s) %s(%s le,%s ri) := TRANSFORM\n",xyz,Ta,to,P,Splits)
		xyz<-sprintf("%sSELF.pos := 1+le.pos - ri.Seq;\n",xyz)
		xyz<-sprintf("%sSELF := le;\n",xyz)
		xyz<-sprintf("%sEND;\n",xyz)
		outfile <- hpcc.get.name()
		xyz<-sprintf("%s%s := JOIN(%s,%s,LEFT.number=RIGHT.number,%s(LEFT,RIGHT),LOOKUP);\n"
					 ,xyz,outfile,P,Splits,to)
		
		modeRec <- hpcc.get.name()
		xyz<-sprintf("%s%s := RECORD\n",xyz,modeRec)
		xyz<-sprintf("%s%s.number;\n",xyz,outfile)
		xyz<-sprintf("%s%s.value;\n",xyz,outfile)
		xyz<-sprintf("%s%s.pos;\n",xyz,outfile)
		xyz<-sprintf("%s%s.field;\n",xyz,outfile)
		xyz<-sprintf("%svals := COUNT(GROUP);\n",xyz)
		xyz<-sprintf("%sEND;\n",xyz)
		MTable <- hpcc.get.name()
		xyz<-sprintf("%s%s := TABLE(%s,%s,number,field,value);\n",xyz,MTable,outfile,modeRec)
		#xyz<-sprintf("%sMTable;\n",xyz)
		
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
		#xyz<-sprintf("%sOUTPUT(newTable,NAMED('TEST'));\n",xyz)
		TestTab <- hpcc.get.name()
		xyz<-sprintf("%s%s := JOIN(%s,%s,LEFT.number = RIGHT.number AND LEFT.value = RIGHT.value);\n",
					 xyz,TestTab,outfile,newTable)
		#xyz<-sprintf("%sTestTab;\n",xyz)
		
		MyRec <- hpcc.get.name()
		xyz<-sprintf("%s%s := RECORD\n",xyz,MyRec)
		xyz<-sprintf("%s%s;\n",xyz,TestTab)
		xyz<-sprintf("%sEND;\n",xyz)
		T1 <- hpcc.get.name()
		xyz<-sprintf("%s%s := TABLE(%s,%s,id,number,field);\n",xyz,T1,TestTab,MyRec)
		
		SingleForm <- hpcc.get.name()
		single <- hpcc.get.name()
		PairRec <- hpcc.get.name()
		note_prod <- hpcc.get.name()
		pairs <- hpcc.get.name()
		PairAccum <- hpcc.get.name()
		exys <- hpcc.get.name()
		with_x <- hpcc.get.name()
		Rec <- hpcc.get.name()
		n <- hpcc.get.name()
		Trans <- hpcc.get.name()
		intjoin <- hpcc.get.name()
		
		if (method=='S')
		{
			xyz<-sprintf("%s%s := Record\n",xyz,SingleForm)
			xyz<-sprintf("%s%s.number;\n",xyz,T1)
			xyz<-sprintf("%s%s.field;\n",xyz,T1)
			xyz<-sprintf("%sREAL8 meanS := AVE(GROUP,%s.po);\n",xyz,T1)
			xyz<-sprintf("%sREAL8 sdS := SQRT(VARIANCE(GROUP,%s.po));\n",xyz,T1)
			xyz<-sprintf("%sEND;\n",xyz)
			
			xyz<-sprintf("%s%s := TABLE(%s,%s,number,field,FEW);",xyz,single,T1,SingleForm)
			
			xyz<-sprintf("%s%s := RECORD\n",xyz,PairRec)
			xyz<-sprintf("%sUNSIGNED4 left_number;\n",xyz)
			xyz<-sprintf("%sUNSIGNED4 right_number;\n",xyz)
			xyz<-sprintf("%sSTRING left_field;\n",xyz)
			xyz<-sprintf("%sSTRING right_field;\n",xyz)
			xyz<-sprintf("%sREAL8   xyS;\n",xyz)
			xyz<-sprintf("%sEND;\n",xyz)
			
			xyz<-sprintf("%s%s %s(%s L, %s R) := TRANSFORM\n",xyz,PairRec,note_prod,T1,T1)
			xyz<-sprintf("%sSELF.left_number := L.number;\n",xyz)
			xyz<-sprintf("%sSELF.right_number := R.number;\n",xyz)
			xyz<-sprintf("%sSELF.left_field := L.field;\n",xyz)
			xyz<-sprintf("%sSELF.right_field := R.field;\n",xyz)
			xyz<-sprintf("%sSELF.xyS := L.po*R.po;\n",xyz)
			xyz<-sprintf("%sEND;\n",xyz)
			
			xyz<-sprintf("%s%s := JOIN(%s,%s,LEFT.id=RIGHT.id AND LEFT.number<RIGHT.number,
						 %s(LEFT,RIGHT));\n"
						 ,xyz,pairs,T1,T1,note_prod)
			
			
			xyz<-sprintf("%s%s := RECORD\n",xyz,PairAccum)
			xyz<-sprintf("%s%s.left_number;\n",xyz,pairs)
			xyz<-sprintf("%s%s.right_number;\n",xyz,pairs)
			xyz<-sprintf("%s%s.left_field;\n",xyz,pairs)
			xyz<-sprintf("%s%s.right_field;\n",xyz,pairs)
			xyz<-sprintf("%se_xyS := SUM(GROUP,%s.xyS);\n",xyz,pairs)
			xyz<-sprintf("%sEND;\n",xyz)
			
			xyz<-sprintf("%s%s := TABLE(%s,%s,left_number,right_number,left_field,right_field,FEW);\n"
						 ,xyz,exys,pairs,PairAccum)
			xyz<-sprintf("%s%s := JOIN(%s,%s,LEFT.left_number = RIGHT.number,LOOKUP);\n"
						 ,xyz,with_x,exys,single)
			
			
			xyz<-sprintf("%s%s := RECORD\n",xyz,Rec)
			xyz<-sprintf("%sUNSIGNED left_number;\n",xyz)
			xyz<-sprintf("%sUNSIGNED right_number;\n",xyz)
			xyz<-sprintf("%sSTRING left_field;\n",xyz)
			xyz<-sprintf("%sSTRING right_field;\n",xyz)
			xyz<-sprintf("%sREAL8 Spearman;\n",xyz)
			xyz<-sprintf("%sEND;\n",xyz)
			
			xyz<-sprintf("%s%s := COUNT(%s);\n",xyz,n,dataframe)
			
			
			xyz<-sprintf("%s%s %s(%s L, %s R) := TRANSFORM\n"
						 ,xyz,Rec,Trans,with_x,single)
			xyz<-sprintf("%sSELF.Spearman := (L.e_xyS - %s*L.meanS*R.meanS)/(%s*L.sdS*R.sdS);\n",xyz,n,n)
			xyz<-sprintf("%sSELF := L;\n",xyz)
			xyz<-sprintf("%sEND;\n",xyz)
			xyz<-sprintf("%s%s:= JOIN(%s,%s,LEFT.right_number=RIGHT.number,%s(LEFT,RIGHT),LOOKUP);\n"
						 ,xyz,intjoin,with_x,single,Trans)
			xyz<-paste(xyz,paste(out.dataframe,":=SORT(TABLE(",intjoin,",{left_field,right_field,Spearman}),
								 left_field,right_field);\n"))
		}
		else if (method=='P')
		{
			xyz<-sprintf("%s%s := Record\n",xyz,SingleForm)
			xyz<-sprintf("%s%s.number;\n",xyz,T1)
			xyz<-sprintf("%s%s.field;\n",xyz,T1)
			xyz<-sprintf("%sREAL8 meanP := AVE(GROUP,%s.value);\n",xyz,T1)
			xyz<-sprintf("%sREAL8 sdP := SQRT(VARIANCE(GROUP,%s.value));\n",xyz,T1)
			xyz<-sprintf("%sEND;\n",xyz)
			
			xyz<-sprintf("%s%s := TABLE(%s,%s,number,field,FEW);",xyz,single,T1,SingleForm)
			
			xyz<-sprintf("%s%s := RECORD\n",xyz,PairRec)
			xyz<-sprintf("%sUNSIGNED4 left_number;\n",xyz)
			xyz<-sprintf("%sUNSIGNED4 right_number;\n",xyz)
			xyz<-sprintf("%sSTRING left_field;\n",xyz)
			xyz<-sprintf("%sSTRING right_field;\n",xyz)
			xyz<-sprintf("%sREAL8   xyP;\n",xyz)
			xyz<-sprintf("%sEND;\n",xyz)
			
			xyz<-sprintf("%s%s %s(%s L, %s R) := TRANSFORM\n",xyz,PairRec,note_prod,T1,T1)
			xyz<-sprintf("%sSELF.left_number := L.number;\n",xyz)
			xyz<-sprintf("%sSELF.right_number := R.number;\n",xyz)
			xyz<-sprintf("%sSELF.left_field := L.field;\n",xyz)
			xyz<-sprintf("%sSELF.right_field := R.field;\n",xyz)
			xyz<-sprintf("%sSELF.xyP := L.value*R.value;\n",xyz)
			xyz<-sprintf("%sEND;\n",xyz)
			
			xyz<-sprintf("%s%s := JOIN(%s,%s,LEFT.id=RIGHT.id AND LEFT.number<RIGHT.number,
						 %s(LEFT,RIGHT));\n"
						 ,xyz,pairs,T1,T1,note_prod)
			
			xyz<-sprintf("%s%s := RECORD\n",xyz,PairAccum)
			xyz<-sprintf("%s%s.left_number;\n",xyz,pairs)
			xyz<-sprintf("%s%s.right_number;\n",xyz,pairs)
			xyz<-sprintf("%s%s.left_field;\n",xyz,pairs)
			xyz<-sprintf("%s%s.right_field;\n",xyz,pairs)
			xyz<-sprintf("%se_xyP := SUM(GROUP,%s.xyP);\n",xyz,pairs)
			xyz<-sprintf("%sEND;\n",xyz)
			
			xyz<-sprintf("%s%s := TABLE(%s,%s,left_number,right_number,left_field,right_field,FEW);\n"
						 ,xyz,exys,pairs,PairAccum)
			xyz<-sprintf("%s%s := JOIN(%s,%s,LEFT.left_number = RIGHT.number,LOOKUP);\n"
						 ,xyz,with_x,exys,single)
			
			xyz<-sprintf("%s%s := RECORD\n",xyz,Rec)
			xyz<-sprintf("%sUNSIGNED left_number;\n",xyz)
			xyz<-sprintf("%sUNSIGNED right_number;\n",xyz)
			xyz<-sprintf("%sSTRING left_field;\n",xyz)
			xyz<-sprintf("%sSTRING right_field;\n",xyz)
			xyz<-sprintf("%sREAL8 Pearson;\n",xyz)
			xyz<-sprintf("%sEND;\n",xyz)
			xyz<-sprintf("%s%s := COUNT(%s);\n",xyz,n,dataframe)
			
			xyz<-sprintf("%s%s %s(%s L, %s R) := TRANSFORM\n"
						 ,xyz,Rec,Trans,with_x,single)
			xyz<-sprintf("%sSELF.Pearson := (L.e_xyP - %s*L.meanP*R.meanP)/(%s*L.sdP*R.sdP);\n",xyz,n,n)
			xyz<-sprintf("%sSELF := L;\n",xyz)
			xyz<-sprintf("%sEND;\n",xyz)
			xyz<-paste(xyz,paste(intjoin,":= JOIN(",with_x,",",single,",LEFT.right_number=RIGHT.number,",Trans,"(LEFT,RIGHT),LOOKUP);"),"\n")
			xyz<-paste(xyz,paste(out.dataframe,":=SORT(TABLE(",intjoin,",{left_field,right_field,Pearson}),
						 left_field,right_field);)\n"))
			
		}
		else
		{
			stop("no proper method")  
		}
		hpcc.submit(xyz)
		return(out.dataframe)
	}
