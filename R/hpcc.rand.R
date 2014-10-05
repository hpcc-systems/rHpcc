hpcc.rand <-
	function(dataframe){
		
		out.dataframe <- hpcc.get.name()
		MyOutRec <- hpcc.get.name()

		xyz<-sprintf("%s := RECORD\n",MyOutRec)
		xyz<-sprintf("%sUNSIGNED DECIMAL8_8 rand;\n",xyz)
		xyz<-sprintf("%s%s;)\n",xyz,dataframe)
		xyz<-sprintf("%sEND;\n",xyz)
		MyTrans <- hpcc.get.name()
		xyz<-paste(xyz,paste("%s %s(%s L, UNSIGNED4 C) := TRANSFORM\n",MyOutRec,MyTrans,dataframe)
		xyz<-sprintf("%sSELF.rand := C/4294967295;\n",xyz)
		xyz<-sprintf("%sSELF := L;\n",xyz)
		xyz<-sprintf("%sEND;\n",xyz)
		
		
		xyz<-sprintf("%s%s:= PROJECT(%s,%s(LEFT, RANDOM()));\n",
					  xyz,out.dataframe,dataframe,MyTrans)
		hpcc.submit(xyz)
		return(out.dataframe)
	}
