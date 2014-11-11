hpcc.freq <-
	function(dataframe,fields,sortorder=NULL) {
		
		
		strim<-function (x)
		{
			gsub("^\\s+|\\s+$", "", x)
			gsub("^,+|,+$", "", x)
		}
		
		semitrim<-function(x)
		{
			gsub(";","",x)
		}
		
		out.dataframe <- hpcc.get.name()
		
		## split layout var types####
		## respective int and string combos## 
		semiperson<-'' #semitrim(personout)
		split_val<- strsplit(semiperson, "\n")
		int<-grep("integer",split_val[[1]])
		rl<-grep("real",split_val[[1]])
		una<-grep("unasign",split_val[[1]])
		dcml<-grep("decimal",split_val[[1]])
		dbl<-grep("double",split_val[[1]])
		real_int<-c(split_val[[1]][int],split_val[[1]][rl],split_val[[1]][una],split_val[[1]][dcml],split_val[[1]][dbl])
		str<-grep("string",split_val[[1]])
		uni<-grep("unicode",split_val[[1]])
		str_uni<-c(split_val[[1]][str],split_val[[1]][uni])
		
		num_new<-NA
		char_new<-NA
		
		##split the var string###  
		field_splt <- strsplit(fields, ",")
		for (i in 1:length(field_splt[[1]]))
		{
			j<-strim(field_splt[[1]][i])
			
			if (any(grepl(j,real_int)))
				num_new[i]<-j
			else
				char_new[i]<-j      
		}
		
		num_new<-na.omit(num_new)
		char_new<-na.omit(char_new)
		
		is.not.na <- function(x) !is.na(x) 
		is.not.null <- function(x) !is.null(x)
		
		if (is.not.na(char_new[1]))
		{
			varlst<-strsplit(char_new, ",")
			str1<-''
			str2<-''
			for (i in 1:length(char_new))
			{
				k<-strim(varlst[[i]][1])
				h<-strsplit(k," ")
				# 				if (i > 1)
				# 				{
				charh<-paste("'",h[[1]][1],"'",sep="")
				str1<-strim(paste(str1,charh,sep=","))
				hh<-strim(paste("LEFT.",h[[1]][1],sep=""))
				str2<-strim(paste(str2,hh,sep=","))
				# 				}
				# 				else
				# 				{
				# 					charh<-paste("'",h[[1]][1],"'",sep="")
				# 					str1<-strim(paste(str1,charh))
				# 					hh<-strim(paste("LEFT.",h[[1]][1],sep=""))
				# 					str2<-strim(paste(str2,hh))
				# 				}
			}
			NumericField <- hpcc.get.name()
			
			xyz<-sprintf("%s :=RECORD\n",NumericField)
			xyz<-sprintf("%sSTRING field;\n",xyz)
			xyz<-sprintf("%sSTRING value;\nEND;\n",xyz)
			OutDSStr <- hpcc.get.name()
			xyz<-paste(xyz,paste(OutDSStr," := NORMALIZE(",dataframe,",",length(varlst),",
								 TRANSFORM(",NumericField,",SELF.field:=CHOOSE(COUNTER,",str1,");
								 SELF.value:=CHOOSE(COUNTER,",str2,")));",sep=""),"\n")
			FreqRecStr <- hpcc.get.name()
			xyz<-sprintf("%s%s:=RECORD\n",xyz,FreqRecStr)
			xyz<-sprintf("%s%s.field;\n",xyz,OutDSStr)     
			xyz<-sprintf("%s%s.value;\n",xyz,OutDSStr)
			xyz<-sprintf("%sINTEGER frequency:=COUNT(GROUP);\n",xyz)
			xyz<-sprintf("%sREAL8 Percent:=(COUNT(GROUP)/COUNT(%s))*100;\nEND;\n",xyz,dataframe)
			Frequency1 <- hpcc.get.name()
			xyz<-paste(xyz,paste(Frequency1," := TABLE(",OutDSStr,",",FreqRecStr,",field,value,MERGE);"),"\n")
		} 
		
		
		if (is.not.na(num_new[1]))
		{
			
			varlst<-strsplit(num_new, ",")
			str1<-''
			str2<-''
			for (i in 1:length(num_new))
			{
				k<-strim(varlst[[i]][1])
				h<-strsplit(k," ")
				# 				if (i > 1)
				# 				{
				charh<-paste("'",h[[1]][1],"'",sep="")
				str1<-strim(paste(str1,charh,sep=","))
				hh<-strim(paste("LEFT.",h[[1]][1],sep=""))
				str2<-strim(paste(str2,hh,sep=","))
				
			}
			NumField <- hpcc.get.name()
			xyz<-sprintf("%s%s:=RECORD\n",xyz,NumField)
			xyz<-sprintf("%sSTRING field;\n",xyz)
			xyz<-sprintf("%sREAL value;\n",xyz)
			xyz<-sprintf("%sEND;\n",xyz)
			OutDSNum <- hpcc.get.name()
			xyz<-paste(xyz,paste(OutDSNum," := NORMALIZE(",dataframe,",",length(varlst),",TRANSFORM(",NumField,",SELF.field:=CHOOSE(COUNTER,",str1,");
								 SELF.value:=CHOOSE(COUNTER,",str2,")));",sep=""),"\n")
			FreqRecNum <- hpcc.get.name()
			xyz<-sprintf("%s%s:=RECORD\n",xyz,FreqRecNum)
			xyz<-sprintf("%s%s.field;\n",xyz,OutDSNum)
			xyz<-sprintf("%s%s.value;\n",xyz,OutDSNum)
			xyz<-sprintf("%sINTEGER frequency:=COUNT(GROUP);\n",xyz)
			xyz<-sprintf("%sREAL8 Percent:=(COUNT(GROUP)/COUNT(%s))*100;\nEND;\n",xyz,dataframe)
			Frequency2 <- hpcc.get.name()
			xyz<-paste(xyz,paste(Frequency2," := TABLE(",OutDSNum,",",FreqRecNum,",field,value,MERGE);"),"\n")  
		}
		
		
		
		if (is.not.null(sortorder))
		{
			if (sortorder=='ASC')
				d<-'+'
			else
				d<-'-'
			
			# 			for (i in 1:length(field_splt[[1]]))
			for (i in 1:1)
				
			{
				
				ind_var<-strim(field_splt[[1]][i])
				if(any(grepl(ind_var,char_new)))
				{
					freq=Frequency1
				}
				else
				{
					freq=Frequency2
				}
				
				dd<-grep(ind_var,split_val[[1]])
				vartype<-strim(split_val[[1]][dd])
				xyz<-paste(xyz,paste(out.dataframe,":=SORT(TABLE(",freq,"(field ='",ind_var,"'),{",vartype,                
									 "STRING value:=value;frequency;Percent}),'",d,ind_var,"');",sep=""),"\n")
			} 
		}
		else
		{
			# 			for (i in 1:length(field_splt[[1]]))
			for (i in 1:1)
				
			{
				ind_var<-strim(field_splt[[1]][i])
				if(any(grepl(ind_var,char_new)))
				{
					freq=Frequency1
				}
				else
				{
					freq=Frequency2
				}
				
				dd<-grep(ind_var,split_val[[1]])
				vartype<-strim(split_val[[1]][dd])
				xyz<-paste(xyz,paste(out.dataframe,":=SORT(TABLE(",freq,"(field ='",ind_var,"'),{",vartype,                
									 "STRING value:=value;frequency;Percent}),'",ind_var,"');",sep=""),"\n")
			}
		}
		hpcc.submit(xyz[1])
		return(out.dataframe)
	}
