hpccMedian<-function(dataframe,fields,out.dataframe){
  
  trim<-function (dataframe)
  {
    gsub("^\\s+|\\s+$", "", dataframe)
  }
  
  varlst<<-strsplit(fields, ",")
  str1<-NULL
  str2<-NULL
  for (i in 1:length(varlst[[1]]))
  {
    k<-strim(varlst[[1]][i])
    h<<-strsplit(k," ")
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
  
  xyz<<-paste(xyz,paste("recmax :=RECORD"),"\n")
  xyz<<-paste(xyz,paste("INTEGER3 id;"),"\n")
  xyz<<-paste(xyz,paste(dataframe,";"),"\n")
  xyz<<-paste(xyz,paste("END;"),"\n")
  xyz<<-paste(xyz,paste("RECMAX maxtrans (",dataframe," L, INTEGER C) := TRANSFORM"),"\n")
  xyz<<-paste(xyz,paste("SELF.id :=C;"),"\n")
  xyz<<-paste(xyz,paste("SELF :=L;"),"\n")
  xyz<<-paste(xyz,paste("END;"),"\n")
  
  xyz<<-paste(xyz,paste("DSRMAX:=PROJECT(",dataframe,",maxtrans(LEFT,COUNTER));"),"\n")
  #xyz<<-paste(xyz,paste("DSRMAX;"),"\n")
  
  xyz<<-paste(xyz,paste("MaxField:=RECORD"),"\n")
  xyz<<-paste(xyz,paste("UNSIGNED id;"),"\n")
  xyz<<-paste(xyz,paste("UNSIGNED4 number;"),"\n")
  xyz<<-paste(xyz,paste("STRING Field;"),"\n")
  xyz<<-paste(xyz,paste("REAL8 value;"),"\n")
  xyz<<-paste(xyz,paste("END;"),"\n")
  xyz<<-paste(xyz,paste("OutDsMed:=NORMALIZE(DSRMAX,",length(varlst[[1]]),",TRANSFORM(MaxField,SELF.id:=LEFT.id,SELF.number:=COUNTER;SELF.Field:=CHOOSE
                        (COUNTER,",str1,sep="",");SELF.value:=CHOOSE(COUNTER,",str2,")));","\n"))
  
  xyz<<-paste(xyz,paste("RankableField := RECORD"),"\n")
  xyz<<-paste(xyz,paste("OutDsMed;"),"\n")
  xyz<<-paste(xyz,paste("UNSIGNED Pos := 0;"),"\n")
  xyz<<-paste(xyz,paste("END;"),"\n")
  
  xyz<<-paste(xyz,paste("T := TABLE(SORT(OutDsMed,Number,field,Value),RankableField);"),"\n")
  
  xyz<<-paste(xyz,paste("TYPEOF(T) add_rank(T le,UNSIGNED c) := TRANSFORM"),"\n")
  xyz<<-paste(xyz,paste("SELF.Pos := c;"),"\n")
  xyz<<-paste(xyz,paste("SELF := le;"),"\n")
  xyz<<-paste(xyz,paste("END;"),"\n")
  
  xyz<<-paste(xyz,paste("P := PROJECT(T,add_rank(LEFT,COUNTER));"),"\n")  
  xyz<<-paste(xyz,paste("RS := RECORD"),"\n")
  xyz<<-paste(xyz,paste("Seq := MIN(GROUP,P.pos);"),"\n")
  xyz<<-paste(xyz,paste("P.number;"),"\n")
  xyz<<-paste(xyz,paste("P.field;"),"\n")
  xyz<<-paste(xyz,paste("END;"),"\n")
  
  xyz<<-paste(xyz,paste("Splits := TABLE(P,RS,number,field,FEW);"),"\n")
  xyz<<-paste(xyz,paste("TYPEOF(T) to(P le,Splits ri) := TRANSFORM"),"\n")
  xyz<<-paste(xyz,paste("SELF.pos := 1+le.pos - ri.Seq;"),"\n")
  xyz<<-paste(xyz,paste("SELF := le;"),"\n")
  xyz<<-paste(xyz,paste("END;"),"\n")
  
  xyz<<-paste(xyz,paste("outfile := JOIN(P,Splits,LEFT.number=RIGHT.number,to(LEFT,RIGHT),LOOKUP);"),"\n")
  xyz<<-paste(xyz,paste("n := COUNT(DSRMAX);"),"\n")
  
  xyz<<-paste(xyz,paste("MedRec := RECORD"),"\n")
  xyz<<-paste(xyz,paste("outfile.number;"),"\n")
  xyz<<-paste(xyz,paste("SET OF UNSIGNED poso := IF(n%2=0,[n/2,n/2 + 1],[(n+1)/2]);"),"\n")
  xyz<<-paste(xyz,paste("END;"),"\n")
  
  xyz<<-paste(xyz,paste("MyT := TABLE(outfile,MedRec,field,number);"),"\n")
  xyz<<-paste(xyz,paste("MedianValues:=JOIN(outfile,MyT,LEFT.number=RIGHT.number AND LEFT.pos IN RIGHT.poso);"),"\n")
  xyz<<-paste(xyz,paste("medianRec := RECORD"),"\n")
  xyz<<-paste(xyz,paste("MedianValues.field;"),"\n")
  xyz<<-paste(xyz,paste("Median := AVE(GROUP, MedianValues.value);"),"\n")
  xyz<<-paste(xyz,paste("END;"),"\n")
  
  xyz<<-paste(xyz,paste(out.dataframe,":= TABLE(MedianValues,medianRec,field);"),"\n")
  xyz <<-strim(paste(xyz,paste("OUTPUT(CHOOSEN(",out.dataframe,",20),named('",out.dataframe,"'));",sep="")))
}