hpccSd<-function(dataframe,fields,out.dataframe){
  
  strim<-function (dataframe)
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
  xyz<<-paste(xyz,paste("recsd :=RECORD"),"\n")
  xyz<<-paste(xyz,paste("INTEGER3 id;"),"\n")
  xyz<<-paste(xyz,paste(dataframe,";"),"\n")
  xyz<<-paste(xyz,paste("END;"),"\n")
  xyz<<-paste(xyz,paste("recsd sdtrans (",dataframe," L, INTEGER C) := TRANSFORM"),"\n")
  xyz<<-paste(xyz,paste("SELF.id :=C;"),"\n")
  xyz<<-paste(xyz,paste("SELF :=L;"),"\n")
  xyz<<-paste(xyz,paste("END;"),"\n")
  xyz<<-paste(xyz,paste("DSRSD:=PROJECT(",dataframe,",sdtrans(LEFT,COUNTER));"),"\n")
  #xyz<<-paste(xyz,paste("DSRSD;"),"\n")
  xyz<<-paste(xyz,paste("NumSdField:=RECORD"),"\n")
  xyz<<-paste(xyz,paste("UNSIGNED id;"),"\n")
  xyz<<-paste(xyz,paste("STRING field;"),"\n")
  xyz<<-paste(xyz,paste("REAL8 value;"),"\n")
  xyz<<-paste(xyz,paste("END;"),"\n")
  xyz<<-paste(xyz,paste("OutDsSd:=NORMALIZE(DSRSD,",length(varlst[[1]]),",TRANSFORM(NumSdField,SELF.id:=LEFT.id,SELF.field:=CHOOSE
                        (COUNTER,",str1,sep="",");SELF.value:=CHOOSE(COUNTER,",str2,")));","\n"))
  #xyz<<-paste(xyz,paste("OutDsSd;"),"\n")
  xyz<<-paste(xyz,paste("SingleSdField := RECORD"),"\n")
  xyz<<-paste(xyz,paste("OutDsSd.field;"),"\n")
  xyz<<-paste(xyz,paste("Sd := SQRT(VARIANCE(GROUP,OutDsSd.value));"),"\n")
  xyz<<-paste(xyz,paste("END;"),"\n")
  xyz<<-paste(xyz,paste(out.dataframe,":= TABLE(OutDsSd,SingleSdField,field);"),"\n")
  xyz <<-strim(paste(xyz,paste("OUTPUT(CHOOSEN(",out.dataframe,",20),named('",out.dataframe,"'));",sep="")))
}

