hpccCorr<-function(dataframe,fields,method,out.dataframe){
  
  strim<<-function (x)
  {
    gsub("^\\s+|\\s+$", "", x)
    gsub("^,+|,+$", "", x)
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
  xyz<<-paste(xyz,paste("INTEGER3 id; "),"\n")
  xyz<<-paste(xyz,paste(dataframe,";"),"\n")
  xyz<<-paste(xyz,paste("END;"),"\n")
  xyz<<-paste(xyz,paste("RECMAX maxtrans (",dataframe,"  L, INTEGER C) := TRANSFORM"),"\n")
  xyz<<-paste(xyz,paste("SELF.id :=C;"),"\n")
  xyz<<-paste(xyz,paste("SELF :=L;"),"\n")
  xyz<<-paste(xyz,paste("END;"),"\n")
  xyz<<-paste(xyz,paste("DSRMAX:=PROJECT(",dataframe," ,maxtrans(LEFT,COUNTER));"),"\n")
  
  xyz<<-paste(xyz,paste("NumericField :=RECORD"),"\n")
  xyz<<-paste(xyz,paste("UNSIGNED id;"),"\n")
  xyz<<-paste(xyz,paste("UNSIGNED4 number;"),"\n")
  xyz<<-paste(xyz,paste("REAL8 value;"),"\n")
  xyz<<-paste(xyz,paste("STRING field;"),"\n")
  xyz<<-paste(xyz,paste("END;"),"\n")
  
  xyz<<-paste(xyz,paste("OutDs:=NORMALIZE(DSRMAX,",length(varlst[[1]]),",TRANSFORM(NumericField,SELF.id:=LEFT.id,SELF.number:=COUNTER;
                        SELF.field:=CHOOSE(COUNTER,",str1,");
                        SELF.value:=CHOOSE(COUNTER,",str2,")));","\n"))
  
  xyz<<-paste(xyz,paste("RankableField :=RECORD"),"\n")
  xyz<<-paste(xyz,paste("outDS;"),"\n")
  xyz<<-paste(xyz,paste("UNSIGNED Pos := 0;"),"\n")
  xyz<<-paste(xyz,paste("END;"),"\n")
  xyz<<-paste(xyz,paste("T :=TABLE(SORT(OutDS,Number,field,Value),RankableField);"),"\n")
  #xyz<<-paste(xyz,paste("T;"),"\n")
  
  xyz<<-paste(xyz,paste("TYPEOF(T) add_rank(T le,UNSIGNED c) := TRANSFORM"),"\n")
  xyz<<-paste(xyz,paste("SELF.Pos := c;"),"\n")
  xyz<<-paste(xyz,paste("SELF := le;"),"\n")
  xyz<<-paste(xyz,paste("END;"),"\n")
  xyz<<-paste(xyz,paste("P := PROJECT(T,add_rank(LEFT,COUNTER));"),"\n")
  
  xyz<<-paste(xyz,paste("RS := RECORD"),"\n")
  xyz<<-paste(xyz,paste("Seq := MIN(GROUP,P.pos);"),"\n")
  xyz<<-paste(xyz,paste("P.number;"),"\n")
  xyz<<-paste(xyz,paste("END;"),"\n")
  xyz<<-paste(xyz,paste("Splits := TABLE(P,RS,number,FEW);"),"\n")
  
  xyz<<-paste(xyz,paste("TYPEOF(T) to(P le,Splits ri) := TRANSFORM"),"\n")
  xyz<<-paste(xyz,paste("SELF.pos := 1+le.pos - ri.Seq;"),"\n")
  xyz<<-paste(xyz,paste("SELF := le;"),"\n")
  xyz<<-paste(xyz,paste("END;"),"\n")
  xyz<<-paste(xyz,paste("outfile := JOIN(P,Splits,LEFT.number=RIGHT.number,to(LEFT,RIGHT),LOOKUP);"),"\n")
  
  xyz<<-paste(xyz,paste("modeRec := RECORD"),"\n")
  xyz<<-paste(xyz,paste("outfile.number;"),"\n")
  xyz<<-paste(xyz,paste("outfile.value;"),"\n")
  xyz<<-paste(xyz,paste("outfile.pos;"),"\n")
  xyz<<-paste(xyz,paste("outfile.field;"),"\n")
  xyz<<-paste(xyz,paste("vals := COUNT(GROUP);"),"\n")
  xyz<<-paste(xyz,paste("END;"),"\n")
  xyz<<-paste(xyz,paste("MTable := TABLE(outfile,modeRec,number,field,value);"),"\n")
  #xyz<<-paste(xyz,paste("MTable;"),"\n")
  
  xyz<<-paste(xyz,paste("newRec := RECORD"),"\n")
  xyz<<-paste(xyz,paste("MTable.number;"),"\n")
  xyz<<-paste(xyz,paste("MTable.value;"),"\n")
  xyz<<-paste(xyz,paste("MTable.field;"),"\n")
  xyz<<-paste(xyz,paste("po := (MTable.pos*Mtable.vals + ((Mtable.vals-1)*Mtable.vals/2))/Mtable.vals;"),"\n")
  xyz<<-paste(xyz,paste("END;"),"\n")
  
  xyz<<-paste(xyz,paste("newTable := TABLE(MTable,newRec);"),"\n")
  #xyz<<-paste(xyz,paste("OUTPUT(newTable,NAMED('TEST'));"),"\n")
  xyz<<-paste(xyz,paste("TestTab := JOIN(outfile,newTable,LEFT.number = RIGHT.number AND LEFT.value = RIGHT.value);"),"\n")
  #xyz<<-paste(xyz,paste("TestTab;"),"\n")
  
  xyz<<-paste(xyz,paste("MyRec := RECORD"),"\n")
  xyz<<-paste(xyz,paste("TestTab;"),"\n")
  xyz<<-paste(xyz,paste("END;"),"\n")
  xyz<<-paste(xyz,paste("T1 := TABLE(TestTab,MyRec,id,number,field);"),"\n")
  #xyz<<-paste(xyz,paste("T1;"),"\n")
  
  if (method=='S')
  {
    xyz<<-paste(xyz,paste("SingleForm := Record"),"\n")
    xyz<<-paste(xyz,paste("T1.number;"),"\n")
    xyz<<-paste(xyz,paste("T1.field;"),"\n")
    xyz<<-paste(xyz,paste("REAL8 meanS := AVE(GROUP,T1.po);"),"\n")
    xyz<<-paste(xyz,paste("REAL8 sdS := SQRT(VARIANCE(GROUP,T1.po));"),"\n")
    xyz<<-paste(xyz,paste("END;"),"\n")
    
    xyz<<-paste(xyz,paste("single := TABLE(T1,SingleForm,number,field,FEW);"))
    
    xyz<<-paste(xyz,paste("PairRec := RECORD"),"\n")
    xyz<<-paste(xyz,paste("UNSIGNED4 left_number;"),"\n")
    xyz<<-paste(xyz,paste("UNSIGNED4 right_number;"),"\n")
    xyz<<-paste(xyz,paste("STRING left_field;"),"\n")
    xyz<<-paste(xyz,paste("STRING right_field;"),"\n")
    xyz<<-paste(xyz,paste("REAL8   xyS;"),"\n")
    xyz<<-paste(xyz,paste("END;"),"\n")
    
    xyz<<-paste(xyz,paste("PairRec note_prod(T1 L, T1 R) := TRANSFORM"),"\n")
    xyz<<-paste(xyz,paste("SELF.left_number := L.number;"),"\n")
    xyz<<-paste(xyz,paste("SELF.right_number := R.number;"),"\n")
    xyz<<-paste(xyz,paste("SELF.left_field := L.field;"),"\n")
    xyz<<-paste(xyz,paste("SELF.right_field := R.field;"),"\n")
    xyz<<-paste(xyz,paste("SELF.xyS := L.po*R.po;"),"\n")
    xyz<<-paste(xyz,paste("END;"),"\n")
    
    xyz<<-paste(xyz,paste("pairs := JOIN(T1,T1,LEFT.id=RIGHT.id AND LEFT.number<![CDATA[<]]>RIGHT.number,note_prod(LEFT,RIGHT));"),"\n")
    
    xyz<<-paste(xyz,paste("PairAccum := RECORD"),"\n")
    xyz<<-paste(xyz,paste("pairs.left_number;"),"\n")
    xyz<<-paste(xyz,paste("pairs.right_number;"),"\n")
    xyz<<-paste(xyz,paste("pairs.left_field;"),"\n")
    xyz<<-paste(xyz,paste("pairs.right_field;"),"\n")
    xyz<<-paste(xyz,paste("e_xyS := SUM(GROUP,pairs.xyS);"),"\n")
    xyz<<-paste(xyz,paste("END;"),"\n")
    
    xyz<<-paste(xyz,paste("exys := TABLE(pairs,PairAccum,left_number,right_number,left_field,right_field,FEW);"),"\n")
    xyz<<-paste(xyz,paste("with_x := JOIN(exys,single,LEFT.left_number = RIGHT.number,LOOKUP);"),"\n")
    
    
    xyz<<-paste(xyz,paste("Rec := RECORD"),"\n")
    xyz<<-paste(xyz,paste("UNSIGNED left_number;"),"\n")
    xyz<<-paste(xyz,paste("UNSIGNED right_number;"),"\n")
    xyz<<-paste(xyz,paste("STRING left_field;"),"\n")
    xyz<<-paste(xyz,paste("STRING right_field;"),"\n")
    xyz<<-paste(xyz,paste("REAL8 Spearman;"),"\n")
    xyz<<-paste(xyz,paste("END;"),"\n")
    xyz<<-paste(xyz,paste("n := COUNT(",dataframe,");"),"\n")
    
    xyz<<-paste(xyz,paste("Rec Trans(with_x L, single R) := TRANSFORM"),"\n")
    xyz<<-paste(xyz,paste("SELF.Spearman := (L.e_xyS - n*L.meanS*R.meanS)/(n*L.sdS*R.sdS);"),"\n")
    xyz<<-paste(xyz,paste("SELF := L;"),"\n")
    xyz<<-paste(xyz,paste("END;"),"\n")
    xyz<<-paste(xyz,paste("intjoin:= JOIN(with_x,single,LEFT.right_number=RIGHT.number,Trans(LEFT,RIGHT),LOOKUP);"),"\n")
    xyz<<-paste(xyz,paste(out.dataframe,":=SORT(TABLE(intjoin,{left_field,right_field,Spearman}),left_field,right_field);"),"\n")
    xyz<<-paste(xyz,paste("OUTPUT(CHOOSEN(",out.dataframe,",20),named('",out.dataframe,"'));",sep=""),"\n")  
  }
  else 
  {
    if (method=='P')
    {
      xyz<<-paste(xyz,paste("SingleForm := Record"),"\n")
      xyz<<-paste(xyz,paste("T1.number;"),"\n")
      xyz<<-paste(xyz,paste("T1.field;"),"\n")
      xyz<<-paste(xyz,paste("REAL8 meanP := AVE(GROUP,T1.value);"),"\n")
      xyz<<-paste(xyz,paste("REAL8 sdP := SQRT(VARIANCE(GROUP,T1.value));"),"\n")
      xyz<<-paste(xyz,paste("END;"),"\n")
      
      xyz<<-paste(xyz,paste("single := TABLE(T1,SingleForm,number,field,FEW);"))
      
      xyz<<-paste(xyz,paste("PairRec := RECORD"),"\n")
      xyz<<-paste(xyz,paste("UNSIGNED4 left_number;"),"\n")
      xyz<<-paste(xyz,paste("UNSIGNED4 right_number;"),"\n")
      xyz<<-paste(xyz,paste("STRING left_field;"),"\n")
      xyz<<-paste(xyz,paste("STRING right_field;"),"\n")
      xyz<<-paste(xyz,paste("REAL8   xyP;"),"\n")
      xyz<<-paste(xyz,paste("END;"),"\n")
      
      xyz<<-paste(xyz,paste("PairRec note_prod(T1 L, T1 R) := TRANSFORM"),"\n")
      xyz<<-paste(xyz,paste("SELF.left_number := L.number;"),"\n")
      xyz<<-paste(xyz,paste("SELF.right_number := R.number;"),"\n")
      xyz<<-paste(xyz,paste("SELF.left_field := L.field;"),"\n")
      xyz<<-paste(xyz,paste("SELF.right_field := R.field;"),"\n")
      xyz<<-paste(xyz,paste("SELF.xyP := L.value*R.value;"),"\n")
      xyz<<-paste(xyz,paste("END;"),"\n")
      
      xyz<<-paste(xyz,paste("pairs := JOIN(T1,T1,LEFT.id=RIGHT.id AND LEFT.number<![CDATA[<]]>RIGHT.number,note_prod(LEFT,RIGHT));"),"\n")
      
      xyz<<-paste(xyz,paste("PairAccum := RECORD"),"\n")
      xyz<<-paste(xyz,paste("pairs.left_number;"),"\n")
      xyz<<-paste(xyz,paste("pairs.right_number;"),"\n")
      xyz<<-paste(xyz,paste("pairs.left_field;"),"\n")
      xyz<<-paste(xyz,paste("pairs.right_field;"),"\n")
      xyz<<-paste(xyz,paste("e_xyP := SUM(GROUP,pairs.xyP);"),"\n")
      xyz<<-paste(xyz,paste("END;"),"\n")
      
      xyz<<-paste(xyz,paste("exys := TABLE(pairs,PairAccum,left_number,right_number,left_field,right_field,FEW);"),"\n")
      xyz<<-paste(xyz,paste("with_x := JOIN(exys,single,LEFT.left_number = RIGHT.number,LOOKUP);"),"\n")
      
      xyz<<-paste(xyz,paste("Rec := RECORD"),"\n")
      xyz<<-paste(xyz,paste("UNSIGNED left_number;"),"\n")
      xyz<<-paste(xyz,paste("UNSIGNED right_number;"),"\n")
      xyz<<-paste(xyz,paste("STRING left_field;"),"\n")
      xyz<<-paste(xyz,paste("STRING right_field;"),"\n")
      xyz<<-paste(xyz,paste("REAL8 Pearson;"),"\n")
      xyz<<-paste(xyz,paste("END;"),"\n")
      xyz<<-paste(xyz,paste("n := COUNT(",dataframe,");"),"\n")
      
      xyz<<-paste(xyz,paste("Rec Trans(with_x L, single R) := TRANSFORM"),"\n")
      xyz<<-paste(xyz,paste("SELF.Pearson := (L.e_xyP - n*L.meanP*R.meanP)/(n*L.sdP*R.sdP);"),"\n")
      xyz<<-paste(xyz,paste("SELF := L;"),"\n")
      xyz<<-paste(xyz,paste("END;"),"\n")
      xyz<<-paste(xyz,paste("intjoin:= JOIN(with_x,single,LEFT.right_number=RIGHT.number,Trans(LEFT,RIGHT),LOOKUP);"),"\n")
      xyz<<-paste(xyz,paste(out.dataframe,":=SORT(TABLE(intjoin,{left_field,right_field,Pearson}),left_field,right_field);"),"\n")
      xyz<<-paste(xyz,paste("OUTPUT(CHOOSEN(",out.dataframe,",20),named('",out.dataframe,"'));",sep=""),"\n")  
      
    }
    else
    {
      stop("no proper method")  
    }
  }
}