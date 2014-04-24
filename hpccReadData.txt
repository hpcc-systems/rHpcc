hpccRead.Data<-function(logicalfilename,layoutname,in.struct,filetype,out.dataframe){
  strim<<-function (x){
    gsub("^\\s+|\\s+$", "", x)
    gsub("^,+|,+$", "", x)
  }
  is.not.null <- function(x) ! is.null(x)
  if(missing(logicalfilename))
  {
    stop("no logicalfinlename")
  }
  else
  {
    xyz<<-""
    strd<-strim(paste(layoutname,":=",in.struct,sep=" "))
    xyz<-strim(paste(strd,sep=""))
    str1<-strim(paste(logicalfilename,"'",sep=""))
    str2<-strim(paste(str1,",",layoutname,",",filetype,");",sep=""))
    xyz<-strim(paste(xyz,paste(out.dataframe,":=DATASET('~",str2,sep="")))
    xyz<<-strim(paste(xyz,paste("OUTPUT(CHOOSEN(",out.dataframe,",20),named('",out.dataframe,"'));",sep="")))
    
  }
}