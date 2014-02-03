hpccRead.InlineData<-function(layoutname,linedata,fieldtype,out.dataframe){
  strim<<-function (x) 
  {
    gsub("^\\s+|\\s+$", "", x)
    gsub("^,+|,+$", "", x)  
  }
  is.not.null <- function(x) ! is.null(x)
  if(missing(linedata))
  {
    stop("no linedata")	 
  }
  else
  {
    xyz<<-""
    strd<-strim(paste(layoutname,":=RECORD",fieldtype,sep=" "))
    xyz<-strim(paste(strd,"END;",sep=""))		
    str1<-strim(paste(out.dataframe,":=DATASET([",sep=""))
    str2<-strim(paste(linedata))
    str3<-strim(paste(str1,str2,"],",layoutname,");",sep=""))
    str4<-strim(paste(str3,"data_creation",sep=","))	    	
    xyz<-strim(paste(xyz,str3))
    xyz<<-strim(paste(xyz,paste("OUTPUT(",out.dataframe,",named('",out.dataframe,"'));",sep="")))
  }				      			       						
}
