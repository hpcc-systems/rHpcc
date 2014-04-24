hpccNormalize<-function(dataframe,numexp,calltransfunc,out.dataframe){
  
  strim<<-function (x) 
  {
    gsub("^\\s+|\\s+$", "", x)
    gsub("^,+|,+$", "", x)	
  }
  
  is.not.null <- function(x) ! is.null(x)
  if (missing(dataframe))
  {
    stop("no dataframe")	
  }
  else
  {   
    strt1<-strim(paste(dataframe,sep=","))
    strt2<-strim(paste(strt1,numexp,sep=","))
    nnn<-strim(paste(calltransfunc,"(LEFT,COUNTER)",sep=""))
    strt3<-strim(paste(strt2,nnn,sep=","))
    xyz <- strim(paste(xyz,paste(out.dataframe,":=NORMALIZE(",strt3,sep="",");")))
    xyz<<-strim(paste(xyz,paste("OUTPUT(",out.dataframe,",named('",out.dataframe,"'));",sep="")))	
  }
}