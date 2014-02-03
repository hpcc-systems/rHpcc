hpccIterate<-function(dataframe,calltransfunc,out.dataframe,local=NULL){
  
  strim<<-function (x) 
  {
    gsub("^\\s+|\\s+$", "", x)
    gsub("^,+|,+$", "", x)	
  }
  
  is.not.null <- function(x) ! is.null(x)
  if (missing(dataframe))
  {
    stop("no dataframe.")	
  }
  else
  {	
    strt1<-strim(paste(dataframe,sep=""))
    strt2<-strim(paste(calltransfunc,"(LEFT,RIGHT)",sep=""))
    strt3<-strim(paste(strt1,strt2,sep=","))
    strl<-strim(paste(strt3,local,sep=","))
    xyz <- strim(paste(xyz,paste(out.dataframe,":=ITERATE(",strl,sep="",");")))
    xyz <<-strim(paste(xyz,paste("OUTPUT(",out.dataframe,",named('",out.dataframe,"'));",sep="")))	
  }		
}