hpccProject<-function
(dataframe,calltransfunc,counter=NULL,out.dataframe,prefetch=NULL,lookahead=NULL,parallel=NULL,keyed=NULL,local=NULL){
  
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
    strt2<-strim(paste(calltransfunc,"(LEFT,",counter,sep=""))
    strt3<-strim(paste(strt1,strt2,")",sep=","))
    strt4<-strim(paste(strt3,prefetch,sep=","))
    strt5<-strim(paste(strt4,lookahead,sep=","))
    strt6<-strim(paste(strt5,parallel,sep=","))
    strt7<-strim(paste(strt6,keyed,sep=","))
    strl<-strim(paste(strt7,local,sep=","))
    xyz <- strim(paste(xyz,paste(out.dataframe,":=PROJECT(",strl,sep="",");")))	
    xyz <<-strim(paste(xyz,paste("OUTPUT(",out.dataframe,",named('",out.dataframe,"'));",sep="")))
  }		
}