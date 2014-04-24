hpccRollup<-function(dataframe,condition=NULL,calltransfunc,out.dataframe,execute=NULL,parallel=NULL,keyed=NULL,local=NULL){
  
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
    strt2<-strim(paste(strt1,condition,sep=","))
    nnn<-strim(paste(calltransfunc,"(LEFT,RIGHT)",sep=""))
    strt3<-strim(paste(strt2,nnn,sep=","))
    strt4<-strim(paste(strt3,parallel,sep=","))
    strt5<-strim(paste(strt4,keyed,sep=","))
    strl<-strim(paste(strt5,local,sep=","))
    xyz <- strim(paste(xyz,paste(out.dataframe,":=ROLLUP(",strl,sep="",");")))
    xyz<<-strim(paste(xyz,paste("OUTPUT(",out.dataframe,",named('",out.dataframe,"'));",sep="")))	
  }
}