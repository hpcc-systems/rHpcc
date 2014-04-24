hpccImport<-function(resulttype,funcname,parameterlist,language,func){
  
  strim<<-function (x) 
  {
    gsub("^\\s+|\\s+$", "", x)
    gsub("^,+|,+$", "", x)	
  }
  
  is.not.null <- function(x) ! is.null(x)
  if (missing(resulttype))
  {
    stop("no resulttype")	
  }
  else
  {   
    strt1<-strim(paste(resulttype,sep=","))
    strt2<-strim(paste(strt1,funcname,sep=" "))
    nnn<-strim(paste("(",parameterlist,")",sep=""))
    strt3<-strim(paste(strt2,nnn,sep=""))
    strt4<-strim(paste(",'",func,"'",sep=""))
    lang<-strim(paste(language,strt4,sep=","))		
    xyz <<- strim(paste(xyz,paste(strt3,":=IMPORT(",lang,sep="",");")))	
  }
}