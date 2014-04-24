hpccSequential<-function(actionlist=NULL,parallel=NULL){
  
  strim<<-function (x) 
  {
    gsub("^\\s+|\\s+$", "", x)
    gsub("^,+|,+$", "", x)	
  }
  
  is.not.null <- function(x) ! is.null(x)
    { 	  
    strt1<-strim(paste(actionlist,sep=","))
    if (is.not.null(parallel))
	{	
    	strt2<-strim(paste("PARALLEL(",parallel,")",sep=""))
	}
    else
	{
	strt2<-""
	}	  	
    strt3<-strim(paste(strt1,strt2,sep=","))
    xyz<-""
    xyz <<- strim(paste(xyz,paste("SEQUENTIAL(",strt3,sep="",");")))	
  }s
}





