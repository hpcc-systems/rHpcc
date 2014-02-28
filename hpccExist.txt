hpccExists<-function(dataframe=NULL,valuelist=NULL,keyed=NULL,return){
  
  strim<<-function (x) 
  {
    gsub("^\\s+|\\s+$", "", x)
    gsub("^,+|,+$", "", x)	
  }
   is.not.null <- function(x) ! is.null(x)
      
    strt1<-strim(paste(dataframe,sep=","))
    strt2<-strim(paste(strt1,valuelist,sep=""))
    strt3<-strim(paste("(",keyed,")",sep=""))
    if (is.not.null(valuelist))
    {
    strt4<-strim(paste(strt2))
    }
    else
    {
     if (is.not.null(keyed))
	{	
    	strt4<-strim(paste(strt2,strt3,sep=""))
	}
	else
	{
	strt4<-strim(paste(strt2))
	}
    }				
    xyz<-""			
    xyz <<- strim(paste(xyz,paste(return,":=EXISTS(",strt4,sep="",");")))	
  
}