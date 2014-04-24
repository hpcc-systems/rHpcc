hpccDenormalize<-function(dataframe,childdataframe,condition,group=NULL,transfunc=NULL,tfuncparameter,
local=NULL,nosort=NULL,out.dataframe){
  
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
    strt2<-strim(paste(strt1,childdataframe,sep=","))
    strt3<-strim(paste(strt2,condition,sep=","))
    
        if (is.not.null(group))
	    {
	     groupkey<-strim(paste(group,sep=""))
	    }
            else
	    {
	    groupkey<-strim(paste(group,sep=""))
            }    
        strt4<-strim(paste(strt3,groupkey,sep=","))
	

        if (is.not.null(transfunc))
	{
   	tfunc<-strim(paste(transfunc,"(",tfuncparameter,")",sep=""))
    	}
	else
	{
   	tfunc<-strim(paste(transfunc))
    	}
     strt5<-strim(paste(strt4,tfunc,sep=",")) 
     strt6<-strim(paste(strt5,local,sep=","))						
     strt7<-strim(paste(strt6,nosort,sep=","))
    xyz<-""				
    xyz <- strim(paste(xyz,paste(out.dataframe,":=DENORMALIZE(",strt7,sep="",");")))
    xyz<<-strim(paste(xyz,paste("OUTPUT(",out.dataframe,",named('",out.dataframe,"'));",sep="")))	
  }
}