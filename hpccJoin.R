hpccJoin<-function(leftdataframe,rightdataframe,joincondition,transfunc=NULL,tfuncparameter,groupfield=NULL,
mergetfunc=NULL,groupkey=NULL,local=NULL,jointype=NULL,joinflags=NULL,few=NULL,many=NULL,out.dataframe){
  
  strim<<-function (x) 
  {
    gsub("^\\s+|\\s+$", "", x)
    gsub("^,+|,+$", "", x)	
  }
  
  is.not.null <- function(x) ! is.null(x)
  if (missing(leftdataframe))
  {
    stop("no leftdataframe")	
  }
  else
  {   
    
    strt1<-strim(paste(leftdataframe,sep=","))
    strt2<-strim(paste(strt1,rightdataframe,sep=","))
    strt3<-strim(paste(strt2,joincondition,sep=","))
    if (is.not.null(transfunc))
	{
   	tfunc<-strim(paste(transfunc,"(",tfuncparameter,")",sep=""))
    	}
	else
	{
   	tfunc<-strim(paste(transfunc))
    	}
    strt4<-strim(paste(strt3,tfunc,sep=","))
    strt5<-strim(paste(strt4,jointype,sep=","))
    
    if (is.not.null(groupfield))
	    {
	     groupkey<-strim(paste("LEFT.",groupfield,sep=""))
	    }
            else
	    {
	    groupkey<-strim(paste(groupfield,sep=""))
            }
     strt6<-strim(paste(strt5,mergetfunc,sep=","))
     strtext<-strim(paste(strt6,groupkey,sep=","))
     strt7<-strim(paste(strtext,joinflags,sep=","))
     strt8<-strim(paste(strt7,jointype,sep=","))	
     strt9<-strim(paste(strt8,local,sep=","))						
     strt10<-strim(paste(strt9,few,sep=","))
     strt11<-strim(paste(strt10,many,sep=","))
    xyz<-""				
    xyz <- strim(paste(xyz,paste(out.dataframe,":=JOIN(",strt11,sep="",");")))
    xyz<<-strim(paste(xyz,paste("OUTPUT(",out.dataframe,",named('",out.dataframe,"'));",sep="")))	
  }
}