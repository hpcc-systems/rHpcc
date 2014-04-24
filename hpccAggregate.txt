hpccAggregate<-function(dataframe,layoutstruct,maintransfunc,mergetransfunc=NULL,groupfield=NULL,local=NULL,few=NULL,many=NULL,out.dataframe){
  
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
    strt2<-strim(paste(strt1,layoutstruct,sep=","))
    maintfunc<-strim(paste(maintransfunc,"(LEFT,RIGHT)",sep=""))
    strt3<-strim(paste(strt2,maintfunc,sep=","))
     if (is.not.null(mergetransfunc))
	{ 
	mergetfunc<-strim(paste(mergetransfunc,"(RIGHT1,RIGHT2)",sep=""))
	}
	else
	{
	mergetfunc<-strim(paste(mergetransfunc,sep=""))
	}
	if (is.not.null(groupfield))
	    {
	     groupkey<-strim(paste("LEFT.",groupfield,sep=""))
	    }
            else
	    {
	    groupkey<-strim(paste(groupfield,sep=""))
            }
     strt4<-strim(paste(strt3,mergetfunc,sep=","))
     strtext<-strim(paste(strt4,groupkey,sep=","))
     strt5<-strim(paste(strtext,local,sep=","))						
     strt6<-strim(paste(strt5,few,sep=","))
     strt7<-strim(paste(strt6,many,sep=","))
    xyz<-""				
    xyz <- strim(paste(xyz,paste(out.dataframe,":=AGGREGATE(",strt7,sep="",");")))
    xyz<<-strim(paste(xyz,paste("OUTPUT(",out.dataframe,",named('",out.dataframe,"'));",sep="")))	
  }
}