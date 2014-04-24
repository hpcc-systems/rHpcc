hpccSort<-function
(dataframe,fields,out.dataframe,joined=NULL,skew=NULL,threshold=NULL,few=NULL,joinedset=NULL,limit=NULL,target=NULL,size=NULL,local=NULL,stable=NULL,
 unstable=NULL,algorithm=NULL){
  
  strim<<-function (x) 
  {
    gsub("^\\s+|\\s+$", "", x)
    gsub("^,+|,+$", "", x)  
  }
  
  is.not.null <- function(x) ! is.null(x)
  if (missing(dataframe)) {
    stop("no dataframe.")	
  }
  else
  {
    if (is.not.null(joined))
    {	 	
      joinstr<-strim(paste(joined,"(",joinedset,")",sep=""))
    }
    else
    {
      joinstr<-strim(paste(joined,sep=" "))	
    }
    if (is.not.null(skew))
    {
      limt<-strim(paste(limit,target,sep=","))
      skewstr<-strim(paste(skew,"(",limt,")",sep=""))
    }
    else
    { 	 	  			  		
      skewstr<-strim(paste(skew,sep=" "))
    }
    if(is.not.null(threshold))
    {
      threstr<-strim(paste(threshold,"(",size,")",sep=""))
    }
    else
    {
      threstr<-strim(paste(threshold,sep=" "))		
    }
    if(is.not.null(stable))
    {
      stabstr<-strim(paste(stable,"(",algorithm,")",sep=""))
    }
    else
    {
      stabstr<-strim(paste(stable,sep=" "))	
    }
    if(is.not.null(unstable))	
    {
      unstabstr<-strim(paste(unstable,"(",algorithm,")",sep=""))	 	
    }
    else
    {
      unstabstr<-strim(paste(unstable,sep=" "))					
    } 
    str1<-strim(paste(dataframe,fields,joinstr,sep=","))	
    str2<-strim(paste(str1,skewstr,sep=","))
    str3<-strim(paste(str2,threstr,sep=","))
    str4<-strim(paste(str3,stabstr,sep=","))
    str5<-strim(paste(str4,unstabstr,sep=","))
    str6<-strim(paste(str5,local,sep=","))
    str7<-strim(paste(str6,few,sep=","))				
    xyz <- strim(paste(xyz,paste(out.dataframe,":= SORT(",str7,sep="",");")))
    xyz <<-strim(paste(xyz,paste("OUTPUT(",out.dataframe,",named('",out.dataframe,"'));",sep="")))
  } 
}