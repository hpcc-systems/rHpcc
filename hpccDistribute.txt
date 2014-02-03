hpccDistribute<-function
(dataframe,out.dataframe,form=NULL,expression=NULL,index=NULL,skew=NULL,sorts=NULL,joincondition=NULL,maxskew=NULL,skewlimit=NULL)
{
  strim<-function (x) 
  {
    gsub("^\\s+|\\s+$", "", x)
  }   
  is.not.null <- function(x) ! is.null(x)
  if (missing(dataframe)) {
    stop("no dataframe.")	
  }
  else
  {
    if (is.null(form))
    {
      xyz<-NULL
      strexp <- strim(paste(dataframe,sep=" "))
      xyz <<- strim(paste(xyz,paste("outdist := DISTRIBUTE(",strexp,sep="",");")))
    }
    else
    {
      if (form=='expression')
      {
        if (is.not.null(sorts))
        {
          xyz<-NULL
          strex<-strim(paste("MERGE(",sorts,")",sep=""))
          strexp <- strim(paste(dataframe,expression,strex,sep=","))
          xyz <<- strim(paste(xyz,paste("outdist := DISTRIBUTE(",strexp,sep="",");")))
        }
        else
        {
          xyz<-NULL
          strexp <- strim(paste(dataframe,expression,sep=","))
          xyz <<- strim(paste(xyz,paste("outdist := DISTRIBUTE(",strexp,sep="",");")))
        }
      }
      else
      {
        if (form=='index')
        {
          if (is.not.null(joincondition)) 
          {
            xyz<-NULL	
            strexp <- strim(paste(dataframe,index,joincondition,sep=","))
            xyz <<- strim(paste(xyz,paste("outdist := DISTRIBUTE(",strexp,sep="",");")))
          }
          else
          {
            xyz<-NULL	
            strexp <- strim(paste(dataframe,index,sep=","))
            xyz <<- strim(paste(xyz,paste("outdist := DISTRIBUTE(",strexp,sep="",");")))
          }
        } 		
        else
        {
          if (form=='skew')
          {
            xyz<-NULL
            stre<-strim(paste(maxskew,skewlimit,sep=" "))
            stre1<-strim(gsub(' {2,}',' ',stre))
            stre2<-strim(gsub(' ',',',stre1))
            strex<-strim(paste("SKEW(",stre2,")",sep=""))
            strexp <- strim(paste(dataframe,strex,sep=","))
            xyz <- strim(paste(xyz,paste(out.dataframe,":= DISTRIBUTE(",strexp,sep="",");")))
            xyz <<-strim(paste(xyz,paste("OUTPUT(",out.dataframe,",named('",out.dataframe,"'));",sep="")))
          }
        }
      }	
    }
  }
}