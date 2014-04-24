hpccTable<-function
(dataframe,format,out.dataframe,expression=NULL,few=NULL,many=NULL,unsorted=NULL,local=NULL,keyed=NULL,merge=NULL){
  
  strim<-function (x) 
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
    if (is.not.null(expression))
    {
      xyz<-NULL
      strexp <- strim(paste(few,many,unsorted,local,keyed,merge,sep=" "))
      strexps <- strim(gsub(' {2,}',' ',strexp))
      str1 <- strim(gsub(' ',',',strexps))
      strexp<-strim(paste(expression))
      str2 <-strim(paste(strexp,str1,sep=","))
      strf<-strim(paste(dataframe,format,str2,sep=","))
      xyz <<- strim(paste(xyz,paste("outdist := TABLE(",strf,sep="",");")))
    }
    else
    {
      xyz<-NULL
      strexp <- strim(paste(few,many,unsorted,local,keyed,merge,sep=" "))
      strexps <- strim(gsub(' {2,}',' ',strexp))
      str1 <- strim(gsub(' ',',',strexps))
      strf<-strim(paste(dataframe,format,str1,sep=","))
      xyz <- strim(paste(xyz,paste(out.dataframe,":= TABLE(",strf,sep="",");")))
      xyz <<-sstrim(paste(xyz,paste("OUTPUT(",out.dataframe,",named('",out.dataframe,"'));",sep="")))
    }
  }
}