hpccDedup<-function(dataframe,out.dataframe,condition=NULL,all=NULL,hash=NULL,keep=NULL,keeper=NULL,local=NULL){
  
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
    if ((is.not.null(all)) & (is.not.null(hash)) & (is.not.null(keep)))
    {
      stop("KEEP is not supported for DEDUP ALL")		 
    }
    else
    {
      if  ((is.not.null(hash)) & (is.not.null(keep)))
      {
        stop("KEEP is not supported for DEDUP ALL")	
      } 
      else
      {
        if  ((is.not.null(all)) & (is.not.null(keep)))			
        {
          stop("KEEP is not supported for DEDUP ALL")		
        }	
        else	
        {
          if((is.null(condition)) & (is.null(all)))
          {
            xyz<-NULL
            strall <- strim(paste(dataframe,condition,"ALL",hash,keeper,local,sep=" "))
            strall1 <- strim(gsub(' {2,}',' ',strall))
            strall2 <- strim(gsub(' ',',',strall1))
            
            strnall <- strim(paste(dataframe,condition,hash,keeper,local,sep=" "))
            strnall1 <- strim(gsub(' {2,}',' ',strnall))
            strnall2 <- strim(gsub(' ',',',strnall1))
            if(is.null(keep))
            {
              strall3 <-strall2
              xyz <<- strim(paste(xyz,paste("outdup := DEDUP(",strall3,sep="",");")))	
            }
            else
            {
              strnall3 <-paste(strnall2,keep,sep=",")
              xyz <<- strim(paste(xyz,paste("outdup := DEDUP(",strnall3,sep="",");")))
            }
          }
          else
          {   			 
            xyz<-NULL
            strall <- strim(paste(dataframe,condition,all,hash,keeper,local,sep=" "))
            strall1 <- strim(gsub(' {2,}',' ',strall))
            strall2 <- strim(gsub(' ',',',strall1))
            
            strnall <- strim(paste(dataframe,condition,hash,keeper,local,sep=" "))
            strnall1 <- strim(gsub(' {2,}',' ',strnall))
            strnall2 <- strim(gsub(' ',',',strnall1))
            
            if(is.null(keep))
            {
              strall3 <-strall2
              xyz <<- strim(paste(xyz,paste("outdup := DEDUP(",strall3,sep="",");")))	
            }
            else
            {
              strnall3 <-paste(strnall2,keep,sep=",")
              xyz <- strim(paste(xyz,paste(out.dataframe,":= DEDUP(",strnall3,sep="",");")))
              xyz <<-strim(paste(xyz,paste("OUTPUT(",out.dataframe,",named('",out.dataframe,"'));",sep="")))
            }	   		
          }
        }
      }
    }
  }
}