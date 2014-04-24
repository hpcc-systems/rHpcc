hpccTransform<-function(datastruct=NULL,funcname,parameterlist,skip=NULL,condition=NULL,locals=NULL){
  
  strim<<-function (x) 
  {
    gsub("^\\s+|\\s+$", "", x)
    gsub("^,+|,+$", "", x)	
  }
  
  is.not.null <- function(x) ! is.null(x)
  if (missing(funcname))
  {
    stop("no funcname")	
  }
  else
  {
    if (is.null(datastruct)) 	
    {
      stru<-strim(paste("MyRec",skip))	  	    	
    }
    else
    {
      stru<-strim(paste(datastruct))		
    }	
    
    if (is.not.null(skip)) 	
    {
      strn1<-strim(paste(":= TRANSFORM,",skip))	  	    	
    }
    else
    {
      strn1<-strim(paste(":= TRANSFORM",skip))		
    }
    
    str1<-strim(paste(stru,funcname,sep=" "))
    str2<-strim(paste(str1,"(",parameterlist,")",sep=""))
    strn2<-strim(paste(strn1,condition,sep=" "))
    xyz <<- strim(paste(xyz,paste(str2,strn2,sep=" ",";END;")))					
    
  }
}

