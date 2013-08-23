# This function is used to trim leading or trailing whitespaces
trim <- function (x) {
  gsub("^\\s+|\\s+$", "", x)
}

# Executes the ECL code on the cluster specified and returns the XML response
eclDirectCall <- function(hostName, port, eclCode, clusterName) {
  urlString <- paste("http://",hostName,":",port,"/EclDirect/RunEcl?ver_=0",sep="");
  content <- postForm(urlString, eclText=enc2utf8(eclCode), cluster=enc2utf8(clusterName))
  content
}

# The ToUpperCase functions return the source string with all lower case characters converted to upper case.
# @param value: A string containing the data to change case
eclToUpperCase <- function(value) {
  expression <- paste("Std.Str.ToUpperCase('", value, "')", sep="")
}

# Parses the XML returned from eclDirectCall() and allows you to download the result in either CSV or XML format
# EXAMPLE R CODE: 
# xmlContent <- ecl$execute()
# data <- parseResults(xmlContent, "C:/Ouput", "csv")
parseResults <- function(xmlResult, downloadPath, format){
  data
  if(missing(xmlResult)) {
    stop("Empty XML String.")
  } else {
    docRoot = xmlRoot(xmlTreeParse(xmlResult))
    nodes = getNodeSet(docRoot, "//Dataset")
    for(i in 1:length(nodes)) {
      datasetNode <- nodes[[i]]
      resultSetName = xmlGetAttr(datasetNode, "name")
      x <- array(1:length(datasetNode)*length(datasetNode[[1]]), dim=c(length(datasetNode),length(datasetNode[[1]])))
      for(j in 1:length(datasetNode)) {
        rowNode <- datasetNode[[j]]
        for(k in 1:length(rowNode)) {
          actualNode <- rowNode[[k]]
          x[j,k] <- xmlValue(actualNode)
        }
      }
      
      # Download the file if "downloadPath" argument is specified
      if(!missing(downloadPath)) {
        if(missing(format)) {
          fileName <- paste(resultSetName,".csv", sep = "")
          path <- paste(downloadPath,"/",fileName, sep = "")
          write.table(x,file=path,sep=",",row.names=F, col.names = F)
        } else {
          fileName <- paste(resultSetName,".xml", sep = "")
          path <- paste(downloadPath,"/",fileName, sep = "")
          write.table(xmlResult,file=path,sep=",",row.names=F, col.names = F)
        }   
      }      
      data <- x
      data = as.data.frame(data, stringsAsFactors=FALSE)
    }
    
    data
  } 
}
