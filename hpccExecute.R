Execute.Hpcc<-function(signal){
  if (signal=='y' | signal=='Y')
  {
    xyz2<<-xyz
    xyz<<-""
    fileout<-getwd()
    str<-.libPaths()
    str1<-paste(str,"/REDA/hostsetting.txt",sep="")
    tt<-read.table(str1,sep="\t")
    f1<<-as.character(tt$V1[[1]])
    f2<<-as.character(tt$V1[[2]])
    eclCode<<-xyz2
    hostname<<-f1
    port<<-f2
    
    body<-""
    body <-paste('<?xml version="1.0" encoding="utf-8"?>\
                 <soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"\
                 xmlns:SOAP-ENC="http://schemas.xmlsoap.org/soap/encoding/"\
                 xmlns="urn:hpccsystems:ws:ecldirect">\
                 <soap:Body>\
                 <RunEclRequest>\
                 <userName>xyz</userName>\
                 <cluster>thor</cluster>\
                 <limitResults>0</limitResults>\
                 <eclText>',eclCode,'</eclText>\
                 <snapshot>test</snapshot>\
                 </RunEclRequest>\
                 </soap:Body>\
                 </soap:Envelope>\n')
    
    headerFields =
      c(Accept = "text/xml",
        Accept = "multipart/*",
        'Content-Type' = "text/xml; charset=utf-8",
        SOAPAction="urn:hpccsystems:ws:ecldirect")
    
    reader = basicTextGatherer()
    handle = getCurlHandle()
    
    ur<-paste("http://",hostname,":",port,"/EclDirect/RunEcl?ver_=1",sep="")
    curlPerform(url = ur,
                httpheader = headerFields,
                postfields = body,
                writefunction = reader$update,
                curl =handle
    )
    status = getCurlInfo( handle )$response.code
    varWu1 <- reader$value()
    newlst<-xmlParse(varWu1)
    layout <- getNodeSet(newlst, "//*[local-name()='results']/text()", namespaces = xmlNamespaceDefinitions(newlst,simplify =
                                                                                                               TRUE))
    colLayout <- layout[[1]]
    layout1<- xmlToList(colLayout)
    contentcsv<<-layout1
    data <- dataresult(contentcsv, downloadPath=fileout)
  }
  
}




# Returns a list of Dataframes for each <Dataset> element of XML.
dataresult<-function (xmlResult, downloadPath) {
  
  # Create an empty list which stores the parsed dataframes.
  
  data <- list() 
  if (missing(xmlResult)) {
    stop("Empty XML String.")
  } 
  else {
    top <- xmlRoot(xmlTreeParse(xmlResult, useInternalNodes = TRUE))
    
    # Get all the nodes "Dataset" nodes
    nodes <- getNodeSet(top, "//Dataset")
    
    for(k in 1:length(nodes)) {  
      
      # Iterate through each element of the get the value.
      plantcat <- xmlSApply(nodes[[k]], function(x) xmlSApply(x, xmlValue))
      #print(class(plantcat))
      
      # Convert to Data Frame
      df <- as.data.frame(plantcat)
      #print(df)
      
      # Transpose of Dataframe. Not needed but just for better presentation
      dfTransposed <- data.frame(t(df),row.names=NULL)
      #print(class(dfTransposed))
      
      #getting the output dataframe in R window
      nodes = getNodeSet(top, "//Dataset")
      datasetNode <- nodes[[k]]
      resultSetName <- xmlGetAttr(datasetNode, "name")
      assign(resultSetName, dfTransposed,envir = .GlobalEnv)
      newdata = data.frame(dfTransposed, stringsAsFactors = FALSE)
      
      # Download the file if "downloadPath" argument is specified
      if(!missing(downloadPath)) {
        # Get the Dataset Attribute name for saving the file
        datasetNode <- nodes[[k]]
        resultSetName <- xmlGetAttr(datasetNode, "name")
        fileName <- paste(resultSetName,".csv", sep = "")
        path <- paste(downloadPath,"/",fileName, sep = "")
        write.table(dfTransposed,file=path,sep=",",row.names=F, col.names = T)
      }
      
      data[[k]] <- list(dfTransposed)
    }  
  }
  
  data
}