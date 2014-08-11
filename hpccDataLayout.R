# Return the RECORD layout for a given logical file name. 
# Return empty String if the file does not exist or does not have a layout defined.
hpccData.layout<-function(logicalfilename){
  out.struct <- ""
  body <-paste('<?xml version="1.0" encoding="utf-8"?>\
               <soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"\
               xmlns:SOAP-ENC="http://schemas.xmlsoap.org/soap/encoding/"\
               xmlns="urn:hpccsystems:ws:wsdfu">\
               <soap:Body>\
               <DFUInfoRequest>\
               <Name>',logicalfilename,'</Name>\
               </DFUInfoRequest>\
               </soap:Body>\
               </soap:Envelope>\n')
  
  headerFields =
    c(Accept = "text/xml",
      Accept = "multipart/*",
      'Content-Type' = "text/xml; charset=utf-8",
      SOAPAction="urn:hpccsystems:ws:wsdfu")
  
  reader = basicTextGatherer()
  handle = getCurlHandle()
  
  fileout<-getwd()
  libFolderPaths<-.libPaths()
  
  # .libPaths() can have multiple values. Hence check for the proper folder where rHpcc package is deployed.
  
  hostpropertiesPath <- NULL
  config <- yaml.load_file("RConfig.yml")
  packageName <- config$package$name
  hostFileName <- config$hpcchost$filename
  if(length(libFolderPaths) > 0) {
    for(i in 1 : length(libFolderPaths)) {
      str1 <- paste(libFolderPaths[i], "/", packageName, "/", hostFileName, sep="");
      if(file.exists(str1)) {
        hostpropertiesPath <- str1
      }
    }
  }
  
  obj_hpccHost_prop <- yaml.load_file(hostpropertiesPath)
  hostName <- obj_hpccHost_prop$hpcc$host
  protocol <- obj_hpccHost_prop$hpcc$protocol
  port <- obj_hpccHost_prop$hpcc$port_thor
  dfu_service <- config$hpcchost$dfu_service
  
  
  url <- paste(protocol,'://',hostName,":",port,"/", dfu_service, sep="")
  curlPerform(url = url,
              httpheader = headerFields,
              postfields = body,
              writefunction = reader$update,
              curl =handle
  )
  
  status = getCurlInfo( handle )$response.code
  if(status >= 200 && status <= 300) {
    sResponse <- reader$value()
    responseXml <- xmlParse(sResponse)
    
    # For a CSV file Format = CSV. For THOR files Format field does not have any value.
    # For older versions of HPCCSystems, ECL field is blank for CSV files, but the newer version provides the layout.
    layout <- getNodeSet(responseXml, "//*[local-name()='Ecl']/text()", 
                         namespaces = xmlNamespaceDefinitions(responseXml,simplify = TRUE))
    if(length(layout) > 0) {
      colLayout <- layout[[1]]
      out.struct <- xmlToList(colLayout,addAttributes=TRUE)
    }
  }
  
  out.struct
}