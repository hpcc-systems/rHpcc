hpcc.execute <-
  function (signal) {
    if(is.null(.uUrlHpccforEx))
      stop('Please start HPCC using the function - hpcc.begin()')
    eclCode <- ''
    for(i in seq(from = 1,to = length(.hpccImport))) {
      if(i!=1)
        eclCode <- sprintf("%s,",eclCode)
      else
        eclCode <- 'IMPORT '
      eclCode <- sprintf("%s %s",eclCode,.hpccImport[i])
    }
    eclCode <- sprintf("%s ;\n",eclCode)
    eclCode <- paste(eclCode,.eclQuery,sep=' ')
    .eclQuery <<- ""
    fileout <- getwd()
    str <- .libPaths()
    eclCode <- paste('<![CDATA[',eclCode,']]>')
    body <- ""
    body <-paste('<?xml version="1.0" encoding="utf-8"?>\
                 <soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"\
                 xmlns:SOAP-ENC="http://schemas.xmlsoap.org/soap/encoding/"\
                 xmlns="urn:hpccsystems:ws:ecldirect">\
                 <soap:Body>\
                 <RunEclRequest>\
                 <userName>r2hpccUser</userName>\n                 
                 <cluster>',.hpccClustername,'</cluster>\
                 <limitResults>0</limitResults>\
                 <eclText>',eclCode,'</eclText>\
                 <snapshot>test</snapshot>\n                 
                 </RunEclRequest>\
                 </soap:Body>\
                 </soap:Envelope>\n', sep="")
    eclCode <<- eclCode
    
    headerFields = c(Accept = "text/xml", Accept = "multipart/*", 
                     `Content-Type` = "text/xml; charset=utf-8", SOAPAction = "urn:hpccsystems:ws:ecldirect")
    reader = basicTextGatherer()
    
    handle = getCurlHandle()
    #uUrlforEx <- paste(hpccProtocol,'://',.hpccHostName,":",.hpccPort,"/",ecl_direct,"/RunEcl?ver_=1", sep="")
    
    #   	ur <- uUrlforEx
    #		url <- 'https://127.0.0.1:8010/'
    
    url<-.uUrlHpccforEx
    
    # 		opts <- list(
    # 			proxy         = "", 
    # 			proxyusername = "", 
    # 			proxypassword = "", 
    # 			proxyport     = ""
    # 		)
    
    curlPerform(url = url, 
                httpheader = headerFields, 
                ssl.verifypeer = FALSE,
                postfields = body, 
                writefunction = reader$update, 
                curl = handle)
    status = getCurlInfo(handle)$response.code
    varWu1 <- reader$value()
    newlst <- xmlParse(varWu1)
    layout <- getNodeSet(newlst, "//*[local-name()='results']/text()", 
                         namespaces = xmlNamespaceDefinitions(newlst, simplify = TRUE))
    
    colLayout <<- layout[[1]]
    
    layout1 <<- xmlToList(colLayout)
    .hpccData <<- .data.result(layout1)
    hpcc.showFilesToDownload()
    # 		if(length(.hpccData)==0)
    # 			return()		
    # 		x <- readline("Do you want to convert any of the objects into Big Data FF objects(Y/N) : ")
  }

.data.result <- function (xmlResult, downloadPath, format) {
	data
	if (missing(xmlResult)) {
		stop("Empty XML String.")
	}
	else {
		
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
				
				if(length(nodes)==0)
					return()
				
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
				names(data[[k]]) <- resultSetName
			}
		}
		
		data
	}
}
