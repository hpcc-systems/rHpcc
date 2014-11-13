hpcc.data.layout <- function(logicalfilename) {
	#		uUrl <- .hpcc.get.url()
	out.struct <- ""
	body <- paste('<?xml version="1.0" encoding="utf-8"?>
				  <soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"
				  xmlns:SOAP-ENC="http://schemas.xmlsoap.org/soap/encoding/"
				  xmlns="urn:hpccsystems:ws:wsdfu">
				  <soap:Body>
				  <DFUInfoRequest>
				  <Name>',logicalfilename,'</Name>
				  </DFUInfoRequest>
				  </soap:Body>
				  </soap:Envelope>')
	
	headerFields =
		c(Accept = "text/xml",
		  Accept = "multipart/*",
		  'Content-Type' = "text/xml; charset=utf-8",
		  SOAPAction="urn:hpccsystems:ws:wsdfu")
	
	reader = basicTextGatherer()
	
	handle = getCurlHandle()
#	url <- 'https://216.19.105.2:18010/EclDirect'
	url <- .uUrlHpcc
	# 	proxy<-readline(prompt="Do you use a proxy connection(Y/N) : ")
	# 	if(proxy=='Y'||proxy=='y') {}
# 	opts <- list(
# 		proxy         = "", 
# 		proxyusername = "", 
# 		proxypassword = "", 
# 		proxyport     = ""
# 	)
	
	curlPerform(url = url,
				httpheader = headerFields,
				ssl.verifypeer = FALSE,
				postfields = body,
				writefunction = reader$update,
				curl = handle)
	
	status = getCurlInfo(handle)$response.code
	if(status >= 200 && status <= 300) {
		sResponse <- reader$value()
		responseXml <- xmlParse(sResponse)
		
		layout <- getNodeSet(responseXml, "//*[local-name()='Ecl']/text()", 
							 namespaces = xmlNamespaceDefinitions(responseXml,simplify = TRUE))
		if(length(layout) > 0) {
			colLayout <- layout[[1]]
			out.struct <- xmlToList(colLayout,addAttributes=TRUE)
		}
	}
	
	return(out.struct)
}
