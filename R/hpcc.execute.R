hpcc.execute <-
	function () {
		if(is.null(uUrl))
			stop('Please start HPCC using the function - hpcc.begin()')
		import <- ''
		for(i in 1:length(hpccImport)) {
			if(i>0) {
				import <- paste(as.character(one), sep="' '", collapse=", ")
				import <- sprintf("IMPORT %s;\n",import)
			}
		}
		eclQuery <<- sprintf("%s%s",import,eclQuery)
		eclCode <- eclQuery
		eclQuery <<- ""
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
					 <userName>',hpccUsername,'</userName>\
					 <cluster>',hpccClustername,'</cluster>\
					 <limitResults>0</limitResults>\
					 <eclText>',eclCode,'</eclText>\
					 <snapshot/>\
					 </RunEclRequest>\
					 </soap:Body>\
					 </soap:Envelope>\n', sep="")
		eclCode <<- eclCode
		
		headerFields = c(Accept = "text/xml", Accept = "multipart/*", 
						 `Content-Type` = "text/xml; charset=utf-8", SOAPAction = "urn:hpccsystems:ws:ecldirect")
		reader = basicTextGatherer()
		
		handle = getCurlHandle()
		uUrlforEx <- paste(hpccProtocol,'://',hpccHostName,":",hpccPort,"/",ecl_direct,"/RunEcl?ver_=1", sep="")
		
		ur <- uUrlforEx
		curlPerform(url = ur, httpheader = headerFields, postfields = body, 
					writefunction = reader$update, curl = handle)
		status = getCurlInfo(handle)$response.code
		varWu1 <- reader$value()
		newlst <- xmlParse(varWu1)
		layout <- getNodeSet(newlst, "//*[local-name()='results']/text()", 
							 namespaces = xmlNamespaceDefinitions(newlst, simplify = TRUE))
		
		colLayout <- layout[[1]]
# 		print(colLayout)
		layout1 <- xmlToList(colLayout)
# 		print(layout1)
		hpccData <<- data.result(layout1)
# 		hpccData
		hpcc.showFilesToDownload()
		if(length(hpccData)==0)
			return()
		i <- readline('Do you want to plot any graph (Y/N): ')
		if(i!='Y') {
			return('Bye')
		}
		for(i in 1:length(hpccData)) {
			print(paste(i,names(hpccData[[i]]),sep=' '))
		}
		a <- readline('Choose the dataset to plot : ')
		a <- as.numeric(a)
		print("1. Histogram")
		print("2. Scatter Plot")
		graphtype <- readline('Choose the graph to plot : ')
		graphtype <- as.numeric(graphtype)
		lis <- as.list(names(hpccData[[a]][[1]]))
		for(i in 1:length(lis)) {
			print(paste(i,lis[[i]][[1]],sep=' '))
		}
		x <- readline('Input the variable to plot as X-axis :' )
		x <- as.numeric(x)
		y <- readline('Input the variable to plot as Y-axis :' )
		y <- as.numeric(y)
		
		if(graphtype==2) {
			plot(as.numeric(hpccData[[a]][[1]][[x]]),as.numeric(hpccData[[a]][[1]][[y]]),xlab=lis[[x]][[1]],ylab=lis[[y]][[1]])
		}
		if(graphtype==1) {
			hist(as.numeric(hpccData[[1]][[a]][[x]]),as.numeric(hpccData[[1]][[a]][[y]]),xlab=lis[[x]][[1]],ylab=lis[[y]][[1]])
		}		
	}

data.result <- function (xmlResult, downloadPath, format) {
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

