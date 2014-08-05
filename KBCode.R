hpcc.output <- function (out.dataframe,noOfRecordsNeed,outName=NULL) {
	outX <- out.dataframe
	if(!is.null(outName))
		outX <- outName 
	if(noOfRecordsNeed==0)
		out <- ''
	out <- paste("OUTPUT(", out.dataframe,"[1..",noOfRecordsNeed,"],named('", outX, "'));", sep = "")
	if(noOfRecordsNeed==0)
		out <- ''
	return(out)
}


hpcc.begin <-function (import='') {
		xyz <<- ""
		fileout<-getwd()
		libFolderPaths<-.libPaths()
		hostpropertiesPath <- NULL
		str1<-''
		packageName <- getPackageName()
		i<-0
		config <-NULL
		if(length(libFolderPaths) > 0) {
			for(i in 1 : length(libFolderPaths)) {
				str1 <- paste(libFolderPaths[i], "/", packageName, "/RConfig.yml", sep="");
				if(file.exists(str1)) {
					config <- yaml.load_file(str1)
					break
				}
			}
		}	
		if(is.null(config))
			stop("Can't fing RConfig.yml")
		hostFileName <- config$hpcchost$filename
		str1 <- paste(libFolderPaths[i], "/", packageName, "/", hostFileName, sep="");
		if(file.exists(str1)) {
			hostpropertiesPath <- str1
		}
		if(is.null(hostpropertiesPath)) {
			print(paste('Host Properties File is missing in ',libFolderPaths))
			inp <- readline(prompt="Do you want to input the details here(Y/N) : ")
			if(inp=='Y') {
				hostName <- readline(prompt="Enter the Host Adress(Ex : 111.111.11.11) : ")
				protocol <- readline(prompt="Enter the Protocol (http/https) : ")
				if(is.null(protocol))
					protocol <- 'http'
				port <- readline(prompt="Enter the Port(Ex : 8010) : ")
			}
			else {
				return()
			}
			
			hpcc <- list(hpcc=list(hostName = hostName,protocol = protocol,port_thor=port))
			hpcc <- as.yaml(hpcc)
			file.create(str1)
			fileCon <- file(str1)
			writeLines(hpcc, fileCon)
			close(fileCon)
			
		}
		else {
			obj_hpccHost_prop <- yaml.load_file(hostpropertiesPath)
			hostName <- obj_hpccHost_prop$hpcc$host
			protocol <- obj_hpccHost_prop$hpcc$protocol
			port <- obj_hpccHost_prop$hpcc$port_thor
		}
		dfu_service <- config$hpcchost$dfu_service
		uUrl <<- paste(protocol,'://',hostName,":",port,"/", dfu_service, sep="")
		uUrlforEx <<- paste(protocol,'://',hostName,":",port,"/EclDirect/RunEcl?ver_=1", sep="")
		xyz <- paste('import',import,sep=' ')
}


data.result <- function (xmlResult, downloadPath, format) {
		data
		if (missing(xmlResult)) {
			stop("Empty XML String.")
		}
		else {
			docRoot = xmlRoot(xmlTreeParse(contentcsv))
			nodes = getNodeSet(docRoot, "//Dataset")
			for (i in 1:length(nodes)) {
				datasetNode <- nodes[[i]]
				resultSetName = xmlGetAttr(datasetNode, "name")
				x <- array(1:length(datasetNode) * length(datasetNode[[1]]), 
						   dim = c(length(datasetNode), length(datasetNode[[1]])))
				for (j in 1:length(datasetNode)) {
					rowNode <- datasetNode[[j]]
					for (k in 1:length(rowNode)) {
						actualNode <- rowNode[[k]]
						x[j, k] <- xmlValue(actualNode)
					}
				}
				y <- array(1:length(datasetNode) * length(datasetNode[[1]]), 
						   dim = c(length(datasetNode), length(datasetNode[[1]])))
				for (j in 1:1) {
					rowNode <- datasetNode[[j]]
					for (k in 1:length(rowNode)) {
						actualNode <- rowNode[[k]]
						y[j, k] <- xmlName(actualNode)
					}
				}
				y1 <- y[1, ]
				colnames(x, do.NULL = FALSE)
				colnames(x) <- c(y1)
				if (!missing(downloadPath)) {
					if (missing(format)) {
						assign(resultSetName, x, envir = .GlobalEnv)
					}
				}
				data = as.list(data, stringsAsFactors = FALSE)
			}
			data
		}
}

dataresult <- function (xmlResult, downloadPath) {
		
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
			}  
		}
		
		data
}


hpcc.execute <-
	function (signal) 
	{
		if(is.null(uUrl))
			stop('Please start HPCC using function beginHpcc')
		xyz2 <<- xyz
		xyz <<- ""
		fileout <- getwd()
		str <- .libPaths()
		eclCode <<- paste('<![CDATA[',xyz2,']]>')
		body <- ""
		body <- paste("<?xml version=\"1.0\" encoding=\"utf-8\"?>\n                 <soap:Envelope xmlns:soap=\"http://schemas.xmlsoap.org/soap/envelope/\" \n                 xmlns:SOAP-ENC=\"http://schemas.xmlsoap.org/soap/encoding/\" \n                 xmlns=\"urn:hpccsystems:ws:ecldirect\">\n                 <soap:Body>\n                 <RunEclRequest>\n                 <userName>xyz</userName>\n                 <cluster>thor</cluster>\n                 <limitResults>0</limitResults>\n                 <eclText>", 
					  eclCode, "</eclText>\n                 <snapshot>test</snapshot>\n                 </RunEclRequest>\n                 </soap:Body>\n                 </soap:Envelope>\n")
		
		headerFields = c(Accept = "text/xml", Accept = "multipart/*", 
						 `Content-Type` = "text/xml; charset=utf-8", SOAPAction = "urn:hpccsystems:ws:ecldirect")
		reader = basicTextGatherer()
		handle = getCurlHandle()
		ur <- uUrlforEx
		curlPerform(url = ur, httpheader = headerFields, postfields = body, 
					writefunction = reader$update, curl = handle)
		status = getCurlInfo(handle)$response.code
		varWu1 <- reader$value()
		newlst <- xmlParse(varWu1)
		layout <- getNodeSet(newlst, "//*[local-name()='results']/text()", 
							 namespaces = xmlNamespaceDefinitions(newlst, simplify = TRUE))
		colLayout <- layout[[1]]
		layout1 <- xmlToList(colLayout)
		contentcsv <<- layout1
		dataXD <<- dataresult(contentcsv, downloadPath = fileout)
	}

hpcc.data.layout <-
	function(logicalfilename) {
		if(is.null(uUrl))
			stop('Start HPCC using begin.hpcc')
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
		curlPerform(url = uUrl,
					httpheader = headerFields,
					postfields = body,
					writefunction = reader$update,
					curl = handle
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

hpcc.read.data <-
	function (logicalfilename, filetype, out.dataframe,output=20,spray=FALSE,submit=TRUE) 
	{
		strim <<- function(x) {
			gsub("^\\s+|\\s+$", "", x)
			gsub("^,+|,+$", "", x)
		}
		is.not.null <- function(x) !is.null(x)
		if (missing(logicalfilename)) {
			stop("no logical file name")
		}
		xyz <- ""
		strd <- strim(paste('layoutname', ":=", hpcc.data.layout(logicalfilename), sep = " "))
		xyz <- strim(paste(strd, sep = ""))
		str1 <- strim(paste(logicalfilename, "'", sep = ""))
		str2 <- strim(paste(str1, ",",'layoutname', ",", filetype,
							");", sep = ""))
		xyz <- strim(paste(xyz, paste(out.dataframe, ":=DATASET('~", 
									  str2, sep = "")))
		xyz <- strim(paste(xyz, hpcc.output(out.dataframe,output)))
		hpcc.submit(xyz,submit)
	}

hpcc.read.inline.data <-
	function (layoutname, linedata, fieldtype, out.dataframe,output=20,spray=FALSE,submit=TRUE) 
	{
		strim <<- function(x) {
			gsub("^\\s+|\\s+$", "", x)
			gsub("^,+|,+$", "", x)
		}
		is.not.null <- function(x) !is.null(x)
		if (missing(linedata)) {
			stop("no linedata")
		}
			xyz <- ""
			strd <- strim(paste(layoutname, ":=RECORD", fieldtype, 
								sep = " "))
			xyz <- strim(paste(strd, "END;", sep = ""))
			str1 <- strim(paste(out.dataframe, ":=DATASET([", sep = ""))
			str2 <- strim(paste(linedata))
			str3 <- strim(paste(str1, str2, "],", layoutname, ");", 
								sep = ""))
			str4 <- strim(paste(str3, "data_creation", sep = ","))
			xyz <- strim(paste(xyz, str3))
			xyz <- strim(paste(xyz, hpcc.output(out.dataframe,output)))
		hpcc.submit(xyz,submit)
	}

hpcc.corr <-
	function(dataframe,fields,method,out.dataframe,output=20,spray=FALSE,submit=TRUE){
		
		strim<<-function (x)
		{
			gsub("^\\s+|\\s+$", "", x)
			gsub("^,+|,+$", "", x)
		}
		
		varlst<<-strsplit(fields, ",")
		str1<-NULL
		str2<-NULL
		for (i in 1:length(varlst[[1]]))
		{
			k<-strim(varlst[[1]][i])
			h<<-strsplit(k," ")
			if (i > 1)
			{
				charh<-paste("'",h[[1]][1],"'",sep="")
				str1<-strim(paste(str1,charh,sep=","))
				hh<-strim(paste("LEFT.",h[[1]][1],sep=""))
				str2<-strim(paste(str2,hh,sep=","))
			}
			else
			{
				charh<-paste("'",h[[1]][1],"'",sep="")
				str1<-strim(paste(str1,charh))
				hh<-strim(paste("LEFT.",h[[1]][1],sep=""))
				str2<-strim(paste(str2,hh))
			}
			
		}
		
		xyz<-paste(xyz,paste("recmax :=RECORD"),"\n")
		xyz<-paste(xyz,paste("INTEGER3 id; "),"\n")
		xyz<-paste(xyz,paste(dataframe,";"),"\n")
		xyz<-paste(xyz,paste("END;"),"\n")
		xyz<-paste(xyz,paste("RECMAX maxtrans (",dataframe,"  L, INTEGER C) := TRANSFORM"),"\n")
		xyz<-paste(xyz,paste("SELF.id :=C;"),"\n")
		xyz<-paste(xyz,paste("SELF :=L;"),"\n")
		xyz<-paste(xyz,paste("END;"),"\n")
		xyz<-paste(xyz,paste("DSRMAX:=PROJECT(",dataframe," ,maxtrans(LEFT,COUNTER));"),"\n")
		
		xyz<-paste(xyz,paste("NumericField :=RECORD"),"\n")
		xyz<-paste(xyz,paste("UNSIGNED id;"),"\n")
		xyz<-paste(xyz,paste("UNSIGNED4 number;"),"\n")
		xyz<-paste(xyz,paste("REAL8 value;"),"\n")
		xyz<-paste(xyz,paste("STRING field;"),"\n")
		xyz<-paste(xyz,paste("END;"),"\n")
		
		xyz<-paste(xyz,paste("OutDs:=NORMALIZE(DSRMAX,",length(varlst[[1]]),",TRANSFORM(NumericField,SELF.id:=LEFT.id,SELF.number:=COUNTER;
							  SELF.field:=CHOOSE(COUNTER,",str1,");
							  SELF.value:=CHOOSE(COUNTER,",str2,")));","\n"))
		
		xyz<-paste(xyz,paste("RankableField :=RECORD"),"\n")
		xyz<-paste(xyz,paste("outDS;"),"\n")
		xyz<-paste(xyz,paste("UNSIGNED Pos := 0;"),"\n")
		xyz<-paste(xyz,paste("END;"),"\n")
		xyz<-paste(xyz,paste("T :=TABLE(SORT(OutDS,Number,field,Value),RankableField);"),"\n")
		#xyz<-paste(xyz,paste("T;"),"\n")
		
		xyz<-paste(xyz,paste("TYPEOF(T) add_rank(T le,UNSIGNED c) := TRANSFORM"),"\n")
		xyz<-paste(xyz,paste("SELF.Pos := c;"),"\n")
		xyz<-paste(xyz,paste("SELF := le;"),"\n")
		xyz<-paste(xyz,paste("END;"),"\n")
		xyz<-paste(xyz,paste("P := PROJECT(T,add_rank(LEFT,COUNTER));"),"\n")
		
		xyz<-paste(xyz,paste("RS := RECORD"),"\n")
		xyz<-paste(xyz,paste("Seq := MIN(GROUP,P.pos);"),"\n")
		xyz<-paste(xyz,paste("P.number;"),"\n")
		xyz<-paste(xyz,paste("END;"),"\n")
		xyz<-paste(xyz,paste("Splits := TABLE(P,RS,number,FEW);"),"\n")
		
		xyz<-paste(xyz,paste("TYPEOF(T) to(P le,Splits ri) := TRANSFORM"),"\n")
		xyz<-paste(xyz,paste("SELF.pos := 1+le.pos - ri.Seq;"),"\n")
		xyz<-paste(xyz,paste("SELF := le;"),"\n")
		xyz<-paste(xyz,paste("END;"),"\n")
		xyz<-paste(xyz,paste("outfile := JOIN(P,Splits,LEFT.number=RIGHT.number,to(LEFT,RIGHT),LOOKUP);"),"\n")
		
		xyz<-paste(xyz,paste("modeRec := RECORD"),"\n")
		xyz<-paste(xyz,paste("outfile.number;"),"\n")
		xyz<-paste(xyz,paste("outfile.value;"),"\n")
		xyz<-paste(xyz,paste("outfile.pos;"),"\n")
		xyz<-paste(xyz,paste("outfile.field;"),"\n")
		xyz<-paste(xyz,paste("vals := COUNT(GROUP);"),"\n")
		xyz<-paste(xyz,paste("END;"),"\n")
		xyz<-paste(xyz,paste("MTable := TABLE(outfile,modeRec,number,field,value);"),"\n")
		#xyz<-paste(xyz,paste("MTable;"),"\n")
		
		xyz<-paste(xyz,paste("newRec := RECORD"),"\n")
		xyz<-paste(xyz,paste("MTable.number;"),"\n")
		xyz<-paste(xyz,paste("MTable.value;"),"\n")
		xyz<-paste(xyz,paste("MTable.field;"),"\n")
		xyz<-paste(xyz,paste("po := (MTable.pos*Mtable.vals + ((Mtable.vals-1)*Mtable.vals/2))/Mtable.vals;"),"\n")
		xyz<-paste(xyz,paste("END;"),"\n")
		
		xyz<-paste(xyz,paste("newTable := TABLE(MTable,newRec);"),"\n")
		#xyz<-paste(xyz,paste("OUTPUT(newTable,NAMED('TEST'));"),"\n")
		xyz<-paste(xyz,paste("TestTab := JOIN(outfile,newTable,LEFT.number = RIGHT.number AND LEFT.value = RIGHT.value);"),"\n")
		#xyz<-paste(xyz,paste("TestTab;"),"\n")
		
		xyz<-paste(xyz,paste("MyRec := RECORD"),"\n")
		xyz<-paste(xyz,paste("TestTab;"),"\n")
		xyz<-paste(xyz,paste("END;"),"\n")
		xyz<-paste(xyz,paste("T1 := TABLE(TestTab,MyRec,id,number,field);"),"\n")
		#xyz<-paste(xyz,paste("T1;"),"\n")
		
		if (method=='S')
		{
			xyz<-paste(xyz,paste("SingleForm := Record"),"\n")
			xyz<-paste(xyz,paste("T1.number;"),"\n")
			xyz<-paste(xyz,paste("T1.field;"),"\n")
			xyz<-paste(xyz,paste("REAL8 meanS := AVE(GROUP,T1.po);"),"\n")
			xyz<-paste(xyz,paste("REAL8 sdS := SQRT(VARIANCE(GROUP,T1.po));"),"\n")
			xyz<-paste(xyz,paste("END;"),"\n")
			
			xyz<-paste(xyz,paste("single := TABLE(T1,SingleForm,number,field,FEW);"))
			
			xyz<-paste(xyz,paste("PairRec := RECORD"),"\n")
			xyz<-paste(xyz,paste("UNSIGNED4 left_number;"),"\n")
			xyz<-paste(xyz,paste("UNSIGNED4 right_number;"),"\n")
			xyz<-paste(xyz,paste("STRING left_field;"),"\n")
			xyz<-paste(xyz,paste("STRING right_field;"),"\n")
			xyz<-paste(xyz,paste("REAL8   xyS;"),"\n")
			xyz<-paste(xyz,paste("END;"),"\n")
			
			xyz<-paste(xyz,paste("PairRec note_prod(T1 L, T1 R) := TRANSFORM"),"\n")
			xyz<-paste(xyz,paste("SELF.left_number := L.number;"),"\n")
			xyz<-paste(xyz,paste("SELF.right_number := R.number;"),"\n")
			xyz<-paste(xyz,paste("SELF.left_field := L.field;"),"\n")
			xyz<-paste(xyz,paste("SELF.right_field := R.field;"),"\n")
			xyz<-paste(xyz,paste("SELF.xyS := L.po*R.po;"),"\n")
			xyz<-paste(xyz,paste("END;"),"\n")
			
			xyz<-paste(xyz,paste("pairs := JOIN(T1,T1,LEFT.id=RIGHT.id AND LEFT.number<![CDATA[<]]>RIGHT.number,note_prod(LEFT,RIGHT));"),"\n")
			
			xyz<-paste(xyz,paste("PairAccum := RECORD"),"\n")
			xyz<-paste(xyz,paste("pairs.left_number;"),"\n")
			xyz<-paste(xyz,paste("pairs.right_number;"),"\n")
			xyz<-paste(xyz,paste("pairs.left_field;"),"\n")
			xyz<-paste(xyz,paste("pairs.right_field;"),"\n")
			xyz<-paste(xyz,paste("e_xyS := SUM(GROUP,pairs.xyS);"),"\n")
			xyz<-paste(xyz,paste("END;"),"\n")
			
			xyz<-paste(xyz,paste("exys := TABLE(pairs,PairAccum,left_number,right_number,left_field,right_field,FEW);"),"\n")
			xyz<-paste(xyz,paste("with_x := JOIN(exys,single,LEFT.left_number = RIGHT.number,LOOKUP);"),"\n")
			
			
			xyz<-paste(xyz,paste("Rec := RECORD"),"\n")
			xyz<-paste(xyz,paste("UNSIGNED left_number;"),"\n")
			xyz<-paste(xyz,paste("UNSIGNED right_number;"),"\n")
			xyz<-paste(xyz,paste("STRING left_field;"),"\n")
			xyz<-paste(xyz,paste("STRING right_field;"),"\n")
			xyz<-paste(xyz,paste("REAL8 Spearman;"),"\n")
			xyz<-paste(xyz,paste("END;"),"\n")
			xyz<-paste(xyz,paste("n := COUNT(",dataframe,");"),"\n")
			
			xyz<-paste(xyz,paste("Rec Trans(with_x L, single R) := TRANSFORM"),"\n")
			xyz<-paste(xyz,paste("SELF.Spearman := (L.e_xyS - n*L.meanS*R.meanS)/(n*L.sdS*R.sdS);"),"\n")
			xyz<-paste(xyz,paste("SELF := L;"),"\n")
			xyz<-paste(xyz,paste("END;"),"\n")
			xyz<-paste(xyz,paste("intjoin:= JOIN(with_x,single,LEFT.right_number=RIGHT.number,Trans(LEFT,RIGHT),LOOKUP);"),"\n")
			xyz<-paste(xyz,paste(out.dataframe,":=SORT(TABLE(intjoin,{left_field,right_field,Spearman}),left_field,right_field);"),"\n")
			xyz<-paste(xyz,hpcc.output(out.dataframe,output),"\n")  
		}
		else if (method=='P')
			{
				xyz<-paste(xyz,paste("SingleForm := Record"),"\n")
				xyz<-paste(xyz,paste("T1.number;"),"\n")
				xyz<-paste(xyz,paste("T1.field;"),"\n")
				xyz<-paste(xyz,paste("REAL8 meanP := AVE(GROUP,T1.value);"),"\n")
				xyz<-paste(xyz,paste("REAL8 sdP := SQRT(VARIANCE(GROUP,T1.value));"),"\n")
				xyz<-paste(xyz,paste("END;"),"\n")
				
				xyz<-paste(xyz,paste("single := TABLE(T1,SingleForm,number,field,FEW);"))
				
				xyz<-paste(xyz,paste("PairRec := RECORD"),"\n")
				xyz<-paste(xyz,paste("UNSIGNED4 left_number;"),"\n")
				xyz<-paste(xyz,paste("UNSIGNED4 right_number;"),"\n")
				xyz<-paste(xyz,paste("STRING left_field;"),"\n")
				xyz<-paste(xyz,paste("STRING right_field;"),"\n")
				xyz<-paste(xyz,paste("REAL8   xyP;"),"\n")
				xyz<-paste(xyz,paste("END;"),"\n")
				
				xyz<-paste(xyz,paste("PairRec note_prod(T1 L, T1 R) := TRANSFORM"),"\n")
				xyz<-paste(xyz,paste("SELF.left_number := L.number;"),"\n")
				xyz<-paste(xyz,paste("SELF.right_number := R.number;"),"\n")
				xyz<-paste(xyz,paste("SELF.left_field := L.field;"),"\n")
				xyz<-paste(xyz,paste("SELF.right_field := R.field;"),"\n")
				xyz<-paste(xyz,paste("SELF.xyP := L.value*R.value;"),"\n")
				xyz<-paste(xyz,paste("END;"),"\n")
				
				xyz<-paste(xyz,paste("pairs := JOIN(T1,T1,LEFT.id=RIGHT.id AND LEFT.number<![CDATA[<]]>RIGHT.number,note_prod(LEFT,RIGHT));"),"\n")
				
				xyz<-paste(xyz,paste("PairAccum := RECORD"),"\n")
				xyz<-paste(xyz,paste("pairs.left_number;"),"\n")
				xyz<-paste(xyz,paste("pairs.right_number;"),"\n")
				xyz<-paste(xyz,paste("pairs.left_field;"),"\n")
				xyz<-paste(xyz,paste("pairs.right_field;"),"\n")
				xyz<-paste(xyz,paste("e_xyP := SUM(GROUP,pairs.xyP);"),"\n")
				xyz<-paste(xyz,paste("END;"),"\n")
				
				xyz<-paste(xyz,paste("exys := TABLE(pairs,PairAccum,left_number,right_number,left_field,right_field,FEW);"),"\n")
				xyz<-paste(xyz,paste("with_x := JOIN(exys,single,LEFT.left_number = RIGHT.number,LOOKUP);"),"\n")
				
				xyz<-paste(xyz,paste("Rec := RECORD"),"\n")
				xyz<-paste(xyz,paste("UNSIGNED left_number;"),"\n")
				xyz<-paste(xyz,paste("UNSIGNED right_number;"),"\n")
				xyz<-paste(xyz,paste("STRING left_field;"),"\n")
				xyz<-paste(xyz,paste("STRING right_field;"),"\n")
				xyz<-paste(xyz,paste("REAL8 Pearson;"),"\n")
				xyz<-paste(xyz,paste("END;"),"\n")
				xyz<-paste(xyz,paste("n := COUNT(",dataframe,");"),"\n")
				
				xyz<-paste(xyz,paste("Rec Trans(with_x L, single R) := TRANSFORM"),"\n")
				xyz<-paste(xyz,paste("SELF.Pearson := (L.e_xyP - n*L.meanP*R.meanP)/(n*L.sdP*R.sdP);"),"\n")
				xyz<-paste(xyz,paste("SELF := L;"),"\n")
				xyz<-paste(xyz,paste("END;"),"\n")
				xyz<-paste(xyz,paste("intjoin:= JOIN(with_x,single,LEFT.right_number=RIGHT.number,Trans(LEFT,RIGHT),LOOKUP);"),"\n")
				xyz<-paste(xyz,paste(out.dataframe,":=SORT(TABLE(intjoin,{left_field,right_field,Pearson}),left_field,right_field);"),"\n")
				xyz<-paste(xyz,paste("OUTPUT(CHOOSEN(",out.dataframe,",20),named('",out.dataframe,"'));",sep=""),"\n")  
				
			}
			else
			{
				stop("no proper method")  
			}
			hpcc.submit(xyz,submit)
		}

hpcc.dedup <-
	function (dataframe, out.dataframe,output=20,spray=FALSE, condition = NULL, all = NULL, 
			  hash = NULL, keep = NULL, keeper = NULL, local = NULL,submit=TRUE) 
	{
		strim <- function(x) {
			gsub("^\\s+|\\s+$", "", x)
		}
		is.not.null <- function(x) !is.null(x)
		if (missing(dataframe)) {
			stop("no dataframe.")
		}
		else {
			if ((is.not.null(all)) & (is.not.null(hash)) & (is.not.null(keep))) {
				stop("KEEP is not supported for DEDUP ALL")
			}
			else {
				if ((is.not.null(hash)) & (is.not.null(keep))) {
					stop("KEEP is not supported for DEDUP ALL")
				}
				else {
					if ((is.not.null(all)) & (is.not.null(keep))) {
						stop("KEEP is not supported for DEDUP ALL")
					}
					else {
						if ((is.null(condition)) & (is.null(all))) {
# 							xyz <- NULL
							strall <- strim(paste(dataframe, condition, 
												  "ALL", hash, keeper, local, sep = " "))
							strall1 <- strim(gsub(" {2,}", " ", strall))
							strall2 <- strim(gsub(" ", ",", strall1))
							strnall <- strim(paste(dataframe, condition, 
												   hash, keeper, local, sep = " "))
							strnall1 <- strim(gsub(" {2,}", " ", strnall))
							strnall2 <- strim(gsub(" ", ",", strnall1))
							if (is.null(keep)) {
								strall3 <- strall2
								xyz <- strim(paste(xyz, paste("outdup := DEDUP(", 
															   strall3, sep = "", ");")))
							}
							else {
								strnall3 <- paste(strnall2, keep, sep = ",")
								xyz <- strim(paste(xyz, paste("outdup := DEDUP(", 
															   strnall3, sep = "", ");")))
							}
						}
						else {
							xyz <- NULL
							strall <- strim(paste(dataframe, condition, 
												  all, hash, keeper, local, sep = " "))
							strall1 <- strim(gsub(" {2,}", " ", strall))
							strall2 <- strim(gsub(" ", ",", strall1))
							strnall <- strim(paste(dataframe, condition, 
												   hash, keeper, local, sep = " "))
							strnall1 <- strim(gsub(" {2,}", " ", strnall))
							strnall2 <- strim(gsub(" ", ",", strnall1))
							if (is.null(keep)) {
								strall3 <- strall2
								xyz <- strim(paste(xyz, paste(out.dataframe," := DEDUP(", 
															   strall3, sep = "", ");")))
							}
							else {
								strnall3 <- paste(strnall2, keep, sep = ",")
								xyz <- strim(paste(xyz, paste(out.dataframe, 
															  ":= DEDUP(", strnall3, sep = "", ");")))
								xyz <- strim(paste(xyz, hpcc.output(out.dataframe,output)))
							}
						}
					}
				}
			}
		}
		hpcc.submit(xyz,submit)
	}


hpcc.distribute <-
	function (dataframe, out.dataframe, output=20,spray=FALSE, form = NULL, expression = NULL, 
			  index = NULL, skew = NULL, sorts = NULL, joincondition = NULL, 
			  maxskew = NULL, skewlimit = NULL,submit=TRUE) 
	{
		strim <- function(x) {
			gsub("^\\s+|\\s+$", "", x)
		}
		is.not.null <- function(x) !is.null(x)
		if (missing(dataframe)) {
			stop("no dataframe.")
		}
		else {
			if (is.null(form)) {
				xyz <- NULL
				strexp <- strim(paste(dataframe, sep = " "))
				xyz <- strim(paste(xyz, paste("outdist := DISTRIBUTE(", 
											   strexp, sep = "", ");")))
			}
			else {
				if (form == "expression") {
					if (is.not.null(sorts)) {
						xyz <- NULL
						strex <- strim(paste("MERGE(", sorts, ")", 
											 sep = ""))
						strexp <- strim(paste(dataframe, expression, 
											  strex, sep = ","))
						xyz <- strim(paste(xyz, paste("outdist := DISTRIBUTE(", 
													   strexp, sep = "", ");")))
					}
					else {
						xyz <- NULL
						strexp <- strim(paste(dataframe, expression, 
											  sep = ","))
						xyz <- strim(paste(xyz, paste("outdist := DISTRIBUTE(", 
													   strexp, sep = "", ");")))
					}
				}
				else {
					if (form == "index") {
						if (is.not.null(joincondition)) {
							xyz <- NULL
							strexp <- strim(paste(dataframe, index, joincondition, 
												  sep = ","))
							xyz <- strim(paste(xyz, paste("outdist := DISTRIBUTE(", 
														   strexp, sep = "", ");")))
						}
						else {
							xyz <- NULL
							strexp <- strim(paste(dataframe, index, sep = ","))
							xyz <- strim(paste(xyz, paste("outdist := DISTRIBUTE(", 
														   strexp, sep = "", ");")))
						}
					}
					else {
						if (form == "skew") {
							xyz <- NULL
							stre <- strim(paste(maxskew, skewlimit, sep = " "))
							stre1 <- strim(gsub(" {2,}", " ", stre))
							stre2 <- strim(gsub(" ", ",", stre1))
							strex <- strim(paste("SKEW(", stre2, ")", 
												 sep = ""))
							strexp <- strim(paste(dataframe, strex, sep = ","))
							xyz <- strim(paste(xyz, paste(out.dataframe, 
														  ":= DISTRIBUTE(", strexp, sep = "", ");")))
							xyz <- strim(paste(xyz, hpcc.output(out.dataframe,output)))
						}
					}
				}
			}
		}
		hpcc.submit(xyz,submit)
	}


hpcc.freq <-
	function(dataframe,fields,sortorder=NULL,out.dataframe,output=20,spray=FALSE,submit=TRUE){
		
		
		strim<<-function (x)
		{
			gsub("^\\s+|\\s+$", "", x)
			gsub("^,+|,+$", "", x)
		}
		
		semitrim<<-function(x)
		{
			gsub(";","",x)
		}
		
		## split layout var types####
		## respective int and string combos## 
		semiperson<-semitrim(personout)
		split_val<<- strsplit(semiperson, "\n")
		int<-grep("integer",split_val[[1]])
		rl<-grep("real",split_val[[1]])
		una<-grep("unasign",split_val[[1]])
		dcml<-grep("decimal",split_val[[1]])
		dbl<-grep("double",split_val[[1]])
		real_int<<-c(split_val[[1]][int],split_val[[1]][rl],split_val[[1]][una],split_val[[1]][dcml],split_val[[1]][dbl])
		str<-grep("string",split_val[[1]])
		uni<-grep("unicode",split_val[[1]])
		str_uni<<-c(split_val[[1]][str],split_val[[1]][uni])
		
		num_new<<-NA
		char_new<<-NA
		
		##split the var string###  
		field_splt <<- strsplit(fields, ",")
		for (i in 1:length(field_splt[[1]]))
		{
			j<-strim(field_splt[[1]][i])
			
			if (any(grepl(j,real_int)))
				num_new[i]<<-j
			else
				char_new[i]<<-j      
		}
		
		num_new<-na.omit(num_new)
		char_new<-na.omit(char_new)
		
		is.not.na <- function(x) !is.na(x) 
		is.not.null <- function(x) !is.null(x)
		
		if (is.not.na(char_new[1]))
		{
			varlst<<-strsplit(char_new, ",")
			str1<-NULL
			str2<-NULL
			for (i in 1:length(char_new))
			{
				k<-strim(varlst[[i]][1])
				h<<-strsplit(k," ")
				if (i > 1)
				{
					charh<-paste("'",h[[1]][1],"'",sep="")
					str1<-strim(paste(str1,charh,sep=","))
					hh<-strim(paste("LEFT.",h[[1]][1],sep=""))
					str2<-strim(paste(str2,hh,sep=","))
				}
				else
				{
					charh<-paste("'",h[[1]][1],"'",sep="")
					str1<-strim(paste(str1,charh))
					hh<-strim(paste("LEFT.",h[[1]][1],sep=""))
					str2<-strim(paste(str2,hh))
				}
			}
			
			xyz<-paste(xyz,paste("NumericField :=RECORD"),"\n")
			xyz<-paste(xyz,paste("STRING field;"),"\n")
			xyz<-paste(xyz,paste("STRING value;"),"\n")
			xyz<-paste(xyz,paste("END;"),"\n")
			xyz<-paste(xyz,paste("OutDSStr := NORMALIZE(",dataframe,",",length(varlst),",TRANSFORM(NumericField,SELF.field:=CHOOSE(COUNTER,",str1,");
								  SELF.value:=CHOOSE(COUNTER,",str2,")));",sep=""),"\n")
			xyz<-paste(xyz,paste("FreqRecStr:=RECORD"),"\n")
			xyz<-paste(xyz,paste("OutDSStr.field;"),"\n")     
			xyz<-paste(xyz,paste("OutDSStr.value;"),"\n")
			xyz<-paste(xyz,paste("INTEGER frequency:=COUNT(GROUP);"),"\n")
			xyz<-paste(xyz,paste("REAL8 Percent:=(COUNT(GROUP)/COUNT(",dataframe,"))*100;"),"\n")
			xyz<-paste(xyz,paste("END;"),"\n")
			xyz<-paste(xyz,paste("Frequency1 := TABLE(OutDSStr,FreqRecStr,field,value,MERGE);"),"\n")
		} 
		
		
		if (is.not.na(num_new[1]))
		{
			
			varlst<<-strsplit(num_new, ",")
			str1<-NULL
			str2<-NULL
			for (i in 1:length(num_new))
			{
				k<-strim(varlst[[i]][1])
				h<<-strsplit(k," ")
				if (i > 1)
				{
					charh<-paste("'",h[[1]][1],"'",sep="")
					str1<-strim(paste(str1,charh,sep=","))
					hh<-strim(paste("LEFT.",h[[1]][1],sep=""))
					str2<-strim(paste(str2,hh,sep=","))
				}
				else
				{
					charh<-paste("'",h[[1]][1],"'",sep="")
					str1<-strim(paste(str1,charh))
					hh<-strim(paste("LEFT.",h[[1]][1],sep=""))
					str2<-strim(paste(str2,hh))
				}
			}
			xyz<-paste(xyz,paste("NumField:=RECORD"),"\n")
			xyz<-paste(xyz,paste("STRING field;"),"\n")
			xyz<-paste(xyz,paste("REAL value;"),"\n")
			xyz<-paste(xyz,paste("END;"),"\n")
			xyz<-paste(xyz,paste("OutDSNum := NORMALIZE(",dataframe,",",length(varlst),",TRANSFORM(NumField,SELF.field:=CHOOSE(COUNTER,",str1,");
								  SELF.value:=CHOOSE(COUNTER,",str2,")));",sep=""),"\n")
			xyz<-paste(xyz,paste("FreqRecNum:=RECORD"),"\n")
			xyz<-paste(xyz,paste("OutDSNum.field;"),"\n")
			xyz<-paste(xyz,paste("OutDSNum.value;"),"\n")
			xyz<-paste(xyz,paste("INTEGER frequency:=COUNT(GROUP);"),"\n")
			xyz<-paste(xyz,paste("REAL8 Percent:=(COUNT(GROUP)/COUNT(",dataframe,"))*100;"),"\n")
			xyz<-paste(xyz,paste("END;"),"\n")
			xyz<-paste(xyz,paste("Frequency2 := TABLE(OutDSNum,FreqRecNum,field,value,MERGE);"),"\n")  
		}
		
		
		
		if (is.not.null(sortorder))
		{
			if (sortorder=='ASC')
				d<<-'+'
			else
				d<<-'-'
			
			for (i in 1:length(field_splt[[1]]))
			{
				
				ind_var<<-strim(field_splt[[1]][i])
				if(any(grepl(ind_var,char_new)))
				{
					freq='frequency1'
				}
				else
				{
					freq='frequency2'
				}
				
				dd<<-grep(ind_var,split_val[[1]])
				vartype<<-strim(split_val[[1]][dd])
				xyz<-paste(xyz,paste(out.dataframe,"_",ind_var,":=SORT(TABLE(",freq,"(field ='",ind_var,"'),{",vartype,                
									  ":=value;frequency;Percent}),",d,ind_var,");",sep=""),"\n")
				xyz<-paste(xyz,paste("OUTPUT(CHOOSEN(",out.dataframe,"_",ind_var,",20),named('",ind_var,"'));",sep=""),"\n")
			} 
		}
		else
		{
			for (i in 1:length(field_splt[[1]]))
			{  
				ind_var<<-strim(field_splt[[1]][i])
				if(any(grepl(ind_var,char_new)))
				{
					freq='frequency1'
				}
				else
				{
					freq='frequency2'
				}
				
				dd<-grep(ind_var,split_val[[1]])
				vartype<-strim(split_val[[1]][dd])
				xyz<-paste(xyz,paste(out.dataframe,"_",ind_var,":=SORT(TABLE(",freq,"(field ='",ind_var,"'),{",vartype,                
									  ":=value;frequency;Percent}),",'+',ind_var,");",sep=""),"\n")
				xyz<-paste(xyz,hpcc.output(out.dataframe,output),"\n")
			}
		}
	hpcc.submit(xyz,submit)	
	}


hpcc.iterate <-
	function (dataframe, calltransfunc, out.dataframe, output=20,spray=FALSE,local = NULL,submit=TRUE) 
	{
		strim <<- function(x) {
			gsub("^\\s+|\\s+$", "", x)
			gsub("^,+|,+$", "", x)
		}
		is.not.null <- function(x) !is.null(x)
		if (missing(dataframe)) {
			stop("no dataframe.")
		}
		else {
			strt1 <- strim(paste(dataframe, sep = ""))
			strt2 <- strim(paste(calltransfunc, "(LEFT,RIGHT)", sep = ""))
			strt3 <- strim(paste(strt1, strt2, sep = ","))
			strl <- strim(paste(strt3, local, sep = ","))
			xyz <- strim(paste(xyz, paste(out.dataframe, ":=ITERATE(", 
										  strl, sep = "", ");")))
			xyz <- strim(paste(xyz, hpcc.output(out.dataframe,output)))
		}
		hpcc.submit(code,submit)
	}

hpcc.max <-
	function(dataframe,fields,out.dataframe,output=20,spray=FALSE,submit=TRUE){
		
		strim<-function (dataframe)
		{
			gsub("^\\s+|\\s+$", "", dataframe)
		}
		
		varlst<<-strsplit(fields, ",")
		str1<-NULL
		str2<-NULL
		for (i in 1:length(varlst[[1]]))
		{
			k<-strim(varlst[[1]][i])
			h<<-strsplit(k," ")
			if (i > 1)
			{
				charh<-paste("'",h[[1]][1],"'",sep="")
				str1<-strim(paste(str1,charh,sep=","))
				hh<-strim(paste("LEFT.",h[[1]][1],sep=""))
				str2<-strim(paste(str2,hh,sep=","))
			}
			else
			{
				charh<-paste("'",h[[1]][1],"'",sep="")
				str1<-strim(paste(str1,charh))
				hh<-strim(paste("LEFT.",h[[1]][1],sep=""))
				str2<-strim(paste(str2,hh))
			}
			
		}
		
		code<-paste(code,paste("recmax :=RECORD"),"\n")
		code<-paste(code,paste("INTEGER3 id;"),"\n")
		code<-paste(code,paste(dataframe,";"),"\n")
		code<-paste(code,paste("END;"),"\n")
		code<-paste(code,paste("RECMAX maxtrans (",dataframe," L, INTEGER C) := TRANSFORM"),"\n")
		code<-paste(code,paste("SELF.id :=C;"),"\n")
		code<-paste(code,paste("SELF :=L;"),"\n")
		code<-paste(code,paste("END;"),"\n")
		code<-paste(code,paste("DSRMAX:=PROJECT(",dataframe,",maxtrans(LEFT,COUNTER));"),"\n")
		#code<-paste(code,paste("DSRMAX;"),"\n")
		code<-paste(code,paste("MaxField:=RECORD"),"\n")
		code<-paste(code,paste("UNSIGNED id;"),"\n")
		code<-paste(code,paste("STRING Field;"),"\n")
		code<-paste(code,paste("REAL8 value;"),"\n")
		code<-paste(code,paste("END;"),"\n")
		code<-paste(code,paste("OutDsMax:=NORMALIZE(DSRMAX,",length(varlst[[1]]),",TRANSFORM(MaxField,SELF.id:=LEFT.id,SELF.Field:=CHOOSE
							  (COUNTER,",str1,sep="",");SELF.value:=CHOOSE(COUNTER,",str2,")));","\n"))
		#code<-paste(code,paste("OutDsMax;"),"\n")
		code<-paste(code,paste("SinglemaxField := RECORD"),"\n")
		code<-paste(code,paste("OutDsMax.Field;"),"\n")
		code<-paste(code,paste("Maxval := MAX(GROUP,OutDsMax.value);"),"\n")
		code<-paste(code,paste("END;"),"\n")
		code<-paste(code,paste(out.dataframe,":= TABLE(OutDsMax,SinglemaxField,Field);"),"\n")
		code <-strim(paste(code,hpcc.output(out.dataframe,output)))
		hpcc.submit(code,submit)
	}

hpcc.mean <-
	function(dataframe,fields,out.dataframe,output=20,spray=FALSE,submit=TRUE){
		
		strim<-function (dataframe)
		{
			gsub("^\\s+|\\s+$", "", dataframe)
		}
		
		varlst<<-strsplit(fields, ",")
		str1<-NULL
		str2<-NULL
		for (i in 1:length(varlst[[1]]))
		{
			k<-strim(varlst[[1]][i])
			h<<-strsplit(k," ")
			if (i > 1)
			{
				charh<-paste("'",h[[1]][1],"'",sep="")
				str1<-strim(paste(str1,charh,sep=","))
				hh<-strim(paste("LEFT.",h[[1]][1],sep=""))
				str2<-strim(paste(str2,hh,sep=","))
			}
			else
			{
				charh<-paste("'",h[[1]][1],"'",sep="")
				str1<-strim(paste(str1,charh))
				hh<-strim(paste("LEFT.",h[[1]][1],sep=""))
				str2<-strim(paste(str2,hh))
			}
			
		}
		
		code<-paste(code,paste("recavg :=RECORD"),"\n")
		code<-paste(code,paste("INTEGER3 id;"),"\n")
		code<-paste(code,paste(dataframe,";"),"\n")
		code<-paste(code,paste("END;"),"\n")
		code<-paste(code,paste("recavg avgtrans (",dataframe," L, INTEGER C) := TRANSFORM"),"\n")
		code<-paste(code,paste("SELF.id :=C;"),"\n")
		code<-paste(code,paste("SELF :=L;"),"\n")
		code<-paste(code,paste("END;"),"\n")
		code<-paste(code,paste("DSRAVG:=PROJECT(",dataframe,",avgtrans(LEFT,COUNTER));"),"\n")
		#code<-paste(code,paste("DSRAVG;"),"\n")
		code<-paste(code,paste("NumAvgField:=RECORD"),"\n")
		code<-paste(code,paste("UNSIGNED id;"),"\n")
		code<-paste(code,paste("STRING field;"),"\n")
		code<-paste(code,paste("REAL8 value;"),"\n")
		code<-paste(code,paste("END;"),"\n")
		code<-paste(code,paste("OutDsavg:=NORMALIZE(DSRAVG,",length(varlst[[1]]),",TRANSFORM
							  (NumAvgField,SELF.id:=LEFT.id,SELF.field:=CHOOSE
							  (COUNTER,",str1,sep="",");SELF.value:=CHOOSE(COUNTER,",str2,")));","\n"))
		#code<-paste(code,paste("OutDsavg;"),"\n")
		code<-paste(code,paste("SingleavgField := RECORD"),"\n")
		code<-paste(code,paste("OutDsavg.field;"),"\n")
		code<-paste(code,paste("Mean := AVE(GROUP,OutDsavg.value);"),"\n")
		code<-paste(code,paste("END;"),"\n")
		code<-paste(code,paste(out.dataframe,":= TABLE(OutDsavg,SingleavgField,field);"),"\n")
		code <-strim(paste(code,hpcc.output(out.dataframe,output)))
		hpcc.submit(code,submit)
	}

hpcc.median <-
	function(dataframe,fields,out.dataframe,output=20,spray=FALSE,submit=TRUE){
		
		trim<-function (dataframe)
		{
			gsub("^\\s+|\\s+$", "", dataframe)
		}
		
		varlst<<-strsplit(fields, ",")
		str1<-NULL
		str2<-NULL
		for (i in 1:length(varlst[[1]]))
		{
			k<-strim(varlst[[1]][i])
			h<<-strsplit(k," ")
			if (i > 1)
			{
				charh<-paste("'",h[[1]][1],"'",sep="")
				str1<-strim(paste(str1,charh,sep=","))
				hh<-strim(paste("LEFT.",h[[1]][1],sep=""))
				str2<-strim(paste(str2,hh,sep=","))
			}
			else
			{
				charh<-paste("'",h[[1]][1],"'",sep="")
				str1<-strim(paste(str1,charh))
				hh<-strim(paste("LEFT.",h[[1]][1],sep=""))
				str2<-strim(paste(str2,hh))
			}
			
		}
		
		xyz<<-paste(xyz,paste("recmax :=RECORD"),"\n")
		xyz<<-paste(xyz,paste("INTEGER3 id;"),"\n")
		xyz<<-paste(xyz,paste(dataframe,";"),"\n")
		xyz<<-paste(xyz,paste("END;"),"\n")
		xyz<<-paste(xyz,paste("RECMAX maxtrans (",dataframe," L, INTEGER C) := TRANSFORM"),"\n")
		xyz<<-paste(xyz,paste("SELF.id :=C;"),"\n")
		xyz<<-paste(xyz,paste("SELF :=L;"),"\n")
		xyz<<-paste(xyz,paste("END;"),"\n")
		
		xyz<<-paste(xyz,paste("DSRMAX:=PROJECT(",dataframe,",maxtrans(LEFT,COUNTER));"),"\n")
		#xyz<<-paste(xyz,paste("DSRMAX;"),"\n")
		
		xyz<<-paste(xyz,paste("MaxField:=RECORD"),"\n")
		xyz<<-paste(xyz,paste("UNSIGNED id;"),"\n")
		xyz<<-paste(xyz,paste("UNSIGNED4 number;"),"\n")
		xyz<<-paste(xyz,paste("STRING Field;"),"\n")
		xyz<<-paste(xyz,paste("REAL8 value;"),"\n")
		xyz<<-paste(xyz,paste("END;"),"\n")
		xyz<<-paste(xyz,paste("OutDsMed:=NORMALIZE(DSRMAX,",length(varlst[[1]]),",TRANSFORM(MaxField,SELF.id:=LEFT.id,SELF.number:=COUNTER;SELF.Field:=CHOOSE
							  (COUNTER,",str1,sep="",");SELF.value:=CHOOSE(COUNTER,",str2,")));","\n"))
		
		xyz<<-paste(xyz,paste("RankableField := RECORD"),"\n")
		xyz<<-paste(xyz,paste("OutDsMed;"),"\n")
		xyz<<-paste(xyz,paste("UNSIGNED Pos := 0;"),"\n")
		xyz<<-paste(xyz,paste("END;"),"\n")
		
		xyz<<-paste(xyz,paste("T := TABLE(SORT(OutDsMed,Number,field,Value),RankableField);"),"\n")
		
		xyz<<-paste(xyz,paste("TYPEOF(T) add_rank(T le,UNSIGNED c) := TRANSFORM"),"\n")
		xyz<<-paste(xyz,paste("SELF.Pos := c;"),"\n")
		xyz<<-paste(xyz,paste("SELF := le;"),"\n")
		xyz<<-paste(xyz,paste("END;"),"\n")
		
		xyz<<-paste(xyz,paste("P := PROJECT(T,add_rank(LEFT,COUNTER));"),"\n")  
		xyz<<-paste(xyz,paste("RS := RECORD"),"\n")
		xyz<<-paste(xyz,paste("Seq := MIN(GROUP,P.pos);"),"\n")
		xyz<<-paste(xyz,paste("P.number;"),"\n")
		xyz<<-paste(xyz,paste("P.field;"),"\n")
		xyz<<-paste(xyz,paste("END;"),"\n")
		
		xyz<<-paste(xyz,paste("Splits := TABLE(P,RS,number,field,FEW);"),"\n")
		xyz<<-paste(xyz,paste("TYPEOF(T) to(P le,Splits ri) := TRANSFORM"),"\n")
		xyz<<-paste(xyz,paste("SELF.pos := 1+le.pos - ri.Seq;"),"\n")
		xyz<<-paste(xyz,paste("SELF := le;"),"\n")
		xyz<<-paste(xyz,paste("END;"),"\n")
		
		xyz<<-paste(xyz,paste("outfile := JOIN(P,Splits,LEFT.number=RIGHT.number,to(LEFT,RIGHT),LOOKUP);"),"\n")
		xyz<<-paste(xyz,paste("n := COUNT(DSRMAX);"),"\n")
		
		xyz<<-paste(xyz,paste("MedRec := RECORD"),"\n")
		xyz<<-paste(xyz,paste("outfile.number;"),"\n")
		xyz<<-paste(xyz,paste("SET OF UNSIGNED poso := IF(n%2=0,[n/2,n/2 + 1],[(n+1)/2]);"),"\n")
		xyz<<-paste(xyz,paste("END;"),"\n")
		
		xyz<<-paste(xyz,paste("MyT := TABLE(outfile,MedRec,field,number);"),"\n")
		xyz<<-paste(xyz,paste("MedianValues:=JOIN(outfile,MyT,LEFT.number=RIGHT.number AND LEFT.pos IN RIGHT.poso);"),"\n")
		xyz<<-paste(xyz,paste("medianRec := RECORD"),"\n")
		xyz<<-paste(xyz,paste("MedianValues.field;"),"\n")
		xyz<<-paste(xyz,paste("Median := AVE(GROUP, MedianValues.value);"),"\n")
		xyz<<-paste(xyz,paste("END;"),"\n")
		
		xyz<<-paste(xyz,paste(out.dataframe,":= TABLE(MedianValues,medianRec,field);"),"\n")
		xyz <<-strim(paste(xyz,hpcc.output(out.dataframe,output)))
	}
hpcc.min <-
	function(dataframe,fields,out.dataframe,output=20,spray=FALSE,submit=TRUE){
		
		strim<-function (dataframe)
		{
			gsub("^\\s+|\\s+$", "", dataframe)
		}
		
		varlst<<-strsplit(fields, ",")
		str1<-NULL
		str2<-NULL
		for (i in 1:length(varlst[[1]]))
		{
			k<-strim(varlst[[1]][i])
			h<<-strsplit(k," ")
			if (i > 1)
			{
				charh<-paste("'",h[[1]][1],"'",sep="")
				str1<-strim(paste(str1,charh,sep=","))
				hh<-strim(paste("LEFT.",h[[1]][1],sep=""))
				str2<-strim(paste(str2,hh,sep=","))
			}
			else
			{
				charh<-paste("'",h[[1]][1],"'",sep="")
				str1<-strim(paste(str1,charh))
				hh<-strim(paste("LEFT.",h[[1]][1],sep=""))
				str2<-strim(paste(str2,hh))
			}
			
		}
		
		xyz<<-paste(xyz,paste("recmin :=RECORD"),"\n")
		xyz<<-paste(xyz,paste("INTEGER3 id;"),"\n")
		xyz<<-paste(xyz,paste(dataframe,";"),"\n")
		xyz<<-paste(xyz,paste("END;"),"\n")
		xyz<<-paste(xyz,paste("recmin mintrans (",dataframe," L, INTEGER C) := TRANSFORM"),"\n")
		xyz<<-paste(xyz,paste("SELF.id :=C;"),"\n")
		xyz<<-paste(xyz,paste("SELF :=L;"),"\n")
		xyz<<-paste(xyz,paste("END;"),"\n")
		xyz<<-paste(xyz,paste("DSRMIN:=PROJECT(",dataframe,",mintrans(LEFT,COUNTER));"),"\n")
		#xyz<<-paste(xyz,paste("DSR;"),"\n")
		xyz<<-paste(xyz,paste("NumField:=RECORD"),"\n")
		xyz<<-paste(xyz,paste("UNSIGNED id;"),"\n")
		xyz<<-paste(xyz,paste("STRING Field;"),"\n")
		xyz<<-paste(xyz,paste("REAL8 value;"),"\n")
		xyz<<-paste(xyz,paste("END;"),"\n")
		xyz<<-paste(xyz,paste("OutDsMin:=NORMALIZE(DSRMIN,",length(varlst[[1]]),",TRANSFORM(NumField,SELF.id:=LEFT.id,SELF.Field:=CHOOSE
							  (COUNTER,",str1,sep="",");SELF.value:=CHOOSE(COUNTER,",str2,")));","\n"))
		#xyz<<-paste(xyz,paste("OutDsMin;"),"\n")
		xyz<<-paste(xyz,paste("SingleField := RECORD"),"\n")
		xyz<<-paste(xyz,paste("OutDSMin.Field;"),"\n")
		xyz<<-paste(xyz,paste("Minval := MIN(GROUP,OutDSMin.value);"),"\n")
		xyz<<-paste(xyz,paste("END;"),"\n")
		xyz<<-paste(xyz,paste(out.dataframe,":= TABLE(OutDSMin,SingleField,Field);"),"\n")
		xyz <<-strim(paste(xyz,hpcc.output(out.dataframe,output)))
	}

hpcc.mode <-
	function(dataframe,fields,out.dataframe,output=20,spray=FALSE,submit=TRUE){
		
		trim<-function (dataframe)
		{
			gsub("^\\s+|\\s+$", "", dataframe)
		}
		
		varlst<<-strsplit(fields, ",")
		str1<-NULL
		str2<-NULL
		for (i in 1:length(varlst[[1]]))
		{
			k<-strim(varlst[[1]][i])
			h<<-strsplit(k," ")
			if (i > 1)
			{
				charh<-paste("'",h[[1]][1],"'",sep="")
				str1<-strim(paste(str1,charh,sep=","))
				hh<-strim(paste("LEFT.",h[[1]][1],sep=""))
				str2<-strim(paste(str2,hh,sep=","))
			}
			else
			{
				charh<-paste("'",h[[1]][1],"'",sep="")
				str1<-strim(paste(str1,charh))
				hh<-strim(paste("LEFT.",h[[1]][1],sep=""))
				str2<-strim(paste(str2,hh))
			}
			
		}
		
		xyz<<-paste(xyz,paste("recmax :=RECORD"),"\n")
		xyz<<-paste(xyz,paste("INTEGER3 id;"),"\n")
		xyz<<-paste(xyz,paste(dataframe,";"),"\n")
		xyz<<-paste(xyz,paste("END;"),"\n")
		xyz<<-paste(xyz,paste("RECMAX maxtrans (",dataframe," L, INTEGER C) := TRANSFORM"),"\n")
		xyz<<-paste(xyz,paste("SELF.id :=C;"),"\n")
		xyz<<-paste(xyz,paste("SELF :=L;"),"\n")
		xyz<<-paste(xyz,paste("END;"),"\n")
		
		xyz<<-paste(xyz,paste("DSRMAX:=PROJECT(",dataframe,",maxtrans(LEFT,COUNTER));"),"\n")
		#xyz<<-paste(xyz,paste("DSRMAX;"),"\n")
		
		xyz<<-paste(xyz,paste("MaxField:=RECORD"),"\n")
		xyz<<-paste(xyz,paste("UNSIGNED id;"),"\n")
		xyz<<-paste(xyz,paste("STRING Field;"),"\n")
		xyz<<-paste(xyz,paste("UNSIGNED4 number;"),"\n")
		xyz<<-paste(xyz,paste("REAL8 value;"),"\n")
		xyz<<-paste(xyz,paste("END;"),"\n")
		xyz<<-paste(xyz,paste("OutDsMode:=NORMALIZE(DSRMAX,",length(varlst[[1]]),",TRANSFORM(MaxField,SELF.id:=LEFT.id,SELF.number:=COUNTER;
							  SELF.Field:=CHOOSE(COUNTER,",str1,sep="",");SELF.value:=CHOOSE(COUNTER,",str2,")));","\n"))
		
		xyz<<-paste(xyz,paste("RankableField := RECORD"),"\n")
		xyz<<-paste(xyz,paste("OutDsMode;"),"\n")
		xyz<<-paste(xyz,paste("UNSIGNED Pos := 0;"),"\n")
		xyz<<-paste(xyz,paste("END;"),"\n")
		
		xyz<<-paste(xyz,paste("T := TABLE(SORT(OutDsMode,Number,field,Value),RankableField);"),"\n")
		
		xyz<<-paste(xyz,paste("TYPEOF(T) add_rank(T le,UNSIGNED c) := TRANSFORM"),"\n")
		xyz<<-paste(xyz,paste("SELF.Pos := c;"),"\n")
		xyz<<-paste(xyz,paste("SELF := le;"),"\n")
		xyz<<-paste(xyz,paste("END;"),"\n")
		
		xyz<<-paste(xyz,paste("P := PROJECT(T,add_rank(LEFT,COUNTER));"),"\n")  
		xyz<<-paste(xyz,paste("RS := RECORD"),"\n")
		xyz<<-paste(xyz,paste("Seq := MIN(GROUP,P.pos);"),"\n")
		xyz<<-paste(xyz,paste("P.number;"),"\n")
		xyz<<-paste(xyz,paste("P.field;"),"\n")
		xyz<<-paste(xyz,paste("END;"),"\n")
		
		xyz<<-paste(xyz,paste("Splits := TABLE(P,RS,number,field,FEW);"),"\n")
		xyz<<-paste(xyz,paste("TYPEOF(T) to(P le,Splits ri) := TRANSFORM"),"\n")
		xyz<<-paste(xyz,paste("SELF.pos := 1+le.pos - ri.Seq;"),"\n")
		xyz<<-paste(xyz,paste("SELF := le;"),"\n")
		xyz<<-paste(xyz,paste("END;"),"\n")
		
		xyz<<-paste(xyz,paste("outfile := JOIN(P,Splits,LEFT.number=RIGHT.number,to(LEFT,RIGHT),LOOKUP);"),"\n")
		xyz<<-paste(xyz,paste("n := COUNT(OutDsMode);"),"\n")
		
		xyz<<-paste(xyz,paste("ModeRec := RECORD"),"\n")
		xyz<<-paste(xyz,paste("outfile.number;"),"\n")
		xyz<<-paste(xyz,paste("outfile.value;"),"\n")
		xyz<<-paste(xyz,paste("outfile.field;"),"\n")
		xyz<<-paste(xyz,paste("outfile.pos;"),"\n")
		xyz<<-paste(xyz,paste("vals := COUNT(GROUP);"),"\n")
		xyz<<-paste(xyz,paste("END;"),"\n")
		
		xyz<<-paste(xyz,paste("MTable := TABLE(outfile,modeRec,number,field,value);"),"\n")
		xyz<<-paste(xyz,paste("newRec := RECORD"),"\n")
		xyz<<-paste(xyz,paste("MTable.number;"),"\n")
		xyz<<-paste(xyz,paste("MTable.value;"),"\n")
		xyz<<-paste(xyz,paste("MTable.field;"),"\n")
		xyz<<-paste(xyz,paste("po := (MTable.pos*Mtable.vals + ((Mtable.vals-1)*Mtable.vals/2))/Mtable.vals;"),"\n")
		xyz<<-paste(xyz,paste("END;"),"\n")
		
		xyz<<-paste(xyz,paste("newTable := TABLE(MTable,newRec);"),"\n")
		xyz<<-paste(xyz,paste("modT := TABLE(MTable,{number,cnt:=MAX(GROUP,vals)},number);"),"\n")
		xyz<<-paste(xyz,paste("Modes:=JOIN(MTable,ModT,LEFT.number=RIGHT.number AND LEFT.vals=RIGHT.cnt);"),"\n")
		xyz<<-paste(xyz,paste("ModesRec := RECORD"),"\n")
		xyz<<-paste(xyz,paste("field := Modes.field;"),"\n")
		xyz<<-paste(xyz,paste("mode := Modes.value;"),"\n")
		xyz<<-paste(xyz,paste("Modes.cnt;"),"\n")
		xyz<<-paste(xyz,paste("END;"),"\n")
		
		xyz<<-paste(xyz,paste(out.dataframe,":= TABLE(Modes,ModesRec);"),"\n")
		xyz <<-strim(paste(xyz,hpcc.output(out.dataframe,output)))
	}
hpcc.new.layout <-
	function (layoutname, prelayout = NULL, newVarType = NULL) 
	{
		strim <<- function(x) {
			gsub("^\\s+|\\s+$", "", x)
			gsub("^,+|,+$", "", x)
		}
		is.not.null <- function(x) !is.null(x)
		if (missing(layoutname)) {
			stop("no layoutname.")
		}
		else {
			if (is.not.null(prelayout)) {
				strt2 <- strim(paste(prelayout, ";", sep = ""))
			}
			else {
				strt2 <- strim(paste(prelayout, sep = ""))
			}
			strt1 <- strim(paste(layoutname, ":=RECORD ", sep = ""))
			strt3 <- strim(paste(newVarType, "END;", sep = ""))
			xyz <<- strim(paste(xyz, paste(strt1, strt2, strt3, sep = "")))
		}
	}

hpcc.project <-
	function (dataframe, calltransfunc, counter = NULL, out.dataframe, output=20,spray=FALSE,
			  prefetch = NULL, lookahead = NULL, parallel = NULL, keyed = NULL, 
			  local = NULL,submit=TRUE) 
	{
		strim <<- function(x) {
			gsub("^\\s+|\\s+$", "", x)
			gsub("^,+|,+$", "", x)
		}
		is.not.null <- function(x) !is.null(x)
		if (missing(dataframe)) {
			stop("no dataframe.")
		}
		else {
			strt1 <- strim(paste(dataframe, sep = ""))
			strt2 <- strim(paste(calltransfunc, "(LEFT,", counter, 
								 sep = ""))
			strt3 <- strim(paste(strt1, strt2, ")", sep = ","))
			strt4 <- strim(paste(strt3, prefetch, sep = ","))
			strt5 <- strim(paste(strt4, lookahead, sep = ","))
			strt6 <- strim(paste(strt5, parallel, sep = ","))
			strt7 <- strim(paste(strt6, keyed, sep = ","))
			strl <- strim(paste(strt7, local, sep = ","))
			code <- strim(paste(out.dataframe, ":=PROJECT(", 
							   strl, sep = "", ");\n"))
			code <- strim(paste(code, hpcc.output(out.dataframe,output)))
			hpcc.submit(code,submit)
		}
	}

hpcc.rollup <-
	function (dataframe, condition = NULL, calltransfunc, out.dataframe, output=20,spray=FALSE,
			  execute = NULL, parallel = NULL, keyed = NULL, local = NULL, submit=TRUE) 
	{
		strim <<- function(x) {
			gsub("^\\s+|\\s+$", "", x)
			gsub("^,+|,+$", "", x)
		}
		is.not.null <- function(x) !is.null(x)
		if (missing(dataframe)) {
			stop("no dataframe")
		}
		else {
			strt1 <- strim(paste(dataframe, sep = ","))
			strt2 <- strim(paste(strt1, condition, sep = ","))
			nnn <- strim(paste(calltransfunc, "(LEFT,RIGHT)", sep = ""))
			strt3 <- strim(paste(strt2, nnn, sep = ","))
			strt4 <- strim(paste(strt3, parallel, sep = ","))
			strt5 <- strim(paste(strt4, keyed, sep = ","))
			strl <- strim(paste(strt5, local, sep = ","))
			xyz <- strim(paste(xyz, paste(out.dataframe, ":=ROLLUP(", 
										  strl, sep = "", ");")))
			xyz <<- strim(paste(xyz, hpcc.output(out.dataframe,output)))
		}
	}

hpcc.rand <-
	function(dataframe,out.dataframe,output=20,spray=FALSE,submit=TRUE){
		
		
		strim<<-function (x)
		{
			gsub("^\\s+|\\s+$", "", x)
			gsub("^,+|,+$", "", x)
		} 
		
		xyz<<-paste(xyz,paste("MyOutRec := RECORD"),"\n")
		xyz<<-paste(xyz,paste("UNSIGNED DECIMAL8_8 rand;"),"\n")
		xyz<<-paste(xyz,paste(dataframe,";"),"\n")
		xyz<<-paste(xyz,paste("end;"),"\n")
		
		xyz<<-paste(xyz,paste("MyOutRec MyTrans(",dataframe," L, UNSIGNED4 C) := TRANSFORM"),"\n")
		xyz<<-paste(xyz,paste("SELF.rand := C/4294967295;"),"\n")
		xyz<<-paste(xyz,paste("SELF := L;"),"\n")
		xyz<<-paste(xyz,paste("end;"),"\n")
		
		
		xyz<<-paste(xyz,paste(out.dataframe,":= project(",dataframe,",MyTrans(LEFT, RANDOM()));"),"\n")
		xyz<<-paste(xyz,hpcc.output(out.dataframe,output),"\n")
		
	}

hpcc.sd <-
	function(dataframe,fields,out.dataframe,output=20,spray=FALSE,submit=TRUE){
		
		strim<-function (dataframe)
		{
			gsub("^\\s+|\\s+$", "", dataframe)
		}
		
		varlst<<-strsplit(fields, ",")
		str1<-NULL
		str2<-NULL
		for (i in 1:length(varlst[[1]]))
		{
			k<-strim(varlst[[1]][i])
			h<<-strsplit(k," ")
			if (i > 1)
			{
				charh<-paste("'",h[[1]][1],"'",sep="")
				str1<-strim(paste(str1,charh,sep=","))
				hh<-strim(paste("LEFT.",h[[1]][1],sep=""))
				str2<-strim(paste(str2,hh,sep=","))
			}
			else
			{
				charh<-paste("'",h[[1]][1],"'",sep="")
				str1<-strim(paste(str1,charh))
				hh<-strim(paste("LEFT.",h[[1]][1],sep=""))
				str2<-strim(paste(str2,hh))
			}
			
		}
		xyz<<-paste(xyz,paste("recsd :=RECORD"),"\n")
		xyz<<-paste(xyz,paste("INTEGER3 id;"),"\n")
		xyz<<-paste(xyz,paste(dataframe,";"),"\n")
		xyz<<-paste(xyz,paste("END;"),"\n")
		xyz<<-paste(xyz,paste("recsd sdtrans (",dataframe," L, INTEGER C) := TRANSFORM"),"\n")
		xyz<<-paste(xyz,paste("SELF.id :=C;"),"\n")
		xyz<<-paste(xyz,paste("SELF :=L;"),"\n")
		xyz<<-paste(xyz,paste("END;"),"\n")
		xyz<<-paste(xyz,paste("DSRSD:=PROJECT(",dataframe,",sdtrans(LEFT,COUNTER));"),"\n")
		#xyz<<-paste(xyz,paste("DSRSD;"),"\n")
		xyz<<-paste(xyz,paste("NumSdField:=RECORD"),"\n")
		xyz<<-paste(xyz,paste("UNSIGNED id;"),"\n")
		xyz<<-paste(xyz,paste("STRING field;"),"\n")
		xyz<<-paste(xyz,paste("REAL8 value;"),"\n")
		xyz<<-paste(xyz,paste("END;"),"\n")
		xyz<<-paste(xyz,paste("OutDsSd:=NORMALIZE(DSRSD,",length(varlst[[1]]),",TRANSFORM(NumSdField,SELF.id:=LEFT.id,SELF.field:=CHOOSE
							  (COUNTER,",str1,sep="",");SELF.value:=CHOOSE(COUNTER,",str2,")));","\n"))
		#xyz<<-paste(xyz,paste("OutDsSd;"),"\n")
		xyz<<-paste(xyz,paste("SingleSdField := RECORD"),"\n")
		xyz<<-paste(xyz,paste("OutDsSd.field;"),"\n")
		xyz<<-paste(xyz,paste("Sd := SQRT(VARIANCE(GROUP,OutDsSd.value));"),"\n")
		xyz<<-paste(xyz,paste("END;"),"\n")
		xyz<<-paste(xyz,paste(out.dataframe,":= TABLE(OutDsSd,SingleSdField,field);"),"\n")
		xyz <<-strim(paste(xyz,hpcc.output(out.dataframe,output)))
	}

hpcc.sort <-
	function (dataframe, fields, out.dataframe, output=20,spray=FALSE,joined = NULL, skew = NULL, 
			  threshold = NULL, few = NULL, joinedset = NULL, limit = NULL, 
			  target = NULL, size = NULL, local = NULL, stable = NULL, 
			  unstable = NULL, algorithm = NULL,submit=TRUE) 
	{
		strim <<- function(x) {
			gsub("^\\s+|\\s+$", "", x)
			gsub("^,+|,+$", "", x)
		}
		is.not.null <- function(x) !is.null(x)
		if (missing(dataframe)) {
			stop("no dataframe.")
		}
		else {
			if (is.not.null(joined)) {
				joinstr <- strim(paste(joined, "(", joinedset, ")", 
									   sep = ""))
			}
			else {
				joinstr <- strim(paste(joined, sep = " "))
			}
			if (is.not.null(skew)) {
				limt <- strim(paste(limit, target, sep = ","))
				skewstr <- strim(paste(skew, "(", limt, ")", sep = ""))
			}
			else {
				skewstr <- strim(paste(skew, sep = " "))
			}
			if (is.not.null(threshold)) {
				threstr <- strim(paste(threshold, "(", size, ")", 
									   sep = ""))
			}
			else {
				threstr <- strim(paste(threshold, sep = " "))
			}
			if (is.not.null(stable)) {
				stabstr <- strim(paste(stable, "(", algorithm, ")", 
									   sep = ""))
			}
			else {
				stabstr <- strim(paste(stable, sep = " "))
			}
			if (is.not.null(unstable)) {
				unstabstr <- strim(paste(unstable, "(", algorithm, 
										 ")", sep = ""))
			}
			else {
				unstabstr <- strim(paste(unstable, sep = " "))
			}
			str1 <- strim(paste(dataframe, fields, joinstr, sep = ","))
			str2 <- strim(paste(str1, skewstr, sep = ","))
			str3 <- strim(paste(str2, threstr, sep = ","))
			str4 <- strim(paste(str3, stabstr, sep = ","))
			str5 <- strim(paste(str4, unstabstr, sep = ","))
			str6 <- strim(paste(str5, local, sep = ","))
			str7 <- strim(paste(str6, few, sep = ","))
			xyz <- strim(paste(xyz, paste(out.dataframe, ":= SORT(", 
										  str7, sep = "", ");")))
			xyz <<- strim(paste(xyz, hpcc.output(out.dataframe,output)))
		}
	}


hpcc.submit <-
	function(code,submit=TRUE) {
		if(!submit)
			return(code)
		xyz <<- paste(xyz,code,sep='')
}


hpcc.table <-
	function (dataframe, format, out.dataframe, output=20,spray=FALSE,expression = NULL, 
			  few = NULL, many = NULL, unsorted = NULL, local = NULL, keyed = NULL, 
			  merge = NULL,submit=TRUE) 
	{
		strim <- function(x) {
			gsub("^\\s+|\\s+$", "", x)
			gsub("^,+|,+$", "", x)
		}
		is.not.null <- function(x) !is.null(x)
		if (missing(dataframe)) {
			stop("no dataframe.")
		}
		else {
			if (is.not.null(expression)) {
				xyz <- NULL
				strexp <- strim(paste(few, many, unsorted, local, 
									  keyed, merge, sep = " "))
				strexps <- strim(gsub(" {2,}", " ", strexp))
				str1 <- strim(gsub(" ", ",", strexps))
				strexp <- strim(paste(expression))
				str2 <- strim(paste(strexp, str1, sep = ","))
				strf <- strim(paste(dataframe, format, str2, sep = ","))
				xyz <<- strim(paste(xyz, paste("outdist := TABLE(", 
											   strf, sep = "", ");")))
			}
			else {
				xyz <- NULL
				strexp <- strim(paste(few, many, unsorted, local, 
									  keyed, merge, sep = " "))
				strexps <- strim(gsub(" {2,}", " ", strexp))
				str1 <- strim(gsub(" ", ",", strexps))
				strf <- strim(paste(dataframe, format, str1, sep = ","))
				xyz <- strim(paste(xyz, paste(out.dataframe, ":= TABLE(", 
											  strf, sep = "", ");")))
				xyz <<- sstrim(paste(xyz, hpcc.output(out.dataframe,output)))
			}
		}
	}

hpcc.transform <-
	function (datastruct = NULL, funcname, parameterlist, skip = NULL, 
			  condition = NULL, locals = NULL,submit=TRUE) 
	{
		strim <<- function(x) {
			gsub("^\\s+|\\s+$", "", x)
			gsub("^,+|,+$", "", x)
		}
		is.not.null <- function(x) !is.null(x)
		if (missing(funcname)) {
			stop("no funcname")
		}
		else {
			if (is.null(datastruct)) {
				stru <- strim(paste("MyRec", skip))
			}
			else {
				stru <- strim(paste(datastruct))
			}
			if (is.not.null(skip)) {
				strn1 <- strim(paste(":= TRANSFORM,", skip))
			}
			else {
				strn1 <- strim(paste(":= TRANSFORM", skip))
			}
			str1 <- strim(paste(stru, funcname, sep = " "))
			strn2 <- strim(paste(strn1, condition, sep = " "))
			xyz <<- strim(paste(xyz, paste(str2, strn2, sep = " ", 
										   ";END;")))
		}
	}
hpcc.filter <- function(data,condition,out.dataframe,output=20,spray=FALSE,submit=TRUE) {
	
	data <- paste(out.dataframe,' := ',data,'(',condition,');\n ',sep="")
	data <- paste(data,hpcc.output(out.dataframe,output),sep='')
	hpcc.submit(data)
}

condition <- function(field,operator,keyword,type="NUM") {
	
	if(type=='STRING') {
		keyword <- keyword(keyword)
	}
	if(type=='SET') {
		keyword <- set(keyword)
	}
	condition <- paste("(",field,operator,keyword,")",sep=" ")
	return(condition)
}

field <- function(name) {
	return(name)
}

set <- function(keyword) {
	if(is.vector(keyword)) {
		if(class(keyword)=="numeric") {
			keyword <- paste(keyword,collapse=',')
		}
		else if(class(keyword)=="character") {
			keyword <- paste(keyword,collapse="','")
			keyword <- paste("'",keyword,"'",sep="")
		}
		else {
			return()
		}
		keyword <- paste("[",keyword,"]",sep="")
		return(keyword)
	}
}

keyword <- function(keyword) {
	if(is.character(keyword)) {
		keyword <- paste("'",keyword,"'",sep="")
	}
	return(keyword)
}

hpcc.percentile <- function(dataset,out.dataframe,...,submit=TRUE) {
	parameters <- list(...)
	if(length(parameters)%%2!=0)
		stop('Parameters are wrong')
	fieldsRaw <- parameters[seq.int(1,length(parameters),2)]
	percentilesRaw <- parameters[seq.int(2,length(parameters),2)]
	value <- ''
	field <- ''
	percents <- ''
	normalize <- ''
	high <- 0
	for(i in 1:length(fieldsRaw)) {
		if(length(percentilesRaw[[i]])>high) 
			high <- length(percentilesRaw[[i]])
		if(i == length(fieldsRaw)){
			value <-  paste(value,"LEFT.",fieldsRaw[[i]],sep='')
			field <- paste(field,"'",fieldsRaw[[i]],"'",sep='')   			
			normalize <- paste(normalize,fieldsRaw[[i]],"P[COUNTER]",sep='')
		}
		else {
			value <- paste(value,'LEFT.',fieldsRaw[[i]],',',sep='',collapse=NULL)
			field <- paste(field,"'",fieldsRaw[[i]],"',",sep='')
			normalize <- paste(normalize,"IF(SELF.field='",fieldsRaw[[i]],"',",fieldsRaw[[i]],'P[COUNTER],',sep='')
		}
		percents <- paste(percents,"SET OF INTEGER ",fieldsRaw[[i]],"P := [0,1,5,10,25,50,75,90,95,99,100",sep='')
		dfg <- paste('',percentilesRaw[[i]],sep='',collapse=',')
		percents <- paste(percents,",",dfg,"];\n",sep='',collapse=NULL)			
	}
	
	for(i in 1:length(fieldsRaw)) {
		normalize <- paste(normalize,")",sep='')
	}
	
	percentile <- paste("NormRec:=RECORD\nINTEGER4 number;\nSTRING field;\nREAL value;\nEND;\n")
	percentile <- paste(percentile, "OutDS:=NORMALIZE(",dataset,",",length(fieldsRaw),",TRANSFORM(NormRec,SELF.number:=COUNTER,SELF.field:=CHOOSE(COUNTER,",field,"),SELF.value:=CHOOSE(COUNTER,",value,")));\n",sep='')
	percentile <- paste(percentile, "RankableField := RECORD\nOutDS;\nUNSIGNED pos:=0;\nEND;\n",sep='')
	percentile <- paste(percentile, "T:=TABLE(SORT(OutDS,field,Value),RankableField);\n",sep='')
	percentile <- paste(percentile, "TYPEOF(T) add_rank(T le, UNSIGNED c):=TRANSFORM\nSELF.pos:=c;\nSELF:=le;\nEND;\n",sep='')
	percentile <- paste(percentile, "P:=PROJECT(T,add_rank(LEFT,COUNTER));\n",sep='')
	percentile <- paste(percentile, "RS:=RECORD\nSeq:=MIN(GROUP,P.pos);\nP.field;\nEND;\n",sep='')
	percentile <- paste(percentile, "Splits := TABLE(P,RS,field,FEW);\n",sep='')
	percentile <- paste(percentile, "TYPEOF(T) to(P le, Splits ri):=TRANSFORM\nSELF.pos:=1+le.pos-ri.Seq;\nSELF:=le;\nEND;\n",sep='')
	percentile <- paste(percentile, "outfile := JOIN(P,Splits,LEFT.field=RIGHT.field, to(LEFT,RIGHT),LOOKUP);\n",sep='')
	
	percentile <- paste(percentile, "N:=COUNT(",dataset,");\n",sep='')
	percentile <- paste(percentile, percents,sep='')
	percentile <- paste(percentile, "Rec1:=RECORD\nSTRING field;\nINTEGER4 percentiles;\nEND;\n",sep='')
	
	percentile <- paste(percentile, "MyTab:=NORMALIZE(Splits,",(11+high),",TRANSFORM(Rec1,SELF.field:=LEFT.field,SELF.percentiles:=",normalize,");\n",sep='')
	
	percentile <- paste(percentile, "PerRec:=RECORD\nMyTab;\nREAL rank:=IF(mytab.percentiles = -1,0," ,
						"IF(mytab.percentiles = 0,1,",
						"IF(ROUND(mytab.percentiles*(N+1)/100)>=N,N,",
						"mytab.percentiles*(N+1)/100)));\nEND;\n",sep='')
	percentile <- paste(percentile, "valuestab := TABLE(mytab,perRec);\n",sep='')
	percentile <- paste(percentile, "rankRec := RECORD\nSTRING field := valuestab.field;\nREAL rank := valuestab.rank;\nINTEGER4 intranks;\nREAL decranks;\nINTEGER4 plusOneranks;\n",
						"valuestab.percentiles;\nEND;\n",sep='')
	percentile <- paste(percentile, "rankRec tr(valuestab L, INTEGER C) := TRANSFORM\n",
						"SELF.decranks := IF(L.rank - (ROUNDUP(L.rank) - 1) = 1,0,L.rank - (ROUNDUP(L.rank) - 1));\n",						  
						"SELF.intranks := IF(ROUNDUP(L.rank) = L.rank,L.rank,(ROUNDUP(L.rank) - 1));\n",
						"SELF.plusOneranks := SELF.intranks + 1;\n",
						"SELF := L;\n",
						"END;\n",sep='')
	percentile <- paste(percentile, "ranksTab := PROJECT(valuestab,tr(LEFT,COUNTER));\n",sep='')
	percentile <- paste(percentile, "ranksRec := RECORD\nSTRING field;\nranksTab.decranks;\nranksTab.percentiles;\nINTEGER4 ranks;\nEND;\n",sep='')
	percentile <- paste(percentile, "rankTab := NORMALIZE(ranksTab,2,TRANSFORM(ranksRec,SELF.field := LEFT.field; SELF.ranks := CHOOSE(COUNTER,LEFT.intranks,LEFT.plusOneranks),SELF := LEFT));\n",sep='')
	percentile <- paste(percentile, "MTable:=SORT(JOIN(rankTab, outfile, LEFT.field = RIGHT.field AND LEFT.ranks = RIGHT.pos),field,percentiles,ranks);\n",sep='')
	percentile <- paste(percentile, "MyTable := DEDUP(MTable, LEFT.percentiles = RIGHT.percentiles AND LEFT.ranks = RIGHT.ranks AND LEFT.field = RIGHT.field);\n",sep='')
	percentile <- paste(percentile, "MyTable RollThem(MyTable L, MyTable R) := TRANSFORM\n",
						"SELF.value := L.value + L.decranks*(R.value - L.value);\n",
						"SELF := L;\n",
						"END;\n",sep='')
	percentile <- paste(percentile,out.dataframe, ":= ROLLUP(MyTable, LEFT.percentiles = RIGHT.percentiles AND LEFT.field=RIGHT.field, RollThem(LEFT,RIGHT));\n",sep='')
	percentile <- paste(percentile,'OUTPUT(TABLE(',out.dataframe,",{field,percentiles,value}),NAMED('",out.dataframe,"'));\n",sep='')
	hpcc.submit(code=percentile)
}

hpcc.join <- function(Dataset1,Dataset2,joinCondition,fields,fieldNames=NULL,type='Inner',out.dataframe,output=20) {
	
	record <-''
	transform <- ''
	transformArg <- ''
	if(fields==Dataset1||fields=='LEFT') {
		transformArg <- 'TRANSFORM(LEFT)'
	}
	else if(fields==Dataset2||fields=='RIGHT') {
		transformArg <- 'TRANSFORM(RIGHT)'
	}
	else {
		recordName <- paste('record',Dataset1,Dataset2,out.dataframe,sep='')
		record <- paste(recordName,' := RECORD\n',sep='')
		for(i in seq_along(fields)) {
			record <- paste(record,'TYPEOF(',fields[i],') ','Field',i,';\n',sep='',collapse='')
		}
		record <- paste(record,'END;\n',sep='',collapse='')
		transFormName <- paste('transform',Dataset1,Dataset2,out.dataframe,sep='')
		transformArg <- paste(transFormName,'(LEFT,RIGHT)',sep='')
		transform <- paste(recordName,' ',transFormName,'(RECORDOF(',Dataset1,') ',Dataset1,',RECORDOF(',Dataset2,') ',Dataset2,') := TRANSFORM\n',sep='')
		for(i in seq_along(fields)) {
			x <- paste('SELF.Field',i,' := ',fields[i],';\n',sep='')
			if(!is.null(fieldNames)) {
				x <- paste('SELF.',fieldNames[i],' := ',fields[i],';\n',sep='')
			}
			transform <- paste(transform,x,sep='')
		}
		transform <- paste(transform,'END;\n',sep='',collapse='')
		
	}
	
	joinArg <- gsub(pattern=paste(Dataset1,'.',sep=''),replacement='LEFT.',x=joinCondition)
	joinArg <- gsub(pattern=paste(Dataset2,'.',sep='',collapse=''),replacement='RIGHT.',x=joinArg)
	
	join <- paste(out.dataframe,' := JOIN(',Dataset1,',',Dataset2,',',joinArg,',',transformArg,',',type,');\n',sep='')
	out <- hpcc.output(out.dataframe,output)
	
	code <- paste(record,transform,join,out,sep='',collapse='')
	hpcc.submit(code=code)
}


hpcc.string.replace <- function(out.dataframe, nameOfString,existingPattern,newPattern,constant=TRUE,submit=TRUE) {
	code <- paste(out.dataframe,' := Std.Str.FindReplace(',nameOfString,",'",existingPattern,"','",newPattern,"');\n",sep='')
	hpcc.submit(code,submit)
}

hpcc.trim <-  function(out.dataframe, nameOfString,LEFT=FALSE,RIGHT=FALSE,ALL=FALSE,submit=TRUE) {
	d <- ''
	if(LEFT)
		d<-paste(d,',LEFT')
	if(RIGHT)
		d<-paste(d,',RIGHT')
	if(ALL)
		d<-paste(d,',ALL')
	code <- paste(out.dataframe,' := TRIM(',nameOfString,',',d,sep='')
	hpcc.submit(code,submit)
}

hpcc.string.find <- function(out.dataframe,nameOfString,patternToFind,constant=TRUE,instance,output=20,submit=TRUE) {
	code <- paste(out.dataframe,' := Std.Str.Find(',nameOfString,",'",patternToFind,"',",instance,');\n',sep='')
	code <- paste(code,hpcc.output(out.dataframe='code',noOfRecordsNeed=output))
	hpcc.submit(code,submit)
}

hpcc.define.record <- function(recordName,argsTypes,argNames,submit=TRUE) {
	code <- paste(recordName,' := RECORD\n')
	for(i in seq_along(argNames)) {
		code <- paste(code,argsTypes[i],' ',argNames[i],';\n')
	}
	code <- paste(code,'END;\n')
	hpcc.submit(code[1],submit)
}

hpcc.define.transform <- function(returnType,transformName,argTypes,argNames,...,submit=TRUE) {
	code <- paste(returnType,' ',transformName,'(')
	s <- list(...)
	for(i in seq_along(argNames)) {
		i
		code <- paste(code,argTypes[i],' ',argNames[i],sep='',collapse=NULL)
		if(i==length(argNames))
			break
		code <- paste(code,',',sep='')
	}
	code <- paste(code,') := TRANSFORM\n',sep='')
	for(i in seq_along(s)) {
		code <- paste(code,s[[i]],'\n',sep='')
	}
	code <- paste(code,'END;\n',sep='')
	hpcc.submit(code[1],submit)
	
}

# hpcc.project <- function(out.dataframe,in.dataframe,condition)

hpcc.count <- function(out.dataframe,in.data,output=20) {
	code <- paste(out.dataframe,' := COUNT(',in.data,');\n');
	code <- paste(code,hpcc.output(out.dataframe,noOfRecordsNeed=output))
	hpcc.submit(code)
}

