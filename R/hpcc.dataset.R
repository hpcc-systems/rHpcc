hpcc.dataset <- function(logicalfilename,filetype='thor', inlineData, layoutname) {
	outputDataset <- ""
	eclQuery<-''
	outputDatasetName <- .hpcc.get.name()
	if(!missing(logicalfilename)) {
		recordLayout <- hpcc.data.layout(logicalfilename)
		data <- sprintf("'~%s'", logicalfilename)
		recLayoutName <- .hpcc.get.name()
		eclQuery <- sprintf("%s := %s",recLayoutName, recordLayout)
		data <- sprintf("%s, %s, %s", data,recLayoutName,filetype)
	} else if (!missing(inlineData) & !missing(layoutname)) {
		functionArgs <- (as.list(match.call()))
		eclQuery <- sprintf("%s := %s;", as.character(functionArgs$layoutname), layoutname)
		data <- sprintf( "[ %s ], %s", inlineData, as.character(functionArgs$layoutname) )
	} else {
		stop('Arguments are not proper')
	}
	outputDataset <- sprintf("%s := DATASET(%s);\n",outputDatasetName, data)
	eclQuery <- sprintf("%s %s",eclQuery,outputDataset)
	.hpcc.submit(eclQuery)
	return(outputDatasetName)
}
