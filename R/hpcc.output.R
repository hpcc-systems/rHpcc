hpcc.output <- function (out.dataframe,noOfRecordsNeed,download=FALSE) {
	functionArgs <- (as.list(match.call()))
	outX <- as.character(functionArgs$out.dataframe)
	out <- paste("OUTPUT(", out.dataframe,"[1..",noOfRecordsNeed,"],named('", outX, "'));\n", sep = "")
	if(noOfRecordsNeed==0)
		out <- ''
	if(download) {
# 		.eclQuery <<- sprintf("import STD;\n%s",.eclQuery)
		.hpcc.import(import = "STD")
		out1 <- paste("OUTPUT(", out.dataframe,",,'~rhpcc::",outX, "',CSV(HEADING(SINGLE)));\n", sep = "")
		despray <- paste("Std.File.Despray('~rhpcc::",outX,"','",.hpccHostName,"','/var/lib/HPCCSystems/mydropzone/",outX,".csv',,,,TRUE);\n",sep='')
		out <- paste(out1,despray,out,sep='')
		out <- paste(out,"STD.File.DeleteLogicalFile('~rhpcc::",outX,"');\n",sep='')
		numbas <- as.character(dim(.hpccSessionVariables)[1]+1)
		.hpccSessionVariables <<- rbind(.hpccSessionVariables,c(numbas,outX,0,0))
	}
	.hpcc.submit(out)
}



.hpcc.submit <-
	function(code,submit=TRUE) {
		if(!submit)
			return(code)
		.eclQuery <<- paste(.eclQuery,code,sep='')
	}
