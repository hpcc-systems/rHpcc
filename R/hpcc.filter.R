hpcc.filter <- function(data,condition) {
	
	out.dataframe <- hpcc.get.name()
	data <- paste(out.dataframe,' := ',data,'(',condition,');\n ',sep="")
	hpcc.submit(data)
	return(out.dataframe)
}
