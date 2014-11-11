hpcc.count <- function(dataset) {

	out.dataframe <- hpcc.get.name()
	code <- paste(out.dataframe,' := COUNT(',in.data,');\n');
	hpcc.submit(code)
	return(out.dataframe)
}

