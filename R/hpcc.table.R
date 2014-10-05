hpcc.table <-
	function (dataframe, format,expression = NULL, 
			  few = NULL, unsorted = FALSE, local =FALSE, keyed = FALSE, 
			  merge = FALSE) 
	{
		out.dataframe <- hpcc.get.name()
		if (missing(dataframe)) {
			stop("no dataframe")
		}
		code <- sprintf("%s := TABLE(%s,%s",out.dataframe,dataframe,format)
		if(is.not.null(expression)) {
			code <- sprintf("%s,%s",code,expression)
			if(few=='FEW' || few=='few' || few == 'MANY' || few=='many') {
				code <- sprintf("%s,%s",few)
			}
			if(unsorted) {
				code <- sprintf("%s,UNSORTED",code)
			}
		}
		if(local) {
			code <- sprintf("%s,LOCAL",code)
		}
		if(keyed) {
			code <- sprintf("%s,KEYED",code)
		}
		if(merge) {
			code <- sprintf("%s,MERGE",code)
		}
		code <- sprintf("%s)",code)
		hpcc.submit(code)
		return(out.dataframe)
	}
