hpcc.iterate <-
	function (dataframe, calltransfunc, local = FALSE) 
	{
		if (missing(dataframe)) {
			stop("no dataframe.")
		}
		out.dataframe <- hpcc.get.name()
		code <- sprintf("%s := ITERATE(%s,%s(LEFT,RIGHT)",out.dataframe,dataframe,calltransfunc)
		if(local) {
			code <- sprintf("%s,LOCAL",code)
		}
		code <- sprintf("%s)",code)
		hpcc.submit(code)
		return(out.dataframe)
	}
