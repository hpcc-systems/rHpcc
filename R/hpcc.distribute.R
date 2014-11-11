hpcc.distribute <-
	function (dataframe, expression = NULL, index = NULL, skew = NULL, sorts = NULL, 
			  joincondition = NULL, maxskew = NULL, skewlimit = NULL) 
	{
		if(missing(dataframe)) {
			stop('Data Frame Missing')
		}
		out.dataframe = hpcc.get.name()
		code <- sprintf("%s := DISTRIBUTE(%s",out.dataframe,dataframe)
		if(is.not.null(expression)) {
			code <- sprintf("%s,%s",code,expression)
			if(is.not.null(sorts)) {
				code <- sprintf("%s,MERGE(%s)",code,sorts)
			}
		}
		else if(is.not.null(index)) {
			code <- sprintf("%s,%s",code,index)
			if(is.not.null(joincondition)) {
				code <- sprintf("%s,%s",code,joincondition)
			}
		}
		else if(is.not.null(maxskew)) {
			code <- sprintf("%s,SKEW(%s",code,maxskew)
			if(is.not.null(joincondition)) {
				code <- sprintf("%s,%s",code,joincondition)
			}
			code <- sprintf("%s)",code)
		}
		code <- sprintf("%s)",code)
		hpcc.submit(code)
		return(out.dataframe)
	}
