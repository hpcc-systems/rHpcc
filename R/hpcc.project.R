hpcc.project <-
	function (dataframe, transfunc=NULL,record=NULL,
			  lookahead = NULL, parallel = FALSE, keyed = FALSE, 
			  local = FALSE) 
	{
		if (missing(dataframe)) {
			stop("no dataframe")
		}
		code <- sprintf("%s := PROJECT(%s",out.dataframe,dataframe)
		if(is.not.null(transfunc)) {
			code <- sprintf("%s,%s",transfunc)
		}
		else if(is.not.null(record)) {
			code <- sprintf("%s,%s",code,record)
		}
		else {
			stop("Either Trans or Record should be mentioned")
		}
		if(is.not.null(lookahead)) {
			code <- sprintf("%s,PREFTECH[(%s",code,lookahead)
			if(parallel) {
				code <- sprintf("%s,PARALLEL",code)
			}
			code <- sprintf('%s)',code)
		}
		if(keyed) {
			code <- sprintf("%s,KEYED",code)
		}
		if(local) {
			code <- sprintf("%s,LOCAL",code)
		}
		code <- sprintf("%s)",code)
		hpcc.submit(code)
		return(out.dataframe)
	}
