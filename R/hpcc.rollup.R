hpcc.rollup <-
	function (recordset, condition = NULL, transfunc,GROUPED=FALSE, out.dataframe,
			  fieldList = NULL, local = FALSE) 
	{

		if (missing(dataframe)) {
			stop("no dataframe")
		}
		out.dataframe <- hpcc.get.name()
		code <- sprintf("%s := ROLLUP(%s",out.dataframe,recordset)
		a <- TRUE
		if(is.not.null(condition)) {
			code <- sprintf("%s,%s",code,condition)
			a <- FALSE
		}
		else if(GROUPED) {
			code <- sprintf("%s,GROUP")
			a <- FALSE
		}
		code <- sprintf("%s,%s(LEFT,RIGHT)",code,transfunc)
		if(a & is.not.null(fieldList)) {
			code <- sprintf("%s,%s",code,fieldList)
		}
		if(local) {
			code <- sprintf("%s,LOCAL",code)
		}
		code <- sprintf("%s)",code)
	}
