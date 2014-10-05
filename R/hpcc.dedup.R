hpcc.dedup <-
	function (dataframe, condition = NULL, all = FALSE, 
			  hash = NULL, keep = 1, keeper = 0, local = FALSE) 
	{
		out.dataframe <- hpcc.get.name()
		code <- sprintf("%s := DEDUP(%s",out.dataframe,dataframe)
		if(is.not.null(condition)) {
			code <- sprintf("%s,%s",condition)
			if(all) {
				code <- sprintf("%s,ALL",code)
				if(is.not.null(hash)) {
					code <- sprintf('%s,%s',code,hash)
				}
			}
			else if(keep!=1 & is.numeric(keep)) {
				code <- sprintf("%s,KEEP %s",code,keep)
			}
			if(keeper=='RIGHT' || keeper==1) {
				code <- sprintf("%s,RIGHT",code)
			}
			if(local) {
				code <- sprintf("%s,%s",code,local)
			}
			
		}
		code <- sprintf("%s);\n",code)
		hpcc.submit(code)
		return(out.dataframe)
	}