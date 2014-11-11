hpcc.sort <-
	function (dataframe, fields,joined = NULL, skew = NULL, 
			  threshold = NULL, few = NULL, joinedset = NULL, limit = NULL, 
			  target = NULL, size = NULL, local = NULL, stable = NULL, 
			  unstable = NULL, algorithm = NULL) 
	{
		out.dataframe <- hpcc.get.name()
		strim <- function(x) {
			gsub("^\\s+|\\s+$", "", x)
			gsub("^,+|,+$", "", x)
		}
		is.not.null <- function(x) !is.null(x)
		if (missing(dataframe)) {
			stop("no dataframe.")
		}
		else {
			if (is.not.null(joined)) {
				joinstr <- strim(sprintf("%s(%s)",joined,joinedset))
		}
		else {
			joinstr <- strim(joined)
		}
		if (is.not.null(skew)) {
			limt <- strim(sprintf("%s,%s",limit, target))
			skewstr <- strim(sprintf("%s(%s)",skew,limt))
		}
		else {
			skewstr <- strim(skew)
		}
		if (is.not.null(threshold)) {
			threstr <- strim(sprintf("%s(%s)",threshold,size))
		}
		else {
			threstr <- strim(threshold)
		}
		if (is.not.null(stable)) {
			stabstr <- strim(sprintf("%s(%s)",stable,algorithm))
		}
		else {
			stabstr <- strim(stable)
		}
		if (is.not.null(unstable)) {
			unstabstr <- strim(pastesprintf("%s(%s)",unstable,algorithm))
			
		}
		else {
			unstabstr <- strim(unstable)
		}
		str1 <- strim(sprintf("%s,%s,%s",dataframe, fields, joinstr))
		str2 <- strim(sprintf("%s,%s",str1, skewstr))
		str3 <- strim(sprintf("%s,%s",str2, threstr))
		str4 <- strim(sprintf("%s,%s",str3, stabstr))
		str5 <- strim(sprintf("%s,%s",str4, unstabstr))
		str6 <- strim(sprintf("%s,%s",str5, local))
		str7 <- strim(sprintf("%s,%s",str6, few))
		xyz <- strim(sprintf("%s := SORT(%s);", out.dataframe, str7))
		
		xyz <- sprintf("%s := SORT(%s,%s);",out.dataframe,dataframe,fields)
		hpcc.submit(xyz)
		return(out.dataframe)
	}
}
