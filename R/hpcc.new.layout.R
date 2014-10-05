hpcc.new.layout <-
	function (layoutname, prelayout = NULL, newVarType = NULL) 
	{
		strim <<- function(x) {
			gsub("^\\s+|\\s+$", "", x)
			gsub("^,+|,+$", "", x)
		}
		is.not.null <- function(x) !is.null(x)
		if (missing(layoutname)) {
			stop("no layoutname.")
		}
		else {
			str2 <- NULL 
			if (is.not.null(prelayout)) {
				strt2 <- strim(paste(prelayout, ";", sep = ""))
			}
			strt1 <- strim(paste(layoutname, ":=RECORD ", sep = ""))
			strt3 <- strim(paste(newVarType, "END;", sep = ""))
			xyz <<- strim(paste(xyz, paste(strt1, strt2, strt3, sep = "")))
		}
	}