hpcc.define.transform <- function(returnType,argTypes,argNames,...,submit=TRUE) {
	transformName <- hpcc.get.name()
	code <- paste(returnType,' ',transformName,'(')
	s <- list(...)
	for(i in seq_along(argNames)) {
		i
		code <- paste(code,argTypes[i],' ',argNames[i],sep='',collapse=NULL)
		if(i==length(argNames))
			break
		code <- paste(code,',',sep='')
	}
	code <- paste(code,') := TRANSFORM\n',sep='')
	for(i in seq_along(s)) {
		code <- paste(code,s[[i]],'\n',sep='')
	}
	code <- paste(code,'END;\n',sep='')
	hpcc.submit(code[1])
	return(transformName)
	
}
