hpcc.string.find <- function(nameOfString,patternToFind,constant=TRUE,instance,output=20,submit=TRUE) {
	out.dataframe <- hpcc.get.name()
	hpcc.import("Std")
	code <- paste(out.dataframe,' := Std.Str.Find(',nameOfString,",'",patternToFind,"',",instance,');\n',sep='')
	code <- paste(code,hpcc.output(out.dataframe='code',noOfRecordsNeed=output))
	hpcc.submit(code,submit)
}
