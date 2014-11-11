hpcc.string.replace <- function(nameOfString,existingPattern,newPattern,constant=TRUE) {
	out.dataframe <- hpcc.get.name()
	hpcc.import("Std")
	code <- paste(out.dataframe,' := Std.Str.FindReplace(',nameOfString,",'",existingPattern,"','",newPattern,"');\n",sep='')
	hpcc.submit(code,submit)
}
