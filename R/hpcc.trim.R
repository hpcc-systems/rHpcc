hpcc.trim <-  function(nameOfString,LEFT=FALSE,RIGHT=FALSE,ALL=FALSE) {
	d <- ''
	out.dataframe <- hpcc.get.name()
	if(LEFT)
		d<-paste(d,',LEFT')
	if(RIGHT)
		d<-paste(d,',RIGHT')
	if(ALL)
		d<-paste(d,',ALL')
	code <- paste(out.dataframe,' := TRIM(',nameOfString,',',d,sep='')
	hpcc.submit(code)
	return(out.dataframe)
}
