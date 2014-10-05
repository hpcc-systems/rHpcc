hpcc.join <- function(Dataset1,Dataset2,joinCondition,fields,fieldNames=NULL,type='Inner') {
	
	out.dataframe <- hpcc.get.name()
	record <-''
	transform <- ''
	transformArg <- ''
	if(fields==Dataset1||fields=='LEFT') {
		transformArg <- 'TRANSFORM(LEFT)'
	}
	else if(fields==Dataset2||fields=='RIGHT') {
		transformArg <- 'TRANSFORM(RIGHT)'
	}
	else {
		recordName <- hpcc.get.name()
		record <- paste(recordName,' := RECORD\n',sep='')
		for(i in seq_along(fields)) {
			record <- paste(record,'TYPEOF(',fields[i],') ','Field',i,';\n',sep='',collapse='')
		}
		record <- paste(record,'END;\n',sep='',collapse='')
		transFormName <- hpcc.get.name()
		transformArg <- paste(transFormName,'(LEFT,RIGHT)',sep='')
		transform <- paste(recordName,' ',transFormName,'(RECORDOF(',Dataset1,') ',Dataset1,',RECORDOF(',Dataset2,') ',Dataset2,') := TRANSFORM\n',sep='')
		for(i in seq_along(fields)) {
			x <- paste('SELF.Field',i,' := ',fields[i],';\n',sep='')
			if(!is.null(fieldNames)) {
				x <- paste('SELF.',fieldNames[i],' := ',fields[i],';\n',sep='')
			}
			transform <- paste(transform,x,sep='')
		}
		transform <- paste(transform,'END;\n',sep='',collapse='')
		
	}
	
	join <- paste(out.dataframe,' := JOIN(',Dataset1,',',Dataset2,',',joinArg,',',transformArg,',',type,');\n',sep='')
	
	code <- paste(record,transform,join,out,sep='',collapse='')
	hpcc.submit(code=code)
	return(out.dataframe)
}
