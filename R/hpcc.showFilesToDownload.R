.hpcc.formURL <- function(fileName) {
	url <- paste("http://",.hpccHostName,":",.hpccPort,"/FileSpray/DownloadFile?Name=",fileName,"&NetAddress=",.hpccHostName,"&Path=/var/lib/HPCCSystems/mydropzone/&OS=1",sep='')
	return(url)
}



hpcc.showFilesToDownload <- function() {
	if(dim(.hpccSessionVariables)[1]==0) {
		print("No Files To download")
		return()
	}
	print('Below are the files available for download')
	for(i in seq(1,dim(.hpccSessionVariables)[1])) {
# 		print(paste(as.numeric(.hpccSessionVariables[i,1]),'--',.hpccSessionVariables[i,2],sep="  "))
		print(.hpccSessionVariables[i,2])
		
	}
	inp <- 1
	while(inp!=0) {
		inp <- readline(prompt="Type the number of the file To Download or 0 to exit : ")
		inp <- as.numeric(inp)
		if(inp>0 & inp<=dim(.hpccSessionVariables)[1]) {
			numb <- as.numeric(inp)
			if(.hpccSessionVariables[numb,3]>0)
				print('File already downloaded:')
			else {
				nameOfFile <- paste(.hpccSessionVariables[numb,2],'.csv',sep='')
				url <- .hpcc.formURL(nameOfFile)
				print(url)
				.hpcc.downloadFile(url,nameOfFile)
				numberOfDown <- sum(as.numeric(.hpccSessionVariables[,3]>0))+1 
				.hpccSessionVariables[numb,3] <- numberOfDown
			}
		}
		else if(inp!=0)
			print('Invalid Input')
	}
}



hpcc.convertFilesToFFObjects <- function() {
	if(sum(as.numeric(.hpccSessionVariables[,3]))==0) {
		print("No Files are downloaded in your system")
		return()
	}
	print('Below are the files downloaded into your System ')
	
	downloadedFiles <- .hpccSessionVariables(.hpccSessionVariables[,3]>0)
	for(i in seq(1,length(downloadedFiles))) {
		print(paste(downloadedFiles[i,3],'--',downloadedFiles[i,2],sep="  "))
	}
	inp <- 1
	while(inp>0) {
		inp <- readline(prompt="Type the file number indicated above to convert that file or 0 to exit : ")
		inp <- as.numeric(inp)
		if(inp>0 & inp<=dim(downloadedFiles)[1]) {
			numb <- as.numeric(inp)
			if(.hpccSessionVariables[numb,3]==1)
				print('File already Converted:')
			else {
				nameOfFile <- paste(.hpccSessionVariables[numb,2],'.csv',sep='')
				url <- .hpcc.formURL(nameOfFile)
				print(url)
				.hpcc.downloadFile(url,nameOfFile)
				.hpccSessionVariables[numb,4]=1
			}
		}
		else if(inp!=0) {
			print('Invalid Input')
		}
	}
}



.hpcc.downloadFile <- function(url,fileName) {
	ds <- rawToChar(as.raw(92))
	ds <- sprintf('%s%s',ds,ds)
	sd <- gsub('/',ds,getwd())
	print(sd)
	dest <- paste(sd,rawToChar(as.raw(92)),'rhpcc',rawToChar(as.raw(92)),
				  'downloads',rawToChar(as.raw(92)),fileName,sep='')
	download.file(url,dest)
	print(paste('File Downloaded at ',dest))
}
