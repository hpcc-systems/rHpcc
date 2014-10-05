hpcc.formURL <- function(fileName) {
	url <- paste("http://",hpccHostName,":",hpccPort,"/FileSpray/DownloadFile?Name=",fileName,"&NetAddress=",hpccHostName,"&Path=/var/lib/HPCCSystems/mydropzone/&OS=1",sep='')
	return(url)
}


hpcc.showFilesToDownload <- function() {
	if(length(hpccSessionVariables)<2) {
		print("No Files To download")
		return()
	}
	print('Below are the files available for download')
	for(i in seq(1,length(hpccSessionVariables),by=2)) {
		print(paste(hpccSessionVariables[i],'--',hpccSessionVariables[(i+1)],sep="  "))
		i<-i+1
	}
	a <- TRUE
	while(a) {
		inp <- readline(prompt="Type the number of the file To Download or 0 to exit : ")
		inp <- as.numeric(inp)
		if(inp>0 & inp<=length(hpccSessionVariables)/2) {
			numb <- as.numeric(inp)
			if(hpccSessionVariables[numb,2] %in% hpccDownloadedFiles[,2])
				print('File already downloaded:')
			else {
				nameOfFile <- paste(hpccSessionVariables[numb,2],'.csv',sep='')
				url <- hpcc.formURL(nameOfFile)
				print(url)
				hpcc.downloadFile(url,nameOfFile)
				rbind(hpccDownloadedFiles,c((length(hpccDownloadedFiles)/2)+1,hpccSessionVariables[numb,2]))
			}
			
		}
		else if(inp==0) {
			a <- FALSE
		}
		else
			print('Invalid Input')
	}
}



hpcc.downloadFile <- function(url,fileName) {
	ds <- rawToChar(as.raw(92))
	ds <- sprintf('%s%s',ds,ds)
	sd <- gsub('/',ds,getwd())
	print(sd)
	dest <- paste(sd,rawToChar(as.raw(92)),'rhpcc',rawToChar(as.raw(92)),'downloads',rawToChar(as.raw(92)),'fileName',sep='')
	download.file(url,dest)
	print(paste('File Downloaded at ',dest))
}
