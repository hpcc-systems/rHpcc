hpcc.formURL <- function(fileName) {
	url <- paste("http://",hpccHostName,":",hpccPort,"/FileSpray/DownloadFile?Name=",fileName,"&NetAddress=192.168.213.128&Path=/var/lib/HPCCSystems/mydropzone/&OS=1",sep='')
	return(url)
}


hpcc.showFilesToDownload <- function() {
	print('Below are the files to ownload')
	print(hpccSessionVariables)
	a <- TRUE
	while(a) {
		inp <- readline(prompt="Type the number of the file To Download or 0 to exit : ")
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
			hpcc.outputDownloadedFiles()
		}
		else
			print('Invalid Input')
	}
}

hpcc.outputDownloadedFiles <- function() {
	print(hpccDownloadedFiles)
		inp <- readline(prompt="Type the number of the file To Download or 0 to exit : ")
		if(inp>0 & inp<=length(hpccDownloadedFiles)/2) {
			numb <- as.numeric(inp)
			nameOfFile <- paste(hpccDownloadedFiles[numb,2],'.csv',sep='')
			read.table(nameOfFile)
			inp <- readline(prompt = 'Do you want to plot the data set(Y/N) : ')

		}
		else if(inp!=0)
			print('Invalid Input')
}


hpcc.downloadFile <- function(url,fileName) {
	
	sd <- gsub('/','\\\\',getwd())
	print(sd)
	dest <- paste(sd,'\\rhpcc\\downloads\\',fileName,sep='')
	download.file(url,dest)
	print(paste('File Downloaded at ',dest))
}