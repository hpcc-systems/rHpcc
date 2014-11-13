.hpcc.get.name <- function() {
	.noOfEclVariables <<- .noOfEclVariables +1
	name <- paste('r2EclVariable',.noOfEclVariables,sep='')
	return(name)
}


.hpcc.import <- function (import) {
	if(missing(import)) {
		return()
	}
	import <- toupper(import)
	if(!(import %in% .hpccImport)) {
		.hpccImport <<-append(.hpccImport,import)
	}
}

hpcc.begin <-function () {
	.hpccSessionVariables <<- matrix(data=character(0),ncol=4)
	.hpccImport <<- character(0)
	.eclQuery <<- ""
	.noOfEclVariables <<- 0
	.hpcc.get.url()
}

.hpcc.get.url <- function() {
	fileout<-getwd()
	libFolderPaths<-.libPaths()
	hostpropertiesPath <- NULL
	str1<-''
	packageName <- getPackageName()
	i<-0
	config <-NULL
	if(length(libFolderPaths) > 0) {
		for(i in 1 : length(libFolderPaths)) {
			str1 <- paste(libFolderPaths[i], "/", packageName, "/RConfig.yml", sep="");
			print(str1)
			if(file.exists(str1)) {
				config <- yaml.load_file(str1)
				break
			}
		}
	}
	if(is.null(config))
		stop("Can't fing RConfig.yml")
	ecl_direct <- config$hpcchost$ecl_direct
	hostFileName <- config$hpcchost$filename
	str1 <- paste(libFolderPaths[i], "/", packageName, "/", hostFileName, sep="");
	if(file.exists(str1)) {
		hostpropertiesPath <- str1
	}
	if(is.null(hostpropertiesPath)) {
		print(paste('Host Properties File is missing in ',libFolderPaths))
		inp <- readline(prompt="Do you want to input the details here(Y/N) : ")
		if(inp=='Y') {
			.hpccHostName <<- readline(prompt="Enter the Host Adress(Ex : 111.111.11.11) : ")
			hpccProtocol <- readline(prompt="Enter the Protocol (http/https) : ")
			if(hpccProtocol=='')
				hpccProtocol <- 'http'
			.hpccPort <<- readline(prompt="Enter the Port(Ex : 8010) : ")
# 			hpccUsername <- readline('Enter the username(if the server needs authentication) : ')
# 			if(hpccUsername!='')
# 				hpccUserPwd <- readline('Enter the password of the account if it needs any authentication : ')
			.hpccClustername <<- readline("Enter the Cluster Name : ")
			hpcc <- list(hpcc=list(hostName = .hpccHostName,protocol = hpccProtocol,port_thor=.hpccPort,clustername=.hpccClustername))
			hpcc <- as.yaml(hpcc)
			file.create(str1)
			fileCon <- file(str1)
			writeLines(hpcc, fileCon)
			close(fileCon)			
		}
		else {
			return()
		}
	}
	else {
		obj_hpccHost_prop <- yaml.load_file(hostpropertiesPath)
		.hpccHostName <<- obj_hpccHost_prop$hpcc$host
		hpccProtocol <- obj_hpccHost_prop$hpcc$protocol
		.hpccPort <<- obj_hpccHost_prop$hpcc$port_thor
		hpccUsername <- obj_hpccHost_prop$hpcc$username
		.hpccClustername <<- obj_hpccHost_prop$hpcc$clustername
		
		
	}
	hpccdfu_service <- config$hpcchost$dfu_service
	ecl_direct <- ecl_direct
	
	.uUrlHpcc <<- paste(hpccProtocol,'://',.hpccHostName,":",.hpccPort,"/", hpccdfu_service, sep="")
	.uUrlHpccforEx <<- paste(hpccProtocol,'://',.hpccHostName,":",.hpccPort,"/",ecl_direct,"/RunEcl?ver_=1", sep="")	
}
