library(RPostgreSQL)

# Connect to database
#Update user and dbname
conn = dbConnect(dbDriver("PostgreSQL"), 
                 user="username", 
                 password="", 
                 host="localhost", 
                 port=5432, 
                 dbname="databasename")

#Set directory to avoid having to use paste to build urls
setwd("~/Downloads/datafeed/servercalls")

#Set column headers for server calls from file
#Set path to column headers
column_headers <- read.delim("~/Downloads/datafeed/lookup/column_headers.tsv", stringsAsFactors=FALSE)


#Loop over entire list of files
#Setting colClasses to character only way to guarantee all data loads
#File formats or implementations can change over time; fix schema in database after data loaded
for(file in list.files()){
  print(file)
  df <- read.csv2(file, sep = "\t", header = FALSE, stringsAsFactors = FALSE, colClasses = "character", quote="", fill=FALSE)
  
  #Set the column headers in data frame and therefore also DB
  names(df) <- names(column_headers)
  
  dbWriteTable(conn, name = 'servercalls', value = df, row.names=FALSE, append = TRUE)
  rm(df)
}

#Run analyze in PostgreSQL so that query planner has accurate information
dbGetQuery(conn, "analyze servercalls")