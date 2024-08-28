#Load library
library(tidyverse)
library(lubridate)
library(httr)
library(jsonlite)

#### ---------- API stuff

## QuantAQ API:

api_key = 'ALQY1DLRK62KLFI5JBV6RXGO'
serial_number = 'MOD-PM-00042' # Specific to this monitoring device

base_url = "https://api.quant-aq.com/device-api/v1"
accounts_endpoint = '/account'
# data_endpoint = paste0('/',serial_number,'/data')
data_endpoint = '/data'
raw_data_endpoint = '/data/raw'


get_request = function(url, api_key, params = NULL){
  response = httr::GET(
    url = url,
    authenticate(
      user = api_key, password = api_key, type = 'basic'),
    query = params
  )
  return(response)
}


# data_points=5000
get_data = function(serial_number, data_points=NULL, start_date=NULL, end_date = NULL){
  # this will save all the data
  main_data = c()
  
  # adding serial number in endpoint
  data_endpoint_with_serial = paste0('/devices','/', serial_number, data_endpoint)
  
  # data endpoint for a specific defined serial number
  url = paste0(base_url, data_endpoint_with_serial)
  
  # adding date filter if its not null in parameters
  date_filter = NULL
  if (all(!is.null(start_date) || !is.null(end_date))) {
    date_filter = paste0("timestamp_local,ge,",start_date,";timestamp_local,le,",end_date)
  }
  # different parameters we are sending with request
  params = list(
    page=1,limit=data_points,sort="timestamp_local,desc",
    per_page=1000, filter=date_filter)
  
  # for multiple page scrape we add a loop
  index = 1
  repeat{
    response = get_request(url = url, api_key = api_key, params = params)
    response_data = content(response, 'parsed', encoding = 'UTF-8')
    main_data = c(main_data, response_data$data)
    url = response_data$meta$next_url
    if(!is.null(url)){
      index = index + 1
      params$page = index
    }else{
      print('break')
      break()
    }
  }
  print('total no of data points')
  print(length(main_data))
  json_data = toJSON(main_data,auto_unbox = TRUE, pretty = TRUE)
  return(json_data)
}

get_raw_data = function(serial_number, data_points=NULL, start_date=NULL, end_date = NULL){
  # this will save all the data
  main_raw_data = c()
  
  # adding serial number in endpoint
  raw_data_endpoint_with_serial = paste0('/devices','/', serial_number, raw_data_endpoint)
  
  # data endpoint for a specific defined serial number
  url = paste0(base_url, raw_data_endpoint_with_serial)
  
  # adding date filter if its not null in parameters
  date_filter = NULL
  if (all(!is.null(start_date) || !is.null(end_date))) {
    date_filter = paste0("timestamp_local,ge,",start_date,";timestamp_local,le,",end_date)
  }
  # different parameters we are sending with request
  params = list(
    page=1,limit=data_points,sort="timestamp_local,desc",
    per_page=1000, filter=date_filter)
  
  # for multiple page scrape we add a loop
  index = 1
  repeat{
    response = get_request(url = url, api_key = api_key, params = params)
    response_data = content(response, 'parsed', encoding = 'UTF-8')
    main_raw_data = c(main_raw_data, response_data$data)
    url = response_data$meta$next_url
    if(!is.null(url)){
      index = index + 1
      params$page = index
    }else{
      print('break')
      break()
    }
  }
  print('total no of data points')
  print(length(main_raw_data))
  json_data = toJSON(main_raw_data, auto_unbox = TRUE, pretty = TRUE)
  return(json_data)
}


##### -----  Below is the action data pull and munging


wk <- read_csv("https://raw.githubusercontent.com/bardcesh/Mount-Saint-Mary/main/data/full_newburgh.csv", col_types = "cddc") 


wk <- wk %>%
  mutate(
    YMD = ymd_hms(YMD)
  )

# Find latest date in .csv file
max_date <- max(wk$YMD)

# Do a call to API to get rest of data
# making time floor to start of hour so that it does not add partial hours to the spreadhseet
data = get_data(serial_number =  serial_number, data_points=NULL, start_date=max_date, end_date = floor_date(now("EST"), unit = "hours"))
recent_data <- jsonlite::fromJSON(data) %>%
  # Need to make this section resilient to there being no new datapoints 
  select( #selects certain variables from dataset 
    timestamp_local, pm25, pm10
  ) %>%
  rename( # Renames them so that they display nicely in plots
    PM2.5 = pm25,
    PM10 = pm10
  ) 


recent_data$timestamp_local <- ymd_hms(recent_data$timestamp_local)

recent_data <- recent_data %>%
  rename(
    YMD = timestamp_local
  )  %>%
  mutate(
    PM2.5 = as.numeric(PM2.5),
    PM10 = as.numeric(PM10)
  )

# Compress to hourly

recent_data_h <- recent_data %>%
  group_by(YMD = cut(YMD, breaks="60 min")) %>%
  summarize(
    PM2.5 = mean(PM2.5,na.rm = TRUE),
    PM10 = mean(PM10,na.rm = TRUE)
  ) %>%
  mutate(
    YMD = ymd_hms(YMD)
  )








# create a new row of data, with todayd's date and urls number
#row <- data.frame(Sys.time(), recent_data$PM2.5)

#print(row)

# append at the end of the csv the new data
write_csv(recent_data_h,paste0('data/full_newburgh.csv'),append = T)   
