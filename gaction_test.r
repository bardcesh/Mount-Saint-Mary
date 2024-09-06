# Load necessary libraries
library(tidyverse)
library(lubridate)
library(httr)
library(jsonlite)

#### ---------- API stuff

## QuantAQ API details
api_key = 'JSPWKD9RVB5IMP526YLBSGCS'
serial_number = 'MOD-PM-00215' # Specific to this monitoring device

base_url = "https://api.quant-aq.com/device-api/v1"
data_endpoint = '/data'

# Function to make API requests
get_request = function(url, api_key, params = NULL){
  response = httr::GET(
    url = url,
    authenticate(user = api_key, password = api_key, type = 'basic'),
    query = params
  )
  return(response)
}

# Function to get data from the API
get_data = function(serial_number, data_points=NULL, start_date=NULL, end_date = NULL){
  # This will save all the data
  main_data = list()  # Use list to accumulate data
  
  # Data endpoint for a specific defined serial number
  url = paste0(base_url, '/devices/', serial_number, data_endpoint)
  
  # Adding date filter if it's not null in parameters
  date_filter = NULL
  if (!is.null(start_date) || !is.null(end_date)) {
    date_filter = paste0("timestamp_local,ge,", start_date, ";timestamp_local,le,", end_date)
  }
  
  # Parameters for the request
  params = list(page=1, limit=data_points, sort="timestamp_local,desc", per_page=1000, filter=date_filter)
  
  # Loop to handle pagination
  repeat {
    response = get_request(url = url, api_key = api_key, params = params)
    response_data = content(response, 'parsed', encoding = 'UTF-8')
    
    if (length(response_data$data) == 0) {
      print('No more data available.')
      break
    }
    
    main_data <- c(main_data, response_data$data)  # Accumulate data
    url = response_data$meta$next_url
    
    if (is.null(url)) {
      print('No more pages.')
      break
    }
    
    params$page = params$page + 1
  }
  
  print('Total number of data points:')
  print(length(main_data))
  
  # Convert the list of data to JSON
  json_data = toJSON(main_data, auto_unbox = TRUE, pretty = TRUE)
  return(json_data)
}

##### ----- Data pull and munging

# Read the existing CSV file
wk <- read_csv("https://raw.githubusercontent.com/bardcesh/Mount-Saint-Mary/main/data/full_newburgh.csv", col_types = "cddc") 

# Convert YMD to datetime
wk <- wk %>%
  mutate(YMD = ymd_hms(YMD))

# Find the latest date in the .csv file
max_date <- max(wk$YMD)

# Fetch new data from API
data = get_data(serial_number = serial_number, data_points=NULL, start_date=max_date, end_date=floor_date(now("EST"), unit="hours"))

# Convert JSON data to a list or data frame
recent_data <- jsonlite::fromJSON(data)

# Check and debug structure of recent_data
print("Structure of recent_data:")
print(str(recent_data))  # Debug: Inspect structure

# Check if recent_data is a list and extract data
if (is.list(recent_data)) {
  if ("data" %in% names(recent_data)) {
    print("Data found in recent_data")
    recent_data <- recent_data$data  # Assuming 'data' holds the actual readings
  } else {
    stop("No 'data' field found in recent_data. Check API response structure.")
  }
}

# Ensure recent_data is a data frame
if (!is.data.frame(recent_data)) {
  stop("Recent data is not a data frame. The structure might be incorrect.")
}

# Process the data
recent_data <- recent_data %>%
  select(timestamp_local, pm25, pm10) %>%
  rename(
    PM2.5 = pm25,
    PM10 = pm10
  ) %>%
  mutate(timestamp_local = ymd_hms(timestamp_local)) %>%
  rename(YMD = timestamp_local) %>%
  mutate(
    PM2.5 = as.numeric(PM2.5),
    PM10 = as.numeric(PM10)
  )

# Compress to hourly data
recent_data_h <- recent_data %>%
  group_by(YMD = cut(YMD, breaks="60 min")) %>%
  summarize(
    PM2.5 = mean(PM2.5, na.rm=TRUE),
    PM10 = mean(PM10, na.rm=TRUE)
  ) %>%
  mutate(YMD = ymd_hms(YMD))


# Append new data to the existing CSV file
write_csv(recent_data_h, 'data/full_newburgh.csv', append = TRUE)

