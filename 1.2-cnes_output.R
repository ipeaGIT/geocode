library(data.table)
library(readr)
library(dplyr)
options(scipen = 99999)

# ano <- 2017

cnes_bring_geocode <- function(ano) {
  
  cnes_estabs <- read_rds(sprintf("../../data/geocode/cnes/%s/cnes_%s_raw.rds", ano, ano))
  
  # open geocoded data
  # first identify the years that we will bring the geocode
  anos_geocoded <- if(ano == 2017) 2017 else seq(from = 2017, to = ano, by = 1)
  
  # open geocode files
  cnes_geocoded <- lapply(sprintf('../../data/geocode/cnes/%s/CNES_%s_output_geocode_streetmap.csv', anos_geocoded, anos_geocoded), fread,
                          encoding = "UTF-8"
                          # colClasses = 'character'
  ) %>%
    rbindlist()
  
  # rename
  cnes_geocoded <- 
    cnes_geocoded %>%
    select(cnes,
           # variables from output
           Status, matched_address = Match_addr, Score, Addr_type,
           # coords
           lon = lon_output, lat = lat_output,
           # variables from input
           type_year_input = year_input
    )
  
  # make sure idestab is 7 chars
  cnes_geocoded[, cnes := stringr::str_pad(cnes, width = 7, pad = 0)]
  cnes_estabs[, cnes := stringr::str_pad(cnes, width = 7, pad = 0)]
  
  # if a same idestab is from 2017 and changed its address in 2018, we must give priority to 2018
  # update the raw data with the coordinates
  cnes_estabs[cnes_geocoded, on = c("cnes"),
              c("Status", "matched_address", "Score", "Addr_type", "lon", "lat", "type_year_input") := 
                list(i.Status, i.matched_address, i.Score,  i.Addr_type, i.lon, i.lat, i.type_year_input)]
  
  # table(cnes_estabs$type_year_input, useNA = 'always')
  # summary(cnes_estabs$lon)
  
  # save it
  fwrite(cnes_estabs,
         sprintf("../../data/geocode/cnes/%s/cnes_%s_raw_geocoded.csv", ano, ano))
}