library(data.table)
library(readr)
library(dplyr)


# ano <- 2017

rais_bring_geocode <- function(ano) {
  
  # censoescolar_raw <- fread(sprintf("../../data-raw/censo_escolar/%s/ESCOLAS.CSV", ano))
  
  # open geocoded data
  escolas_geocode <- fread("../../data/geocode/educacao/escolas_output_geocode_com_bairro.csv")
  
  # identificar os enderecos que vamos manter do streetmap
  escolas_geocode[, streetmap := fifelse(Status %in% c("T", "U"), TRUE,
                                              fifelse(Addr_type == "PointAddress", FALSE,
                                                      fifelse(Addr_type %in% c("StreetAddress", "StreetAddressExt", "StreetName") & Score >= 90, FALSE, TRUE)))]
  
  # go
  escolas_geocode[, ':='(lon1 = fifelse(!streetmap, lon_output, lon),
                         lat1 = fifelse(!streetmap, lat_output, lat))]
  
  
  # output
  fwrite(escolas_geocode,  "../../data/geocode/escolas_")
  
  
}


# bring 