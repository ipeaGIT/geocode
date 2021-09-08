library(data.table)
library(readr)
library(dplyr)
options(scipen = 99999)

# ano <- 2019

rais_bring_geocode <- function(ano) {
  
  # abrir rais raw do ano
  # select columns
  colnames <- fread(sprintf('//storage6/bases/DADOS/RESTRITO/RAIS/csv/estab%s.csv', ano),
                    nrows = 100,
                    # select = c("id_estab", "qt_vinc_ativos", 'nat_jur2018',  "logradouro", "bairro", "codemun", "uf", "cep"),
                    colClasses = "character") %>% colnames()
  
  columns <- c("id_estab", "qt_vinc_ativos", colnames[colnames %like% "nat_jur"], "bairro", "codemun", "cep")
  
  rais_estabs <- fread(sprintf('//storage6/bases/DADOS/RESTRITO/RAIS/csv/estab%s.csv', ano)
                       #, nrows = 5
                       , colClasses='character'
                       , select = columns)
  
  # 1.3) Renomear columns
  colnames(rais_estabs) <- c("id_estab", "qt_vinc_ativos", "nat_jur", "bairro", "codemun", "cep")
  
  
  # open geocoded data
  # first identify the years that we will bring the geocode
  anos_geocoded <- if(ano == 2017) 2017 else seq(from = 2017, to = ano, by = 1)
  
  # open geocode files
  rais_geocoded <- lapply(sprintf("../../data/geocode/rais/%s/rais_%s_output_geocode_streetmap_new.csv", anos_geocoded, anos_geocoded), fread,
                          select = c("id_estab", "Status", "Match_addr","Score", "Addr_type",
                                     "lon_output", "lat_output",
                                    "logradouro", "name_muni", "code_muni", "uf", "type_input_galileo"),
                          encoding = "UTF-8"
                          # colClasses = 'character'
                          ) %>%
    rbindlist()
  
  # rename
  rais_geocoded <- 
    rais_geocoded %>%
    select(id_estab,
           # variables from output
           Status, matched_address = Match_addr, Score, Addr_type,
           # coords
           lon = lon_output, lat = lat_output,
           # variables from input
           logradouro, name_muni, uf, type_year_input = type_input_galileo
    )
  
  # make sure idestab is 14 chars
  rais_geocoded[, id_estab := stringr::str_pad(id_estab, width = 14, pad = 0)]
  rais_estabs[, id_estab := stringr::str_pad(id_estab, width = 14, pad = 0)]
  
  # if a same idestab is from 2017 and changed its address in 2018, we must give priority to 2018
  # update the raw data with the coordinates
  rais_estabs[rais_geocoded, on = c("id_estab"),
                            c("Status", "matched_address", "Score", "Addr_type", "lon", "lat", "logradouro",  "name_muni", "uf", "type_year_input") := 
                              list(i.Status, i.matched_address, i.Score,  i.Addr_type, i.lon, i.lat, i.logradouro, i.name_muni, i.uf, i.type_year_input)]
  
  # table(rais_estabs$type_year_input, useNA = 'always')
  
  # save it
  fwrite(rais_estabs,
         sprintf("../../data/geocode/rais/%s/rais_%s_raw_geocoded.csv", ano, ano))
}

