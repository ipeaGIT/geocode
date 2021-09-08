library(data.table)
library(dplyr)
library(Hmisc)

# ano <- 2019

saude_filter <- function(ano) {
  
  # 1) Ler os dados da base do CNES que foi disponibilizada pelo MSaude ---------------------------------
  
  if(ano %in% c("2017", "2018")) {
    
    
    cnes_temp <- readxl::read_xlsx(path = '../../data-raw/hospitais/2017-2018/BANCO_BANCO_ESTAB_10_2017_E_10_2018_02_10_2020.xlsx',
                                   sheet = 'BANCO', skip = 13, 
                                   col_types = "text")
    
    
    # format column names
    cnes_temp <- janitor::clean_names(cnes_temp)
    
    # filter year 
    cnes_temp <- setDT(cnes_temp)[competencia %like% ano]
    
    
    # nrow(cnes_temp) # 328631 obs
    # colnames(cnes_temp)
    
    # rename columns
    names(cnes_temp)[16:32] <- c("instal_fisica_ambu", "instal_fisica_hospt", "instal_fisica_urgencia",
                                 "complex_alta_ambu_est", "complex_alta_ambu_mun", "complex_baix_ambu_est", "complex_baix_ambu_mun", "complex_medi_ambu_est", "complex_medi_ambu_mun", 
                                 "complex_alta_hosp_est", "complex_alta_hosp_mun", "complex_baix_hosp_est", "complex_baix_hosp_mun", "complex_medi_hosp_est", "complex_medi_hosp_mun", 
                                 "complex_nao_aplic_est", "complex_nao_aplic_mun")
    
    # select columns
    cnes_temp[, logradouro := paste0(logradouro, ", ", numero)]
    # cnes_input_geocode <- cnes_temp[, .(cnes, logradouro, bairro, municipio, uf, cep)]
    
    # save
    readr::write_rds(cnes_temp, sprintf("../../data/geocode/cnes/%s/cnes_%s_raw.rds", ano, ano))
    
    
    
  } else if (ano == 2019) {
    
    cnes_temp <- readxl::read_xlsx(path = '../../data-raw/hospitais/2019/CNES_NDIS_01_10_2019_BANCO_COMP_08_2019.xlsx',
                                   sheet = 'BANCO', skip = 14, 
                                   col_types = "text")
    
    # format column names
    cnes_temp <- janitor::clean_names(cnes_temp)
    
    # remove 1st NA rows
    cnes_temp <- cnes_temp[-c(1:3),]
    
    # rename columns
    names(cnes_temp)[15:30] <- c("instal_fisica_ambu", "instal_fisica_hospt", 
                                 "complex_alta_ambu_est", "complex_alta_ambu_mun", 
                                 "complex_baix_ambu_est", "complex_baix_ambu_mun", 
                                 "complex_medi_ambu_est", "complex_medi_ambu_mun", 
                                 "complex_alta_hosp_est", "complex_alta_hosp_mun", 
                                 "complex_baix_hosp_est", "complex_baix_hosp_mun", 
                                 "complex_medi_hosp_est", "complex_medi_hosp_mun", 
                                 "complex_nao_aplic_est", "complex_nao_aplic_mun")
    # nrow(cnes_temp) # 340115 obs
    # colnames(cnes_temp)
    
    
    # select columns
    setDT(cnes_temp)
    cnes_temp[, logradouro := paste0(logradouro, ", ", numero)]
    # cnes_input_geocode <- cnes_temp[, .(cnes, logradouro, bairro, municipio, uf, cep)]
    
    # table(nchar(cnes_input_geocode$cnes))
    
    # save
    readr::write_rds(cnes_temp, sprintf("../../data/geocode/cnes/%s/cnes_%s_raw.rds", ano, ano))
    
    
  } 
  
  
  # select the whole file if it is 2017
  # if not, geocode only new hospitals
  
  if (ano == 2017) {
    
    # identify type input
    cnes_input_geocode[, year_input := 2017]
    
    # salvar
    fwrite(cnes_input_geocode, sprintf("../../data/geocode/cnes/%s/cnes_%s_input_geococde_streetmap.csv", ano, ano))
    
  } else {
    
    # abrir o do ano anterior
    cnes_input_geocode_anterior <- readr::read_rds(sprintf("../../data/geocode/cnes/%s/cnes_%s_raw.rds", ano-1, ano-1))
    
    # table(nchar(cnes_input_geocode$cnes))
    # table(nchar(cnes_input_geocode_anterior$cnes))
    
    # force everyone to 7 chars
    cnes_input_geocode[, cnes := stringr::str_pad(cnes, width = 7, pad = 0, side = "left")]
    cnes_input_geocode_anterior[, cnes := stringr::str_pad(cnes, width = 7, pad = 0, side = "left")]
    
    # Selecionar somente os estabs que nao foram georef antes
    cnes_input_geocode_new <- cnes_input_geocode[cnes %nin% cnes_input_geocode_anterior$cnes]
    
    cnes_input_geocode_new[, year_input := ano]
    
    
    # salvar
    fwrite(cnes_input_geocode_new, sprintf("../../data/geocode/cnes/%s/cnes_%s_input_geococde_streetmap.csv", ano, ano))
    
    
  }
  
  
  
}
w