# description -------------------------------------------------------------

# este script le e salva os dados (raw) restritos das bases da familia-domilcilio do 
# cadunico necessarios para fazer a sua geocodificacao. 
# sao selecionadas as colunas necessarias para o georreferenciamento,
# os dados sao tratados e salvos em .csv para o georreferenciamento

# feito para os anos 2011:2019


# 0 load setup ------------------------------------------------------------

library(data.table)
library(geobr)
library(purrr)
library(magrittr)
library(dplyr)
library(readr)
library(stringr)
library(furrr)
library(future)


# 1 set function ----------------------------------------------------------

f_cadunico_familia <- function(year){
  
  # * 1.1 read data ---------------------------------------------------------

  # year <- 2011
  
  # select columns to read
  colunas_familia <- c(
    'co_uf',
    'cd_ibge_cadastro',
    'co_familiar_fam',
    'no_tip_logradouro_fam',
    'no_tit_logradouro_fam',
    'no_logradouro_fam',
    'nu_logradouro_fam',
    'nu_cep_logradouro_fam'
  )
  
  # read cadunico data. 
  df_cadunico_familia <- data.table::fread(
    input = paste0("//storage6/bases/DADOS/RESTRITO/CADASTRO_UNICO/csv/cad_familia_12", year, ".csv")
    #, nrows = 100
    , select = colunas_familia
    , colClasses = "character"
    )
  
  # read municipalties data geobr
  df_br <- geobr::read_municipality()
  data.table::setDT(df_br)[, code_muni := as.character(code_muni)]

  # * 1.2 clean data ----------------------------------------------------------

  df_cadunico_familia[
    ,
    `:=`(
      # make sure cep have 8 characters
      nu_cep_logradouro_fam = stringr::str_pad(nu_cep_logradouro_fam, width = 8, pad = 0)
      # make sure cd_ibre_cadastro (code_muni) have 7 characters
      , cd_ibge_cadastro = stringr::str_pad(cd_ibge_cadastro, width = 7, pad = 0)
    )
  ]

  # left join on cadunico data: abbrev_state and name_muni
  df_cadunico_familia[
    df_br,
    `:=`(
      name_muni = i.name_muni,
      abbrev_state = i.abbrev_state
    ),
    on = c("cd_ibge_cadastro" = "code_muni")
  ]
  
  # replace SN in number for "" and remove leading zeros
  df_cadunico_familia[
    ,
    nu_logradouro_fam := data.table::fifelse(
      test = grepl("SN", nu_logradouro_fam),
      yes = '',
      no = sub("^0+", "", nu_logradouro_fam)
    )
  ]
  
  #df_cadunico_familia[
  #  , 
  #  nu_logradouro_fam := sub("SN", "", nu_logradouro_fam)
  #  ]
  
  #df_cadunico_familia[
  #  , 
  #  nu_logradouro_fam := sub("^0+", "", nu_logradouro_fam)
  #]
  
  # create columns
  df_cadunico_familia[
    ,
    `:=`(
      # create address columns: concatenate address components & remove excess spaces..
      #..(type of address, title address, address name, address number)
      logradouro = trimws(gsub("\\s+", " ", paste(
        no_tip_logradouro_fam, 
        no_tit_logradouro_fam,
        paste0(no_logradouro_fam, ","),
        nu_logradouro_fam,
        sep = " " )))
      , ano = as.character(year)
      
      # add "-" to CEP (A PRINCIPIO, NAO E NECESSARIO)
      #cep = gsub("^([0-9]{5})([0-9]+)$", "\\1-\\2", cadunico$nu_cep_logradouro_fam)
    )]
  
  # create year column
  df_cadunico_familia[, c("ano") := as.character(year)]

  # remove unnecessary columns
  df_cadunico_familia[
    , 
    c('no_tip_logradouro_fam','no_tit_logradouro_fam','no_logradouro_fam',
      'nu_logradouro_fam') := NULL
    ]
  
  ## make sure it is saved using UTF-8 encoding
  colunas <- colnames(df_cadunico_familia)
  df_cadunico_familia <- df_cadunico_familia[
    ,
    lapply(.SD, enc2utf8),
    .SDcols = colunas
  ]
  
  for (name in colnames(df_cadunico_familia)) {
    Encoding(colnames(df_cadunico_familia)) <- 'UTF-8'
  }

  
  # * 1.3 save data -----------------------------------------------------------

  if (!dir.exists(sprintf("../../data/geocode/cadunico//%s",year))){
    dir.create(sprintf("../../data/geocode/cadunico//%s",year))
  }
  
  # save data as .csv
  data.table::fwrite(
    x = df_cadunico_familia
    , file = sprintf("../../data/geocode/cadunico/%s/cadunico_familia_%s_input_geocode.csv", year, year)
    , sep = ";"
    , append = F
    , bom = T
  )
  
}

# 2 run function ------------------------------------------------------------
anos <- 2011:2019

future::plan(future::multisession)

furrr::future_walk(
  anos,
  ~f_cadunico_familia(.)
)