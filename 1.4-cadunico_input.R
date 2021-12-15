# description -------------------------------------------------------------

# este script le e salva os dados (raw) restritos das bases da familia-domilcilio do 
# cadunico necessarios para fazer a sua geocodificacao. 
# sao selecionadas as colunas necessarias para o georreferenciamento,
# os dados sao tratados e salvos em .csv para o georreferenciamento

# feito para os anos 2011:2019

# https://www.mds.gov.br/webarquivos/publicacao/cadastro_unico/Manual%20do%20Entrevistador%204%20ed%20-%20Livro%20Consolidado%20-%2013042017.pdf
# sobre coluna bairro:
# duas variaveis contem informacoes sobre bairro no cadunico: 
# no_localidade_fam (Quesito 1.11 - Localidade):
  # preenchimento obrigatório; nas áreas urbanas contém o nome do bairro e nas áreas 
  # rurais a regiao do municipio (vila, povoado, etc)

# no_unidade_territorial_fam (Quesito 1.19 - Unidade Territorial Local)
  # preenchimento nao-obrigatorio; divisao territorial definida e organizada pelo..
  # municipio (municipio deve padronizar e organizar os codigos de cada Unidade)
  # OBS: apesar disso, algumas observacoes (famílias) contém informação sobre bairro no
  # unidade territorial

# Qual coluna utilizar para pegar informação sobre bairro:
  # 1- se tiver informação, utilizar no_localidade_fam
  # 2- se no_localidade_fam não tiver informação, utilizar no_unidade_territorial_fam
  # 3- se nenhuma tiver informação, deixar em branco
  

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

  # year <- 2019
  
  # select columns to read
  col_familia <- c(
    'co_uf'
    ,'cd_ibge_cadastro'
    ,'co_familiar_fam'
    ,'no_tip_logradouro_fam'
    ,'no_tit_logradouro_fam'
    ,'no_logradouro_fam'
    ,'nu_logradouro_fam'
    ,'nu_cep_logradouro_fam'
    ,"no_localidade_fam"
    ,"no_unidade_territorial_fam"
  )
  
  # read cadunico data. 
  df_cadunico_familia <- data.table::fread(
    input = paste0("//storage6/bases/DADOS/RESTRITO/CADASTRO_UNICO/csv/cad_familia_12", 
                   year, ".csv")
    #, nrows = 1000
    , select = col_familia
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
      yes = "",
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
      logradouro = trimws(
        gsub("\\s+", " ", paste(
          no_tip_logradouro_fam
          , no_tit_logradouro_fam
          , paste0(no_logradouro_fam, ",")
          , nu_logradouro_fam
          , sep = " " ))
        )
      
      , ano = as.character(year)
      
      # add "-" to CEP (A PRINCIPIO, NAO E NECESSARIO)
      #cep = gsub("^([0-9]{5})([0-9]+)$", "\\1-\\2", cadunico$nu_cep_logradouro_fam)
    )]
  
  # create bairro column
  ## vector with necessary columns
  col_bairro <- c("no_localidade_fam", "no_unidade_territorial_fam")
  ## remove whitespace
  df_cadunico_familia[
    ,
    (col_bairro) := lapply(.SD, stringr::str_squish), 
    .SDcols = col_bairro
  ]


  # replace empty no_localidade_fam with NA
  df_cadunico_familia[
    ,
    no_localidade_fam := gsub("^$", NA_character_, no_localidade_fam)
    ]
  # replace empty no_unidade_territorial_fam with NA
  #df_cadunico_familia[
  #  ,
  #  no_unidade_territorial_fam := gsub("^$", NA_character_, no_unidade_territorial_fam)
  #]
  
  # bairro: 
  # 1- no_localidade_fam if non empty (""); 
  # 2- no_unidade_territorial_fam if no_localidade_fam empty
  # 3- empty if both empry
  df_cadunico_familia[
    ,
    bairro := data.table::fcase(
      !is.na(no_localidade_fam), no_localidade_fam,
      is.na(no_localidade_fam) & !is.na(no_unidade_territorial_fam), no_unidade_territorial_fam,
      default = ""
    )
  ]
  
  
  # remove unnecessary columns
  df_cadunico_familia[
    , 
    c('no_tip_logradouro_fam','no_tit_logradouro_fam','no_logradouro_fam',
      'nu_logradouro_fam', "no_localidade_fam", "no_unidade_territorial_fam") := NULL
    ]
  
  ## make sure it is saved using UTF-8 encoding
  col_encoding <- colnames(df_cadunico_familia)
  df_cadunico_familia <- df_cadunico_familia[
    ,
    lapply(.SD, enc2utf8),
    .SDcols = col_encoding
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
