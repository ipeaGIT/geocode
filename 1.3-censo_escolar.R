library(data.table)
library(dplyr)
library(stringr)
library(Hmisc)

# 1) Trazer as coordenadas da escolas fornecidas pelo INEP -------------------------------------------
escolas_coords <- fread("../../data-raw/censo_escolar/coordenadas_geo/escolas_inep_2020.csv",
                        encoding = "UTF-8",
                        colClasses = "character")


# reformat columns
escolas_coords <- janitor::clean_names(escolas_coords)
escolas_coords <- escolas_coords %>% 
  select(co_entidade = codigo_inep, endereco, lon = longitude, lat = latitude)


  

escolas_coords_break <- escolas_coords %>%
  # tirar ponto que esteja dentro do numero
  mutate(endereco = gsub(pattern = "(\\d{1,})\\.(\\d{3})", "\\1\\2", x = endereco)) %>% 
  # slice(1:100) %>%
  # extrair o logradouro
  mutate(logradouro = sub(pattern = "^(.*),(.*)", replacement = "\\1", x = endereco)) %>%
  # extrair o numero
  mutate(numero = sub(pattern = "^(.*), (\\d{,5}\\s|SN\\s|S/N\\s|S/Nº\\s)(.*)", replacement = "\\2", x = endereco)) %>%
  mutate(numero = trimws(numero, "both")) %>%
  mutate(numero1 = ifelse(grepl(x = numero, pattern = "^(SN|S/N|S/Nº|\\d{,5})$"), numero, "")) %>% 
  # extrair cep
  mutate(cep = stringr::str_extract(pattern = "\\d{5}-\\d{3}", string = endereco)) %>%
  # extrair cidade - extrair a string que esta entre o cep (?<=\\d{5}-\\d{3} ) e um espaco e um tracinho (?= -)
  mutate(cidade = str_extract(pattern = "(?<=\\d{5}-\\d{3} ).*(?= -)", string = endereco)) %>%
  mutate(cidade = trimws(cidade, "both")) %>%
  # extrrair a UF - extrair 2 caracter maisculos [:upper:]{2} que estejam antes de um . e do final da string (?=\\.$)
  mutate(uf = str_extract(pattern = "[:upper:]{2}(?=\\.$)", string = endereco)) %>%
  # extrair bairro (ou tentar)
  mutate(bairro = str_extract(pattern = "(?<=, ).*(?=\\d{5}-\\d{3})", string = endereco)) %>%
  mutate(bairro = gsub(pattern = "SN|S/N|S/Nº|\\d{,5}", replacement = "", x = bairro)) %>%
  mutate(bairro = trimws(x = bairro, which = "both")) %>%
  mutate(bairro = gsub(pattern = "\\.$", replacement = "", x = bairro)) %>%
  # deletar rua. do nome do bairro
  mutate(bairro = gsub(pattern = "^RUA. ", "", bairro)) %>%
  # deletar predio. do nome do bairro
  mutate(bairro = gsub(pattern = "^PREDIO. ", "", bairro)) %>%
  # deletar avenida. do nome do bairro
  mutate(bairro = gsub(pattern = "^AVENIDA. ", "", bairro)) %>%
  mutate(bairro = gsub(pattern = "^TRAVESSA. ", "", bairro)) %>%
  mutate(bairro = gsub(pattern = "^PRACA. ", "", bairro)) %>%
  mutate(bairro = gsub(pattern = "^CHACARA. ", "", bairro)) %>%
  mutate(bairro = gsub(pattern = "^CASA. ", "", bairro)) %>%
  # juntar logradouro com o numero
  mutate(logradouro = paste0(logradouro, ", ", numero1))

# renomear por fim
escolas_coords_break <- escolas_coords_break %>%
  select(co_entidade, logradouro, bairro, cep, name_muni = cidade, uf)


# save output
fwrite(escolas_coords_break, "../../data/geocode/educacao/escolas_input_geocode.csv")
