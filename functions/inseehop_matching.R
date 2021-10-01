library(tidyverse)
library(stringi)
library(lubridate)
library(stringdist)
library(parallel)
library(future)
library(future.apply)
library(rlang)

############################### PLAN ################################################

# 1) Arguments
# 2) Entry tibble and arguments conformity checks
## a) Is local_tb a tibble ?
## b) Arguments of mapping function : are their type corrects ?
## c) Test on forbiden names
## d) Are all the necessary variables present ?
## e) Are all the variable from the entry tibble classes correct ?
## f) Alert message about gender coding/repartition.
# 3) Functions usefull for the scripts
## a) French National Mortality Database(FNMD) import function  = fn_import_insee
## b) string normalization function = fn_norm_chaine
## c) City name normalization function = fn_norm_localite
# 4) Local data Cleaning
## a) Selection an renaming of necessary variables
## b) City birth names data cleaning (if present)
## c) Creation of the necessary new variables
# 5) Import of the FNDM variables necessary for the matching steps.
# 6) Mapping/linking
## a) linking function =  fn_compte
## b) Choice of the optimal number of cores (if not chosen by user) and computation estimation time.
## c) Blocking on birth dates
## d) Blocking on the four fisrt character of the first name and the family name
# 7) Selection of the most pertinent valid paires (if used)
# 8) Finalization of the output tibble.


# 1) Arguments
mapping_fn <-
  function(local_tb,
                          
available_ram_go,

birth_place_variable = "",

birth_date_variable,

gender_variable,

male_modality,

female_modality,

birth_surname_variable="",

use_surname_variable="",

firstname_variable,

n_core_to_use = NA_integer_,

max_total_dld = 2,

max_firstname_dld =2,

max_surname_dld =1,

max_birthdate_dld = 1,

max_gender_dld = 1,

death_date_lower_limit ="",

death_date_upper_limit="" ,

output_short = F,

english_language = T,

automatic_saving = T){
  

# 2) Entry tibble conformity checks
## a) Is local_tb a tibble ?
#   
nom_du_tibble_source <-  deparse(substitute(local_tb))

if(!is_tibble(local_tb)){stop(paste0(nom_du_tibble_source," must be a tibble. To convert a dataframe to a tibble, you can use dplyr::as_tibble(",nom_du_tibble_source,"). Dataframes are depricated for the tidyverse. See https://r4ds.had.co.nz/tibbles.html"))}


## b) Arguments of mapping function : are their type corrects ?

# Test functions  
fn_is_num_stop_simple <- function(variable){if(!is.numeric(variable) | is.na(variable)){stop(paste0("The ",deparse(substitute(variable))," argument must be a non missing numeric."))}}
fn_is_char_stop_simple <- function(variable){if(!is.character(variable)|is.na(variable)){stop(paste0(deparse(substitute(variable)),"  argument must be a non missing character."))}}


# Test on numeric arguments
fn_is_num_stop_simple(available_ram_go)
fn_is_num_stop_simple(max_total_dld)
fn_is_num_stop_simple(max_firstname_dld)
fn_is_num_stop_simple(max_surname_dld)
fn_is_num_stop_simple(max_birthdate_dld)
fn_is_num_stop_simple(max_gender_dld)

# Test on character arguments
fn_is_char_stop_simple(birth_place_variable)
fn_is_char_stop_simple(birth_date_variable)
fn_is_char_stop_simple(gender_variable)
fn_is_char_stop_simple(male_modality)
fn_is_char_stop_simple(female_modality)
fn_is_char_stop_simple(birth_surname_variable)
fn_is_char_stop_simple(use_surname_variable)
fn_is_char_stop_simple(firstname_variable)

# test on dates
fn_is_date = function(x) {
  formatted = try(as.Date(x, "%Y-%m-%d"), silent = TRUE)
  return(ifelse(is.na(as.character(formatted) == x),F,T))
}


fn_is_char_stop_simple(death_date_lower_limit)
fn_is_char_stop_simple(death_date_upper_limit)


if(death_date_lower_limit!=""&death_date_upper_limit==""){stop("Both death_date_lower_limit and death_date_upper_limit must be let empty or completed")}
if(death_date_lower_limit==""&death_date_upper_limit!=""){stop("Both death_date_lower_limit and death_date_upper_limit must be let empty or completed")}

if(death_date_lower_limit!=""& !fn_is_date(death_date_lower_limit)){stop("death_date_lower_limit must be to format 'YYYY-MM-DD' or let empty.")}
if(death_date_upper_limit!=""& !fn_is_date(death_date_upper_limit)){stop("death_date_lower_limit must be a character 'YYYY-MM-DD' or let empty.")}

if(death_date_lower_limit!=""&death_date_upper_limit!=""&(as_date(death_date_upper_limit)<as_date(death_date_lower_limit))){stop("the upper death limit is before the lower death limit.")}
 
# test on booléans.
if(!is.logical(output_short)){stop("Output_short must be a logical.")}
if(!is.logical(english_language)){stop("English_language must be a logical.")}
if(!is.logical(automatic_saving)){stop("English_language must be a logical.")}
  
 
## c) Test on forbiden names

forbiden_names <- c("available_ram_go", "birth_date_variable", "birth_place_variable", 
                    "birth_surname_variable", "compteur_integralite", "defaultW", 
                    "female_modality", "firstname_variable", "fn_blocage_ddn", "fn_blocage_prenomnom", 
                    "fn_calcul_conso_ram", "fn_calcul_temps_execution", "fn_compte", 
                    "fn_import_insee", "fn_is_char_stop_simple", "fn_is_date", "fn_is_num_stop_simple", 
                    "fn_noms_tb_min", "fn_norm_chaine", "fn_norm_localite", "fn_stop_class_character", 
                    "fn_stop_class_character_names", "fn_stop_class_date", "fn_stop_interdit", 
                    "fn_stop_presence", "forbiden_names", "gender_variable", "death_date_upper_limit", 
                    "death_date_lower_limit", "male_modality", "max_birthdate_dld", 
                    "max_firstname_dld", "max_gender_dld", "max_surname_dld", "max_total_dld", 
                    "n_coeur_final", "n_coeur_plus_rapide", "n_coeurs_libres", "n_core_to_use", 
                    "n_female", "n_male", "n_other_gender", "n_subjects_after_filter", 
                    "n_subjects_before_filter", "n_subjects_filtered", "n_sujets", 
                    "n_thread_max_possible", "nom_du_tibble_source", "output_short", 
                    "presence_lib_lieu_naissance", "taille_go_fpd", "taille_go_local", 
                    "taille_go_source", "tb_apparie_1", "tb_apparie_sup1_aumoinsunedist0", 
                    "tb_apparie_sup1_sansdist0", "tb_compte_sexe", "tb_conso_ram", 
                    "tb_corres", "tb_corres_simplifie", "tb_fpd_insee", "tb_insee", 
                    "local_tb", "tb_long", "tb_nantes", "tb_paires_valides_ddns", 
                    "tb_paires_valides_prenomnom", "tb_patients_locaux", "tb_prett", 
                    "tb_short", "temps_estime", "use_surname_variable", "v_ddn_locales", 
                    "v_gp_prenomnom_locales", "v_sexe_trouve", "v_variables_a_croiser",
                    "nom_norm_fpd", "prenom_0_norm_fpd", "prenom_1_norm_fpd", "prenom_1_2_norm_fpd", 
                    "nom_fpd", "prenoms_fpd", "sexe_fpd", "date_naissance_fpd", "code_insee_naissance_fpd", 
                    "libelle_commune_naissance_aff_fpd", "pays_naissance_fpd", "date_deces_fpd", 
                    "code_insee_deces_fpd", "libelle_commune_deces_tbc_fpd", "sexe_norm_fpd", 
                    "libelle_commune_naissance_norm_fpd", "id_insee", "gp_prenomnom","surname_fnmd", 
                    "first_names_fnmd","gender_fnmd", "birthdate_fnmd","insee_birth_code_fnmd", "birth_city_name_fnmd",
                    "country_birth_name_fnmd","death_date_fnmd", "insee_death_city_code", "insee_death_city_name","id_local", "nom_naissance_local", "nom_usuel_local", "prenom_local", 
                    "sexe_local", "date_naissance_local","libelle_commune_naissance_aff_local","nom_usuel_local_norm",
                    "nom_naissance_local_norm","nom_etatcivil_local_norm","prenom_local_norm"
)
  
fn_stop_interdit <- function(variable){if(variable %in% forbiden_names){stop(paste0("the name '",variable,"' is reserved for the mapping function. No variable in ",nom_du_tibble_source," is allowed to be named this way. See Read Me."))}}

walk(.x = names(local_tb),.f = fn_stop_interdit)



## d) Are all the necessary variables presents ?

### presence function

fn_stop_presence <- function(variable){if(!variable %in% names(local_tb)){stop(paste0(variable," not found in local_tb"))}}


### application to always used variables
walk(.x = c(birth_date_variable,gender_variable,firstname_variable),.f = fn_stop_presence)


### application to birth_place_variable
if(birth_place_variable!= ""){fn_stop_presence(birth_place_variable)}



### Application to all the surnames possibilities
if(use_surname_variable=="" & birth_surname_variable==""){stop("A least one surnamne variable must be completed.")}
if(use_surname_variable=="" & birth_surname_variable!=""){fn_stop_presence(birth_surname_variable)}
if(birth_surname_variable=="" & use_surname_variable!=""){fn_stop_presence(use_surname_variable)}
if(birth_surname_variable!=""& use_surname_variable!=""){walk(.x = c(use_surname_variable,use_surname_variable),.f = fn_stop_presence)}

### Duplication of a surname if one has not been completed.

if(use_surname_variable=="") {
  local_tb <-
    local_tb %>%
    mutate(nom_usuel_dup_local = !!as.name(birth_surname_variable))
  use_surname_variable <- "nom_usuel_dup_local"
  print("No use surname was completed. Only the bith surname will be used")
  warning("No use surname was completed. Only the bith surname was used")
}


if(birth_surname_variable=="") {
  local_tb <-
    local_tb %>%
    mutate(nom_naissance_dup_naiss = !!as.name(use_surname_variable))
  birth_surname_variable <- "nom_naissance_dup_naiss"
  print("No birth surname was completed. Only the use surrname will be used")
  warning("No birth surname was completed. Only the use surrname was used")
}


## e) Are all the variable from the entry tibble classes correct ?


fn_stop_class_date <- function(variable){if(class(get(variable,local_tb))!="Date"){stop(paste(variable," from ",nom_du_tibble_source," is not a Date class"))}}
fn_stop_class_character <- function(variable){if(class(get(variable,local_tb))!="character"){stop(paste(variable," from ",nom_du_tibble_source," is not a Character class"))}}
fn_stop_class_character_names <- function(variable){if(class(get(variable,local_tb))!="character"){stop("Birth Surname or/and Use Surname in ",nom_du_tibble_source," is/are  not character class")}}

fn_stop_class_date(birth_date_variable)
walk(.x = c(firstname_variable,gender_variable),.f = fn_stop_class_character)
walk(.x = c(use_surname_variable,birth_surname_variable),.f = fn_stop_class_character_names)
if(birth_place_variable!=""){fn_stop_class_character(birth_place_variable)}


## f) Alert message about gender coding/repartition.

tb_compte_sexe <- 
 local_tb %>%
   group_by(!!sym(gender_variable)) %>%
   tally()

v_sexe_trouve <- 
  tb_compte_sexe %>%
  pull(!!sym(gender_variable))

print(paste0("Concerning the repartion of gender in ",gender_variable," from ",nom_du_tibble_source, ":" ))

if (!male_modality %in% v_sexe_trouve){
  warning(paste0("Male modality was not found once in ",gender_variable," from ",nom_du_tibble_source,"."))
  
}

if (!female_modality %in% v_sexe_trouve){
  warning(paste0("Female modality was not found once in ",gender_variable," from ",nom_du_tibble_source,"."))
  
}

n_male <-
  tb_compte_sexe %>%
  filter(!!sym(gender_variable)==male_modality) %>%
  pull(n)

if(length(n_male)==0){n_male=0}

n_female <-
  tb_compte_sexe %>%
  filter(!!sym(gender_variable)==female_modality) %>%
  pull(n)

if(length(n_female)==0){n_female=0}


n_other_gender <- nrow(local_tb) - n_male - n_female


print(paste0("   male modality was found ",n_male," time(s)."))

print(paste0("   female modality was found ",n_female," time(s)."))

print(paste0("   unknow/missing gender was found ",n_other_gender ," time(s)."))


# 3) Functions usefull for the scripts

## a) FNMD import function = fn_import_insee
  
compteur_integralite <- 0    

fn_import_insee <- function(){
  
  tb_temp <-
      read_delim("data/fpd_nettoye.csv", 
                 ";", escape_double = FALSE, col_types = cols(date_deces_fpd = col_date(format = "%Y-%m-%d"), 
                                                              date_naissance_fpd = col_date(format = "%Y-%m-%d"), 
                                                              sexe_fpd = col_character()), na = "empty", 
                 trim_ws = TRUE) %>%
        mutate(id_insee = row_number()) %>%
        mutate(gp_prenomnom=paste0(substr(prenom_1_norm_fpd,1,4),substr(nom_norm_fpd,1,4))) %>%
        mutate(libelle_commune_deces_tbc_fpd=ifelse(libelle_commune_deces_tbc_fpd=="",NA_character_,libelle_commune_deces_tbc_fpd))
  
  if(death_date_lower_limit==""| death_date_upper_limit=="") {
    death_date_lower_limit <-     
    tb_temp %>%
      pull(date_deces_fpd) %>%
      min(na.rm = T)
    
    death_date_upper_limit <-
    tb_temp %>%
      pull(date_deces_fpd) %>%
      max(na.rm = T)
    
    if(compteur_integralite==0){print("   every records from the FNMD will be used.")}
    
  } else {
    
    if(compteur_integralite==0){print(paste0("   Only the records from the mortality database between ",death_date_lower_limit," and ",death_date_upper_limit," will be used."))}
    tb_temp %>% pull(date_deces_fpd) %>% min(na.rm = T)
    death_date_lower_limit <- as_date(death_date_lower_limit)
    death_date_upper_limit <- as_date(death_date_upper_limit)
    
  }
  
        tb_temp %>%
        filter(date_deces_fpd>=death_date_lower_limit) %>%
        filter(date_deces_fpd<=death_date_upper_limit)}

## b) string normalization function = fn_norm_chaine

  fn_norm_chaine <- function(string){
    
    # Minuscule
    string <- tolower(string)
    
    # Accents
    
    string<-stri_trans_general(string,"Latin-ASCII")
    # Caractères spéciaux
    string = gsub("[^a-z]", "", string)
    
    # Espaces
    string = gsub(" ", "", string)
    
    # NA
    
    string = ifelse(string=="",NA,string)
    
    return(string)
  }
  
  
  fn_noms_tb_min <- function(nom_tb){
    names(nom_tb) <- tolower(names(nom_tb))
    return(nom_tb)
  }
  
## c) City name normalization function = fn_norm_localite
  fn_norm_localite <- function(string){
    
    # Minuscule
    string <- tolower(string)
    
    # Saints :   remplacer " st " par "saint" 
    
    string <- sub(pattern =" st ",replacement = "saint",x = string)
    
    # Sur :   remplacer " s  ","/s " , "  sr  " et "/" par "sur" 
    string <- sub(pattern =" s |/s|/| sr ",replacement = "sur",x = string)
    
    
    # Gestion des arrondissements = pour les communues de naissance /// Vaut également pour les fichiers de l'INSEE
    
    # Accents
    
    string<-stri_trans_general(string,"Latin-ASCII")
    
    # Caractères spéciaux
    string = gsub("[^a-z]", "", string)
    
    # Espaces
    string = gsub(" ", "", string)
    
    # NA
    
    string = ifelse(string=="",NA,string)
    
    return(string)
  }

# 4) Local data Cleaning
  
## a) Selection an renaming of necessary variables


print(paste0(nom_du_tibble_source," records cleaning:"))  
  

presence_lib_lieu_naissance <- ifelse(birth_place_variable=="",F,T)

v_variables_a_croiser <- c("id_local",
                           as.character(birth_surname_variable),
                           as.character(use_surname_variable),
                           as.character(firstname_variable),
                           as.character(gender_variable),
                           as.character(birth_date_variable))

if(presence_lib_lieu_naissance==T){v_variables_a_croiser <- c(v_variables_a_croiser,as.character(birth_place_variable))}

                   

tb_patients_locaux <-
  local_tb%>%
  mutate(id_local=row_number()) %>%
  select(all_of(v_variables_a_croiser)) %>%
  rename("date_naissance_local" := !!birth_date_variable,
         "sexe_local" := !!gender_variable,
         "nom_usuel_local" := !!use_surname_variable,
         "nom_naissance_local" := !!birth_surname_variable,
         "prenom_local" := !!firstname_variable) %>%
  mutate(sexe_local=ifelse(is.na(sexe_local),"i",
                           ifelse(as.character(sexe_local)==as.character(female_modality),"f",
                                  ifelse(as.character(sexe_local)==as.character(male_modality),"m","i")
                           )))

if(presence_lib_lieu_naissance==T){tb_patients_locaux <- tb_patients_locaux%>%rename("libelle_commune_naissance_aff_local" := !! birth_place_variable)}


## b) City birth names data cleaning (if present)

if(presence_lib_lieu_naissance==T){
  
tb_temp_1 <-
  tb_patients_locaux %>%
  select(libelle_commune_naissance_aff_local) %>%
  distinct() 

tb_arrond <-
  tb_temp_1 %>%
  filter(str_detect(tolower(libelle_commune_naissance_aff_local),"arrond"))


tb_chiffre <-
  tb_temp_1 %>%
  filter(str_detect(tolower(libelle_commune_naissance_aff_local),"[[:digit:]]+"))


tb_paris_autres <-
  tb_temp_1 %>%
  filter(str_detect(string = tolower(libelle_commune_naissance_aff_local),pattern = "paris ")) %>%
  filter(!tolower(libelle_commune_naissance_aff_local)%in%tolower(tb_arrond$libelle_commune_naissance_aff_local)) %>%
  filter(!tolower(libelle_commune_naissance_aff_local)%in%tolower(tb_chiffre$libelle_commune_naissance_aff_local)) %>%
  filter((!str_detect(tolower(libelle_commune_naissance_aff_local),"touquet"))) %>%
  distinct()

tb_recodage_fpd_libelle_commune <-
  bind_rows(tb_arrond,tb_chiffre,tb_paris_autres) %>%
  mutate(libelle_commune_naissance_simplifie_norm_local= fn_norm_chaine(gsub(pattern = "([[:alpha:]]+).*", replacement = "\\1", x= tolower(libelle_commune_naissance_aff_local)))) %>%
  distinct()

tb_patients_locaux <-
tb_patients_locaux%>%
  left_join(tb_recodage_fpd_libelle_commune,by = "libelle_commune_naissance_aff_local") %>%
  mutate(libelle_commune_naissance_simplifie_norm_local=ifelse(is.na(libelle_commune_naissance_simplifie_norm_local),fn_norm_chaine(libelle_commune_naissance_aff_local),libelle_commune_naissance_simplifie_norm_local)) %>%
  mutate(libelle_commune_naissance_mod_norm_local=fn_norm_localite(libelle_commune_naissance_aff_local)) 
}

## c) Creation of the necessary new variables


n_subjects_before_filter <- 
  nrow(tb_patients_locaux)

tb_patients_locaux <-
  tb_patients_locaux%>%
  mutate(nom_usuel_local_norm = fn_norm_chaine(nom_usuel_local),
         nom_naissance_local_norm = fn_norm_chaine(nom_naissance_local),
         nom_etatcivil_local_norm = ifelse(is.na(nom_naissance_local_norm),nom_usuel_local_norm,nom_naissance_local_norm),
         prenom_local_norm = fn_norm_chaine(prenom_local)) %>%
  filter(!is.na(date_naissance_local)) %>%
  filter(!(is.na(nom_usuel_local_norm)&is.na(nom_naissance_local_norm))) %>%
  filter(!(is.na(prenom_local_norm))) %>% 
  mutate(gp_prenomnom=paste0(substr(prenom_local_norm,1,4),substr(nom_etatcivil_local_norm,1,4))) 

n_subjects_after_filter <-
  nrow(tb_patients_locaux)

n_subjects_filtered <- n_subjects_before_filter - n_subjects_after_filter

if(n_subjects_filtered==0){print(paste0("   all records from ",nom_du_tibble_source," have the minimum informations required to be matched to the mortality database."))} else {
  print(paste0("   ",n_subjects_filtered," subject(s)  from ",nom_du_tibble_source, " don't have the minimum informations required to be matched to the mortality database, ie a valid first name and/or a valid surname (use or birth), and/or a valid bithdate. It/they will stay in the output tibble, but can't be matched."))
  
}


# 5) Import of the FNDM variables necessary for the matching steps.

print("Mortality database import:")


tb_fpd_insee <-
  fn_import_insee() %>%
  select(id_insee,gp_prenomnom,nom_norm_fpd,prenom_0_norm_fpd,prenom_1_norm_fpd,prenom_1_2_norm_fpd,date_naissance_fpd,sexe_norm_fpd,libelle_commune_naissance_norm_fpd)
  
compteur_integralite <- 1

# 6) Mapping/linking

## a) linking function =  fn_compte

fn_compte <- ifelse(presence_lib_lieu_naissance==T,

 function(df){
  df %>%
    mutate(

           dl_nom_usuel = stringdist(nom_usuel_local_norm, nom_norm_fpd, method = 'dl'),
           dl_nom_naissance = stringdist(nom_naissance_local_norm, nom_norm_fpd, method = 'dl'),
           dl_nom_min = pmin(dl_nom_usuel,dl_nom_naissance,na.rm = T),
           dl_prenom0=stringdist(prenom_local_norm, prenom_0_norm_fpd, method = 'dl'),
           dl_prenom1=stringdist(prenom_local_norm, prenom_1_norm_fpd, method = 'dl'),
           dl_prenom12=stringdist(prenom_local_norm, prenom_1_2_norm_fpd, method = 'dl'),
           dl_prenom_min=pmin(dl_prenom0,dl_prenom1,dl_prenom12),
           dl_ddn = stringdist(date_naissance_local, date_naissance_fpd, method = 'dl'),
           dl_sexe= stringdist(sexe_local, sexe_norm_fpd, method = 'dl'),
           dl_comm_naiss_simplifie = stringdist(libelle_commune_naissance_simplifie_norm_local, libelle_commune_naissance_norm_fpd, method = 'dl'),
           dl_comm_naiss_mod = stringdist(libelle_commune_naissance_mod_norm_local, libelle_commune_naissance_norm_fpd, method = 'dl'),           
           dl_comm_naiss_min = pmin(dl_comm_naiss_simplifie,dl_comm_naiss_mod,na.rm = T)) %>%
    mutate(dl_total = dl_prenom_min+dl_nom_min+dl_ddn+dl_sexe) %>%
    filter(dl_nom_min<=max_surname_dld,
           dl_prenom_min<=max_firstname_dld,
           dl_ddn<=max_birthdate_dld,
           dl_sexe<=max_gender_dld,
           dl_total<=max_total_dld) %>%
     select(c("id_local", "id_insee" , 
              "dl_nom_min", "dl_prenom_min", "dl_ddn", "dl_sexe", "dl_comm_naiss_min", "dl_total"))
},

function(df){
  df %>%
    mutate(
      
      dl_nom_usuel = stringdist(nom_usuel_local_norm, nom_norm_fpd, method = 'dl'),
      dl_nom_naissance = stringdist(nom_naissance_local_norm, nom_norm_fpd, method = 'dl'),
      dl_nom_min = pmin(dl_nom_usuel,dl_nom_naissance,na.rm = T),
      dl_prenom0=stringdist(prenom_local_norm, prenom_0_norm_fpd, method = 'dl'),
      dl_prenom1=stringdist(prenom_local_norm, prenom_1_norm_fpd, method = 'dl'),
      dl_prenom12=stringdist(prenom_local_norm, prenom_1_2_norm_fpd, method = 'dl'),
      dl_prenom_min=pmin(dl_prenom0,dl_prenom1,dl_prenom12),
      dl_ddn = stringdist(date_naissance_local, date_naissance_fpd, method = 'dl'),
      dl_sexe= stringdist(sexe_local, sexe_norm_fpd, method = 'dl')) %>%
    mutate(dl_total = dl_prenom_min+dl_nom_min+dl_ddn+dl_sexe) %>%
    filter(dl_nom_min<=max_surname_dld,
           dl_prenom_min<=max_firstname_dld,
           dl_ddn<=max_birthdate_dld,
           dl_sexe<=max_gender_dld,
           dl_total<=max_total_dld) %>%
    select(c("id_local", "id_insee" , 
             "dl_nom_min", "dl_prenom_min", "dl_ddn", "dl_sexe", "dl_total"))
})


## b) Choice of the optimal number of cores (if not chosen by user) and computation estimation time.

# number of local records to match
n_sujets <- nrow(tb_patients_locaux)

# number of available core 
n_coeurs_libres <- (detectCores()-1)

# RAM need estimation function
fn_calcul_conso_ram <- function(n_coeurs){
  taille_go_fpd+taille_go_local+2*taille_go_source+3*(n_coeurs*taille_go_fpd+n_coeurs*taille_go_local)
}

# execution time estimation function
fn_calcul_temps_execution <- function(n_sujets,n_coeurs){-0.0970931 + 0.0001346*(n_sujets/n_coeurs)+0.1056206*n_coeurs}

# choice of the best compromise
if(n_coeurs_libres==0&is.na(n_core_to_use)){n_coeur_final <- 1} else if (is.na(n_core_to_use)) {
  
  
  n_coeur_plus_rapide <-
    tibble(n_coeurs = 1:n_coeurs_libres) %>%
    mutate(n_sujets=n_sujets) %>%
    mutate(temps_estime = map2_dbl(n_sujets,n_coeurs,fn_calcul_temps_execution)) %>%
    filter(temps_estime==min(temps_estime)) %>%
    pull(n_coeurs)
  
  taille_go_fpd <- as.numeric(object.size(tb_fpd_insee))/(1024^3)
  taille_go_local <- as.numeric(object.size(tb_patients_locaux))/(1024^3)
  taille_go_source <- as.numeric(object.size(local_tb))/(1024^3)
  

  
  tb_conso_ram <-
    tibble(nb_coeurs = 1:n_coeurs_libres) %>%
    mutate(conso_ram = fn_calcul_conso_ram(nb_coeurs)) %>%
    filter(conso_ram<available_ram_go)
  
  if(nrow(tb_conso_ram)==0){n_thread_max_possible <- 1} else {
    n_thread_max_possible <- 
      tb_conso_ram %>%
      filter(nb_coeurs==max(nb_coeurs)) %>%
      pull(nb_coeurs)
  }
  
  
  
  
  n_coeur_final <- min(n_coeur_plus_rapide,n_thread_max_possible) 
  
} else {n_coeur_final <- n_core_to_use}

temps_estime <-fn_calcul_temps_execution(n_sujets = n_sujets,n_coeurs = n_coeur_final)

print("Computation parameters:")
print (paste0("   the final number of core used is ",n_coeur_final,"."))

print("   if your RAM is saturated during blocking/matching setps, deacrese n_core_to_use in the function arguments.")
print("   if you have a lot of free RAM during blocking/matching setps and a millions records local file, you can try to increase n_core_to_use in the function arguments.")

print(paste0("   computation time estimated arround ", round(temps_estime,digits = 2), " hour(s)."))  



## c) Blocking on birth dates

# Print message
print("Databases matching:")
print("    blocking on birthdate.")



# Birthdate blocking function
fn_blocage_ddn <- function(date_filtre){
  
  require(tidyverse)
  require(stringi)
  require(lubridate)
  require(stringdist)

  
  tb_patients_locaux_ddnsel <-
    tb_patients_locaux %>%
    filter(date_naissance_local==date_filtre)
  
  tb_fpd_insee_ddnsel <-
    tb_fpd_insee %>%
    filter(date_naissance_fpd==date_filtre)
  
  

  tb_patients_locaux_ddnsel %>%
    full_join(tb_fpd_insee_ddnsel,by=c("date_naissance_local"="date_naissance_fpd"),keep=T) %>% 
    fn_compte()
  
}



# extraction of all distinct birthdate to which the blocking function will be applied
v_ddn_locales <-
  tb_patients_locaux %>%
  pull(date_naissance_local) %>%
  unique()



# application of the matching, on a single or multiple cores

if(n_coeur_final>1){

  options(future.globals.maxSize= 2091289600000)
  plan(multisession, workers = n_coeur_final)
  tb_paires_valides_ddns <- future.apply::future_lapply(v_ddn_locales, fn_blocage_ddn)
  

} else {
 
tb_paires_valides_ddns <- map(v_ddn_locales, fn_blocage_ddn) }

tb_paires_valides_ddns <- do.call(bind_rows,tb_paires_valides_ddns)




## d) Blocking on the four fisrt character of the first name and the family name


# print message
print("    blocking on the four first characters of the first name/surname.")


# firstname/surname blocking fuction
fn_blocage_prenomnom <- function(char_prenomnom){
  require(tidyverse)
  require(stringi)
  require(lubridate)
  require(stringdist)

  tb_patients_locaux_nomsel <-
    tb_patients_locaux %>%
    filter(gp_prenomnom==char_prenomnom)

  tb_fpd_insee_nomsel <-
    tb_fpd_insee %>%
    filter(gp_prenomnom==char_prenomnom)


  # extraction des paires qui respectent les conditions


  tb_patients_locaux_nomsel %>%
    full_join(tb_fpd_insee_nomsel,by="gp_prenomnom") %>%
    fn_compte()

}

# extraction of all combinaisons of character to which the blocking function will be applied
v_gp_prenomnom_locales <-
  tb_patients_locaux %>%
  pull(gp_prenomnom) %>%
  unique()


# application of the matching, on a single or multiple cores

if(n_coeur_final>1){
  
plan(multisession, workers = n_coeur_final)
tb_paires_valides_prenomnom <- future.apply::future_lapply(v_gp_prenomnom_locales, fn_blocage_prenomnom) } else {



tb_paires_valides_prenomnom <- map(v_gp_prenomnom_locales, fn_blocage_prenomnom)

}

tb_paires_valides_prenomnom <- do.call(bind_rows,tb_paires_valides_prenomnom)


# 7) Selection of the most pertinent valid paires (if used)
# More explanations in the article

defaultW <- getOption("warn")
options(warn = -1)



tb_prett <- 
  tb_paires_valides_ddns %>% 
  bind_rows(tb_paires_valides_prenomnom)  %>%
  distinct() %>%
  group_by(id_local) %>%
  add_tally() %>%
  ungroup() 



tb_apparie_1 <-
  tb_prett %>%
  filter(n==1) %>%
  select(id_local,id_insee) 



tb_apparie_sup1_aumoinsunedist0 <-
tb_prett %>%
  filter(n>1) %>%
  select(-n) %>%
  filter(dl_prenom_min==0|dl_nom_min==0|dl_ddn==0 |dl_sexe==0 )%>%
  select(id_local,id_insee) 


if(presence_lib_lieu_naissance) {
tb_apparie_sup1_sansdist0 <-
tb_prett %>%
    filter(!id_local %in% unique(c(tb_apparie_1$id_local,tb_apparie_sup1_aumoinsunedist0$id_local))) %>%
    filter(dl_comm_naiss_min==min(dl_comm_naiss_min)) %>%
    select(id_local,id_insee) 
     
  
}else{
  
  tb_apparie_sup1_sansdist0 <-
    tb_prett %>%
    filter(!id_local %in% unique(c(tb_apparie_1$id_local,tb_apparie_sup1_aumoinsunedist0$id_local))) %>%
    filter(dl_total==min(dl_total)) %>%
    select(id_local,id_insee) }
  
options(warn = defaultW)
  
tb_corres <- 
  bind_rows(tb_apparie_1,tb_apparie_sup1_aumoinsunedist0,tb_apparie_sup1_sansdist0) %>%
  select(id_local,id_insee) %>%
  distinct() %>%
  arrange(id_local)

# 8) Finalization of the output tibble.

# print the information


print("Output tibble(s).")
print("   generation of the output tibble(s).")
  
# remove useless data and free the RAM
rm(list = c("max_birthdate_dld", "max_surname_dld", 
     "max_firstname_dld", "max_gender_dld", "max_total_dld", "fn_blocage_ddn", 
     "fn_blocage_prenomnom", "fn_compte", "fn_noms_tb_min", 
     "fn_norm_chaine", "fn_norm_localite", 
     "female_modality", "male_modality", "n_core_to_use", 
     "presence_lib_lieu_naissance", "tb_apparie_1", "tb_apparie_sup1_aumoinsunedist0", 
     "tb_apparie_sup1_sansdist0", "tb_fpd_insee", "tb_paires_valides_ddns", 
     "tb_paires_valides_prenomnom", "tb_patients_locaux", 
     "tb_prett", "v_ddn_locales", "v_gp_prenomnom_locales", "v_variables_a_croiser", 
     "birth_date_variable", "birth_place_variable", "birth_surname_variable", 
     "use_surname_variable", "firstname_variable", "gender_variable"),envir =  environment())  
gc()  


# importation of the variables of the FNMD usefull for user experience
tb_insee <-  
  fn_import_insee() %>%
  select(c("nom_fpd", 
           "prenoms_fpd", "sexe_fpd", "date_naissance_fpd", "code_insee_naissance_fpd", 
           "libelle_commune_naissance_aff_fpd", "pays_naissance_fpd", "date_deces_fpd", 
           "code_insee_deces_fpd", "libelle_commune_deces_tbc_fpd","id_insee"))

if(english_language==T){
  tb_insee <-
  tb_insee %>%
    rename("surname_fnmd"="nom_fpd", 
           "first_names_fnmd"="prenoms_fpd",
           "gender_fnmd"="sexe_fpd", 
           "birthdate_fnmd"="date_naissance_fpd",
           "insee_birth_code_fnmd"="code_insee_naissance_fpd", 
           "birth_city_name_fnmd"="libelle_commune_naissance_aff_fpd",
           "country_birth_name_fnmd"="pays_naissance_fpd",
           "death_date_fnmd"="date_deces_fpd", 
           "insee_death_city_code"="code_insee_deces_fpd", 
           "insee_death_city_name"="libelle_commune_deces_tbc_fpd")
  
  
}

# Creation of the simplified and short output tibbles
local_tb <-
local_tb%>%
  mutate(id_local=row_number())


tb_corres_simplifie <-
  tb_corres %>%
  select(id_local) %>%
  distinct() %>%
  mutate(pres_id_insee="oui")

tb_short <- 
  local_tb %>% 
  left_join(tb_corres_simplifie,by = "id_local")  %>%
  select(-id_local) %>%
  mutate(matched_once_or_more=ifelse(is.na(pres_id_insee),F,T)) %>%
  select(-pres_id_insee) 

tb_long <-
  local_tb %>% 
  left_join(tb_corres,by = "id_local") %>%
  left_join(tb_insee, by = "id_insee") %>%
  select(-id_local) %>%
  select(-id_insee)

# output tibble

if(automatic_saving==T){
print("   automatic export of the short/long output tibbles in the data folder. Choose the automatic_saving = F in the function arguments to disable it.")
write_csv2(tb_short,"data/short_output.csv",na = "")
write_csv2(tb_long,"data/long_output.csv",na = "")}

if(output_short==T){tb_short} else{tb_long}

  }
