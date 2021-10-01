library(tidyverse)
library(stringi)
library(lubridate)

# I) Usefull functions
## 1) String normalization function
## 2) tolower tibble names function
## 3) Test date function
# II) Original file cleaning function
# III) Compilation function
# IV) City name cleaning function



# I) Usefull functions
##  1) String normalization function

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


## 2) tolower tibble names function
fn_noms_tb_min <- function(nom_tb){
  names(nom_tb) <- tolower(names(nom_tb))
  return(nom_tb)
}

# 3) Test date function
fn_is_date = function(x) {
  formatted = try(as.Date(x, "%Y-%m-%d"), silent = TRUE)
  return(ifelse(is.na(as.character(formatted) == x),F,T))
}



# II) Original file cleaning function
fn_nettoyage_fichier_deces = function(fichier_a_nettoyer){
  tb_corres_sexe <- tibble(sexe=c("1","2"),sexe_norm=c("m","f"))
  tb_corres_com_deces <- 
    read_delim("data/France2017.txt", 
               "\t", escape_double = FALSE, col_types = cols(ARTICLCT = col_character()), 
               locale = locale(encoding = "ASCII"), 
               trim_ws = TRUE) %>%
    fn_noms_tb_min() %>%
    mutate(code_commune=paste0(dep,com)) %>%
    mutate(artmin=gsub("\\(|\\)","",artmin)) %>%
    mutate(nccenr=ifelse(is.na(artmin),nccenr,paste0(artmin,nccenr))) %>%
    select(code_commune,nccenr) %>%
    group_by(code_commune) %>%
    # Des doublons tous parfaitement cohérents
    add_tally() %>%
    filter(n==1) %>%
    select(-n) %>%
    rename("code_insee_deces"="code_commune",
           "libelle_commune_deces_tbc"="nccenr")
  
  fichier_a_nettoyer <-
    
    fichier_a_nettoyer %>%
    
    # Suppress empty line
    dplyr::filter(datedeces != '') %>%
    
    # Separate surname and first names block
    separate(col = nomprenom, into = c("nom_src", "prenoms"), sep = "\\*") %>%
    
    # Separate the first names block in individual first names 
    dplyr::mutate(prenoms = stringr::str_remove(prenoms, "/"),
                  prenoms = stringr::str_trim(prenoms, side = "right"),
                  prenoms_liste = prenoms) %>%
    
    # Selection of the two first first names
    separate(col = prenoms_liste, into = c("prenom_1_src", "prenom_2_src"), sep = " ",extra = "drop",fill = "right") %>%
    
    # Creation of the first element of first name and creation of the two first first name
    dplyr::mutate(prenom_0_src = sub("(.*)-.*", "\\1",prenom_1_src),
                  prenom_1_2_src = ifelse(is.na(prenom_2_src),prenom_1_src,paste0(prenom_1_src, prenom_2_src))) %>%
    
    
    
    # Normalizsation of the names/surnames
    dplyr::mutate(nom_norm = fn_norm_chaine(nom_src),
                  prenom_0_norm = fn_norm_chaine(prenom_0_src),
                  prenom_1_norm = fn_norm_chaine(prenom_1_src),
                  prenom_1_2_norm = fn_norm_chaine(prenom_1_2_src)
                  
    )%>%
    
    # renaming variables.
    dplyr::rename(date_naissance = datenaiss,
                  code_insee_naissance = lieunaiss,
                  pays_naissance = paysnaiss,
                  date_deces = datedeces,
                  code_insee_deces = lieudeces
    )   %>%
    
    # Birth dates and death dates : verfications, tests of valid configuration if not valid initialy
    dplyr::mutate(
      date_naissance = if_else(date_naissance == 0, "00000000", date_naissance,missing = "00000000"),
      annee_naissance = stringr::str_sub(date_naissance, 1, 4),
      jour_naissance = stringr::str_sub(date_naissance, 7, 8),
      jour_naissance_o = jour_naissance,
      mois_naissance = stringr::str_sub(date_naissance, 5, 6),
      validite_date_originale =if_else(fn_is_date(paste0(annee_naissance,'-',mois_naissance,'-',jour_naissance)),T,F,missing = F),
      jour_naissance = ifelse(validite_date_originale==T,jour_naissance,
                              ifelse(fn_is_date(paste0(annee_naissance,'-',jour_naissance,mois_naissance)),mois_naissance,"01")),
      mois_naissance = ifelse(validite_date_originale==T,mois_naissance,
                              ifelse(fn_is_date(paste0(annee_naissance,'-',jour_naissance_o,mois_naissance)),jour_naissance_o,"01")),
      date_naissance = if_else(fn_is_date(paste0(annee_naissance,'-',mois_naissance,'-',jour_naissance)),as_date(paste0(annee_naissance,"-",mois_naissance,"-",jour_naissance)),NA_Date_),
      jour_deces = stringr::str_sub(date_deces, 7, 8),
      jour_deces = ifelse(jour_deces=="00","01",jour_deces),
      mois_deces = stringr::str_sub(date_deces, 5, 6),
      mois_deces = ifelse(mois_deces=="00","01",mois_deces),
      annee_deces = stringr::str_sub(date_deces, 1, 4),
      date_deces = if_else(fn_is_date(paste0(annee_deces,"-",mois_deces,"-",jour_deces)),as_date(paste0(annee_deces,"-",mois_deces,"-",jour_deces)),NA_Date_))%>%
  #  Selection of the usefull variables
  dplyr::select(c("nom_norm", "prenom_0_norm","prenom_1_norm", "prenom_1_2_norm","nom_src","prenoms", "sexe", "date_naissance", "code_insee_naissance", 
                  "commnaiss",
                  "pays_naissance", "date_deces","code_insee_deces" )) %>%
  rename(nom=nom_src) %>%
  left_join(tb_corres_com_deces,by = "code_insee_deces") %>%
  left_join(tb_corres_sexe, by = "sexe")




names(fichier_a_nettoyer) <- paste0(names(fichier_a_nettoyer),"_fpd")



return(fichier_a_nettoyer)
}

# III) Compilation function
fn_compile_fichiers_insee = function() {
  
  
  
  # read the csv function
  fn_lit_fichier_csv = function(fichier_csv){
    
    read.csv2(file.path(paste0("data/",fichier_csv)), 
              header=TRUE, 
              sep=";",
              stringsAsFactors = FALSE) %>%
      mutate_all(as.character)


  }
  
  # creation of the empty compiled file
  fichier_insee_compile = ""
  
  # list of the original insee files in the source folder
  if(length(list.files("data")[str_detect(list.files("data"),"deces-")])==0){stop("No original FNMD file found in the data folder.")}
  liste_fichiers_insee <- list.files("data")[str_detect(list.files("data"),"deces-")]
  
  print("Import and cleaning of the French national mortality database (FNMD) files:")

  
  # Importation and cleaning of the insee files
  for(i in 1:length(liste_fichiers_insee)){
    print(paste0("   ",liste_fichiers_insee[i]))
    fichier_courant = fn_nettoyage_fichier_deces(fn_lit_fichier_csv(liste_fichiers_insee[i]))
    fichier_insee_compile <- rbind(fichier_insee_compile, fichier_courant)
  }
  print("   End of import. Some records of the FNMD are not valid and will throw a warning message, it's normal.")
  
  return(as_tibble(fichier_insee_compile) %>%
           rename(libelle_commune_naissance_aff_fpd="commnaiss_fpd"))
}



# IV) City name cleaning

fn_enrichissement_libelle_ville <- function(){ 
tb_temp_1 <-
  fichier_nettoye %>%
  select(libelle_commune_naissance_aff_fpd) %>%
  distinct() 


tb_arrond <-
  tb_temp_1 %>%
  mutate(libelle_commune_naissance_aff_fpd=iconv(libelle_commune_naissance_aff_fpd, 'UTF-8', 'UTF-8')) %>%
  filter(str_detect(tolower(libelle_commune_naissance_aff_fpd),"arrond"))

tb_chiffre <-
  tb_temp_1 %>%
  filter(str_detect(tolower(libelle_commune_naissance_aff_fpd),"[[:digit:]]+"))

gc()

tb_paris_autres <-
  tb_temp_1 %>%
  filter(str_detect(string = tolower(libelle_commune_naissance_aff_fpd),pattern = "paris ")) %>%
  filter(!tolower(libelle_commune_naissance_aff_fpd)%in%tolower(tb_arrond$libelle_commune_naissance_aff_fpd)) %>%
  filter(!tolower(libelle_commune_naissance_aff_fpd)%in%tolower(tb_chiffre$libelle_commune_naissance_aff_fpd)) %>%
  filter((!str_detect(tolower(libelle_commune_naissance_aff_fpd),"touquet")))

gc()

tb_recodage_fpd_libelle_commune <-
  bind_rows(tb_arrond,tb_chiffre,tb_paris_autres) %>%
  mutate(libelle_commune_naissance_simplifie_fpd=gsub(pattern = "([[:alpha:]]+).*", replacement = "\\1", x= tolower(libelle_commune_naissance_aff_fpd))) %>%
  distinct()

gc()

fichier_nettoye <-
  fichier_nettoye %>%
  left_join(tb_recodage_fpd_libelle_commune,by = "libelle_commune_naissance_aff_fpd") %>%
  mutate(libelle_commune_naissance_norm_fpd=ifelse(is.na(libelle_commune_naissance_simplifie_fpd),fn_norm_chaine(libelle_commune_naissance_aff_fpd),fn_norm_chaine(libelle_commune_naissance_simplifie_fpd)))

gc()

fichier_nettoye <-
fichier_nettoye %>%
  filter(nom_norm_fpd!="") %>%
  select(-libelle_commune_naissance_simplifie_fpd)




}







