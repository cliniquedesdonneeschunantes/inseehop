library(tidyverse)

# Step 1 selection of the working directory where the execution script is located :


## For RStudio environment
setwd(dirname(rstudioapi::getActiveDocumentContext()$path))
## For non Rstudio environment
setwd(getSrcDirectory()[1])


# Step 2 : compilation and cleaning of the french national mortality database (FNMD) files 
## !! This step must be performed only when a FNMD file that you are interested in is available on the INSEE website, see readme. !!
## the compiled and cleaned file produced for step 3 ('fpd_nettoye.csv') is saved in the data folder.

source("functions/inseehop_cleaning_compiling.R")

fichier_nettoye <- 
  fn_compile_fichiers_insee()

print("Free the RAM for the next steps:")

gc()

fichier_nettoye <- 
  fn_enrichissement_libelle_ville()

print("Export of the cleaned and compiled FNMD file in data file.")

write_csv2(fichier_nettoye,"data/fpd_nettoye.csv",na = "")

rm(list=c("fichier_nettoye", "fn_compile_fichiers_insee", "fn_enrichissement_libelle_ville", 
     "fn_noms_tb_min", "fn_norm_chaine", "fn_is_date", "fn_nettoyage_fichier_deces"))

gc()


# step 3 : matching/linkage of the local database to the FNMD.
source("functions/inseehop_matching.R")

# Use the mapping function to match/link your local database to the FNMD
# an example is available

# tb_hospital <-
# read_delim(...)
# 
# results <-
# mapping_fn(local_tb = tb_hospital,
# 
#            available_ram_go = 15,
# 
#            birth_date_variable = "birthdate_hospital",
# 
#            gender_variable = "gender_hospital",
# 
#            male_modality = "M",
# 
#            female_modality = "F",
# 
#            birth_surname_variable = "birthname_hospital",
# 
#            use_surname_variable = "surname_hospital",
# 
#            firstname_variable = "firstname_hospital"
#           )




