
# INSEEHOP
## Presentation

This package provides a Damerau-Levenshtein distances (DLD) based algorithm to link/match a local database of subjects with the French national mortality database (FNMD - Fichier des personnes décédées de l'INSEE). This algorithm :
- can manage a local database with millions of subjects.
- cas run on a local, modest or powerfull configuration.
- has been evaluated with high metholody standarts.

For more details, you can refer to the following article :

## Usage
 1. Download this github repository.
 2. If you haven't already, [install R](https://cran.rstudio.com/) or update it.
 3. Download and unzip the INSEE FNMD files that you want to link with your database.
- INSEE csv files are not provided here but can be downloaded on INSEE website : <https://www.insee.fr/fr/information/4190491>. 
- Be careful when downloading INSEE FNMD files to select .csv files and not .txt files, the R scripts presented here work only with .csv files.
 4. Put the downloaded and unzip INSEE FNMD files and your local database file in the _data_ folder of this repository.
 5. In _execution_script.R_ :  
- run the first step to select the good the working directory.
- run the second step to clean and compile the originale FNMD INSEE files.
- import your local database in the main environement and check it fullfills all the requirements (see requirements).
```r
# Example 1
tb_nantes <- read_delim("hospital_patients.csv",
						";",
						escape_double = FALSE,
						col_types = cols(birthdate_hospital = col_date(format = "%Y-%m-%d")) 
```
- run the third step to match your local database to the compiled and cleaned FNMD file. 
```r
 # Example 2
 resultats <- mapping_fn(local_tb = tb_nantes,
 						available_ram_go = 15,
 						birth_date_variable = "birthdate_hospital",
 						gender_variable = "gender_hospital",
 						male_modality = "M",
 						female_modality = "F",
 						birth_surname_variable = "birthname_hospital",
 						use_surname_variable = "surname_hospital",
 						firstname_variable = "firstname_hospital")
```

## Requirements

### Required packages
```r
install.packages(c("tidyverse", "stringi", "lubridate", "stringdist","parallel","future","future.apply","rlang"))
```
### Local database requirements

Your local database must be a tibble: dataframes are depricated for the tidyverse.
You can use dplyr::as_tibble() ton convert your database in a tibble.
See https://r4ds.had.co.nz/tibbles.html

To be linked to the FNMD, the local database:
* **must** contains :
 * **birth name** [type: `character`] **AND/OR** **surname** [type: `character`]
 ** both should be used if possible. At least one of them is necessary to run the script.
 * **first name** [type: `character`]
 * **birth date** [type: `Date`]
 ** if you have only the year of birth in your database, you can set the date as 1st january ("YYYY-01-01").
* **should** contain **gender** [type: `character`]
 * if _gender_ is missing then the value is set to "_unknown_" by the algorithm.
* **can** contain **city name of birth** (optional, see article) [type: `character`]


### Requiered parameters for the matching function mapping_fn

| Parameter                |     Type    | Description                                                                                             |
|--------------------------|:-----------:|---------------------------------------------------------------------------------------------------------|
| ram_go_dispc             |  `numeric`  | Amount of RAM allocated in Go for calculation, in gigabytes.                                                  |
| local_tb                 |   `tibble`  | Tibble to be linked to the FNMD.                                                                          |
| birth_date_variable      | `character` | Birth date variable name in the local database.                                                               |
| gender_variable          | `character` | Gender variable name in the local database. If gender not available, create an empty column for the gender.                                                                  |
| female_modality          | `character` | Coding of the female gender modality in the local database (Example: "F")                                     |
| male_modality            | `character` | Coding of the masculin gender modality in the local database (Example: "M")                                   |
| firstname_variable       | `character` | First name variable name in the local database.                                                               |
| birth_surname_variable  | `character` |  Birth surname variable name in the local database. **If only use surname available, let empty.** 
| use_surname_variable  | `character` |   Use surname variable name in the local database. **If only birh surname avaialble, let empty.**

**Beware : the mapping function can run even if you entered wrong parameters for female_modality and male_modality. Please check theim twice**

### Optionnal parameters for the matching function mapping_fn

| Parameter               |     Type    | Default value | Description                                                                                                                                                                                                                            |
|-------------------------|:-----------:|:-------------:|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
                                                                                                                                                                                          |
| n_core_to_use           |  `numeric`  |      'NA'     | Number of cores allocated for calculation. By default, the algorithm will automatically choose the most adapated .                                                                                                                                                                                             |
| max_total_dld           |  `numeric`  |      `2`      | Maximum total Damereau-Levenshtein distance.                                                                                                                                                                                     |
| max_firstname_dld       |  `numeric`  |      `2`      | Maximum first name Damereau-Levenshtein distance.                                                                                                                                                                                |
| max_surname_dld         |  `numeric`  |      `1`      | Maximum surname Damereau-Levenshtein distance .                                                                                                                                                                                   |
| max_birthdate_dld       |  `numeric`  |      `1`      | Maximum birth date Damereau-Levenshtein distance.                                                                                                                                                                                |
| max_gender_dld          |  `numeric`  |      `1`      | Maximum gender Damereau-Levenshtein distance. 0 or 1.                                                                                                                                                                                    |
| output_short            |  `boolean`  |    `False`    | Output tibble format.<br>- `True` : output tibble is the local database with a new column "_matched_once_or_more_" which is set to "_yes_" if the subject is linked  at least once with the FNMD file, and set to "_no_" otherwise.<br>- `False` : output tibble is a join of the local database and the FNDM file ; multiple line per subject if multiple records from the FNMD matched. |
| english_language        |  `boolean`  |    `True`     | Variable language of the output database.<br>- `True` : variables are in English.<br>- `False` : variables are in French.                                                                                                     |
| automatic_saving        |  `boolean`  |    `True`     | Output results automatic saving.<br>- `True` : saved in the _data_ folder.<br>- `False` : not saved                                                                                                       |
| death_date_lower_limit   | `character` |""| Format: "AAAA-MM-DD". Earliest death date of interest. If value let by default, all the records from the FNMD will be used. |
| death_date_upper_limit   | `character` |""| Format: "AAAA-MM-DD". Latest deat date of interest.  If value let by default, all the records from the FNMD will be used.     |

