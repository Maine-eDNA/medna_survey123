###############################
# medna survey123
# General functions reused within edna_survey123 project
# CREATED BY: mkimble
# CREATED DATE: 10-06-2020
###############################

###############################
# Install or load Libraries
###############################
install_or_load_pack <- function(pack){
  # https://nhsrcommunity.com/blog/a-simple-function-to-install-and-load-packages-in-r/
  # Swapped to this code, works better
  # install_or_load_pack(pack)
  # pack: expects list of libraries, e.g., pack<-c("tidyverse","tm","wordcloud","ggwordcloud","topicmodels")
  create_pkg <- pack[!(pack %in% installed.packages()[, "Package"])]
  if (length(create_pkg))
    install.packages(create_pkg, dependencies = TRUE)
  sapply(pack, require, character.only = TRUE)
}

load_pack <- function(pack){
  # load_pack(pack)
  # pack: expects list of libraries, e.g., pack<-c("tidyverse","tm","wordcloud","ggwordcloud","topicmodels")
  sapply(pack, require, character.only = TRUE)
}

###############################
# Generate labels
###############################
create_ifnot_labels <- function(outputFolderName, outputFileName, inputLabels, overwrite=FALSE) {
  # If labels do not exist, create them. If they do exist and overwrite = FALSE, load them.
  load_pack(c())
  outputFile = paste(outputFolderName,outputFileName,".RData",sep="")
  if (!file.exists(outputFile) | overwrite) {
    warning(paste("\nCreating new labels:",outputFileName,"\n"))
    
  } else {
    warning(paste("\nAlready exists, loading:",outputFileName,"\n"))
    # RData already exists & overwrite == FALSE; load saved RData object
    labels_ld<-load(file=outputFile)
    labels <- get(labels_ld)
    rm(labels_ld)
  }
  return(labels)
}
###############################
# Generate labels
###############################
convert_ifnot_dt_date <- function(inputDf, dateCols, lapply=lubridate::ymd, ...) {
  library(lubridate)
  library(data.table)
  # If not already DT, convert
  if ("data.table" %in% class(inputDf)) {
    warning(paste("\nAlready data.table, formatting dates\n"))
    # lubridate::ymd
    inputDf[, (date_cols) := lapply(.SD, ...), .SDcols = date_cols]
  } else{
    warning(paste("\nConverting to data.table and formatting dates\n"))
    # lubridate::ymd
    setDT(inputDf)[, (date_cols) := lapply(.SD, ...), .SDcols = date_cols]
  }
  return(inputDf)
}

###############################
# s123
###############################
add_seasons_df <- function(df) {
  df<-df %>%
    mutate(season = case_when(survey_month==12 | survey_month==1 | survey_month==2 ~ "Winter", 
                              survey_month==3 | survey_month==4 | survey_month==5 ~ "Spring",
                              survey_month==6 | survey_month==7 | survey_month==8 ~ "Summer",
                              survey_month==9 | survey_month==10 | survey_month==11 ~ "Fall")) %>%
    mutate(season_year = paste0(season," ",survey_year))
  return(df)
}
