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
# s123 - add seasons
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
###############################
# s123 - subset by project
###############################
subset_prj_df <- function(df_bplot, site_id_prjs, selected_projects) {
  selected_projects <- gsub("[()]", "", selected_projects)
  selected_projects <- tolower(selected_projects)
  
  df_sub_prj <- df_bplot %>%
    dplyr::mutate(project_ids=na_if(project_ids,"")) %>%
    dplyr::mutate(project_ids = replace_na(project_ids, "Maine eDNA")) %>%
    dplyr::left_join(site_id_prjs) %>%
    dplyr::mutate(sid_prjs_sub = gsub(",", " ", sid_prjs)) %>%
    dplyr::mutate(sid_prjs_sub = gsub("[()]", "",sid_prjs_sub)) %>%
    dplyr::mutate(sid_prjs_sub = tolower(sid_prjs_sub)) %>%
    dplyr::filter(grepl(paste(selected_projects, collapse="|"), sid_prjs_sub)) %>%
    dplyr::select(-sid_prjs_sub)
  
  return(df_sub_prj)
}

###############################
# s123 Tables - Sites with measurements
###############################
create_table_sids <- function (df_map, site_ids_sfdf){
  rcdext_sids_all <- df_map %>%
    dplyr::filter(!if_any(c(site_id), is.na)) %>%
    dplyr::select(site_id) %>%
    dplyr::filter(site_id!="other") %>%
    dplyr::distinct() %>%
    dplyr::left_join(as.data.frame(site_ids_sfdf)) %>%
    dplyr::mutate(RecordsExist="Yes") %>%
    dplyr::select(site_id, RecordsExist) %>%
    dplyr::full_join(as.data.frame(site_ids_sfdf)) %>%
    dplyr::mutate(id = row_number()) %>%
    dplyr::mutate(id = paste0("s",id)) %>%
    dplyr::mutate(RecordsExist = ifelse(is.na(RecordsExist), "No", RecordsExist)) %>%
    dplyr::select(id, site_id, general_location_name, RecordsExist, 
                  project_ids, system_type, watershed_code, lon, lat) %>%
    dplyr::arrange(site_id) 
  
  rcdext_sids_other <- df_map %>%
    dplyr::filter(!if_any(c(site_id), is.na)) %>%
    dplyr::filter(site_id == "other") %>%
    dplyr::filter(lat_manual > 0 | long_manual > 0) %>%
    dplyr::select(site_id, general_location_name, project_ids, lat_manual, long_manual) %>%
    dplyr::distinct() %>%
    dplyr::rename(lat=lat_manual) %>%
    dplyr::rename(lon=long_manual) %>%
    dplyr::mutate(RecordsExist="Yes") %>%
    dplyr::mutate(watershed_code="other") %>%
    dplyr::mutate(system_type="other") %>%
    dplyr::mutate(id = row_number()) %>%
    dplyr::mutate(id = paste0("o",id)) %>%
    dplyr::select(id, everything()) %>%
    dplyr::arrange(general_location_name)
  
  if (nrow(rcdext_sids_other)>0) {
    rcdext_df <- rbind(data.table(rcdext_sids_all), data.table(rcdext_sids_other), fill=TRUE)
  } else {
    rcdext_df <- as.data.frame(rcdext_sids_all)
    
    return(rcdext_df)
  }
}

###############################
# s123 Maps - Sites with measurements formatted
###############################
fmt_sids_table <- function(rcdext_df) {
  rcdext_fmt_df <- rcdext_df %>%
    dplyr::select(site_id, general_location_name, project_ids, RecordsExist) %>%
    dplyr::rename(SiteID = site_id) %>%
    dplyr::rename(SiteName = general_location_name) %>%
    dplyr::rename(Projects = project_ids)
  rcdext_fmt_df$SiteName <- stringr::str_wrap(rcdext_fmt_df$SiteName, 40)
  rcdext_fmt_df$Projects <- stringr::str_wrap(rcdext_fmt_df$Projects, 40)
  return(rcdext_fmt_df)
}

create_table_envmeas_sids <- function(survey_envmeas_join, select_var_maps){
  #select_var_maps = "turbidity"
  rcdext_df <- create_table_sids(survey_envmeas_join  %>%
                                   dplyr::mutate(env_measurements=na_if(env_measurements, "")) %>%
                                   dplyr::filter(!if_any(c(select_var_maps, site_id, env_measurements), is.na)), 
                                 site_ids_sfdf)
  return(rcdext_df)
}

create_table_envmeas_sids_fmt <- function(survey_envmeas_join, select_var_maps){
  #select_var_maps = "turbidity"
  rcdext_df <- create_table_envmeas_sids(survey_envmeas_join, select_var_maps)
  rcdext_fmt_df <- fmt_sids_table(rcdext_df)
  return(rcdext_fmt_df)
}

create_table_col_sids <- function(survey_collection_join, select_var_maps) {
  if (select_var_maps == "collection_type"){
    df_map = survey_collection_join %>%
      dplyr::mutate(collection_type=na_if(collection_type, "")) %>%
      dplyr::filter(!if_any(c(collection_type, site_id), is.na))
  } else {
    df_map <- survey_collection_join %>%
      dplyr::mutate(collection_type=na_if(collection_type, "")) %>%
      dplyr::filter(!if_any(c(collection_type, site_id), is.na) & collection_type==select_var_maps)
  }
  rcdext_df <- create_table_sids(df_map, site_ids_sfdf)
  return(rcdext_df)
}

create_table_col_sids_fmt <- function(survey_collection_join, select_var_maps){
  #select_var_maps = "water_sample"
  rcdext_df <- create_table_col_sids(survey_collection_join, select_var_maps)
  rcdext_fmt_df <- fmt_sids_table(rcdext_df)
  return(rcdext_fmt_df)
}

create_table_colmeas_sids <- function(survey_collection_join, select_var_maps){
  #select_var_maps = "water_depth"
  rcdext_df <- create_table_sids(survey_collection_join %>%
                                   dplyr::mutate(collection_type=na_if(collection_type, "")) %>%
                                   dplyr::filter(!if_any(c(select_var_maps, collection_type, site_id), is.na)), 
                                 site_ids_sfdf)
  return(rcdext_df)
}

create_table_colmeas_sids_fmt <- function(survey_collection_join, select_var_maps){
  #select_var_maps = "water_depth"
  rcdext_df <- create_table_colmeas_sids(survey_collection_join, select_var_maps)
  rcdext_fmt_df <- fmt_sids_table(rcdext_df)
  return(rcdext_fmt_df)
}

create_table_filters_sids <- function(clean_filter_join, select_var_maps){
  #select_var_maps = "water_depth"
  if (select_var_maps == "filter_type"){
    rcdext_df <- create_table_sids(clean_filter_join %>%
                                     dplyr::mutate(filter_type=na_if(filter_type, "")) %>%
                                     dplyr::filter(!if_any(c(survey_global_id, gid, filter_type, filter_label), is.na)),
                                   site_ids_sfdf)
  } else {
    rcdext_df = create_table_sids(clean_filter_join %>%
                                    dplyr::mutate(filter_type=na_if(filter_type, "")) %>%
                                    dplyr::filter(!if_any(c(survey_global_id, gid, filter_type, filter_label), is.na) & filter_type==select_var_maps),
                                  site_ids_sfdf)
  }
  return(rcdext_df)
}

create_table_filters_sids_fmt <- function(clean_filter_join, select_var_maps){
  #select_var_maps = "water_depth"
    rcdext_df <- create_table_filters_sids(clean_filter_join, select_var_maps)
  
  rcdext_fmt_df <- fmt_sids_table(rcdext_df)
  return(rcdext_fmt_df)
}

create_table_subcores_sids <- function(clean_subcore_join) {
  rcdext_df <- create_table_sids(clean_subcore_join %>%
                                   dplyr::filter(!if_any(c(gid, survey_global_id, collection_global_id), is.na)),
                                 site_ids_sfdf)
  return(rcdext_df)
  
}

create_table_subcores_sids_fmt <- function(clean_subcore_join) {
  rcdext_df <- create_table_subcores_sids(clean_subcore_join)
  rcdext_fmt_df <- fmt_sids_table(rcdext_df)
  return(rcdext_fmt_df)
  
}

###############################
# s123 Maps - Sites with measurements
###############################
create_map_sids <- function(boundingbox_sids, countries_sfdf, WBD_sfdf, lakes_sfdf, rivers_sfdf, site_ids_sfdf, rcdext_df, title_var) {
  ## SURVEY SUMMARY MAP SETUP
  rcdext_df_sfdf <- rcdext_df %>%
    sf::st_as_sf(., coords=c('lon', 'lat'), crs=WGS84_SRID) %>% # st_as_sf combines fields into geometry field to create sf object
    dplyr::mutate(geom=gsub(geometry, pattern="(\\))|(\\()|c", replacement="")) %>% # remove geometry characters
    tidyr::separate(geom, into=c("lon","lat"), sep=",") # reseparate out lat, long
  
  
  #outputPngFileName <- file.path(outputPngFolder,paste0("s123_envmeas_all_sids_map.png"))
  plotTitle = sprintf("Maine-eDNA Sites with %s", title_var)
  ggplot() + 
    geom_sf(data=countries_sfdf, color="grey", fill="antiquewhite") +
    geom_sf(data=WBD_sfdf, aes(fill=region_cod), alpha=0.1) +
    geom_sf(data=lakes_sfdf, color="lightblue", fill="lightblue") +
    geom_sf(data=rivers_sfdf, color="lightblue", fill="lightblue") +
    geom_text(data=WBD_sfdf, aes(x=lon, y=lat, label=region_cod),
              color="black", fontface="bold", size=2, check_overlap=FALSE) +
    geom_sf(data=site_ids_sfdf, fill="antiquewhite", pch=21, color="black", alpha=0.7) +
    geom_sf(data=rcdext_df_sfdf, fill="forestgreen", pch=21, color="black", alpha=0.7) +
    #geom_sf_text(data=survey_envmeas_join_sids_all_sfdf, aes(x=lon, y=lat, label=site_id),
    #          color="black", fontface="bold", size=2, check_overlap=FALSE) +
    coord_sf(xlim = c(boundingbox_sids$xmin[[1]]*1.01, boundingbox_sids$xmax[[1]]*0.99), 
             ylim = c(boundingbox_sids$ymin[[1]]*0.99, boundingbox_sids$ymax[[1]]*1.01), expand = FALSE) + 
    theme(panel.grid.major = element_line(color = gray(.5), linetype = "dashed", size = 0.5), 
          panel.background = element_rect(fill = "aliceblue")) + 
    xlab("Longitude") + 
    ylab("Latitude") + 
    ggtitle(plotTitle) +
    theme(plot.title = element_text(hjust = 0.5),
          legend.position="none")
}

###############################
# s123 Tables - Count by Season
###############################
create_table_count_season <- function(df_sub_prj){
  df_sub_prj %>%
    dplyr::filter(!if_any(c(gid, season), is.na)) %>%
    dplyr::select(gid, season) %>% 
    dplyr::group_by(season) %>%
    dplyr::count(season) %>%
    dplyr::mutate(perc=round((n/nrow(df_sub_prj %>%
                                       dplyr::filter(!if_any(c(gid, season), is.na))))*100, 2)) %>%
    dplyr::mutate(season=factor(season, levels= c("Winter", "Spring", "Summer", "Fall"))) %>%
    dplyr::arrange(season)
}

###############################
# s123 Tables - Count by System
###############################
create_table_count_system <- function(df_sub_prj){
  df_sub_prj %>%
    dplyr::filter(!if_any(c(gid, system_type), is.na)) %>%
    dplyr::select(gid, system_type) %>% 
    dplyr::group_by(system_type) %>%
    dplyr::count(system_type) %>%
    dplyr::mutate(perc=round((n/nrow(df_sub_prj %>%
                                       dplyr::filter(!if_any(c(gid, system_type), is.na))))*100, 2)) %>%
    dplyr::mutate(system_type=factor(system_type, levels= c("Coast", "Estuary", "Stream", "Lake", "Aquarium", "other"))) %>%
    dplyr::arrange(system_type)
}

###############################
# s123 Tables - Count by Site
###############################
create_table_count_site <- function(df_sub_prj, site_ids_sfdf){
  df_sub_prj %>%
    dplyr::filter(!if_any(c(gid, survey_date, site_id), is.na)) %>%
    dplyr::select(gid, site_id) %>% 
    dplyr::group_by(site_id) %>%
    dplyr::count(site_id)  %>%
    dplyr::mutate(perc=round((n/nrow(df_sub_prj %>%
                                       dplyr::filter(!if_any(c(gid, survey_date, site_id), is.na))))*100, 2)) %>%
    dplyr::left_join(as.data.frame(site_ids_sfdf)) %>%
    dplyr::select(site_id, general_location_name, n, perc)
}

###############################
# s123 Tables - Count by Season and Year
###############################
create_table_count_season_year <- function(df_sub_prj){
  df_sub_prj %>%
    dplyr::filter(!if_any(c(gid, season_year), is.na)) %>%
    dplyr::select(gid, season_year) %>% 
    dplyr::group_by(season_year) %>%
    dplyr::count(season_year) %>%
    dplyr::mutate(perc=round((n/nrow(df_sub_prj %>%
                                       dplyr::filter(!if_any(c(gid, season_year), is.na))))*100, 2)) %>%
    tidyr::separate(season_year, into=c("season","year"),sep=" ") %>%
    dplyr::mutate(season=factor(season, levels= c("Winter", "Spring", "Summer", "Fall"))) %>%
    dplyr::arrange(season, year) %>%
    dplyr::left_join(df_sub_prj %>%
                       dplyr::filter(!if_any(c(gid, season_year), is.na)) %>%
                       dplyr::select(gid, season_year) %>% 
                       dplyr::group_by(season_year) %>%
                       dplyr::count(season_year) %>% 
                       tidyr::separate(season_year, into=c("season","year"),sep=" ") %>%
                       dplyr::group_by(season) %>%
                       dplyr::summarize(
                         avg_n_season = mean(n, na.rm=TRUE),
                         sd_n_season = sd(n, na.rm=TRUE))) %>% 
    mutate_if(is.numeric, round, 2)
}

###############################
# s123 Tables - Count by System and Year
###############################
create_table_count_system_year <- function(df_sub_prj){
  df_sub_prj %>%
    dplyr::filter(!if_any(c(gid, survey_year, system_type), is.na)) %>%
    dplyr::select(gid, survey_year, system_type) %>% 
    dplyr::mutate(sys_year=paste0(system_type, " ", survey_year)) %>%
    dplyr::group_by(sys_year) %>%
    dplyr::count(sys_year) %>%
    dplyr::mutate(perc=round((n/nrow(df_sub_prj %>%
                                       dplyr::filter(!if_any(c(gid, survey_year, system_type), is.na))))*100, 2)) %>%
    tidyr::separate(sys_year, into=c("system_type","year"),sep=" ") %>%
    dplyr::mutate(system_type=factor(system_type, levels= c("Coast", "Estuary", "Stream", "Lake", "Aquarium", "other"))) %>%
    dplyr::arrange(system_type, year) %>%
    dplyr::left_join(df_sub_prj %>%
                       dplyr::filter(!if_any(c(gid, survey_year, system_type), is.na)) %>%
                       dplyr::select(gid, survey_year, system_type) %>% 
                       dplyr::mutate(sys_year=paste0(system_type, " ", survey_year)) %>%
                       dplyr::group_by(sys_year) %>%
                       dplyr::count(sys_year) %>%
                       tidyr::separate(sys_year, into=c("system_type","year"),sep=" ") %>%
                       dplyr::group_by(system_type) %>%
                       dplyr::summarize(
                         avg_n_system = mean(n, na.rm=TRUE),
                         sd_n_system = sd(n, na.rm=TRUE))) %>% 
    mutate_if(is.numeric, round, 2)
}

###############################
# s123 Tables - Count by Month and Year
###############################
create_table_count_month_year <- function(df_sub_prj){
  df_sub_prj %>%
    dplyr::filter(!if_any(c(gid, survey_date), is.na)) %>%
    dplyr::select(gid, survey_date) %>%
    dplyr::mutate(survey_date = as.Date(survey_date, "%m/%d/%Y")) %>%
    dplyr::mutate(survey_date = lubridate::ymd(survey_date, tz="UTC")) %>%
    dplyr::mutate(survey_yrmo = format(survey_date, format="%b %Y")) %>%
    dplyr::group_by(survey_yrmo) %>%
    dplyr::count(survey_yrmo) %>%
    dplyr::mutate(perc=round((n/nrow(df_sub_prj %>%
                                       dplyr::filter(!if_any(c(gid, survey_date), is.na))))*100, 2)) %>%
    tidyr::separate(survey_yrmo, into=c("month","year"),sep=" ") %>%
    dplyr::mutate(month=factor(month, levels= c("Jan", "Feb", "Mar", 
                                                "Apr", "May", "Jun", 
                                                "Jul", "Aug", "Sep", 
                                                "Oct", "Nov", "Dec"))) %>%
    dplyr::arrange(month, year) %>%
    dplyr::left_join(df_sub_prj %>%
                       dplyr::filter(!if_any(c(gid, survey_date), is.na)) %>%
                       dplyr::select(gid, survey_date) %>%
                       dplyr::mutate(survey_date = as.Date(survey_date, "%m/%d/%Y")) %>%
                       dplyr::mutate(survey_date = lubridate::ymd(survey_date, tz="UTC")) %>%
                       dplyr::mutate(survey_yrmo = format(survey_date, format="%b %Y")) %>%
                       dplyr::group_by(survey_yrmo) %>%
                       dplyr::count(survey_yrmo) %>%
                       tidyr::separate(survey_yrmo, into=c("month","year"),sep=" ") %>%
                       dplyr::group_by(month) %>%
                       dplyr::summarize(
                         avg_n_month = mean(n, na.rm=TRUE),
                         sd_n_month = sd(n, na.rm=TRUE))) %>% 
    dplyr::mutate_if(is.numeric, round, 2)
}

###############################
# s123 Tables - Count by Site and Year
###############################
create_table_count_site_year <- function(df_sub_prj, site_ids_sfdf){
  df_sub_prj %>%
    dplyr::filter(!if_any(c(gid, survey_date), is.na)) %>%
    dplyr::select(gid, site_id, survey_year) %>% 
    dplyr::mutate(sid_year=paste0(site_id, " ", survey_year)) %>%
    dplyr::group_by(sid_year) %>%
    dplyr::count(sid_year) %>%
    dplyr::mutate(perc=round((n/nrow(df_sub_prj %>%
                                       dplyr::filter(!if_any(c(gid, survey_date), is.na))))*100, 2)) %>%
    tidyr::separate(sid_year, into=c("site_id","year"),sep=" ") %>%
    dplyr::left_join(as.data.frame(site_ids_sfdf)) %>%
    dplyr::select(site_id, general_location_name, year, n, perc) %>%
    dplyr::arrange(site_id, year) %>%
    dplyr::left_join(df_sub_prj %>%
                       dplyr::filter(!if_any(c(gid, survey_date), is.na)) %>%
                       dplyr::select(gid, site_id, survey_year) %>% 
                       dplyr::mutate(sid_year=paste0(site_id, " ", survey_year)) %>%
                       dplyr::group_by(sid_year) %>%
                       dplyr::count(sid_year) %>%
                       tidyr::separate(sid_year, into=c("site_id","year"),sep=" ") %>%
                       dplyr::group_by(site_id) %>%
                       dplyr::summarize(
                         avg_n_site = mean(n, na.rm=TRUE),
                         sd_n_site = sd(n, na.rm=TRUE))) %>% 
    dplyr::mutate_if(is.numeric, round, 2)
}

###############################
# s123 Tables - Count by Filter Type and Season
###############################
create_table_count_filtertype_season <- function(df_sub_prj){
  df_sub_prj %>%
    dplyr::mutate(filter_type=na_if(filter_type, "")) %>%
    tidyr::drop_na(survey_global_id, gid, season, filter_type, filter_label) %>%
    dplyr::select(gid, filter_type, season) %>% 
    dplyr::mutate(ft_st=paste0(filter_type, " ", season)) %>%
    dplyr::group_by(ft_st) %>%
    dplyr::count(ft_st) %>%
    dplyr::mutate(perc=round((n/nrow(df_sub_prj %>%
                                       dplyr::mutate(filter_type=na_if(filter_type, "")) %>%
                                       tidyr::drop_na(survey_global_id, gid, season, filter_type, filter_label)))*100, 2)) %>%
    tidyr::separate(ft_st, into=c("filter_type","season"),sep=" ") %>%
    dplyr::mutate(filter_type=factor(filter_type, levels= c("nitex", "gff", "supor", "cn", "other"))) %>%
    dplyr::mutate(season=factor(season, levels= c("Winter", "Spring", "Summer", "Fall"))) %>%
    dplyr::arrange(filter_type, season) %>%
    dplyr::left_join(df_sub_prj %>%
                       dplyr::mutate(filter_type=na_if(filter_type, "")) %>%
                       tidyr::drop_na(survey_global_id, gid, season, filter_type, filter_label) %>%
                       dplyr::select(gid, filter_type, season) %>% 
                       dplyr::mutate(ft_st=paste0(filter_type, " ", season)) %>%
                       dplyr::group_by(ft_st) %>%
                       dplyr::count(ft_st) %>%
                       tidyr::separate(ft_st, into=c("filter_type","season"),sep=" ") %>%
                       dplyr::group_by(season) %>%
                       dplyr::summarize(
                         avg_n_season = mean(n, na.rm=TRUE),
                         sd_n_season = sd(n, na.rm=TRUE))) %>% 
    dplyr::mutate_if(is.numeric, round, 2)
}

###############################
# s123 Tables - Count by Filter Type and System
###############################
create_table_count_filtertype_system <- function(df_sub_prj){
  df_sub_prj %>%
    dplyr::mutate(filter_type=na_if(filter_type, "")) %>%
    tidyr::drop_na(survey_global_id, gid, season, filter_type, filter_label) %>%
    dplyr::select(gid, filter_type, system_type) %>% 
    dplyr::mutate(ft_st=paste0(filter_type, " ", system_type)) %>%
    dplyr::group_by(ft_st) %>%
    dplyr::count(ft_st) %>%
    dplyr::mutate(perc=round((n/nrow(df_sub_prj %>%
                                       dplyr::mutate(filter_type=na_if(filter_type, "")) %>%
                                       tidyr::drop_na(survey_global_id, gid, season, filter_type, filter_label)))*100, 2)) %>%
    tidyr::separate(ft_st, into=c("filter_type","system_type"),sep=" ") %>%
    dplyr::mutate(filter_type=factor(filter_type, levels= c("nitex", "gff", "supor", "cn", "other"))) %>%
    dplyr::mutate(system_type=factor(system_type, levels= c("Coast", "Estuary", "Stream", "Lake", "Aquarium", "other"))) %>%
    dplyr::arrange(filter_type, system_type) %>%
    dplyr::left_join(df_sub_prj %>%
                       dplyr::mutate(filter_type=na_if(filter_type, "")) %>%
                       tidyr::drop_na(survey_global_id, gid, season, filter_type, filter_label) %>%
                       dplyr::select(gid, filter_type, system_type) %>% 
                       dplyr::mutate(ft_st=paste0(filter_type, " ", system_type)) %>%
                       dplyr::group_by(ft_st) %>%
                       dplyr::count(ft_st) %>%
                       tidyr::separate(ft_st, into=c("filter_type","system_type"),sep=" ") %>%
                       dplyr::group_by(system_type) %>%
                       dplyr::summarize(
                         avg_n_system = mean(n, na.rm=TRUE),
                         sd_n_system = sd(n, na.rm=TRUE))) %>% 
    dplyr::mutate_if(is.numeric, round, 2)
}

###############################
# s123 Tables - Count by Filter Type and Year
###############################
create_table_count_filtertype_year <- function(df_sub_prj){
  df_sub_prj %>%
    dplyr::mutate(filter_type=na_if(filter_type, "")) %>%
    tidyr::drop_na(survey_global_id, gid, season, filter_type, filter_label) %>%
    dplyr::select(gid, filter_type, survey_year) %>% 
    dplyr::mutate(ft_year=paste0(filter_type, " ", survey_year)) %>%
    dplyr::group_by(ft_year) %>%
    dplyr::count(ft_year) %>%
    dplyr::mutate(perc=round((n/nrow(df_sub_prj %>%
                                       dplyr::mutate(filter_type=na_if(filter_type, "")) %>%
                                       tidyr::drop_na(survey_global_id, gid, season, filter_type, filter_label)))*100, 2)) %>%
    tidyr::separate(ft_year, into=c("filter_type","year"),sep=" ") %>%
    dplyr::mutate(filter_type=factor(filter_type, levels= c("nitex", "gff", "supor", "cn", "other"))) %>%
    dplyr::arrange(filter_type, year) %>%
    dplyr::left_join(df_sub_prj %>%
                       dplyr::mutate(filter_type=na_if(filter_type, "")) %>%
                       tidyr::drop_na(survey_global_id, gid, season, filter_type, filter_label) %>%
                       dplyr::select(gid, filter_type, survey_year) %>% 
                       dplyr::mutate(ft_year=paste0(filter_type, " ", survey_year)) %>%
                       dplyr::group_by(ft_year) %>%
                       dplyr::count(ft_year) %>%
                       tidyr::separate(ft_year, into=c("filter_type","year"),sep=" ") %>%
                       dplyr::group_by(year) %>%
                       dplyr::summarize(
                         avg_n_year = mean(n, na.rm=TRUE),
                         sd_n_year = sd(n, na.rm=TRUE))) %>% 
    dplyr::mutate_if(is.numeric, round, 2)
}

###############################
# s123 Barplots - Count by Season
###############################
create_barplot_count_season <- function(df_sub_prj, dfname) {
#outputPngFileName <- file.path(outputPngFolder,paste0("s123_survey_all_season_count_barplot.png")) 
plotTitle = sprintf("Count of %s Records by Season", tools::toTitleCase(dfname))
p<-ggplot(df_sub_prj %>%
         dplyr::filter(!if_any(c(gid, season), is.na)) %>%
         dplyr::select(gid, season) %>% 
         dplyr::group_by(season) %>%
         dplyr::count(season) %>%
         dplyr::mutate(perc=round((n/nrow(df_sub_prj %>%
                                            dplyr::filter(!if_any(c(gid, season), is.na))))*100, 2)) %>%
         dplyr::mutate(season=factor(season, levels= c("Winter", "Spring", "Summer", "Fall"))), 
       aes(season, n, fill=season)) +
  geom_bar(stat="identity") +
  xlab("Season") +
  ylab("Count") +
  ggtitle(plotTitle) +
  viridis::scale_fill_viridis(discrete = T) +
  theme(plot.title = element_text(hjust = 0.5)) +
  #theme(axis.text.x = element_text(angle=50, hjust=1)) +
  theme(legend.position = "none")
return(p)
}

###############################
# s123 Barplots - Count by System
###############################
create_barplot_count_system <- function(df_sub_prj, dfname) {
  #outputPngFileName <- file.path(outputPngFolder,paste0("s123_survey_all_season_count_barplot.png")) 
  plotTitle = sprintf("Count of %s Records by System", tools::toTitleCase(dfname))
  ggplot(df_sub_prj %>%
           dplyr::filter(!if_any(c(gid, system_type), is.na)) %>%
           dplyr::select(gid, system_type) %>% 
           dplyr::group_by(system_type) %>%
           dplyr::count(system_type) %>%
           dplyr::mutate(perc=round((n/nrow(df_sub_prj %>%
                                              dplyr::filter(!if_any(c(gid, system_type), is.na))))*100, 2)) %>%
           dplyr::mutate(system_type=factor(system_type, levels= c("Coast", "Estuary", "Stream", "Lake", "Aquarium", "other"))), 
         aes(system_type, n, fill=system_type)) +
    geom_bar(stat="identity") +
    xlab("System") +
    ylab("Count") +
    ggtitle(plotTitle) +
    viridis::scale_fill_viridis(discrete = T) +
    theme(plot.title = element_text(hjust = 0.5)) +
    #theme(axis.text.x = element_text(angle=50, hjust=1)) +
    theme(legend.position = "none") 
}
###############################
# s123 Barplots - Count by Site
###############################
create_barplot_count_site <- function(df_sub_prj, dfname) {
  #outputPngFileName <- file.path(outputPngFolder,paste0("s123_envmeas_all_sids_count_barplot.png"))
  plotTitle = sprintf("Count of %s Records by Site", tools::toTitleCase(dfname))
  ggplot(df_sub_prj %>%
           dplyr::filter(!if_any(c(gid, survey_date), is.na)) %>%
           dplyr::select(gid, site_id) %>% 
           dplyr::group_by(site_id) %>%
           dplyr::count(site_id),
         aes(site_id, n)) +
    geom_bar(position="stack", stat="identity") +
    viridis::scale_fill_viridis(discrete = T) +
    xlab("Site ID") +
    ylab("Count") +
    ggtitle(plotTitle) +
    theme(axis.text.x = element_text(angle=50, hjust=1)) +
    theme(plot.title = element_text(hjust = 0.5)) +
    theme(legend.position = "none") 
}
###############################
# s123 Barplots - Count by Season and Year
###############################
create_barplot_count_season_year <- function(df_sub_prj, dfname) {
#outputPngFileName <- file.path(outputPngFolder,paste0("s123_survey_all_season_year_count_barplot.png"))
plotTitle = sprintf("Count of %s Records by Season and Year", tools::toTitleCase(dfname))
ggplot(df_sub_prj %>%
         dplyr::filter(!if_any(c(gid, season_year), is.na)) %>%
         dplyr::select(gid, season_year) %>% 
         dplyr::group_by(season_year) %>%
         dplyr::count(season_year) %>%
         dplyr::mutate(perc=round((n/nrow(df_sub_prj %>%
                                            dplyr::filter(!if_any(c(gid, season_year), is.na))))*100, 2)) %>%
         tidyr::separate(season_year, into=c("season","year"),sep=" ") %>%
         dplyr::mutate(season=factor(season, levels= c("Winter", "Spring", "Summer", "Fall"))),
       aes(season, n)) +
  geom_bar(aes(fill=year), position="stack", stat="identity") +
  viridis::scale_fill_viridis(discrete = T) +
  xlab("Season") +
  ylab("Count") + 
  labs(fill="") +
  #theme(legend.position="top", legend.box = "horizontal") +
  ggtitle(plotTitle) +
  #theme(axis.text.x = element_text(angle=50, hjust=1)) +
  theme(plot.title = element_text(hjust = 0.5)) 
}

###############################
# s123 Barplots - Count by System and Year
###############################
create_barplot_count_system_year <- function(df_sub_prj, dfname) {
  #outputPngFileName <- file.path(outputPngFolder,paste0("s123_survey_all_season_year_count_barplot.png"))
  plotTitle = sprintf("Count of %s Records by System and Year", tools::toTitleCase(dfname))
  ggplot(df_sub_prj %>%
           dplyr::filter(!if_any(c(gid, survey_year, system_type), is.na)) %>%
           dplyr::select(gid, survey_year, system_type) %>% 
           dplyr::mutate(sys_year=paste0(system_type, " ", survey_year)) %>%
           dplyr::group_by(sys_year) %>%
           dplyr::count(sys_year) %>%
           dplyr::mutate(perc=round((n/nrow(df_sub_prj %>%
                                              dplyr::filter(!if_any(c(gid, survey_year, system_type), is.na))))*100, 2)) %>%
           tidyr::separate(sys_year, into=c("system_type","year"),sep=" ") %>%
           dplyr::mutate(system_type=factor(system_type, levels= c("Coast", "Estuary", "Stream", "Lake", "Aquarium", "other"))),
         aes(system_type, n)) +
    geom_bar(aes(fill=year), position="stack", stat="identity") +
    viridis::scale_fill_viridis(discrete = T) +
    xlab("System") +
    ylab("Count") +
    labs(fill="") +
    #theme(legend.position="top", legend.box = "horizontal") +
    ggtitle(plotTitle) +
    #theme(axis.text.x = element_text(angle=50, hjust=1)) +
    theme(plot.title = element_text(hjust = 0.5)) 
}

###############################
# s123 Barplots - Count by Month and Year - barSRMonthYear
###############################
create_barplot_count_month_year <- function(df_sub_prj, dfname) {
  #outputPngFileName <- file.path(outputPngFolder,paste0("s123_survey_all_month_year_count_barplot.png"))
  plotTitle = sprintf("Count of %s Records by Month and Year", tools::toTitleCase(dfname))
  ggplot(df_sub_prj %>%
           dplyr::filter(!if_any(c(gid, survey_date), is.na)) %>%
           dplyr::select(gid, survey_date) %>%
           dplyr::mutate(survey_date = as.Date(survey_date, "%m/%d/%Y")) %>%
           dplyr::mutate(survey_date = lubridate::ymd(survey_date, tz="UTC")) %>%
           dplyr::mutate(survey_yrmo = format(survey_date, format="%b %Y")) %>%
           dplyr::group_by(survey_yrmo) %>%
           dplyr::count(survey_yrmo) %>%
           dplyr::mutate(perc=round((n/nrow(df_sub_prj %>%
                                              dplyr::filter(!if_any(c(gid, survey_date), is.na))))*100, 2)) %>%
           tidyr::separate(survey_yrmo, into=c("month","year"),sep=" ") %>%
           dplyr::mutate(month=factor(month, levels= c("Jan", "Feb", "Mar", 
                                                       "Apr", "May", "Jun", 
                                                       "Jul", "Aug", "Sep", 
                                                       "Oct", "Nov", "Dec"))),
         aes(month, n)) +
    geom_bar(aes(fill=year), position="stack", stat="identity") +
    viridis::scale_fill_viridis(discrete = T) +
    xlab("Month") +
    ylab("Count") +
    labs(fill="") +
    #theme(legend.position="top", legend.box = "horizontal") +
    ggtitle(plotTitle) +
    #theme(axis.text.x = element_text(angle=50, hjust=1)) +
    theme(plot.title = element_text(hjust = 0.5)) 
}

###############################
# s123 Barplots - Count by Site and Year
###############################
create_barplot_count_site_year <- function(df_sub_prj, dfname) {
  #outputPngFileName <- file.path(outputPngFolder,paste0("s123_survey_all_sids_year_count_barplot.png"))
  plotTitle = sprintf("Count of %s Records by Site and Year", tools::toTitleCase(dfname))
  ggplot(df_sub_prj %>%
           dplyr::filter(!if_any(c(gid, survey_date), is.na)) %>%
           dplyr::select(gid, site_id, survey_year) %>% 
           dplyr::mutate(sid_year=paste0(site_id, " ", survey_year)) %>%
           dplyr::group_by(sid_year) %>%
           dplyr::count(sid_year) %>%
           dplyr::mutate(perc=round((n/nrow(df_sub_prj %>%
                                              dplyr::filter(!if_any(c(gid, survey_date), is.na))))*100, 2)) %>%
           tidyr::separate(sid_year, into=c("site_id","year"),sep=" "),
         aes(site_id, n)) +
    geom_bar(aes(fill=year), position="stack", stat="identity") +
    viridis::scale_fill_viridis(discrete = T) +
    xlab("Site ID") +
    ylab("Count") +
    labs(fill="") +
    ggtitle(plotTitle) +
    theme(axis.text.x = element_text(angle=50, hjust=1)) +
    theme(plot.title = element_text(hjust = 0.5)) 
}

###############################
# s123 Barplots - Count by Filter Type by Season
###############################
create_barplot_count_filtertype_season <- function(df_sub_prj, selected_filtertype_fmt) {
  #outputPngFileName <- file.path(outputPngFolder,paste0("s123_filters_type_season_count_barplot.png"))
  plotTitle = sprintf("Count of %s by Season", selected_filtertype_fmt)
  ggplot(df_sub_prj %>%
           dplyr::mutate(filter_type=na_if(filter_type, "")) %>%
           tidyr::drop_na(survey_global_id, gid, season, filter_type, filter_label) %>%
           dplyr::select(gid, filter_type, season) %>% 
           dplyr::mutate(ft_st=paste0(filter_type, " ", season)) %>%
           dplyr::group_by(ft_st) %>%
           dplyr::count(ft_st) %>%
           dplyr::mutate(perc=round((n/nrow(df_sub_prj %>%
                                              dplyr::mutate(filter_type=na_if(filter_type, "")) %>%
                                              tidyr::drop_na(survey_global_id, gid, season, filter_type, filter_label)))*100, 2)) %>%
           tidyr::separate(ft_st, into=c("filter_type","season"),sep=" ") %>%
           dplyr::mutate(filter_type=factor(filter_type, levels= c("nitex", "gff", "supor", "cn", "other"))) %>%
           dplyr::mutate(season=factor(season, levels= c("Winter", "Spring", "Summer", "Fall"))),
         aes(season, n)) +
    geom_bar(aes(fill=filter_type), position="stack", stat="identity") +
    viridis::scale_fill_viridis(discrete = T) +
    xlab("Season") +
    ylab("Count") +
    labs(fill="") +
    #theme(legend.position="top", legend.box = "horizontal") +
    ggtitle(plotTitle) +
    theme(plot.title = element_text(hjust = 0.5))
}
###############################
# s123 Barplots - Count by Filter Type by System
###############################

create_barplot_count_filtertype_system <- function(df_sub_prj, selected_filtertype_fmt) {
  #outputPngFileName <- file.path(outputPngFolder,paste0("s123_filters_type_system_count_barplot.png"))
  plotTitle = sprintf("Count of %s by System", selected_filtertype_fmt)
  ggplot(df_sub_prj %>%
           dplyr::mutate(filter_type=na_if(filter_type, "")) %>%
           tidyr::drop_na(survey_global_id, gid, season, filter_type, filter_label) %>%
           dplyr::select(gid, filter_type, system_type) %>% 
           dplyr::mutate(ft_st=paste0(filter_type, " ", system_type)) %>%
           dplyr::group_by(ft_st) %>%
           dplyr::count(ft_st) %>%
           dplyr::mutate(perc=round((n/nrow(df_sub_prj %>%
                                              dplyr::mutate(filter_type=na_if(filter_type, "")) %>%
                                              tidyr::drop_na(survey_global_id, gid, season, filter_type, filter_label)))*100, 2)) %>%
           tidyr::separate(ft_st, into=c("filter_type","system_type"),sep=" ") %>%
           dplyr::mutate(filter_type=factor(filter_type, levels= c("nitex", "gff", "supor", "cn", "other"))) %>%
           dplyr::mutate(system_type=factor(system_type, levels= c("Coast", "Estuary", "Stream", "Lake", "Aquarium", "other"))),
         aes(system_type, n)) +
    geom_bar(aes(fill=filter_type), position="stack", stat="identity") +
    viridis::scale_fill_viridis(discrete = T) +
    xlab("System") +
    ylab("Count") +
    labs(fill="") +
    #theme(legend.position="top", legend.box = "horizontal") +
    ggtitle(plotTitle) +
    theme(plot.title = element_text(hjust = 0.5))
}
###############################
# s123 Barplots - Count by Filter Type by Year
###############################
create_barplot_count_filtertype_year <- function(df_sub_prj, selected_filtertype_fmt) {
  # outputPngFileName <- file.path(outputPngFolder, paste0("s123_filters_type_year_count_barplot.png"))
  plotTitle = sprintf("Count of %s by Year", selected_filtertype_fmt)
  ggplot(df_sub_prj %>%
           dplyr::mutate(filter_type=na_if(filter_type, "")) %>%
           tidyr::drop_na(survey_global_id, gid, season, filter_type, filter_label) %>%
           dplyr::select(gid, filter_type, survey_year) %>% 
           dplyr::mutate(ft_year=paste0(filter_type, " ", survey_year)) %>%
           dplyr::group_by(ft_year) %>%
           dplyr::count(ft_year) %>%
           dplyr::mutate(perc=round((n/nrow(df_sub_prj %>%
                                              dplyr::mutate(filter_type=na_if(filter_type, "")) %>%
                                              tidyr::drop_na(survey_global_id, gid, season, filter_type, filter_label)))*100, 2)) %>%
           tidyr::separate(ft_year, into=c("filter_type","year"),sep=" ") %>%
           dplyr::mutate(filter_type=factor(filter_type, levels= c("nitex", "gff", "supor", "cn", "other"))),
         aes(filter_type, n)) +
    geom_bar(aes(fill=year), position="stack", stat="identity") +
    viridis::scale_fill_viridis(discrete = T) +
    xlab(selected_filtertype_fmt) +
    ylab("Count") +
    labs(fill="") +
    #theme(legend.position="top", legend.box = "horizontal") +
    ggtitle(plotTitle) +
    theme(plot.title = element_text(hjust = 0.5))
}

###############################
# s123 Summary Tables - EnvMeas
###############################
create_summary_envmeas_table <- function(survey_envmeas_join, selected_var_plots, start_range, end_range) {
  df_summaryplot <- survey_envmeas_join %>%
    dplyr::filter(!if_any(c(selected_var_plots, site_id), is.na)) %>%
    dplyr::select(gid, survey_date, project_ids, system_type,
                  site_id, other_site_id, general_location_name, 
                  supervisor, username, recorder_first_name, recorder_last_name, 
                  envmeas_datetime, envmeas_depth, envmeas_instrument,
                  ctd_filename, ctd_notes, ysi_filename, ysi_model, ysi_serial_number, ysi_notes, secchi_depth,
                  secchi_notes, niskin_number, niskin_notes, other_instruments, env_measurements, flow_rate,
                  water_temp, salinity, ph, par1, par2, turbidity,
                  conductivity, do, pheophytin, chla, no3no2, no2,
                  nh4, phosphate, bottom_substrate, lab_date, envmeas_notes,
                  lat_manual, long_manual, 
                  survey_month, survey_year, season, season_year, survey_global_id) %>%
    dplyr::filter(!!as.symbol(selected_var_plots)>=start_range & !!as.symbol(selected_var_plots)<=end_range)
}

###############################
# s123 Summary Tables - Collection
###############################
create_summary_col_table <- function(survey_collection_join, selected_plot_coltype, start_range, end_range) {
  if (selected_plot_coltype == "water_sample"){
    selected_var_plots<-"water_depth"
    df_summaryplot <- survey_collection_join %>%
      dplyr::mutate(collection_type=na_if(collection_type, "")) %>%
      dplyr::filter(!if_any(c(survey_global_id, gid, collection_type, selected_var_plots), is.na)) %>%
      dplyr::filter(collection_type==selected_plot_coltype) %>%
      dplyr::select(gid, collection_type, survey_date, project_ids,
                    system_type, site_id, other_site_id, general_location_name, 
                    supervisor, username, recorder_first_name, recorder_last_name,
                    water_collect_datetime, water_control, water_control_type, water_depth,
                    water_vessel_material, water_vessel_color, water_vessel_label, 
                    water_collect_notes, was_filtered, lat_manual, long_manual,
                    survey_month, survey_year, season, season_year, 
                    survey_global_id)
  } else if (selected_plot_coltype == "sed_sample"){
    selected_var_plots<-"depth_core_collected"
    df_summaryplot <- survey_collection_join %>%
      dplyr::mutate(collection_type=na_if(collection_type, "")) %>%
      filter(!if_any(c(survey_global_id, gid, collection_type, selected_var_plots), is.na)) %>%
      dplyr::filter(collection_type==selected_plot_coltype) %>%
      dplyr::select(gid, collection_type, survey_date, project_ids,
                    system_type, site_id, other_site_id, general_location_name, 
                    supervisor, username, recorder_first_name, recorder_last_name, 
                    core_datetime_start, core_datetime_end,core_label,core_control,
                    core_method,depth_core_collected,core_length,core_diameter,
                    core_notes,subcores_taken,purpose_other_cores, lat_manual, long_manual,
                    survey_month, survey_year, season, season_year,
                    survey_global_id)
  }
  df_summaryplot %>%
    dplyr::filter(!!as.symbol(selected_var_plots)>=start_range & !!as.symbol(selected_var_plots)<=end_range)
}

###############################
# s123 Summary Plots - Histogram
###############################
create_plot_histogram <- function(df_inrange, selected_var_plots) {
  #selected_var_plots_fmt <- tools::toTitleCase(gsub("_", " ",selected_var_plots))
  selected_var_plots_fmt <- var_fmt$var_fmt[var_fmt$var==selected_var_plots]
  # outputPngFileName <- file.path(outputPngFolder, paste0(sprintf("s123_%s_%s_histogram.png", dfname, selected_var_plots_fmt))
  plotTitle=sprintf("%s Distribution", selected_var_plots_fmt)
  # check distribution
  ggplot(df_inrange,
         aes_string(x=selected_var_plots)) + 
    ylab("Density") +
    xlab(selected_var_plots_fmt) +
    ggtitle(plotTitle) +
    geom_histogram(aes(y=..density..), colour="black", fill="white") +
    geom_density(alpha=.2, fill="#FF6666") +
    theme(plot.title = element_text(hjust = 0.5))
}
###############################
# s123 Summary Plots - Boxplot by Site and Month
###############################
create_plot_boxplot_site_month <- function(df_inrange, selected_var_plots) {
  #selected_var_plots_fmt <- tools::toTitleCase(gsub("_", " ",selected_var_plots))
  selected_var_plots_fmt <- var_fmt$var_fmt[var_fmt$var==selected_var_plots]
  # outputPngFileName <- file.path(outputPngFolder, paste0(sprintf("s123_%s_%s_sids_month_boxplot.png", dfname, selected_var_plots)))
  plotTitle=sprintf("%s by Site and Month", selected_var_plots_fmt)
  ggplot(df_inrange %>%
           dplyr::filter(!if_any(c(selected_var_plots, site_id), is.na)) %>% 
           dplyr::mutate(survey_date = as.Date(survey_date, "%m/%d/%Y")) %>%
           dplyr::mutate(survey_date = lubridate::ymd(survey_date, tz="UTC")) %>%
           dplyr::mutate(month = format(survey_date, format="%b")) %>%
           dplyr::mutate(month=factor(month, levels= c("Jan", "Feb", "Mar", 
                                                       "Apr", "May", "Jun", 
                                                       "Jul", "Aug", "Sep", 
                                                       "Oct", "Nov", "Dec"))) %>%
           dplyr::mutate(system_type=factor(system_type, levels= c("Coast", "Estuary", "Stream", "Lake", "Aquarium", "other"))),
         aes_string(x="site_id", y=selected_var_plots, fill="system_type")) + 
    geom_boxplot() +
    ylab(selected_var_plots_fmt) +
    xlab("Site ID") +
    labs(fill="") +
    theme(legend.position="top", legend.box = "horizontal") +
    facet_grid(month ~ .) +
    ggtitle(plotTitle) +
    theme(axis.text.x = element_text(angle=50, hjust=1)) +
    theme(plot.title = element_text(hjust = 0.5))
}
###############################
# s123 Summary Plots - Boxplot by System
###############################
create_plot_boxplot_system <- function(df_inrange, selected_var_plots) {
  #selected_var_plots_fmt <- tools::toTitleCase(gsub("_", " ",selected_var_plots))
  selected_var_plots_fmt <- var_fmt$var_fmt[var_fmt$var==selected_var_plots]
  # outputPngFileName <- file.path(outputPngFolder, paste0(sprintf("s123_%s_%s_system_boxplot.png", dfname, selected_var_plots)))
  plotTitle=sprintf("%s by System", selected_var_plots_fmt)
  ggplot(df_inrange %>%
           dplyr::mutate(system_type=factor(system_type, levels= c("Coast", "Estuary", "Stream", "Lake", "Aquarium", "other"))), 
         aes_string(x="system_type", y=selected_var_plots, fill="system_type")) + 
    geom_boxplot() +
    ylab(selected_var_plots_fmt) +
    xlab("System") +
    ggtitle(plotTitle) +
    #theme(axis.text.x = element_text(angle=50, hjust=1)) +
    theme(plot.title = element_text(hjust = 0.5)) +
    theme(legend.position = "none") 
}

###############################
# s123 Summary Plots - Boxplot by Site
###############################
create_plot_boxplot_site <- function(df_inrange, selected_var_plots) {
  #selected_var_plots_fmt <- tools::toTitleCase(gsub("_", " ",selected_var_plots))
  selected_var_plots_fmt <- var_fmt$var_fmt[var_fmt$var==selected_var_plots]
  # outputPngFileName <- file.path(outputPngFolder, paste0(sprintf("s123_%s_%s_sids_boxplot.png", dfname, selected_var_plots)))
  plotTitle=sprintf("%s by Site", selected_var_plots_fmt)
  ggplot(df_inrange %>%
           dplyr::mutate(system_type=factor(system_type, levels= c("Coast", "Estuary", "Stream", "Lake", "Aquarium", "other"))), 
         aes_string(x="site_id", y=selected_var_plots, fill="system_type")) + 
    geom_boxplot() +
    ylab(selected_var_plots_fmt) +
    xlab("Site ID") +
    labs(fill="") +
    theme(legend.position="top", legend.box = "horizontal") +
    ggtitle(plotTitle) +
    theme(axis.text.x = element_text(angle=50, hjust=1)) +
    theme(plot.title = element_text(hjust = 0.5))
}
###############################
# s123 Summary Plots - Scatterplot by System, Depth, and Month
###############################
create_plot_splot_system_depth_month <- function(df_inrange, depth_var, selected_var_plots) {
  #selected_var_plots_fmt <- tools::toTitleCase(gsub("_", " ",selected_var_plots))
  selected_var_plots_fmt <- var_fmt$var_fmt[var_fmt$var==selected_var_plots]
  # outputPngFileName <- file.path(outputPngFolder, paste0(sprintf("s123_%s_%s_system_month_depth_splot.png", dfname, selected_var_plots)))
  plotTitle=sprintf("%s by System, Depth, and Month", selected_var_plots_fmt)
  ggplot(df_inrange %>%
           dplyr::mutate(system_type=factor(system_type, levels= c("Coast", "Estuary", "Stream", "Lake", "Aquarium", "other"))) %>%   
           dplyr::mutate(survey_date = as.Date(survey_date, "%m/%d/%Y")) %>%
           dplyr::mutate(survey_date = lubridate::ymd(survey_date, tz="UTC")) %>%
           dplyr::mutate(month = format(survey_date, format="%b")) %>%
           dplyr::mutate(month=factor(month, levels= c("Jan", "Feb", "Mar", 
                                                       "Apr", "May", "Jun", 
                                                       "Jul", "Aug", "Sep", 
                                                       "Oct", "Nov", "Dec"))), 
         aes_string(x="month", y=depth_var, color=selected_var_plots)) +
    geom_point(size=2) +
    geom_jitter(width = 0.25, height = 2) +
    scale_x_discrete(name ="Month") +
    scale_y_continuous(name="Depth (M)") +
    labs(color=selected_var_plots_fmt) +
    ggtitle(plotTitle) +
    theme(plot.title = element_text(hjust = 0.5)) +
    theme(legend.position="top", legend.box = "horizontal") +
    facet_grid(system_type ~ .)
}


