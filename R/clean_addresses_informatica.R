#' A function that preps and uploads address data for cleaning via informatica
#' 
#' @param ads data.frame with address data. See details for the required columns
#' @param hhsaw_con database connection to HHSAW
#' 
#' @details 
#' The required columns include: geo_add1_raw, geo_add2_raw, geo_add3_raw, geo_city_raw, geo_state_raw, geo_zip_raw, and geo_source
#' 
#' The columns will be trimmed for whitespace and cast to upper case. geo_zip_raw will be shotened to the first 5 characters.
#' These columns must exist, but can be NA if required. Remember, "" != NA.
#' 
#' @return the timestamp used/uploaded
#' 
#' @importFrom data.table setDT
#' @importFrom stringr str_replace_na
#' @importFrom openssl sha256
#' @importFrom DBI dbGetQuery dbWriteTable Id
#' 
clean_address_informatica = function(ads, hhsaw_con){
  setDT(ads)
  
  old_hash = DBI::dbGetQuery(hhsaw_com, 'select geo_hash_raw from ref.address_clean')
  
  #missing names check
  misnames = setdiff(c('geo_add1_raw', 'geo_add2_raw', 'geo_add3_raw',
                     'geo_city_raw', 'geo_state_raw', 'geo_zip_raw',
                     'geo_source'), names(ads))
  
  if(length(misnames)>0){
    stop(paste('Missing the following columns:', collapse = ', '))
  }
  
  #Create the time stamp
  ts = Sys.time()
  
  #Clean the columns
  ads[, .(geo_add1_raw = toupper(trimws(street)), 
          geo_add2_raw = toupper(trimws(street2)),
          geo_add3_raw = toupper(trimws(street2)),
          geo_city_raw = toupper(trimws(city)),
          geo_state_raw = toupper(trimws(state)),
          geo_zip_raw = substr(toupper(as.character(ZIP),1,5)),
          geo_source = geo_source,
          timestamp = ts)]
  
  #create geohoash
  keepnames = names(ads)
  rawnames = grep('raw', names(ads), value = T)

  #add the raw geohash
  ads[, (rawnames) := lapply(rawnames, function(x) stringr::str_replace_na(get(x), ''))]
  ads[, geo_hash_raw := openssl::sha256(paste(
    geo_add1_raw, geo_add2_raw, geo_add3_raw, geo_city_raw,
    geo_state_raw, geo_zip_raw, sep = '|'
  ))]
  ads[, geo_hash_raw := toupper(geo_hash_raw)]
  #clean up/replace NAs back
  ads[, (rawnames) := lapply(.SD, function(x) {
    x[x==""] <- NA_character_
    x
  }), .SDcols = rawnames]
  
  #See if the regenerating of the hashes caught anything else
  # This is probably unnecessary, but the operation isn't TOO expensive
  ads = ads[!geo_hash_raw %in% old_hash$geo_hash_raw, .SD, .SDcols = keepnames]
  
  if(nrow(ads)>0){
    DBI::dbWriteTable(aw,
                 name = DBI::Id(schema = 'ref', table = 'informatica_address_input'),
                 value = ads,
                 append = TRUE)
  }else{
    warning('0 rows uploaded. Everything is already in ref.address_clean')
  }
  
  return(ts)
  
}
