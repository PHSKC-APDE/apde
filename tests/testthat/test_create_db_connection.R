test_that("create_db_connection validates arguments correctly", {
  # Test for invalid server
  expect_error(create_db_connection(server = "invalid_server"), 
               "'arg' should be one of")
  
  # Test for invalid prod
  expect_error(create_db_connection(prod = "yes"), 
               "'prod' must be a logical value")
  
  # Test for invalid interactive
  expect_error(create_db_connection(interactive = "no"), 
               "'interactive' must be a logical value")
})

test_that("create_db_connection builds expected connection parameters", {
  # Mock DBI::dbConnect to avoid actual connections
  mockery::stub(create_db_connection, "DBI::dbConnect", function(...) {
    args <- list(...)
    return(args)  # Return the arguments instead of making a connection
  })
  
  # Mock odbc::dbConnect to avoid actual connections
  mockery::stub(create_db_connection, "odbc::dbConnect", function(...) {
    args <- list(...)
    return(args)  # Return the arguments instead of making a connection
  })
  
  # Mock keyring functions
  mockery::stub(create_db_connection, "keyring::key_list", function(...) {
    return(list(username = "test_user"))
  })
  
  mockery::stub(create_db_connection, "keyring::key_get", function(...) {
    return("test_password")
  })
  
  # Test phextractstore prod connection
  phextract_prod <- create_db_connection("phextractstore", prod = TRUE)
  expect_equal(phextract_prod$Server, "KCITSQLPRPHIP40")
  expect_equal(phextract_prod$Database, "PHExtractStore")
  
  # Test phextractstore dev connection
  phextract_dev <- create_db_connection("phextractstore", prod = FALSE)
  expect_equal(phextract_dev$Server, "KCITSQLUATHIP40")
  expect_equal(phextract_dev$Database, "PHExtractStore")
  
  # Test hhsaw production connection (non-interactive)
  hhsaw_prod <- create_db_connection("hhsaw", prod = TRUE, interactive = FALSE)
  expect_equal(hhsaw_prod$database, "hhs_analytics_workspace")
  expect_equal(hhsaw_prod$server, "tcp:kcitazrhpasqlprp16.azds.kingcounty.gov,1433")
  expect_equal(hhsaw_prod$Authentication, "ActiveDirectoryPassword")
  
  # Test inthealth production connection (non-interactive)
  inthealth_prod <- create_db_connection("inthealth", prod = TRUE, interactive = FALSE)
  expect_equal(inthealth_prod$database, "inthealth_edw")
  expect_equal(inthealth_prod$server, "tcp:kcitazrhpasqlprp16.azds.kingcounty.gov,1433")
  expect_equal(inthealth_prod$Authentication, "ActiveDirectoryPassword")
})

# Run additional manual tests in /tests/manual/test_connections