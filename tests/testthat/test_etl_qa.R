library(rads)
library(DBI)

# test etl_qa_run_pipeline functionality ----
  # Note: tests should usually be self contained, but the nature of these functions 
  # necessitates testing some with our infrastructure
  
  # For efficiency, will only test the high level function since it calls on all 
  # sub functions. 

  # Create a temporary directory to save output ----
  myOutputFolder <- tempdir()
  
  # Run with RADS ----
  qa.rads <- etl_qa_run_pipeline(
    data_source_type = 'rads',
    data_params = list(
      function_name = 'get_data_birth',
      time_var = 'chi_year',
      time_range = c(2021, 2022),
      cols = c('chi_age', 'race4', 'birth_weight_grams', 'birthplace_city', 
               'num_prev_cesarean', 'mother_date_of_birth'),
      version = 'final', 
      kingco = FALSE, 
      check_chi = FALSE
    ), 
    output_directory = myOutputFolder
  )
  
  
  # Run with R dataframe ----
  birth_data <- rads::get_data_birth(year = c(2021:2022), 
                               kingco = F, 
                               cols = c('chi_age', 'race4', 'birth_weight_grams', 
                                        'birthplace_city', 'num_prev_cesarean', 
                                        'chi_year', 'mother_date_of_birth'), 
  )
  qa.df <- etl_qa_run_pipeline(
    data_source_type = 'r_dataframe',
    data_params = list(
      data = birth_data,
      time_var = 'chi_year',
      time_range = c(2021, 2022),
      cols = c('chi_age', 'race4', 'birth_weight_grams', 'birthplace_city', 
               'num_prev_cesarean', 'mother_date_of_birth'), 
      check_chi = FALSE
    ), 
    output_directory = myOutputFolder
  )
  
  
  # Run with SQL Server ----
  myconnection <- rads::validate_hhsaw_key()
  qa.sql <- etl_qa_run_pipeline(
    data_source_type = 'sql_server',
    connection = myconnection,
    data_params = list(
      schema_table = 'birth.final_analytic',
      time_var = 'chi_year',
      time_range = c(2021, 2022),
      cols =c('chi_age', 'race4', 'birth_weight_grams', 'birthplace_city', 
              'num_prev_cesarean', 'mother_date_of_birth'), 
      check_chi = FALSE
    ), 
    output_directory = myOutputFolder
  )

  # Run with SQL Server & check_chi = TRUE ----
  myconnection <- rads::validate_hhsaw_key()
  qa.sqlchi <- etl_qa_run_pipeline(
    data_source_type = 'sql_server',
    connection = myconnection,
    data_params = list(
      schema_table = 'birth.final_analytic',
      time_var = 'chi_year',
      time_range = c(2021, 2022),
      cols =c('chi_age', 'race4', 'birth_weight_grams', 'birthplace_city', 
              'num_prev_cesarean', 'mother_date_of_birth'), 
      check_chi = TRUE
    ), 
    output_directory = myOutputFolder
  )  
  
  # Actual tests ----
  test_that("Identical names of objects in the returned result list", {
    expect_identical(names(qa.rads), names(qa.df)) 
    expect_identical(names(qa.rads), names(qa.sql)) 
  })  
  
  test_that("Results are identical regardless of method of accessing data", {
    expect_identical(qa.rads$final, qa.df$final)
    expect_identical(qa.rads$final, qa.sql$final)
  })
  
  test_that("1 Excel file and 2 PDF files were exported", {
    expect_true(file.exists(qa.rads$exported$pdf_missing))
    expect_true(file.exists(qa.rads$exported$pdf_values))
    expect_true(file.exists(qa.rads$exported$excel))
    
    expect_true(file.exists(qa.df$exported$pdf_missing))
    expect_true(file.exists(qa.df$exported$pdf_values))
    expect_true(file.exists(qa.df$exported$excel))
    
    expect_true(file.exists(qa.sql$exported$pdf_missing))
    expect_true(file.exists(qa.sql$exported$pdf_values))
    expect_true(file.exists(qa.sql$exported$excel))
  })
  
  test_that('chi_check = TRUE works as expected', {
    # all specified cols are still there when use chi_check = TRUE
    expect_true(all(qa.sql$final$missingness$varname %in% qa.sqlchi$final$missingness$varname))
    
    # chi_check = TRUE returns additional cols, at least 20 of which begin with 'chi'
    expect_gt(sum(grepl('^chi_', setdiff(unique(qa.sqlchi$final$missingness$varname), unique(qa.sql$final$missingness$varname)))), 
              20)
  })
  
  test_that('Ensure kingco = TRUE works as expected', {
    KCfalse <- etl_qa_run_pipeline(
      data_source_type = 'rads',
      data_params = list(
        function_name = 'get_data_birth',
        time_var = 'chi_year',
        time_range = c(2021, 2022),
        cols = c('chi_geo_kc'),
        version = 'final', 
        kingco = FALSE, 
        check_chi = FALSE
      ), 
      output_directory = myOutputFolder
    )
    
    KCtrue <- etl_qa_run_pipeline(
      data_source_type = 'rads',
      data_params = list(
        function_name = 'get_data_birth',
        time_var = 'chi_year',
        time_range = c(2021, 2022),
        cols = c('chi_geo_kc'),
        version = 'final', 
        kingco = TRUE, 
        check_chi = FALSE
      ), 
      output_directory = myOutputFolder
    )
    
    expect_gt(sum(KCfalse$final$values$count, na.rm = T), sum(KCtrue$final$values$count, na.rm = T))
  })
  
  test_that('Ensure processing a single numeric, or character, or date var at a time does not cause problems', {
    # test for both rads and SQL, to effectively test all options since rads uses the data.frame/data.table code
    # character only ----
    qa.rads.char <- etl_qa_run_pipeline(
      data_source_type = 'rads',
      data_params = list(
        function_name = 'get_data_birth',
        time_var = 'chi_year',
        time_range = c(2021, 2022),
        cols = c('chi_geo_kc'),
        version = 'final', 
        kingco = FALSE, 
        check_chi = FALSE
      ), 
      output_directory = myOutputFolder
    )
    
    expect_identical(names(qa.rads.char), c('config', 'initial', 'final', 'exported'))
    expect_equal(nrow(qa.rads.char$final$values), 6) # two rows for chi_geo_KC == 'King County' and two rows where is.na(chi_geo_kc) and empty rows for continous and dates
    
    qa.sql.char <- etl_qa_run_pipeline(
      data_source_type = 'sql_server',
      connection = myconnection,
      data_params = list(
        schema_table = 'birth.final_analytic',
        time_var = 'chi_year',
        time_range = c(2021, 2022),
        cols =c('chi_geo_kc'), 
        check_chi = FALSE
      ), 
      output_directory = myOutputFolder
    )
    expect_identical(names(qa.sql.char), c('config', 'initial', 'final', 'exported'))
    expect_equal(nrow(qa.sql.char$final$values), 6)
    
    # continuous only ----
    qa.rads.cont <- etl_qa_run_pipeline(
      data_source_type = 'rads',
      data_params = list(
        function_name = 'get_data_birth',
        time_var = 'chi_year',
        time_range = c(2021, 2022),
        cols = c('birth_weight_grams'),
        version = 'final', 
        kingco = FALSE, 
        check_chi = FALSE
      ), 
      output_directory = myOutputFolder
    )
    
    expect_identical(names(qa.rads.cont), c('config', 'initial', 'final', 'exported'))
    expect_equal(nrow(qa.rads.cont$final$values), 4) # two years for continuous plus emptry rows for character and date
    
    qa.sql.cont <- etl_qa_run_pipeline(
      data_source_type = 'sql_server',
      connection = myconnection,
      data_params = list(
        schema_table = 'birth.final_analytic',
        time_var = 'chi_year',
        time_range = c(2021, 2022),
        cols =c('birth_weight_grams'), 
        check_chi = FALSE
      ), 
      output_directory = myOutputFolder
    )
    expect_identical(names(qa.sql.cont), c('config', 'initial', 'final', 'exported'))
    expect_equal(nrow(qa.sql.cont$final$values), 4)
    
    # date only ----
    qa.rads.date <- etl_qa_run_pipeline(
      data_source_type = 'rads',
      data_params = list(
        function_name = 'get_data_birth',
        time_var = 'chi_year',
        time_range = c(2021, 2022),
        cols = c('mother_date_of_birth'),
        version = 'final', 
        kingco = FALSE, 
        check_chi = FALSE
      ), 
      output_directory = myOutputFolder
    )
    
    expect_identical(names(qa.rads.date), c('config', 'initial', 'final', 'exported'))
    expect_equal(nrow(qa.rads.date$final$values), 4) # two years for dates plus empty row for categorical and continous
    
    qa.sql.date <- etl_qa_run_pipeline(
      data_source_type = 'sql_server',
      connection = myconnection,
      data_params = list(
        schema_table = 'birth.final_analytic',
        time_var = 'chi_year',
        time_range = c(2021, 2022),
        cols =c('mother_date_of_birth'), 
        check_chi = FALSE
      ), 
      output_directory = myOutputFolder
    )
    expect_identical(names(qa.sql.date), c('config', 'initial', 'final', 'exported'))
    expect_equal(nrow(qa.sql.date$final$values), 4)
  })
  
# test etl_qa_run_pipeline error messages ----
  
# test `default_value` (`%||%`) ----

  expect_equal(10L %||% 100, 10L) 
  expect_equal(NULL %||% 100, 100) 
  